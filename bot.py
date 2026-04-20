import uvloop
import asyncio
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from pymongo import MongoClient
import re
from os import environ

print("Starting Bot...")
uvloop.install()

# --- CONFIGURATION ---
DEFAULT_BATCH_SIZE = 1000
ADMINS = [int(admin) for admin in environ.get("ADMINS", "").split()]
# --- END CONFIGURATION ---

id_pattern = re.compile(r'^.\d+$')

# Load from environment
SESSION = environ.get("SESSION", "")
BOT_TOKEN = environ.get("BOT_TOKEN", "")
API_ID = int(environ.get("API_ID", ""))
API_HASH = environ.get("API_HASH", "")
MONGO_URI = environ.get("MONGO_URI", "")

# Global variables (Initialized empty, filled by load_all_settings)
TARGET_CHANNELS = []
SOURCE_CHANNELS = []
BATCH_SIZE = DEFAULT_BATCH_SIZE

# Setup MongoDB
mongo = MongoClient(MONGO_URI)
db = mongo["forwarding_bot"]
state_collection = db["forward_state"]
distribution_collection = db["distribution_state"]
config_collection = db["bot_config"] 

# --- MongoDB Helpers ---

def load_all_settings():
    """Loads all IDs and Batch Size from DB. Falls back to ENV if DB is empty."""
    global SOURCE_CHANNELS, TARGET_CHANNELS, BATCH_SIZE
    doc = config_collection.find_one({"_id": "settings"})
    
    if doc:
        SOURCE_CHANNELS = doc.get("source_ids", [])
        TARGET_CHANNELS = doc.get("target_ids", [])
        BATCH_SIZE = doc.get("batch_size", DEFAULT_BATCH_SIZE)
    else:
        # First time run: Load from ENV and save to DB
        SOURCE_CHANNELS = [int(ch) if id_pattern.search(ch) else ch for ch in environ.get("SOURCE_CHANNELS", "").split()]
        TARGET_CHANNELS = [int(ch) if id_pattern.search(ch) else ch for ch in environ.get("TARGET_CHANNELS", "").split()]
        save_db_settings()

def save_db_settings():
    """Syncs the current global lists/vars to MongoDB."""
    config_collection.update_one(
        {"_id": "settings"},
        {"$set": {
            "source_ids": SOURCE_CHANNELS,
            "target_ids": TARGET_CHANNELS,
            "batch_size": BATCH_SIZE
        }},
        upsert=True
    )

def get_last_forwarded(chat_id):
    doc = state_collection.find_one({"_id": str(chat_id)})
    return doc["last_message_id"] if doc else 0

def save_last_forwarded(chat_id, message_id):
    state_collection.update_one(
        {"_id": str(chat_id)},
        {"$set": {"last_message_id": message_id}},
        upsert=True
    )

def get_distribution_state():
    doc = distribution_collection.find_one({"_id": "batch_distribution_state"})
    if doc:
        return doc.get("current_target_index", 0), doc.get("message_count", 0)
    return 0, 0

def save_distribution_state(index, count):
    distribution_collection.update_one(
        {"_id": "batch_distribution_state"},
        {"$set": {"current_target_index": index, "message_count": count}},
        upsert=True
    )

# --- Pyrogram client setup ---
app = Client(
    name="forwarder_session",
    session_string=SESSION,
    bot_token=BOT_TOKEN,
    api_id=API_ID,
    api_hash=API_HASH
)

# --- ADMIN COMMANDS ---

@app.on_message(filters.command("set_batch") & filters.user(ADMINS))
async def update_batch(client, message):
    global BATCH_SIZE
    if len(message.command) < 2:
        return await message.reply("Usage: `/set_batch 1000`")
    try:
        BATCH_SIZE = int(message.command[1])
        save_db_settings()
        await message.reply(f"✅ BATCH_SIZE updated to `{BATCH_SIZE}` and saved to DB.")
    except ValueError:
        await message.reply("Invalid number.")

@app.on_message(filters.command(["add_source", "add_target", "del_source", "del_target"]) & filters.user(ADMINS))
async def manage_ids(client, message):
    global SOURCE_CHANNELS, TARGET_CHANNELS
    cmd = message.command[0]
    if len(message.command) < 2:
        return await message.reply("Usage: `/command ID`")

    try:
        new_id = int(message.command[1])
    except ValueError:
        return await message.reply("Invalid numeric ID.")

    if cmd == "add_source":
        if new_id not in SOURCE_CHANNELS:
            SOURCE_CHANNELS.append(new_id)
            save_db_settings()
            await message.reply(f"✅ Added `{new_id}` to Sources & Database.")
        else:
            await message.reply("ID already exists.")
            
    elif cmd == "add_target":
        if new_id not in TARGET_CHANNELS:
            TARGET_CHANNELS.append(new_id)
            save_db_settings()
            await message.reply(f"✅ Added `{new_id}` to Targets & Database.")
        else:
            await message.reply("ID already exists.")

    elif cmd == "del_source":
        if new_id in SOURCE_CHANNELS:
            SOURCE_CHANNELS.remove(new_id)
            save_db_settings()
            await message.reply(f"❌ Removed `{new_id}` from DB.")
        else:
            await message.reply("ID not found.")

    elif cmd == "del_target":
        if new_id in TARGET_CHANNELS:
            TARGET_CHANNELS.remove(new_id)
            save_db_settings()
            await message.reply(f"❌ Removed `{new_id}` from DB.")
        else:
            await message.reply("ID not found.")

@app.on_message(filters.command("status") & filters.user(ADMINS))
async def show_status(client, message):
    status_text = (
        f"**🤖 Database Synced Status**\n\n"
        f"📂 **Sources:** `{SOURCE_CHANNELS}`\n"
        f"🎯 **Targets:** `{TARGET_CHANNELS}`\n"
        f"📊 **Batch Size:** `{BATCH_SIZE}`"
    )
    await message.reply(status_text)

# --- FORWARDER ---

@app.on_message()
async def forward_messages(client, message):
    if message.text and message.text.startswith("/"):
        return

    if message.chat.id in SOURCE_CHANNELS:
        if not (message.video or message.document):
            return

        chat_id = str(message.chat.id)
        last_id = get_last_forwarded(chat_id)

        if message.id <= last_id:
            return 

        current_target_index, message_count = get_distribution_state()
        total_targets = len(TARGET_CHANNELS)
        
        if total_targets == 0:
            return

        target_chat_id = TARGET_CHANNELS[current_target_index % total_targets]
        
        next_message_count = message_count + 1
        next_target_index = current_target_index
        
        if next_message_count >= BATCH_SIZE:
            next_message_count = 0
            next_target_index = (current_target_index + 1) % total_targets

        while True:
            try:
                await message.copy(target_chat_id)
                save_last_forwarded(chat_id, message.id)
                save_distribution_state(next_target_index, next_message_count)
                break 
            except FloodWait as e:
                await asyncio.sleep(e.value)
            except Exception as e:
                print(f"❌ Error: {e}")
                break

# --- Start ---

async def main():
    load_all_settings() # Load EVERYTHING from MongoDB on startup
    await app.start()
    me = await app.get_me()
    print(f"✅ Bot Online: {me.first_name}")
    await asyncio.Event().wait()

if __name__ == "__main__":
    app.run(main())
