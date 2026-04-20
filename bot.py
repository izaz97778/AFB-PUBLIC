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
# Default Batch Size if not set
DEFAULT_BATCH_SIZE = 1000
# IDs of users allowed to use admin commands
ADMINS = [int(admin) for admin in environ.get("ADMINS", "").split()]
# --- END CONFIGURATION ---

id_pattern = re.compile(r'^.\d+$')

# Load from environment
SESSION = environ.get("SESSION", "")
BOT_TOKEN = environ.get("BOT_TOKEN", "")
API_ID = int(environ.get("API_ID", ""))
API_HASH = environ.get("API_HASH", "")
MONGO_URI = environ.get("MONGO_URI", "")

# Global variables
TARGET_CHANNELS = [int(ch) if id_pattern.search(ch) else ch for ch in environ.get("TARGET_CHANNELS", "").split()]
SOURCE_CHANNELS = [int(ch) if id_pattern.search(ch) else ch for ch in environ.get("SOURCE_CHANNELS", "").split()]
BATCH_SIZE = DEFAULT_BATCH_SIZE

# Setup MongoDB
mongo = MongoClient(MONGO_URI)
db = mongo["forwarding_bot"]
state_collection = db["forward_state"]
distribution_collection = db["distribution_state"]
config_collection = db["bot_config"] # New collection for settings

# --- MongoDB Helpers ---

def load_config():
    """Loads BATCH_SIZE from database or uses default."""
    global BATCH_SIZE
    doc = config_collection.find_one({"_id": "settings"})
    if doc:
        BATCH_SIZE = doc.get("batch_size", DEFAULT_BATCH_SIZE)

def save_batch_config(new_size):
    """Saves new BATCH_SIZE to database."""
    config_collection.update_one(
        {"_id": "settings"},
        {"$set": {"batch_size": new_size}},
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
        new_size = int(message.command[1])
        if new_size < 1:
            return await message.reply("Batch size must be at least 1.")
        
        BATCH_SIZE = new_size
        save_batch_config(new_size)
        await message.reply(f"✅ BATCH_SIZE updated to `{BATCH_SIZE}`.")
    except ValueError:
        await message.reply("Please provide a valid number.")

@app.on_message(filters.command(["add_source", "add_target", "del_source", "del_target"]) & filters.user(ADMINS))
async def manage_ids(client, message):
    global SOURCE_CHANNELS, TARGET_CHANNELS
    cmd = message.command[0]
    
    if len(message.command) < 2:
        return await message.reply("Usage: `/command ID` \nExample: `/add_source -3942849208`")

    try:
        new_id = int(message.command[1])
    except ValueError:
        return await message.reply("Please provide a valid numeric ID.")

    if cmd == "add_source":
        if new_id not in SOURCE_CHANNELS:
            SOURCE_CHANNELS.append(new_id)
            await message.reply(f"✅ Added `{new_id}` to Source list.")
        else:
            await message.reply("ID is already in Sources.")
            
    elif cmd == "add_target":
        if new_id not in TARGET_CHANNELS:
            TARGET_CHANNELS.append(new_id)
            await message.reply(f"✅ Added `{new_id}` to Target list.")
        else:
            await message.reply("ID is already in Targets.")

    elif cmd == "del_source":
        if new_id in SOURCE_CHANNELS:
            SOURCE_CHANNELS.remove(new_id)
            await message.reply(f"❌ Removed `{new_id}` from Sources.")
        else:
            await message.reply("ID not found in Sources.")

    elif cmd == "del_target":
        if new_id in TARGET_CHANNELS:
            TARGET_CHANNELS.remove(new_id)
            await message.reply(f"❌ Removed `{new_id}` from Targets.")
        else:
            await message.reply("ID not found in Targets.")

@app.on_message(filters.command("status") & filters.user(ADMINS))
async def show_status(client, message):
    status_text = (
        f"**🤖 Bot Status**\n\n"
        f"📂 **Sources:** `{SOURCE_CHANNELS}`\n"
        f"🎯 **Targets:** `{TARGET_CHANNELS}`\n"
        f"📊 **Batch Size:** `{BATCH_SIZE}` messages"
    )
    await message.reply(status_text)

# --- MAIN FORWARDER LOGIC ---

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
        
        # Use the dynamic BATCH_SIZE
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

# --- Execution ---

async def main():
    load_config() # Initial load from MongoDB
    await app.start()
    me = await app.get_me()
    print(f"✅ Bot is running as {me.first_name}!")
    print(f"📊 Current Batch Size: {BATCH_SIZE}")
    await asyncio.Event().wait()

if __name__ == "__main__":
    app.run(main())
