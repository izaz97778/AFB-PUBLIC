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

# Global variables
TARGET_CHANNELS = []
SOURCE_CHANNELS = []
BATCH_SIZE = DEFAULT_BATCH_SIZE

# Setup MongoDB
mongo = MongoClient(MONGO_URI)
db = mongo["forwarding_bot"]
state_collection = db["forward_state"]
distribution_collection = db["distribution_state"]
config_collection = db["bot_config"] 
hash_collection = db["processed_hashes"]  # NEW: For duplicate checking
stats_collection = db["bot_stats"]        # NEW: For tracking total counts

# --- MongoDB Helpers ---

def load_all_settings():
    global SOURCE_CHANNELS, TARGET_CHANNELS, BATCH_SIZE
    doc = config_collection.find_one({"_id": "settings"})
    
    if doc:
        SOURCE_CHANNELS = doc.get("source_ids", [])
        TARGET_CHANNELS = doc.get("target_ids", [])
        BATCH_SIZE = doc.get("batch_size", DEFAULT_BATCH_SIZE)
    else:
        SOURCE_CHANNELS = [int(ch) if id_pattern.search(ch) else ch for ch in environ.get("SOURCE_CHANNELS", "").split()]
        TARGET_CHANNELS = [int(ch) if id_pattern.search(ch) else ch for ch in environ.get("TARGET_CHANNELS", "").split()]
        save_db_settings()

def save_db_settings():
    config_collection.update_one(
        {"_id": "settings"},
        {"$set": {
            "source_ids": SOURCE_CHANNELS,
            "target_ids": TARGET_CHANNELS,
            "batch_size": BATCH_SIZE
        }},
        upsert=True
    )

# NEW: Duplicate Checking Helpers
def is_duplicate(file_hash):
    return hash_collection.find_one({"_id": file_hash}) is not None

def save_hash(file_hash):
    hash_collection.update_one({"_id": file_hash}, {"$set": {"seen": True}}, upsert=True)

# NEW: Stats Helpers
def increment_stats():
    stats_collection.update_one({"_id": "total_forwarded"}, {"$inc": {"count": 1}}, upsert=True)

def get_total_stats():
    doc = stats_collection.find_one({"_id": "total_forwarded"})
    return doc["count"] if doc else 0

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
    name="forwarder",
    session_string=SESSION,
    bot_token=BOT_TOKEN,
    api_id=API_ID,
    api_hash=API_HASH
)

# --- ADMIN COMMANDS (MULTIPLE ID SUPPORT) ---

@app.on_message(filters.command(["add_source", "add_target", "del_source", "del_target"]) & filters.user(ADMINS))
async def manage_ids(client, message):
    global SOURCE_CHANNELS, TARGET_CHANNELS
    cmd = message.command[0]
    
    if len(message.command) < 2:
        return await message.reply(f"Usage: `/{cmd} ID1 ID2 ID3 ...`")

    input_ids = message.command[1:]
    success_ids = []
    failed_ids = []

    for raw_id in input_ids:
        try:
            clean_id = int(re.sub(r'[\[\],]', '', raw_id))
            
            if "add_source" == cmd:
                if clean_id not in SOURCE_CHANNELS:
                    SOURCE_CHANNELS.append(clean_id)
                    success_ids.append(str(clean_id))
            elif "add_target" == cmd:
                if clean_id not in TARGET_CHANNELS:
                    TARGET_CHANNELS.append(clean_id)
                    success_ids.append(str(clean_id))
            elif "del_source" == cmd:
                if clean_id in SOURCE_CHANNELS:
                    SOURCE_CHANNELS.remove(clean_id)
                    success_ids.append(str(clean_id))
            elif "del_target" == cmd:
                if clean_id in TARGET_CHANNELS:
                    TARGET_CHANNELS.remove(clean_id)
                    success_ids.append(str(clean_id))
        except ValueError:
            failed_ids.append(raw_id)

    if success_ids or failed_ids:
        save_db_settings()
        response = ""
        if success_ids:
            response += f"✅ **Processed:** {len(success_ids)} IDs\n"
        if failed_ids:
            response += f"❌ **Invalid IDs:** {len(failed_ids)} entries"
        await message.reply(response)

@app.on_message(filters.command("set_batch") & filters.user(ADMINS))
async def update_batch(client, message):
    global BATCH_SIZE
    if len(message.command) < 2:
        return await message.reply("Usage: `/set_batch 1000`")
    try:
        BATCH_SIZE = int(message.command[1])
        save_db_settings()
        await message.reply(f"✅ BATCH_SIZE updated to `{BATCH_SIZE}`.")
    except ValueError:
        await message.reply("Invalid number.")

@app.on_message(filters.command("status") & filters.user(ADMINS))
async def show_status(client, message):
    # FIXED: Only show COUNT to prevent MESSAGE_TOO_LONG crash
    curr_idx, curr_count = get_distribution_state()
    total_targets = len(TARGET_CHANNELS)
    total_sources = len(SOURCE_CHANNELS)
    total_fwd = get_total_stats()
    
    progress = 0
    if total_targets > 0:
        progress = round(((curr_idx + (curr_count / BATCH_SIZE)) / total_targets) * 100, 2)
    
    next_target = TARGET_CHANNELS[curr_idx % total_targets] if total_targets > 0 else "N/A"

    status_text = (
        f"**📊 Bot Statistics & Progress**\n\n"
        f"✅ **Total Files Forwarded:** `{total_fwd}`\n"
        f"🔄 **Rotation Progress:** `{progress}%` complete\n"
        f"🎯 **Next Target ID:** `{next_target}`\n"
        f"🔢 **Batch Status:** `{curr_count}/{BATCH_SIZE}`\n\n"
        f"📂 **Sources:** `{total_sources}` channels\n"
        f"📍 **Targets:** `{total_targets}` channels"
    )
    await message.reply(status_text)

# --- FORWARDER ---

@app.on_message(~filters.edited_message) # FIXED: Added ~filters.edited to prevent KeyError crash
async def forward_messages(client, message):
    if message.text and message.text.startswith("/"):
        return

    if message.chat.id in SOURCE_CHANNELS:
        if not (message.video or message.document):
            return

        # NEW: Duplicate Hash Checking logic
        media = message.video or message.document
        file_hash = media.file_unique_id
        
        if is_duplicate(file_hash):
            return  # Skip quietly

        chat_id = str(message.chat.id)
        last_id = get_last_forwarded(chat_id)

        if message.id <= last_id:
            return 

        current_target_index, message_count = get_distribution_state()
        total_targets = len(TARGET_CHANNELS)
        if total_targets == 0: return

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
                # NEW: Save hash and increment stats on success
                save_hash(file_hash)
                increment_stats()
                break 
            except FloodWait as e:
                await asyncio.sleep(e.value)
            except Exception as e:
                print(f"Error: {e}")
                break

# --- Start ---

async def main():
    load_all_settings() 
    await app.start()
    me = await app.get_me()
    print(f"✅ Logged in as: {me.first_name}")
    await asyncio.Event().wait()

if __name__ == "__main__":
    app.run(main())
