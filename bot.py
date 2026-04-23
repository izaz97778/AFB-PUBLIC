import uvloop
import asyncio
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from pymongo import MongoClient
import re
from os import environ

print("Starting Bot with Stats & Duplicate Checking...")
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
hash_collection = db["processed_hashes"]  # For duplicate checking
stats_collection = db["bot_stats"]        # For total forward counts

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
    config_collection.update_one({"_id": "settings"}, {"$set": {
        "source_ids": SOURCE_CHANNELS, "target_ids": TARGET_CHANNELS, "batch_size": BATCH_SIZE
    }}, upsert=True)

# --- Hash & Stats Helpers ---

def is_duplicate(file_hash):
    """Check if file_unique_id exists in DB."""
    return hash_collection.find_one({"_id": file_hash}) is not None

def save_hash(file_hash):
    """Save unique ID to prevent duplicates."""
    hash_collection.update_one({"_id": file_hash}, {"$set": {"seen": True}}, upsert=True)

def increment_total_stats():
    """Count every successful forward."""
    stats_collection.update_one({"_id": "global_stats"}, {"$inc": {"total": 1}}, upsert=True)

def get_total_stats():
    doc = stats_collection.find_one({"_id": "global_stats"})
    return doc["total"] if doc else 0

def get_last_forwarded(chat_id):
    doc = state_collection.find_one({"_id": str(chat_id)})
    return doc["last_message_id"] if doc else 0

def save_last_forwarded(chat_id, message_id):
    state_collection.update_one({"_id": str(chat_id)}, {"$set": {"last_message_id": message_id}}, upsert=True)

def get_distribution_state():
    doc = distribution_collection.find_one({"_id": "batch_distribution_state"})
    if doc:
        return doc.get("current_target_index", 0), doc.get("message_count", 0)
    return 0, 0

def save_distribution_state(index, count):
    distribution_collection.update_one({"_id": "batch_distribution_state"}, 
        {"$set": {"current_target_index": index, "message_count": count}}, upsert=True)

# --- Pyrogram client setup ---
app = Client(name="forwarder", session_string=SESSION, bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH)

# --- ADMIN COMMANDS ---

@app.on_message(filters.command(["add_source", "add_target", "del_source", "del_target"]) & filters.user(ADMINS))
async def manage_ids(client, message):
    global SOURCE_CHANNELS, TARGET_CHANNELS
    cmd = message.command[0]
    if len(message.command) < 2: return await message.reply(f"Usage: `/{cmd} ID1 ID2...`")
    
    input_ids = message.command[1:]
    success_ids, failed_ids = [], []

    for raw_id in input_ids:
        try:
            clean_id = int(re.sub(r'[\[\],]', '', raw_id))
            if "add_source" == cmd and clean_id not in SOURCE_CHANNELS: SOURCE_CHANNELS.append(clean_id)
            elif "add_target" == cmd and clean_id not in TARGET_CHANNELS: TARGET_CHANNELS.append(clean_id)
            elif "del_source" == cmd and clean_id in SOURCE_CHANNELS: SOURCE_CHANNELS.remove(clean_id)
            elif "del_target" == cmd and clean_id in TARGET_CHANNELS: TARGET_CHANNELS.remove(clean_id)
            success_ids.append(str(clean_id))
        except: failed_ids.append(raw_id)

    save_db_settings()
    await message.reply(f"✅ **Done:** `{', '.join(success_ids)}`" + (f"\n❌ **Failed:** `{', '.join(failed_ids)}`" if failed_ids else ""))

@app.on_message(filters.command("set_batch") & filters.user(ADMINS))
async def update_batch(client, message):
    global BATCH_SIZE
    if len(message.command) < 2: return await message.reply("Usage: `/set_batch 1000`")
    try:
        BATCH_SIZE = int(message.command[1])
        save_db_settings()
        await message.reply(f"✅ BATCH_SIZE updated to `{BATCH_SIZE}`.")
    except: await message.reply("Invalid number.")

@app.on_message(filters.command("status") & filters.user(ADMINS))
async def show_status(client, message):
    curr_idx, curr_count = get_distribution_state()
    total_targets = len(TARGET_CHANNELS)
    total_fwd = get_total_stats()
    
    # Calculate Rotation Progress
    progress = 0
    if total_targets > 0:
        progress = round(((curr_idx + (curr_count / BATCH_SIZE)) / total_targets) * 100, 2)
    
    next_target = TARGET_CHANNELS[curr_idx % total_targets] if total_targets > 0 else "None"

    status_text = (
        f"**📊 Bot Statistics & Progress**\n\n"
        f"✅ **Total Forwarded:** `{total_fwd}` files\n"
        f"🔄 **Rotation:** `{progress}%` complete\n"
        f"🎯 **Target Index:** `{curr_idx + 1}/{total_targets}`\n"
        f"🔢 **Batch Status:** `{curr_count}/{BATCH_SIZE}`\n\n"
        f"📂 **Sources:** `{len(SOURCE_CHANNELS)}` channels\n"
        f"📍 **Next Channel:** `{next_target}`"
    )
    await message.reply(status_text)

# --- FORWARDER LOGIC ---

@app.on_message()
async def forward_messages(client, message):
    if message.text and message.text.startswith("/"): return
    if message.chat.id not in SOURCE_CHANNELS: return
    if not (message.video or message.document): return

    # 1. Duplicate Check
    media = message.video or message.document
    file_hash = media.file_unique_id # Fingerprint
    
    if is_duplicate(file_hash):
        return # Skip quietly

    # 2. Sequence Check
    chat_id = str(message.chat.id)
    last_id = get_last_forwarded(chat_id)
    if message.id <= last_id: return 

    # 3. Distribution Setup
    curr_idx, curr_count = get_distribution_state()
    if not TARGET_CHANNELS: return
    target_chat_id = TARGET_CHANNELS[curr_idx % len(TARGET_CHANNELS)]
    
    next_count = curr_count + 1
    next_idx = curr_idx
    if next_count >= BATCH_SIZE:
        next_count = 0
        next_idx = (curr_idx + 1) % len(TARGET_CHANNELS)

    # 4. Action
    while True:
        try:
            await message.copy(target_chat_id)
            save_last_forwarded(chat_id, message.id)
            save_distribution_state(next_idx, next_count)
            save_hash(file_hash)
            increment_total_stats()
            break 
        except FloodWait as e:
            await asyncio.sleep(e.value)
        except Exception as e:
            print(f"Error: {e}")
            break

async def main():
    load_all_settings()
    await app.start()
    me = await app.get_me()
    print(f"✅ Bot is active as {me.first_name}")
    await asyncio.Event().wait()

if __name__ == "__main__":
    app.run(main())
