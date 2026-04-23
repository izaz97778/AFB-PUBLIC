import uvloop
import asyncio
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from pymongo import MongoClient
import re
from os import environ

# Initialize uvloop
uvloop.install()

# --- CONFIGURATION ---
ADMINS = [int(admin) for admin in environ.get("ADMINS", "").split() if admin]
DEFAULT_BATCH_SIZE = 1000
id_pattern = re.compile(r'^.\d+$')

# Environment Variables
SESSION = environ.get("SESSION", "")
BOT_TOKEN = environ.get("BOT_TOKEN", "")
API_ID = int(environ.get("API_ID", ""))
API_HASH = environ.get("API_HASH", "")
MONGO_URI = environ.get("MONGO_URI", "")

# Global variables & Fast Cache
TARGET_CHANNELS = []
SOURCE_CHANNELS = []
BATCH_SIZE = DEFAULT_BATCH_SIZE
HASH_CACHE = set()  # Fast RAM lookup for duplicates

# Setup MongoDB
mongo = MongoClient(MONGO_URI)
db = mongo["forwarding_bot"]
state_collection = db["forward_state"]
distribution_collection = db["distribution_state"]
config_collection = db["bot_config"] 
hash_collection = db["processed_hashes"]
stats_collection = db["bot_stats"]

# --- Optimized Helpers ---

def load_all_settings():
    global SOURCE_CHANNELS, TARGET_CHANNELS, BATCH_SIZE, HASH_CACHE
    doc = config_collection.find_one({"_id": "settings"})
    
    if doc:
        SOURCE_CHANNELS = doc.get("source_ids", [])
        TARGET_CHANNELS = doc.get("target_ids", [])
        BATCH_SIZE = doc.get("batch_size", DEFAULT_BATCH_SIZE)
    
    # Pre-load hashes into RAM for instant duplicate checking
    hashes = hash_collection.find({}, {"_id": 1})
    HASH_CACHE = {h["_id"] for h in hashes}
    print(f"✅ Loaded {len(HASH_CACHE)} hashes into memory.")

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

async def async_save_data(file_hash, chat_id, message_id, next_idx, next_count):
    """Handles all DB writes in the background to avoid blocking the main loop"""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, lambda: _sync_save(file_hash, chat_id, message_id, next_idx, next_count))

def _sync_save(file_hash, chat_id, message_id, next_idx, next_count):
    # Batch these into one operation if needed, but separate is fine for async
    hash_collection.update_one({"_id": file_hash}, {"$set": {"seen": True}}, upsert=True)
    state_collection.update_one({"_id": str(chat_id)}, {"$set": {"last_message_id": message_id}}, upsert=True)
    distribution_collection.update_one(
        {"_id": "batch_distribution_state"},
        {"$set": {"current_target_index": next_idx, "message_count": next_count}},
        upsert=True
    )
    stats_collection.update_one({"_id": "total_forwarded"}, {"$inc": {"count": 1}}, upsert=True)

# --- Pyrogram Client ---
app = Client(
    name="forwarder",
    session_string=SESSION,
    bot_token=BOT_TOKEN,
    api_id=API_ID,
    api_hash=API_HASH
)

# --- Background Worker ---

async def perform_forward(message, target_id, file_hash, next_idx, next_count):
    """The actual heavy lifting done in the background"""
    try:
        await message.copy(target_id)
        # Update Cache and DB
        HASH_CACHE.add(file_hash)
        await async_save_data(file_hash, message.chat.id, message.id, next_idx, next_count)
    except FloodWait as e:
        await asyncio.sleep(e.value)
        await perform_forward(message, target_id, file_hash, next_idx, next_count)
    except Exception as e:
        print(f"❌ Error forwarding: {e}")

# --- Handlers ---

@app.on_message(filters.all & ~filters.command(["start", "status", "set_batch", "add_source", "add_target", "del_source", "del_target"]))
async def forward_handler(client, message):
    if message.chat.id not in SOURCE_CHANNELS:
        return

    media = message.video or message.document
    if not media:
        return

    # 1. Instant Duplicate Check (RAM)
    if media.file_unique_id in HASH_CACHE:
        return

    # 2. Logic for distribution
    curr_idx, curr_count = 0, 0
    doc = distribution_collection.find_one({"_id": "batch_distribution_state"})
    if doc:
        curr_idx, curr_count = doc.get("current_target_index", 0), doc.get("message_count", 0)

    total_targets = len(TARGET_CHANNELS)
    if total_targets == 0:
        return

    target_id = TARGET_CHANNELS[curr_idx % total_targets]
    
    next_count = curr_count + 1
    next_idx = curr_idx
    
    if next_count >= BATCH_SIZE:
        next_count = 0
        next_idx = (curr_idx + 1) % total_targets

    # 3. FIRE AND FORGET: Don't 'await' the forward, just start the task
    asyncio.create_task(perform_forward(message, target_id, media.file_unique_id, next_idx, next_count))

# (Keep your existing admin commands here...)

async def main():
    load_all_settings() 
    await app.start()
    print("✅ Bot is flying!")
    await asyncio.Event().wait()

if __name__ == "__main__":
    app.run(main())
