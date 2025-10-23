import uvloop
import asyncio
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from pymongo import MongoClient
import re
from os import environ
import asyncio

print("Starting...")
uvloop.install()

# Regex for checking numeric IDs (e.g. -100...)
id_pattern = re.compile(r'^.\d+$')

# Load from environment
SESSION = environ.get("SESSION", "")
API_ID = int(environ.get("API_ID", ""))
API_HASH = environ.get("API_HASH", "")
# --- This is a LIST of channels now ---
TARGET_CHANNELS = [int(ch) if id_pattern.search(ch) else ch for ch in environ.get("TARGET_CHANNELS", "").split()]
SOURCE_CHANNELS = [int(ch) if id_pattern.search(ch) else ch for ch in environ.get("SOURCE_CHANNELS", "").split()]
MONGO_URI = environ.get("MONGO_URI", "")

# Setup MongoDB
mongo = MongoClient(MONGO_URI)
db = mongo["forwarding_bot"]
# Collection to track last forwarded message ID from each source channel
state_collection = db["forward_state"]
# --- NEW: Collection to track the next target channel index ---
distribution_collection = db["distribution_state"]

# --- MongoDB Helpers ---

def get_last_forwarded(chat_id):
    """Gets the last message ID forwarded from a specific source channel."""
    doc = state_collection.find_one({"_id": str(chat_id)})
    return doc["last_message_id"] if doc else 0

def save_last_forwarded(chat_id, message_id):
    """Saves the last message ID forwarded from a specific source channel."""
    state_collection.update_one(
        {"_id": str(chat_id)},
        {"$set": {"last_message_id": message_id}},
        upsert=True
    )

def get_next_target_index():
    """Gets the index of the next target channel to send to."""
    doc = distribution_collection.find_one({"_id": "distribution_state"})
    return doc.get("next_target_index", 0) if doc else 0

def save_next_target_index(index):
    """Saves the index of the *next* target channel."""
    distribution_collection.update_one(
        {"_id": "distribution_state"},
        {"$set": {"next_target_index": index}},
        upsert=True
    )

# --- Pyrogram client setup ---
app = Client(
    name=SESSION,
    session_string=SESSION,
    api_id=API_ID,
    api_hash=API_HASH
)

# --- Start the bot ---
async def start_bot():
    if not TARGET_CHANNELS:
        print("❌ Error: No TARGET_CHANNELS configured. Please check your environment variables.")
        return
    if not SOURCE_CHANNELS:
        print("❌ Error: No SOURCE_CHANNELS configured. Please check your environment variables.")
        return
        
    await app.start()
    user = await app.get_me()
    print(f"✅ Logged in as: {user.first_name} (@{user.username}) [{user.id}]")
    print(f"Source Channels: {SOURCE_CHANNELS}")
    print(f"Target Channels (for distribution): {TARGET_CHANNELS}")
    await asyncio.Event().wait()

# --- MODIFIED: Message Distribution Handler (Round-Robin) ---
@app.on_message(filters.channel)
async def forward_messages(client, message):
    if message.chat.id in SOURCE_CHANNELS:
        # Only forward video or document
        if not (message.video or message.document):
            return

        chat_id = str(message.chat.id)
        last_id = get_last_forwarded(chat_id)

        if message.id <= last_id:
            return  # Already processed this message

        # --- Round-Robin Distribution Logic ---
        
        # 1. Get the current target channel for this message
        current_target_index = get_next_target_index()
        target_chat_id = TARGET_CHANNELS[current_target_index]
        
        # 2. Calculate the *next* target's index for the *next* message
        total_targets = len(TARGET_CHANNELS)
        next_target_index = (current_target_index + 1) % total_targets

        # 3. Try to forward the message to the chosen target
        while True:
            try:
                await message.copy(target_chat_id)
                print(f"✅ Distributed message {message.id} from {chat_id} to {target_chat_id} (Target {current_target_index + 1}/{total_targets})")
                
                # --- IMPORTANT ---
                # On success, update *both* states:
                # 1. Save that this source message is done
                save_last_forwarded(chat_id, message.id)
                # 2. Save the index for the *next* message
                save_next_target_index(next_target_index)
                
                break # Success, exit loop
                
            except FloodWait as e:
                print(f"⏳ FloodWait: Waiting {e.value}s for message {message.id} -> {target_chat_id}")
                await asyncio.sleep(e.value)
            except Exception as e:
                print(f"❌ Error forwarding message {message.id} to {target_chat_id}: {e}")
                # Don't save state, will retry this message to the same target next time
                break

# --- Run the app ---
app.run(start_bot())
