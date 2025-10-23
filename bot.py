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

# --- CONFIGURATION ---
# Set the number of messages per batch
BATCH_SIZE = 1000
# --- END CONFIGURATION ---

# Regex for checking numeric IDs (e.g. -100...)
id_pattern = re.compile(r'^.\d+$')

# Load from environment
SESSION = environ.get("SESSION", "")
API_ID = int(environ.get("API_ID", ""))
API_HASH = environ.get("API_HASH", "")
TARGET_CHANNELS = [int(ch) if id_pattern.search(ch) else ch for ch in environ.get("TARGET_CHANNELS", "").split()]
SOURCE_CHANNELS = [int(ch) if id_pattern.search(ch) else ch for ch in environ.get("SOURCE_CHANNELS", "").split()]
MONGO_URI = environ.get("MONGO_URI", "")

# Setup MongoDB
mongo = MongoClient(MONGO_URI)
db = mongo["forwarding_bot"]
# Collection to track last forwarded message ID from each source channel
state_collection = db["forward_state"]
# Collection to track the batch distribution state
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

def get_distribution_state():
    """Gets the current target index and the message count for that target."""
    doc = distribution_collection.find_one({"_id": "batch_distribution_state"})
    if doc:
        return doc.get("current_target_index", 0), doc.get("message_count", 0)
    return 0, 0 # Default: first channel, 0 messages sent

def save_distribution_state(index, count):
    """Saves the current target index and message count."""
    distribution_collection.update_one(
        {"_id": "batch_distribution_state"},
        {"$set": {"current_target_index": index, "message_count": count}},
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
    print(f"Target Channels (for batch distribution): {TARGET_CHANNELS}")
    print(f"Batch Size: {BATCH_SIZE} messages per channel")
    await asyncio.Event().wait()

# --- MODIFIED: Message Batch Distribution Handler ---
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

        # --- Batch Distribution Logic ---
        
        # 1. Get the current state
        current_target_index, message_count = get_distribution_state()
        total_targets = len(TARGET_CHANNELS)
        
        # 2. Determine the target for *this* message
        target_chat_id = TARGET_CHANNELS[current_target_index]
        
        # 3. Calculate the *next* state (for after this message succeeds)
        next_message_count = message_count + 1
        next_target_index = current_target_index
        
        # 4. Check if this batch is full
        if next_message_count >= BATCH_SIZE:
            # Batch is full, reset count and move to next channel
            next_message_count = 0
            next_target_index = (current_target_index + 1) % total_targets
            print(f"✅ Batch complete for {target_chat_id}. Moving to next target.")

        # 5. Try to forward the message
        while True:
            try:
                await message.copy(target_chat_id)
                
                # More descriptive log
                log_msg = (
                    f"✅ Batch {current_target_index + 1}/{total_targets} ({next_message_count}/{BATCH_SIZE}): "
                    f"Msg {message.id} -> {target_chat_id}"
                )
                print(log_msg)
                
                # --- IMPORTANT ---
                # On success, update *both* states:
                # 1. Save that this source message is done
                save_last_forwarded(chat_id, message.id)
                # 2. Save the state for the *next* message
                save_distribution_state(next_target_index, next_message_count)
                
                break # Success, exit loop
                
            except FloodWait as e:
                print(f"⏳ FloodWait: Waiting {e.value}s for message {message.id} -> {target_chat_id}")
                await asyncio.sleep(e.V)
            except Exception as e:
                print(f"❌ Error forwarding message {message.id} to {target_chat_id}: {e}")
                # Don't save state, will retry this message to the same target next time
                break

# --- Run the app ---
app.run(start_bot())
