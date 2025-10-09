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
TARGET_CHANNEL = int(environ.get("TARGET_CHANNEL", ""))
SOURCE_CHANNELS = [int(ch) if id_pattern.search(ch) else ch for ch in environ.get("SOURCE_CHANNELS", "").split()]
MONGO_URI = environ.get("MONGO_URI", "")

# Setup MongoDB
mongo = MongoClient(MONGO_URI)
db = mongo["forwarding_bot"]
state_collection = db["forward_state"]

# --- MongoDB Helpers ---
def get_last_forwarded(chat_id):
    doc = state_collection.find_one({"_id": str(chat_id)})
    return doc["last_message_id"] if doc else 0

def save_last_forwarded(chat_id, message_id):
    state_collection.update_one(
        {"_id": str(chat_id)},
        {"$set": {"last_message_id": message_id}},
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
    await app.start()
    user = await app.get_me()
    print(f"✅ Logged in as: {user.first_name} (@{user.username}) [{user.id}]")
    await asyncio.Event().wait()

# --- Message Forward Handler ---
@app.on_message(filters.channel)
async def forward_messages(client, message):
    if message.chat.id in SOURCE_CHANNELS:
        # Only forward video or document (ignore photo, audio, etc.)
        if not (message.video or message.document):
            return

        chat_id = str(message.chat.id)
        last_id = get_last_forwarded(chat_id)

        if message.id <= last_id:
            return  # Already forwarded

        while True:
            try:
                await message.copy(TARGET_CHANNEL)
                print(f"✅ Forwarded message {message.id} from {chat_id} to {TARGET_CHANNEL}")
                save_last_forwarded(chat_id, message.id)
                break
            except FloodWait as e:
                print(f"⏳ FloodWait: Waiting {e.value}s for message {message.id} from {chat_id}")
                await asyncio.sleep(e.value)
            except Exception as e:
                print(f"❌ Error forwarding message {message.id} from {chat_id}: {e}")
                break

# --- Run the app ---
app.run(start_bot())
