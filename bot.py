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
SOURCE_CHANNELS = [int(ch) if id_pattern.search(ch) else ch for ch in environ.get("SOURCE_CHANNELS", "-1002963353219 -1002899326384 -1001267885871 -1001247742004 -1001267885871 -1001615681369 -1002814817003 -1002221030375 -1002672501506 -1001633958065 -1002484085103 -1002287995762 -1002382301866 -1002842615271 -1002795822830 -1002657598430 -1002903711312 -1002775929686 -1002980343109 -1002852213235 -1001752488068 -1001520738081 -1002548059741 -1001202419242 -1002855487891 -1002367987798 -1002792742542 -1002673475553 -1002628804974 -1002670115170 -1002820033697 -1002621015649 -1001995810221 -1002537616822 -1002552452659 -1002813701079 -1002896849864 -1002567159570 -1003082342762 -1002766230848 -1002883951301 -1002071977054 -1002843569131 -1002811145178 -1002590466015 -1002394425543 -1003030606358 -1003086093222 -1002704288270 -1002640125569 -1002892518819 -1002523447534 -1002747946683 -1003062358024 -1002635529053 -1002717982158 -1002805412416 -1002448708330 -1002256858596 -1002603343644 -1002995730319 -1002929385575 -1001961755638 -1002818722105 -1001945247286 -1003011645187 -1002310882853 -1002871905348 -1002112854589 -1002570474246 -1002439173562 -1002620354260 -1002830554755 -1002815509088 -1002646161494 -1002726550105 -1002698739973 -1002525291498 -1002542636490 -1002364955763 -1003048704910 -1002464668943 -1002746167272 -1003067454037 -1002728791130 -1002701805658").split()]
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
