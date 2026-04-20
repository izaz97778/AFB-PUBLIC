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
BATCH_SIZE = 1000
# IDs of users allowed to use /add and /del commands
ADMINS = [int(admin) for admin in environ.get("ADMINS", "").split()]
# --- END CONFIGURATION ---

id_pattern = re.compile(r'^.\d+$')

# Load from environment
SESSION = environ.get("SESSION", "")
BOT_TOKEN = environ.get("BOT_TOKEN", "")
API_ID = int(environ.get("API_ID", ""))
API_HASH = environ.get("API_HASH", "")
MONGO_URI = environ.get("MONGO_URI", "")

# Global lists to hold IDs (initialized from environment)
TARGET_CHANNELS = [int(ch) if id_pattern.search(ch) else ch for ch in environ.get("TARGET_CHANNELS", "").split()]
SOURCE_CHANNELS = [int(ch) if id_pattern.search(ch) else ch for ch in environ.get("SOURCE_CHANNELS", "").split()]

# Setup MongoDB
mongo = MongoClient(MONGO_URI)
db = mongo["forwarding_bot"]
state_collection = db["forward_state"]
distribution_collection = db["distribution_state"]

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
# Using both SESSION and BOT_TOKEN allows the bot to act as a Userbot 
# (to see other bots/private groups) while responding to bot commands.
app = Client(
    name="forwarder_session",
    session_string=SESSION,
    bot_token=BOT_TOKEN,
    api_id=API_ID,
    api_hash=API_HASH
)

# --- ADMIN COMMANDS ---

@app.on_message(filters.command(["add_source", "add_target", "del_source", "del_target"]) & filters.user(ADMINS))
async def manage_ids(client, message):
    global SOURCE_CHANNELS, TARGET_CHANNELS
    cmd = message.command[0]
    
    if len(message.command) < 2:
        return await message.reply("Usage: `/command ID` \nExample: `/add_source -1003942849208`")

    try:
        raw_id = message.command[1]
        # Auto-prefix for supergroups if user forgets -100
        if not raw_id.startswith("-") and len(raw_id) > 9:
            new_id = int(f"-100{raw_id}")
        else:
            new_id = int(raw_id)
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
    # Ignore commands from the forwarder logic
    if message.text and message.text.startswith("/"):
        return

    # Check if the message is from an authorized source
    if message.chat.id in SOURCE_CHANNELS:
        # Filter for Documents and Videos only
        if not (message.video or message.document):
            return

        chat_id = str(message.chat.id)
        last_id = get_last_forwarded(chat_id)

        # Basic duplicate check
        if message.id <= last_id:
            return 

        # --- Batch Distribution Calculation ---
        current_target_index, message_count = get_distribution_state()
        total_targets = len(TARGET_CHANNELS)
        
        if total_targets == 0:
            return # Nowhere to send!

        # Select the target based on the current index
        target_chat_id = TARGET_CHANNELS[current_target_index % total_targets]
        
        # Prepare state for the NEXT message
        next_message_count = message_count + 1
        next_target_index = current_target_index
        
        if next_message_count >= BATCH_SIZE:
            next_message_count = 0
            next_target_index = (current_target_index + 1) % total_targets

        # Attempt the copy
        while True:
            try:
                # .copy() sends the file without the "Forwarded from" tag
                await message.copy(target_chat_id)
                
                print(f"✅ Forwarded: {message.id} -> {target_chat_id} ({next_message_count}/{BATCH_SIZE})")
                
                # Save progress to Mongo
                save_last_forwarded(chat_id, message.id)
                save_distribution_state(next_target_index, next_message_count)
                break 
                
            except FloodWait as e:
                print(f"⏳ Rate limited. Waiting {e.value} seconds...")
                await asyncio.sleep(e.value)
            except Exception as e:
                print(f"❌ Error forwarding message {message.id}: {e}")
                break

# --- Execution ---

async def main():
    await app.start()
    me = await app.get_me()
    print(f"✅ Bot is running as {me.first_name}!")
    await asyncio.Event().wait()

if __name__ == "__main__":
    app.run(main())
