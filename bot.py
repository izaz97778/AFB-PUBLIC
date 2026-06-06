import uvloop
import asyncio
import io
import subprocess
import sys
import os
import time
import psutil
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
url_pattern = re.compile(r'(https?://\S+|www\.\S+)', re.IGNORECASE)

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
CHECK_DUPLICATES = True
LINKS_CHANNEL = None       # Single channel ID/username for links & text
LINKS_FORWARDING_ENABLED = False  # Master on/off switch

# Setup MongoDB
mongo = MongoClient(MONGO_URI)
db = mongo["forwarding_bot"]
state_collection = db["forward_state"]
distribution_collection = db["distribution_state"]
config_collection = db["bot_config"] 
hash_collection = db["processed_hashes"]
stats_collection = db["bot_stats"]

# --- MongoDB Helpers ---

def load_all_settings():
    global SOURCE_CHANNELS, TARGET_CHANNELS, BATCH_SIZE, CHECK_DUPLICATES
    global LINKS_CHANNEL, LINKS_FORWARDING_ENABLED
    doc = config_collection.find_one({"_id": "settings"})
    
    if doc:
        SOURCE_CHANNELS = doc.get("source_ids", [])
        TARGET_CHANNELS = doc.get("target_ids", [])
        BATCH_SIZE = doc.get("batch_size", DEFAULT_BATCH_SIZE)
        CHECK_DUPLICATES = doc.get("check_duplicates", True)
        LINKS_CHANNEL = doc.get("links_channel", None)
        LINKS_FORWARDING_ENABLED = doc.get("links_forwarding_enabled", False)
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
            "batch_size": BATCH_SIZE,
            "check_duplicates": CHECK_DUPLICATES,
            "links_channel": LINKS_CHANNEL,
            "links_forwarding_enabled": LINKS_FORWARDING_ENABLED
        }},
        upsert=True
    )

def is_duplicate(file_hash):
    return hash_collection.find_one({"_id": file_hash}) is not None

def save_hash(file_hash):
    hash_collection.update_one({"_id": file_hash}, {"$set": {"seen": True}}, upsert=True)

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

# --- ADMIN COMMANDS ---

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
            response += f"✅ **Processed:** `{len(success_ids)} IDs`\n"
        if failed_ids:
            response += f"❌ **Invalid IDs:** `{len(failed_ids)} entries`"
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

@app.on_message(filters.command("toggle_dup") & filters.user(ADMINS))
async def toggle_duplicate_cmd(client, message):
    global CHECK_DUPLICATES
    CHECK_DUPLICATES = not CHECK_DUPLICATES
    save_db_settings()
    status = "ENABLED" if CHECK_DUPLICATES else "DISABLED"
    await message.reply(f"🔄 Duplicate Checking is now **{status}**.")

# --- LINKS CHANNEL COMMANDS ---

@app.on_message(filters.command("set_links_channel") & filters.user(ADMINS))
async def set_links_channel_cmd(client, message):
    global LINKS_CHANNEL
    if len(message.command) < 2:
        return await message.reply(
            "Usage: `/set_links_channel -100XXXXXXXXXX`\n"
            "Provide the channel ID (or username) where links & text will be forwarded."
        )
    raw = message.command[1]
    try:
        LINKS_CHANNEL = int(re.sub(r'[\[\],]', '', raw))
    except ValueError:
        LINKS_CHANNEL = raw  # accept @username too
    save_db_settings()
    await message.reply(f"✅ Links channel set to `{LINKS_CHANNEL}`.")

@app.on_message(filters.command("del_links_channel") & filters.user(ADMINS))
async def del_links_channel_cmd(client, message):
    global LINKS_CHANNEL, LINKS_FORWARDING_ENABLED
    LINKS_CHANNEL = None
    LINKS_FORWARDING_ENABLED = False
    save_db_settings()
    await message.reply("🗑️ Links channel removed. Links forwarding disabled.")

@app.on_message(filters.command("toggle_links") & filters.user(ADMINS))
async def toggle_links_cmd(client, message):
    global LINKS_FORWARDING_ENABLED
    if not LINKS_CHANNEL:
        return await message.reply(
            "⚠️ No links channel set yet.\nUse `/set_links_channel ID` first."
        )
    LINKS_FORWARDING_ENABLED = not LINKS_FORWARDING_ENABLED
    save_db_settings()
    status = "ENABLED ✅" if LINKS_FORWARDING_ENABLED else "DISABLED ❌"
    await message.reply(f"🔗 Links & text forwarding is now **{status}**.")

@app.on_message(filters.command("botstatus") & filters.user(ADMINS))
async def show_status(client, message):
    curr_idx, curr_count = get_distribution_state()
    total_targets = len(TARGET_CHANNELS)
    total_sources = len(SOURCE_CHANNELS)
    total_fwd = get_total_stats()
    
    progress = round(((curr_idx + (curr_count / BATCH_SIZE)) / total_targets) * 100, 2) if total_targets > 0 else 0
    next_target = TARGET_CHANNELS[curr_idx % total_targets] if total_targets > 0 else "N/A"
    dup_status = "ON" if CHECK_DUPLICATES else "OFF"

    links_status = "OFF"
    if LINKS_CHANNEL:
        links_status = f"ON → `{LINKS_CHANNEL}`" if LINKS_FORWARDING_ENABLED else f"OFF (set: `{LINKS_CHANNEL}`)"

    status_text = (
        f"**📊 Bot Statistics**\n\n"
        f"✅ **Total Forwarded:** `{total_fwd}`\n"
        f"🔄 **Rotation:** `{progress}%` complete\n"
        f"🎯 **Next Target ID:** `{next_target}`\n"
        f"🔢 **Batch Status:** `{curr_count}/{BATCH_SIZE}`\n"
        f"🛡️ **Duplicates Checking:** `{dup_status}`\n"
        f"🔗 **Links Forwarding:** `{links_status}`\n\n"
        f"📂 **Sources:** `{total_sources}` channels\n"
        f"📍 **Targets:** `{total_targets}` channels\n\n"
        f"💡 *To see full lists, use* `/view_ids`"
    )
    await message.reply(status_text)

@app.on_message(filters.command("view_ids") & filters.user(ADMINS))
async def view_ids(client, message):
    source_list = "\n".join(map(str, SOURCE_CHANNELS)) or "(none)"
    target_list = "\n".join(map(str, TARGET_CHANNELS)) or "(none)"
    links_ch_str = str(LINKS_CHANNEL) if LINKS_CHANNEL else "(not set)"
    links_enabled_str = "ON" if LINKS_FORWARDING_ENABLED else "OFF"
    
    full_text = (
        f"📊 BOT ID CONFIGURATION\n"
        f"========================\n\n"
        f"📂 SOURCE CHANNELS ({len(SOURCE_CHANNELS)}):\n"
        f"------------------------\n"
        f"{source_list}\n\n"
        f"📍 TARGET CHANNELS ({len(TARGET_CHANNELS)}):\n"
        f"------------------------\n"
        f"{target_list}\n\n"
        f"🔗 LINKS CHANNEL:\n"
        f"------------------------\n"
        f"{links_ch_str}\n"
        f"Status: {links_enabled_str}"
    )

    file_buffer = io.BytesIO(full_text.encode())
    file_buffer.name = "channel_ids.txt"

    await message.reply_document(
        document=file_buffer,
        caption=f"✅ **ID List Exported**\n📂 Sources: `{len(SOURCE_CHANNELS)}` | 📍 Targets: `{len(TARGET_CHANNELS)}`"
    )

# --- SERVER STATUS ---

@app.on_message(filters.command("serverstatus") & filters.user(ADMINS))
async def server_status(client, message):
    # CPU
    cpu_percent = psutil.cpu_percent(interval=1)
    cpu_count = psutil.cpu_count()

    # RAM
    ram = psutil.virtual_memory()
    ram_used = ram.used / (1024 ** 3)
    ram_total = ram.total / (1024 ** 3)
    ram_percent = ram.percent

    # Disk
    disk = psutil.disk_usage('/')
    disk_used = disk.used / (1024 ** 3)
    disk_total = disk.total / (1024 ** 3)
    disk_percent = disk.percent

    # Network
    net = psutil.net_io_counters()
    net_sent = net.bytes_sent / (1024 ** 3)
    net_recv = net.bytes_recv / (1024 ** 3)

    # Uptime
    boot_time = psutil.boot_time()
    uptime_seconds = int(time.time() - boot_time)
    uptime_hours = uptime_seconds // 3600
    uptime_minutes = (uptime_seconds % 3600) // 60

    # Progress bar helper
    def bar(percent):
        filled = int(percent / 10)
        return "█" * filled + "░" * (10 - filled)

    status_text = (
        f"**🖥️ Server Status**\n\n"
        f"**⚙️ CPU**\n"
        f"`{bar(cpu_percent)}` {cpu_percent}%\n"
        f"Cores: `{cpu_count}`\n\n"
        f"**🧠 RAM**\n"
        f"`{bar(ram_percent)}` {ram_percent}%\n"
        f"Used: `{ram_used:.2f} GB` / `{ram_total:.2f} GB`\n\n"
        f"**💾 Disk**\n"
        f"`{bar(disk_percent)}` {disk_percent}%\n"
        f"Used: `{disk_used:.2f} GB` / `{disk_total:.2f} GB`\n\n"
        f"**🌐 Network**\n"
        f"↑ Sent: `{net_sent:.2f} GB`\n"
        f"↓ Recv: `{net_recv:.2f} GB`\n\n"
        f"**⏱️ Uptime:** `{uptime_hours}h {uptime_minutes}m`"
    )
    await message.reply(status_text)


# --- UPDATE & RESTART ---

@app.on_message(filters.command("update") & filters.user(ADMINS))
async def update_and_restart(client, message):
    """
    Pull latest code from git and restart the bot process.
    Works in any environment where the bot runs from a git repo.
    Set GIT_REPO env var to override the repo URL for a fresh re-clone.
    """
    msg = await message.reply("🔄 **Pulling latest code from git...**")

    # Step 1: git pull
    try:
        pull_result = subprocess.run(
            ["git", "pull"],
            capture_output=True,
            text=True,
            timeout=60
        )
        pull_output = pull_result.stdout.strip() or pull_result.stderr.strip()
    except FileNotFoundError:
        return await msg.edit("❌ `git` not found. Make sure git is installed in the container.")
    except subprocess.TimeoutExpired:
        return await msg.edit("❌ `git pull` timed out after 60s.")
    except Exception as e:
        return await msg.edit(f"❌ git pull failed:\n`{e}`")

    if pull_result.returncode != 0:
        return await msg.edit(
            f"❌ **git pull failed:**\n```\n{pull_output}\n```"
        )

    already_up = "already up to date" in pull_output.lower()
    status_line = "✅ Already up to date." if already_up else f"✅ Updated:\n```\n{pull_output}\n```"

    # Step 2: install any new dependencies if requirements.txt changed
    pip_note = ""
    req_changed = not already_up and (
        "requirements.txt" in pull_output or "requirements" in pull_output
    )
    if req_changed:
        try:
            pip_result = subprocess.run(
                [sys.executable, "-m", "pip", "install", "-r", "requirements.txt", "-q"],
                capture_output=True, text=True, timeout=120
            )
            pip_note = "\n📦 Dependencies re-installed." if pip_result.returncode == 0 \
                       else f"\n⚠️ pip install failed:\n```{pip_result.stderr[:300]}```"
        except Exception as e:
            pip_note = f"\n⚠️ pip install error: `{e}`"

    await msg.edit(
        f"{status_line}{pip_note}\n\n♻️ **Restarting bot...**"
    )
    await asyncio.sleep(1)

    # Step 3: replace current process — clean restart, keeps env vars
    os.execv(sys.executable, [sys.executable] + sys.argv)


# --- FORWARDER ---

@app.on_message()
async def forward_messages(client, message):
    # Skip edited messages to prevent KeyError crash
    if getattr(message, "edit_date", None):
        return

    if message.text and message.text.startswith("/"):
        return

    if message.chat.id not in SOURCE_CHANNELS:
        return

    # ── LINKS / TEXT FORWARDING ──────────────────────────────────────────
    # Runs independently from video forwarding. Handles text messages and
    # captions that contain URLs. Media-only (no text) messages are skipped.
    if LINKS_FORWARDING_ENABLED and LINKS_CHANNEL:
        content = message.text or message.caption or ""
        if content and (url_pattern.search(content) or message.text):
            # Forward any text/caption that has content (links or plain text)
            while True:
                try:
                    await message.copy(LINKS_CHANNEL)
                    break
                except FloodWait as e:
                    await asyncio.sleep(e.value)
                except Exception as e:
                    print(f"[Links] Error forwarding to links channel: {e}")
                    break

    # ── VIDEO / DOCUMENT FORWARDING ──────────────────────────────────────
    if not (message.video or message.document):
        return

    media = message.video or message.document
    file_hash = media.file_unique_id
    
    if CHECK_DUPLICATES and is_duplicate(file_hash):
        return 

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
            if CHECK_DUPLICATES:
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
