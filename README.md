# 📦 Telegram Media Forwarding Bot

A Telegram bot built with **Pyrofork** and **MongoDB** that automatically forwards videos and documents from source channels to target channels with smart batch distribution, duplicate detection, and a separate links/text forwarding channel.

---

## ✨ Features

- 🔄 **Auto Forwarding** — Forwards videos and documents from source channels to target channels automatically
- 🔗 **Links & Text Forwarding** — Separately forwards messages containing links or text to a dedicated links channel
- 📦 **Batch Distribution** — Distributes messages evenly across multiple target channels in configurable batch sizes
- 🛡️ **Duplicate Detection** — Skips already-forwarded files using unique file hash tracking
- 💾 **MongoDB Persistence** — All settings, state, and stats are saved to MongoDB and survive restarts
- ⚡ **FloodWait Handling** — Automatically waits and retries on Telegram rate limits
- 🖥️ **Server Status** — Real-time CPU, RAM, disk, network, and uptime stats
- ♻️ **Remote Update** — Pull latest code from git and restart the bot via command
- 👑 **Admin-Only Commands** — All commands are restricted to authorized admins only

---

## ⚙️ Environment Variables

| Variable | Description |
|---|---|
| `SESSION` | Pyrogram session string |
| `BOT_TOKEN` | Your Telegram bot token from [@BotFather](https://t.me/BotFather) |
| `API_ID` | Telegram API ID from [my.telegram.org](https://my.telegram.org) |
| `API_HASH` | Telegram API Hash from [my.telegram.org](https://my.telegram.org) |
| `MONGO_URI` | MongoDB connection URI (`mongodb+srv://...`) |
| `ADMINS` | Space-separated list of admin Telegram user IDs (e.g. `123456 789012`) |
| `SOURCE_CHANNELS` | Space-separated source channel IDs (used only on first run, then saved to DB) |
| `TARGET_CHANNELS` | Space-separated target channel IDs (used only on first run, then saved to DB) |
| `PORT` | Port for Flask health check server (default: `8080`) |

---

## 🚀 Installation & Setup

**1. Clone the repository**
```bash
git clone https://github.com/yourusername/your-repo-name.git
cd your-repo-name
```

**2. Install dependencies**
```bash
pip install -r requirements.txt
```

**3. Set environment variables**
```bash
export SESSION="your_session_string"
export BOT_TOKEN="your_bot_token"
export API_ID="your_api_id"
export API_HASH="your_api_hash"
export MONGO_URI="your_mongo_uri"
export ADMINS="123456789"
export SOURCE_CHANNELS="-100xxxxxxxxx"
export TARGET_CHANNELS="-100yyyyy -100zzzzz"
```

**4. Run the bot**
```bash
bash start.sh
```

Or with Docker:
```bash
docker build -t forwarder-bot .
docker run --env-file .env forwarder-bot
```

---

## 🤖 Admin Commands

### 📂 Source & Target Management
| Command | Description |
|---|---|
| `/add_source ID1 ID2 ...` | Add one or more source channel IDs |
| `/del_source ID1 ID2 ...` | Remove one or more source channel IDs |
| `/add_target ID1 ID2 ...` | Add one or more target channel IDs |
| `/del_target ID1 ID2 ...` | Remove one or more target channel IDs |

### 🔗 Links Channel
| Command | Description |
|---|---|
| `/set_links_channel ID` | Set the dedicated channel for links & text forwarding |
| `/del_links_channel` | Remove the links channel and disable links forwarding |
| `/toggle_links` | Toggle links & text forwarding ON/OFF |

### ⚙️ Settings & Info
| Command | Description |
|---|---|
| `/set_batch 500` | Set the batch size per target channel |
| `/toggle_dup` | Toggle duplicate file detection ON/OFF |
| `/botstatus` | Show bot stats and current forwarding state |
| `/view_ids` | Export full source, target, and links channel ID list as a file |

### 🖥️ System
| Command | Description |
|---|---|
| `/serverstatus` | Show real-time CPU, RAM, disk, network, and uptime stats |
| `/update` | Pull latest code from git, reinstall deps, and restart the bot |

---

## 🗄️ MongoDB Collections

| Collection | Purpose |
|---|---|
| `bot_config` | Stores all settings: source/target/links IDs, batch size, toggles |
| `forward_state` | Tracks the last forwarded message ID per source channel |
| `distribution_state` | Tracks current target index and message count for batch rotation |
| `processed_hashes` | Stores unique file hashes to prevent duplicates |
| `bot_stats` | Tracks total number of messages forwarded |

---

## 📋 Requirements

- Python 3.11+
- pyrofork
- tgcrypto
- uvloop
- pymongo[srv]
- Flask + gunicorn
- psutil
- dnspython

---

## 📝 Notes

- The bot forwards **videos and documents** to target channels — other media types are ignored for video forwarding
- **Links/text forwarding** is separate and independent — it forwards any message from source channels that contains text or URLs to the links channel
- Source/target channel IDs must be **numeric** (e.g. `-100xxxxxxxxx`)
- Settings are loaded from environment variables on the **first run only**, then saved to MongoDB
- The bot must be a **member** of all source, target, and links channels with appropriate permissions
- `/update` requires `git` to be installed in the environment (included in the Dockerfile)
