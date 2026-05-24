# 📦 Telegram Media Forwarding Bot

A Telegram bot built with **Pyrogram** and **MongoDB** that automatically forwards videos and documents from source channels to target channels with smart batch distribution and duplicate detection.

---

## ✨ Features

- 🔄 **Auto Forwarding** — Forwards videos and documents from source channels to target channels automatically
- 📦 **Batch Distribution** — Distributes messages evenly across multiple target channels in configurable batch sizes
- 🛡️ **Duplicate Detection** — Skips already-forwarded files using unique file hash tracking
- 💾 **MongoDB Persistence** — All settings, state, and stats are saved to MongoDB and survive restarts
- ⚡ **FloodWait Handling** — Automatically waits and retries on Telegram rate limits
- 👑 **Admin-Only Commands** — All commands are restricted to authorized admins only

---

## ⚙️ Environment Variables

Set these variables before running the bot:

| Variable | Description |
|---|---|
| `SESSION` | Pyrogram session string |
| `BOT_TOKEN` | Your Telegram bot token from [@BotFather](https://t.me/BotFather) |
| `API_ID` | Telegram API ID from [my.telegram.org](https://my.telegram.org) |
| `API_HASH` | Telegram API Hash from [my.telegram.org](https://my.telegram.org) |
| `MONGO_URI` | MongoDB connection URI |
| `ADMINS` | Space-separated list of admin Telegram user IDs (e.g. `123456 789012`) |
| `SOURCE_CHANNELS` | Space-separated source channel IDs (used only on first run, then saved to DB) |
| `TARGET_CHANNELS` | Space-separated target channel IDs (used only on first run, then saved to DB) |

---

## 🚀 Installation & Setup

**1. Clone the repository**
```bash
git clone https://github.com/yourusername/your-repo-name.git
cd your-repo-name
```

**2. Install dependencies**
```bash
pip install pyrogram pymongo uvloop tgcrypto
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
python bot.py
```

---

## 🤖 Admin Commands

| Command | Description |
|---|---|
| `/add_source ID1 ID2 ...` | Add one or more source channel IDs |
| `/del_source ID1 ID2 ...` | Remove one or more source channel IDs |
| `/add_target ID1 ID2 ...` | Add one or more target channel IDs |
| `/del_target ID1 ID2 ...` | Remove one or more target channel IDs |
| `/set_batch 500` | Set the batch size per target channel |
| `/toggle_dup` | Toggle duplicate file detection ON/OFF |
| `/status` | Show bot stats and current forwarding state |
| `/view_ids` | Export full source and target channel ID lists as a file |

---

## 🗄️ MongoDB Collections

| Collection | Purpose |
|---|---|
| `bot_config` | Stores source/target IDs, batch size, and duplicate toggle |
| `forward_state` | Tracks the last forwarded message ID per source channel |
| `distribution_state` | Tracks current target index and message count for batch rotation |
| `processed_hashes` | Stores unique file hashes to prevent duplicates |
| `bot_stats` | Tracks total number of messages forwarded |

---

## 📋 Requirements

- Python 3.8+
- pyrogram
- pymongo
- uvloop
- tgcrypto

---

## 📝 Notes

- The bot only forwards **videos** and **documents** — other message types are ignored
- Source and target channel IDs must be **numeric** (e.g. `-100xxxxxxxxx`)
- Settings are loaded from environment variables on the **first run only**, then saved to MongoDB
- The bot must be a **member** of all source and target channels with appropriate permissions
