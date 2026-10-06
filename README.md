# Telegram Join-Request Auto-Approver Bot

Built with **Python 3.10+**, **aiogram 3.x**, **asyncpg (PostgreSQL)** / aiosqlite (optional SQLite).

Approves join requests of **private channels / groups** (the ones with
"Approve new members" enabled) one by one, at an interval you control.

## How it works
1. A user taps **Request to Join** on your invite link.
2. The bot instantly **saves the request in the SQLite queue** and DMs the user your **custom welcome message**.
3. A background worker approves **one request every N seconds** (admin-set) – only while the admin has pressed **▶️ Start**.
4. Everything survives restarts (queue, interval, welcome text, running state are in the DB).

## Setup
1. Create a bot with [@BotFather](https://t.me/BotFather) → copy token.
2. Get your numeric ID from [@userinfobot](https://t.me/userinfobot).
3. Copy `.env.example` → `.env` and fill in `BOT_TOKEN`, `ADMIN_ID` (comma-separate multiple admins) and `DATABASE_URL`.
4. Add the bot as **admin** to each channel/group with the **"Invite Users via Link / Add Members"** permission.
5. Create an invite link with **"Request admin approval"** enabled (or use a private group's join-request mode).
6. Run:
   ```
   pip install -r requirements.txt
   python main.py
   ```
7. Open the bot in private chat → `/start` → admin panel.

## Admin panel
| Button | What it does |
|---|---|
| ▶️ Start / ⏹ Stop | Begin or pause approving the queue (requests keep queuing while stopped) |
| ⏱ Set Interval | Seconds between approvals (1 – 86400) |
| 💬 Set Welcome | Text or photo+caption, formatting kept |
| 👁 Preview | See the welcome message |
| 🔔 Welcome ON/OFF | Toggle the welcome DM |
| 📋 Queue Details | Pending count per channel/group |
| ♻️ Retry Failed | Re-queue failed items |
| 🗑 Clear Pending | Empty the pending queue |

**Welcome placeholders:** `{mention}` `{first_name}` `{full_name}` `{username}` `{chat_title}`

## Notes
- Telegram only lets the bot DM a user who sent a join request for a limited window, so the welcome is sent **immediately** when the request arrives.
- Failed items (user already joined, request withdrawn, bot lost rights) are marked `failed`; fix the cause and press **Retry Failed**.
- `DATABASE_URL` accepts a PostgreSQL URL (`postgresql://user:pass@host:5432/db`, tables are created automatically) or `sqlite:///bot_data.db` for a local file. On Railway, add a PostgreSQL service and reference its `DATABASE_URL` variable.

## Files
- `main.py` – starts the bot, dispatcher and queue worker
- `features.py` – DB, join-request handler, queue worker, admin panel
- `requirements.txt`, `.env.example`, `.gitignore`
