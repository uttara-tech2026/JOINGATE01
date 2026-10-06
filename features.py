"""
All bot logic: database, queue worker, join-request handler, admin panel.

NEW in this version
  • Interval modes: 📌 Fixed (with preset jitter %)  /  🎲 Random (min–max range)
  • Welcome Manager: create / modify / view / delete multiple welcome messages,
    each with a separate caption + separate media (photo / video / GIF),
    a rename option, "set active", and a real-destination preview.
  • Queue Details = analytics: Today / Yesterday / This week / This month / All time,
    each with Approved / Pending / Failed + how many requesters are bot users
    (people who started the bot and can receive DMs).
  • 📣 Broadcast: send any message to bot users; every broadcast carries a
    🔕 Unsubscribe button, unsubscribed users are separated and excluded.
  • "Clear Pending" button removed.
  • 💬 Massage = message library: list of all messages, one ⭐ PRIMARY (sent to every
    join request), per-message Modify / View / Set Primary / Broadcast / Delete.
  • 👤 UserInfo = Subscribed list (everyone who requested to join and has NOT unsubscribed)
    + Unsubscribed list.
  • Broadcast asks the audience: Subscribed / Unsubscribed / Both.
"""
import asyncio
import csv
import io
import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone
from html import escape
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import aiosqlite
from aiogram import Bot, F, Router
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramRetryAfter,
)
from aiogram.filters import Command, CommandStart, Filter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    ChatJoinRequest,
    ChatMemberUpdated,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

log = logging.getLogger("features")

ADMIN_IDS = {
    int(x) for x in os.getenv("ADMIN_ID", "").replace(" ", "").split(",") if x.strip().isdigit()
}

# Timezone used for "Today / Yesterday / This week / This month" (e.g. Asia/Kolkata)
TZ_NAME = os.getenv("TIMEZONE", "UTC")
try:
    TZ = ZoneInfo(TZ_NAME)
except Exception:  # unknown zone or missing tzdata
    TZ, TZ_NAME = timezone.utc, "UTC"

DEFAULT_WELCOME = (
    "👋 Hello {mention}!\n\n"
    "Thanks for requesting to join <b>{chat_title}</b>.\n"
    "Your request is in the queue and will be approved shortly."
)

JITTER_PRESETS = [0, 10, 20, 30, 50]  # percent

DEFAULTS = {
    "running": "0",            # 1 = approving, 0 = stopped
    "interval_mode": "fixed",  # fixed | random
    "interval": "10",          # fixed mode: base seconds
    "jitter_pct": "20",        # fixed mode: ± percent
    "rand_min": "5",           # random mode: min seconds
    "rand_max": "20",          # random mode: max seconds
    "welcome_enabled": "1",
    "active_welcome": "0",     # id of the welcome message in use
}

MEDIA_LABEL = {"photo": "🖼 Photo", "video": "🎬 Video", "animation": "🎞 GIF", "": "— none"}

router = Router()

# ───────────────────────── Database ─────────────────────────
# DATABASE_URL examples:
#   postgresql://user:pass@host:5432/dbname      (PostgreSQL – e.g. Railway)
#   sqlite:///bot_data.db                        (local SQLite file)
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
IS_PG = DATABASE_URL.startswith(("postgres://", "postgresql://", "postgresql+asyncpg://"))
SQLITE_PATH = "bot_data.db"

if IS_PG:
    DATABASE_URL = DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://", 1)
else:
    _p = DATABASE_URL
    for _prefix in ("sqlite+aiosqlite:///", "sqlite:///"):
        if _p.startswith(_prefix):
            _p = _p[len(_prefix):]
            break
    SQLITE_PATH = _p or "bot_data.db"

_pool = None  # asyncpg pool (PostgreSQL only)


def _pg_sql(sql: str) -> str:
    """Convert '?' placeholders to $1, $2 ... for asyncpg."""
    out, n = [], 0
    for ch in sql:
        if ch == "?":
            n += 1
            out.append(f"${n}")
        else:
            out.append(ch)
    return "".join(out)


async def db_execute(sql: str, *args) -> int:
    """Run a write statement, return affected row count."""
    if IS_PG:
        status = await _pool.execute(_pg_sql(sql), *args)
        tail = status.split()[-1] if status else ""
        return int(tail) if tail.isdigit() else 0
    async with aiosqlite.connect(SQLITE_PATH) as db:
        cur = await db.execute(sql, args)
        await db.commit()
        return cur.rowcount


async def db_insert_id(sql: str, *args) -> int:
    """Run an INSERT and return the new row id."""
    if IS_PG:
        row = await _pool.fetchrow(_pg_sql(sql) + " RETURNING id", *args)
        return int(row["id"])
    async with aiosqlite.connect(SQLITE_PATH) as db:
        cur = await db.execute(sql, args)
        await db.commit()
        return int(cur.lastrowid)


async def db_fetchall(sql: str, *args) -> list[dict]:
    if IS_PG:
        rows = await _pool.fetch(_pg_sql(sql), *args)
        return [dict(r) for r in rows]
    async with aiosqlite.connect(SQLITE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(sql, args) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def db_fetchone(sql: str, *args) -> dict | None:
    rows = await db_fetchall(sql, *args)
    return rows[0] if rows else None


async def init_db():
    global _pool
    if IS_PG:
        import asyncpg
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
        pk, big, real = "BIGSERIAL PRIMARY KEY", "BIGINT", "DOUBLE PRECISION"
        log.info("Using PostgreSQL database")
    else:
        pk, big, real = "INTEGER PRIMARY KEY AUTOINCREMENT", "INTEGER", "REAL"
        await db_execute("PRAGMA journal_mode=WAL")
        log.info("Using SQLite database at %s", SQLITE_PATH)

    await db_execute(
        "CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
    )
    await db_execute(
        f"""CREATE TABLE IF NOT EXISTS queue (
            id {pk},
            chat_id {big} NOT NULL,
            chat_title TEXT,
            user_id {big} NOT NULL,
            user_chat_id {big},
            username TEXT,
            full_name TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            error TEXT,
            created_at {real} NOT NULL,
            processed_at {real},
            UNIQUE(chat_id, user_id)
        )"""
    )
    await db_execute("CREATE INDEX IF NOT EXISTS idx_queue_status ON queue(status, id)")
    await db_execute(
        f"""CREATE TABLE IF NOT EXISTS welcomes (
            id {pk},
            name TEXT NOT NULL,
            caption TEXT NOT NULL DEFAULT '',
            media_type TEXT NOT NULL DEFAULT '',
            media_id TEXT NOT NULL DEFAULT '',
            created_at {real} NOT NULL
        )"""
    )
    await db_execute(
        f"""CREATE TABLE IF NOT EXISTS users (
            user_id {big} PRIMARY KEY,
            username TEXT,
            full_name TEXT,
            started INTEGER NOT NULL DEFAULT 0,
            subscribed INTEGER NOT NULL DEFAULT 1,
            first_seen {real} NOT NULL,
            last_seen {real} NOT NULL,
            unsub_at {real}
        )"""
    )
    await db_execute(
        f"""CREATE TABLE IF NOT EXISTS broadcasts (
            id {pk},
            created_at {real} NOT NULL,
            total INTEGER NOT NULL DEFAULT 0,
            sent INTEGER NOT NULL DEFAULT 0,
            failed INTEGER NOT NULL DEFAULT 0,
            blocked INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'running'
        )"""
    )
    for k, v in DEFAULTS.items():
        await db_execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO NOTHING",
            k, v,
        )
    await backfill_users()
    await migrate_welcomes()


async def close_db():
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


async def get_setting(key: str) -> str:
    row = await db_fetchone("SELECT value FROM settings WHERE key=?", key)
    return row["value"] if row else DEFAULTS[key]


async def set_setting(key: str, value: str):
    await db_execute(
        "INSERT INTO settings (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        key, value,
    )


async def enqueue(req: ChatJoinRequest):
    u = req.from_user
    await db_execute(
        """INSERT INTO queue (chat_id, chat_title, user_id, user_chat_id, username,
                              full_name, status, created_at)
           VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)
           ON CONFLICT(chat_id, user_id) DO UPDATE SET
             status='pending', error=NULL, processed_at=NULL,
             chat_title=excluded.chat_title, user_chat_id=excluded.user_chat_id,
             username=excluded.username, full_name=excluded.full_name,
             created_at=excluded.created_at""",
        req.chat.id, req.chat.title or "", u.id, req.user_chat_id,
        u.username, u.full_name, time.time(),
    )


async def next_pending():
    return await db_fetchone("SELECT * FROM queue WHERE status='pending' ORDER BY id LIMIT 1")


async def mark(row_id: int, status: str, error: str | None = None):
    await db_execute(
        "UPDATE queue SET status=?, error=?, processed_at=? WHERE id=?",
        status, error, time.time(), row_id,
    )


async def counts() -> dict:
    out = {"pending": 0, "approved": 0, "failed": 0}
    for r in await db_fetchall("SELECT status, COUNT(*) AS n FROM queue GROUP BY status"):
        out[r["status"]] = int(r["n"])
    return out


async def pending_by_chat():
    rows = await db_fetchall(
        "SELECT chat_id, COALESCE(MAX(chat_title), '') AS title, COUNT(*) AS n FROM queue "
        "WHERE status='pending' GROUP BY chat_id ORDER BY COUNT(*) DESC"
    )
    return [(r["chat_id"], r["title"], int(r["n"])) for r in rows]


async def bulk_update(sql: str) -> int:
    return await db_execute(sql)


async def last_chat_title() -> str:
    row = await db_fetchone(
        "SELECT chat_title FROM queue WHERE chat_title <> '' ORDER BY id DESC LIMIT 1"
    )
    return row["chat_title"] if row else "Sample Channel"


# ───────────────────────── Users / analytics ─────────────────────────
async def touch_user(user, started: bool):
    """Remember a user. started=True means they interacted with the bot (can receive DMs)."""
    now = time.time()
    await db_execute(
        """INSERT INTO users (user_id, username, full_name, started, subscribed, first_seen, last_seen)
           VALUES (?, ?, ?, ?, 1, ?, ?)
           ON CONFLICT(user_id) DO UPDATE SET
             username=excluded.username, full_name=excluded.full_name,
             last_seen=excluded.last_seen,
             started=CASE WHEN excluded.started=1 THEN 1 ELSE users.started END""",
        user.id, user.username, user.full_name, 1 if started else 0, now, now,
    )


async def backfill_users():
    """Everyone who ever requested to join is a subscriber until they unsubscribe."""
    await db_execute(
        """INSERT INTO users (user_id, username, full_name, started, subscribed, first_seen, last_seen)
           SELECT user_id, MAX(username), MAX(full_name), 0, 1, MIN(created_at), MAX(created_at)
           FROM queue
           WHERE user_id NOT IN (SELECT user_id FROM users)
           GROUP BY user_id"""
    )


async def set_started(user_id: int, started: bool):
    await db_execute("UPDATE users SET started=? WHERE user_id=?", 1 if started else 0, user_id)


async def user_overview() -> dict:
    row = await db_fetchone(
        """SELECT COUNT(*) AS total,
                  COALESCE(SUM(CASE WHEN started=1 THEN 1 ELSE 0 END), 0) AS started,
                  COALESCE(SUM(CASE WHEN subscribed=1 THEN 1 ELSE 0 END), 0) AS audience,
                  COALESCE(SUM(CASE WHEN subscribed=0 THEN 1 ELSE 0 END), 0) AS unsub,
                  COALESCE(SUM(CASE WHEN started=1 AND subscribed=0 THEN 1 ELSE 0 END), 0) AS unsub_reach,
                  COALESCE(SUM(CASE WHEN started=1 AND subscribed=1 THEN 1 ELSE 0 END), 0) AS reach_sub
           FROM users"""
    )
    return {k: int(v or 0) for k, v in row.items()}


def time_windows():
    now = datetime.now(TZ)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)
    week = today - timedelta(days=today.weekday())  # Monday
    month = today.replace(day=1)
    end = (now + timedelta(days=1)).timestamp()
    return [
        ("📅 Today", today.timestamp(), end),
        ("🕘 Yesterday", yesterday.timestamp(), today.timestamp()),
        ("🗓 This week", week.timestamp(), end),
        ("📆 This month", month.timestamp(), end),
        ("♾ All time", 0.0, end),
    ]


async def window_stats(t0: float, t1: float) -> dict:
    out = {"pending": 0, "approved": 0, "failed": 0, "users": 0, "bot_users": 0}
    for r in await db_fetchall(
        "SELECT status, COUNT(*) AS n FROM queue WHERE created_at >= ? AND created_at < ? GROUP BY status",
        t0, t1,
    ):
        out[r["status"]] = int(r["n"])
    row = await db_fetchone(
        """SELECT COUNT(DISTINCT q.user_id) AS total,
                  COUNT(DISTINCT CASE WHEN u.started=1 THEN q.user_id END) AS started
           FROM queue q LEFT JOIN users u ON u.user_id = q.user_id
           WHERE q.created_at >= ? AND q.created_at < ?""",
        t0, t1,
    )
    out["users"], out["bot_users"] = int(row["total"] or 0), int(row["started"] or 0)
    return out


def fmt_ts(ts) -> str:
    return datetime.fromtimestamp(ts, TZ).strftime("%d %b %Y, %H:%M") if ts else "-"


# ───────────────────────── Welcome storage ─────────────────────────
async def wl_create(name: str, caption: str = DEFAULT_WELCOME,
                    media_type: str = "", media_id: str = "") -> int:
    return await db_insert_id(
        "INSERT INTO welcomes (name, caption, media_type, media_id, created_at) "
        "VALUES (?, ?, ?, ?, ?)",
        name, caption, media_type, media_id, time.time(),
    )


async def wl_get(wid: int) -> dict | None:
    return await db_fetchone("SELECT * FROM welcomes WHERE id=?", wid)


async def wl_all() -> list[dict]:
    return await db_fetchall("SELECT * FROM welcomes ORDER BY id")


async def wl_update(wid: int, **fields):
    allowed = {"name", "caption", "media_type", "media_id"}
    keys = [k for k in fields if k in allowed]
    if not keys:
        return
    sets = ", ".join(f"{k}=?" for k in keys)
    await db_execute(f"UPDATE welcomes SET {sets} WHERE id=?", *[fields[k] for k in keys], wid)


async def wl_delete(wid: int):
    await db_execute("DELETE FROM welcomes WHERE id=?", wid)
    if await get_setting("active_welcome") == str(wid):
        await set_setting("active_welcome", "0")  # no primary until admin picks another


async def get_active_welcome() -> dict | None:
    try:
        wid = int(await get_setting("active_welcome"))
    except ValueError:
        return None
    return await wl_get(wid) if wid else None


async def migrate_welcomes():
    """First run (or upgrade from the old single-welcome version)."""
    if await db_fetchone("SELECT id FROM welcomes LIMIT 1"):
        return
    old_t = await db_fetchone("SELECT value FROM settings WHERE key='welcome_text'")
    old_p = await db_fetchone("SELECT value FROM settings WHERE key='welcome_photo'")
    caption = old_t["value"] if old_t else DEFAULT_WELCOME
    photo = old_p["value"] if old_p else ""
    wid = await wl_create("Default", caption, "photo" if photo else "", photo)
    await set_setting("active_welcome", str(wid))


# ───────────────────────── Welcome delivery ─────────────────────────
def render_welcome(template: str, user, chat_title: str) -> str:
    full_name = escape(user.full_name or "friend")
    username = f"@{user.username}" if getattr(user, "username", None) else full_name
    mention = f'<a href="tg://user?id={user.id}">{full_name}</a>'
    return (
        template.replace("{first_name}", escape(user.first_name or "friend"))
        .replace("{full_name}", full_name)
        .replace("{username}", escape(username))
        .replace("{mention}", mention)
        .replace("{chat_title}", escape(chat_title or "the group"))
    )


async def deliver_welcome(bot: Bot, chat_id: int, w: dict, user, chat_title: str,
                          reply_markup=None) -> bool:
    """Send a message exactly as the receiver will see it. Returns False if empty."""
    text = render_welcome(w.get("caption") or "", user, chat_title).strip()
    mtype, mid = w.get("media_type") or "", w.get("media_id") or ""

    if mid:
        # Telegram caption limit is 1024 → longer text is sent as a follow-up message
        split = bool(text) and len(text) > 1024
        caption = None if split else (text or None)
        mk = None if split else reply_markup
        if mtype == "photo":
            await bot.send_photo(chat_id, mid, caption=caption, reply_markup=mk)
        elif mtype == "video":
            await bot.send_video(chat_id, mid, caption=caption, reply_markup=mk)
        elif mtype == "animation":
            await bot.send_animation(chat_id, mid, caption=caption, reply_markup=mk)
        else:
            return False
        if split:
            await bot.send_message(chat_id, text, disable_web_page_preview=True, reply_markup=reply_markup)
        return True
    if text:
        await bot.send_message(chat_id, text, disable_web_page_preview=True, reply_markup=reply_markup)
        return True
    return False


async def send_welcome(bot: Bot, chat_id: int, user, chat_title: str):
    if await get_setting("welcome_enabled") != "1":
        return
    w = await get_active_welcome()
    if not w:
        return
    try:
        await deliver_welcome(bot, chat_id, w, user, chat_title)
    except (TelegramForbiddenError, TelegramBadRequest) as e:
        log.warning("Could not send welcome to %s: %s", user.id, e)
    except Exception:
        log.exception("Welcome send failed")


# ───────────────────────── Interval logic ─────────────────────────
async def interval_cfg() -> dict:
    return {
        "mode": await get_setting("interval_mode"),
        "base": float(await get_setting("interval")),
        "jitter": int(float(await get_setting("jitter_pct"))),
        "rmin": float(await get_setting("rand_min")),
        "rmax": float(await get_setting("rand_max")),
    }


def describe_interval(c: dict) -> str:
    if c["mode"] == "random":
        return f"🎲 Random {c['rmin']:g}–{c['rmax']:g}s"
    if c["jitter"]:
        lo = c["base"] * (1 - c["jitter"] / 100)
        hi = c["base"] * (1 + c["jitter"] / 100)
        return f"📌 Fixed {c['base']:g}s ±{c['jitter']}% ({lo:.1f}–{hi:.1f}s)"
    return f"📌 Fixed {c['base']:g}s (no jitter)"


def avg_interval(c: dict) -> float:
    return (c["rmin"] + c["rmax"]) / 2 if c["mode"] == "random" else c["base"]


def pick_delay(c: dict) -> float:
    if c["mode"] == "random":
        d = random.uniform(c["rmin"], c["rmax"])
    else:
        j = c["jitter"] / 100
        d = c["base"] * random.uniform(1 - j, 1 + j) if j else c["base"]
    return max(0.5, d)


# ───────────────────────── Join-request handler ─────────────────────────
@router.chat_join_request()
async def on_join_request(req: ChatJoinRequest, bot: Bot):
    await enqueue(req)
    await touch_user(req.from_user, False)
    log.info("Queued join request user=%s chat=%s", req.from_user.id, req.chat.id)
    await send_welcome(bot, req.user_chat_id, req.from_user, req.chat.title or "")


# ───────────────────────── Queue worker ─────────────────────────
async def process_one(bot: Bot, row) -> bool:
    """Returns True if the row was handled (approved/failed), False if it should retry."""
    try:
        await bot.approve_chat_join_request(row["chat_id"], row["user_id"])
        await mark(row["id"], "approved")
        log.info("Approved user=%s chat=%s", row["user_id"], row["chat_id"])
        return True
    except TelegramRetryAfter as e:
        log.warning("Flood control, sleeping %ss", e.retry_after)
        await asyncio.sleep(e.retry_after + 1)
        return False
    except (TelegramBadRequest, TelegramForbiddenError) as e:
        await mark(row["id"], "failed", str(e)[:200])
        log.warning("Failed user=%s chat=%s: %s", row["user_id"], row["chat_id"], e)
        return True


async def queue_worker(bot: Bot):
    while True:
        try:
            if await get_setting("running") != "1":
                await asyncio.sleep(2)
                continue
            row = await next_pending()
            if row is None:
                await asyncio.sleep(2)
                continue
            if await process_one(bot, row):
                try:
                    delay = pick_delay(await interval_cfg())
                except (ValueError, TypeError):
                    delay = 10.0
                log.info("Next approval in %.1fs", delay)
                await asyncio.sleep(delay)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Worker error")
            await asyncio.sleep(5)


# ───────────────────────── Admin helpers ─────────────────────────
class IsAdmin(Filter):
    async def __call__(self, event) -> bool:
        return bool(event.from_user and event.from_user.id in ADMIN_IDS)


class Form(StatesGroup):
    iv_fixed = State()
    iv_range = State()
    wl_caption = State()
    wl_media = State()
    wl_name = State()


def fmt_duration(seconds: float) -> str:
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return " ".join(p for p in (f"{h}h" if h else "", f"{m}m" if m else "", f"{s}s" if s or not (h or m) else "") if p)


def btn(text: str, data: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text=text, callback_data=data)


async def edit(cb: CallbackQuery, text: str, kb: InlineKeyboardMarkup):
    try:
        await cb.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        pass  # "message is not modified"


# ───────────────────────── Main panel ─────────────────────────
async def panel() -> tuple[str, InlineKeyboardMarkup]:
    running = await get_setting("running") == "1"
    cfg = await interval_cfg()
    welcome_on = await get_setting("welcome_enabled") == "1"
    active = await get_active_welcome()
    c = await counts()
    eta = fmt_duration(c["pending"] * avg_interval(cfg))
    text = (
        "<b>🤖 Join Request Manager</b>\n\n"
        f"Status: {'🟢 <b>RUNNING</b>' if running else '🔴 <b>STOPPED</b>'}\n"
        f"Interval: <b>{describe_interval(cfg)}</b>\n"
        f"⭐ Primary message: <b>{escape(active['name']) if active else 'none set'}</b>\n"
        f"Auto-reply to requests: {'✅ ON' if welcome_on else '❌ OFF'}\n\n"
        f"⏳ Pending: <b>{c['pending']}</b> (≈ {eta})\n"
        f"✅ Approved: <b>{c['approved']}</b>\n"
        f"⚠️ Failed: <b>{c['failed']}</b>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [btn("⏹ Stop" if running else "▶️ Start", "toggle_run")],
        [btn("⏱ Set Interval", "iv:menu"), btn("💬 Massage", "ms:menu")],
        [btn("🔔 Welcome: ON" if welcome_on else "🔕 Welcome: OFF", "toggle_welcome"),
         btn("📋 Queue Details", "queue")],
        [btn("👤 UserInfo", "ui:menu"), btn("♻️ Retry Failed", "retry_failed")],
        [btn("🔄 Refresh", "refresh")],
    ])
    return text, kb


async def show_panel(target: Message | CallbackQuery):
    text, kb = await panel()
    if isinstance(target, CallbackQuery):
        await edit(target, text, kb)
    else:
        await target.answer(text, reply_markup=kb)


BACK_KB = InlineKeyboardMarkup(inline_keyboard=[[btn("⬅️ Back", "refresh")]])


@router.message(CommandStart(), IsAdmin())
@router.message(Command("panel"), IsAdmin())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await show_panel(message)


@router.message(Command("cancel"), IsAdmin())
async def cmd_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Cancelled.")
    await show_panel(message)


@router.message(CommandStart())
async def cmd_start_non_admin(message: Message):
    if message.chat.type == "private":
        if message.from_user:
            await touch_user(message.from_user, True)
        await message.answer("👋 This bot automatically handles channel/group join requests.")


@router.callback_query(F.data == "refresh", IsAdmin())
async def cb_refresh(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await show_panel(cb)
    await cb.answer()


@router.callback_query(F.data == "toggle_run", IsAdmin())
async def cb_toggle_run(cb: CallbackQuery):
    running = await get_setting("running") == "1"
    await set_setting("running", "0" if running else "1")
    await show_panel(cb)
    await cb.answer("Stopped ⏹" if running else "Started ▶️")


@router.callback_query(F.data == "toggle_welcome", IsAdmin())
async def cb_toggle_welcome(cb: CallbackQuery):
    on = await get_setting("welcome_enabled") == "1"
    await set_setting("welcome_enabled", "0" if on else "1")
    await show_panel(cb)
    await cb.answer("Welcome OFF" if on else "Welcome ON")


# ───────────────────────── Interval menu ─────────────────────────
async def iv_view() -> tuple[str, InlineKeyboardMarkup]:
    c = await interval_cfg()
    fixed = c["mode"] == "fixed"
    text = (
        "⏱ <b>Approval Interval</b>\n\n"
        f"Current: <b>{describe_interval(c)}</b>\n\n"
        "📌 <b>Fixed</b> – a set number of seconds with a preset random wobble (jitter).\n"
        "🎲 <b>Random</b> – every delay is a random value between your min and max."
    )
    rows = [[
        btn(("✅ " if fixed else "") + "📌 Fixed", "iv:mode:fixed"),
        btn(("" if fixed else "✅ ") + "🎲 Random", "iv:mode:random"),
    ]]
    if fixed:
        rows.append([btn(f"✏️ Set seconds ({c['base']:g}s)", "iv:set_fixed")])
        rows.append([
            btn(("✅ " if c["jitter"] == j else "") + f"±{j}%", f"iv:jit:{j}")
            for j in JITTER_PRESETS
        ])
        text += "\n\n<b>Jitter preset:</b> choose how much the fixed time may vary."
    else:
        rows.append([btn(f"✏️ Set range ({c['rmin']:g}–{c['rmax']:g}s)", "iv:set_range")])
    rows.append([btn("⬅️ Back", "refresh")])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


async def show_iv(target: Message | CallbackQuery):
    text, kb = await iv_view()
    if isinstance(target, CallbackQuery):
        await edit(target, text, kb)
    else:
        await target.answer(text, reply_markup=kb)


@router.callback_query(F.data == "iv:menu", IsAdmin())
async def cb_iv_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await show_iv(cb)
    await cb.answer()


@router.callback_query(F.data.startswith("iv:mode:"), IsAdmin())
async def cb_iv_mode(cb: CallbackQuery):
    await set_setting("interval_mode", cb.data.split(":")[2])
    await show_iv(cb)
    await cb.answer("Mode updated")


@router.callback_query(F.data.startswith("iv:jit:"), IsAdmin())
async def cb_iv_jitter(cb: CallbackQuery):
    pct = int(cb.data.split(":")[2])
    if pct in JITTER_PRESETS:
        await set_setting("jitter_pct", str(pct))
    await show_iv(cb)
    await cb.answer(f"Jitter ±{pct}%")


@router.callback_query(F.data == "iv:set_fixed", IsAdmin())
async def cb_iv_set_fixed(cb: CallbackQuery, state: FSMContext):
    await state.set_state(Form.iv_fixed)
    await cb.message.answer(
        "📌 Send the <b>fixed interval in seconds</b> (e.g. <code>10</code>).\n"
        "Allowed: 1 – 86400.\n\n/cancel to abort."
    )
    await cb.answer()


@router.message(Form.iv_fixed, IsAdmin())
async def msg_iv_fixed(message: Message, state: FSMContext):
    try:
        value = float((message.text or "").strip().replace(",", "."))
        if not 1 <= value <= 86400:
            raise ValueError
    except ValueError:
        await message.answer("❌ Send a number between 1 and 86400 (or /cancel).")
        return
    await set_setting("interval", str(value))
    await state.clear()
    await message.answer(f"✅ Fixed interval set to <b>{value:g}s</b>.")
    await show_iv(message)


@router.callback_query(F.data == "iv:set_range", IsAdmin())
async def cb_iv_set_range(cb: CallbackQuery, state: FSMContext):
    await state.set_state(Form.iv_range)
    await cb.message.answer(
        "🎲 Send the random range as <code>min-max</code> in seconds.\n"
        "Example: <code>5-20</code>\n"
        "Allowed: 1 – 86400, min must be lower than max.\n\n/cancel to abort."
    )
    await cb.answer()


@router.message(Form.iv_range, IsAdmin())
async def msg_iv_range(message: Message, state: FSMContext):
    raw = (message.text or "").strip().replace(",", ".").replace("–", "-").replace(" ", "-")
    parts = [p for p in raw.split("-") if p]
    try:
        lo, hi = float(parts[0]), float(parts[1])
        if len(parts) != 2 or not (1 <= lo < hi <= 86400):
            raise ValueError
    except (ValueError, IndexError):
        await message.answer("❌ Format: <code>min-max</code>, e.g. <code>5-20</code> (or /cancel).")
        return
    await set_setting("rand_min", str(lo))
    await set_setting("rand_max", str(hi))
    await state.clear()
    await message.answer(f"✅ Random range set to <b>{lo:g}–{hi:g}s</b>.")
    await show_iv(message)


# ───────────────────────── Massage (message library) ─────────────────────────
MENU_LABEL = "Massage"  # menu / button name – change here if you want another label


def short(html: str, n: int = 220) -> str:
    html = (html or "").strip()
    if not html:
        return "<i>(empty)</i>"
    return escape(html[:n]) + ("…" if len(html) > n else "")


async def is_primary(wid: int) -> bool:
    return await get_setting("active_welcome") == str(wid)


# ---- list of all messages ----
async def ms_menu_view() -> tuple[str, InlineKeyboardMarkup]:
    allw = await wl_all()
    active = await get_setting("active_welcome")
    prim = next((w for w in allw if str(w["id"]) == active), None)
    text = (
        f"💬 <b>{MENU_LABEL}</b>\n\n"
        f"Saved: <b>{len(allw)}</b>\n"
        f"⭐ Primary: <b>{escape(prim['name']) if prim else 'none set'}</b>\n\n"
        + ("Tap a message to modify, view, set primary, broadcast or delete it.\n"
           if allw else "No messages yet – create your first one.\n")
        + "<i>The ⭐ primary message is the one every new join request receives.</i>"
    )
    rows = [
        [btn(("⭐ " if str(w["id"]) == active else "") + w["name"][:40], f"ms:open:{w['id']}")]
        for w in allw[:50]
    ]
    rows.append([btn("➕ Create New Message", "ms:new")])
    rows.append([btn("⬅️ Back", "refresh")])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


async def show_ms_menu(target: Message | CallbackQuery):
    text, kb = await ms_menu_view()
    if isinstance(target, CallbackQuery):
        await edit(target, text, kb)
    else:
        await target.answer(text, reply_markup=kb)


@router.callback_query(F.data == "ms:menu", IsAdmin())
async def cb_ms_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await show_ms_menu(cb)
    await cb.answer()


# ---- create ----
@router.callback_query(F.data == "ms:new", IsAdmin())
async def cb_ms_new(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    wid = await wl_create("New message")
    await wl_update(wid, name=f"Message #{wid}")
    if not await get_active_welcome():
        await set_setting("active_welcome", str(wid))  # first message becomes primary
    await show_editor(cb, wid)
    await cb.answer("Created ✅")


# ---- one message: Modify / View / Set Primary / Broadcast / Delete ----
async def ms_open_view(wid: int) -> tuple[str, InlineKeyboardMarkup] | None:
    w = await wl_get(wid)
    if not w:
        return None
    prim = await is_primary(wid)
    text = (
        f"💬 <b>{escape(w['name'])}</b>  {'⭐ <i>primary</i>' if prim else ''}\n\n"
        f"📝 <b>Caption:</b>\n{short(w['caption'])}\n\n"
        f"🖼 <b>Media:</b> {MEDIA_LABEL.get(w['media_type'], '— none')}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [btn("✏️ Modify", f"ms:edit:{wid}"), btn("👁 View", f"ms:view:{wid}")],
        [btn("⭐ Primary ✓" if prim else "⭐ Set Primary", f"ms:prim:{wid}")],
        [btn("📣 Broadcast", f"ms:bc:{wid}"), btn("🗑 Delete", f"ms:del:{wid}")],
        [btn("⬅️ Back", "ms:menu")],
    ])
    return text, kb


async def show_ms_open(target: Message | CallbackQuery, wid: int):
    view = await ms_open_view(wid)
    if not view:
        if isinstance(target, CallbackQuery):
            await target.answer("Message not found.", show_alert=True)
        return
    text, kb = view
    if isinstance(target, CallbackQuery):
        await edit(target, text, kb)
    else:
        await target.answer(text, reply_markup=kb)


@router.callback_query(F.data.startswith("ms:open:"), IsAdmin())
async def cb_ms_open(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await show_ms_open(cb, int(cb.data.split(":")[2]))
    await cb.answer()


@router.callback_query(F.data.startswith("ms:prim:"), IsAdmin())
async def cb_ms_primary(cb: CallbackQuery):
    wid = int(cb.data.split(":")[2])
    if not await wl_get(wid):
        await cb.answer("Message not found.", show_alert=True)
        return
    if await is_primary(wid):
        await cb.answer("Already the primary message.")
        return
    await set_setting("active_welcome", str(wid))  # single value ⇒ only ONE primary
    await show_ms_open(cb, wid)
    await cb.answer("⭐ Now primary. The previous one is no longer primary.", show_alert=True)


# ---- editor (Modify) ----
async def editor_view(wid: int) -> tuple[str, InlineKeyboardMarkup] | None:
    w = await wl_get(wid)
    if not w:
        return None
    prim = await is_primary(wid)
    text = (
        f"✏️ <b>{escape(w['name'])}</b>  {'⭐ <i>primary</i>' if prim else ''}\n\n"
        f"📝 <b>Caption:</b>\n{short(w['caption'])}\n\n"
        f"🖼 <b>Media:</b> {MEDIA_LABEL.get(w['media_type'], '— none')}\n\n"
        "<i>Placeholders: {mention} {first_name} {full_name} {username} {chat_title}</i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [btn("📝 Set Caption", f"ms:cap:{wid}"), btn("🖼 Set Media", f"ms:media:{wid}")],
        [btn("❌ Remove Media", f"ms:nomedia:{wid}"), btn("🏷 Rename", f"ms:name:{wid}")],
        [btn("👁 Preview (real look)", f"ms:view:{wid}")],
        [btn("⬅️ Back", f"ms:open:{wid}")],
    ])
    return text, kb


async def show_editor(target: Message | CallbackQuery, wid: int):
    view = await editor_view(wid)
    if not view:
        if isinstance(target, CallbackQuery):
            await target.answer("Message not found.", show_alert=True)
        return
    text, kb = view
    if isinstance(target, CallbackQuery):
        await edit(target, text, kb)
    else:
        await target.answer(text, reply_markup=kb)


@router.callback_query(F.data.startswith("ms:edit:"), IsAdmin())
async def cb_ms_edit(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await show_editor(cb, int(cb.data.split(":")[2]))
    await cb.answer()


@router.callback_query(F.data.startswith("ms:nomedia:"), IsAdmin())
async def cb_ms_nomedia(cb: CallbackQuery):
    wid = int(cb.data.split(":")[2])
    await wl_update(wid, media_type="", media_id="")
    await show_editor(cb, wid)
    await cb.answer("Media removed")


# ---- caption ----
@router.callback_query(F.data.startswith("ms:cap:"), IsAdmin())
async def cb_ms_cap(cb: CallbackQuery, state: FSMContext):
    await state.set_state(Form.wl_caption)
    await state.update_data(wid=int(cb.data.split(":")[2]))
    await cb.message.answer(
        "📝 Send the <b>caption / text</b> for this message.\n"
        "• Formatting (bold, italic, links, spoilers…) is kept\n"
        "• Send <code>-</code> to leave it empty (media only)\n\n"
        "<b>Placeholders:</b>\n"
        "<code>{mention}</code> – clickable name\n"
        "<code>{first_name}</code>, <code>{full_name}</code>, <code>{username}</code>\n"
        "<code>{chat_title}</code> – channel/group name\n\n/cancel to abort."
    )
    await cb.answer()


@router.message(Form.wl_caption, IsAdmin())
async def msg_wl_caption(message: Message, state: FSMContext):
    if not message.text:
        await message.answer("❌ Send the caption as a text message (or /cancel).")
        return
    wid = (await state.get_data())["wid"]
    html = "" if message.text.strip() == "-" else message.html_text
    if len(html) > 4000:
        await message.answer("❌ Too long (max ~4000 characters).")
        return
    await wl_update(wid, caption=html)
    await state.clear()
    await message.answer("✅ Caption saved.")
    await show_editor(message, wid)


# ---- media ----
@router.callback_query(F.data.startswith("ms:media:"), IsAdmin())
async def cb_ms_media(cb: CallbackQuery, state: FSMContext):
    await state.set_state(Form.wl_media)
    await state.update_data(wid=int(cb.data.split(":")[2]))
    await cb.message.answer(
        "🖼 Send the <b>photo, video or GIF</b> for this message.\n"
        "The caption is set separately – any caption you attach here is ignored.\n\n"
        "/cancel to abort."
    )
    await cb.answer()


@router.message(Form.wl_media, IsAdmin())
async def msg_wl_media(message: Message, state: FSMContext):
    if message.photo:
        mtype, fid = "photo", message.photo[-1].file_id
    elif message.video:
        mtype, fid = "video", message.video.file_id
    elif message.animation:
        mtype, fid = "animation", message.animation.file_id
    else:
        await message.answer("❌ Please send a photo, video or GIF (not as a file), or /cancel.")
        return
    wid = (await state.get_data())["wid"]
    await wl_update(wid, media_type=mtype, media_id=fid)
    await state.clear()
    await message.answer(f"✅ Media saved ({MEDIA_LABEL[mtype]}).")
    await show_editor(message, wid)


# ---- rename ----
@router.callback_query(F.data.startswith("ms:name:"), IsAdmin())
async def cb_ms_name(cb: CallbackQuery, state: FSMContext):
    await state.set_state(Form.wl_name)
    await state.update_data(wid=int(cb.data.split(":")[2]))
    await cb.message.answer("🏷 Send a new <b>name</b> for this message (max 40 chars).\n\n/cancel to abort.")
    await cb.answer()


@router.message(Form.wl_name, IsAdmin())
async def msg_wl_name(message: Message, state: FSMContext):
    name = (message.text or "").strip()
    if not name or len(name) > 40:
        await message.answer("❌ Name must be 1–40 characters (or /cancel).")
        return
    wid = (await state.get_data())["wid"]
    await wl_update(wid, name=name)
    await state.clear()
    await message.answer("✅ Renamed.")
    await show_editor(message, wid)


# ---- view / preview (real destination look) ----
async def send_preview(bot: Bot, chat_id: int, wid: int, admin_user, reply_markup=None,
                       header: str = "👁 <b>Preview</b> — exactly what the receiver sees:") -> bool:
    w = await wl_get(wid)
    if not w:
        return False
    await bot.send_message(chat_id, header)
    try:
        sent = await deliver_welcome(bot, chat_id, w, admin_user, await last_chat_title(),
                                     reply_markup=reply_markup)
        if not sent:
            await bot.send_message(chat_id, "⚠️ This message is empty – add a caption or media.")
    except (TelegramBadRequest, TelegramForbiddenError) as e:
        await bot.send_message(chat_id, f"⚠️ Telegram rejected this message:\n<code>{escape(str(e))}</code>")
    return True


@router.callback_query(F.data.startswith("ms:view:"), IsAdmin())
async def cb_ms_view(cb: CallbackQuery, bot: Bot):
    ok = await send_preview(bot, cb.message.chat.id, int(cb.data.split(":")[2]), cb.from_user)
    if ok:
        await cb.answer()
    else:
        await cb.answer("Message not found.", show_alert=True)


# ---- delete ----
@router.callback_query(F.data.startswith("ms:del:"), IsAdmin())
async def cb_ms_del(cb: CallbackQuery):
    wid = int(cb.data.split(":")[2])
    w = await wl_get(wid)
    if not w:
        await cb.answer("Already deleted.", show_alert=True)
        return
    warn = ("\n\n⚠️ This is the <b>primary</b> message. After deleting it, join requests "
            "will get <b>no message</b> until you set another primary.") if await is_primary(wid) else ""
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        btn("✅ Yes, delete", f"ms:delyes:{wid}"), btn("❌ No", f"ms:open:{wid}"),
    ]])
    await edit(cb, f"🗑 Delete <b>{escape(w['name'])}</b>? This cannot be undone.{warn}", kb)
    await cb.answer()


@router.callback_query(F.data.startswith("ms:delyes:"), IsAdmin())
async def cb_ms_delyes(cb: CallbackQuery):
    await wl_delete(int(cb.data.split(":")[2]))
    await show_ms_menu(cb)
    await cb.answer("Deleted 🗑")


# ───────────────────────── Queue tools ─────────────────────────
async def queue_view() -> tuple[str, InlineKeyboardMarkup]:
    parts = [
        f"📋 <b>Queue Analytics</b>  <i>({escape(TZ_NAME)})</i>\n"
        "<i>Requests grouped by the day they were received.</i>"
    ]
    for label, t0, t1 in time_windows():
        st = await window_stats(t0, t1)
        total = st["pending"] + st["approved"] + st["failed"]
        parts.append(
            f"\n<b>{label}</b> — {total} request(s)\n"
            f"   ⏳ Pending <b>{st['pending']}</b> · ✅ Approved <b>{st['approved']}</b> · "
            f"⚠️ Failed <b>{st['failed']}</b>\n"
            f"   🤖 Bot users: <b>{st['bot_users']}</b> of {st['users']} requester(s)"
        )
    ov = await user_overview()
    parts.append(
        "\n<b>🤖 Bot users</b> <i>(started the bot, can receive DMs)</i>\n"
        f"   Known users: <b>{ov['total']}</b> · Started: <b>{ov['started']}</b>\n"
        f"   ✅ Subscribed: <b>{ov['audience']}</b> · 🔕 Unsubscribed: <b>{ov['unsub']}</b>"
    )
    rows = await pending_by_chat()
    if rows:
        parts.append("\n<b>⏳ Pending by chat</b>")
        for cid, title, n in rows[:10]:
            parts.append(f"• {escape(title or 'Untitled')} (<code>{cid}</code>): <b>{n}</b>")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [btn("🔄 Refresh", "queue"), btn("⬅️ Back", "refresh")],
    ])
    return "\n".join(parts), kb


@router.callback_query(F.data == "queue", IsAdmin())
async def cb_queue(cb: CallbackQuery):
    text, kb = await queue_view()
    await edit(cb, text, kb)
    await cb.answer()


@router.callback_query(F.data == "retry_failed", IsAdmin())
async def cb_retry(cb: CallbackQuery):
    n = await bulk_update(
        "UPDATE queue SET status='pending', error=NULL, processed_at=NULL WHERE status='failed'"
    )
    await show_panel(cb)
    await cb.answer(f"{n} failed request(s) re-queued")


# ───────────────────────── UserInfo (subscribed / unsubscribed lists) ─────────────────────────
UI_PAGE = 15


async def ui_menu_view() -> tuple[str, InlineKeyboardMarkup]:
    ov = await user_overview()
    hist = await db_fetchall("SELECT * FROM broadcasts ORDER BY id DESC LIMIT 3")
    text = (
        "👤 <b>UserInfo</b>\n\n"
        f"✅ Subscribed: <b>{ov['audience']}</b>\n"
        f"      🤖 of them started the bot: <b>{ov['reach_sub']}</b>\n"
        f"🔕 Unsubscribed: <b>{ov['unsub']}</b>\n"
        f"👥 Total members: <b>{ov['total']}</b>\n\n"
        "<i>Subscribed = requested to join your channel/group and has not unsubscribed yet.\n"
        "Telegram only lets a bot DM people who started it; others show as unreachable "
        "in broadcast results.\n"
        "Broadcast from 💬 Massage → open a message → 📣 Broadcast.</i>"
    )
    if hist:
        text += "\n\n<b>Recent broadcasts</b>"
        for h in hist:
            text += (
                f"\n#{h['id']} · {fmt_ts(h['created_at'])} — ✅ {h['sent']} · "
                f"🚫 {h['blocked']} · ⚠️ {h['failed']} / {h['total']} ({h['status']})"
            )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [btn(f"✅ Subscribed ({ov['audience']})", "ui:list:sub:0"),
         btn(f"🔕 Unsubscribed ({ov['unsub']})", "ui:list:unsub:0")],
        [btn("🔄 Refresh", "ui:menu"), btn("⬅️ Back", "refresh")],
    ])
    return text, kb


@router.callback_query(F.data == "ui:menu", IsAdmin())
async def cb_ui_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    text, kb = await ui_menu_view()
    await edit(cb, text, kb)
    await cb.answer()


def ui_query(kind: str) -> tuple[str, str]:
    if kind == "sub":
        return "subscribed=1", "first_seen DESC"
    return "subscribed=0", "unsub_at DESC"


@router.callback_query(F.data.startswith("ui:list:"), IsAdmin())
async def cb_ui_list(cb: CallbackQuery):
    _, _, kind, page_s = cb.data.split(":")
    ov = await user_overview()
    total = ov["audience"] if kind == "sub" else ov["unsub"]
    pages = max(1, -(-total // UI_PAGE))
    page = min(max(int(page_s), 0), pages - 1)
    where, order = ui_query(kind)
    rows = await db_fetchall(
        f"SELECT user_id, username, full_name, started, first_seen, unsub_at FROM users "
        f"WHERE {where} ORDER BY {order} LIMIT ? OFFSET ?",
        UI_PAGE, page * UI_PAGE,
    )
    title = "✅ <b>Subscribed members</b>" if kind == "sub" else "🔕 <b>Unsubscribed members</b>"
    if not rows:
        text = f"{title}\n\n<i>Nobody here yet.</i>"
    else:
        lines = [
            f"• {escape(r['full_name'] or 'User')}"
            f"{' (@' + escape(r['username']) + ')' if r['username'] else ''} "
            f"<code>{r['user_id']}</code> — "
            f"{fmt_ts(r['first_seen']) if kind == 'sub' else fmt_ts(r['unsub_at'])}"
            f"{' 🤖' if r['started'] else ''}"
            for r in rows
        ]
        text = f"{title} ({total}) — page {page + 1}/{pages}\n\n" + "\n".join(lines)
        text += "\n\n<i>🤖 = started the bot (can receive DMs)</i>"
    nav = []
    if page > 0:
        nav.append(btn("⬅️ Prev", f"ui:list:{kind}:{page - 1}"))
    if page < pages - 1:
        nav.append(btn("Next ➡️", f"ui:list:{kind}:{page + 1}"))
    kb_rows = ([nav] if nav else []) + [
        [btn("📤 Export CSV", f"ui:export:{kind}")],
        [btn("⬅️ Back", "ui:menu")],
    ]
    await edit(cb, text, InlineKeyboardMarkup(inline_keyboard=kb_rows))
    await cb.answer()


@router.callback_query(F.data.startswith("ui:export:"), IsAdmin())
async def cb_ui_export(cb: CallbackQuery):
    kind = cb.data.split(":")[2]
    where, order = ui_query(kind)
    rows = await db_fetchall(
        f"SELECT user_id, username, full_name, started, first_seen, unsub_at FROM users WHERE {where} ORDER BY {order}"
    )
    if not rows:
        await cb.answer("Nothing to export.", show_alert=True)
        return
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["user_id", "username", "full_name", "started_bot", "first_seen", "unsubscribed_at"])
    for r in rows:
        w.writerow([r["user_id"], r["username"] or "", r["full_name"] or "",
                    "yes" if r["started"] else "no",
                    fmt_ts(r["first_seen"]), fmt_ts(r["unsub_at"])])
    name = "subscribed_users.csv" if kind == "sub" else "unsubscribed_users.csv"
    await cb.message.answer_document(
        BufferedInputFile(buf.getvalue().encode("utf-8-sig"), filename=name),
        caption=f"{'✅' if kind == 'sub' else '🔕'} {len(rows)} member(s)",
    )
    await cb.answer()


# ───────────────────────── Broadcast (from a saved message) ─────────────────────────
_bc_stops: dict[int, asyncio.Event] = {}
_bg_tasks: set = set()

AUDIENCES = {
    "sub": ("✅ Subscribed", "subscribed=1"),
    "unsub": ("🔕 Unsubscribed", "subscribed=0"),
    "both": ("👥 Both", "1=1"),
}


def unsub_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[btn("🔕 Unsubscribe", "bc:unsub")]])


def resub_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[btn("🔔 Subscribe again", "bc:resub")]])


async def audience_counts() -> dict:
    ov = await user_overview()
    return {"sub": ov["audience"], "unsub": ov["unsub"], "both": ov["total"]}


@router.callback_query(F.data.startswith("ms:bc:"), IsAdmin())
async def cb_ms_bc(cb: CallbackQuery):
    """Step 1 – ask WHICH list should receive the broadcast."""
    wid = int(cb.data.split(":")[2])
    w = await wl_get(wid)
    if not w:
        await cb.answer("Message not found.", show_alert=True)
        return
    n = await audience_counts()
    ov = await user_overview()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [btn(f"✅ Subscribed members ({n['sub']})", f"ms:bca:{wid}:sub")],
        [btn(f"🔕 Unsubscribed members ({n['unsub']})", f"ms:bca:{wid}:unsub")],
        [btn(f"👥 Both ({n['both']})", f"ms:bca:{wid}:both")],
        [btn("⬅️ Back", f"ms:open:{wid}")],
    ])
    await edit(
        cb,
        f"📣 <b>Broadcast “{escape(w['name'])}”</b>\n\nWho should receive it?\n\n"
        "<i>Subscribed = requested to join and not unsubscribed. Telegram lets a bot DM only "
        f"people who started it: {ov['reach_sub']} of {ov['audience']} subscribed and "
        f"{ov['unsub_reach']} of {ov['unsub']} unsubscribed members can be reached now – "
        "the rest will show as unreachable. Unsubscribed members asked not to be messaged, "
        "so send to them only if you really need to.</i>",
        kb,
    )
    await cb.answer()


@router.callback_query(F.data.startswith("ms:bca:"), IsAdmin())
async def cb_ms_bca(cb: CallbackQuery, bot: Bot):
    """Step 2 – preview + confirmation."""
    _, _, wid_s, aud = cb.data.split(":")
    wid = int(wid_s)
    w = await wl_get(wid)
    if not w or aud not in AUDIENCES:
        await cb.answer("Message not found.", show_alert=True)
        return
    if not (w["caption"] or "").strip() and not w["media_id"]:
        await cb.answer("This message is empty – add a caption or media first.", show_alert=True)
        return
    n = (await audience_counts())[aud]
    if n == 0:
        await cb.answer("No members in that list.", show_alert=True)
        return
    ov = await user_overview()
    reach = {"sub": ov["reach_sub"], "unsub": ov["unsub_reach"],
             "both": ov["reach_sub"] + ov["unsub_reach"]}[aud]
    mk = resub_kb() if aud == "unsub" else unsub_kb()
    await send_preview(bot, cb.message.chat.id, wid, cb.from_user, reply_markup=mk,
                       header="👁 <b>Broadcast preview</b> — exactly what members receive:")
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        btn(f"✅ Send to {n}", f"ms:bcgo:{wid}:{aud}"), btn("❌ Cancel", f"ms:open:{wid}"),
    ]])
    await cb.message.answer(
        f"📣 <b>{escape(w['name'])}</b>\nAudience: <b>{AUDIENCES[aud][0]}</b> — {n} member(s)\n"
        f"🤖 Can receive DMs now: ~{reach} (others will show as unreachable)\n\nSend now?",
        reply_markup=kb,
    )
    await cb.answer()


@router.callback_query(F.data.startswith("ms:bcgo:"), IsAdmin())
async def cb_ms_bcgo(cb: CallbackQuery, bot: Bot):
    _, _, wid_s, aud = cb.data.split(":")
    wid = int(wid_s)
    if aud not in AUDIENCES or not await wl_get(wid):
        await cb.answer("Message not found.", show_alert=True)
        return
    if _bc_stops:
        await cb.answer("A broadcast is already running.", show_alert=True)
        return
    total = (await audience_counts())[aud]
    bid = await db_insert_id(
        "INSERT INTO broadcasts (created_at, total, status) VALUES (?, ?, 'running')",
        time.time(), total,
    )
    task = asyncio.create_task(run_broadcast(bot, bid, cb.message.chat.id, wid, aud))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)
    await edit(cb, "🚀 Broadcast started…", InlineKeyboardMarkup(inline_keyboard=[]))
    await cb.answer()


async def send_one(bot: Bot, u: dict, w: dict, chat_title: str) -> str:
    uid = u["user_id"]
    name = u.get("full_name") or "friend"
    user = SimpleNamespace(
        id=uid, username=u.get("username"), full_name=name, first_name=name.split()[0],
    )
    mk = unsub_kb() if u["subscribed"] == 1 else resub_kb()
    for _ in range(3):
        try:
            await deliver_welcome(bot, uid, w, user, chat_title, reply_markup=mk)
            return "sent"
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after + 1)
        except TelegramForbiddenError:
            await set_started(uid, False)  # user blocked the bot
            return "blocked"
        except TelegramBadRequest:
            return "failed"
        except Exception:
            log.exception("Broadcast send error")
            return "failed"
    return "failed"


async def run_broadcast(bot: Bot, bid: int, admin_chat: int, wid: int, aud: str):
    stop = _bc_stops[bid] = asyncio.Event()
    stop_kb = InlineKeyboardMarkup(inline_keyboard=[[btn("⏹ Stop", f"bc:stop:{bid}")]])
    sent = failed = blocked = 0
    status = "done"
    try:
        w = await wl_get(wid)
        chat_title = await last_chat_title()
        users = await db_fetchall(
            f"SELECT user_id, username, full_name, subscribed FROM users "
            f"WHERE {AUDIENCES[aud][1]} ORDER BY user_id"
        )
        total = len(users)
        prog = await bot.send_message(admin_chat, f"📣 Broadcasting… 0/{total}", reply_markup=stop_kb)
        last_edit = time.time()
        for i, u in enumerate(users, 1):
            if stop.is_set():
                status = "stopped"
                break
            res = await send_one(bot, u, w, chat_title)
            if res == "sent":
                sent += 1
            elif res == "blocked":
                blocked += 1
            else:
                failed += 1
            await asyncio.sleep(0.05)  # ≈ 20 msgs/sec, under Telegram's limits
            if time.time() - last_edit > 3:
                last_edit = time.time()
                try:
                    await bot.edit_message_text(
                        f"📣 Broadcasting… {i}/{total}\n✅ {sent} · 🚫 {blocked} · ⚠️ {failed}",
                        chat_id=admin_chat, message_id=prog.message_id, reply_markup=stop_kb,
                    )
                except TelegramBadRequest:
                    pass
        await db_execute(
            "UPDATE broadcasts SET sent=?, failed=?, blocked=?, status=? WHERE id=?",
            sent, failed, blocked, status, bid,
        )
        try:
            await bot.edit_message_text(
                f"{'⏹ Broadcast stopped' if status == 'stopped' else '✅ Broadcast finished'}\n\n"
                f"📨 Audience: <b>{AUDIENCES[aud][0]}</b> ({total})\n"
                f"✅ Delivered: <b>{sent}</b>\n🚫 Unreachable (never started / blocked): <b>{blocked}</b>\n"
                f"⚠️ Failed: <b>{failed}</b>",
                chat_id=admin_chat, message_id=prog.message_id,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[btn("⬅️ Back", "ms:menu")]]),
            )
        except TelegramBadRequest:
            pass
    except Exception:
        log.exception("Broadcast crashed")
        await db_execute("UPDATE broadcasts SET status='error' WHERE id=?", bid)
    finally:
        _bc_stops.pop(bid, None)


@router.callback_query(F.data.startswith("bc:stop:"), IsAdmin())
async def cb_bc_stop(cb: CallbackQuery):
    ev = _bc_stops.get(int(cb.data.split(":")[2]))
    if ev:
        ev.set()
    await cb.answer("Stopping…")


# ---- user-side: unsubscribe / subscribe again (open to everyone) ----
@router.callback_query(F.data == "bc:unsub")
async def cb_unsub(cb: CallbackQuery):
    await touch_user(cb.from_user, True)
    await db_execute(
        "UPDATE users SET subscribed=0, unsub_at=? WHERE user_id=?", time.time(), cb.from_user.id
    )
    try:
        await cb.message.edit_reply_markup(reply_markup=resub_kb())
    except TelegramBadRequest:
        pass
    await cb.answer("🔕 You're unsubscribed. You won't receive further broadcasts.", show_alert=True)


@router.callback_query(F.data == "bc:resub")
async def cb_resub(cb: CallbackQuery):
    await touch_user(cb.from_user, True)
    await db_execute("UPDATE users SET subscribed=1, unsub_at=NULL WHERE user_id=?", cb.from_user.id)
    try:
        await cb.message.edit_reply_markup(reply_markup=unsub_kb())
    except TelegramBadRequest:
        pass
    await cb.answer("🔔 Subscribed again.", show_alert=True)


# ───────────────────────── Bot-user detection ─────────────────────────
@router.my_chat_member(F.chat.type == "private")
async def on_my_chat_member(ev: ChatMemberUpdated):
    """User started / blocked / unblocked the bot."""
    status = ev.new_chat_member.status
    if status == "kicked":
        await set_started(ev.from_user.id, False)
    elif status == "member":
        await touch_user(ev.from_user, True)


@router.message(F.chat.type == "private")
async def track_private(message: Message):
    """Any private message to the bot marks that user as a bot user (must stay last)."""
    if message.from_user and not message.from_user.is_bot:
        await touch_user(message.from_user, True)