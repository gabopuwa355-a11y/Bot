# ==========================================================
# ADMIN BOT
# - Login with username/password
# - View registrations (paginated, with recovery email)
# - Export PDF of all registrations
# - "Done" button: clicks "done" on source bot via the saved client session
#   then waits 15s for "how to logout" text → shows ✅ / ❌
# - "Back" button: go back to list
#
# ENV VARS:
#   ADMIN_BOT_TOKEN   — this bot's token
#   DATABASE_URL      — same postgres as userbot
#   API_ID / API_HASH — same Telegram API creds as userbot
#   SESSION1..10      — same session strings as userbot
#   ADMIN_USERNAME    — login username  (default: sakib55t)
#   ADMIN_PASSWORD    — login password  (default: 123asd@#₹)
#   ADMIN_EMAIL       — login email     (default: sakib55t@xyzbaazar.com)
#   SOURCE_BOT        — @username of source bot
# ==========================================================

import os
import io
import time
import asyncio
import logging
import asyncpg
import aiohttp

from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle,
    Paragraph, Spacer,
)
from reportlab.lib.pagesizes import landscape, A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet

# ==========================================================
# CONFIG
# ==========================================================

def _env(key, default=""):
    return os.getenv(key, default).strip()

ADMIN_BOT_TOKEN = _env("ADMIN_BOT_TOKEN")
DATABASE_URL    = _env("DATABASE_URL")
API_ID          = int(_env("API_ID", "0"))
API_HASH        = _env("API_HASH")
SOURCE_BOT      = _env("SOURCE_BOT", "@GmailFarmerBot")

ADMIN_USERNAME  = _env("ADMIN_USERNAME", "sakib55t")
ADMIN_PASSWORD  = _env("ADMIN_PASSWORD", "123asd@#₹")
ADMIN_EMAIL     = _env("ADMIN_EMAIL",    "sakib55t@xyzbaazar.com")

# Persistent admin whitelist from env: comma-separated Telegram user IDs
# e.g. ADMIN_TELEGRAM_IDS=123456789,987654321
# These IDs never need to re-login after bot restart.
_PERSISTENT_ADMIN_IDS: set[int] = {
    int(x.strip())
    for x in _env("ADMIN_TELEGRAM_IDS", "").split(",")
    if x.strip().isdigit()
}

SESSION_STRINGS = [
    s for s in (_env(f"SESSION{i}") for i in range(1, 11)) if s
]

PAGE_SIZE        = 5     # registrations per page
LOGOUT_WAIT_SECS = 15    # wait for "how to logout" text after clicking done
DONE_TIMEOUT     = 20    # timeout for source bot response after click

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)

# ==========================================================
# SHARED STATE
# ==========================================================

_pool: asyncpg.Pool | None = None
_clients: list[TelegramClient] = []        # indexed same as SESSION_STRINGS
_authed_users: set[int] = set()            # telegram user_ids that have logged in

# Per-admin pagination state  admin_tg_id -> offset
_page_state: dict[int, int] = {}

# Brute force protection: user_id -> (fail_count, banned_until_timestamp)
_login_attempts: dict[int, list] = {}  # [fail_count, banned_until]
MAX_LOGIN_FAILS = 5
LOGIN_BAN_SECS  = 600  # 10 minutes

# ==========================================================
# DATABASE
# ==========================================================

async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    return _pool


async def db_get_registrations(offset: int = 0, limit: int = PAGE_SIZE):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
        SELECT id, user_id, first_name, email, password, recovery_email,
               client_idx, source_msg_id, created_at, state
        FROM registrations
        ORDER BY id DESC
        LIMIT $1 OFFSET $2
        """, limit, offset)
        total = await conn.fetchval("SELECT COUNT(*) FROM registrations")
    return rows, int(total)


async def db_get_reg_by_id(reg_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchrow("""
        SELECT id, user_id, first_name, email, password, recovery_email,
               client_idx, source_msg_id, created_at, state
        FROM registrations WHERE id=$1
        """, reg_id)


async def db_set_state(reg_id: int, state: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE registrations SET state=$1 WHERE id=$2",
            state, reg_id
        )

# ==========================================================
# PDF GENERATOR
# ==========================================================

def generate_pdf(rows) -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=landscape(A4),
        leftMargin=20, rightMargin=20,
        topMargin=30, bottomMargin=30,
    )
    styles = getSampleStyleSheet()

    small = styles["Normal"].clone("sm")
    small.fontSize = 7
    small.leading  = 9
    small.wordWrap = "CJK"

    hdr = styles["Normal"].clone("hdr")
    hdr.fontSize  = 7
    hdr.leading   = 9
    hdr.fontName  = "Helvetica-Bold"

    HEADERS = ["#", "Email", "Password", "Recovery Email", "State", "Date"]
    table_data = [[Paragraph(h, hdr) for h in HEADERS]]

    for i, r in enumerate(rows, 1):
        try:
            date_str = time.strftime("%Y-%m-%d %H:%M", time.localtime(int(r["created_at"])))
        except Exception:
            date_str = "-"
        table_data.append([
            Paragraph(str(i), small),
            Paragraph(r["email"] or "-", small),
            Paragraph(r["password"] or "-", small),
            Paragraph(r["recovery_email"] or "-", small),
            Paragraph(r["state"] or "-", small),
            Paragraph(date_str, small),
        ])

    col_widths = [25, 175, 130, 175, 60, 85]
    tbl = Table(table_data, colWidths=col_widths, repeatRows=1)
    tbl.setStyle(TableStyle([
        ("BACKGROUND",     (0,0), (-1,0),  colors.HexColor("#2d6cdf")),
        ("TEXTCOLOR",      (0,0), (-1,0),  colors.white),
        ("FONTNAME",       (0,0), (-1,0),  "Helvetica-Bold"),
        ("FONTSIZE",       (0,0), (-1,0),  7),
        ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#f0f4ff")]),
        ("GRID",           (0,0), (-1,-1), 0.4, colors.HexColor("#aaaaaa")),
        ("VALIGN",         (0,0), (-1,-1), "TOP"),
        ("LEFTPADDING",    (0,0), (-1,-1), 4),
        ("RIGHTPADDING",   (0,0), (-1,-1), 4),
        ("TOPPADDING",     (0,0), (-1,-1), 3),
        ("BOTTOMPADDING",  (0,0), (-1,-1), 3),
    ]))

    title = Paragraph(
        f"<b>Registrations Export</b> — {len(rows)} record(s)",
        styles["Normal"]
    )
    doc.build([title, Spacer(1, 8), tbl])
    buf.seek(0)
    return buf.read()

# ==========================================================
# SOURCE BOT INTERACTION  —  click "done" on original message
# ==========================================================

async def click_done_on_source(client_idx: int, source_msg_id: int) -> str:
    """
    Using the saved Telethon client:
    1. Find the message on SOURCE_BOT by source_msg_id
    2. Click the "done" button
    3. Wait up to LOGOUT_WAIT_SECS for a message containing "how to logout"
    4. Return "success" or "failed"
    """
    if client_idx < 0 or client_idx >= len(_clients):
        log.error(f"[click_done] Invalid client_idx={client_idx}")
        return "failed"

    client = _clients[client_idx]

    try:
        # Get message from SOURCE_BOT
        msg = await client.get_messages(SOURCE_BOT, ids=source_msg_id)
        if not msg:
            log.warning(f"[click_done] Message {source_msg_id} not found on {SOURCE_BOT}")
            return "failed"

        # Click "done" button
        clicked = False
        if msg.buttons:
            for row in msg.buttons:
                for btn in row:
                    if "done" in (btn.text or "").lower():
                        await btn.click()
                        clicked = True
                        log.info(f"[click_done] Clicked '{btn.text}' on msg {source_msg_id}")
                        break
                if clicked:
                    break

        if not clicked:
            log.warning(f"[click_done] No 'done' button found on msg {source_msg_id}")
            return "failed"

        # Wait up to LOGOUT_WAIT_SECS for "how to logout" text
        deadline = time.time() + LOGOUT_WAIT_SECS
        async with client.conversation(SOURCE_BOT, timeout=LOGOUT_WAIT_SECS + 5) as conv:
            while time.time() < deadline:
                try:
                    response = await conv.get_response(timeout=deadline - time.time())
                    text_lower = (response.text or "").lower()
                    log.info(f"[click_done] Got response: {text_lower[:60]}")
                    if "how to logout" in text_lower:
                        return "success"
                except asyncio.TimeoutError:
                    break

        return "failed"

    except Exception as e:
        log.error(f"[click_done] Error: {e}")
        return "failed"

# ==========================================================
# MESSAGE BUILDER
# ==========================================================

def _fmt_time(ts) -> str:
    try:
        return time.strftime("%Y-%m-%d %H:%M", time.localtime(int(ts)))
    except Exception:
        return "-"


def build_reg_message(r, idx: int, total: int, offset: int = 0) -> tuple[str, InlineKeyboardMarkup]:
    """Build text + keyboard for one registration card. idx = absolute position (1-based)."""
    state_icon = {"fetched": "🔵", "done": "✅", "failed": "❌"}.get(r["state"], "⚪")

    text = (
        f"📋 *Registration {idx}/{total}*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 Name: `{r['first_name'] or '-'}`\n"
        f"📧 Email: `{r['email'] or '-'}`\n"
        f"🔑 Password: `{r['password'] or '-'}`\n"
        f"📩 Recovery Email: `{r['recovery_email'] or '-'}`\n"
        f"📌 State: {state_icon} `{r['state']}`\n"
        f"🕐 Date: `{_fmt_time(r['created_at'])}`"
    )

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Done", callback_data=f"done:{r['id']}"),
            InlineKeyboardButton("🔙 Back", callback_data="back"),
        ]
    ])
    return text, keyboard


def build_list_keyboard(offset: int, total: int) -> InlineKeyboardMarkup:
    """Paginated list keyboard."""
    page  = offset // PAGE_SIZE + 1
    pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    nav = []
    if offset > 0:
        nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"list:{offset - PAGE_SIZE}"))
    nav.append(InlineKeyboardButton(f"📄 {page}/{pages}", callback_data="noop"))
    if offset + PAGE_SIZE < total:
        nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"list:{offset + PAGE_SIZE}"))

    rows_btns = [nav] if nav else []
    rows_btns.append([InlineKeyboardButton("📥 Export PDF", callback_data="pdf")])
    return InlineKeyboardMarkup(rows_btns)

# ==========================================================
# AUTH CHECK
# ==========================================================

def is_authed(user_id: int) -> bool:
    return user_id in _authed_users or user_id in _PERSISTENT_ADMIN_IDS

# ==========================================================
# HANDLERS
# ==========================================================

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🔐 *Admin Login*\n\nSend your credentials in this format:\n"
        "`username|password|email`\n\n"
        "Example: `sakib55t|123asd@#₹|sakib55t@xyzbaazar.com`",
        parse_mode="Markdown"
    )


async def handle_login(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = (update.message.text or "").strip()

    # Already authed — handle as command
    if is_authed(user_id):
        await handle_authed_message(update, ctx)
        return

    # Brute force check
    attempt = _login_attempts.get(user_id, [0, 0])
    if attempt[1] > time.time():
        remaining = int(attempt[1] - time.time())
        await update.message.reply_text(
            f"🚫 Too many failed attempts. Try again in {remaining}s."
        )
        return

    # Try login
    parts = [p.strip() for p in text.split("|")]
    if len(parts) != 3:
        await update.message.reply_text(
            "❌ Format: `username|password|email`", parse_mode="Markdown"
        )
        return

    username, password, email = parts
    if (username == ADMIN_USERNAME and
        password == ADMIN_PASSWORD and
        email    == ADMIN_EMAIL):
        _login_attempts.pop(user_id, None)  # reset on success
        _authed_users.add(user_id)
        await update.message.reply_text(
            "✅ *Login successful!*\n\nCommands:\n"
            "/list — View registrations\n"
            "/pdf  — Export PDF\n"
            "/logout — Logout",
            parse_mode="Markdown"
        )
    else:
        attempt[0] += 1
        if attempt[0] >= MAX_LOGIN_FAILS:
            attempt[1] = time.time() + LOGIN_BAN_SECS
            _login_attempts[user_id] = attempt
            await update.message.reply_text(
                f"🚫 {MAX_LOGIN_FAILS} failed attempts. Banned for {LOGIN_BAN_SECS // 60} minutes."
            )
        else:
            _login_attempts[user_id] = attempt
            remaining_tries = MAX_LOGIN_FAILS - attempt[0]
            await update.message.reply_text(
                f"❌ Wrong credentials. {remaining_tries} attempt(s) left."
            )


async def handle_authed_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Use /list to view registrations or /pdf to export."
    )


async def cmd_logout(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    _authed_users.discard(user_id)
    await update.message.reply_text("👋 Logged out.")


async def cmd_list(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_authed(user_id):
        await update.message.reply_text("🔐 Please login first with /start")
        return

    offset = 0
    _page_state[user_id] = offset
    await send_list(update.message, offset)


async def send_list(message_or_query, offset: int):
    """Send paginated registration list."""
    rows, total = await db_get_registrations(offset=offset, limit=PAGE_SIZE)

    if total == 0:
        text = "📭 No registrations found."
        kb   = None
    else:
        lines = [f"📋 *Registrations* — {total} total\n"]
        for i, r in enumerate(rows, start=offset + 1):
            state_icon = {"fetched": "🔵", "done": "✅", "failed": "❌"}.get(r["state"], "⚪")
            lines.append(
                f"{i}. {state_icon} `{r['email']}`\n"
                f"    📩 `{r['recovery_email'] or '-'}`\n"
                f"    /reg_{r['id']}"
            )
        text = "\n".join(lines)
        kb   = build_list_keyboard(offset, total)

    if hasattr(message_or_query, "reply_text"):
        await message_or_query.reply_text(text, parse_mode="Markdown", reply_markup=kb)
    else:
        await message_or_query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)


async def cmd_reg_detail(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /reg_<id> commands."""
    user_id = update.effective_user.id
    if not is_authed(user_id):
        await update.message.reply_text("🔐 Please login first.")
        return

    text = update.message.text or ""
    try:
        reg_id = int(text.replace("/reg_", "").strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid registration ID.")
        return

    r = await db_get_reg_by_id(reg_id)
    if not r:
        await update.message.reply_text("❌ Registration not found.")
        return

    _, total = await db_get_registrations(offset=0, limit=1)
    # Find actual position of this record in DESC order
    pool = await get_pool()
    async with pool.acquire() as conn:
        position = await conn.fetchval(
            "SELECT COUNT(*) FROM registrations WHERE id >= $1", reg_id
        )
    msg_text, kb = build_reg_message(r, idx=int(position or 1), total=total, offset=0)
    await update.message.reply_text(msg_text, parse_mode="Markdown", reply_markup=kb)


async def cmd_pdf(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_authed(user_id):
        await update.message.reply_text("🔐 Please login first.")
        return

    await update.message.reply_text("⏳ Generating PDF...")
    rows, total = await db_get_registrations(offset=0, limit=10000)

    if total == 0:
        await update.message.reply_text("📭 No data to export.")
        return

    pdf_bytes = generate_pdf(rows)
    bio = io.BytesIO(pdf_bytes)
    bio.name = "registrations.pdf"
    bio.seek(0)

    await update.message.reply_document(
        document=bio,
        filename=f"registrations_{time.strftime('%Y%m%d_%H%M')}.pdf",
        caption=f"📄 Registrations export — {total} records"
    )

# ==========================================================
# CALLBACK QUERY HANDLER
# ==========================================================

async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q       = update.callback_query
    user_id = q.from_user.id
    data    = q.data or ""
    await q.answer()

    if not is_authed(user_id):
        await q.message.reply_text("🔐 Please login first with /start")
        return

    # ── noop ──
    if data == "noop":
        return

    # ── List page ──
    if data.startswith("list:"):
        offset = int(data.split(":")[1])
        _page_state[user_id] = offset
        await send_list(q, offset)
        return

    # ── Back ──
    if data == "back":
        offset = _page_state.get(user_id, 0)
        await send_list(q, offset)
        return

    # ── PDF export ──
    if data == "pdf":
        await q.message.reply_text("⏳ Generating PDF...")
        rows, total = await db_get_registrations(offset=0, limit=10000)
        if total == 0:
            await q.message.reply_text("📭 No data.")
            return
        pdf_bytes = generate_pdf(rows)
        bio = io.BytesIO(pdf_bytes)
        bio.seek(0)
        await q.message.reply_document(
            document=bio,
            filename=f"registrations_{time.strftime('%Y%m%d_%H%M')}.pdf",
            caption=f"📄 {total} records"
        )
        return

    # ── Done button ──
    if data.startswith("done:"):
        reg_id = int(data.split(":")[1])
        r = await db_get_reg_by_id(reg_id)
        if not r:
            await q.message.reply_text("❌ Registration not found.")
            return

        await q.message.reply_text(
            f"⏳ Clicking *done* on source bot...\n"
            f"📩 Recovery: `{r['recovery_email']}`\n"
            f"Waiting {LOGOUT_WAIT_SECS}s for confirmation...",
            parse_mode="Markdown"
        )

        result = await click_done_on_source(
            client_idx=int(r["client_idx"] or -1),
            source_msg_id=int(r["source_msg_id"] or 0),
        )

        await db_set_state(reg_id, result)

        if result == "success":
            status_line = "✅ *Success!* `how to logout` text found."
        else:
            status_line = "❌ *Failed.* `how to logout` text not found within 15s."

        await q.message.reply_text(
            f"📩 Recovery Email: `{r['recovery_email']}`\n{status_line}",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="back")
            ]])
        )
        return

# ==========================================================
# TELETHON CLIENTS (read-only — just to perform clicks)
# ==========================================================

async def start_telethon_clients():
    for i, session in enumerate(SESSION_STRINGS):
        try:
            client = TelegramClient(StringSession(session), API_ID, API_HASH)
            await client.connect()
            if await client.is_user_authorized():
                _clients.append(client)
                log.info(f"[telethon] Client {i} ready")
            else:
                log.warning(f"[telethon] Client {i} unauthorized")
                await client.disconnect()
        except Exception as e:
            log.error(f"[telethon] Client {i} error: {e}")

# ==========================================================
# MAIN
# ==========================================================

_admin_health_start = int(time.time())

async def _health_server():
    """Minimal HTTP health check on HEALTH_PORT (default 9001)."""
    from aiohttp import web
    port = int(os.getenv("HEALTH_PORT", "9001"))

    async def health(request):
        pool = await get_pool()
        total = await pool.fetchval("SELECT COUNT(*) FROM registrations") or 0
        return web.json_response({
            "status": "ok",
            "uptime_sec": int(time.time()) - _admin_health_start,
            "telethon_clients": len(_clients),
            "authed_admins": len(_authed_users),
            "total_registrations": int(total),
        })

    app = web.Application()
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info(f"[health] Admin bot listening on :{port}/health")


async def main():
    if not ADMIN_BOT_TOKEN:
        raise RuntimeError("ADMIN_BOT_TOKEN not set")
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set")

    await get_pool()
    log.info("[db] Connected ✓")

    await start_telethon_clients()
    log.info(f"[telethon] {len(_clients)} client(s) ready")

    app = Application.builder().token(ADMIN_BOT_TOKEN).build()

    # Commands
    app.add_handler(CommandHandler("start",  cmd_start))
    app.add_handler(CommandHandler("list",   cmd_list))
    app.add_handler(CommandHandler("pdf",    cmd_pdf))
    app.add_handler(CommandHandler("logout", cmd_logout))

    # /reg_<id> pattern
    app.add_handler(MessageHandler(
        filters.TEXT & filters.Regex(r"^/reg_\d+$"),
        cmd_reg_detail
    ))

    # Callback buttons
    app.add_handler(CallbackQueryHandler(handle_callback))

    # All other messages (login + post-auth)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_login))

    asyncio.ensure_future(_health_server())

    log.info("[admin_bot] Starting polling...")
    await app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    asyncio.run(main())
