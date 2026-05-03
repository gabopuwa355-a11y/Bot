# ==========================================================
# FIXED PRODUCTION TELEGRAM MULTI SESSION USERBOT
# Fixes: conversation+handler conflict, CLIENT_STATE tracking,
#        duplicate saves, lock scope, job error handling,
#        handler multi-registration, security, regex, concurrency
# requirements.txt:
#   telethon==1.41.1
#   asyncpg==0.29.0
# ==========================================================

import os
import re
import time
import asyncio
import logging
import asyncpg
import aiohttp

from telethon import TelegramClient, events
from telethon.sessions import StringSession

# ==========================================================
# CONFIG  —  NO hardcoded defaults for secrets
# ==========================================================

def _require_env(key: str) -> str:
    val = os.getenv(key, "").strip()
    if not val:
        raise RuntimeError(f"[config] Missing required env var: {key}")
    return val

API_ID      = int(_require_env("API_ID"))
API_HASH    = _require_env("API_HASH")
SOURCE_BOT  = os.getenv("SOURCE_BOT", "@GmailFarmerBot").strip()
DATABASE_URL = _require_env("DATABASE_URL")

# Collect up to 10 sessions — skip empty ones
SESSION_STRINGS = [
    s for s in (
        os.getenv(f"SESSION{i}", "").strip() for i in range(1, 11)
    ) if s
]

FETCH_TIMEOUT    = 60    # seconds to wait for each bot response step
JOB_DELAY        = 2     # seconds between job-loop polls
CLEANUP_AFTER    = 300   # seconds before an unfinished task is abandoned
RETRY_BUSY_AFTER = 5     # seconds to wait before retrying on busy server
STEP_DELAY       = 1.5   # seconds between button clicks

# ==========================================================
# LOGGING
# ==========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)

# ==========================================================
# GLOBALS
# ==========================================================

# clients[i] corresponds to locks[i]  (1-to-1, never reordered)
clients: list[TelegramClient] = []
locks:   list[asyncio.Lock]   = []

# Round-robin index — protected by a dedicated lock
_client_index     = 0
_client_index_lock = asyncio.Lock()

# Per-client active task:  client_idx -> task_dict | None
# This replaces the old shared CLIENT_STATE keyed by msg_id
# (which broke because follow-up messages have different IDs)
CLIENT_TASK: dict[int, dict | None] = {}

# Global asyncpg connection pool
_pool: asyncpg.Pool | None = None

# ==========================================================
# DATABASE
# ==========================================================

async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
    return _pool


async def init_db():
    pool = await get_pool()
    async with pool.acquire() as conn:

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS registrations(
            id            SERIAL PRIMARY KEY,
            user_id       BIGINT      NOT NULL,
            first_name    TEXT        DEFAULT '',
            last_name     TEXT        DEFAULT '',
            email         TEXT        NOT NULL,
            password      TEXT        DEFAULT '',
            recovery_email TEXT       DEFAULT '',
            task_id       TEXT        UNIQUE,          -- prevents duplicate saves
            msg_id        BIGINT,
            source_msg_id BIGINT      DEFAULT 0,
            client_idx    INTEGER     DEFAULT -1,
            created_at    BIGINT,
            state         TEXT        DEFAULT 'fetched'
        )
        """)

        # Migration — add columns if not present
        for col, defn in [
            ("source_msg_id", "BIGINT DEFAULT 0"),
            ("client_idx",    "INTEGER DEFAULT -1"),
        ]:
            try:
                await conn.execute(
                    f"ALTER TABLE registrations ADD COLUMN IF NOT EXISTS {col} {defn}"
                )
            except Exception:
                pass

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs(
            id          SERIAL PRIMARY KEY,
            user_id     BIGINT,
            job_type    TEXT,
            payload     TEXT    DEFAULT '',
            status      TEXT    DEFAULT 'pending',   -- pending|processing|done|error
            created_at  BIGINT,
            updated_at  BIGINT,
            error       TEXT    DEFAULT ''
        )
        """)

    log.info("[db] PostgreSQL tables ready ✓")

    # Recover jobs stuck in 'processing' from a previous crash.
    # Any job still 'processing' after CLEANUP_AFTER seconds is re-queued.
    async with pool.acquire() as conn:
        recovered = await conn.fetchval("""
        UPDATE jobs
        SET status='pending', error='recovered after crash', updated_at=$1
        WHERE status='processing'
          AND updated_at < $2
        RETURNING id
        """, now(), now() - CLEANUP_AFTER)
    if recovered:
        log.warning(f"[db] Recovered {recovered} stuck processing job(s)")

# ==========================================================
# HELPERS
# ==========================================================

def now() -> int:
    return int(time.time())


async def get_next_client_idx() -> tuple[int | None, str]:
    """
    Return (client_idx, reason).
    - (idx, "ok")       — free client found
    - (None, "no_clients") — SESSION list is empty
    - (None, "all_busy")   — every client is currently locked
    Tries round-robin starting from last position.
    """
    global _client_index
    async with _client_index_lock:
        if not clients:
            return None, "no_clients"
        n = len(clients)
        for i in range(n):
            idx = (_client_index + i) % n
            if not locks[idx].locked():
                _client_index = (idx + 1) % n
                return idx, "ok"
        return None, "all_busy"

# ==========================================================
# PARSE  —  robust multi-pattern recovery extraction
# ==========================================================

def clean_value(v: str) -> str:
    if not v:
        return ""
    v = v.strip().strip("'\"")
    v = re.sub(r"\s+", " ", v)
    return v.strip()


_EMAIL_RE = r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}"

def parse_task(text: str) -> dict:
    # Normalise smart-quotes and backticks
    text = text.replace("`", "").replace("\u2019", "'").replace("\u2018", "'")

    def _find(pattern):
        m = re.search(pattern, text, re.I | re.S)
        return clean_value(m.group(1)) if m else ""

    first    = _find(r"First\s*name\s*[:\-]?\s*['\"]?(.+?)['\"]?\s*(?:\n|$)")
    last     = _find(r"Last\s*name\s*[:\-]?\s*['\"]?(.+?)['\"]?\s*(?:\n|$)")
    email    = _find(rf"Email\s*[:\-]?\s*['\"]?({_EMAIL_RE})['\"]?")
    password = _find(r"Password\s*[:\-]?\s*['\"]?(.+?)['\"]?\s*(?:\n|$)")

    # Recovery email — try multiple patterns in priority order
    recovery = ""
    for pat in [
        rf"Recovery\s*email\s*[:\-]\s*\n?\s*({_EMAIL_RE})",
        rf"add\s*Recovery\s*email\s*\n?\s*({_EMAIL_RE})",
        rf"recovery\s*[:\-]\s*({_EMAIL_RE})",
        rf"({_EMAIL_RE})\s*(?:is your recovery|as recovery)",
    ]:
        m = re.search(pat, text, re.I | re.S)
        if m:
            recovery = clean_value(m.group(1))
            break

    return {
        "first_name":     first,
        "last_name":      last,
        "email":          email,
        "password":       password,
        "recovery_email": recovery or "Not Provided",
    }

# ==========================================================
# BUTTON CLICKER  —  exact-first, then partial
# ==========================================================

# Exact button texts the bot uses (in preferred click order)
BUTTON_PRIORITY = [
    "done ✅",
    "done",
    "confirm again ✅",
    "confirm again",
    "confirm ✅",
    "confirm",
    "complete",
    "next",
    "continue",
    "start",
]

async def click_best_button(msg) -> bool:
    """
    Click the highest-priority button found on msg.
    Returns True if a button was clicked.
    """
    if not msg or not msg.buttons:
        return False

    available = {}
    for row in msg.buttons:
        for btn in row:
            txt = (btn.text or "").strip()
            available[txt.lower()] = btn.text  # lower -> original

    for priority_key in BUTTON_PRIORITY:
        if priority_key in available:
            original_text = available[priority_key]
            try:
                await msg.click(text=original_text)
                log.info(f"[click] Clicked '{original_text}' on msg {msg.id}")
                return True
            except Exception as e:
                log.error(f"[click] Failed to click '{original_text}': {e}")
                return False

    log.warning(f"[click] No known button found. Available: {list(available.keys())}")
    return False

# ==========================================================
# SAVE  —  INSERT with conflict guard
# ==========================================================

async def save_registration(user_id: int, msg_id: int, data: dict,
                             client_idx: int = -1, source_msg_id: int = 0):
    pool = await get_pool()
    task_id = f"{user_id}_{msg_id}_{now()}"
    async with pool.acquire() as conn:
        try:
            await conn.execute("""
            INSERT INTO registrations(
                user_id, first_name, last_name, email,
                password, recovery_email, task_id,
                msg_id, source_msg_id, client_idx,
                created_at, state
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'fetched')
            ON CONFLICT (task_id) DO NOTHING
            """,
                user_id,
                data["first_name"],
                data["last_name"],
                data["email"],
                data["password"],
                data["recovery_email"],
                task_id,
                msg_id,
                source_msg_id,
                client_idx,
                now(),
            )
            log.info(f"[db] Saved registration for user {user_id} email={data['email']}")
        except Exception as e:
            log.error(f"[db] save_registration error: {e}")
            raise

# ==========================================================
# CORE FETCH  —  pure conversation-based, no event handler
# ==========================================================
#
# Architecture decision:
#   We do NOT mix client.conversation() with add_event_handler().
#   Telethon's conversation() intercepts incoming messages internally,
#   so event handlers never see those messages.
#
#   Instead we drive the entire registration flow here, step by step,
#   using conv.get_response() / conv.get_edit() with a timeout.
#   This is reliable and avoids the CLIENT_STATE msg_id mismatch bug.
#

async def _do_fetch(client: TelegramClient, client_idx: int, user_id: int):
    """
    Drive one full registration session with SOURCE_BOT.
    Runs entirely inside a single conversation context.
    """
    log.info(f"[fetch] client={client_idx} user={user_id} starting")

    try:
        async with client.conversation(SOURCE_BOT, timeout=FETCH_TIMEOUT, exclusive=True) as conv:

            # Step 1 — trigger registration
            await conv.send_message("➕ Register a new Gmail")
            msg = await conv.get_response()
            log.info(f"[fetch] Step1 msg: {(msg.text or '')[:80]}")

            # Step 2 — click through bot prompts until we hit recovery email step
            for step in range(20):  # safety cap
                text_lower = (msg.text or "").lower()

                # ── Terminal: recovery email step ──
                if "recovery email" in text_lower or "add recovery" in text_lower:
                    data = parse_task(msg.text)
                    if data["email"]:
                        await save_registration(
                            user_id, msg.id, data,
                            client_idx=client_idx,
                            source_msg_id=msg.id,
                        )
                        log.info(f"[fetch] Done — saved email={data['email']}")
                    else:
                        log.warning(f"[fetch] Recovery step reached but email not parsed. Text:\n{msg.text}")
                    return

                # ── Server busy ──
                if "server busy" in text_lower or "5 sec" in text_lower:
                    log.warning(f"[fetch] Server busy, retrying in {RETRY_BUSY_AFTER}s")
                    await asyncio.sleep(RETRY_BUSY_AFTER)
                    await conv.send_message("➕ Register a new Gmail")
                    msg = await conv.get_response()
                    continue

                # ── Try clicking a button ──
                clicked = await click_best_button(msg)
                await asyncio.sleep(STEP_DELAY)

                # ── Wait for next message (edit or new) ──
                try:
                    # Some bots edit the same message; try get_edit first
                    msg = await conv.get_edit(timeout=10)
                except asyncio.TimeoutError:
                    try:
                        msg = await conv.get_response(timeout=FETCH_TIMEOUT)
                    except asyncio.TimeoutError:
                        log.warning(f"[fetch] Timeout waiting for response at step {step}")
                        return

                log.info(f"[fetch] Step{step+2} msg: {(msg.text or '')[:80]}")

            log.warning(f"[fetch] Safety cap reached for user={user_id}")

    except asyncio.TimeoutError:
        log.error(f"[fetch] Conversation timed out for user={user_id}")
    except Exception as e:
        log.error(f"[fetch] Error for user={user_id}: {e}")


async def fetch_task(user_id: int) -> tuple[bool, str]:
    """
    Pick a free client and run _do_fetch on it (awaited inline).
    Returns (True, "ok") if dispatched, (False, reason) if not.
    """
    idx, reason = await get_next_client_idx()
    if idx is None:
        log.info(f"[fetch] user={user_id} skipped — {reason}")
        return False, reason

    log.info(f"[fetch] user={user_id} → client={idx}")
    async with locks[idx]:
        await _do_fetch(clients[idx], idx, user_id)
    return True, "ok"

# ==========================================================
# JOB LOOP  —  with proper error/retry handling
# ==========================================================

# Tracks which jobs have been dispatched as background tasks
# job_id -> asyncio.Task, so we can check completion and mark done/error
_active_tasks: dict[int, asyncio.Task] = {}


async def _run_job(job: dict):
    """Run one job and mark done/error in DB when complete."""
    pool = await get_pool()
    job_id = job["id"]
    try:
        if job["job_type"] == "fetch":
            dispatched, reason = await fetch_task(job["user_id"])
            if not dispatched:
                # All clients busy — requeue so loop retries
                async with pool.acquire() as conn:
                    await conn.execute("""
                    UPDATE jobs SET status='pending', updated_at=$1, error=$2
                    WHERE id=$3
                    """, now(), f"requeued: {reason}", job_id)
                log.info(f"[jobs] job={job_id} requeued — {reason}")
                return
        else:
            log.warning(f"[jobs] Unknown job_type: {job['job_type']}")

        # fetch_task fully awaited above — mark done
        async with pool.acquire() as conn:
            await conn.execute("""
            UPDATE jobs SET status='done', updated_at=$1
            WHERE id=$2
            """, now(), job_id)

    except Exception as e:
        log.error(f"[jobs] job={job_id} error: {e}")
        try:
            async with pool.acquire() as conn:
                await conn.execute("""
                UPDATE jobs SET status='error', updated_at=$1, error=$2
                WHERE id=$3
                """, now(), str(e), job_id)
        except Exception as db_err:
            log.error(f"[jobs] Could not mark error for job={job_id}: {db_err}")
    finally:
        _active_tasks.pop(job_id, None)


async def job_loop():
    """
    Dispatch pending jobs to free clients in parallel.
    - Does NOT await job completion — each job runs as its own asyncio Task.
    - One job = one client (enforced by lock in fetch_task/_run).
    - If all clients busy, loop waits JOB_DELAY then retries.
    - Completed/errored tasks clean themselves up.
    """
    while True:
        try:
            pool = await get_pool()

            # How many free clients do we have right now?
            free_slots = sum(1 for l in locks if not l.locked())

            if free_slots == 0:
                await asyncio.sleep(JOB_DELAY)
                continue

            # Fetch as many pending jobs as we have free clients
            async with pool.acquire() as conn:
                jobs = await conn.fetch(f"""
                SELECT * FROM jobs
                WHERE status='pending'
                ORDER BY id ASC
                LIMIT {free_slots}
                FOR UPDATE SKIP LOCKED
                """)

                if not jobs:
                    await asyncio.sleep(JOB_DELAY)
                    continue

                # Mark all as processing atomically
                job_ids = [j["id"] for j in jobs]
                await conn.execute("""
                UPDATE jobs SET status='processing', updated_at=$1
                WHERE id = ANY($2::int[])
                """, now(), job_ids)

            # Dispatch each job as independent task — non-blocking
            for job in jobs:
                if job["id"] not in _active_tasks:
                    task = asyncio.create_task(_run_job(dict(job)))
                    _active_tasks[job["id"]] = task
                    log.info(f"[jobs] Dispatched job={job['id']} user={job['user_id']}")

            # Small yield so tasks can actually start
            await asyncio.sleep(0.5)

        except Exception as e:
            log.error(f"[jobs] Loop error: {e}")
            await asyncio.sleep(2)

# ==========================================================
# CLIENTS
# ==========================================================

async def start_clients():
    for i, session in enumerate(SESSION_STRINGS):
        try:
            client = TelegramClient(
                StringSession(session),
                API_ID,
                API_HASH,
            )
            await client.connect()

            if not await client.is_user_authorized():
                log.warning(f"[client] Session {i} unauthorized — skipping")
                await client.disconnect()
                continue

            # NOTE: No event_handler registered here.
            # All message handling is done inside conversation() in _do_fetch.

            clients.append(client)
            locks.append(asyncio.Lock())
            CLIENT_TASK[len(clients) - 1] = None

            log.info(f"[client] Session {i} ready (client index {len(clients)-1})")

        except Exception as e:
            log.error(f"[client] Session {i} failed to start: {e}")

# ==========================================================
# MAIN
# ==========================================================

_health_start_time = int(time.time())

async def _health_server():
    """Minimal HTTP health check on HEALTH_PORT (default 9000)."""
    from aiohttp import web
    port = int(os.getenv("HEALTH_PORT", "9000"))

    async def health(request):
        free = sum(1 for l in locks if not l.locked())
        busy = len(locks) - free
        pool = await get_pool()
        pending = await pool.fetchval("SELECT COUNT(*) FROM jobs WHERE status='pending'") or 0
        processing = await pool.fetchval("SELECT COUNT(*) FROM jobs WHERE status='processing'") or 0
        return web.json_response({
            "status": "ok",
            "uptime_sec": int(time.time()) - _health_start_time,
            "clients_total": len(clients),
            "clients_free": free,
            "clients_busy": busy,
            "jobs_pending": int(pending),
            "jobs_processing": int(processing),
        })

    app = web.Application()
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info(f"[health] Listening on :{port}/health")


async def main():
    # Validate DB before anything else
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")

    await init_db()
    await start_clients()

    if not clients:
        log.error("[main] No clients started — exiting")
        return

    log.info(f"[main] {len(clients)} client(s) active")

    asyncio.create_task(job_loop())
    asyncio.create_task(_health_server())

    log.info("[main] System started successfully ✓")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
