# -*- coding: utf-8 -*-
import re
import time
import sqlite3
import os
import socket
import json
import hmac
import hashlib
from datetime import datetime
import datetime
import random
import string
import asyncio
import smtplib
import imaplib
import email as email_pkg
from email.message import EmailMessage

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    WebAppInfo,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

from reportlab.lib.pagesizes import A4, A2, landscape
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet

# Optional DNS lookup (for email domain MX checks)
try:
    import dns.resolver as dns_resolver  # pip install dnspython
except Exception:
    dns_resolver = None

# =========================
# CONFIG (NO .env)
# =========================
BOT_TOKEN = "8182599906:AAGMU6PRt9_VB9-rcPEsr2jIZxLdlwBbirc"
BOT_USERNAME = os.getenv("BOT_USERNAME", "").strip("@")  # optional
ADMIN_ID = 7988263992  # only admin access
PIN_CHAT_ID = None  # set to a group/channel id (bot must be admin) to pin messages there
# Channels gate (user must join to use bot)
REQUIRED_CHANNELS = [
    ("@gmail_earning", "https://t.me/gmail_earning"),
    ("@gmailprojectnews", "https://t.me/gmailprojectnews"),
]

FINGERPRINT_PUBLIC_BASE_URL = ""  # device-verify webapp disabled; keep empty to avoid NameError

# =========================
# GMAIL SMTP + IMAP (Deliverability check via real send + bounce)
# NOTE: This does NOT "probe" SMTP for existence; it sends a tiny test email and checks for bounce.
# You must use a Gmail account with 2FA + App Password, and enable IMAP in Gmail settings.
ENABLE_SMTP_BOUNCE_CHECK = True
SMTP_GMAIL_USER = "aadiltyagi459@gmail.com"
SMTP_GMAIL_APP_PASSWORD = "kawl rdaz jawr nhfp"
BOUNCE_POLL_SECONDS = 4  # max wait time (fast mode)
BOUNCE_POLL_INTERVALS = (1, 1, 2)  # total <= 4 sec


# Tutorial videos (Telegram file_id) — works on Railway/Termux without local files
VIDEO_FILE_ID_CREATE = "BAACAgUAAxkBAAIBImmQnFR75KNF4qzxT4uiN3bK9XCBAAJLGwACbBiJVGvSCPjuDQvxOgQ"
VIDEO_FILE_ID_LOGOUT = "BAACAgUAAxkBAAIBhGmSCMPRmt0lpPxNI8FQd-S21kefAAKFHAACR9jJV93FvyDND0OeOgQ"

# (Legacy path variables kept empty for compatibility; not used)
VIDEO_CREATE_PATHS = []
VIDEO_LOGOUT_PATHS = []
VIDEO_FILE_ID_CACHE = {"create": None, "logout": None}
# Provisional HOLD credit added immediately when user confirms (reverted on admin reject)
PRE_CREDIT_AMOUNT = 10.0

# Business rules
MAX_PER_MIN = 3
ACTION_TIMEOUT_HOURS = 20
HOLD_TO_MAIN_AFTER_DAYS = 1

# CONFIRM AGAIN cooldown (prevents spam clicks without action)
CONFIRM_COOLDOWN_SEC = 50  # wait before running real email check after CONFIRM AGAIN

# UI header used in CONFIRM AGAIN progress effect (keeps same look on every edit)
CONFIRM_EFFECT_HEADER = ""

# Task milestones: approved registrations -> reward added to MAIN (one-time per milestone)
TASK_MILESTONES = [10,20,30,40,50,70,100,200,300,500,1000]
# in-memory temp storage (preview data per user)
temp_data = {}

# NOTE:
# I am not implementing "random credential generation for paid registrations".
# This bot collects user-provided registration data (legitimate use).
# You can rename text strings as you like.

# =========================
# DB
# =========================
# DB location (use /sdcard/Download so Termux + sqlite3 commands match)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_DIR = "/sdcard/Download"
if os.path.isdir(DOWNLOAD_DIR):
    DB = os.path.join(DOWNLOAD_DIR, "bot.db")
else:
    DB = os.path.join(SCRIPT_DIR, "bot.db")
def db():
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    # Ensure critical tables exist even if DB is old/corrupted migration-wise
    try:
        cur = con.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS blocked_users(
            user_id INTEGER PRIMARY KEY,
            blocked_at INTEGER
        )""")
        con.commit()
    except Exception:
        pass
    return con

def init_db():
    con = db()
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users(
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        lang TEXT DEFAULT 'hi',
        referrer_id INTEGER,
        main_balance REAL DEFAULT 0,
        hold_balance REAL DEFAULT 0,
        created_at INTEGER,
        referral_bonus_paid INTEGER DEFAULT 0
    )
    """)
    

    cur.execute("""
    CREATE TABLE IF NOT EXISTS pending_referrals(
        user_id INTEGER PRIMARY KEY,
        referrer_id INTEGER,
        created_at INTEGER
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS referral_bonuses(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        referrer_id INTEGER,
        referred_user_id INTEGER,
        amount REAL,
        created_at INTEGER,
        UNIQUE(referrer_id, referred_user_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS task_rewards(
        user_id INTEGER,
        milestone INTEGER,
        amount REAL,
        paid_at INTEGER,
        PRIMARY KEY(user_id, milestone)
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS rate(
        user_id INTEGER,
        minute_key INTEGER,
        count INTEGER,
        PRIMARY KEY(user_id, minute_key)
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS registrations(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        first_name TEXT,
        last_name TEXT,
        email TEXT UNIQUE,
        password TEXT,
        created_at INTEGER,
        state TEXT DEFAULT 'created'   -- created, confirmed_by_user, approved, rejected, canceled, timeout
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS actions(
        action_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        reg_id INTEGER,
        created_at INTEGER,
        expires_at INTEGER,
        state TEXT DEFAULT 'shown'     -- shown, done1, waiting_admin, approved, rejected, canceled, timeout
    )
    """)

    # Lightweight migrations (add columns if missing)
    try:
        cur.execute("ALTER TABLE actions ADD COLUMN updated_at INTEGER")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE registrations ADD COLUMN updated_at INTEGER")
    except Exception:
        pass
    cur.execute("""
    CREATE TABLE IF NOT EXISTS hold_credits(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        amount REAL,
        created_at INTEGER,
        matured_at INTEGER,
        moved INTEGER DEFAULT 0
    )
    """)

    # Provisional credits added at confirm time (reverted on admin reject)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS precredits(
        action_id INTEGER PRIMARY KEY,
        user_id INTEGER,
        hold_credit_id INTEGER,
        amount REAL,
        created_at INTEGER,
        reverted INTEGER DEFAULT 0
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS payouts(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        amount INTEGER,
        upi_or_qr TEXT,
        created_at INTEGER,
        state TEXT DEFAULT 'pending'   -- pending, approved, rejected
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS payout_proofs(
        payout_id INTEGER PRIMARY KEY,
        user_id INTEGER,
        amount INTEGER,
        upi_or_qr TEXT,
        utr TEXT,
        proof_file_id TEXT,
        created_at INTEGER
    )
    """)
    
    # --- migrate payouts table (reservation/refund) ---
    cur.execute("PRAGMA table_info(payouts)")
    _cols = {row[1] for row in cur.fetchall()}
    if "reserved" not in _cols:
        cur.execute("ALTER TABLE payouts ADD COLUMN reserved INTEGER DEFAULT 0")
    if "refunded" not in _cols:
        cur.execute("ALTER TABLE payouts ADD COLUMN refunded INTEGER DEFAULT 0")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS form_table(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        reg_id INTEGER UNIQUE,
        user_id INTEGER,
        first_name TEXT,
        email TEXT,
        password TEXT,
        created_at INTEGER
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS autoreply(
        id INTEGER PRIMARY KEY CHECK(id=1),
        enabled INTEGER DEFAULT 0,
        text TEXT DEFAULT ''
    )
    """)
    cur.execute("INSERT OR IGNORE INTO autoreply(id, enabled, text) VALUES(1,0,'')")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS blocked_users(
        user_id INTEGER PRIMARY KEY,
        blocked_at INTEGER
    )
    """)

    # Device / location logs (Telegram bots cannot read user's IP address or exact device model)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS device_logs(
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        lang TEXT,
        first_seen INTEGER,
        last_seen INTEGER,
        last_chat_type TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS user_locations(
        user_id INTEGER PRIMARY KEY,
        latitude REAL,
        longitude REAL,
        updated_at INTEGER
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS device_fingerprints(
        user_id INTEGER PRIMARY KEY,
        fp_hash TEXT,
        ua TEXT,
        platform TEXT,
        tz TEXT,
        screen TEXT,
        hw INTEGER,
        mem REAL,
        touch INTEGER,
        android_version TEXT,
        device_model TEXT,
        device_name TEXT,
        updated_at INTEGER
    )
    """)
    # Ensure new columns exist (safe migrations)
    for ddl in [
        "ALTER TABLE device_fingerprints ADD COLUMN device_name TEXT",
        "ALTER TABLE device_fingerprints ADD COLUMN ip_address TEXT",
        "ALTER TABLE device_fingerprints ADD COLUMN ua_snip TEXT",
        "ALTER TABLE device_fingerprints ADD COLUMN is_verified INTEGER",
        "ALTER TABLE device_fingerprints ADD COLUMN verified_at INTEGER",
    ]:
        try:
            cur.execute(ddl)
        except Exception:
            pass


    # Ledger (earnings history)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ledger(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        delta_main REAL DEFAULT 0,
        delta_hold REAL DEFAULT 0,
        reason TEXT,
        created_at INTEGER
    )
    """)

    
    # Admin email verification decisions (status/badge + reason)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS admin_email_verify(
        action_id INTEGER PRIMARY KEY,
        decided_by INTEGER,
        status TEXT,      -- VERIFIED / NOT_VERIFIED
        reason TEXT,
        decided_at INTEGER
    )
    """)
    con.commit()
    con.close()


def ensure_user(user_id, username, referrer_id=None):
    """Ensure a user row exists; set referrer only once; create referral_bonuses row (amount=0) once.

    NOTE: sqlite3 cursor returns tuples by default (unless row_factory is set). We keep tuple-safe code here.
    """
    con = db()
    cur = con.cursor()

    cur.execute("SELECT user_id, referrer_id FROM users WHERE user_id=?", (int(user_id),))
    r = cur.fetchone()
    now = int(time.time())

    if not r:
        cur.execute(
            "INSERT INTO users(user_id, username, referrer_id, created_at) VALUES (?,?,?,?)",
            (int(user_id), str(username or ""), int(referrer_id) if referrer_id else None, now),
        )
    else:
        # set referrer only once (if currently NULL)
        if referrer_id and r[1] is None and int(referrer_id) != int(user_id):
            cur.execute(
                "UPDATE users SET referrer_id=? WHERE user_id=?",
                (int(referrer_id), int(user_id)),
            )
        # always update username
        cur.execute(
            "UPDATE users SET username=? WHERE user_id=?",
            (str(username or ""), int(user_id)),
        )

    # record inviter->invitee (amount=0 row) once
    if referrer_id and int(referrer_id) != int(user_id):
        try:
            cur.execute(
                "INSERT OR IGNORE INTO referral_bonuses(referrer_id, referred_user_id, amount, created_at) VALUES(?,?,0,?)",
                (int(referrer_id), int(user_id), now),
            )
        except Exception:
            pass

    con.commit()
    con.close()

def get_lang(user_id):

    con = db()
    cur = con.cursor()
    cur.execute("SELECT lang FROM users WHERE user_id=?", (user_id,))
    r = cur.fetchone()
    con.close()
    return (r[0] if r else "hi")

def set_lang(user_id, lang):
    con = db()
    cur = con.cursor()
    cur.execute("UPDATE users SET lang=? WHERE user_id=?", (lang, user_id))
    con.commit()
    con.close()

def get_balances(user_id):
    con = db()
    cur = con.cursor()
    cur.execute("SELECT main_balance, hold_balance FROM users WHERE user_id=?", (user_id,))
    r = cur.fetchone()
    con.close()
    if not r:
        return 0.0, 0.0
    return float(r[0]), float(r[1])



def apply_task_rewards(cur, user_id: int, approved_count: int) -> float:
    """Pay out milestone rewards to MAIN balance. Returns total newly paid."""
    paid_total = 0.0
    for m in TASK_MILESTONES:
        if approved_count >= m:
            cur.execute("SELECT 1 FROM task_rewards WHERE user_id=? AND milestone=?", (user_id, m))
            if cur.fetchone():
                continue
            amt = float(m)
            cur.execute(
                "INSERT INTO task_rewards(user_id, milestone, amount, paid_at) VALUES(?,?,?,?)",
                (user_id, m, amt, int(time.time())),
            )
            cur.execute("UPDATE users SET main_balance = main_balance + ? WHERE user_id=?", (amt, user_id))
            add_ledger_entry(user_id, delta_main=float(amt), reason=f"Task reward milestone {m}")
            paid_total += amt
    return paid_total


def task_menu_text(user_id: int) -> str:
    con = db()
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM registrations WHERE user_id=? AND state='approved'", (user_id,))
    approved = int(cur.fetchone()["c"])
    cur.execute("SELECT milestone FROM task_rewards WHERE user_id=?", (user_id,))
    claimed = {int(r["milestone"]) for r in cur.fetchall()}
    con.close()

    lines = []
    lines.append("✅ TASK MENU")
    lines.append(f"Approved ✅: {approved}")
    lines.append("")
    for m in TASK_MILESTONES:
        if m in claimed:
            lines.append(f"✅ {m} APPROVE ✅ = ₹{m}")
        else:
            left = max(m - approved, 0)
            if left == 0:
                lines.append(f"🟡 {m} APPROVE ✅ = ₹{m}  (will add soon)")
            else:
                lines.append(f"⏳ {m} APPROVE ✅ = ₹{m}  (need {left} more)")
    return "\n".join(lines)


def add_hold_credit(user_id, amount) -> int:
    """Add amount to HOLD and create a hold_credits row. Returns hold_credits.id."""
    now = int(time.time())
    matured_at = now + HOLD_TO_MAIN_AFTER_DAYS * 24 * 3600
    con = db()
    cur = con.cursor()
    cur.execute("UPDATE users SET hold_balance = hold_balance + ? WHERE user_id=?", (amount, user_id))
    add_ledger_entry(user_id, delta_hold=float(amount), reason="HOLD credit added")
    cur.execute(
        "INSERT INTO hold_credits(user_id, amount, created_at, matured_at, moved) VALUES(?,?,?,?,0)",
        (user_id, float(amount), now, matured_at),
    )
    hid = cur.lastrowid
    con.commit()
    con.close()
    return int(hid)

def revert_hold_credit(hold_credit_id: int, user_id: int, amount: float) -> None:
    """Revert a previously added HOLD credit (prevent maturation + subtract from hold_balance)."""
    con = db()
    cur = con.cursor()
    # prevent this credit from ever maturing
    cur.execute("UPDATE hold_credits SET moved=1 WHERE id=? AND user_id=?", (int(hold_credit_id), int(user_id)))
    # subtract from hold balance (guard against negative)
    cur.execute("SELECT hold_balance FROM users WHERE user_id=?", (int(user_id),))
    r = cur.fetchone()
    hb = float(r[0]) if r else 0.0
    new_hb = hb - float(amount)
    if new_hb < 0:
        new_hb = 0.0
    cur.execute("UPDATE users SET hold_balance=? WHERE user_id=?", (new_hb, int(user_id)))
    con.commit()
    con.close()


def move_matured_hold_to_main(user_id):
    """Move matured HOLD credits to MAIN. Returns amount moved (float)."""
    now = int(time.time())
    con = db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT id, amount FROM hold_credits
        WHERE user_id=? AND moved=0 AND matured_at<=?
        """,
        (user_id, now),
    )
    rows = cur.fetchall()
    if not rows:
        con.close()
        return 0.0

    total = sum(float(x["amount"]) for x in rows)

    cur.execute(
        "UPDATE users SET hold_balance = hold_balance - ?, main_balance = main_balance + ? WHERE user_id=?",
        (total, total, user_id),
    )
    add_ledger_entry(user_id, delta_main=float(total), delta_hold=-float(total), reason="HOLD matured to MAIN")
    ids = [str(x["id"]) for x in rows]
    cur.execute(f"UPDATE hold_credits SET moved=1 WHERE id IN ({','.join(ids)})")

    con.commit()
    con.close()
    return float(total)


def can_do_action(user_id):
    minute_key = int(time.time() // 60)
    con = db()
    cur = con.cursor()
    cur.execute("SELECT count FROM rate WHERE user_id=? AND minute_key=?", (user_id, minute_key))
    r = cur.fetchone()
    if not r:
        cur.execute("INSERT INTO rate(user_id, minute_key, count) VALUES(?,?,1)", (user_id, minute_key))
        con.commit()
        con.close()
        return True
    if r["count"] >= MAX_PER_MIN:
        con.close()
        return False
    cur.execute("UPDATE rate SET count=count+1 WHERE user_id=? AND minute_key=?", (user_id, minute_key))
    con.commit()
    con.close()
    return True

# =========================
# UI MENUS (7 menus)
# =========================

def webapp_verify_kb():
    # Telegram Web App button (opens inside Telegram). Requires HTTPS URL.
    url = (FINGERPRINT_PUBLIC_BASE_URL or "").rstrip("/") + "/webapp"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔐 VERIFY DEVICE", web_app=WebAppInfo(url=url))],
        [InlineKeyboardButton("✅ I VERIFIED", callback_data="WEBAPP_CHECK")],
    ])

MAIN_MENU = ReplyKeyboardMarkup(
    [
        ["➕ Register a new account", "📋 My accounts"],
        ["💰 Balance", "👥 My referrals"],
        ["⚙️ Settings", "✅ TASK"],
        ["💬 Help", "👤 Profile"],
            ],
    resize_keyboard=True
)
# =========================
# HELP MENU (9 BUTTONS)
# =========================
HELP_BUTTONS = [
    ("⏰What is hold? (होल्ड क्या है?)", "HELP_1"),
    ("📲How to avoid sms confirmation? (एसएमएस कन्फर्मेशन से कैसे बचें?)", "HELP_2"),
    ("🔴Why is the account unavailable? (अकाउंट उपलब्ध क्यों नहीं है?)", "HELP_3"),
    ("❇️How can I avoid blocking my Gmail account? (अपने जीमेल अकाउंट को ब्लॉक होने से कैसे बचाएं?)", "HELP_4"),
    ("♾️How does the referral 🫂 system work? (रेफरल सिस्टम कैसे काम करता है?)", "HELP_5"),
    ("💧How many Gmail accounts can pay 💲for a bot? (एक बॉट के लिए कितने जीमेल अकाउंट पे कर सकते हैं?)", "HELP_6"),
]

HELP_TEXTS = {
    "HELP_1": """\"Hold\" is a 2-days period during which the Gmail account is \"resting\". The fact is that within 2 days after creating an account, Google can block it. At the end of the "recess", the account goes to moderation, after which funds are accrued to \"Balance\".

\"होल्ड\" 2 दिन की अवधि होती है जिसमें जीमेल अकाउंट \"आराम\" करता है। अकाउंट बनाने के 2 दिनों के अंदर गूगल उसे ब्लॉक कर सकता है। इस अवधि के बाद अकाउंट मॉडरेशन में जाता है और फिर फंड \"बैलेंस\" में जोड़ दिए जाते हैं।""",

    "HELP_2": """To prevent Google from asking you to confirm your phone number during registration, you should follow some recommendations:

Do not register more than two accounts per day from the same browser.
Do not register more than two accounts per day from the same IP address.
✖️Do not install extensions in the browser.
✖️Do not use a VPN.

✅ Use the browser mode "Incognito" (or clear the browser cache after each registration).
✅ Use android device emulators.
✅ Use several "Portable" browsers.

❕If your Internet provider provides dynamic IP addresses, disable and enable the modem. This operation will change your IP address.
❕When registering via a mobile network, turn off and turn on the Internet. This operation will change your IP address.

If the above actions do not help to bypass sms confirmation, then you will have to specify a number to which you can receive sms.

---

रजिस्ट्रेशन के दौरान गूगल द्वारा फोन नंबर कन्फर्मेशन मांगने से बचने के लिए:

एक ही ब्राउज़र से दिन में दो से ज्यादा अकाउंट रजिस्टर न करें।
एक ही आईपी एड्रेस से दिन में दो से ज्यादा अकाउंट रजिस्टर न करें।
✖️ ब्राउज़र में एक्सटेंशन इंस्टॉल न करें।
✖️ वीपीएन का उपयोग न करें।

✅ "इनकॉग्निटो" मोड इस्तेमाल करें या हर रजिस्ट्रेशन के बाद कैश क्लियर करें।
✅ एंड्रॉइड एमुलेटर का उपयोग करें।
✅ कई "पोर्टेबल" ब्राउज़र का उपयोग करें।

❕ डायनामिक आईपी होने पर मॉडेम बंद/चालू करें।
❕ मोबाइल नेटवर्क पर इंटरनेट बंद/चालू करें।

अगर ये तरीके काम न करें तो एसएमएस प्राप्त करने के लिए नंबर देना होगा।""",

    "HELP_3": """Within 2 days after registration, Google may block suspicious accounts. Such accounts are not paid and are marked as "Unavailable".

If you try to log into such an account, you will understand that this account cannot be used.

---

रजिस्ट्रेशन के 2 दिनों के अंदर गूगल संदिग्ध अकाउंट को ब्लॉक कर सकता है। ऐसे अकाउंट का भुगतान नहीं होता और उन्हें "अनअवेलेबल" मार्क किया जाता है।

ऐसे अकाउंट में लॉगिन करने पर पता चलेगा कि उसे इस्तेमाल नहीं किया जा सकता।""",

    "HELP_4": """To prevent Google from blocking your account, you should follow some recommendations:

✖do not log in to your account after registration.
Do not register more than two accounts per day from the same browser.
Do not register more than two accounts per day from the same IP address.
✖️Do not use a VPN.

✅ Log out of your account immediately after registration.
✅ Use "Incognito" mode.
✅ Use android device emulators.
✅ Use several "Portable" browsers.

❕Change IP by restarting modem or mobile internet.

---

अपने अकाउंट को ब्लॉक होने से बचाने के लिए:

✖️ रजिस्ट्रेशन के बाद लॉगिन न करें।
एक ही ब्राउज़र और आईपी से दिन में दो से ज्यादा अकाउंट न बनाएं।
✖️ वीपीएन का उपयोग न करें।

✅ रजिस्ट्रेशन के तुरंत बाद लॉगआउट करें।
✅ इनकॉग्निटो मोड इस्तेमाल करें।
✅ एंड्रॉइड एमुलेटर का उपयोग करें।
✅ अलग-अलग ब्राउज़र का उपयोग करें।

❕ आईपी बदलने के लिए मॉडेम या इंटरनेट रीस्टार्ट करें।""",

    "HELP_5": """Every user who goes to the bot using your referral link will become your referral.

Each Gmail account registered by your referral will bring you a referral fee after it is accepted.

You can have any number of referrals.

---

जो यूज़र आपके रेफरल लिंक से बॉट में आएगा वह आपका रेफरल बनेगा।

उसके द्वारा रजिस्टर किया गया हर जीमेल अकाउंट स्वीकार होने के बाद आपको रेफरल फीस देगा।

आप कितने भी रेफरल रख सकते हैं।""",

    "HELP_6": """The bot will accept any number of accounts that you can register. The main thing is that Google would not block them during the 2-days hold.

---

बॉट उतने अकाउंट स्वीकार करेगा जितने आप रजिस्टर कर सकते हैं। मुख्य बात यह है कि 2 दिन के होल्ड के दौरान गूगल उन्हें ब्लॉक न करे।""",
}

def help_menu_kb() -> InlineKeyboardMarkup:
    rows = []
    for label, cb in HELP_BUTTONS:
        rows.append([InlineKeyboardButton(label, callback_data=cb)])

    rows.append([InlineKeyboardButton("Technical Support (तकनीकी सहायता)", url="https://t.me/onlythiiiis")])
    rows.append([InlineKeyboardButton("Project News (प्रोजेक्ट समाचार)", url="https://t.me/gmailprojectnews")])
    rows.append([InlineKeyboardButton("Buy Accounts (अकाउंट खरीदें)", url="https://t.me/Tyhhy7")])
    return InlineKeyboardMarkup(rows)

def help_back_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 BACK", callback_data="HELP_BACK")]])
    
def balance_menu():
    return ReplyKeyboardMarkup(
        [["💳 Payout", "🧾 Balance history"], ["🔙 Back"]],
        resize_keyboard=True
    )

def settings_menu():
    return ReplyKeyboardMarkup([["LANGUAGE🔤"], ["🔙 Back"]], resize_keyboard=True)

def language_menu():
    return ReplyKeyboardMarkup(
        [["ENGLISH 🅰️", "हिंदी ✔️"], ["اردو❤️"], ["🔙 Back"]],
        resize_keyboard=True
    )

def reg_buttons(action_id):
    # 3 buttons (stacked like in your reference):
    # 1) DONE  2) CANCEL  3) How to create account (video)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("DONE✅", callback_data=f"REG_DONE:{action_id}")],
        [InlineKeyboardButton("CANCEL ❌ REGISTRATION", callback_data=f"REG_CANCEL:{action_id}")],
        [InlineKeyboardButton("❓How to create account ?", callback_data="VID_CREATE")],
    ])

def confirm_again_button(action_id):
    # After DONE: show CONFIRM AGAIN  # CONFIRM AGAIN HEAVY EFFECT + logout tutorial (always attached)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("CONFIRM ⭐ AGAIN", callback_data=f"REG_CONFIRM:{action_id}")],
        [InlineKeyboardButton("📲How to logout of account ?", callback_data="VID_LOGOUT")],
    ])

def post_confirm_buttons():
    # After successful CONFIRM (request sent to admin), CONFIRM button disappears, logout stays.
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📲How to logout of account ?", callback_data="VID_LOGOUT")],
    ])


def cancel_confirm_buttons(action_id: int) -> InlineKeyboardMarkup:
    # On CANCEL click: show DONE + SURE TO CANCEL (cancel only on sure)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("DONE✅", callback_data=f"REG_DONE:{action_id}")],
        [InlineKeyboardButton("SURE TO CANCEL ✖️ REGISTER", callback_data=f"REG_CANCEL_SURE:{action_id}")],
    ])


def accounts_nav(offset, total):
    btns = []
    if offset - 5 >= 0:
        btns.append(InlineKeyboardButton("◀️ PREV", callback_data=f"ACC:{offset-5}"))
    if offset + 5 < total:
        btns.append(InlineKeyboardButton("NEXT ▶️", callback_data=f"ACC:{offset+5}"))
    return InlineKeyboardMarkup([btns]) if btns else None

def payout_amounts_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("55💲", callback_data="PAY_AMT:55"),
         InlineKeyboardButton("110❤️", callback_data="PAY_AMT:110")],
        [InlineKeyboardButton("210🥰", callback_data="PAY_AMT:210"),
         InlineKeyboardButton("310😁", callback_data="PAY_AMT:310")],
        [InlineKeyboardButton("510😯", callback_data="PAY_AMT:510"),
         InlineKeyboardButton("1050💰", callback_data="PAY_AMT:1050")],
    ])

# =========================
# HELPERS
# =========================

async def user_in_required_channels(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> bool:
    """
    Checks if user is member of all REQUIRED_CHANNELS.
    NOTE: For channels, bot usually must be admin to reliably check membership.
    If Telegram denies access, we treat it as not-joined (safe default).
    """
    for chat_username, _url in REQUIRED_CHANNELS:
        try:
            m = await context.bot.get_chat_member(chat_id=chat_username, user_id=user_id)
            status = getattr(m, "status", None)
            if status in ("left", "kicked"):
                return False
        except Exception:
            return False
    return True

def join_channels_kb() -> InlineKeyboardMarkup:
    btns = []
    for _chat, url in REQUIRED_CHANNELS:
        btns.append([InlineKeyboardButton("JOIN THIS CHANNEL", url=url)])
    btns.append([InlineKeyboardButton("✅ I JOINED", callback_data="CHK_JOIN")])
    return InlineKeyboardMarkup(btns)

async def _send_video_by_paths(context: ContextTypes.DEFAULT_TYPE, chat_id: int, paths, caption: str = "", cache_key: str = ""):
    """Send local video file by trying multiple paths.
    Uses Telegram file_id cache for faster sending after first upload.
    """
    if cache_key and VIDEO_FILE_ID_CACHE.get(cache_key):
        try:
            await context.bot.send_video(chat_id=chat_id, video=VIDEO_FILE_ID_CACHE[cache_key], caption=caption)
            return True
        except Exception:
            pass

    for p in paths:
        try:
            if p and os.path.exists(p):
                with open(p, "rb") as f:
                    m = await context.bot.send_video(chat_id=chat_id, video=f, caption=caption)
                if cache_key:
                    try:
                        VIDEO_FILE_ID_CACHE[cache_key] = m.video.file_id
                    except Exception:
                        pass
                return True
        except Exception:
            continue
    return False

async def send_create_account_video(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    try:
        await context.bot.send_video(
            chat_id=chat_id,
            video=VIDEO_FILE_ID_CREATE,
            caption="✅ How to create account (video)",
        )
    except Exception:
        await context.bot.send_message(chat_id=chat_id, text="❌ Video send failed. Try again later.")

async def send_logout_video(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    try:
        await context.bot.send_video(
            chat_id=chat_id,
            video=VIDEO_FILE_ID_LOGOUT,
            caption="✅ How to logout of account (video)",
        )
    except Exception:
        await context.bot.send_message(chat_id=chat_id, text="❌ Video send failed. Try again later.")


def _confirm_bar(p: int, width: int = 10) -> str:
    p = max(0, min(100, int(p)))
    filled = int((p / 100) * width)
    return "█" * filled + "░" * (width - filled)

async def animate_confirm_effect(q, base_text: str, action_id: int):
    # Edit the SAME message with a progress effect
    for p in (0, 10, 20, 30, 40, 50, 60, 70, 80, 90):
        try:
            await q.edit_message_text(
                base_text + f"\n\n🔍 EMAIL CHECKING...\n[{_confirm_bar(p)}] {p}%",
                reply_markup=confirm_again_button(action_id),
            )
        except Exception:
            pass
        await asyncio.sleep(0.0005)



async def _edit_message_safe(bot, chat_id: int, message_id: int, text: str, reply_markup=None):
    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, reply_markup=reply_markup)
        return True
    except Exception:
        return False

async def animate_confirm_effect_msg(bot, chat_id: int, message_id: int, base_text: str, action_id: int, keep_buttons: bool = True):
    # Progress effect by editing a separate "confirm" message
    kb = confirm_again_button(action_id) if keep_buttons else None
    for p in (0, 10, 20, 30, 40, 50, 60, 70, 80, 90):
        await _edit_message_safe(
            bot,
            chat_id,
            message_id,
            base_text + f"\n\n🔍 EMAIL CHECKING...\n[{_confirm_bar(p)}] {p}%",
            reply_markup=kb,
        )
        await asyncio.sleep(0.0005)


async def gate_if_not_joined(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Returns True if user is allowed, else sends join prompt and returns False."""
    user = update.effective_user
    # Keep device log (does not block)
    try:
        upsert_device_log(update)
    except Exception:
        pass

    if user is None or is_admin(user.id):
        return True

    # Device verify system removed: only enforce required channel joins
    ok = await user_in_required_channels(context, user.id)
    if ok:
        return True

    msg = "{😊 FIRST PLEASE JOIN THIS CHANNEL ✅}"
    if update.message:
        await update.message.reply_text(msg, reply_markup=join_channels_kb())
    elif update.callback_query:
        try:
            await update.callback_query.message.reply_text(msg, reply_markup=join_channels_kb())
        except Exception:
            pass
    return False

def is_valid_upi_id(s: str) -> bool:
    """Basic UPI id validation (example: name@bank)."""
    s = (s or "").strip()
    if " " in s:
        return False
    # common UPI ID pattern
    return bool(re.fullmatch(r"[A-Za-z0-9._\-]{2,256}@[A-Za-z]{2,64}", s))

def classify_upi_or_qr(s: str) -> str:
    """Returns 'upi' or 'qr' depending on the input."""
    s = (s or "").strip()
    if s.lower().startswith("upi://"):
        return "upi"
    if "@" in s and is_valid_upi_id(s):
        return "upi"
    return "qr"



# =========================
# EMAIL CHECK (SAFE)
# =========================
# Note: This checks only syntax + domain MX availability (deliverability check).
# It does NOT confirm whether a specific Gmail address exists (providers often block that for privacy).

EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")

def is_valid_email_syntax(email: str) -> bool:
    return bool(EMAIL_RE.match((email or "").strip()))


def _smtp_send_test_email(to_addr: str, subject: str, body: str) -> str:
    """Blocking SMTP send. Returns 'sent' or raises."""
    msg = EmailMessage()
    msg["From"] = SMTP_GMAIL_USER
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg.set_content(body)

    # Timeout to keep it fast
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=15) as s:
        s.login(SMTP_GMAIL_USER, SMTP_GMAIL_APP_PASSWORD)
        s.send_message(msg)
    return "sent"


def _imap_find_bounce(recipient: str, token: str, lookback: int = 50) -> str | None:
    """
    Blocking IMAP scan for DSN bounces related to (recipient, token).
    Returns:
      - "no_such_user" if 5.1.1 / NoSuchUser found
      - "bounced_other" if bounce found but not clearly 5.1.1
      - None if not found yet
    """
    M = imaplib.IMAP4_SSL("imap.gmail.com")
    M.login(SMTP_GMAIL_USER, SMTP_GMAIL_APP_PASSWORD)
    M.select("INBOX")

    typ, data = M.search(None, "ALL")
    if typ != "OK":
        M.logout()
        return None

    ids = data[0].split()[-lookback:]
    recip_lc = recipient.lower()
    token_lc = (token or "").lower()

    bounce_keywords = (
        "delivery status notification",
        "mail delivery subsystem",
        "undelivered mail returned to sender",
        "delivery status",
        "failure notice",
        "delivery failed",
    )

    for msg_id in reversed(ids):
        typ, msg_data = M.fetch(msg_id, "(RFC822)")
        if typ != "OK":
            continue

        raw = msg_data[0][1]
        msg = email_pkg.message_from_bytes(raw)

        subj = (msg.get("Subject", "") or "").lower()
        from_ = (msg.get("From", "") or "").lower()

        if not (any(k in subj for k in bounce_keywords) or "mailer-daemon" in from_ or "postmaster" in from_):
            continue

        # Extract text parts
        parts: list[str] = []
        if msg.is_multipart():
            for p in msg.walk():
                ctype = (p.get_content_type() or "").lower()
                if ctype in ("text/plain", "message/delivery-status"):
                    try:
                        parts.append(p.get_payload(decode=True).decode(errors="ignore"))
                    except Exception:
                        pass
        else:
            try:
                parts.append(msg.get_payload(decode=True).decode(errors="ignore"))
            except Exception:
                pass

        blob = "\n".join(parts).lower()

        # Must match token or recipient (to reduce false matches)
        if token_lc and token_lc not in subj and token_lc not in blob and recip_lc not in blob:
            continue
        if recip_lc not in blob and recip_lc not in subj:
            # some DSNs don't include subject, but usually include recipient
            continue

        if "5.1.1" in blob or "nosuchuser" in blob or "user unknown" in blob or "no such user" in blob:
            M.logout()
            return "no_such_user"

        M.logout()
        return "bounced_other"

    M.logout()
    return None


async def smtp_bounce_check_fast(recipient: str, token: str) -> str:
    """
    Fast deliverability check:
      1) Try SMTP send: if recipient is rejected immediately, return 'no_such_user'
      2) Otherwise poll IMAP up to ~60s for bounce; if bounce indicates 5.1.1, return 'no_such_user'
      3) If no bounce seen quickly, return 'ok_or_unknown'
    """
    if not ENABLE_SMTP_BOUNCE_CHECK:
        return "disabled"
    if not SMTP_GMAIL_USER or not SMTP_GMAIL_APP_PASSWORD:
        return "no_creds"

    subject = f"Verify-{token}"
    body = f"Verification ping for {recipient}. Token={token}"

    # SMTP send in thread to avoid blocking event loop
    try:
        await asyncio.to_thread(_smtp_send_test_email, recipient, subject, body)
    except smtplib.SMTPRecipientsRefused:
        return "no_such_user"
    except smtplib.SMTPException:
        # Could be rate limit, auth, etc. Treat as unknown to avoid blocking.
        return "unknown"

    # Poll IMAP for DSN bounces quickly
    total = 0
    for w in BOUNCE_POLL_INTERVALS:
        total += w
        await asyncio.sleep(w)
        try:
            res = await asyncio.to_thread(_imap_find_bounce, recipient, token, 60)
        except Exception:
            res = None
        if res == "no_such_user":
            return "no_such_user"
        # other bounce types -> treat as unknown (could be temporary)
        if res == "bounced_other":
            return "unknown"

    return "ok_or_unknown"


# =========================
# EMAIL SYSTEM (PostgreSQL + Gmail API)
# Replaces IMAP handle search.
# =========================

EMAIL_HANDLE_RE = re.compile(r"\b([a-z0-9._%+\-]{2,64})@gmail\.com\b", re.I)

def _pg_conn():
    # Railway provides DATABASE_URL
    import os
    import psycopg2
    return psycopg2.connect(os.environ["DATABASE_URL"])

def _pg_email_init():
    """Create tables used by the email-handle cache."""
    with _pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS email_cache (
                    handle TEXT PRIMARY KEY,
                    last_seen BIGINT NOT NULL
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS email_meta (
                    k TEXT PRIMARY KEY,
                    v TEXT NOT NULL
                );
            """)
        conn.commit()

def _pg_email_set_meta(k: str, v: str):
    with _pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO email_meta(k,v) VALUES(%s,%s)
                     ON CONFLICT (k) DO UPDATE SET v=EXCLUDED.v""",
                (k, str(v)),
            )
        conn.commit()

def _pg_email_get_meta(k: str, default: str = ""):
    with _pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT v FROM email_meta WHERE k=%s", (k,))
            row = cur.fetchone()
            return row[0] if row else default

def _pg_email_upsert_handle(handle: str):
    handle = (handle or "").strip().lower()
    if not handle:
        return
    with _pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO email_cache(handle,last_seen) VALUES(%s,%s)
                     ON CONFLICT (handle) DO UPDATE SET last_seen=EXCLUDED.last_seen""",
                (handle, int(time.time())),
            )
        conn.commit()

def _pg_email_handle_exists(handle: str) -> bool:
    handle = (handle or "").strip().lower()
    if not handle:
        return False
    with _pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM email_cache WHERE handle=%s LIMIT 1", (handle,))
            return cur.fetchone() is not None

def _gmail_api_service():
    """Build Gmail API service using token/credentials from env or local files."""
    import os
    from googleapiclient.discovery import build
    from google.oauth2.credentials import Credentials

    # Optional: write JSONs from Railway env vars
    creds_env = os.environ.get("GMAIL_CREDENTIALS_JSON", "").strip()
    token_env = os.environ.get("GMAIL_TOKEN_JSON", "").strip()
    if creds_env:
        with open("credentials.json", "w", encoding="utf-8") as f:
            f.write(creds_env)
    if token_env:
        with open("token.json", "w", encoding="utf-8") as f:
            f.write(token_env)

    creds = Credentials.from_authorized_user_file("token.json", ["https://www.googleapis.com/auth/gmail.readonly"])
    return build("gmail", "v1", credentials=creds, cache_discovery=False)

async def _gmail_sync_loop(poll_sec: int = 5, max_list: int = 200):
    """Poll Gmail every poll_sec seconds, extract gmail handles from message text, store into PostgreSQL."""
    import os

    ignore = {h.strip().lower() for h in os.environ.get("IGNORE_HANDLES", "").split(",") if h.strip()}
    svc = _gmail_api_service()

    _pg_email_init()
    last_msg_id = _pg_email_get_meta("last_msg_id", "")

    query = os.environ.get("GMAIL_SYNC_QUERY", 'newer_than:14d (gmail.com OR "email for" OR "sent to")')

    while True:
        try:
            res = svc.users().messages().list(userId="me", q=query, maxResults=max_list).execute()
            msgs = res.get("messages", []) or []

            for m in msgs:
                mid = m.get("id")
                if not mid:
                    continue
                if last_msg_id and mid == last_msg_id:
                    break

                msg = svc.users().messages().get(
                    userId="me",
                    id=mid,
                    format="metadata",
                    metadataHeaders=["Subject", "From", "To"],
                ).execute()

                # Use snippet (fast). Handle is in body text -> snippet usually contains it.
                snippet = (msg.get("snippet", "") or "")
                # Also include header values (sometimes the email appears there)
                payload = msg.get("payload", {}) or {}
                headers = payload.get("headers", []) or []
                header_text = " ".join((h.get("value", "") or "") for h in headers)

                text = (snippet + " " + header_text)

                for full in EMAIL_HANDLE_RE.findall(text):
                    h = full.split("@")[0].lower().strip()
                    if h and h not in ignore:
                        _pg_email_upsert_handle(h)

            if msgs:
                last_msg_id = msgs[0].get("id", last_msg_id)
                if last_msg_id:
                    _pg_email_set_meta("last_msg_id", last_msg_id)

        except Exception:
            # keep running on transient errors
            pass

        await asyncio.sleep(poll_sec)





def is_upi_or_qr_used(value: str, kind: str, current_user_id: int) -> bool:
    """True if same UPI/QR was used by another user before."""
    v = (value or "").strip()
    if kind == "upi":
        v = v.lower()
    con = db()
    cur = con.cursor()
    if kind == "upi":
        cur.execute("SELECT user_id FROM payouts WHERE lower(upi_or_qr)=? LIMIT 1", (v,))
    else:
        cur.execute("SELECT user_id FROM payouts WHERE upi_or_qr=? LIMIT 1", (v,))
    row = cur.fetchone()
    con.close()
    return bool(row and int(row[0]) != int(current_user_id))

def fmt_ts(ts: int) -> str:
    try:
        ts = int(ts)
    except Exception:
        return "-"
    try:
        # If imported `import datetime`, use datetime.datetime.fromtimestamp
        return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
    except Exception:
        try:
            # If imported `from datetime import datetime` somewhere, fallback
            return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
        except Exception:
            return "-"




# =========================
# USER STATS & LEDGER
# =========================

def add_ledger_entry(user_id: int, delta_main: float = 0.0, delta_hold: float = 0.0, reason: str = ""):
    try:
        con = db()
        cur = con.cursor()
        cur.execute(
            "INSERT INTO ledger(user_id, delta_main, delta_hold, reason, created_at) VALUES(?,?,?,?,?)",
            (int(user_id), float(delta_main), float(delta_hold), str(reason or ""), int(time.time())),
        )
        con.commit()
        con.close()
    except Exception:
        try:
            con.close()
        except Exception:
            pass

def get_profile_counts(user_id: int):
    """Profile counts:
    - TOTAL REGISTRATIONS: only those that reached admin verify queue (actions waiting_admin/approved/rejected)
    - TOTAL APPROVED: actions approved (VERIFIED ✅)
    - TOTAL REJECT: actions rejected (NOT VERIFIED)
    - TOTAL CANCELED: registrations canceled
    """
    con = db()
    cur = con.cursor()

    cur.execute(
        "SELECT COUNT(*) AS c FROM actions WHERE user_id=? AND state IN ('waiting_admin','approved','rejected')",
        (int(user_id),),
    )
    total = int(cur.fetchone()["c"] or 0)

    cur.execute("SELECT COUNT(*) AS c FROM actions WHERE user_id=? AND state='approved'", (int(user_id),))
    approved = int(cur.fetchone()["c"] or 0)

    cur.execute("SELECT COUNT(*) AS c FROM actions WHERE user_id=? AND state='rejected'", (int(user_id),))
    rejected = int(cur.fetchone()["c"] or 0)

    cur.execute("SELECT COUNT(*) AS c FROM registrations WHERE user_id=? AND state='canceled'", (int(user_id),))
    canceled = int(cur.fetchone()["c"] or 0)

    con.close()
    return total, approved, rejected, canceled

def get_ledger_rows(user_id: int, limit: int = 15):
    con = db()
    cur = con.cursor()
    cur.execute(
        "SELECT delta_main, delta_hold, reason, created_at FROM ledger WHERE user_id=? ORDER BY id DESC LIMIT ?",
        (int(user_id), int(limit)),
    )
    rows = cur.fetchall()
    con.close()
    return rows

def rebuild_payout_proofs_pdf(pdf_path: str = "payout_proofs.pdf") -> str:
    """Rebuild a PDF table of all completed payouts with proof."""
    con = db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT p.id AS payout_id, p.user_id, p.amount, p.upi_or_qr, pp.utr, pp.proof_file_id, pp.created_at
        FROM payouts p
        JOIN payout_proofs pp ON pp.payout_id = p.id
        ORDER BY pp.created_at DESC
        """
    )
    rows = cur.fetchall()
    con.close()

    styles = getSampleStyleSheet()
    doc = SimpleDocTemplate(pdf_path, pagesize=A4)
    elements = [Paragraph("PAYOUT PROOFS", styles["Title"]), Spacer(1, 12)]

    data = [["PAYOUT_ID", "USERID", "AMOUNT", "UPI/QR", "UTR", "PROOF_FILE_ID", "TIME"]]
    for r in rows:
        data.append([
            str(r["payout_id"]),
            str(r["user_id"]),
            str(r["amount"]),
            (r["upi_or_qr"] or "")[:24],
            (r["utr"] or "")[:24],
            (r["proof_file_id"] or "")[:18],
            fmt_ts(int(r["created_at"])) if r["created_at"] else "",
        ])

    table = Table(data, repeatRows=1)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.lightgrey),
        ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
        ("FONTSIZE", (0,0), (-1,-1), 8),
        ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
    ]))
    elements.append(table)
    doc.build(elements)
    return pdf_path


def referral_link(bot_username: str, referrer_id: int) -> str:
    return f"https://t.me/{bot_username}?start={referrer_id}"

def get_referral_overview(referrer_id: int, limit: int = 10):
    """
    Returns:
      total_referrals, total_earned, rows[list]
    Each row: {user_id, username, joined_at, approved_count, bonus_paid}
    """
    con = db()
    cur = con.cursor()

    cur.execute("SELECT COUNT(*) AS c FROM users WHERE referrer_id=?", (referrer_id,))
    total_ref = int(cur.fetchone()["c"])

    cur.execute("SELECT COALESCE(SUM(amount),0) AS s FROM referral_bonuses WHERE referrer_id=?", (referrer_id,))
    total_earned = float(cur.fetchone()["s"] or 0)

    cur.execute(
        """
        SELECT
            u.user_id AS user_id,
            u.username AS username,
            u.created_at AS joined_at,
            COALESCE(SUM(CASE WHEN r.state='approved' THEN 1 ELSE 0 END), 0) AS approved_count,
            CASE WHEN rb.id IS NULL THEN 0 ELSE 1 END AS bonus_paid
        FROM users u
        LEFT JOIN registrations r ON r.user_id = u.user_id
        LEFT JOIN referral_bonuses rb
            ON rb.referrer_id = ? AND rb.referred_user_id = u.user_id
        WHERE u.referrer_id = ?
        GROUP BY u.user_id, u.username, u.created_at, rb.id
        ORDER BY u.created_at DESC
        LIMIT ?
        """,
        (referrer_id, referrer_id, limit),
    )
    rows = [dict(x) for x in cur.fetchall()]
    con.close()
    return total_ref, total_earned, rows

def save_form_row(reg_id: int, user_id: int, first_name: str, email: str, password: str, created_at: int):
    con = db()
    cur = con.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO form_table(reg_id, user_id, first_name, email, password, created_at) "
        "VALUES(?,?,?,?,?,?)",
        (reg_id, user_id, first_name, email.lower(), password, created_at),
    )
    con.commit()
    con.close()

def export_form_csv(out_path: str = "form_data.csv"):
    import csv
    con = db()
    cur = con.cursor()
    cur.execute(
        "SELECT user_id, first_name, email, password, created_at "
        "FROM form_table ORDER BY id DESC"
    )
    rows = cur.fetchall()
    con.close()

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["USERID", "FIRSTNAME", "EMAIL", "PASSWORD", "TIME"])
        for r in rows:
            t = datetime.fromtimestamp(int(r["created_at"])).strftime("%Y-%m-%d %H:%M:%S")
            w.writerow([r["user_id"], r["first_name"], r["email"], r["password"], t])

def _bot_link_start(param: str) -> str:
    if BOT_USERNAME:
        return f"https://t.me/{BOT_USERNAME}?start={param}"
    return f"(set BOT_USERNAME) start={param}"


def set_pending_ref(user_id: int, referrer_id: int):
    try:
        con = db(); cur = con.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO pending_referrals(user_id, referrer_id, created_at) VALUES(?,?,?)",
            (int(user_id), int(referrer_id), int(time.time()))
        )
        con.commit(); con.close()
    except Exception:
        pass

def pop_pending_ref(user_id: int):
    try:
        con = db(); cur = con.cursor()
        cur.execute("SELECT referrer_id FROM pending_referrals WHERE user_id=?", (int(user_id),))
        r = cur.fetchone()
        if not r:
            con.close()
            return None
        ref = int(r["referrer_id"])
        cur.execute("DELETE FROM pending_referrals WHERE user_id=?", (int(user_id),))
        con.commit(); con.close()
        return ref
    except Exception:
        try:
            con.close()
        except Exception:
            pass
        return None


def _ref_link(user_id: int) -> str:
    return _bot_link_start(f"ref_{int(user_id)}")

def _get_referrals(referrer_id: int, limit: int = 50):
    con = db(); cur = con.cursor()
    cur.execute(
        "SELECT user_id, username, created_at FROM users WHERE referrer_id=? ORDER BY created_at DESC LIMIT ?",
        (int(referrer_id), int(limit)),
    )
    rows = cur.fetchall()
    con.close()
    return rows

def _referral_stats(referrer_id: int):
    con = db(); cur = con.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM users WHERE referrer_id=?", (int(referrer_id),))
    total = int(cur.fetchone()["c"] or 0)

    cur.execute("""
        SELECT COUNT(DISTINCT u.user_id) AS c
        FROM users u
        JOIN registrations r ON r.user_id = u.user_id
        WHERE u.referrer_id=? AND r.state='approved'
    """, (int(referrer_id),))
    approved_any = int(cur.fetchone()["c"] or 0)

    cur.execute("SELECT COALESCE(SUM(amount),0) AS s FROM referral_bonuses WHERE referrer_id=?", (int(referrer_id),))
    total_bonus = float(cur.fetchone()["s"] or 0)

    con.close()
    return total, approved_any, total_bonus

async def referral_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user(user.id, user.username or user.full_name)
    link = _ref_link(user.id)
    total, approved_any, total_bonus = _referral_stats(user.id)

    lines = [
        "👥 Referral Tracking",
        f"🔗 Your referral link: {link}",
        "",
        f"👤 Total invited: {total}",
        f"✅ Invited with at least 1 approved: {approved_any}",
        f"💰 Total referral bonus: ₹{total_bonus:.2f}",
        "",
        "📋 Latest invited users:",
    ]
    rows = _get_referrals(user.id, 30)
    if not rows:
        lines.append("— none yet —")
    else:
        con = db(); cur = con.cursor()
        for i, r in enumerate(rows, start=1):
            uid = int(r["user_id"])
            uname = (r["username"] or "").strip()
            cur.execute("SELECT COUNT(*) AS c FROM registrations WHERE user_id=? AND state='approved'", (uid,))
            ac = int(cur.fetchone()["c"] or 0)
            lines.append(f"{i}. {uid} | {uname} | approved: {ac}")
        con.close()

    await update.message.reply_text("\n".join(lines))


async def export_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Admin-only: export the "form" table as PDF and send it
    if update.effective_user.id != ADMIN_ID:
        return
    path = "form_data.pdf"
    export_form_pdf(path, limit=50)
    await update.message.reply_document(document=open(path, "rb"), filename="form_data.pdf")



def _fetch_form_rows(limit: int = 50):
    con = db()
    cur = con.cursor()
    cur.execute(
        "SELECT user_id, first_name, email, password, created_at "
        "FROM form_table ORDER BY id DESC LIMIT ?",
        (limit,),
    )
    rows = cur.fetchall()
    con.close()
    return rows

def _pdf_escape(s: str) -> str:
    # Escape characters for PDF literal strings
    return s.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")

def export_form_pdf(out_path: str = "form_data.pdf", limit: int = 50):
    """
    Create a lightweight PDF (no extra libraries) that looks like a simple table:
    USERID | FIRSTNAME | EMAIL | PASSWORD | TIME
    """
    rows = _fetch_form_rows(limit=limit)
    headers = ["USERID", "FIRSTNAME", "EMAIL", "PASSWORD", "TIME"]

    # Build fixed-width table lines (monospace)
    def trunc(s, n):
        s = str(s)
        return s if len(s) <= n else s[:n-1] + "…"

    colw = [10, 12, 24, 20, 16]  # character widths
    def fmt_row(cols):
        parts = []
        for val, w in zip(cols, colw):
            v = trunc(val, w).ljust(w)
            parts.append(v)
        return " | ".join(parts)

    lines = []
    lines.append(fmt_row(headers))
    lines.append("-" * len(lines[0]))

    for r in rows:
        t = datetime.fromtimestamp(int(r["created_at"])).strftime("%Y-%m-%d %H:%M")
        lines.append(fmt_row([
            str(r["user_id"]),
            str(r["first_name"]),
            str(r["email"]),
            str(r["password"]),
            t
        ]))

    # PDF basics (A4 portrait: 595x842 points)
    page_w, page_h = 595, 842
    font_size = 10
    leading = 14
    x0 = 36
    y0 = page_h - 60

    # Create content stream
    content = []
    content.append("BT")
    content.append(f"/F1 {font_size} Tf")
    content.append(f"{x0} {y0} Td")
    for i, line in enumerate(lines):
        esc = _pdf_escape(line)
        content.append(f"({esc}) Tj")
        if i != len(lines) - 1:
            content.append(f"0 {-leading} Td")
    content.append("ET")
    content_stream = "\n".join(content).encode("utf-8")

    # Build PDF objects
    objs = []
    def obj(n, body: bytes):
        objs.append((n, body))

    obj(1, b"<< /Type /Catalog /Pages 2 0 R >>")
    obj(2, b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>")
    obj(5, b"<< /Type /Font /Subtype /Type1 /BaseFont /Courier >>")
    obj(4, b"<< /Length %d >>\nstream\n" % len(content_stream) + content_stream + b"\nendstream")
    page_obj = b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 %d %d] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>" % (page_w, page_h)
    obj(3, page_obj)

    # Write file with xref
    with open(out_path, "wb") as f:
        f.write(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
        offsets = {0: 0}
        for n, body in objs:
            offsets[n] = f.tell()
            f.write(f"{n} 0 obj\n".encode("ascii"))
            f.write(body)
            f.write(b"\nendobj\n")
        xref_pos = f.tell()
        f.write(b"xref\n0 %d\n" % (len(objs) + 1))
        f.write(b"0000000000 65535 f \n")
        for n, _ in sorted(objs, key=lambda x: x[0]):
            f.write(f"{offsets[n]:010d} 00000 n \n".encode("ascii"))
        f.write(b"trailer\n")
        f.write(b"<< /Size %d /Root 1 0 R >>\n" % (len(objs) + 1))
        f.write(b"startxref\n")
        f.write(f"{xref_pos}\n".encode("ascii"))
        f.write(b"%%EOF\n")

    return out_path

async def formimg_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Admin-only: send the table in a photo-like way.
    Since many Android setups don't have Pillow installed, we send a PDF that looks like a table.
    Command name stays /formimg.
    """
    if update.effective_user.id != ADMIN_ID:
        return
    path = export_form_pdf("form_data.pdf", limit=50)
    await update.message.reply_document(document=open(path, "rb"), filename="form_data.pdf",
                                        caption="FORM TABLE (USERID | FIRSTNAME | EMAIL | PASSWORD | TIME)")



def is_admin(user_id):
    return user_id == ADMIN_ID

def is_blocked(user_id: int) -> bool:
    try:
        con = db()
        cur = con.cursor()
        cur.execute("SELECT 1 FROM blocked_users WHERE user_id=?", (int(user_id),))
        r = cur.fetchone()
        con.close()
        return r is not None
    except Exception:
        return False

def block_user_db(user_id: int):
    con = db()
    cur = con.cursor()
    # table ensured in db()
    cur.execute(
        "INSERT OR REPLACE INTO blocked_users(user_id, blocked_at) VALUES(?,?)",
        (int(user_id), int(time.time()))
    )
    con.commit()
    con.close()

def unblock_user_db(user_id: int):
    con = db()
    cur = con.cursor()
    cur.execute("DELETE FROM blocked_users WHERE user_id=?", (int(user_id),))
    con.commit()
    con.close()

def action_valid(action_id):
    con = db()
    cur = con.cursor()
    cur.execute(
        "SELECT * FROM actions WHERE action_id = ?",
        (action_id,)
    )
    a = cur.fetchone()
    con.close()

    if not a:
        return False, None

    now = int(time.time())
    if now > a["expires_at"] or a["state"] in (
        "timeout", "approved", "rejected"
    ):
        return False, a

    return True, a

def set_action_state(action_id, state):
    con = db()
    cur = con.cursor()
    now = int(time.time())
    try:
        cur.execute("UPDATE actions SET state=?, updated_at=? WHERE action_id=?", (state, now, action_id))
    except Exception:
        cur.execute("UPDATE actions SET state=? WHERE action_id=?", (state, action_id))
    con.commit()
    con.close()

def set_reg_state(reg_id, state):
    con = db()
    cur = con.cursor()
    now = int(time.time())
    try:
        cur.execute("UPDATE registrations SET state=?, updated_at=? WHERE id=?", (state, now, reg_id))
    except Exception:
        cur.execute("UPDATE registrations SET state=? WHERE id=?", (state, reg_id))
    con.commit()
    con.close()

# =========================
# START
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user

    # Parse referral FIRST so we don't lose it if user must join channels first
    ref = None
    if user and context.args:
        arg0 = str(context.args[0]).strip()
        if arg0.startswith('ref_') and arg0[4:].isdigit():
            rid = int(arg0[4:])
            if rid != user.id:
                ref = rid
        elif arg0.isdigit():
            rid = int(arg0)
            if rid != user.id:
                ref = rid

    # Gate check (required channel join)
    if not await gate_if_not_joined(update, context):
        if user and ref:
            set_pending_ref(user.id, ref)
        return

    # If user passed gate, use ref from start param OR pending saved earlier
    if user:
        pending = pop_pending_ref(user.id)
        if not ref and pending:
            ref = pending

        ensure_user(user.id, user.username or user.full_name, referrer_id=ref)

        moved = move_matured_hold_to_main(user.id)
        if moved > 0:
            try:
                await context.bot.send_message(chat_id=user.id, text="💸 Accrual of funds to the balance")
            except Exception:
                pass

    await update.message.reply_text("Welcome! Menu se option choose karo 👇", reply_markup=MAIN_MENU)


# =========================
# A) REGISTER (LEGIT FLOW)
# =========================
# Step flow:
# Tap "Register" -> ask First Name
# then Email -> then Password -> show final preview + DONE/CANCEL
# DONE -> show CONFIRM AGAIN  # CONFIRM AGAIN HEAVY EFFECT
# CONFIRM AGAIN  # CONFIRM AGAIN HEAVY EFFECT -> send to ADMIN for approve/reject (admin panel buttons)
# Admin Approve -> add HOLD credit (example amount) + user notified
# Admin Reject -> notify user

async def menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await gate_if_not_joined(update, context):
        return
    if user.id != ADMIN_ID and is_blocked(user.id):
        return

    ensure_user(user.id, user.username or user.full_name)
    moved = move_matured_hold_to_main(user.id)
    if moved > 0:
        try:
            await context.bot.send_message(chat_id=user.id, text="💸 Accrual of funds to the balance")
        except Exception:
            pass

    txt = update.message.text.strip()

    # Auto-reply (admin configurable)
    if not is_admin(user.id):
        con = db()
        cur = con.cursor()
        cur.execute("SELECT enabled, text FROM autoreply WHERE id=1")
        ar = cur.fetchone()
        con.close()
        if ar and int(ar["enabled"]) == 1 and txt and not txt.startswith("/"):
            # only if not in specific flows
            if not context.user_data.get("reg_flow") and not context.user_data.get("await_upi"):
                await update.message.reply_text(ar["text"])
                # continue normal handling too (if it's a menu tap, it will match)

    # MAIN MENU routes
    if txt == "➕ Register a new account":
        if not can_do_action(user.id):
            await update.message.reply_text("You have performed this action too often. Try again later")
            return

        await register(update, context)
        return

        # Begin legit input flow
        context.user_data["reg_flow"] = {"step": 1, "first_name": "", "email": "", "password": ""}
        await update.message.reply_text(
            "Register account using the specified data and get from ₹08 to ₹10\n\n"
            "Please enter FIRST NAME (A-Z, 5/6/7 characters):"
        )
        return

    if txt == "📋 My accounts":
        con = db()
        cur = con.cursor()
        # Only show registrations that were sent to admin (waiting_admin / approved)
        cur.execute("""
            SELECT COUNT(*) AS c
            FROM actions a
            JOIN registrations r ON r.id = a.reg_id
            WHERE a.user_id=? AND a.state IN ('waiting_admin','approved','rejected','canceled')
        """, (user.id,))
        total = int(cur.fetchone()["c"])
        if total == 0:
            con.close()
            await update.message.reply_text("Abhi koi approved/processing account nahi hai.")
            return

        cur.execute("""
            SELECT r.email AS email,
                   a.state AS astate,
                   COALESCE(a.updated_at, a.created_at, r.updated_at, r.created_at) AS stime
            FROM actions a
            JOIN registrations r ON r.id = a.reg_id
            WHERE a.user_id=? AND a.state IN ('waiting_admin','approved','rejected','canceled')
            ORDER BY COALESCE(a.updated_at, a.created_at) DESC
            LIMIT 5 OFFSET 0
        """, (user.id,))
        rows = cur.fetchall()
        con.close()

        lines = []
        for rr in rows:
            status = ("approved" if rr["astate"] == "approved" else ("rejected" if rr["astate"] == "rejected" else ("canceled" if rr["astate"] == "canceled" else "processing")))
            lines.append(f"• {rr['email']}  —  {status}  —  {fmt_ts(rr['stime'])}")

        msg = "📋 My accounts (page 1):\n\n" + "\n".join(lines)
        await update.message.reply_text(msg, reply_markup=accounts_nav(0, total))
        return

    if txt == "💰 Balance":
        mainb, holdb = get_balances(user.id)
        # exactly TWO TEXT lines requested
        await update.message.reply_text(
            f"MAIN BALANCE= ₹{mainb:.2f}\n"
            f"HOLD BALANCE= ₹{holdb:.2f}",
            reply_markup=balance_menu()
        )
        return


    if txt == "👤 Profile":
        mainb, holdb = get_balances(user.id)
        total, approved, rejected, canceled = get_profile_counts(user.id)
        total_ref, approved_any, total_bonus = _referral_stats(user.id)
        ratio = 0.0
        if (approved + rejected) > 0:
            ratio = (approved / float(approved + rejected)) * 100.0
        msg = (
            "👤 PROFILE\n\n"
            f"🆔 User ID: {user.id}\n"
            f"👤 Username: {user.username or user.full_name}\n\n"
            f"MAIN BALANCE= ₹{mainb:.2f}\n"
            f"HOLD BALANCE= ₹{holdb:.2f}\n\n"
            f"📌 TOTAL REGISTRATIONS: {total}\n"
            f"✅ TOTAL APPROVED REGISTRATION: {approved}\n"
            f"✖️ TOTAL REJECT REGISTERATION: {rejected}\n"
            f"🚫 TOTAL CANCELED REGISTRATION: {canceled}\n"
            f"📈 APPROVAL RATIO: {ratio:.1f}%\n\n"
            f"👥 TOTAL REFERRALS: {total_ref}\n"
            f"💰 TOTAL REFERRAL EARNED: ₹{total_bonus:.2f}"
        )
        await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("BACK 🔙", callback_data="PROFILE_BACK")]]))
        return



    if txt == "💳 Payout":
        await update.message.reply_text(
            "CHOOSE AMOYNT\n10% FEES IS APPLICABLE",
            reply_markup=payout_amounts_kb()
        )
        return

    if txt == "🧾 Balance history":
        now = int(time.time())
        thirty_days_ago = now - 30 * 24 * 3600
        con = db()
        cur = con.cursor()
        cur.execute("""
            SELECT amount, upi_or_qr, created_at, state
            FROM payouts
            WHERE user_id=? AND created_at>=?
            ORDER BY id DESC LIMIT 5
        """, (user.id, thirty_days_ago))
        rows = cur.fetchall()
        con.close()
        if not rows:
            await update.message.reply_text("Koi payout request nahi (last 30 days).")
            return
        lines = []
        for r in rows:
            snip = r["upi_or_qr"][:18] + ("..." if len(r["upi_or_qr"]) > 18 else "")
            lines.append(f"• ₹{r['amount']} | {snip} | {fmt_ts(r['created_at'])} | {r['state']}")
        await update.message.reply_text("🧾 Last 5 payout requests (last 30 days):\n\n" + "\n".join(lines))

        # Also show recent ledger entries
        ledger = get_ledger_rows(user.id, 10)
        if ledger:
            llines = []
            for e in ledger:
                dm = float(e["delta_main"] or 0)
                dh = float(e["delta_hold"] or 0)
                signm = "+" if dm >= 0 else ""
                signh = "+" if dh >= 0 else ""
                t = fmt_ts(int(e["created_at"])) if e["created_at"] else ""
                llines.append(f"• {t} | MAIN {signm}{dm:.2f} | HOLD {signh}{dh:.2f} | {e['reason']}")
            await update.message.reply_text("📈 Earnings History (Ledger):\n\n" + "\n".join(llines))
        return

    if txt == "👥 My referrals":
        bot_username = context.bot.username
        link = referral_link(bot_username, user.id)

        total_ref, total_earned, rows = get_referral_overview(user.id, limit=10)

        lines = []
        for x in rows:
            uname = x.get("username") or str(x.get("user_id"))
            joined = fmt_ts(int(x["joined_at"])) if x.get("joined_at") else "-"
            approved = int(x.get("approved_count") or 0)
            paid = "✅" if int(x.get("bonus_paid") or 0) == 1 else "⏳"
            lines.append(f"• {uname} | joined {joined} | approved {approved}/15 | bonus {paid}")

        details = "\n\n" + "\n".join(lines) if lines else "\n\n(No referrals yet)"

        msg = (
            "🔸 PER REFERRAL BONUS = ₹10 (once per referred user)\n"
            "🔸 WHEN YOUR REFERRAL COMPLETE 15 REGISTRATION"
           
                    f"🔗 Your referral    link:\n{link}\n\n"
            f"🔸 TOTAL REFERRALS: {total_ref}\n"
            f"🔸 TOTAL EARNED FROM REFERRALS: ₹{int(total_earned)}"
                     f"{details}"
        )
        await update.message.reply_text(msg)
        return


    if txt == "⚙️ Settings":
        await update.message.reply_text("Settings:", reply_markup=settings_menu())
        return

    if txt == "LANGUAGE🔤":
        await update.message.reply_text("Choose language:", reply_markup=language_menu())
        return

    if txt in ("ENGLISH 🅰️", "हिंदी ✔️", "اردو❤️"):
        if txt == "ENGLISH 🅰️":
            set_lang(user.id, "en")
            await update.message.reply_text("✅ Language set: English", reply_markup=MAIN_MENU)
        elif txt == "हिंदी ✔️":
            set_lang(user.id, "hi")
            await update.message.reply_text("✅ Language set: Hindi", reply_markup=MAIN_MENU)
        else:
            set_lang(user.id, "ur")
            await update.message.reply_text("✅ Language set: Urdu", reply_markup=MAIN_MENU)
        return

    if txt == "✅ TASK":
        await update.message.reply_text(task_menu_text(user.id))
        return
 
    if txt == "💬 Help":

        await update.message.reply_text("HELP MENU✅", reply_markup=help_menu_kb())
        return

    if txt == "🔙 Back":
        await update.message.reply_text("Main menu:", reply_markup=MAIN_MENU)
        return


    if txt == "💳 PAYOUT REQUEST":
        # Show processing payouts and allow selection
        con = db()
        cur = con.cursor()
        cur.execute("SELECT id, user_id, amount, upi_or_qr, created_at, state FROM payouts WHERE state='processing' ORDER BY id DESC LIMIT 10")
        rows = cur.fetchall()
        con.close()
        if not rows:
            await update.message.reply_text("No PROCESSING payout requests.", reply_markup=ADMIN_MENU_KB)
            return

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"#{r['id']} | ₹{r['amount']} | {r['user_id']}", callback_data=f"PAY_SEL:{r['id']}")]
            for r in rows
        ])
        await update.message.reply_text("Select a payout to process:", reply_markup=kb)
        return

    if txt == "SUBMIT THE PAYMENT PROOF 🧾":
        pid = context.user_data.get("pay_selected")
        if not pid:
            await update.message.reply_text("First select a payout from PAYOUT REQUEST.", reply_markup=ADMIN_MENU_KB)
            return
        context.user_data["admin_mode"] = "pay_proof_wait_photo"
        await update.message.reply_text("Send PAYMENT screenshot (photo).")
        return

    if txt == "📤 SEND":
        pid = context.user_data.get("pay_selected")
        proof = context.user_data.get("pay_proof", {}).get(pid) if context.user_data.get("pay_proof") else None
        if not pid:
            await update.message.reply_text("First select a payout from PAYOUT REQUEST.", reply_markup=ADMIN_MENU_KB)
            return
        if not proof:
            await update.message.reply_text("First submit proof: SUBMIT THE PAYMENT PROOF 🧾", reply_markup=PAYOUT_SUBMENU_KB)
            return

        # Load payout row
        con = db()
        cur = con.cursor()
        cur.execute("SELECT * FROM payouts WHERE id=?", (pid,))
        p = cur.fetchone()
        if not p:
            con.close()
            await update.message.reply_text("Payout not found.", reply_markup=ADMIN_MENU_KB)
            return

        # Store proof in DB
        now = int(time.time())
        cur.execute(
            "INSERT OR REPLACE INTO payout_proofs(payout_id, user_id, amount, upi_or_qr, utr, proof_file_id, created_at) VALUES(?,?,?,?,?,?,?)",
            (pid, int(p["user_id"]), int(p["amount"]), p["upi_or_qr"], proof["utr"], proof["photo_file_id"], now),
        )
        cur.execute("UPDATE payouts SET state='completed' WHERE id=?", (pid,))
        con.commit()
        con.close()

        # Build share button
        share_text = "YOUR WITHDRAWAL💲 IS SUCCESSFUL.\nTELL YOUR FRIENDS ABOUT YOUR WITHDRAWAL 💲"
        share_url = "https://t.me/share/url?text=" + share_text.replace(" ", "%20").replace("\n", "%0A")
        user_kb = InlineKeyboardMarkup([[InlineKeyboardButton("TELL YOUR FRIENDS 🫂", url=share_url)]])

        caption = (
            "YOUR WITHDRAWAL💲 IS SUCCESSFUL.\n"
            "TELL YOUR FRIENDS ABOUT YOUR WITHDRAWAL 💲\n\n"
            f"Amount: ₹{int(p['amount'])}\n"
            f"UTR: {proof['utr']}"
        )

        # Send to user
        try:
            await context.bot.send_photo(chat_id=int(p["user_id"]), photo=proof["photo_file_id"], caption=caption, reply_markup=user_kb)
        except Exception:
            # fallback to text
            await context.bot.send_message(chat_id=int(p["user_id"]), text=caption, reply_markup=user_kb)

        # Rebuild PDF table
        try:
            rebuild_payout_proofs_pdf("payout_proofs.pdf")
        except Exception:
            pass

        # Clear selection
        context.user_data["pay_selected"] = None
        await update.message.reply_text("✅ Proof sent to user and saved in payout_proofs.pdf", reply_markup=ADMIN_MENU_KB)
        return

    if txt == "📌 Pin Message":
        context.user_data["admin_mode"] = "pin_wait"
        await update.message.reply_text("Send a message (text/photo) to PIN in configured PIN_CHAT_ID.")
        return
    if txt == "📄 Download payout_proofs.pdf":
        if not os.path.exists("payout_proofs.pdf"):
            await update.message.reply_text(
                "❌ payout_proofs.pdf not found yet. Complete at least one payout proof send.",
                reply_markup=ADMIN_MENU_KB
            )
            return
        try:
            await update.message.reply_document(
                document=open("payout_proofs.pdf", "rb"),
                reply_markup=ADMIN_MENU_KB
            )
        except Exception as e:
            await update.message.reply_text(f"❌ Failed to send PDF: {e}", reply_markup=ADMIN_MENU_KB)
        return

    if txt == "🔙 Back":
        await update.message.reply_text("Main menu:", reply_markup=MAIN_MENU)
        return

# ================ GENERATORS ===============

# ----------------------------
# NEW HUMAN-LIKE NAME GENERATOR
# ----------------------------
import random
import time

VOWELS = "AEIOU"
CONSONANTS = "BCDFGHJKLMNPQRSTVWXYZ"

def random_name():
    length = random.choice([4, 5, 6, 7])
    name = ""
    for i in range(length):
        name += random.choice(CONSONANTS if i % 2 == 0 else VOWELS)
    return name.capitalize()

# ----------------------------
# REALISTIC EMAIL GENERATOR
# (not based on first name)
# ----------------------------
def random_email():
    def part(min_len, max_len):
        letters = "abcdefghijklmnopqrstuvwxyz"
        return "".join(random.choice(letters) for _ in range(random.randint(min_len, max_len)))

    first_part = part(4, 7)
    last_part  = part(4, 7)
    number = random.randint(100, 999)
    return f"{first_part}{last_part}{number}@gmail.com"

# ----------------------------
# STRONG PASSWORD (no 0, no l)
# ----------------------------

def strong_password(length=None):
    if length is None:
        length = random.choice([9,10,11,12,13,14,15])

    uppercase = "ABCDEFGHJKLMNPQRSTUVWXYZ"
    lowercase = "abcdefghijkmnopqrstuvwxyz"
    numbers   = "123456789"
    symbols   = "!@#$&"

    all_chars = uppercase + lowercase + numbers + symbols

    pwd = [
        random.choice(uppercase),
        random.choice(lowercase),
        random.choice(numbers),
        random.choice(symbols),
    ]

    pwd += random.choices(all_chars, k=length - 4)
    random.shuffle(pwd)

    return "".join(pwd)
    
# =========================
# REGISTER (THIS MUST BE ASYNC)
# =========================
async def register(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user

    # Generate preview data
    data = {
        "name": random_name(),
        "email": random_email(),
        "password": strong_password(),
    }
    temp_data[user.id] = data

    # Create DB rows (registration + action) so callbacks can work
    now = int(time.time())
    con = db()
    cur = con.cursor()

    cur.execute("""
    INSERT INTO registrations(
        user_id, first_name, last_name, email, password, created_at, state
    ) VALUES(?,?,?,?,?,?,?)
    """, (
        user.id,
        data["name"],
        "❌",
        data["email"],
        data["password"],
        now,
        "created",
    ))
    reg_id = cur.lastrowid

    expires_at = now + ACTION_TIMEOUT_HOURS * 3600
    cur.execute("""
    INSERT INTO actions(
        user_id, reg_id, created_at, expires_at, state
    ) VALUES(?,?,?,?,?)
    """, (
        user.id,
        reg_id,
        now,
        expires_at,
        "shown",
    ))
    action_id = cur.lastrowid

    con.commit()
    con.close()

    await update.message.reply_text(
        "Register account using the specified\n"
        "data and get from ₹8 to ₹10\n\n"
        f"👤 Name: {data['name']}\n\n"
        f"📧 Email: {data['email']}\n\n"
        f"🔐 Password: {data['password']}\n\n"
        "🔒 Be sure to use the specified data,\n"
        "otherwise the account will not be paid.\n\n"
        "____________________________________\n"
        "AGE CHOOSE : 1990-2007\n"
        "____________________________________\n\n"
        "GENDER : YOUR CHOICE,\n",
        parse_mode="Markdown",
        reply_markup=reg_buttons(action_id),
    )
# =========================
# CALLBACKS
# =========================


# =========================
# ADMIN: EMAIL ✉️ VERIFY QUEUE (30-day paging)
# =========================
ADMIN_EV_CB_DAY = "ADMIN_EV_DAY"              # ADMIN_EV_DAY:<days_ago>
ADMIN_EV_CB_ITEM = "ADMIN_EV_ITEM"            # ADMIN_EV_ITEM:<action_id>:<days_ago>
ADMIN_EV_CB_VER = "ADMIN_EV_VER"              # ADMIN_EV_VER:<action_id>:<days_ago>
ADMIN_EV_CB_NVER = "ADMIN_EV_NVER"            # ADMIN_EV_NVER:<action_id>:<days_ago>
ADMIN_EV_CB_REASON = "ADMIN_EV_REASON"        # ADMIN_EV_REASON:<action_id>:<days_ago>:<idx>
ADMIN_EV_CB_BACK = "ADMIN_EV_BACK"            # ADMIN_EV_BACK:<days_ago>

NOT_VERIFIED_REASONS = [
    "WRONG PASSWORD 🔑",
    "NOT COMING",
    "NOTHING",
    "PHONE VERY",
    "WE THINK 💬 YOU NOT",
]

def _day_bounds_ts(days_ago: int):
    d = (datetime.date.today() - datetime.timedelta(days=int(days_ago)))
    start = int(datetime.datetime.combine(d, datetime.time.min).timestamp())
    end = int(datetime.datetime.combine(d, datetime.time.max).timestamp())
    return d, start, end

def _admin_ev_badge(cur, action_id: int) -> str:
    cur.execute("SELECT status FROM admin_email_verify WHERE action_id=?", (int(action_id),))
    row = cur.fetchone()
    if row:
        return "VERIFIED ✅" if (row["status"] or "") == "VERIFIED" else "REJECT ✖️"
    cur.execute("SELECT state FROM actions WHERE action_id=?", (int(action_id),))
    a = cur.fetchone()
    st = (a["state"] if a else "")
    if st == "approved":
        return "VERIFIED ✅"
    if st == "rejected":
        return "REJECT ✖️"
    return "PENDING ⏳"

async def send_admin_email_verify_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, days_ago: int = 0):
    if not is_admin(update.effective_user.id):
        return
    d, start_ts, end_ts = _day_bounds_ts(days_ago)

    con = db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT a.action_id, a.user_id, a.state, a.created_at,
               r.email AS email
        FROM actions a
        LEFT JOIN registrations r ON r.id = a.reg_id
        WHERE a.created_at BETWEEN ? AND ?
        ORDER BY a.created_at DESC
        """,
        (start_ts, end_ts),
    )
    rows = cur.fetchall()

    title = f"EMAIL ✉️ VERIFY — {d.isoformat()}"

    body_lines = []
    buttons = []
    for x in rows:
        st = (x["state"] or "")
        # show only those that reached admin queue
        if st not in ("waiting_admin", "approved", "rejected"):
            continue
        badge = _admin_ev_badge(cur, int(x["action_id"]))
        email = (x["email"] or "unknown")
        body_lines.append(f"• {badge} | {email} | Action {x['action_id']}")
        buttons.append([
            InlineKeyboardButton(
                f"{badge} • {email[:22]}",
                callback_data=f"{ADMIN_EV_CB_ITEM}:{int(x['action_id'])}:{int(days_ago)}"
            )
        ])

    if not buttons:
        body = "No verify-requests found for this day."
    else:
        body = "\n".join(body_lines[:80])

    con.close()

    nav = []
    if int(days_ago) < 29:
        nav.append(InlineKeyboardButton("YESTERDAY", callback_data=f"{ADMIN_EV_CB_DAY}:{int(days_ago)+1}"))
    nav.append(InlineKeyboardButton("BACK 🔙", callback_data="ADMIN_BACK_TO_PANEL"))

    kb = InlineKeyboardMarkup(buttons + ([nav] if nav else []))

    if update.callback_query:
        await update.callback_query.edit_message_text(f"{title}\n\n{body}", reply_markup=kb)
    else:
        await update.message.reply_text(f"{title}\n\n{body}", reply_markup=kb)

async def admin_ev_show_item(update: Update, context: ContextTypes.DEFAULT_TYPE, action_id: int, days_ago: int):
    q = update.callback_query
    await q.answer()

    con = db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT a.action_id, a.user_id, a.state, a.created_at, r.email
        FROM actions a
        LEFT JOIN registrations r ON r.id=a.reg_id
        WHERE a.action_id=?
        """,
        (int(action_id),),
    )
    x = cur.fetchone()
    badge = _admin_ev_badge(cur, int(action_id))
    con.close()

    if not x:
        await q.edit_message_text("Not found.")
        return

    email = (x["email"] or "unknown")
    user_id = int(x["user_id"])
    text_msg = f"Action: {action_id}\nUser: {user_id}\nEmail: {email}\nStatus: {badge}"

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("VERIFIED ✅", callback_data=f"{ADMIN_EV_CB_VER}:{int(action_id)}:{int(days_ago)}")],
        [InlineKeyboardButton("NOT VERIFIED ❌", callback_data=f"{ADMIN_EV_CB_NVER}:{int(action_id)}:{int(days_ago)}")],
        [InlineKeyboardButton("BACK 🔙", callback_data=f"{ADMIN_EV_CB_BACK}:{int(days_ago)}")],
    ])
    await q.edit_message_text(text_msg, reply_markup=kb)

async def admin_ev_show_reasons(update: Update, context: ContextTypes.DEFAULT_TYPE, action_id: int, days_ago: int):
    q = update.callback_query
    await q.answer()
    kb_rows = [[InlineKeyboardButton(r, callback_data=f"{ADMIN_EV_CB_REASON}:{int(action_id)}:{int(days_ago)}:{i}")]
               for i, r in enumerate(NOT_VERIFIED_REASONS)]
    kb_rows.append([InlineKeyboardButton("BACK 🔙", callback_data=f"{ADMIN_EV_CB_ITEM}:{int(action_id)}:{int(days_ago)}")])
    await q.edit_message_text("Select reason:", reply_markup=InlineKeyboardMarkup(kb_rows))

def _admin_ev_set_verified(cur, action_id: int, admin_id: int):
    # Keep the existing provisional credit; mark states as approved.
    set_action_state(int(action_id), "approved")

    cur.execute("SELECT reg_id, user_id FROM actions WHERE action_id=?", (int(action_id),))
    a = cur.fetchone()
    if a:
        set_reg_state(int(a["reg_id"]), "approved")

        # Task rewards
        cur.execute("SELECT COUNT(*) AS c FROM registrations WHERE user_id=? AND state='approved'", (int(a["user_id"]),))
        approved_count = int(cur.fetchone()["c"])
        apply_task_rewards(cur, int(a["user_id"]), approved_count)

        # Referral bonus: 10 approved -> ₹10 (one-time)
        cur.execute("SELECT referrer_id FROM users WHERE user_id=?", (int(a["user_id"]),))
        ur = cur.fetchone()
        ref_id = ur["referrer_id"] if ur else None
        if ref_id:
            cur.execute("SELECT COUNT(*) AS c FROM registrations WHERE user_id=? AND state='approved'", (int(a["user_id"]),))
            c = int(cur.fetchone()["c"])
            if c >= 10:
                cur.execute("SELECT 1 FROM referral_bonuses WHERE referrer_id=? AND referred_user_id=?", (int(ref_id), int(a["user_id"])))
                already = cur.fetchone()
                if not already:
                    cur.execute(
                        "INSERT INTO referral_bonuses(referrer_id, referred_user_id, amount, created_at) VALUES(?,?,?,?)",
                        (int(ref_id), int(a["user_id"]), 10.0, int(time.time())),
                    )
                    cur.execute("UPDATE users SET main_balance=main_balance+10 WHERE user_id=?", (int(ref_id),))
                    add_ledger_entry(int(ref_id), delta_main=10.0, reason="Referral bonus")

    cur.execute(
        "INSERT OR REPLACE INTO admin_email_verify(action_id, decided_by, status, reason, decided_at) VALUES(?,?,?,?,?)",
        (int(action_id), int(admin_id), "VERIFIED", None, int(time.time())),
    )

def _admin_ev_set_not_verified(cur, action_id: int, admin_id: int, reason: str):
    # Revert provisional HOLD credit
    cur.execute("SELECT * FROM actions WHERE action_id=?", (int(action_id),))
    a = cur.fetchone()
    if a:
        cur.execute("SELECT hold_credit_id, amount, reverted FROM precredits WHERE action_id=?", (int(action_id),))
        pc = cur.fetchone()
        if pc and int(pc["reverted"] or 0) == 0:
            try:
                revert_hold_credit(int(pc["hold_credit_id"]), int(a["user_id"]), float(pc["amount"]))
            except Exception:
                pass
            cur.execute("UPDATE precredits SET reverted=1 WHERE action_id=?", (int(action_id),))

        set_action_state(int(action_id), "rejected")
        set_reg_state(int(a["reg_id"]), "rejected")

    cur.execute(
        "INSERT OR REPLACE INTO admin_email_verify(action_id, decided_by, status, reason, decided_at) VALUES(?,?,?,?,?)",
        (int(action_id), int(admin_id), "NOT_VERIFIED", str(reason), int(time.time())),
    )

async def callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    try:
        await q.answer(cache_time=0)
    except Exception:
        pass

    user = update.effective_user
    if user.id != ADMIN_ID and is_blocked(user.id):
        return

    ensure_user(user.id, user.username or user.full_name)
    moved = move_matured_hold_to_main(user.id)
    data = q.data or ""

    # =========================
    # ADMIN: EMAIL ✉️ VERIFY (new menu)
    # =========================
    if data.startswith(f"{ADMIN_EV_CB_DAY}:"):
        if not is_admin(user.id):
            return
        days_ago = int(data.split(":")[1])
        await send_admin_email_verify_menu(update, context, days_ago=days_ago)
        return

    if data.startswith(f"{ADMIN_EV_CB_BACK}:"):
        if not is_admin(user.id):
            return
        days_ago = int(data.split(":")[1])
        await send_admin_email_verify_menu(update, context, days_ago=days_ago)
        return

    if data.startswith(f"{ADMIN_EV_CB_ITEM}:"):
        if not is_admin(user.id):
            return
        parts = data.split(":")
        action_id = int(parts[1])
        days_ago = int(parts[2]) if len(parts) > 2 else 0
        await admin_ev_show_item(update, context, action_id, days_ago)
        return

    if data.startswith(f"{ADMIN_EV_CB_VER}:"):
        if not is_admin(user.id):
            return
        parts = data.split(":")
        action_id = int(parts[1])
        days_ago = int(parts[2]) if len(parts) > 2 else 0

        con = db()
        cur = con.cursor()
        _admin_ev_set_verified(cur, action_id, user.id)
        con.commit()
        con.close()

        # no user message on verify
        await q.edit_message_text("✅ VERIFIED ✅ (approved)")
        # go back to list
        await send_admin_email_verify_menu(update, context, days_ago=days_ago)
        return

    if data.startswith(f"{ADMIN_EV_CB_NVER}:"):
        if not is_admin(user.id):
            return
        parts = data.split(":")
        action_id = int(parts[1])
        days_ago = int(parts[2]) if len(parts) > 2 else 0
        await admin_ev_show_reasons(update, context, action_id, days_ago)
        return

    if data.startswith(f"{ADMIN_EV_CB_REASON}:"):
        if not is_admin(user.id):
            return
        parts = data.split(":")
        action_id = int(parts[1])
        days_ago = int(parts[2])
        idx = int(parts[3])
        reason = NOT_VERIFIED_REASONS[idx] if 0 <= idx < len(NOT_VERIFIED_REASONS) else "UNKNOWN"

        # Apply reject + revert precredit
        con = db()
        cur = con.cursor()
        # fetch user_id for messaging
        cur.execute("SELECT user_id FROM actions WHERE action_id=?", (action_id,))
        rowu = cur.fetchone()
        target_user = int(rowu["user_id"]) if rowu else None

        _admin_ev_set_not_verified(cur, action_id, user.id, reason)
        con.commit()
        con.close()

        # notify user with reason + deduct amount
        if target_user:
            try:
                await context.bot.send_message(
                    chat_id=target_user,
                    text=(
                        "WRONG PASSWORD\n\n"
                        f"REASON= {reason}\n"
                        f"DEDUCT MONEY = {PRE_CREDIT_AMOUNT}\n\n"
                        "PLEASE CHECK IT ONCE 🔂 YOUR SELF"
                    ),
                )
            except Exception:
                pass

        await send_admin_email_verify_menu(update, context, days_ago=days_ago)
        return

    if data == "ADMIN_BACK_TO_PANEL":
        if not is_admin(user.id):
            return
        # Hide current menu message and show admin panel again
        try:
            await q.message.delete()
        except Exception:
            pass
        try:
            await context.bot.send_message(chat_id=user.id, text="Admin Panel:", reply_markup=ADMIN_MENU_KB)
        except Exception:
            pass
        return
    
    if data == "PROFILE_BACK":
        # Hide profile message and show main menu
        try:
            await q.message.delete()
        except Exception:
            try:
                await q.edit_message_text(" ")
            except Exception:
                pass
        try:
            await context.bot.send_message(chat_id=user.id, text="✅ Back", reply_markup=MAIN_MENU)
        except Exception:
            pass
        return

    if moved > 0:
        try:
            await context.bot.send_message(chat_id=user.id, text="💸 Accrual of funds to the balance")
        except Exception:
            pass

    # Admin: Daily Total Approval (Today/Yesterday picker)
    if data in ("ADMIN_DAILY_TOTAL_APPROVAL", "ADMIN_DAILY_TOTAL_TODAY", "ADMIN_DAILY_TOTAL_YESTERDAY"):
        if not is_admin(user.id):
            return
        # show picker
        if data == "ADMIN_DAILY_TOTAL_APPROVAL":
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("📅 Today", callback_data="ADMIN_DAILY_TOTAL_TODAY"),
                InlineKeyboardButton("⏮️ Yesterday", callback_data="ADMIN_DAILY_TOTAL_YESTERDAY"),
            ]])
            await q.edit_message_text("☄️ Daily Total Approval\nChoose date:", reply_markup=kb)
            return

        # compute totals
        con = db()
        cur = con.cursor()
        if data == "ADMIN_DAILY_TOTAL_TODAY":
            total = daily_total_approval_for_offset(cur, 0)
            label = "Today"
        else:
            total = daily_total_approval_for_offset(cur, 1)
            label = "Yesterday"
        con.close()

        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("⬅️ Back", callback_data="ADMIN_DAILY_TOTAL_APPROVAL"),
        ]])
        await q.edit_message_text(f"☄️ Daily Total Approval ({label}): {total}", reply_markup=kb)
        return
    # Help menu (9 buttons)
    if data == "HELP_BACK":
        await q.edit_message_text("HELP MENU✅", reply_markup=help_menu_kb())
        return
    if re.fullmatch(r"HELP_[1-6]", data or ""):
        txt = HELP_TEXTS.get(data, "Help info not found.")
        await q.edit_message_text(txt, reply_markup=help_back_kb())
        return
        
    # Channel join check
    if data == "CHK_JOIN":
        # Always answer callback first (prevents timeout / stuck)
        try:
            await q.answer("⏳ Checking...", show_alert=False)
        except Exception:
            pass

        ok = await user_in_required_channels(context, user.id)

        if not ok:
            try:
                await q.answer("❌ Pehle sab channel join karo", show_alert=True)
            except Exception:
                pass
            await q.message.reply_text(
                "{😊 FIRST PLEASE JOIN THIS CHANNEL ✅}",
                reply_markup=join_channels_kb()
            )
            return

        # ✅ Joined -> remove inline buttons + show main menu
        try:
            await q.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass

        # Apply pending referral (saved when user clicked /start before joining channels)
        pending_ref = pop_pending_ref(user.id)
        if pending_ref:
            try:
                ensure_user(user.id, user.username or user.full_name, referrer_id=pending_ref)
            except Exception:
                pass

        await q.message.reply_text(
            "✅ Channel join verified successfully",
            reply_markup=MAIN_MENU
        )
        return
    # Tutorial videos (always clickable)
    if data == "VID_CREATE":
        await send_create_account_video(context, user.id)
        return

    if data == "VID_LOGOUT":
        await send_logout_video(context, user.id)
        return

    # Admin selects a payout to process
    if data.startswith("PAY_SEL:"):
        if not is_admin(user.id):
            return
        pid = int(data.split(":")[1])
        context.user_data["pay_selected"] = pid
        await q.message.reply_text(f"✅ Selected payout #{pid}. Now choose an action:", reply_markup=PAYOUT_SUBMENU_KB)
        return


    # B) Accounts pagination
    if data.startswith("ACC:"):
        offset = int(data.split(":")[1])
        con = db()
        cur = con.cursor()
        cur.execute("""
            SELECT COUNT(*) AS c
            FROM actions a
            JOIN registrations r ON r.id=a.reg_id
            WHERE a.user_id=? AND a.state IN ('waiting_admin','approved','rejected','canceled')
        """, (user.id,))
        total = int(cur.fetchone()["c"])

        cur.execute("""
            SELECT r.email AS email,
                   a.state AS astate,
                   COALESCE(a.updated_at, a.created_at, r.updated_at, r.created_at) AS stime
            FROM actions a
            JOIN registrations r ON r.id=a.reg_id
            WHERE a.user_id=? AND a.state IN ('waiting_admin','approved','rejected','canceled')
            ORDER BY COALESCE(a.updated_at, a.created_at) DESC
            LIMIT 5 OFFSET ?
        """, (user.id, offset))
        rows = cur.fetchall()
        con.close()

        page = offset // 5 + 1
        lines = []
        for rr in rows:
            status = ("approved" if rr["astate"] == "approved" else ("rejected" if rr["astate"] == "rejected" else ("canceled" if rr["astate"] == "canceled" else "processing")))
            lines.append(f"• {rr['email']}  —  {status}  —  {fmt_ts(rr['stime'])}")

        msg = f"📋 My accounts (page {page}):\n\n" + "\n".join(lines)
        await q.edit_message_text(msg, reply_markup=accounts_nav(offset, total))
        return

    # C) Payout amount selection
    if data.startswith("PAY_AMT:"):
        amt = int(data.split(":")[1])

        allowed = (55, 110, 210, 310, 510, 1050)
        if amt not in allowed:
            await q.message.reply_text("Invalid amount.", reply_markup=MAIN_MENU)
            return

        mainb, _holdb = get_balances(user.id)
        if float(mainb) < float(amt):
            await q.message.reply_text("BALANCE IS NOT SUFFICIENT FOR WITHDRAWAL 💲", reply_markup=MAIN_MENU)
            return

        context.user_data["await_upi"] = True
        context.user_data["payout_amt"] = amt
        await q.message.reply_text("PLEASE ENTER YOUR UPI ID OR QR CODE")
        return


    # A) Register buttons
    if data.startswith("REG_DONE:") or data.startswith("REG_CANCEL:") or data.startswith("REG_CANCEL_SURE:") or data.startswith("REG_CONFIRM:"):
        action_id = int(data.split(":")[1])
        ok, a = action_valid(action_id)

        # timeout
        if not ok:
            # After 20 hours: show TIME OUT on the same message and remove buttons
            if a and int(time.time()) > int(a["expires_at"]):
                try:
                    txt0 = q.message.text or ""
                    if "TIME OUT" not in txt0:
                        txt0 = txt0 + "\n\n⏰ TIME OUT" 
                    await q.edit_message_text(txt0, reply_markup=None)
                except Exception:
                    try:
                        await q.edit_message_reply_markup(reply_markup=None)
                    except Exception:
                        pass
                set_action_state(action_id, "timeout")
                set_reg_state(a["reg_id"], "timeout")
                return
            await q.answer("Please wait…", show_alert=False)
            return


        # only owner can click
        if a["user_id"] != user.id:
            return

        if data.startswith("REG_CANCEL_SURE:"):

            # cancel ONLY when user confirms here

            set_action_state(action_id, "canceled")

            set_reg_state(a["reg_id"], "canceled")


            # ✅ Edit same registration message: append canceled marker + remove buttons

            try:

                txt0 = q.message.text or ""

                if "CANCELED ✖️ REGISTRATION" not in txt0:

                    txt0 = txt0 + "\n_________________________________\nCANCELED ✖️ REGISTRATION"

                await q.edit_message_text(txt0, reply_markup=None)

            except Exception:

                try:

                    await q.edit_message_reply_markup(reply_markup=None)

                except Exception:

                    pass

            return


        if data.startswith("REG_CANCEL:"):
            # show cancel confirmation buttons (do not cancel immediately)
            set_action_state(action_id, "canceled_prompt")
            try:
                await q.edit_message_reply_markup(reply_markup=cancel_confirm_buttons(action_id))
            except Exception:
                pass
            return

        if data.startswith("REG_DONE:"):
            # After DONE: edit SAME message text (rebuild Markdown to preserve monospace)
            set_action_state(action_id, "done1")

            # Start cooldown timer from DONE click (so if user waits 50s before first confirm, it runs immediately)
            ts_key = f"confirm_ts_{action_id}"
            ready_key = f"confirm_ready_{action_id}"
            context.user_data[ts_key] = int(time.time())
            context.user_data[ready_key] = False

            # Load registration from DB (so we can rebuild the original formatted text)
            con = db()
            cur = con.cursor()
            cur.execute("SELECT * FROM registrations WHERE id=?", (a["reg_id"],))
            r = cur.fetchone()
            con.close()

            # Safety: avoid breaking Markdown if data contains backticks
            def _safe_code(s: str) -> str:
                s = (s or "").strip()
                return s.replace("`", "'")

            first_name = _safe_code(r["first_name"] if r else "")
            last_name  = _safe_code(r["last_name"] if r else "")
            name = (first_name + " " + last_name).strip()
           
            email = _safe_code(r["email"] if r else "")
            password = _safe_code(r["password"] if r else "")

            base_text = (
                "Register account using the specified\n"
                "data and get from ₹8 to ₹10\n\n"
                f"👤 Name: `{name}`\n\n"
                f"📧 Email: `{email}`\n\n"
                f"🔐 Password: `{password}`\n\n"
                "🔐 Be sure to use the specified data,\n"
                "otherwise the account will not be paid"
            )

            # Append Recovery email note (email monospace for easy copy)
            recovery_email = "aadiltyagi459@gmail.com"
            base_text += (
                "\n________________________\n\n"
                "🚦 You need to add Recovery email\n"
                f"`{recovery_email}`\n"
            )

            try:
                await q.edit_message_text(
                    text=base_text,
                    parse_mode="Markdown",
                    reply_markup=confirm_again_button(action_id),
                )
            except Exception:
                # fallback: at least update buttons
                try:
                    await q.edit_message_reply_markup(
                        reply_markup=confirm_again_button(action_id)
                    )
                except Exception:
                    pass

            return
            
        if data.startswith("REG_CONFIRM:"):
            # CONFIRM AGAIN: show effect in the SAME message, then enforce a 50s cooldown
            # Real check will run only after cooldown (prevents spam clicks without action)

            # Load registration    
            con = db()    
            cur = con.cursor()    
            cur.execute("SELECT * FROM registrations WHERE id=?", (a["reg_id"],))    
            r = cur.fetchone()    
            con.close()    

            email = (r["email"] or "").strip()            # Original registration message id (jump target)    
            target_msg_id = q.message.message_id    
            chat_id = q.message.chat_id    

            # Always create a NEW result message (reply) on every tap (jump via reply header)    
            confirm_msg_id = None    
            try:    
                sent = await context.bot.send_message(    
                    chat_id=chat_id,    
                    text=f"\n\n🔍 EMAIL CHECKING...\n[{_confirm_bar(0)}] 0%",    
                    reply_to_message_id=target_msg_id,    
                )    
                confirm_msg_id = sent.message_id    
            except Exception:    
                confirm_msg_id = None    

            # Progress effect (edit separate message)    
            try:    
                await q.answer()    
            except Exception:    
                pass    

            if confirm_msg_id:    
                try:    
                    await animate_confirm_effect_msg(    
                        context.bot,    
                        chat_id,    
                        confirm_msg_id,    
                        "",    
                        action_id,    
                        keep_buttons=False    
                    )    
                except Exception:    
                    pass    

            # Cooldown gating (ONLY ONCE per action_id)    
            now = int(time.time())    
            ts_key = f"confirm_ts_{action_id}"    
            ready_key = f"confirm_ready_{action_id}"    
            first_ts = context.user_data.get(ts_key)    
            is_ready = bool(context.user_data.get(ready_key, False))    

            if not first_ts:    
                context.user_data[ts_key] = now    
                first_ts = now    

            if not is_ready:    
                elapsed = now - int(first_ts)    
                if elapsed < CONFIRM_COOLDOWN_SEC:    
                    remain = CONFIRM_COOLDOWN_SEC - elapsed    
                    try:    
                        await _edit_message_safe(    
                            context.bot,    
                            chat_id,    
                            confirm_msg_id,    
                            f"ERROR"    
                        )    
                    except Exception:    
                        pass    
                    return    
                else:    
                    context.user_data[ready_key] = True    

    

            # After cooldown: perform REAL check
            handle = (email.split("@")[0] if "@" in email else email).strip()

            ok = await asyncio.to_thread(_pg_email_handle_exists, handle)

            if not ok:
                # keep action active for retry
                set_action_state(action_id, "done1")
                set_reg_state(a["reg_id"], "created")

                try:
                    await _edit_message_safe(
                        context.bot,
                        chat_id,
                        confirm_msg_id,
                        f"\n\n🚫 it seems you haven't add recoverey email in this `{handle}@gmail.com`\n\n"
                    )
                except Exception:
                    pass

                # Back to original message with confirm button still active
                try:
                    await q.edit_message_reply_markup(
                        reply_markup=confirm_again_button(action_id)
                    )
                except Exception:
                    pass
                return
                
            # RIGHT flow    
            set_action_state(action_id, "waiting_admin")    
            set_reg_state(a["reg_id"], "confirmed_by_user")    

            try:    
                await _edit_message_safe(    
                    context.bot,    
                    chat_id,    
                    confirm_msg_id,    
                    "\n\n✅ DONE CHECK YOUR BALANCE "    
                )    
            except Exception:    
                pass
            # Log successful email-check (✅ right) for admin daily totals
            try:
                con_ec = db()
                cur_ec = con_ec.cursor()
                cur_ec.execute(
                    "INSERT INTO email_checks(user_id, reg_id, action_id, email, ok, created_at) VALUES(?,?,?,?,1,?)",
                    (int(user.id), int(a["reg_id"]), int(a["action_id"]), str(email).lower(), int(time.time())),
                )
                con_ec.commit()
                con_ec.close()
            except Exception:
                try:
                    con_ec.close()
                except Exception:
                    pass
# Safety: avoid breaking Markdown if data contains backticks
            def _safe_code(s: str) -> str:
                s = (s or "").strip()
                return s.replace("`", "'")

            first_name = _safe_code(r["first_name"] if r else "")
            last_name  = _safe_code(r["last_name"] if r else "")
            name = (first_name + " " + last_name).strip()
           
            email = _safe_code(r["email"] if r else "")
            password = _safe_code(r["password"] if r else "")

            base_text = (
                "Register account using the specified\n"
                "data and get from ₹8 to ₹10\n\n"
                f"👤 Name: `{name}`\n\n"
                f"📧 Email: `{email}`\n\n"
                f"🔐 Password: `{password}`\n\n"
                "🔐 Be sure to use the specified data,\n"
                "otherwise the account will not be paid"
            )

            # Append Recovery email note (email monospace for easy copy)
            recovery_email = "aadiltyagi459@gmail.com"
            base_text += (
                "\n________________________\n\n"
                "🚦 You need to add Recovery email\n\n"
                "Funds will be transferred to the main balance after 1-day hold.\n\n"
                "🚨 Be sure to LOG OUT of account on your device\n"
                f"`{recovery_email}`\n"
            )

            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("📲 How to logout of account ?", callback_data="VID_LOGOUT")
            ]])

            try:
                await q.edit_message_text(
                    text=base_text,
                    parse_mode="Markdown",
                    reply_markup=kb
                )
            except Exception:
                pass

            try:
                await _edit_message_safe(context.bot, chat_id, confirm_msg_id, base_text, reply_markup=kb)
            except Exception:
                pass

            await send_logout_video(context, user.id)
            returnk
    # =========================
    # ADMIN: Registration Accept/Reject
    # =========================
    if data.startswith("ADM_REG_ACCEPT:") or data.startswith("ADM_REG_REJECT:"):
        if not is_admin(user.id):
            return

        action_id = int(data.split(":")[1])
        con = db()
        cur = con.cursor()
        cur.execute("SELECT * FROM actions WHERE action_id=?", (action_id,))
        a = cur.fetchone()
        if not a:
            con.close()
            await q.message.reply_text("Not found.")
            return

        cur.execute("SELECT * FROM registrations WHERE id=?", (a["reg_id"],))
        r = cur.fetchone()

        if data.startswith("ADM_REG_ACCEPT:"):
            # HOLD already credited at user-confirm time (provisional).
            # If, for some reason, it was not credited, credit it now.
            cur.execute("SELECT hold_credit_id, amount, reverted FROM precredits WHERE action_id=?", (action_id,))
            pc = cur.fetchone()
            if not pc:
                hid = add_hold_credit(a["user_id"], PRE_CREDIT_AMOUNT)
                cur.execute(
                    "INSERT INTO precredits(action_id, user_id, hold_credit_id, amount, created_at, reverted) VALUES(?,?,?,?,?,0)",
                    (action_id, a["user_id"], hid, float(PRE_CREDIT_AMOUNT), int(time.time())),
                )
            elif int(pc["reverted"]) == 1:
                # was reverted earlier; re-credit on accept
                hid = add_hold_credit(a["user_id"], float(pc["amount"]))
                cur.execute(
                    "UPDATE precredits SET hold_credit_id=?, reverted=0 WHERE action_id=?",
                    (hid, action_id),
                )

            set_action_state(action_id, "approved")
            set_reg_state(a["reg_id"], "approved")

            # Task rewards: pay milestones based on approved registrations count
            cur.execute("SELECT COUNT(*) AS c FROM registrations WHERE user_id=? AND state=\'approved\'", (a["user_id"],))
            approved_count = int(cur.fetchone()["c"])
            paid_task = apply_task_rewards(cur, a["user_id"], approved_count)

                        # Referral bonus tracking (₹10 after 10 approved regs of referred user)
            cur.execute("SELECT referrer_id FROM users WHERE user_id=?", (a["user_id"],))
            ur = cur.fetchone()
            ref_id = ur["referrer_id"] if ur else None

            if ref_id:
                # count approved regs for this referred user
                cur.execute(
                    "SELECT COUNT(*) AS c FROM registrations WHERE user_id=? AND state='approved'",
                    (a["user_id"],),
                )
                c = int(cur.fetchone()["c"])

                if c >= 10:
                    # Pay only once per (referrer, referred) pair
                    cur.execute(
                        "SELECT 1 FROM referral_bonuses WHERE referrer_id=? AND referred_user_id=?",
                        (ref_id, a["user_id"]),
                    )
                    already = cur.fetchone()
                    if not already:
                        cur.execute(
                            "INSERT INTO referral_bonuses(referrer_id, referred_user_id, amount, created_at) VALUES(?,?,?,?)",
                            (ref_id, a["user_id"], 10.0, int(time.time())),
                        )
                        cur.execute(
                            "UPDATE users SET main_balance=main_balance+10 WHERE user_id=?",
                            (ref_id,),
                        )
                        add_ledger_entry(int(ref_id), delta_main=10.0, reason="Referral bonus")

            # Notify user about newly credited task rewards
            if paid_task > 0:
                try:
                    await context.bot.send_message(chat_id=a["user_id"], text=f"🎁 Task reward added to MAIN: ₹{int(paid_task)}")
                except Exception:
                    pass

            con.commit()
            con.close()

            await q.edit_message_text("✅ Accepted. HOLD credited (matures to MAIN after 2 days).")
            await context.bot.send_message(chat_id=a["user_id"], text="✅ Admin accepted your registration. HOLD BALANCE updated.")
            await context.bot.send_message(chat_id=a["user_id"], text="💡 Tip: Please LOG OUT of the account on your device and wait for HOLD to mature into MAIN.")
        else:
            # Revert provisional HOLD credit (if it was added on confirm)
            cur.execute("SELECT hold_credit_id, amount, reverted FROM precredits WHERE action_id=?", (action_id,))
            pc = cur.fetchone()
            if pc and int(pc["reverted"] or 0) == 0:
                try:
                    revert_hold_credit(int(pc["hold_credit_id"]), int(a["user_id"]), float(pc["amount"]))
                except Exception:
                    pass
                cur.execute("UPDATE precredits SET reverted=1 WHERE action_id=?", (action_id,))

            set_action_state(action_id, "rejected")
            set_reg_state(a["reg_id"], "rejected")
            con.commit()
            con.close()

            await q.edit_message_text("❌ Rejected.")
            await context.bot.send_message(chat_id=a["user_id"], text="❌ Admin rejected your registration.")
            await context.bot.send_message(chat_id=a["user_id"], text="💡 Tip: Check EMAIL/PASSWORD and try again with correct details.")
            return

    # =========================
    # ADMIN: Payout Accept/Reject (from panel list)
    # =========================
    if data.startswith("ADM_PAY_ACCEPT:") or data.startswith("ADM_PAY_REJECT:"):
        if not is_admin(user.id):
            return
        pid = int(data.split(":")[1])
        con = db()
        cur = con.cursor()
        cur.execute("SELECT * FROM payouts WHERE id=?", (pid,))
        p = cur.fetchone()
        if not p:
            con.close()
            await q.message.reply_text("Payout not found.")
            return
        if data.startswith("ADM_PAY_ACCEPT:"):
            # Mark approved (funds already reserved/deducted at request time)
            cur.execute("UPDATE payouts SET state='processing', reserved=0 WHERE id=?", (pid,))
            con.commit()
            con.close()
            await q.edit_message_text("✅ Payout moved to PROCESSING.")
            await context.bot.send_message(chat_id=p["user_id"], text="✅ Your payout request is now PROCESSING.")
            return
        else:
            # Refund if we had reserved funds and not refunded yet
            reserved = int(p["reserved"]) if "reserved" in p.keys() and p["reserved"] is not None else 0
            refunded = int(p["refunded"]) if "refunded" in p.keys() and p["refunded"] is not None else 0
            if reserved == 1 and refunded == 0:
                cur.execute("UPDATE users SET main_balance = main_balance + ? WHERE user_id=?", (int(p["amount"]), int(p["user_id"])))
                cur.execute("UPDATE payouts SET state='rejected', refunded=1, reserved=0 WHERE id=?", (pid,))
            else:
                cur.execute("UPDATE payouts SET state='rejected' WHERE id=?", (pid,))
            con.commit()
            con.close()
            await q.edit_message_text("❌ Payout rejected.")
            await context.bot.send_message(chat_id=p["user_id"], text="❌ Your payout request rejected.")
            return

# =========================
# UPI INPUT HANDLER
# =========================
async def upi_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id != ADMIN_ID and is_blocked(user.id):
        return

    ensure_user(user.id, user.username or user.full_name)
    moved = move_matured_hold_to_main(user.id)
    if moved > 0:
        try:
            await context.bot.send_message(chat_id=user.id, text="💸 Accrual of funds to the balance")
        except Exception:
            pass

    if not context.user_data.get("await_upi"):
        return

    upi = (update.message.text or "").strip()
    amt = int(context.user_data.get("payout_amt", 0))
    context.user_data["await_upi"] = False
    context.user_data["payout_amt"] = 0

    if amt not in (55,110,210,310,510,1050):
        await update.message.reply_text("Invalid amount.", reply_markup=MAIN_MENU)
        return

    # Check balance again (safety)
    mainb, _holdb = get_balances(user.id)
    if float(mainb) < float(amt):
        await update.message.reply_text("BALANCE IS NOT SUFFICIENT FOR WITHDRAWAL 💲", reply_markup=MAIN_MENU)
        return

    kind = classify_upi_or_qr(upi)
    if kind == "upi":
        if not (upi.lower().startswith("upi://") or is_valid_upi_id(upi)):
            await update.message.reply_text("❌ INVALID UPI ID. Example: name@bank", reply_markup=MAIN_MENU)
            return
        if is_upi_or_qr_used(upi, "upi", user.id):
            await update.message.reply_text("THIS UPI ID IS ALREADY USED ✅", reply_markup=MAIN_MENU)
            return
    else:
        if len(upi) < 10:
            await update.message.reply_text("❌ INVALID QR CODE", reply_markup=MAIN_MENU)
            return
        if is_upi_or_qr_used(upi, "qr", user.id):
            await update.message.reply_text("THIS QR CODE IS ALREADY USED ✅", reply_markup=MAIN_MENU)
            return

    now = int(time.time())
    con = db()
    cur = con.cursor()

    # Reserve funds immediately to prevent double-withdraw
    cur.execute("BEGIN")
    cur.execute(
        "UPDATE users SET main_balance = main_balance - ? WHERE user_id=? AND main_balance >= ?",
        (amt, user.id, amt)
    )
    if cur.rowcount != 1:
        con.rollback()
        con.close()
        await update.message.reply_text("BALANCE IS NOT SUFFICIENT FOR WITHDRAWAL 💲", reply_markup=MAIN_MENU)
        return

    cur.execute(
        "INSERT INTO payouts(user_id, amount, upi_or_qr, created_at, state, reserved, refunded) VALUES(?,?,?,?,?,?,?)",
        (user.id, amt, (upi.lower() if kind=="upi" else upi), now, "pending", 1, 0)
    )
    pid = cur.lastrowid
    con.commit()
    con.close()

    # Notify admin with accept/reject buttons
    admin_kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ ACCEPT", callback_data=f"ADM_PAY_ACCEPT:{pid}"),
        InlineKeyboardButton("❌ REJECT", callback_data=f"ADM_PAY_REJECT:{pid}")
    ]])
    await context.bot.send_message(
        chat_id=ADMIN_ID,
        text=(
            "💳 NEW PAYOUT REQUEST\n\n"
            f"User ID: {user.id}\n"
            f"Name: {user.username or user.full_name}\n"
            f"Amount: ₹{amt}\n"
            f"UPI/QR: {upi}\n"
            f"Time: {fmt_ts(now)}"
        ),
        reply_markup=admin_kb
    )

    await update.message.reply_text("YOUR PAYOUT REQUEST IS SENT 📤 TO MY PAYMENT DEPARTMENT 🏬", reply_markup=MAIN_MENU)

# =========================
# ADMIN PANEL
# =========================
ADMIN_MENU_KB = ReplyKeyboardMarkup(
    [
["📢 Broadcast Text", "🔗 Broadcast Link"],
        ["🖼️ Broadcast Image", "🖼️ Image + Link"],
        ["🗃️ Broadcast File", "👤 Personal Message"],
        ["⛔ Block User", "✅ Unblock User"],
        ["💳 PAYOUT REQUEST", "📌 Pin Message"],
        ["📄 Download payout_proofs.pdf"],
        ["🔝 TOP 50 DAILY USER", "🔝 TOP 50 MONTHLY USER"],
        ["🎭ALL USER", "ADD OR DEDUCT BALANCE ♎"],
        ["EMAIL ✉️ VERIFY"],
        ["🔙 Back"],
    ],
    resize_keyboard=True
)

PAYOUT_SUBMENU_KB = ReplyKeyboardMarkup(
    [
        ["SUBMIT THE PAYMENT PROOF 🧾", "📤 SEND"],
        ["🔙 Back"],
    ],
    resize_keyboard=True
)

async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user(user.id, user.username or user.full_name)
    if not is_admin(user.id):
        await update.message.reply_text("Access denied.")
        return
    await update.message.reply_text("ADMIN PANEL:", reply_markup=ADMIN_MENU_KB)


def _start_of_day_ts(ts: int) -> int:
    t = time.localtime(ts)
    return int(time.mktime((t.tm_year, t.tm_mon, t.tm_mday, 0,0,0, t.tm_wday, t.tm_yday, t.tm_isdst)))

def _start_of_month_ts(ts: int) -> int:
    t = time.localtime(ts)
    return int(time.mktime((t.tm_year, t.tm_mon, 1, 0,0,0, t.tm_wday, t.tm_yday, t.tm_isdst)))

def admin_top_users(period: str = "daily", limit: int = 50):
    """Top users by number of requests sent to admin (actions that reached waiting_admin/approved/rejected)."""
    now = int(time.time())
    if period == "monthly":
        start = _start_of_month_ts(now)
        title = "🔝 TOP 50 MONTHLY USER"
    else:
        start = _start_of_day_ts(now)
        title = "🔝 TOP 50 DAILY USER"

    con = db(); cur = con.cursor()
    cur.execute(
        """
        SELECT a.user_id, COALESCE(u.username, '') AS username, COUNT(*) AS c
        FROM actions a
        LEFT JOIN users u ON u.user_id = a.user_id
        WHERE a.created_at >= ? AND a.state IN ('waiting_admin','approved','rejected','canceled')
        GROUP BY a.user_id
        ORDER BY c DESC
        LIMIT ?
        """,
        (start, int(limit)),
    )
    rows = cur.fetchall(); con.close()
    lines = [title, f"From: {datetime.fromtimestamp(start).strftime('%Y-%m-%d %H:%M')}"]
    if not rows:
        lines.append("(no data)")
        return "\n".join(lines)

    for i, r in enumerate(rows, 1):
        uname = (r["username"] or "").strip()
        label = uname if uname else str(r["user_id"])
        lines.append(f"{i}. {label} | ID {r['user_id']} | requests {r['c']}")
    return "\n".join(lines)

def admin_list_users(limit: int = 200):
    con = db(); cur = con.cursor()
    cur.execute("SELECT user_id, username, main_balance, hold_balance FROM users ORDER BY created_at DESC LIMIT ?", (int(limit),))
    rows = cur.fetchall(); con.close()
    return rows

def admin_total_users() -> int:
    con = db(); cur = con.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM users")
    row = cur.fetchone()
    con.close()
    try:
        return int(row["c"])
    except Exception:
        return int(row[0] if row else 0)

def admin_find_user(query: str):
    q = (query or "").strip()
    con = db(); cur = con.cursor()
    if q.isdigit():
        cur.execute("SELECT user_id, username, main_balance, hold_balance FROM users WHERE user_id=? LIMIT 1", (int(q),))
        r = cur.fetchone(); con.close()
        return r
    # username partial
    cur.execute("SELECT user_id, username, main_balance, hold_balance FROM users WHERE username LIKE ? ORDER BY created_at DESC LIMIT 10", (f"%{q}%",))
    rows = cur.fetchall(); con.close()
    return rows


async def admin_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_admin(user.id):
        return

    txt = (update.message.text or "").strip()

    if txt == "EMAIL ✉️ VERIFY":
        # Show today's verification queue (inline)
        await send_admin_email_verify_menu(update, context, days_ago=0)
        return

    # Top users
    if txt == "🔝 TOP 50 DAILY USER":
        await update.message.reply_text(admin_top_users("daily", 50))
        return

    if txt == "🔝 TOP 50 MONTHLY USER":
        await update.message.reply_text(admin_top_users("monthly", 50))
        return

    # All users list + search
    if txt == "🎭ALL USER":
        rows = admin_list_users(200)
        if not rows:
            await update.message.reply_text("No users found.")
            return
        total = admin_total_users()
        lines = [f"🎭 ALL USER (Total: {total})", "Send username/userid to search 🔍 (type now)", ""]
        for i, r in enumerate(rows[:50], start=1):
            uname = (r['username'] or '').strip()
            lines.append(f"{i}. {r['user_id']} | {uname} | MAIN ₹{float(r['main_balance']):.2f} | HOLD ₹{float(r['hold_balance']):.2f}")
        await update.message.reply_text("\n".join(lines))
        context.user_data["admin_mode"] = "all_users_search"
        return

    if txt == "ADD OR DEDUCT BALANCE ♎":
        context.user_data["admin_mode"] = "bal_select"
        await update.message.reply_text("Send USERID or username to select user:")
        return

    if txt == "📢 Broadcast Text":
        context.user_data["admin_mode"] = "bc_text"
        await update.message.reply_text("Send text to broadcast:")
        return

    if txt == "🔗 Broadcast Link":
        context.user_data["admin_mode"] = "bc_link"
        await update.message.reply_text("Send link to broadcast (https://...):")
        return

    if txt == "🖼️ Broadcast Image":
        context.user_data["admin_mode"] = "bc_photo"
        await update.message.reply_text("Send photo with caption (optional):")
        return

    if txt == "🖼️ Image + Link":
        context.user_data["admin_mode"] = "bc_photo_wait"
        await update.message.reply_text("Send photo (caption optional). Then I will ask for link:")
        return

    if txt == "🗃️ Broadcast File":
        context.user_data["admin_mode"] = "bc_file"
        await update.message.reply_text("Send file/document with caption (optional):")
        return

    if txt == "👤 Personal Message":
        context.user_data["admin_mode"] = "pm_wait_user"
        await update.message.reply_text("Send USER ID to message:")
        return

    if txt == "⛔ Block User":
        context.user_data["admin_mode"] = "block_wait"
        await update.message.reply_text("Send USER ID to BLOCK:")
        return

    if txt == "✅ Unblock User":
        context.user_data["admin_mode"] = "unblock_wait"
        await update.message.reply_text("Send USER ID to UNBLOCK:")
        return

    if txt == "🤖 Auto Reply":
        context.user_data["admin_mode"] = "ar_menu"
        con = db()
        cur = con.cursor()
        cur.execute("SELECT enabled, text FROM autoreply WHERE id=1")
        ar = cur.fetchone()
        con.close()
        await update.message.reply_text(
            f"Auto Reply is {'ON' if int(ar['enabled'])==1 else 'OFF'}\n\n"
            "Commands:\n"
            "1) Send: ON\n"
            "2) Send: OFF\n"
            "3) Send new auto-reply text"
        )
        return

    if txt == "💳 Pending Payouts":
        con = db()
        cur = con.cursor()
        cur.execute("SELECT * FROM payouts WHERE state='pending' ORDER BY id DESC LIMIT 5")
        rows = cur.fetchall()
        con.close()
        if not rows:
            await update.message.reply_text("No pending payouts.")
            return
        for p in rows:
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ ACCEPT", callback_data=f"ADM_PAY_ACCEPT:{p['id']}"),
                InlineKeyboardButton("❌ REJECT", callback_data=f"ADM_PAY_REJECT:{p['id']}")
            ]])
            await update.message.reply_text(
                f"Pending Payout #{p['id']}\nUser: {p['user_id']}\nAmount: ₹{p['amount']}\nUPI/QR: {p['upi_or_qr']}\nTime: {fmt_ts(p['created_at'])}",
                reply_markup=kb
            )
        return

    if txt == "✅ Pending Confirmations":
        con = db()
        cur = con.cursor()
        cur.execute("""
            SELECT a.action_id, a.user_id, r.email, r.first_name, r.password, r.created_at
            FROM actions a
            JOIN registrations r ON r.id=a.reg_id
            WHERE a.state='waiting_admin'
            ORDER BY a.action_id DESC LIMIT 5
        """)
        rows = cur.fetchall()
        con.close()
        if not rows:
            await update.message.reply_text("No pending confirmations.")
            return
        for x in rows:
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ ACCEPT", callback_data=f"ADM_REG_ACCEPT:{x['action_id']}"),
                InlineKeyboardButton("❌ REJECT", callback_data=f"ADM_REG_REJECT:{x['action_id']}")
            ]])
            await update.message.reply_text(
                f"Pending Confirmation (action {x['action_id']})\n"
                f"User: {x['user_id']}\n"
                f"FIRST NAME: {x['first_name']}\n"
                f"EMAIL: {x['email']}\n"
                f"PASSWORD: {x['password']}\n"
                f"Created: {fmt_ts(x['created_at'])}",
                reply_markup=kb
            )
        return

    if txt == "🔙 Back":
        await update.message.reply_text("Main menu:", reply_markup=MAIN_MENU)
        return


async def admin_content_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_admin(user.id):
        return

    mode = context.user_data.get("admin_mode")
    # If admin pressed normal USER menu buttons, cancel any pending admin input mode
    try:
        txt = (update.message.text or '').strip() if update.message else ''
        if txt in {'➕ Register a new account', '📋 My accounts', '💰 Balance', '👥 My referrals', '⚙️ Settings', '✅ TASK', '💬 Help'}:
            context.user_data['admin_mode'] = None
            return
    except Exception:
        pass


    # All users search
    if mode == "all_users_search":
        q = (update.message.text or "").strip()
        if not q:
            await update.message.reply_text("Send username or user id:")
            return
        res = admin_find_user(q)
        if res is None:
            await update.message.reply_text("Not found.")
            return
        if isinstance(res, list):
            lines = ["Search results:"]
            for r in res:
                lines.append(f"- {r['user_id']} | {r['username'] or ''} | MAIN ₹{float(r['main_balance']):.2f} | HOLD ₹{float(r['hold_balance']):.2f}")
            await update.message.reply_text("\n".join(lines))
        else:
            r = res
            await update.message.reply_text(f"Found: {r['user_id']} | {r['username'] or ''}\nMAIN ₹{float(r['main_balance']):.2f} | HOLD ₹{float(r['hold_balance']):.2f}")
        # keep in same mode so admin can search repeatedly
        return

    # Balance add/deduct flow
    if mode == "bal_select":
        q = (update.message.text or "").strip()
        if not q:
            await update.message.reply_text("Send USERID or username:")
            return
        res = admin_find_user(q)
        if res is None:
            await update.message.reply_text("User not found.")
            return
        if isinstance(res, list):
            # if multiple, show first and ask exact id
            lines = ["Multiple users found, send exact USERID:"]
            for r in res[:10]:
                lines.append(f"- {r['user_id']} | {r['username'] or ''}")
            await update.message.reply_text("\n".join(lines))
            return
        r = res
        context.user_data["bal_user_id"] = int(r["user_id"])
        context.user_data["admin_mode"] = "bal_apply"
        await update.message.reply_text(
            f"Selected: {r['user_id']} | {r['username'] or ''}\n"
            f"Current MAIN ₹{float(r['main_balance']):.2f} | HOLD ₹{float(r['hold_balance']):.2f}\n\n"
            "Now send adjustment like:\n"
            "+100 main\n-50 hold\n+10 hold\n-25 main"
        )
        return

    if mode == "bal_apply":
        uid = context.user_data.get("bal_user_id")
        if not uid:
            context.user_data["admin_mode"] = None
            await update.message.reply_text("No user selected.", reply_markup=ADMIN_MENU_KB)
            return
        txt = (update.message.text or "").strip().lower()
        m2 = re.match(r'^([\+\-])\s*(\d+(?:\.\d+)?)\s*(main|hold)\s*$', txt)
        if not m2:
            await update.message.reply_text("Format: +100 main OR -50 hold")
            return
        sign, amt_s, which = m2.groups()
        amt = float(amt_s)
        if sign == "-":
            amt = -amt
        con = db(); cur = con.cursor()
        if which == "main":
            # prevent negative
            cur.execute("SELECT main_balance FROM users WHERE user_id=?", (int(uid),))
            r = cur.fetchone()
            bal = float(r[0]) if r else 0.0
            nb = bal + amt
            if nb < 0:
                con.close()
                await update.message.reply_text("❌ MAIN balance can't go negative.")
                return
            cur.execute("UPDATE users SET main_balance=? WHERE user_id=?", (nb, int(uid)))
        else:
            cur.execute("SELECT hold_balance FROM users WHERE user_id=?", (int(uid),))
            r = cur.fetchone()
            bal = float(r[0]) if r else 0.0
            nb = bal + amt
            if nb < 0:
                con.close()
                await update.message.reply_text("❌ HOLD balance can't go negative.")
                return
            cur.execute("UPDATE users SET hold_balance=? WHERE user_id=?", (nb, int(uid)))
        con.commit()
        # show updated
        cur.execute("SELECT username, main_balance, hold_balance FROM users WHERE user_id=?", (int(uid),))
        r2 = cur.fetchone()
        con.close()
        await update.message.reply_text(
            f"✅ Updated user {uid} ({r2['username'] or ''})\nMAIN ₹{float(r2['main_balance']):.2f} | HOLD ₹{float(r2['hold_balance']):.2f}",
            reply_markup=ADMIN_MENU_KB
        )
        # keep mode for more edits
        return


    # Payout proof flow (admin)
    if mode == "pay_proof_wait_photo":
        pid = context.user_data.get("pay_selected")
        if not pid:
            context.user_data["admin_mode"] = None
            await update.message.reply_text("First select a payout from PAYOUT REQUEST.", reply_markup=ADMIN_MENU_KB)
            return
        if not update.message.photo:
            await update.message.reply_text("❌ Please send a PHOTO screenshot.")
            return
        file_id = update.message.photo[-1].file_id
        # store temporarily until UTR
        context.user_data.setdefault("pay_proof_tmp", {})[pid] = {"photo_file_id": file_id}
        context.user_data["admin_mode"] = "pay_proof_wait_utr"
        await update.message.reply_text("Now send UTR number (text).")
        return

    if mode == "pay_proof_wait_utr":
        pid = context.user_data.get("pay_selected")
        if not pid:
            context.user_data["admin_mode"] = None
            await update.message.reply_text("First select a payout from PAYOUT REQUEST.", reply_markup=ADMIN_MENU_KB)
            return
        utr = (update.message.text or "").strip()
        if len(utr) < 6:
            await update.message.reply_text("❌ UTR invalid. Try again:")
            return
        tmp = context.user_data.get("pay_proof_tmp", {}).get(pid)
        if not tmp or not tmp.get("photo_file_id"):
            context.user_data["admin_mode"] = None
            await update.message.reply_text("First submit screenshot: SUBMIT THE PAYMENT PROOF 🧾", reply_markup=PAYOUT_SUBMENU_KB)
            return

        context.user_data.setdefault("pay_proof", {})[pid] = {"photo_file_id": tmp["photo_file_id"], "utr": utr}
        # clear tmp and exit mode
        context.user_data.get("pay_proof_tmp", {}).pop(pid, None)
        context.user_data["admin_mode"] = None
        await update.message.reply_text("✅ Proof saved. Now press 📤 SEND.", reply_markup=PAYOUT_SUBMENU_KB)
        return

    # Pin message flow
    if mode == "pin_wait":
        if PIN_CHAT_ID is None:
            context.user_data["admin_mode"] = None
            await update.message.reply_text("❌ PIN_CHAT_ID not set in code. Set PIN_CHAT_ID to your channel/group id where bot is admin.", reply_markup=ADMIN_MENU_KB)
            return

        try:
            sent = None
            if update.message.text:
                sent = await context.bot.send_message(chat_id=PIN_CHAT_ID, text=update.message.text)
            elif update.message.photo:
                sent = await context.bot.send_photo(chat_id=PIN_CHAT_ID, photo=update.message.photo[-1].file_id, caption=update.message.caption or "")
            elif update.message.document:
                sent = await context.bot.send_document(chat_id=PIN_CHAT_ID, document=update.message.document.file_id, caption=update.message.caption or "")
            else:
                await update.message.reply_text("Send text/photo/document to pin.")
                return

            await context.bot.pin_chat_message(chat_id=PIN_CHAT_ID, message_id=sent.message_id)
            context.user_data["admin_mode"] = None
            await update.message.reply_text("✅ Pinned.", reply_markup=ADMIN_MENU_KB)
        except Exception as e:
            context.user_data["admin_mode"] = None
            await update.message.reply_text(f"❌ Pin failed: {e}", reply_markup=ADMIN_MENU_KB)
        return


    # Auto reply config
    if mode == "ar_menu":
        msg = (update.message.text or "").strip()
        con = db()
        cur = con.cursor()
        if msg.upper() == "ON":
            cur.execute("UPDATE autoreply SET enabled=1 WHERE id=1")
            con.commit()
            con.close()
            await update.message.reply_text("✅ Auto reply ON")
            return
        if msg.upper() == "OFF":
            cur.execute("UPDATE autoreply SET enabled=0 WHERE id=1")
            con.commit()
            con.close()
            await update.message.reply_text("✅ Auto reply OFF")
            return
        cur.execute("UPDATE autoreply SET text=? WHERE id=1", (msg,))
        con.commit()
        con.close()
        await update.message.reply_text("✅ Auto reply text updated.")
        return

    # Broadcast text
    if mode == "bc_text":
        text_msg = update.message.text or ""
        context.user_data["admin_mode"] = None
        await broadcast_text(context, text_msg)
        await update.message.reply_text("✅ Broadcast sent.", reply_markup=ADMIN_MENU_KB)
        return

    # Broadcast link (button)
    if mode == "bc_link":
        link = (update.message.text or "").strip()
        if not (link.startswith("http://") or link.startswith("https://")):
            await update.message.reply_text("❌ Please send a valid link starting with https://")
            return
        context.user_data["admin_mode"] = None
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Open Link", url=link)]])
        await broadcast_text(context, "🔗 Link:", reply_markup=kb)
        await update.message.reply_text("✅ Link broadcast sent.", reply_markup=ADMIN_MENU_KB)
        return

    # Broadcast image
    if mode == "bc_photo" and update.message.photo:
        caption = update.message.caption or ""
        file_id = update.message.photo[-1].file_id
        context.user_data["admin_mode"] = None
        await broadcast_photo(context, file_id, caption)
        await update.message.reply_text("✅ Image broadcast sent.", reply_markup=ADMIN_MENU_KB)
        return

    # Broadcast photo + link (two step)
    if mode == "bc_photo_wait" and update.message.photo:
        caption = update.message.caption or ""
        file_id = update.message.photo[-1].file_id
        context.user_data["bc_photo_file_id"] = file_id
        context.user_data["bc_photo_caption"] = caption
        context.user_data["admin_mode"] = "bc_photo_link_wait"
        await update.message.reply_text("Now send link (https://...) to attach as button:")
        return

    if mode == "bc_photo_link_wait":
        link = (update.message.text or "").strip()
        if not (link.startswith("http://") or link.startswith("https://")):
            await update.message.reply_text("❌ Please send a valid link starting with https://")
            return
        file_id = context.user_data.get("bc_photo_file_id")
        caption = context.user_data.get("bc_photo_caption", "")
        context.user_data.pop("bc_photo_file_id", None)
        context.user_data.pop("bc_photo_caption", None)
        context.user_data["admin_mode"] = None
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Open Link", url=link)]])
        await broadcast_photo(context, file_id, caption, reply_markup=kb)
        await update.message.reply_text("✅ Photo+link broadcast sent.", reply_markup=ADMIN_MENU_KB)
        return

    # Broadcast file
    if mode == "bc_file" and update.message.document:
        caption = update.message.caption or ""
        file_id = update.message.document.file_id
        context.user_data["admin_mode"] = None
        await broadcast_file(context, file_id, caption)
        await update.message.reply_text("✅ File broadcast sent.", reply_markup=ADMIN_MENU_KB)
        return

    # Personal message flow
    if mode == "pm_wait_user":
        uid_txt = (update.message.text or "").strip()
        if not uid_txt.isdigit():
            await update.message.reply_text("❌ Please send numeric USER ID")
            return
        context.user_data["pm_user_id"] = int(uid_txt)
        context.user_data["admin_mode"] = "pm_wait_text"
        await update.message.reply_text("Now send the message text:")
        return

    if mode == "pm_wait_text":
        uid = context.user_data.get("pm_user_id")
        text_msg = update.message.text or ""
        context.user_data.pop("pm_user_id", None)
        context.user_data["admin_mode"] = None
        try:
            await context.bot.send_message(chat_id=uid, text=text_msg)
            await update.message.reply_text("✅ Personal message sent.", reply_markup=ADMIN_MENU_KB)
        except Exception as e:
            await update.message.reply_text(f"❌ Failed: {e}", reply_markup=ADMIN_MENU_KB)
        return

    # Block / Unblock
    if mode == "block_wait":
        uid_txt = (update.message.text or "").strip()
        if not uid_txt.isdigit():
            await update.message.reply_text("❌ Please send numeric USER ID")
            return
        block_user_db(int(uid_txt))
        context.user_data["admin_mode"] = None
        await update.message.reply_text("✅ User blocked.", reply_markup=ADMIN_MENU_KB)
        return

    if mode == "unblock_wait":
        uid_txt = (update.message.text or "").strip()
        if not uid_txt.isdigit():
            await update.message.reply_text("❌ Please send numeric USER ID")
            return
        unblock_user_db(int(uid_txt))
        context.user_data["admin_mode"] = None
        await update.message.reply_text("✅ User unblocked.", reply_markup=ADMIN_MENU_KB)
        return


async def broadcast_text(context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup=None):
    con = db()
    cur = con.cursor()
    cur.execute("SELECT user_id FROM users")
    users = [row["user_id"] for row in cur.fetchall()]
    con.close()
    for uid in users:
        try:
            if uid != ADMIN_ID and is_blocked(uid):
                continue
            await context.bot.send_message(chat_id=uid, text=text, reply_markup=reply_markup)
        except Exception:
            pass

async def broadcast_photo(context: ContextTypes.DEFAULT_TYPE, file_id: str, caption: str, reply_markup=None):
    con = db()
    cur = con.cursor()
    cur.execute("SELECT user_id FROM users")
    users = [row["user_id"] for row in cur.fetchall()]
    con.close()
    for uid in users:
        try:
            if uid != ADMIN_ID and is_blocked(uid):
                continue
            await context.bot.send_photo(chat_id=uid, photo=file_id, caption=caption, reply_markup=reply_markup)
        except Exception:
            pass

async def broadcast_file(context: ContextTypes.DEFAULT_TYPE, file_id: str, caption: str):
    con = db()
    cur = con.cursor()
    cur.execute("SELECT user_id FROM users")
    users = [row["user_id"] for row in cur.fetchall()]
    con.close()
    for uid in users:
        try:
            if uid != ADMIN_ID and is_blocked(uid):
                continue
            await context.bot.send_document(chat_id=uid, document=file_id, caption=caption)
        except Exception:
            pass

# =========================
# ADDITIONS (Non-destructive)
# =========================



# =========================
# MAIN
# =========================
def main():
    init_db()
    print(f"DB: {DB}")

    app = Application.builder().token(BOT_TOKEN).build()

    # Commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin", admin_cmd))
    app.add_handler(CommandHandler("export", export_cmd))
    app.add_handler(CommandHandler("formimg", formimg_cmd))
    app.add_handler(CommandHandler("referral", referral_cmd))

    # Callbacks
    app.add_handler(CallbackQueryHandler(callbacks))

    # Admin content handlers (broadcast media/text + auto reply config)
    app.add_handler(MessageHandler(filters.PHOTO | filters.Document.ALL, admin_content_handler), group=0)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, admin_content_handler), group=0)

    # UPI input handler
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, upi_handler), group=1)

    # Admin menu handler
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, admin_menu_handler), group=2)

    # User menu handler
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, menu_handler), group=3)

    # ✅ Start Gmail API -> PostgreSQL email-handle sync (background)
    try:
        if os.environ.get("DATABASE_URL") and (os.path.exists("token.json") or os.environ.get("GMAIL_TOKEN_JSON")):
            poll_sec = int(os.environ.get("POLL_SEC", "5"))
            app.job_queue.run_once(
                lambda *_: asyncio.create_task(_gmail_sync_loop(poll_sec=poll_sec)),
                when=0
            )
    except Exception:
        pass

    # =========================
    # WEBHOOK (Railway)
    # =========================
    print("✅ Bot started (WEBHOOK)...")

    port = int(os.environ.get("PORT", "8080"))

    # Railway usually provides this. If not present, set it manually in Railway Variables.
    railway_static = os.environ.get("RAILWAY_STATIC_URL")  # example: "your-app.up.railway.app"
    if not railway_static:
        raise RuntimeError("RAILWAY_STATIC_URL missing. Add it in Railway Variables (without https://)")

    webhook_url = f"https://{railway_static}/{BOT_TOKEN}"

    app.run_webhook(
        listen="0.0.0.0",
        port=port,
        webhook_url=webhook_url,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()