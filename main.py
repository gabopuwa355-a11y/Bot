# -*- coding: utf-8 -*-
import os
import shutil
import subprocess

print("pg_dump:", shutil.which("pg_dump"))
print("psql:", shutil.which("psql"))

try:
    print(subprocess.check_output(["pg_dump", "--version"]).decode())
except Exception as e:
    print("pg_dump error:", e)

try:
    print(subprocess.check_output(["psql", "--version"]).decode())
except Exception as e:
    print("psql error:", e)

import re
import time
from db_pg import sqlite3, db, init_db, DB  # PostgreSQL shim — replaces sqlite3
import threading
import os
import socket
import json
import hmac
import hashlib
from decimal import Decimal, ROUND_DOWN
from datetime import datetime, timedelta
import random
import string
import asyncio
import math
import smtplib
import imaplib
import email as email_pkg
import urllib.parse
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
import builtins

DEBUG = False

def dprint(*args, **kwargs):
    if DEBUG:
        builtins.dprint(*args, **kwargs)

# =========================
# IN-MEMORY CACHE (SPEED OPTIMIZATION)
# DB hit sirf pehli baar, baad mein memory se instant response
# =========================
_balance_cache = {}   # {user_id: (main_balance, hold_balance, timestamp)}
_lang_cache = {}      # {user_id: lang}
_blocked_cache = {}   # {user_id: (is_blocked_bool, timestamp)}
_channel_cache = {}   # {user_id: (is_member_bool, timestamp)} — Telegram API call cache
_currency_cache = {}  # {user_id: (currency_str, timestamp)} — currency cache
_CACHE_TTL = 30       # seconds — 30 sec baad fresh DB se fetch hoga
_CHANNEL_CACHE_TTL = 120  # 2 min — channel membership check Telegram API cache

def _cache_get_balance(user_id):
    entry = _balance_cache.get(user_id)
    if entry and (time.time() - entry[2]) < _CACHE_TTL:
        return entry[0], entry[1]
    return None

def _cache_set_balance(user_id, main_b, hold_b):
    _balance_cache[user_id] = (main_b, hold_b, time.time())

def cache_invalidate_balance(user_id):
    """Jab balance change ho (payout/approval) — call karo."""
    _balance_cache.pop(user_id, None)

def _cache_get_lang(user_id):
    entry = _lang_cache.get(user_id)
    if entry and (time.time() - entry[1]) < 300:  # lang 5 min cache
        return entry[0]
    return None

def _cache_set_lang(user_id, lang):
    _lang_cache[user_id] = (lang, time.time())

def _cache_get_blocked(user_id):
    entry = _blocked_cache.get(user_id)
    if entry and (time.time() - entry[1]) < _CACHE_TTL:
        return entry[0]
    return None

def _cache_set_blocked(user_id, blocked_bool):
    _blocked_cache[user_id] = (blocked_bool, time.time())

def cache_invalidate_blocked(user_id):
    _blocked_cache.pop(user_id, None)

# --- Channel membership cache (BIGGEST SPEED FIX — Telegram API 1-3 sec saved) ---
def _cache_get_channel(user_id):
    entry = _channel_cache.get(user_id)
    if entry and (time.time() - entry[1]) < _CHANNEL_CACHE_TTL:
        return entry[0]
    return None

def _cache_set_channel(user_id, is_member_bool):
    _channel_cache[user_id] = (is_member_bool, time.time())

def cache_invalidate_channel(user_id):
    """Jab user CHK_JOIN press kare — fresh Telegram API check force karo."""
    _channel_cache.pop(user_id, None)

# --- Currency cache ---
def _cache_get_currency(user_id):
    entry = _currency_cache.get(user_id)
    if entry and (time.time() - entry[1]) < 300:  # 5 min
        return entry[0]
    return None

def _cache_set_currency(user_id, code):
    _currency_cache[user_id] = (code, time.time())

def cache_invalidate_currency(user_id):
    _currency_cache.pop(user_id, None)

USDT_RATE = float(os.environ.get("USDT_RATE") or "91")  # 1 USDT = ₹91 (fixed)

def usd_to_inr_fixed(usd_amount: float) -> int:
    """Fixed conversion for payouts: 1 USD = ₹91 (USDT_RATE)."""
    try:
        return int(round(float(usd_amount or 0.0) * float(USDT_RATE)))
    except Exception:
        return int(round(float(usd_amount or 0.0) * 91))

def inr_to_usd_fixed(inr_amount: float) -> float:
    """Fixed conversion for payouts: ₹ -> USD using USDT_RATE."""
    try:
        rate = float(USDT_RATE) if float(USDT_RATE) else 91.0
        return float(inr_amount or 0.0) / rate
    except Exception:
        return 0.0

MANUAL_EMAIL_REASONS = ["Wrong password", "not logout account from your device", "Recovery phone not accessible", "Password recently changed", "Account Ban"]


# Optional DNS lookup (for email domain MX checks)
try:
    import dns.resolver as dns_resolver  # pip install dnspython
except Exception:
    dns_resolver = None

# =========================
# CONFIG (NO .env)
# =========================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
BOT_USERNAME = os.getenv("BOT_USERNAME", "").strip("@")  # optional
ADMIN_ID = 7988263992  # only admin access
PIN_CHAT_ID = None  # set to a group/channel id (bot must be admin) to pin messages there
# Channels gate (user must join to use bot)
REQUIRED_CHANNELS = [
    ("@gmaillhelp", "https://t.me/gmaillhelp"),
    ("@gmaillnews", "https://t.me/gmaillnews"),
]

FINGERPRINT_PUBLIC_BASE_URL = ""  # device-verify webapp disabled; keep empty to avoid NameError

# =========================
# GMAIL SMTP + IMAP (Deliverability check via real send + bounce)
# NOTE: This does NOT "probe" SMTP for existence; it sends a tiny test email and checks for bounce.
# You must use a Gmail account with 2FA + App Password, and enable IMAP in Gmail settings.
ENABLE_SMTP_BOUNCE_CHECK = False
SMTP_GMAIL_USER = "aadiltyagi459@gmail.com"
SMTP_GMAIL_APP_PASSWORD = "kawl rdaz jawr nhfp"
BOUNCE_POLL_SECONDS = 4  # max wait time (fast mode)
BOUNCE_POLL_INTERVALS = (1, 1, 2)  # total <= 4 sec


# Tutorial videos (Telegram file_id) — works on Railway/Termux without local files
VIDEO_FILE_ID_CREATE = "BAACAgUAAxkBAAIFRGoD9Ok-EtEex8mChlUJdNkZEoh0AAKrJAACClYgVI1GQRj1GtKjOwQ"
VIDEO_FILE_ID_LOGOUT = "BAACAgUAAxkBAAMsacjjpEDnlOFct7m4SIRW_IYaEfoAAkYdAAKfvUFWfGP6l1Zu60k6BA"

# (Legacy path variables kept empty for compatibility; not used)
VIDEO_CREATE_PATHS = []
VIDEO_LOGOUT_PATHS = []
VIDEO_FILE_ID_CACHE = {"create": None, "logout": None}
# Provisional HOLD credit added immediately when user confirms (reverted on admin reject)
PRE_CREDIT_AMOUNT = 08.0

# Business rules
MAX_PER_MIN = 3
ACTION_TIMEOUT_HOURS = 20
HOLD_TO_MAIN_AFTER_DAYS = float(os.environ.get("HOLD_TO_MAIN_AFTER_DAYS", "1"))  # Admin env se set kare (e.g. 0.5 = 12 hours, 2 = 2 days)

# CONFIRM AGAIN cooldown (prevents spam clicks without action)
CONFIRM_COOLDOWN_SEC = 50  # wait before running real email check after CONFIRM AGAIN

# UI header used in CONFIRM AGAIN progress effect (keeps same look on every edit)
CONFIRM_EFFECT_HEADER = ""

# Task milestones: approved registrations -> reward added to MAIN (one-time per milestone)
TASK_MILESTONES = [10,20,50,100,1000]

# Referral commission % on every registration approval (admin /setcommission se change kar sakte ho)
REFERRAL_COMMISSION_DEFAULT = 10.0
# in-memory temp storage (preview data per user)
temp_data = {}

# =========================
# SELL FLOW STATES
# =========================
STATE_WAIT_ENTRY    = "wait_entry"
STATE_WAIT_EMAIL    = "wait_email"
STATE_WAIT_PASSWORD = "wait_password"
STATE_WAIT_RECOVERY = "wait_recovery"
STATE_WAIT_2FA      = "wait_2fa"
STATE_CONFIRM       = "confirm_data"
STATE_WAIT_BULK     = "wait_bulk"

# NOTE:
# I am not implementing "random credential generation for paid registrations".
# This bot collects user-provided registration data (legitimate use).
# You can rename text strings as you like.

# =========================
# DB
# =========================
# Serialize SQLite writes to avoid 'database is locked'
DB_WRITE_LOCK = threading.RLock()

# SQLite file path:
# - On Railway, use a persistent Volume mounted at /data and set DB_PATH=/data/bot.db (recommended)
# - On Termux, /sdcard/Download is convenient for manual inspection
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# db(), init_db(), DB — provided by db_pg.py (PostgreSQL shim)


def ensure_user(user_id, username, referrer_id=None):
    """Ensure a user row exists; set referrer only once.

    NOTE: sqlite3 cursor returns tuples by default (unless row_factory is set). We keep tuple-safe code here.
    SPEED OPTIMIZATION: Already-seen users ka username update skip karo (sirf pehli baar ya referrer set hone par DB hit).
    """
    # Agar user already known hai aur koi referrer set nahi karna — DB skip
    _seen_key = f"eu_{user_id}_{username}"
    if not referrer_id and _seen_key in _lang_cache:
        return  # Already processed, kuch naya nahi karna

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

    # referrer tracking: already handled via users.referrer_id
    # NOTE: amount=0 placeholder rows removed — they conflict with actual bonus payment check

    con.commit()
    con.close()
    # Mark as seen so next call (same username) skips DB
    _lang_cache[_seen_key] = (username, time.time())

def get_lang(user_id):
    """Safe language lookup with fallback during startup/migration."""
    if not user_id:
        return "en"
    # Cache check pehle (SPEED OPTIMIZATION)
    cached = _cache_get_lang(user_id)
    if cached is not None:
        return cached
    con = None
    try:
        con = db()
        cur = con.cursor()
        cur.execute("SELECT lang FROM users WHERE user_id=?", (user_id,))
        r = cur.fetchone()
        lang = r[0] if r and r[0] else "en"
        # agar DB mein 'hi' stored hai as default (purana data), 'en' return karo
        lang = lang if lang in ('en', 'hi', 'ur') else "en"
        _cache_set_lang(user_id, lang)
        return lang
    except Exception:
        return "en"
    finally:
        try:
            if con is not None:
                con.close()
        except Exception:
            pass

def set_lang(user_id, lang):
    con = db()
    cur = con.cursor()
    cur.execute("UPDATE users SET lang=? WHERE user_id=?", (lang, user_id))
    con.commit()
    con.close()
    _cache_set_lang(user_id, lang)  # Cache update


# =========================
# I18N (external translations.json)
# =========================
TRANSLATIONS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "translations.json")

def _default_translations():
    return {
        "en": {
            "menu_register": "💱 Sell your Gmail",
            "menu_accounts": "📑 My accounts",
            "menu_balance": "💰 Balance",
            "menu_referrals": "👥 My referrals",
            "menu_settings": "⚙️ Settings",
            "menu_task": "📚Task",
            "menu_help": "💬 Help",
            "menu_profile": "🙆 My status",
            "settings_language": "LANGUAGE🔤",
            "settings_currency": "💱 Currency",
            "back": "🔙 Back",
            "back_upper": "🔙 BACK",
            "payout": "💳 Payout",
            "balance_history": "🧾 Balance history",
            "payout_upi": "1. UPI 🚀",
            "payout_crypto": "2. CRYPTO ( USDT BEP-20)",
            "lang_en": "ENGLISH 🅰️",
            "lang_hi": "हिंदी ✔️",
            "lang_ur": "اردو❤️",
            "welcome_menu": "Welcome! Choose an option from the menu 👇",
            "funds_accrual": "💸 Accrual of funds to the balance",
            "action_too_often": "You have performed this action too often. Try again later",
            "my_accounts_empty": "No account history yet.",
            "my_accounts_title": "📋 My accounts (page {page}/{total_pages}):",
            "reg_not_over": "⚫ Registration is not over",
            "accepted": "🟢 Accepted",
            "until_hold": "Until hold: {time}",
            "registration_canceled": "🔴 Registration Canceled",
            "created": "Created: {time}",
            "main_balance_line": "MAIN BALANCE= ₹{mainb:.2f}{approx}",
            "hold_balance_line": "HOLD BALANCE= ₹{holdb:.2f}{approx}",
            "balance_approx": " (≈ {value})",
            "profile_title": "👤 PROFILE",
            "user_id": "🆔 User ID: {value}",
            "username": "👤 Username: {value}",
            "total_registrations": "📌 TOTAL REGISTRATIONS: {value}",
            "total_approved": "✅ TOTAL APPROVED REGISTRATION: {value}",
            "total_rejected": "✖️ TOTAL REJECT REGISTRATION: {value}",
            "total_canceled": "🚫 TOTAL CANCELED REGISTRATION: {value}",
            "approval_ratio": "📈 APPROVAL RATIO: {value:.1f}%",
            "total_referrals": "👥 TOTAL REFERRALS: {value}",
            "total_ref_earned": "💰 TOTAL REFERRAL EARNED: ₹{value:.2f}",
            "profile_back": "BACK 🔙",
            "choose_withdrawal": "Choose withdrawal method",
            "choose_amount": "Choose withdrawal amount:",
            "upi_mode": "UPI MODE",
            "send_bep20": "Wallet address like: 0xb2450F5B107b4e04087cB70cDD8E6476385236B1\nBlockchain : BEP-20\n\nSend your BEP-20 wallet address now:",
            "ref_bonus": "🔸 PER REFERRAL BONUS = ₹10 (once per referred user)",
            "ref_when_complete": "🔸 WHEN YOUR REFERRAL COMPLETE 15 REGISTRATION",
            "your_ref_link": "🔸🔗 Your referral link:\n{link}",
            "total_referrals_line": "🔸 TOTAL REFERRALS: {value}",
            "total_earned_line": "🔸 TOTAL EARNED FROM REFERRALS: ₹{value}",
            "no_referrals": "(No referrals yet)",
            "settings_text": "Settings:\n💱 Currency: {cur}",
            "choose_language": "Choose language:",
            "choose_currency": "Choose display currency (current: {cur}):",
            "currency_set": "✅ Currency set: {code}",
            "language_set_en": "✅ Language set: English",
            "language_set_hi": "✅ Language set: Hindi",
            "language_set_ur": "✅ Language set: Urdu",
            "help_menu": "HELP MENU✅",
            "main_menu_text": "Main menu:",
            "back_done": "✅ Back",
            "help_not_found": "Help info not found.",
            "help_support": "Technical Support",
            "help_news": "Project News",
            "help_buy": "Buy Accounts",
            "task_menu_title": "✅ TASK MENU",
            "task_approved": "🟢 Accepted: {value}",
            "task_done": "✅ {m} 🟢 Accepted = ₹{m}",
            "task_pending_ready": "🟡 {m} 🟢 Accepted = ₹{m}  (will add soon)",
            "task_need_more": "⏳ {m} 🟢 Accepted = ₹{m}  (need {left} more)",
            "help_1_label": "⏰ What is Hold?",
            "help_2_label": "📲 How to avoid SMS confirmation?",
            "help_3_label": "🔴 Why is the account unavailable?",
            "help_4_label": "❇️ How to avoid Gmail account blocking?",
            "help_5_label": "♾️ How does the referral system work?",
            "help_6_label": "💧 How many Gmail accounts can be submitted to the bot?",

            "help_1_text": "\"Hold\" is a 2-days period during which the Gmail account is \"resting\". Within 2 days after creating an account, Google may block it. After the hold ends, the account goes to moderation and then funds are added to Balance.",

            "help_2_text": "To prevent Google from asking for phone number confirmation during registration:\n\nDo not register more than two accounts per day from the same browser.\nDo not register more than two accounts per day from the same IP address.\n✖️ Do not install browser extensions.\n✖️ Do not use VPN.\n\n✅ Use \"Incognito\" mode or clear browser cache after each registration.\n✅ Use Android emulators.\n✅ Use several portable browsers.\n\n❕ If your internet provider gives dynamic IP, restart your modem to change IP.\n❕ When using mobile internet, turn internet off and on to change IP.\n\nIf these steps do not bypass SMS confirmation, you must use a phone number to receive SMS.",

            "help_3_text": "Within 2 days after registration, Google may block suspicious accounts. Such accounts are not paid and are marked as unavailable.\n\nIf you try to log in to such an account, you will see that the account cannot be used.",

            "help_4_text": "To prevent Google from blocking your account:\n\n✖️ Do not log in after registration.\nDo not register more than two accounts per day from the same browser.\nDo not register more than two accounts per day from the same IP.\n✖️ Do not use VPN.\n\n✅ Log out immediately after registration.\n✅ Use Incognito mode.\n✅ Use Android emulators.\n✅ Use different portable browsers.\n\n❕ Change IP by restarting modem or mobile internet.",

            "help_5_text": "Every user who joins the bot using your referral link becomes your referral.\n\nEach Gmail account registered by your referral will give you a referral reward after it is accepted.\n\nYou can have unlimited referrals.",

            "help_6_text": "The bot will accept any number of Gmail accounts you can register. The main condition is that Google does not block them during the 2-day hold.",

            "join_required_text": "Please join all required channels first, then tap ✅ I JOINED.",
            "channel_verified": "✅ Channel membership verified. You can now use the bot.",
            "invalid_amount": "❌ Invalid amount selected.",
            "insufficient_withdrawal": "❌ Insufficient balance for this withdrawal.",
            "payout_sent": "✅ Withdrawal request sent successfully.",
            "balance_title": "💰 Balance",
            "referral_row": "• {uname} | Joined: {joined} | Approvals: {approved} | Paid: {paid}",
            "email_checking": "🔍 Checking email... {progress}",
            "invalid_upi": "❌ Invalid UPI ID.",
            "used_upi": "❌ This UPI ID was already used.",
            "invalid_qr": "❌ Invalid QR code.",
            "used_qr": "❌ This QR code was already used.",
            "invalid_bep20": "❌ Invalid BEP-20 wallet address.",
            "invalid_amount_usd": "❌ Invalid USD amount.",
            "min_crypto_withdraw": "❌ Minimum crypto withdrawal is 0.25 USD.",
            "address_missing": "❌ Wallet address missing or invalid.",
            "crypto_address_saved": "✅ Wallet address saved. Available balance: {balance} USD",
            "crypto_payout_sent": "✅ Crypto withdrawal request sent successfully.",
            "ref_bonus": "🔸 REFERRAL COMMISSION = percentage-based (set by admin)",
            "ref_when_complete": "🔸 WHEN YOUR REFERRAL COMPLETE 15 REGISTRATION",
            "total_referrals_line": "🔸 TOTAL REFERRALS: {value}",
            "total_earned_line": "🔸 TOTAL EARNED FROM REFERRALS: ₹{value}",
            "back_done": "✅ Back",
            "BALANCE  UPDATED✅": "BALANCE UPDATED✅",
            "l⚠️ Please add a recovery email in {handle}@gmail.com": "⚠️ Please add a recovery email in {handle}@gmail.com",
            "👋 Welcome!\n\n": "👋 Welcome!\n\n",
            "🚫 You are not joined in required channel.\n\n": "🚫 You are not joined in required channel.\n\n"
        },   

        "hi": {
            "menu_register": "➕ नया अकाउंट रजिस्टर करें",
            "menu_accounts": "📋 मेरे अकाउंट",
            "menu_balance": "💰 बैलेंस",
            "menu_referrals": "👥 मेरे रेफरल",
            "menu_settings": "⚙️ सेटिंग्स",
            "menu_task": "📚 टास्क",
            "menu_help": "💬 मदद",
            "menu_profile": "👤 प्रोफाइल",
            "settings_language": "LANGUAGE🔤",
            "settings_currency": "💱 करेंसी",
            "back": "🔙 वापस",
            "back_upper": "🔙 वापस",
            "payout": "💳 पेआउट",
            "balance_history": "🧾 बैलेंस हिस्ट्री",
            "payout_upi": "1. UPI 🚀",
            "payout_crypto": "2. CRYPTO ( USDT BEP-20)",
            "lang_en": "ENGLISH 🅰️",
            "lang_hi": "हिंदी ✔️",
            "lang_ur": "اردو❤️",
            "welcome_menu": "स्वागत है! नीचे मेन्यू से विकल्प चुनें 👇",
            "funds_accrual": "💸 बैलेंस में राशि जोड़ दी गई है",
            "action_too_often": "आपने यह काम बहुत जल्दी-जल्दी किया है। बाद में फिर कोशिश करें।",
            "my_accounts_empty": "अभी कोई अकाउंट हिस्ट्री नहीं है।",
            "my_accounts_title": "📋 मेरे अकाउंट (पेज {page}/{total_pages}):",
            "reg_not_over": "⚫ रजिस्ट्रेशन अभी पूरा नहीं हुआ",
            "accepted": "🟢 स्वीकार किया गया",
            "until_hold": "होल्ड समाप्त: {time}",
            "registration_canceled": "🔴 रजिस्ट्रेशन रद्द",
            "created": "बनाया गया: {time}",
            "main_balance_line": "MAIN BALANCE= ₹{mainb:.2f}{approx}",
            "hold_balance_line": "HOLD BALANCE= ₹{holdb:.2f}{approx}",
            "balance_approx": " (≈ {value})",
            "profile_title": "👤 प्रोफाइल",
            "user_id": "🆔 यूज़र आईडी: {value}",
            "username": "👤 यूज़रनेम: {value}",
            "total_registrations": "📌 कुल रजिस्ट्रेशन: {value}",
            "total_approved": "✅ कुल अप्रूव रजिस्ट्रेशन: {value}",
            "total_rejected": "✖️ कुल रिजेक्ट रजिस्ट्रेशन: {value}",
            "total_canceled": "🚫 कुल कैंसल रजिस्ट्रेशन: {value}",
            "approval_ratio": "📈 अप्रूवल रेशियो: {value:.1f}%",
            "total_referrals": "👥 कुल रेफरल: {value}",
            "total_ref_earned": "💰 कुल रेफरल कमाई: ₹{value:.2f}",
            "profile_back": "वापस 🔙",
            "choose_withdrawal": "निकासी का प्रकार चुनें",
            "choose_amount": "निकासी राशि चुनें:",
            "upi_mode": "UPI मोड",
            "send_bep20": "वॉलेट एड्रेस उदाहरण: 0xb2450F5B107b4e04087cB70cDD8E6476385236B1\nब्लॉकचेन : BEP-20\n\nअब अपना BEP-20 वॉलेट एड्रेस भेजें:",
            "ref_bonus": "🔸 प्रति रेफरल बोनस = ₹10 (हर रेफर किए गए यूज़र पर एक बार)",
            "ref_when_complete": "🔸 जब आपका रेफरल 15 रजिस्ट्रेशन पूरा करेगा",
            "your_ref_link": "🔸🔗 आपका रेफरल लिंक:\n{link}",
            "total_referrals_line": "🔸 कुल रेफरल: {value}",
            "total_earned_line": "🔸 रेफरल से कुल कमाई: ₹{value}",
            "no_referrals": "(अभी कोई रेफरल नहीं)",
            "settings_text": "सेटिंग्स:\n💱 करेंसी: {cur}",
            "choose_language": "भाषा चुनें:",
            "choose_currency": "डिस्प्ले करेंसी चुनें (अभी: {cur}):",
            "currency_set": "✅ करेंसी सेट हो गई: {code}",
            "language_set_en": "✅ भाषा सेट: English",
            "language_set_hi": "✅ भाषा सेट: हिंदी",
            "language_set_ur": "✅ भाषा सेट: اردو",
            "help_menu": "मदद मेन्यू ✅",
            "main_menu_text": "मुख्य मेन्यू:",
            "back_done": "✅ वापस",
            "help_not_found": "मदद जानकारी नहीं मिली।",
            "help_support": "तकनीकी सहायता",
            "help_news": "प्रोजेक्ट समाचार",
            "help_buy": "अकाउंट खरीदें",
            "task_menu_title": "✅ टास्क मेन्यू",
            "task_approved": "🟢 Accepted: {value}",
            "task_done": "✅ {m} 🟢 Accepted = ₹{m}",
            "task_pending_ready": "🟡 {m} 🟢 Accepted = ₹{m}  (जल्द जोड़ दिया जाएगा)",
            "task_need_more": "⏳ {m} 🟢 Accepted = ₹{m}  (और {left} चाहिए)",
            "help_1_label": "⏰ होल्ड क्या है?",
            "help_2_label": "📲 SMS confirmation से कैसे बचें?",
            "help_3_label": "🔴 अकाउंट उपलब्ध क्यों नहीं है?",
            "help_4_label": "❇️ Gmail account को block होने से कैसे बचाएं?",
            "help_5_label": "♾️ Referral system कैसे काम करता है?",
            "help_6_label": "💧 एक bot के लिए कितने Gmail account दे सकते हैं?",

            "help_1_text": "Hold 2 दिन की अवधि होती है जिसमें Gmail अकाउंट \"आराम\" करता है। अकाउंट बनाने के 2 दिनों के अंदर Google उसे ब्लॉक कर सकता है। Hold खत्म होने के बाद अकाउंट moderation में जाता है और फिर फंड Balance में जोड़ दिए जाते हैं।",

            "help_2_text": "रजिस्ट्रेशन के दौरान Google द्वारा फोन नंबर confirmation से बचने के लिए:\n\nएक ही browser से दिन में दो से ज्यादा अकाउंट रजिस्टर न करें।\nएक ही IP address से दिन में दो से ज्यादा अकाउंट रजिस्टर न करें।\n✖️ Browser extensions इंस्टॉल न करें।\n✖️ VPN का उपयोग न करें।\n\n✅ \"Incognito\" mode का उपयोग करें या हर registration के बाद cache clear करें।\n✅ Android emulator का उपयोग करें।\n✅ कई portable browsers का उपयोग करें।\n\n❕ अगर आपका internet provider dynamic IP देता है तो modem restart करें।\n❕ Mobile internet बंद/चालू करके IP बदल सकते हैं।\n\nअगर ये तरीके काम न करें तो SMS प्राप्त करने के लिए नंबर देना होगा।",

            "help_3_text": "Registration के 2 दिनों के अंदर Google संदिग्ध अकाउंट को ब्लॉक कर सकता है। ऐसे अकाउंट का भुगतान नहीं होता और उन्हें unavailable मार्क किया जाता है।\n\nऐसे अकाउंट में login करने पर पता चलेगा कि उसे इस्तेमाल नहीं किया जा सकता।",

            "help_4_text": "अपने Gmail अकाउंट को ब्लॉक होने से बचाने के लिए:\n\n✖️ Registration के बाद login न करें।\nएक ही browser और IP से दिन में दो से ज्यादा अकाउंट न बनाएं।\n✖️ VPN का उपयोग न करें।\n\n✅ Registration के तुरंत बाद logout करें।\n✅ Incognito mode का उपयोग करें।\n✅ Android emulator का उपयोग करें।\n✅ अलग-अलग portable browsers का उपयोग करें।\n\n❕ IP बदलने के लिए modem या mobile internet restart करें।",

            "help_5_text": "जो यूज़र आपके referral link से bot में आएगा वह आपका referral बन जाएगा।\n\nउसके द्वारा रजिस्टर किया गया हर Gmail अकाउंट स्वीकार होने के बाद आपको referral reward देगा।\n\nआप कितने भी referrals रख सकते हैं।",

            "help_6_text": "Bot उतने Gmail अकाउंट स्वीकार करेगा जितने आप रजिस्टर कर सकते हैं। मुख्य बात यह है कि 2 दिन के hold के दौरान Google उन्हें ब्लॉक न करे।"          
        },
        "ur": {
            "menu_register": "\u200F➕ نیا اکاؤنٹ رجسٹر کریں",
            "menu_accounts": "\u200F📋 میرے اکاؤنٹس",
            "menu_balance": "\u200F💰 بیلنس",
            "menu_referrals": "\u200F👥 میرے حوالہ جات",
            "menu_settings": "\u200F⚙️ ترتیبات",
            "menu_task": "\u200F✅ کام",
            "menu_help": "\u200F💬 مدد",
            "menu_profile": "\u200F👤 پروفائل",
            "settings_language": "\u200F🔤 زبان",
            "settings_currency": "\u200F💱 کرنسی",
            "back": "\u200F🔙 واپس",
            "back_upper": "\u200F🔙 واپس",
            "payout": "\u200F💳 ادائیگی",
            "balance_history": "\u200F🧾 بیلنس کی تاریخ",
            "payout_upi": "\u200F1. یو پی آئی 🚀",
            "payout_crypto": "\u200F2. کرپٹو ( یو ایس ڈی ٹی بی ای پی-20 )",
            "lang_en": "\u200Fانگریزی 🅰️",
            "lang_hi": "\u200Fہندی ✔️",
            "lang_ur": "\u200Fاردو ❤️",
            "welcome_menu": "\u200Fخوش آمدید! نیچے مینو سے آپشن منتخب کریں 👇",
            "funds_accrual": "\u200F💸 رقم بیلنس میں شامل کر دی گئی",
            "action_too_often": "\u200Fآپ نے یہ کام بہت زیادہ بار کیا ہے۔ بعد میں دوبارہ کوشش کریں۔",
            "my_accounts_empty": "\u200Fابھی کوئی اکاؤنٹ کی تاریخ موجود نہیں ہے۔",
            "my_accounts_title": "\u200F📋 میرے اکاؤنٹس (صفحہ {page}/{total_pages}):",
            "reg_not_over": "\u200F⚫ رجسٹریشن ابھی مکمل نہیں ہوئی",
            "accepted": "\u200F🟢 منظور شدہ",
            "until_hold": "\u200Fہولڈ ختم ہونے تک: {time}",
            "registration_canceled": "\u200F🔴 رجسٹریشن منسوخ",
            "created": "\u200Fبنایا گیا: {time}",
            "main_balance_line": "\u200Fمرکزی بیلنس = ₹{mainb:.2f}{approx}",
            "hold_balance_line": "\u200Fہولڈ بیلنس = ₹{holdb:.2f}{approx}",
            "balance_approx": "\u200F(تقریباً {value})",
            "profile_title": "\u200F👤 پروفائل",
            "user_id": "\u200F🆔 صارف شناخت: {value}",
            "username": "\u200F👤 صارف نام: {value}",
            "total_registrations": "\u200F📌 کل رجسٹریشن: {value}",
            "total_approved": "\u200F✅ کل منظور شدہ رجسٹریشن: {value}",
            "total_rejected": "\u200F✖️ کل مسترد رجسٹریشن: {value}",
            "total_canceled": "\u200F🚫 کل منسوخ رجسٹریشن: {value}",
            "approval_ratio": "\u200F📈 منظوری کا تناسب: {value:.1f}%",
            "total_referrals": "\u200F👥 کل حوالہ جات: {value}",
            "total_ref_earned": "\u200F💰 حوالہ سے کل کمائی: ₹{value:.2f}",
            "profile_back": "\u200Fواپس 🔙",
            "choose_withdrawal": "\u200Fنکالنے کی قسم منتخب کریں",
            "choose_amount": "\u200Fنکالنے کی رقم منتخب کریں:",
            "upi_mode": "\u200Fیو پی آئی موڈ",
            "send_bep20": "\u200Fوالیٹ پتا مثال: 0xb2450F5B107b4e04087cB70cDD8E6476385236B1\nبلاک چین: بی ای پی-20\n\nاب اپنا بی ای پی-20 والیٹ پتا بھیجیں:",
            "ref_bonus": "\u200F🔸 فی حوالہ بونس = ₹10 (ہر حوالہ دیے گئے صارف پر ایک بار)",
            "ref_when_complete": "\u200F🔸 جب آپ کا حوالہ 15 رجسٹریشن مکمل کرے",
            "your_ref_link": "\u200F🔸🔗 آپ کا حوالہ لنک:\n{link}",
            "total_referrals_line": "\u200F🔸 کل حوالہ جات: {value}",
            "total_earned_line": "\u200F🔸 حوالہ سے کل کمائی: ₹{value}",
            "no_referrals": "\u200F(ابھی کوئی حوالہ نہیں)",
            "settings_text": "\u200Fترتیبات:\n💱 کرنسی: {cur}",
            "choose_language": "\u200Fزبان منتخب کریں:",
            "choose_currency": "\u200Fڈسپلے کرنسی منتخب کریں (موجودہ: {cur}):",
            "currency_set": "\u200F✅ کرنسی سیٹ ہو گئی: {code}",
            "language_set_en": "\u200F✅ زبان سیٹ: انگریزی",
            "language_set_hi": "\u200F✅ زبان سیٹ: ہندی",
            "language_set_ur": "\u200F✅ زبان سیٹ: اردو",
            "help_menu": "\u200Fمدد مینو ✅",
            "main_menu_text": "\u200Fمرکزی مینو:",
            "back_done": "\u200F✅ واپس",
            "help_not_found": "\u200Fمدد کی معلومات نہیں ملیں۔",
            "help_support": "\u200Fتکنیکی مدد",
            "help_news": "\u200Fمنصوبے کی خبریں",
            "help_buy": "\u200Fاکاؤنٹس خریدیں",
            "task_menu_title": "\u200F✅ کام مینو",
            "task_approved": "\u200Fمنظور شدہ ✅: {value}",
            "task_done": "\u200F✅ {m} منظور شدہ = ₹{m}",
            "task_pending_ready": "\u200F🟡 {m} منظور شدہ = ₹{m}  (جلد شامل ہو جائے گا)",
            "task_need_more": "\u200F⏳ {m} منظور شدہ = ₹{m}  ({left} مزید چاہیے)",
            "help_1_label": "\u200F⏰ ہولڈ کیا ہے؟",
            "help_2_label": "\u200F📲 ایس ایم ایس تصدیق سے کیسے بچیں؟",
            "help_3_label": "\u200F🔴 اکاؤنٹ دستیاب کیوں نہیں ہے؟",
            "help_4_label": "\u200F❇️ جی میل اکاؤنٹ کو بلاک ہونے سے کیسے بچائیں؟",
            "help_5_label": "\u200F♾️ حوالہ نظام کیسے کام کرتا ہے؟",
            "help_6_label": "\u200F💧 ایک بوٹ کے لئے کتنے جی میل اکاؤنٹس دے سکتے ہیں؟",

            "help_1_text": "\u200Fہولڈ 2 دن کا عرصہ ہوتا ہے جس میں جی میل اکاؤنٹ \"آرام\" کرتا ہے۔ اکاؤنٹ بنانے کے 2 دن کے اندر گوگل اسے بلاک کر سکتا ہے۔ ہولڈ ختم ہونے کے بعد اکاؤنٹ جانچ کے مرحلے میں جاتا ہے اور پھر رقم بیلنس میں شامل کر دی جاتی ہے۔",

            "help_2_text": "\u200Fرجسٹریشن کے دوران گوگل کی طرف سے فون نمبر کی تصدیق سے بچنے کے لئے:\n\nایک ہی براؤزر سے دن میں دو سے زیادہ اکاؤنٹس رجسٹر نہ کریں۔\nایک ہی آئی پی ایڈریس سے دن میں دو سے زیادہ اکاؤنٹس رجسٹر نہ کریں۔\n✖️ براؤزر ایکسٹینشن نصب نہ کریں۔\n✖️ وی پی این استعمال نہ کریں۔\n\n✅ \"انکوگنیٹو\" موڈ استعمال کریں یا ہر رجسٹریشن کے بعد کیش صاف کریں۔\n✅ اینڈرائیڈ ایمولیٹر استعمال کریں۔\n✅ مختلف پورٹیبل براؤزر استعمال کریں۔\n\n❕ اگر آپ کا انٹرنیٹ فراہم کنندہ متحرک آئی پی دیتا ہے تو موڈم دوبارہ شروع کریں۔\n❕ موبائل انٹرنیٹ بند اور آن کر کے آئی پی تبدیل کریں۔\n\nاگر یہ طریقے ایس ایم ایس تصدیق کو بائی پاس نہ کریں تو آپ کو ایسا نمبر دینا ہوگا جس پر ایس ایم ایس موصول ہو سکے۔",

            "help_3_text": "\u200Fرجسٹریشن کے 2 دن کے اندر گوگل مشکوک اکاؤنٹس کو بلاک کر سکتا ہے۔ ایسے اکاؤنٹس کی ادائیگی نہیں ہوتی اور انہیں دستیاب نہیں کے طور پر نشان زد کر دیا جاتا ہے۔\n\nاگر آپ ایسے اکاؤنٹ میں لاگ اِن کرنے کی کوشش کریں گے تو معلوم ہوگا کہ اسے استعمال نہیں کیا جا سکتا۔",

            "help_4_text": "\u200Fاپنے جی میل اکاؤنٹ کو بلاک ہونے سے بچانے کے لئے:\n\n✖️ رجسٹریشن کے بعد لاگ اِن نہ کریں۔\nایک ہی براؤزر اور آئی پی سے دن میں دو سے زیادہ اکاؤنٹس نہ بنائیں۔\n✖️ وی پی این استعمال نہ کریں۔\n\n✅ رجسٹریشن کے فوراً بعد لاگ آؤٹ کریں۔\n✅ انکوگنیٹو موڈ استعمال کریں۔\n✅ اینڈرائیڈ ایمولیٹر استعمال کریں۔\n✅ مختلف پورٹیبل براؤزر استعمال کریں۔\n\n❕ آئی پی تبدیل کرنے کے لئے موڈم یا موبائل انٹرنیٹ دوبارہ شروع کریں۔",

            "help_5_text": "\u200Fجو صارف آپ کے حوالہ لنک کے ذریعے بوٹ میں آتا ہے وہ آپ کا حوالہ بن جاتا ہے۔\n\nاس کے رجسٹر کئے گئے ہر جی میل اکاؤنٹ کے قبول ہونے کے بعد آپ کو حوالہ انعام ملتا ہے۔\n\nآپ جتنے چاہیں حوالہ جات رکھ سکتے ہیں۔",

            "help_6_text": "\u200Fبوٹ اتنے جی میل اکاؤنٹس قبول کرے گا جتنے آپ رجسٹر کر سکتے ہیں۔ اصل بات یہ ہے کہ 2 دن کے ہولڈ کے دوران گوگل انہیں بلاک نہ کرے۔"
        }
    }

def load_translations():
    if os.path.exists(TRANSLATIONS_FILE):
        try:
            with open(TRANSLATIONS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, dict) and data:
                    return data
        except Exception:
            pass
    data = _default_translations()
    try:
        with open(TRANSLATIONS_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass
    return data

TRANSLATIONS = load_translations()
ALL_LANGS = ('en','hi','ur')

def tr_lang(lang: str, key: str, **kwargs) -> str:
    lang = (lang or 'en').lower()
    base = TRANSLATIONS.get(lang) or TRANSLATIONS.get('en') or {}
    fallback = TRANSLATIONS.get('en') or {}
    val = base.get(key, fallback.get(key, key))
    try:
        return str(val).format(**kwargs)
    except Exception:
        return str(val)

def tr(user_id: int, key: str, **kwargs) -> str:
    return tr_lang(get_lang(user_id), key, **kwargs)

def any_label(key: str):
    vals = []
    for lg in ALL_LANGS:
        vals.append(tr_lang(lg, key))
    return tuple(dict.fromkeys(vals))

def txt_is(txt: str, key: str) -> bool:
    return txt in any_label(key)

def main_menu_markup(user_id: int):
    return ReplyKeyboardMarkup([
        [tr(user_id,'menu_register'), tr(user_id,'menu_accounts')],
        [tr(user_id,'menu_balance'), tr(user_id,'menu_referrals')],
        [tr(user_id,'menu_settings'), tr(user_id,'menu_task')],
        [tr(user_id,'menu_help'), tr(user_id,'menu_profile')],
    ], resize_keyboard=True)

def balance_menu(user_id=None):
    if user_id is None:
        return ReplyKeyboardMarkup([[tr_lang('en','payout'), tr_lang('en','balance_history')], [tr_lang('en','back')]], resize_keyboard=True)
    return ReplyKeyboardMarkup([[tr(user_id,'payout'), tr(user_id,'balance_history')], [tr(user_id,'back')]], resize_keyboard=True)

def payout_menu_kb(user_id=None):
    if user_id is None:
        return ReplyKeyboardMarkup([[tr_lang('en','payout_upi')],[tr_lang('en','payout_crypto')],[tr_lang('en','back_upper')]], resize_keyboard=True)
    return ReplyKeyboardMarkup([[tr(user_id,'payout_upi')],[tr(user_id,'payout_crypto')],[tr(user_id,'back_upper')]], resize_keyboard=True)

def payout_selected_kb(selected_label: str, user_id=None):
    back_label = tr(user_id,'back_upper') if user_id is not None else tr_lang('en','back_upper')
    return ReplyKeyboardMarkup([[selected_label],[back_label]], resize_keyboard=True)

def back_only_menu(user_id=None):
    back_label = tr(user_id,'back_upper') if user_id is not None else tr_lang('en','back_upper')
    return ReplyKeyboardMarkup([[back_label]], resize_keyboard=True)

def settings_menu(user_id=None):
    if user_id is None:
        return ReplyKeyboardMarkup([[tr_lang('en','settings_language')],[tr_lang('en','settings_currency')],[tr_lang('en','back')]], resize_keyboard=True)
    return ReplyKeyboardMarkup([[tr(user_id,'settings_language')],[tr(user_id,'settings_currency')],[tr(user_id,'back')]], resize_keyboard=True)

def language_menu(user_id=None):
    if user_id is None:
        return ReplyKeyboardMarkup([[tr_lang('en','lang_en'), tr_lang('en','lang_hi')],[tr_lang('en','lang_ur')],[tr_lang('en','back')]], resize_keyboard=True)
    return ReplyKeyboardMarkup([[tr(user_id,'lang_en'), tr(user_id,'lang_hi')],[tr(user_id,'lang_ur')],[tr(user_id,'back')]], resize_keyboard=True)

def help_menu_kb(user_id: int) -> InlineKeyboardMarkup:
    rows = []
    for i in range(1,7):
        rows.append([InlineKeyboardButton(tr(user_id, f'help_{i}_label'), callback_data=f'HELP_{i}')])
    rows.append([InlineKeyboardButton(tr(user_id,'help_support'), url='https://t.me/Gs24x7Bot')])
    rows.append([InlineKeyboardButton(tr(user_id,'help_news'), url='https://t.me/gmaillnews')])
    rows.append([InlineKeyboardButton(tr(user_id,'help_buy'), url='http://t.me/GmailstocksBot')])
    return InlineKeyboardMarkup(rows)

def help_back_kb(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(tr(user_id,'back_upper'), callback_data='HELP_BACK')]])

def task_menu_text(user_id: int) -> str:
    con = db()
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM registrations WHERE user_id=? AND state='approved'", (user_id,))
    accepted_count = int(cur.fetchone()["c"])
    cur.execute("SELECT milestone FROM task_rewards WHERE user_id=?", (user_id,))
    claimed = {int(r["milestone"]) for r in cur.fetchall()}
    con.close()

    # Badge metadata: milestone -> (icon, label, reward)
    BADGE_META = {
        10:   ("🏅", "Starter",  10),
        20:   ("🏅", "Hustler",  20),
        50:   ("🎖️", "Expert",   50),
        100:  ("🏆", "Elite",   100),
        1000: ("🌟", "Godmode", 1000),
    }

    # Total earned = sum of claimed milestone rewards
    total_earned = sum(BADGE_META[m][2] for m in TASK_MILESTONES if m in claimed)

    # Next unclaimed milestone
    next_milestone = next((m for m in TASK_MILESTONES if m not in claimed), None)
    next_left = max(next_milestone - accepted_count, 0) if next_milestone else 0

    lines = []
    lines.append("🔥 TASK STREAK")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"Current 🟢 Accepted: {accepted_count} 🔥")
    lines.append("")
    lines.append("BADGES EARNED:")

    for m in TASK_MILESTONES:
        icon, label, reward = BADGE_META[m]
        left = max(m - accepted_count, 0)
        if m in claimed:
            status = "✅"
            lines.append(f"  {icon} {label:<10}({m})  {status}")
        elif left == 0:
            # Ready to claim (pending admin credit)
            status = f"🟡 CLAIM ₹{reward}!"
            lines.append(f"  {icon} {label:<10}({m})  {status}")
        else:
            status = f"🔒 {left} more"
            lines.append(f"  {icon} {label:<10}({m})  {status}")

    lines.append("")
    lines.append(f"💰 Total earned: ₹{total_earned}")
    if next_milestone:
        lines.append(f"🎯 Next: {next_left} approvals → ₹{BADGE_META[next_milestone][2]}")

    return "\n".join(lines)

def get_balances(user_id):
    # Cache check pehle (SPEED OPTIMIZATION)
    cached = _cache_get_balance(user_id)
    if cached is not None:
        return cached
    con = db()
    cur = con.cursor()
    cur.execute("SELECT main_balance, hold_balance FROM users WHERE user_id=?", (user_id,))
    r = cur.fetchone()
    con.close()
    if not r:
        return 0.0, 0.0
    result = float(r[0]), float(r[1])
    _cache_set_balance(user_id, result[0], result[1])
    return result



def apply_task_rewards(cur, user_id: int, approved_count: int) -> float:
    """Pay out milestone rewards to MAIN balance. Returns total newly paid."""
    # Admin can disable task rewards globally
    if get_setting("task_rewards") != "on":
        return 0.0
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
            add_ledger_entry_cur(cur, user_id, delta_main=float(amt), reason=f"Task reward milestone {m}")
            paid_total += amt
    return paid_total



def add_hold_credit(user_id, amount) -> int:
    """Add amount to HOLD and create a hold_credits row. Returns hold_credits.id."""
    def _op():
        con = db()
        cur = con.cursor()
        hid, _ = add_hold_credit_cur(cur, int(user_id), float(amount))
        con.commit()
        con.close()
        return int(hid)
    result = int(_db_write_retry(_op))
    cache_invalidate_balance(user_id)  # Cache clear
    return result

def revert_hold_credit(hold_credit_id: int, user_id: int, amount: float) -> None:
    """Revert a previously added HOLD credit (prevent maturation + subtract from hold_balance)."""
    def _op():
        con = db()
        cur = con.cursor()
        revert_hold_credit_cur(cur, int(hold_credit_id), int(user_id), float(amount))
        con.commit()
        con.close()
    _db_write_retry(_op)
    cache_invalidate_balance(user_id)  # Cache clear

def move_matured_hold_to_main(user_id):
    """Move matured HOLD credits to MAIN.
    Returns (amount_moved, task_paid, ref_id, commission_amt).

    FIXES:
    - Bug 1: commission_pct transaction ke bahar fetch hota hai (nested connection deadlock fix)
    - Bug 4: status update sirf pending rows pe hota hai, approved pe nahi
    - hold_balance negative nahi ho sakta (GREATEST guard)
    """
    # Commission pehle lo — transaction ke bahar, alag connection issue na ho
    commission_pct = get_referral_commission_pct()

    now = int(time.time())
    con = db()
    cur = con.cursor()

    # Pehle matured rows fetch karo
    cur.execute(
        "SELECT id, amount FROM hold_credits WHERE user_id=? AND moved=0 AND matured_at<=?",
        (user_id, now),
    )
    rows = cur.fetchall()
    if not rows:
        con.close()
        return 0.0, 0.0, None, 0.0

    total = sum(float(x["amount"]) for x in rows)
    ids = [str(x["id"]) for x in rows]

    # 1. moved=1 PEHLE mark karo — spam rokne ke liye
    cur.execute(f"UPDATE hold_credits SET moved=1 WHERE id IN ({','.join(['?']*len(ids))})", ids)

    # 2. Balance update karo (GREATEST se hold_balance negative nahi ho sakta)
    cur.execute(
        "UPDATE users SET hold_balance = GREATEST(0, hold_balance - ?), main_balance = main_balance + ? WHERE user_id=?",
        (total, total, user_id),
    )
    add_ledger_entry_cur(cur, user_id, delta_main=float(total), delta_hold=-float(total), reason="HOLD matured to MAIN")

    # 3. Registration approve karo — sirf woh jo abhi bhi pending hain (Bug 4 fix)
    cur.execute(
        "SELECT a.action_id, a.reg_id FROM actions a "
        "WHERE a.user_id=? AND a.state IN ('waiting_admin', 'confirmed_by_user', 'waiting_hold')",
        (user_id,),
    )
    pending_actions = cur.fetchall()
    for pa in pending_actions:
        cur.execute(
            "UPDATE actions SET state='approved', updated_at=? WHERE action_id=? AND state IN ('waiting_admin', 'confirmed_by_user', 'waiting_hold')",
            (now, int(pa["action_id"])),
        )
        cur.execute(
            "UPDATE registrations SET state='approved' WHERE id=? AND state != 'approved'",
            (int(pa["reg_id"]),),
        )

    # 4. Referral commission (Bug 1 fix: commission_pct pehle se fetch hai, no nested DB call)
    ref_id = None
    commission_amt = 0.0
    try:
        cur.execute("SELECT referrer_id FROM users WHERE user_id=?", (int(user_id),))
        ref_row = cur.fetchone()
        if ref_row and ref_row["referrer_id"]:
            ref_id = int(ref_row["referrer_id"])

        if ref_id:
            commission_amt = round(float(total) * commission_pct / 100.0, 2)
            if commission_amt > 0 and get_setting("referral_rewards") == "on":
                # Har registration pe commission milegi (no dedup)
                cur.execute(
                    "UPDATE users SET main_balance = main_balance + ? WHERE user_id=?",
                    (commission_amt, ref_id),
                )
                add_ledger_entry_cur(cur, ref_id, delta_main=float(commission_amt), delta_hold=0.0,
                                     reason=f"Referral commission from user {user_id}")
                cur.execute(
                    "INSERT INTO referral_bonuses(referrer_id, referred_user_id, amount, created_at) VALUES(?,?,?,?)",
                    (ref_id, int(user_id), commission_amt, now),
                )
    except Exception as ref_err:
        dprint(f"[SWEEPER] Referral commission error for user {user_id}: {ref_err!r}")
        ref_id = None
        commission_amt = 0.0

    # 5. Task rewards — ab approved count sahi hoga (abhi approve hua upar)
    cur.execute(
        "SELECT COUNT(*) AS c FROM registrations WHERE user_id=? AND state='approved'",
        (user_id,),
    )
    accepted_count = int(cur.fetchone()["c"])
    task_paid = apply_task_rewards(cur, int(user_id), accepted_count)

    # 6. Commit karo
    con.commit()
    con.close()
    cache_invalidate_balance(user_id)
    if ref_id:
        cache_invalidate_balance(ref_id)
    return float(total), float(task_paid), ref_id, float(commission_amt)


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

MAIN_MENU = ReplyKeyboardMarkup([["💲Sell", "📋 My accounts"],["💰 Balance", "👥 My referrals"],["⚙️ Settings", "✅ TASK"],["💬 Help", "👤 Profile"]], resize_keyboard=True)

def reg_buttons(action_id):
    # STEP 1 — Initial buttons
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✓ Done", callback_data=f"REG_DONE:{action_id}")],
        [InlineKeyboardButton("Cancel Registration", callback_data=f"REG_CANCEL:{action_id}")],
        [InlineKeyboardButton("❓ How to create account?", callback_data="VID_CREATE")],
    ])

def complete_back_buttons(action_id):
    # STEP 2 — After Done: Complete or Back
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🪀 Complete", callback_data=f"REG_COMPLETE:{action_id}")],
        [InlineKeyboardButton("🔙 Back", callback_data=f"REG_STEP1:{action_id}")],
    ])

def confirm_again_button(action_id):
    # STEP 2(a) — After Complete: Confirm check + recovery video + back
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💲 Click again to confirm", callback_data=f"REG_CONFIRM:{action_id}")],
        [InlineKeyboardButton("📕 How to add recovery email?", callback_data="VID_RECOVERY")],
        [InlineKeyboardButton("🔙 Back", callback_data=f"REG_STEP2:{action_id}")],
    ])

def post_confirm_buttons():
    # After successful CONFIRM — only logout button
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔔 How to logout of account?", callback_data="VID_LOGOUT")],
    ])

def cancel_confirm_buttons(action_id: int) -> InlineKeyboardMarkup:
    # On CANCEL click — confirm cancel or go back
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ Cancelled Registration", callback_data=f"REG_CANCEL_SURE:{action_id}")],
        [InlineKeyboardButton("🔙 Back", callback_data=f"REG_STEP1:{action_id}")],
    ])


# ─────────────────────────────────────────────────────────────────
# All possible action states:
#   shown            → user hasn't started yet
#   done1            → user clicked DONE, recovery email being checked
#   waiting_admin    → email verified, waiting admin review
#   approved         → admin approved
#   rejected         → admin rejected
#   canceled         → user/system canceled
#   timeout          → time expired before user finished
# ─────────────────────────────────────────────────────────────────
def _account_status_text(user_id: int, st: str, rstate: str, ev_status_norm: str,
                          ev_reason: str, stime: int, now_ts: int) -> tuple:
    """
    Returns (status_text, extra_line) for a given action state.
    Only shows: Hold, Accepted, Rejected (with reason).
    All other states are skipped (filtered out upstream).
    """
    extra = ""

    if st == "waiting_admin":
        status_text = "🟡 Hold"

    elif st == "approved":
        status_text = "🟢 Accepted"

    elif st == "rejected":
        if ev_status_norm in ("NOT_VERIFIED", "REJECTED") and ev_reason.strip():
            status_text = f"🔴 Rejected — {ev_reason.strip()}"
        else:
            status_text = "🔴 Rejected"

    else:
        # All other states hidden from user view
        status_text = ""

    return status_text, extra


def _build_account_lines(rows, user_id: int, now_ts: int) -> list:
    """Build formatted lines for account history rows.
    Only shows: Hold, Accepted, Rejected (with reason). Skips other states.
    """
    lines = []
    dprint(f"[ACCOUNTS] _build_account_lines called, rows count: {len(rows)}", flush=True)
    for i, rr in enumerate(rows, 1):
        try:
            dprint(f"[ACCOUNTS] row {i} keys: {list(rr.keys())}", flush=True)
            st        = str(rr["astate"] or "").strip()
            # Skip states that shouldn't be shown
            if st not in ("waiting_admin", "approved", "rejected"):
                dprint(f"[ACCOUNTS] row {i} skipped (state={st})", flush=True)
                continue
            try:
                rstate = str(rr["rstate"] or "").strip()
            except Exception:
                rstate = ""
            try:
                ev_status = str(rr["ev_status"] or "").strip()
            except Exception:
                ev_status = ""
            try:
                ev_reason = str(rr["ev_reason"] or "").strip()
            except Exception:
                ev_reason = ""
            ev_norm = ev_status.upper().replace(" ", "_")
            try:
                stime = int(rr["stime"] or 0)
            except Exception:
                stime = 0

            status_text, extra = _account_status_text(
                user_id, st, rstate, ev_norm, ev_reason, stime, now_ts
            )

            if not status_text:
                continue

            email_display = str(rr["email"] or "—")

            # Format submitted time in IST
            try:
                from datetime import timezone, timedelta
                IST = timezone(timedelta(hours=5, minutes=30))
                sub_ts = int(rr.get("sub_ts") or rr.get("stime") or 0)
                if sub_ts:
                    sub_time = datetime.fromtimestamp(sub_ts, tz=IST).strftime("%d %b %Y %I:%M %p")
                    time_line = f"\n🕐 {sub_time} IST"
                else:
                    time_line = ""
            except Exception:
                time_line = ""

            # Show "Hold until" only for waiting_admin (Hold) state
            hold_until_line = ""
            if st == "waiting_admin":
                try:
                    matured_at = int(rr.get("matured_at") or 0)
                    if matured_at:
                        from datetime import timezone, timedelta
                        IST = timezone(timedelta(hours=5, minutes=30))
                        mat_dt = datetime.fromtimestamp(matured_at, tz=IST)
                        mat_str = mat_dt.strftime("%d %b %Y %I:%M %p IST")
                        remaining_sec = matured_at - int(time.time())
                        if remaining_sec > 0:
                            hrs = remaining_sec // 3600
                            mins = (remaining_sec % 3600) // 60
                            if hrs > 0:
                                remain_str = f"{hrs}h {mins}m remaining"
                            else:
                                remain_str = f"{mins}m remaining"
                            hold_until_line = f"\n⏳ Hold until: {mat_str} ({remain_str})"
                        else:
                            hold_until_line = f"\n✅ Hold period over — processing soon"
                except Exception:
                    pass

            line = (
                f"📧 {email_display}\n"
                f"📌 {status_text}"
                f"{hold_until_line}"
                f"{time_line}"
            )
            lines.append(line)
            dprint(f"[ACCOUNTS] row {i} appended OK: {email_display}", flush=True)
        except Exception as e:
            import traceback
            err = traceback.format_exc()
            dprint(f"[ACCOUNTS] row {i} ERROR: {e}\n{err}", flush=True)
    dprint(f"[ACCOUNTS] returning {len(lines)} lines", flush=True)
    return lines


def accounts_nav(offset, total):
    page = offset // 5 + 1
    total_pages = max(1, (total + 4) // 5)
    nav_row = []
    if offset - 5 >= 0:
        nav_row.append(InlineKeyboardButton("◀️ PREV", callback_data=f"ACC:{offset-5}"))
    nav_row.append(InlineKeyboardButton(f"📄 {page}/{total_pages}", callback_data="NOOP"))
    if offset + 5 < total:
        nav_row.append(InlineKeyboardButton("NEXT ▶️", callback_data=f"ACC:{offset+5}"))
    rows = [nav_row]
    return InlineKeyboardMarkup(rows)

def payout_amounts_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("55💲", callback_data="PAY_AMT:55"),
         InlineKeyboardButton("110❤️", callback_data="PAY_AMT:110")],
        [InlineKeyboardButton("210🥰", callback_data="PAY_AMT:210"),
         InlineKeyboardButton("310😁", callback_data="PAY_AMT:310")],
        [InlineKeyboardButton("510😯", callback_data="PAY_AMT:510"),
         InlineKeyboardButton("1050💰", callback_data="PAY_AMT:1050")],
    ])


def payout_amounts_with_back_kb() -> InlineKeyboardMarkup:
    """Reply-only payout flow: keep compatibility alias but no inline BACK button."""
    return payout_amounts_kb()


# =========================
# CURRENCY (INR base + hourly cache)
# =========================

CURRENCY_CHOICES = [
    ("INR", "₹ INR"),
    ("USD", "$ USD"),
    ("EUR", "€ EUR"),
    ("GBP", "£ GBP"),
    ("AED", "AED"),
    ("SAR", "SAR"),
    ("PKR", "PKR"),
    ("BDT", "BDT"),
    ("NPR", "NPR"),
]

_rates_cache = {"ts": 0, "base": "INR", "rates": {}}  # refreshed at most once per hour

def get_user_currency(user_id: int) -> str:
    # Cache check pehle (SPEED OPTIMIZATION)
    cached = _cache_get_currency(user_id)
    if cached is not None:
        return cached
    try:
        con = db()
        cur = con.cursor()
        cur.execute("SELECT currency FROM users WHERE user_id=?", (user_id,))
        row = cur.fetchone()
        if not row or not row[0]:
            _cache_set_currency(user_id, "INR")
            return "INR"
        result = str(row[0]).upper().strip() or "INR"
        _cache_set_currency(user_id, result)
        return result
    except Exception:
        return "INR"
    finally:
        try:
            con.close()
        except Exception:
            pass

def set_user_currency(user_id: int, code: str):
    code = (code or "INR").upper().strip()
    if not any(c[0] == code for c in CURRENCY_CHOICES):
        code = "INR"
    con = db()
    cur = con.cursor()
    cur.execute("UPDATE users SET currency=? WHERE user_id=?", (code, user_id))
    con.commit()
    con.close()
    _cache_set_currency(user_id, code)  # Cache update

def _refresh_rates_if_needed():
    """Refresh INR-base FX rates with a 1-hour cache.

    Priority order (as in your screen recording):
      1) If CURRENCY_API_KEY is 32-hex -> treat it as OpenExchangeRates app_id.
         OpenExchangeRates free plan is USD-base, so we convert to INR-base.
      2) Otherwise, if CURRENCY_API_KEY exists -> use currencyapi.com (INR base supported).
      3) If no key -> free fallback: open.er-api.com (INR base).
    """
    now = int(time.time())
    if _rates_cache.get("rates") and (now - int(_rates_cache.get("ts") or 0) < 3600):
        return

    symbols_list = [c[0] for c in CURRENCY_CHOICES if c[0] != "INR"]
    api_key = (os.environ.get("CURRENCY_API_KEY") or "").strip()

    import requests

    try:
        rates: dict[str, float] = {}

        # 1) OpenExchangeRates app_id (32-hex)
        if api_key and re.fullmatch(r"[0-9a-fA-F]{32}", api_key):
            url = "https://openexchangerates.org/api/latest.json"
            params = {"app_id": api_key}
            r = requests.get(url, params=params, timeout=15)
            data = r.json() if r is not None else {}

            raw = (data.get("rates") or {})  # USD-base rates
            usd_to_inr = float(raw.get("INR") or 0.0)
            if usd_to_inr > 0:
                for code in symbols_list:
                    v = raw.get(code)
                    if v is None:
                        continue
                    # Convert USD-base to INR-base: (code_per_USD) / (INR_per_USD)
                    rates[code.upper()] = float(v) / usd_to_inr

        # 2) currencyapi.com (if any other key)
        elif api_key:
            url = "https://api.currencyapi.com/v3/latest"
            params = {
                "apikey": api_key,
                "base_currency": "INR",
                "currencies": ",".join(symbols_list),
            }
            r = requests.get(url, params=params, timeout=15)
            data = r.json() if r is not None else {}
            for k, v in (data.get("data") or {}).items():
                try:
                    rates[k.upper()] = float(v.get("value"))
                except Exception:
                    pass

        # 3) Free fallback: open.er-api.com
        else:
            url = "https://open.er-api.com/v6/latest/INR"
            r = requests.get(url, timeout=15)
            data = r.json() if r is not None else {}
            raw = (data.get("rates") or {})
            for code in symbols_list:
                if code in raw:
                    try:
                        rates[code.upper()] = float(raw[code])
                    except Exception:
                        pass

        if rates:
            _rates_cache["ts"] = now
            _rates_cache["rates"] = rates
            try:
                dprint("RATES LOADED:", rates)
            except Exception:
                pass

    except Exception as e:
        try:
            dprint("RATE FETCH ERROR:", e)
        except Exception:
            pass
        return

def convert_inr(amount_inr: float, to_code: str) -> float:
    to_code = (to_code or "INR").upper().strip()
    if to_code == "INR":
        return float(amount_inr)
    _refresh_rates_if_needed()
    rate = (_rates_cache.get("rates") or {}).get(to_code)
    if not rate:
        return float(amount_inr)
    return float(amount_inr) * float(rate)

def fmt_money(amount: float, code: str) -> str:
    code = (code or "INR").upper().strip()
    sym = {"INR":"₹", "USD":"$", "EUR":"€", "GBP":"£"}.get(code, code + " ")
    try:
        return f"{sym}{float(amount):.2f}"
    except Exception:
        return f"{sym}{amount}"

def currency_kb():
    btns = [[label] for _, label in CURRENCY_CHOICES]
    btns.append(["🔙 Back"])
    return ReplyKeyboardMarkup(btns, resize_keyboard=True)

def inr_to_usd(inr_amount: float) -> float:
    """Convert INR -> USD using the same hourly INR-base rates cache."""
    try:
        _refresh_rates_if_needed()
        r = float((_rates_cache.get("rates") or {}).get("USD") or 0.0)  # USD per 1 INR
        return float(inr_amount or 0.0) * r if r else 0.0
    except Exception:
        return 0.0

def usd_to_inr(usd_amount: float) -> int:
    """Convert USD -> INR using INR-base rates cache. Returns rounded integer INR."""
    try:
        _refresh_rates_if_needed()
        r = float((_rates_cache.get("rates") or {}).get("USD") or 0.0)  # USD per 1 INR
        if not r:
            # Safe fallback (very rough) if rates not available
            return int(round(float(usd_amount or 0.0) * 85))
        return int(round(float(usd_amount or 0.0) / r))
    except Exception:
        return int(round(float(usd_amount or 0.0) * 85))


def usd_balance_sufficient(main_balance_inr: float, requested_usd: float) -> bool:
    """Round to 2dp to avoid float precision false insufficient errors."""
    try:
        avail = round(float(inr_to_usd(float(main_balance_inr))), 2)
        req = round(float(requested_usd), 2)
        return avail + 1e-9 >= req
    except Exception:
        return False


# HELPERS
# =========================

async def user_in_required_channels(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> bool:
    """
    Checks if user is member of all REQUIRED_CHANNELS.
    SPEED OPTIMIZATION: Result 2 min tak cache — har message pe Telegram API call nahi.
    """
    cached = _cache_get_channel(user_id)
    if cached is not None:
        return cached

    for chat_username, _url in REQUIRED_CHANNELS:
        try:
            m = await context.bot.get_chat_member(chat_id=chat_username, user_id=user_id)
            status = getattr(m, "status", None)
            if status in ("left", "kicked"):
                _cache_set_channel(user_id, False)
                return False
        except Exception as _e:
            # Bot is not admin in channel or API error — skip this channel check
            print(f"[CHANNEL CHECK] Warning: cannot check {chat_username}: {_e!r}", flush=True)
            continue
    _cache_set_channel(user_id, True)
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

async def _edit_message_safe(bot, chat_id: int, message_id: int, text: str, reply_markup=None):
    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, reply_markup=reply_markup)
        return True
    except Exception:
        return False

# Spinner frames for smooth "checking" animation
SPIN = ["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"]

try:
    from telegram.error import RetryAfter
except Exception:
    RetryAfter = None

async def animate_until_done(bot, chat_id: int, message_id: int, base_text: str, action_id: int, done: asyncio.Event):
    """Animate a separate 'confirm' message until `done` is set."""
    i = 0
    while not done.is_set():
        progress = (i * 12) % 100
        text = (
            (base_text or "")
            + f"\n\n{SPIN[i % len(SPIN)]} 🔍 EMAIL CHECKING..."
            + f"\n[{_confirm_bar(progress)}] {progress}%"
        )
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                reply_markup=None,
            )
        except Exception as e:
            if RetryAfter is not None and isinstance(e, RetryAfter):
                try:
                    await asyncio.sleep(float(getattr(e, "retry_after", 0.5)))
                except Exception:
                    pass
            # ignore other edit errors (message deleted / same content / etc.)
        await asyncio.sleep(0.25)
        i += 1

async def run_check_with_animation(bot, chat_id: int, message_id: int, base_text: str, action_id: int, check_coro):
    """Run `check_coro` while animating the confirm message."""
    done = asyncio.Event()
    anim_task = asyncio.create_task(
        animate_until_done(bot, chat_id, message_id, base_text, action_id, done)
    )
    try:
        return await check_coro
    finally:
        done.set()
        try:
            await anim_task
        except Exception:
            pass


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

    msg = tr(user.id, "join_required_text")
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


def is_valid_bep20_address(addr: str) -> bool:
    """Basic BEP-20/EVM address validation (0x + 40 hex chars)."""
    a = (addr or "").strip()
    return bool(re.fullmatch(r"0x[a-fA-F0-9]{40}", a))


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
# EMAIL SYSTEM (SQLite + Gmail API)
# Replaces IMAP handle search.
# =========================

EMAIL_HANDLE_RE = re.compile(r"\b([a-z0-9._%+\-]{2,64})@gmail\.com\b", re.I)

# Runtime debug/state for Gmail sync (helps troubleshoot NOT 🚫 always)
SYNC_STATE = {
    "started": False,
    "last_tick": 0,
    "last_list_count": 0,
    "last_handles_saved": 0,
    "last_error": "",
}

def _email_sqlite_init():
    """Create tables used by the email-handle cache (SQLite)."""
    con = db()
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS email_cache (
            handle TEXT PRIMARY KEY,
            last_seen INTEGER NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS email_meta (
            k TEXT PRIMARY KEY,
            v TEXT NOT NULL
        )
    """)
    con.commit()
    con.close()

def _email_set_meta(k: str, v: str):
    con = db()
    cur = con.cursor()
    cur.execute(
        """INSERT INTO email_meta(k,v) VALUES(?,?)
             ON CONFLICT(k) DO UPDATE SET v=excluded.v""",
        (str(k), str(v)),
    )
    con.commit()
    con.close()

def _email_get_meta(k: str, default: str = "") -> str:
    con = db()
    cur = con.cursor()
    cur.execute("SELECT v FROM email_meta WHERE k=?", (str(k),))
    row = cur.fetchone()
    con.close()
    return (row[0] if row else default)

def _email_upsert_handle(handle: str):
    handle = (handle or "").strip().lower()
    if not handle:
        return
    con = db()
    cur = con.cursor()
    cur.execute(
        """INSERT INTO email_cache(handle,last_seen) VALUES(?,?)
             ON CONFLICT(handle) DO UPDATE SET last_seen=excluded.last_seen""",
        (handle, int(time.time())),
    )
    con.commit()
    con.close()

def _email_handle_exists(handle: str) -> bool:
    handle = (handle or "").strip().lower()
    if not handle:
        return False
    con = db()
    cur = con.cursor()
    cur.execute("SELECT 1 FROM email_cache WHERE handle=? LIMIT 1", (handle,))
    ok = cur.fetchone() is not None
    con.close()
    return ok

def _gmail_api_service():
    """Build Gmail API service using token/credentials from env or local files."""
    import os
    from googleapiclient.discovery import build
    from google.oauth2.credentials import Credentials

    # Optional: write JSONs from Railway env vars
    creds_env = (os.environ.get("GMAIL_CREDENTIALS_JSON", "") or "").strip()
    token_env = (os.environ.get("GMAIL_TOKEN_JSON", "") or "").strip()
    if creds_env:
        with open("credentials.json", "w", encoding="utf-8") as f:
            f.write(creds_env)
    if token_env:
        with open("token.json", "w", encoding="utf-8") as f:
            f.write(token_env)

    creds = Credentials.from_authorized_user_file(
        "token.json",
        ["https://www.googleapis.com/auth/gmail.readonly"],
    )
    return build("gmail", "v1", credentials=creds, cache_discovery=False)

async def _gmail_sync_loop(poll_sec: int = 5, max_list: int = 200):
    """Poll Gmail every poll_sec seconds, extract gmail handles from message text, store into SQLite."""
    import os

    try:
        SYNC_STATE["started"] = True
        SYNC_STATE["last_error"] = ""
        _email_sqlite_init()
        dprint(f"[SYNC] Gmail sync loop started (poll_sec={poll_sec}, max_list={max_list})")
    except Exception as e:
        SYNC_STATE["last_error"] = f"init: {e!r}"
        dprint(f"[SYNC] init failed: {e!r}")
        return

    ignore = {h.strip().lower() for h in (os.environ.get("IGNORE_HANDLES", "") or "").split(",") if h.strip()}

    # Service build can fail if token.json missing/invalid; keep error visible
    try:
        svc = _gmail_api_service()
    except Exception as e:
        SYNC_STATE["last_error"] = f"service: {e!r}"
        dprint(f"[SYNC] service build failed: {e!r}")
        return

    last_msg_id = _email_get_meta("last_msg_id", "")
    query = os.environ.get(
        "GMAIL_SYNC_QUERY",
        'newer_than:14d (in:inbox OR in:spam) (gmail.com OR "email for" OR "sent to" OR "verification" OR "verify")'
    )

    while True:
        try:
            res = svc.users().messages().list(userId="me", q=query, maxResults=max_list).execute()
            msgs = res.get("messages", []) or []

            SYNC_STATE["last_tick"] = int(time.time())
            SYNC_STATE["last_list_count"] = int(len(msgs))
            SYNC_STATE["last_handles_saved"] = 0

            for m in msgs:
                mid = m.get("id")
                if not mid:
                    continue

                # stop when we reach last processed message
                if last_msg_id and mid == last_msg_id:
                    break

                msg = svc.users().messages().get(
                    userId="me",
                    id=mid,
                    format="metadata",
                    metadataHeaders=["Subject", "From", "To", "Delivered-To", "X-Forwarded-To"],
                ).execute()

                snippet = (msg.get("snippet", "") or "")
                payload = msg.get("payload", {}) or {}
                headers = payload.get("headers", []) or []
                header_text = " ".join((h.get("value", "") or "") for h in headers)

                text = (snippet + " " + header_text)

                # If no @gmail.com found in snippet+headers, fetch full body (costs 1 extra API call)
                found_handles = EMAIL_HANDLE_RE.findall(text)
                if not found_handles:
                    try:
                        full_msg = svc.users().messages().get(
                            userId="me", id=mid, format="full"
                        ).execute()
                        import base64
                        def _extract_text(payload):
                            parts = payload.get("parts") or []
                            if not parts:
                                data = (payload.get("body") or {}).get("data", "")
                                if data:
                                    try:
                                        return base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="replace")
                                    except Exception:
                                        return ""
                                return ""
                            out = []
                            for p in parts:
                                mime = p.get("mimeType", "")
                                if mime == "text/plain":
                                    data = (p.get("body") or {}).get("data", "")
                                    if data:
                                        try:
                                            out.append(base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="replace"))
                                        except Exception:
                                            pass
                                elif mime.startswith("multipart/"):
                                    out.append(_extract_text(p))
                            return " ".join(out)
                        body = _extract_text(full_msg.get("payload", {}))
                        text = text + " " + body
                        found_handles = EMAIL_HANDLE_RE.findall(text)
                    except Exception as body_err:
                        dprint(f"[SYNC] body fetch error for {mid}: {body_err!r}")

                # Store handles (local-part only)
                for full in found_handles:
                    h = full.split("@")[0].lower().strip()
                    if h and h not in ignore:
                        _email_upsert_handle(h)
                        SYNC_STATE["last_handles_saved"] += 1

            if msgs:
                # newest message becomes marker
                last_msg_id = msgs[0].get("id", last_msg_id)
                if last_msg_id:
                    _email_set_meta("last_msg_id", last_msg_id)

        except Exception as e:
            SYNC_STATE["last_error"] = repr(e)
            try:
                dprint(f"[SYNC] tick error: {e!r}")
            except Exception:
                pass

        await asyncio.sleep(int(poll_sec))



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
        # If imported `import datetime`, use datetime.fromtimestamp
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
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
    def _op():
        con = db()
        cur = con.cursor()
        add_ledger_entry_cur(cur, int(user_id), float(delta_main), float(delta_hold), str(reason or ""))
        con.commit()
        con.close()
    try:
        _db_write_retry(_op)
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


def build_balance_history_text(user_id: int, limit_ledger: int = 10, limit_payouts: int = 5) -> str:
    """Balance history like requested:
    - HOLD/Main movements (ledger)
    - Last payout requests (method-wise)
    """
    now = int(time.time())
    thirty_days_ago = now - 30 * 24 * 3600

    # Ledger
    ledger = get_ledger_rows(user_id, limit_ledger)

    # Payouts (last 30d)
    con = db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT amount, amount_usd, method, upi_or_qr, created_at, state
        FROM payouts
        WHERE user_id=? AND created_at>=?
        ORDER BY id DESC LIMIT ?
        """,
        (int(user_id), int(thirty_days_ago), int(limit_payouts)),
    )
    payouts = cur.fetchall()
    con.close()

    lines = []
    lines.append("🧾 BALANCE HISTORY\n")

    # HOLD/Main movement section
    if ledger:
        lines.append("🟢 Account acceptance / Hold-Main movement:")
        for e in ledger:
            dm = float(e[0] if isinstance(e, (tuple, list)) else (e["delta_main"] or 0))
            dh = float(e[1] if isinstance(e, (tuple, list)) else (e["delta_hold"] or 0))
            reason = (e[2] if isinstance(e, (tuple, list)) else (e["reason"] or "")).strip()
            ts = int(e[3] if isinstance(e, (tuple, list)) else (e["created_at"] or 0))
            t = fmt_ts(ts)

            hold_part = f"Hold: {'+' if dh>0 else ''}{dh:.2f}$" if dh != 0 else "Hold: 0"
            main_part = f"Main: {'+' if dm>0 else ''}{dm:.2f}$" if dm != 0 else "Main: 0"

            # Use INR sign for balances (your DB is INR-based), but keep the same numeric formatting
            # If you want ₹ always:
            hold_part = hold_part.replace('$', '₹')
            main_part = main_part.replace('$', '₹')

            lines.append(f"• {reason}\n  {hold_part} | {main_part}\n  Date: {t}")
        lines.append("")

    # Payout requests section
    if payouts:
        lines.append("🧾 Last 5 payout requests (last 30 days):")
        for p in payouts:
            method = ((p[2] if isinstance(p, (tuple, list)) else (p["method"] or "upi")) or "upi").lower()
            upi_or_qr = (p[3] if isinstance(p, (tuple, list)) else (p["upi_or_qr"] or ""))
            snip = upi_or_qr[:18] + ("..." if len(upi_or_qr) > 18 else "")
            created_at = int(p[4] if isinstance(p, (tuple, list)) else (p["created_at"] or 0))
            state = (p[5] if isinstance(p, (tuple, list)) else (p["state"] or ""))
            amt_inr = float(p[0] if isinstance(p, (tuple, list)) else (p["amount"] or 0))
            amt_usd = float(p[1] if isinstance(p, (tuple, list)) else (p["amount_usd"] or 0.0))

            if method == "crypto":
                # crypto: USD + INR fixed 91
                if amt_usd <= 0:
                    amt_usd = inr_to_usd_fixed(amt_inr)
                inr_fixed = usd_to_inr_fixed(amt_usd)
                lines.append(f"• CRYPTO ${amt_usd:.2f} (₹{inr_fixed}) | {snip} | {fmt_ts(created_at)} | {state}")
            else:
                # upi: INR only
                lines.append(f"• UPI ₹{int(amt_inr)} | {snip} | {fmt_ts(created_at)} | {state}")

    return "\n".join(lines)


# -------------------------
# BALANCE HISTORY PAGINATION (inline Next/Back like your screenshot)
# -------------------------

def _fetch_balance_history_events(user_id: int, days: int = 30, max_rows: int = 200):
    """Return combined list of payout + ledger events sorted by time desc."""
    now = int(time.time())
    since = now - int(days) * 24 * 3600

    con = db()
    cur = con.cursor()

    cur.execute(
        """
        SELECT amount, amount_usd, method, upi_or_qr, created_at, state
        FROM payouts
        WHERE user_id=? AND created_at>=?
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (int(user_id), int(since), int(max_rows)),
    )
    payouts = cur.fetchall()

    cur.execute(
        """
        SELECT delta_main, delta_hold, reason, created_at
        FROM ledger
        WHERE user_id=? AND created_at>=?
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (int(user_id), int(since), int(max_rows)),
    )
    led = cur.fetchall()

    con.close()

    events = []

    for p in payouts:
        method = ((p[2] if isinstance(p, (tuple, list)) else (p["method"] or "upi")) or "upi").lower()
        upi_or_qr = (p[3] if isinstance(p, (tuple, list)) else (p["upi_or_qr"] or ""))
        created_at = int(p[4] if isinstance(p, (tuple, list)) else (p["created_at"] or 0))
        state = (p[5] if isinstance(p, (tuple, list)) else (p["state"] or ""))
        amt_inr = float(p[0] if isinstance(p, (tuple, list)) else (p["amount"] or 0))
        amt_usd = float(p[1] if isinstance(p, (tuple, list)) else (p["amount_usd"] or 0.0))

        if method == "crypto":
            if amt_usd <= 0:
                amt_usd = inr_to_usd_fixed(amt_inr)
            inr_fixed = usd_to_inr_fixed(amt_usd)
            snip = upi_or_qr[:22] + ("..." if len(upi_or_qr) > 22 else "")
            line = f"• CRYPTO ${amt_usd:.2f} (₹{inr_fixed}) | {snip} | {fmt_ts(created_at)} | {state}"
        else:
            snip = upi_or_qr[:22] + ("..." if len(upi_or_qr) > 22 else "")
            line = f"• UPI ₹{int(amt_inr)} | {snip} | {fmt_ts(created_at)} | {state}"

        events.append({"ts": created_at, "line": line})

    for e in led:
        dm = float(e[0] if isinstance(e, (tuple, list)) else (e["delta_main"] or 0))
        dh = float(e[1] if isinstance(e, (tuple, list)) else (e["delta_hold"] or 0))
        reason = (e[2] if isinstance(e, (tuple, list)) else (e["reason"] or "")).strip()
        created_at = int(e[3] if isinstance(e, (tuple, list)) else (e["created_at"] or 0))

        hold_part = f"Hold: {'+' if dh>0 else ''}{dh:.2f}₹" if dh != 0 else "Hold: 0₹"
        main_part = f"Main: {'+' if dm>0 else ''}{dm:.2f}₹" if dm != 0 else "Main: 0₹"
        line = f"• {reason}\n  {hold_part} | {main_part}\n  Date: {fmt_ts(created_at)}"
        events.append({"ts": created_at, "line": line})

    events.sort(key=lambda x: int(x["ts"]), reverse=True)
    return events

def balance_history_page_text(user_id: int, page: int = 1, per_page: int = 5):
    events = _fetch_balance_history_events(user_id, days=90, max_rows=500)
    if not events:
        return "🧾 BALANCE HISTORY\n\n(No transactions yet)", 1

    per_page = max(3, int(per_page))
    total_pages = max(1, int(math.ceil(len(events) / float(per_page))))
    page = max(1, min(int(page), total_pages))
    start = (page - 1) * per_page
    chunk = events[start:start + per_page]

    lines = [f"🧾 BALANCE HISTORY (page {page}/{total_pages})\n📅 Last 30 days", ""]
    lines.extend([c["line"] for c in chunk])
    return "\n\n".join(lines), total_pages

def balance_history_kb(page: int, total_pages: int) -> InlineKeyboardMarkup | None:
    page = int(page)
    total_pages = int(total_pages)
    btns = []
    if total_pages <= 1:
        return None
    row = []
    if page > 1:
        row.append(InlineKeyboardButton(f"<< Back ({page-1}/{total_pages})", callback_data=f"BH:{page-1}"))
    if page < total_pages:
        row.append(InlineKeyboardButton(f"Next ({page+1}/{total_pages}) >>", callback_data=f"BH:{page+1}"))
    if row:
        btns.append(row)
    return InlineKeyboardMarkup(btns) if btns else None


def _fetch_payout_proof_rows(limit: int | None = None, days: int | None = None):
    """
    Fetch payout proof rows for PDF generation.
    - Only completed payouts that have a row in payout_proofs.
    - If days is provided, restrict to last `days` days (by payout_proofs.created_at).
    - If limit is provided, return last N payouts (by payout_proofs.created_at DESC).
    """
    con = db()
    cur = con.cursor()
    params = []
    where = ["p.state='completed'"]

    if days is not None:
        since = int(time.time()) - int(days) * 24 * 3600
        where.append("pp.created_at>=?")
        params.append(int(since))

    sql = f"""
        SELECT
            p.id AS payout_id,
            p.user_id AS user_id,
            COALESCE(p.method,'upi') AS method,
            COALESCE(p.amount,0) AS amount_inr,
            COALESCE(p.amount_usd,0) AS amount_usd,
            COALESCE(p.upi_or_qr,'') AS upi_or_wallet,
            COALESCE(p.state,'') AS state,
            COALESCE(pp.utr,'') AS utr_or_txid,
            COALESCE(pp.proof_file_id,'') AS proof_file_id,
            COALESCE(pp.created_at, p.created_at, 0) AS proof_time
        FROM payouts p
        JOIN payout_proofs pp ON pp.payout_id = p.id
        WHERE {' AND '.join(where)}
        ORDER BY pp.created_at DESC
    """
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))

    cur.execute(sql, tuple(params))
    rows = cur.fetchall()
    con.close()
    return rows


def generate_payout_proofs_pdf_from_db(pdf_path: str, limit: int | None = None, days: int | None = None) -> str:
    """
    Build payout_proofs PDF directly from DB (no local persistence required).
    Returns the pdf_path.
    """
    rows = _fetch_payout_proof_rows(limit=limit, days=days)

    styles = getSampleStyleSheet()
    doc = SimpleDocTemplate(pdf_path, pagesize=landscape(A4))
    title = "PAYOUT PROOFS (Last N)" if limit else (f"PAYOUT PROOFS (Last {days} days)" if days else "PAYOUT PROOFS")
    elements = [Paragraph(title, styles["Title"]), Spacer(1, 10)]

    data = [[
        "PAYOUT_ID", "USER_ID", "METHOD", "AMOUNT", "UPI/WALLET", "UTR/TXID", "STATUS", "TIME"
    ]]

    for r in rows:
        method = (r["method"] or "upi").lower()
        if method == "crypto":
            amt_usd = float(r["amount_usd"] or 0.0)
            if amt_usd <= 0:
                amt_usd = inr_to_usd_fixed(float(r["amount_inr"] or 0.0))
            amt_txt = f"${amt_usd:.2f} (₹{usd_to_inr_fixed(amt_usd)})"
        else:
            amt_txt = f"₹{int(float(r['amount_inr'] or 0))}"

        upi_wallet = (r["upi_or_wallet"] or "")
        upi_wallet = upi_wallet if len(upi_wallet) <= 30 else upi_wallet[:27] + "..."

        tx = (r["utr_or_txid"] or "")
        tx = tx if len(tx) <= 28 else tx[:25] + "..."

        t = fmt_ts(int(r["proof_time"] or 0)) if (r["proof_time"] or 0) else "-"

        data.append([
            str(r["payout_id"]),
            str(r["user_id"]),
            "CRYPTO" if method == "crypto" else "UPI",
            amt_txt,
            upi_wallet,
            tx,
            str(r["state"] or ""),
            t,
        ])

    table = Table(data, repeatRows=1, colWidths=[60, 60, 55, 90, 170, 150, 70, 90])
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

def save_form_row(reg_id: int, user_id: int, first_name: str, email: str, password: str, recovery_email: str, created_at: int, twofa_key: str = ""):
    con = db()
    cur = con.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO form_table(reg_id, user_id, first_name, email, password, recovery_email, twofa_key, created_at) "
        "VALUES(?,?,?,?,?,?,?,?)",
        (reg_id, user_id, first_name, email.lower(), password, recovery_email, twofa_key or "", created_at),
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
    # Admin-only: export form_table as PDF with filters (menu-style)
    if update.effective_user.id != ADMIN_ID:
        return
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("TODAY", callback_data="EXP_FORM:today"),
         InlineKeyboardButton("YESTERDAY", callback_data="EXP_FORM:yesterday")],
        [InlineKeyboardButton("ENTER DATE (FROM–TO)", callback_data="EXP_FORM:range")],
        [InlineKeyboardButton("1 MONTH AGO", callback_data="EXP_FORM:month")],
        [InlineKeyboardButton("ALL", callback_data="EXP_FORM:all")],
    ])
    await update.message.reply_text("📤 Export form_table to PDF. Choose range:", reply_markup=kb)


# ─────────────────────────────────────────────
# NEW ADMIN FEATURES
# ─────────────────────────────────────────────

def admin_bot_stats() -> str:
    """Return a formatted stats summary for admin."""
    from datetime import timezone, timedelta
    IST = timezone(timedelta(hours=5, minutes=30))
    now_dt = datetime.now(IST)
    # IST midnight (today's start) as UTC timestamp
    ist_midnight = now_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    today_start = int(ist_midnight.timestamp())

    con = db()
    cur = con.cursor()

    cur.execute("SELECT COUNT(*) FROM users")
    total_users = int(cur.fetchone()[0] or 0)

    cur.execute("SELECT COUNT(*) FROM users WHERE created_at >= ?", (today_start,))
    today_users = int(cur.fetchone()[0] or 0)

    cur.execute("SELECT COUNT(*) FROM form_table")
    total_forms = int(cur.fetchone()[0] or 0)

    cur.execute("SELECT COUNT(*) FROM form_table WHERE created_at >= ?", (today_start,))
    today_forms = int(cur.fetchone()[0] or 0)

    cur.execute("SELECT COUNT(*) FROM payouts WHERE state='pending'")
    pending_payouts = int(cur.fetchone()[0] or 0)

    cur.execute("SELECT COUNT(*) FROM payouts WHERE state='approved'")
    approved_payouts = int(cur.fetchone()[0] or 0)

    cur.execute("SELECT COALESCE(SUM(amount),0) FROM payouts WHERE state='approved'")
    total_paid_inr = float(cur.fetchone()[0] or 0)

    cur.execute("SELECT COUNT(*) FROM users WHERE main_balance > 0")
    users_with_balance = int(cur.fetchone()[0] or 0)

    cur.execute("SELECT COALESCE(SUM(main_balance),0) FROM users")
    total_main_bal = float(cur.fetchone()[0] or 0)

    cur.execute("SELECT COALESCE(SUM(hold_balance),0) FROM users")
    total_hold_bal = float(cur.fetchone()[0] or 0)

    con.close()

    lines = [
        "📊 *BOT STATISTICS*",
        "",
        "👤 *Users*",
        f"  • Total: `{total_users}`",
        f"  • Joined Today: `{today_users}`",
        f"  • With Balance: `{users_with_balance}`",
        "",
        "📋 *Form Submissions*",
        f"  • Total: `{total_forms}`",
        f"  • Today: `{today_forms}`",
        "",
        "💳 *Payouts*",
        f"  • Pending: `{pending_payouts}`",
        f"  • Approved: `{approved_payouts}`",
        f"  • Total Paid: `₹{total_paid_inr:,.0f}`",
        "",
        "💰 *Balances (All Users)*",
        f"  • Total Main: `₹{total_main_bal:,.2f}`",
        f"  • Total Hold: `₹{total_hold_bal:,.2f}`",
        "",
        f"🕐 `{now_dt.strftime('%d %b %Y %I:%M %p')} IST`",
    ]
    return "\n".join(lines)


def export_users_csv() -> str:
    """Export all users to a CSV string."""
    import csv, io
    con = db()
    cur = con.cursor()
    cur.execute("""
        SELECT u.user_id, u.username, u.main_balance, u.hold_balance,
               u.currency, u.created_at,
               (SELECT COUNT(*) FROM referral_bonuses rb WHERE rb.referrer_id=u.user_id) AS referral_count
        FROM users u
        ORDER BY u.created_at DESC
    """)
    rows = cur.fetchall()
    con.close()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["UserID", "Username", "Main Balance (INR)", "Hold Balance (INR)", "Currency", "Joined At", "Referrals"])
    for r in rows:
        try:
            joined = datetime.fromtimestamp(int(r["created_at"])).strftime("%Y-%m-%d %H:%M")
        except Exception:
            joined = str(r["created_at"])
        writer.writerow([
            r["user_id"],
            r["username"] or "",
            f"{float(r['main_balance'] or 0):.2f}",
            f"{float(r['hold_balance'] or 0):.2f}",
            r["currency"] or "INR",
            joined,
            r["referral_count"] or 0,
        ])
    return output.getvalue()


def search_form_by_email(email_query: str):
    """Search form_table rows matching email (partial)."""
    con = db()
    cur = con.cursor()
    cur.execute("""
        SELECT user_id, first_name, email, recovery_email, password, created_at
        FROM form_table
        WHERE lower(email) LIKE lower(?)
        ORDER BY id DESC
        LIMIT 20
    """, (f"%{email_query.strip()}%",))
    rows = cur.fetchall()
    con.close()
    return rows


def export_form_csv(rows) -> str:
    """Export form rows to CSV string."""
    import csv, io
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["#", "UserID", "FirstName", "Email", "Password", "Recovery Email", "2FA Key", "Time"])
    for i, r in enumerate(rows, 1):
        try:
            t = datetime.fromtimestamp(int(r["created_at"])).strftime("%Y-%m-%d %H:%M")
        except Exception:
            t = str(r["created_at"])
        writer.writerow([i, r["user_id"], r["first_name"] or "", r["email"] or "",
                         r["password"] or "", r["recovery_email"] or "",
                         r.get("twofa_key") or "", t])
    return output.getvalue()


def _fetch_form_rows(limit: int = 50):
    con = db()
    cur = con.cursor()
    cur.execute(
        "SELECT user_id, first_name, email, recovery_email, password, twofa_key, created_at "
        "FROM form_table ORDER BY id DESC LIMIT ?",
        (limit,),
    )
    rows = cur.fetchall()
    con.close()
    return rows


def _fetch_form_rows_range(start_ts: int | None = None, end_ts: int | None = None, limit: int | None = None):
    """Fetch rows from form_table optionally filtered by created_at [start_ts, end_ts)."""
    con = db()
    cur = con.cursor()
    q = "SELECT user_id, first_name, email, recovery_email, password, twofa_key, created_at FROM form_table"
    params = []
    where = []
    if start_ts is not None:
        where.append("created_at >= ?")
        params.append(int(start_ts))
    if end_ts is not None:
        where.append("created_at < ?")
        params.append(int(end_ts))
    if where:
        q += " WHERE " + " AND ".join(where)
    q += " ORDER BY id DESC"
    if limit is not None:
        q += " LIMIT ?"
        params.append(int(limit))
    cur.execute(q, tuple(params))
    rows = cur.fetchall()
    con.close()
    return rows

def _pdf_escape(s: str) -> str:
    # Escape characters for PDF literal strings
    return s.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")

def export_form_pdf(out_path: str = "form_data.pdf", limit: int = 50, *, rows=None, start_ts=None, end_ts=None):
    """
    Create a PDF table using ReportLab: USERID | FIRSTNAME | EMAIL | PASSWORD | RECOVERY_EMAIL | TIME
    Landscape A4, multi-page, all columns visible.
    """
    if rows is None:
        _lim = limit if limit is not None else None
        rows = _fetch_form_rows_range(start_ts=start_ts, end_ts=end_ts, limit=_lim)

    # Landscape A4
    doc = SimpleDocTemplate(
        out_path,
        pagesize=landscape(A4),
        leftMargin=20, rightMargin=20,
        topMargin=30, bottomMargin=30,
    )

    styles = getSampleStyleSheet()
    small_style = styles["Normal"].clone("small")
    small_style.fontSize = 7
    small_style.leading = 9
    small_style.wordWrap = "CJK"

    header_style = styles["Normal"].clone("hdr")
    header_style.fontSize = 7
    header_style.leading = 9
    header_style.textColor = colors.white
    header_style.fontName = "Helvetica-Bold"

    headers = ["#", "USERID", "FIRSTNAME", "EMAIL", "PASSWORD", "RECOVERY EMAIL", "2FA KEY", "TIME"]
    table_data = [[Paragraph(h, header_style) for h in headers]]

    for r in rows:
        try:
            t = datetime.fromtimestamp(int(r["created_at"])).strftime("%Y-%m-%d %H:%M")
        except Exception:
            t = str(r["created_at"])
        table_data.append([
            Paragraph(str(len(table_data)), small_style),
            Paragraph(str(r["user_id"] or ""), small_style),
            Paragraph(str(r["first_name"] or ""), small_style),
            Paragraph(str(r["email"] or ""), small_style),
            Paragraph(str(r["password"] or ""), small_style),
            Paragraph(str(r["recovery_email"] or ""), small_style),
            Paragraph(str(r.get("twofa_key") or ""), small_style),
            Paragraph(t, small_style),
        ])

    # Column widths for landscape A4 (usable ~800pt)
    col_widths = [22, 45, 60, 145, 115, 140, 100, 75]

    table = Table(table_data, colWidths=col_widths, repeatRows=1)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#2d6cdf")),
        ("TEXTCOLOR",  (0, 0), (-1, 0), colors.white),
        ("FONTNAME",   (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE",   (0, 0), (-1, 0), 7),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 5),
        ("TOPPADDING",    (0, 0), (-1, 0), 5),
        ("FONTNAME",   (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE",   (0, 1), (-1, -1), 7),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f0f4ff")]),
        ("GRID",       (0, 0), (-1, -1), 0.4, colors.HexColor("#aaaaaa")),
        ("VALIGN",     (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING",  (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING",   (0, 1), (-1, -1), 3),
        ("BOTTOMPADDING",(0, 1), (-1, -1), 3),
    ]))

    total = len(table_data) - 1
    title_para = Paragraph(f"<b>Form Data Export</b>  —  {total} record(s)", styles["Normal"])
    doc.build([title_para, Spacer(1, 8), table])
    return out_path

async def formimg_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Admin-only: legacy alias for export menu
    if update.effective_user.id != ADMIN_ID:
        return
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("TODAY", callback_data="EXP_FORM:today"),
         InlineKeyboardButton("YESTERDAY", callback_data="EXP_FORM:yesterday")],
        [InlineKeyboardButton("ENTER DATE (FROM–TO)", callback_data="EXP_FORM:range")],
        [InlineKeyboardButton("1 MONTH AGO", callback_data="EXP_FORM:month")],
        [InlineKeyboardButton("ALL", callback_data="EXP_FORM:all")],
    ])
    await update.message.reply_text("📤 Export form_table to PDF. Choose range:", reply_markup=kb)




# =========================
# ADMIN SETTINGS (Runtime Config)
# =========================

def get_admin_setting(key: str, default=None):
    try:
        con = db()
        cur = con.cursor()
        cur.execute("SELECT value FROM admin_settings WHERE key=?", (key,))
        r = cur.fetchone()
        con.close()
        if not r:
            return default
        v = r[0] if isinstance(r, (tuple, list)) else r["value"]
        return v if v is not None else default
    except Exception:
        return default

def set_admin_setting(key: str, value: str):
    try:
        con = db()
        cur = con.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO admin_settings(key, value, updated_at) VALUES(?,?,?)",
            (key, str(value), int(time.time()))
        )
        con.commit()
        con.close()
    except Exception:
        pass

def get_referral_commission_pct() -> float:
    """Returns current referral commission % from admin_settings. Default = REFERRAL_COMMISSION_DEFAULT."""
    try:
        val = get_admin_setting("referral_commission_pct", str(REFERRAL_COMMISSION_DEFAULT))
        pct = float(val)
        return max(0.0, min(100.0, pct))
    except Exception:
        return float(REFERRAL_COMMISSION_DEFAULT)

# =========================
# REWARD TOGGLE HELPERS
# =========================

def get_setting(key: str, default: str = "on") -> str:
    """Get an admin_settings value by key, with default fallback."""
    return str(get_admin_setting(key, default) or default)

def set_setting(key: str, value: str) -> None:
    """Set an admin_settings key to value."""
    set_admin_setting(key, value)

def is_admin(user_id):
    return user_id == ADMIN_ID


def _ensure_blocked_users_table():
    try:
        con = db()
        cur = con.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS blocked_users(
                user_id INTEGER PRIMARY KEY,
                blocked_at INTEGER,
                reason TEXT DEFAULT ''
            )
        """)
        # Add reason column if older DB doesn't have it
        try:
            cur.execute("ALTER TABLE blocked_users ADD COLUMN reason TEXT DEFAULT ''")
        except Exception:
            pass
        con.commit()
        con.close()
    except Exception:
        pass

_ensure_blocked_users_table()

def is_blocked(user_id: int) -> bool:
    # Cache check (SPEED OPTIMIZATION) — har message pe DB hit nahi
    cached = _cache_get_blocked(user_id)
    if cached is not None:
        return cached
    try:
        con = db()
        cur = con.cursor()
        cur.execute("SELECT 1 FROM blocked_users WHERE user_id=?", (int(user_id),))
        r = cur.fetchone()
        con.close()
        result = r is not None
        _cache_set_blocked(user_id, result)
        return result
    except Exception:
        return False

def block_user_db(user_id: int, reason: str = ""):
    con = db()
    cur = con.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO blocked_users(user_id, blocked_at, reason) VALUES(?,?,?)",
        (int(user_id), int(time.time()), str(reason or "").strip())
    )
    con.commit()
    con.close()
    _cache_set_blocked(user_id, True)  # Cache update

def get_block_info(user_id: int):
    """Returns (blocked_at, reason) or None if not blocked."""
    try:
        con = db()
        cur = con.cursor()
        cur.execute("SELECT blocked_at, reason FROM blocked_users WHERE user_id=?", (int(user_id),))
        r = cur.fetchone()
        con.close()
        if not r:
            return None
        reason = ""
        blocked_at = 0
        try:
            blocked_at = int(r[0] if isinstance(r, (tuple, list)) else r["blocked_at"])
        except Exception:
            pass
        try:
            reason = str(r[1] if isinstance(r, (tuple, list)) else r["reason"] or "")
        except Exception:
            pass
        return blocked_at, reason
    except Exception:
        return None

def unblock_user_db(user_id: int):
    con = db()
    cur = con.cursor()
    cur.execute("DELETE FROM blocked_users WHERE user_id=?", (int(user_id),))
    con.commit()
    con.close()
    _cache_set_blocked(user_id, False)  # Cache update

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
    if now > a["expires_at"] or a["state"] in ("timeout", "approved", "rejected", "canceled"):
        return False, a

    return True, a

def _db_write_retry(fn, retries: int = 3, base_sleep: float = 0.05):
    """Serialize DB writes + tiny retries.

    - Uses a process-wide re-entrant lock so admin/user callbacks don't write concurrently.
    - Catches both sqlite3 and psycopg2 OperationalError (PostgreSQL compatibility fix).
    """
    import psycopg2 as _psycopg2
    _op_errors = (sqlite3.OperationalError, _psycopg2.OperationalError)
    for _ in range(int(retries)):
        try:
            with DB_WRITE_LOCK:
                return fn()
        except _op_errors as e:
            if "locked" in str(e).lower() or "could not obtain" in str(e).lower():
                time.sleep(float(base_sleep))
                continue
            raise
    with DB_WRITE_LOCK:
        return fn()


def set_action_state_cur(cur, action_id: int, state: str):
    now = int(time.time())
    try:
        cur.execute(
            "UPDATE actions SET state=?, updated_at=? WHERE action_id=?",
            (state, now, int(action_id)),
        )
    except Exception:
        cur.execute(
            "UPDATE actions SET state=? WHERE action_id=?",
            (state, int(action_id)),
        )


def set_reg_state_cur(cur, reg_id: int, state: str):
    now = int(time.time())
    try:
        cur.execute(
            "UPDATE registrations SET state=?, updated_at=? WHERE id=?",
            (state, now, int(reg_id)),
        )
    except Exception:
        cur.execute(
            "UPDATE registrations SET state=? WHERE id=?",
            (state, int(reg_id)),
        )


def add_ledger_entry_cur(cur, user_id: int, delta_main: float = 0.0, delta_hold: float = 0.0, reason: str = ""):
    cur.execute(
        "INSERT INTO ledger(user_id, delta_main, delta_hold, reason, created_at) VALUES(?,?,?,?,?)",
        (int(user_id), float(delta_main), float(delta_hold), str(reason or ""), int(time.time())),
    )


def add_hold_credit_cur(cur, user_id: int, amount: float):
    """Add amount to HOLD and create a hold_credits row using the SAME cursor/connection.
    Returns (hold_credit_id, matured_at) so caller can schedule instant trigger."""
    now = int(time.time())
    matured_at = now + int(float(HOLD_TO_MAIN_AFTER_DAYS) * 24 * 3600)
    cur.execute("UPDATE users SET hold_balance = hold_balance + ? WHERE user_id=?", (float(amount), int(user_id)))
    add_ledger_entry_cur(cur, int(user_id), delta_hold=float(amount), reason="HOLD credit added")
    cur.execute(
        "INSERT INTO hold_credits(user_id, amount, created_at, matured_at, moved) VALUES(?,?,?,?,0)",
        (int(user_id), float(amount), now, matured_at),
    )
    return int(cur.lastrowid), matured_at


def revert_hold_credit_cur(cur, hold_credit_id: int, user_id: int, amount: float) -> None:
    """Revert HOLD credit using SAME cursor/connection (avoid nested connections -> locks)."""
    cur.execute("UPDATE hold_credits SET moved=1 WHERE id=? AND user_id=?", (int(hold_credit_id), int(user_id)))
    cur.execute("SELECT hold_balance FROM users WHERE user_id=?", (int(user_id),))
    r = cur.fetchone()
    hb = float(r[0]) if r else 0.0
    new_hb = hb - float(amount)
    if new_hb < 0:
        new_hb = 0.0
    cur.execute("UPDATE users SET hold_balance=? WHERE user_id=?", (float(new_hb), int(user_id)))

def set_action_state(action_id, state):
    def _op():
        con = db()
        cur = con.cursor()
        set_action_state_cur(cur, int(action_id), str(state))
        con.commit()
        con.close()
    _db_write_retry(_op)


def set_reg_state(reg_id, state):
    def _op():
        con = db()
        cur = con.cursor()
        set_reg_state_cur(cur, int(reg_id), str(state))
        con.commit()
        con.close()
    _db_write_retry(_op)


# =========================
# START
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    try:
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

        # Save referral before gate check
        if user and ref:
            set_pending_ref(user.id, ref)

        # Check if already joined — existing users go straight to menu
        already_joined = await user_in_required_channels(context, user.id)

        if already_joined:
            if user:
                pending = pop_pending_ref(user.id)
                if not ref and pending:
                    ref = pending
                ensure_user(user.id, user.username or user.full_name, referrer_id=ref)
            await update.message.reply_text(tr(user.id, "welcome_menu"), reply_markup=main_menu_markup(user.id))
            return

        # New/unjoined user — show welcome + channel join
        welcome_text = (
            "👋 Yaar, sahi jagah aaye ho!\n\n"
            "💰 Gmail Account Banao — Paisa Kamao!\n\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "🎯 Kaam kya hai?\n"
            "Bas Gmail account banane hai — aur tumhe milenge paise seedha UPI mein!\n\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "💵 Kitna milega?\n"
            "✅ Har Gmail ke → ₹08 - ₹10\n"
            "👥 Dost refer karo → Extra bonus\n"
            "🏆 Jitna karo, utna kamao — koi limit nahi!\n\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "⚡ Shuru kaise karein?\n"
            "1️⃣ Neeche diye channels join karo\n"
            "2️⃣ ✅ I JOINED button dabao\n"
            "3️⃣ Bot tumhe sab sikhayega — step by step!\n\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "📢 Channels join karna kyun zaroori hai?\n"
            "🔔 Naye tasks aur updates sabse pehle channel pe aate hain\n"
            "💡 Tips & tricks milti hain jisse aur zyada kamao\n"
            "🛡️ Sirf channel members ko hi bot ka access milta hai\n"
            "👨\u200d👩\u200d👧 Hamaari community se judo — akele mat karo!\n\n"
            "➡️ In channels ko join karo aur apni earning shuru karo:"
        )
        await update.message.reply_text(welcome_text, reply_markup=join_channels_kb())

    except Exception as e:
        import traceback
        print(f"[START ERROR] {e!r}\n{traceback.format_exc()}", flush=True)
        try:
            # Retry ensure_user once before giving up
            if user:
                try:
                    ensure_user(user.id, user.username or user.full_name)
                except Exception:
                    pass
            await update.message.reply_text(
                tr(user.id if user else 0, "welcome_menu"),
                reply_markup=main_menu_markup(user.id if user else 0)
            )
        except Exception:
            pass



async def menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await gate_if_not_joined(update, context):
        return
    if user.id != ADMIN_ID and is_blocked(user.id):
        return

    ensure_user(user.id, user.username or user.full_name)

    # ── QR photo in 2FA wait state → auto-extract secret ────────────────────
    if (
        context.user_data.get("sell_state") == STATE_WAIT_2FA
        and update.message
        and update.message.photo
    ):
        await _handle_qr_photo_for_2fa(update, context)
        return

    txt = update.message.text.strip()

    # Auto-reply (admin configurable)
    if not is_admin(user.id):
        if txt and not txt.startswith("/"):
            # only if not in specific flows
            if (
                not context.user_data.get("reg_flow")
                and not context.user_data.get("await_upi")
                and not context.user_data.get("await_crypto_addr")
                and not context.user_data.get("await_crypto_amt")
            ):
                try:
                    await send_configured_autoreply(update, context)
                except Exception:
                    pass

    # ── Sell flow text input handler ──────────────────────────────────
    if context.user_data.get("sell_state"):
        handled = await _handle_sell_flow(update, context)
        if handled:
            return

    
    if txt_is(txt, "menu_register"):
        await register(update, context)
        return

    if txt_is(txt, "menu_accounts"):
        _ALL_ST = "('waiting_admin','approved','rejected')"
        dprint(f"[ACCOUNTS-MENU] user={user.id} tapped My Accounts", flush=True)
        con = db()
        cur = con.cursor()
        try:
            # DEBUG: check raw actions count for this user (no state filter)
            cur.execute("SELECT COUNT(*) FROM actions WHERE user_id = ?", (user.id,))
            raw_row = cur.fetchone()
            raw_total = int(raw_row[0] if raw_row else 0)
            dprint(f"[ACCOUNTS-MENU] user={user.id} raw actions count (no filter)={raw_total}", flush=True)

            # DEBUG: show all action states for this user
            cur.execute("SELECT action_id, state, reg_id FROM actions WHERE user_id = ?", (user.id,))
            all_actions = cur.fetchall()
            for aa in all_actions:
                dprint(f"[ACCOUNTS-MENU] action_id={aa['action_id']} state={aa['state']} reg_id={aa['reg_id']}", flush=True)

            cur.execute(f"""
                SELECT COUNT(*) FROM actions a
                JOIN registrations r ON r.id = a.reg_id
                WHERE a.user_id = ? AND a.state IN {_ALL_ST}
            """, (user.id,))
            total_row = cur.fetchone()
            total = int(total_row[0] if total_row else 0)
            dprint(f"[ACCOUNTS-MENU] user={user.id} filtered total={total}", flush=True)

            cur.execute(f"""
                SELECT
                    r.id AS reg_id,
                    r.email AS email,
                    a.action_id AS action_id,
                    a.state AS astate,
                    COALESCE(a.updated_at, a.created_at) AS stime,
                    a.created_at AS sub_ts,
                    ev.status AS ev_status,
                    ev.reason AS ev_reason,
                    r.state AS rstate,
                    hc.matured_at AS matured_at
                FROM actions a
                JOIN registrations r ON r.id = a.reg_id
                LEFT JOIN admin_email_verify ev ON ev.action_id = a.action_id
                LEFT JOIN precredits pc ON pc.action_id = a.action_id
                LEFT JOIN hold_credits hc ON hc.id = pc.hold_credit_id
                WHERE a.user_id = ? AND a.state IN {_ALL_ST}
                ORDER BY COALESCE(a.updated_at, a.created_at) DESC
                LIMIT 5 OFFSET 0
            """, (user.id,))
            rows = cur.fetchall()
            dprint(f"[ACCOUNTS-MENU] user={user.id} rows returned={len(rows)}", flush=True)
            con.commit()
        except Exception as e:
            try: con.rollback()
            except Exception: pass
            dprint(f"[ACCOUNTS-MENU] DB error: {e!r}", flush=True)
            rows = []
            total = 0
        finally:
            try: con.close()
            except Exception: pass

        if not rows:
            await update.message.reply_text(tr(user.id, "my_accounts_empty"))
            return

        now_ts = int(time.time())
        lines = _build_account_lines(rows, user.id, now_ts)
        total_pages = max(1, (total + 4) // 5)
        header = tr(user.id, "my_accounts_title", page=1, total_pages=total_pages)
        msg = header + "\n\n" + "\n\n".join(lines)
        if len(msg) > 4000:
            msg = msg[:4000] + "\n..."
        await update.message.reply_text(msg, reply_markup=accounts_nav(0, total))
        return
    
    if txt_is(txt, "menu_balance"):
        mainb, holdb = get_balances(user.id)
        cur_code = get_user_currency(user.id)

        # exactly TWO TEXT lines requested
        if cur_code and cur_code != "INR":
            # If rate missing / API blocked, show N/A instead of wrong "same amount"
            _refresh_rates_if_needed()
            _rate_ok = bool((_rates_cache.get("rates") or {}).get(cur_code))
            main_conv = convert_inr(mainb, cur_code)
            hold_conv = convert_inr(holdb, cur_code)
            main_disp = fmt_money(main_conv, cur_code) if _rate_ok else "N/A"
            hold_disp = fmt_money(hold_conv, cur_code) if _rate_ok else "N/A"

            await update.message.reply_text(
                f"MAIN BALANCE= ₹{mainb:.2f} (≈ {main_disp})\n"
                f"HOLD BALANCE= ₹{holdb:.2f} (≈ {hold_disp})",
                reply_markup=balance_menu(user.id)
            )
        else:
            await update.message.reply_text(
                f"MAIN BALANCE= ₹{mainb:.2f}\n"
                f"HOLD BALANCE= ₹{holdb:.2f}",
                reply_markup=balance_menu(user.id)
            )
        return

    if txt_is(txt, "menu_profile"):
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
            f"{tr(user.id, 'total_ref_earned', value=total_bonus)}"
        )

        await update.message.reply_text(
            msg,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton(tr(user.id, "profile_back"), callback_data="PROFILE_BACK")]]
            )
        )
        return

    if txt_is(txt, "payout"):
        # Payout submenu inside Balance (REPLY ONLY flow)
        context.user_data["payout_reply_mode"] = "menu"
        context.user_data["payout_type_select"] = True
        context.user_data["await_upi"] = False
        context.user_data["await_crypto_addr"] = False
        context.user_data["await_crypto_amt"] = False
        await update.message.reply_text(
            tr(user.id, "choose_withdrawal"),
            reply_markup=payout_menu_kb(user.id)
        )
        return

    # Payout submenu choices (reply keyboard)
    if context.user_data.get("payout_type_select") and context.user_data.get("payout_reply_mode") == "menu" and (
        txt_is(txt, "payout_upi") or txt in ("1. UPI",)
    ):
        context.user_data["payout_reply_mode"] = "upi"
        # Same UPI amount picker, but BACK is reply-menu only (no inline back)
        await update.message.reply_text(
            tr(user.id, "choose_amount"),
            reply_markup=payout_amounts_kb()
        )
        await update.message.reply_text(
            tr(user.id, "upi_mode"),
            reply_markup=back_only_menu(user.id)
        )
        return

    if context.user_data.get("payout_type_select") and context.user_data.get("payout_reply_mode") == "menu" and (
        txt_is(txt, "payout_crypto") or txt.startswith("2. CRYPTO")
    ):
        context.user_data["payout_reply_mode"] = "crypto"
        context.user_data["await_crypto_addr"] = True
        mainb, _holdb = get_balances(user.id)
        bal_usd = inr_to_usd_fixed(float(mainb))
        await update.message.reply_text(
            tr(user.id, "send_bep20"),
            reply_markup=back_only_menu(user.id)
        )
        return

    if context.user_data.get("payout_type_select") and (
        txt in ("🔙 BACK", "🔙 Back", "⬅️ BACK", "⬅ BACK")
        or txt_is(txt, "back")
        or txt_is(txt, "back_upper")
    ):
        mode = context.user_data.get("payout_reply_mode")

        if mode in ("upi", "crypto"):
            # Selected submenu BACK -> go to 3-option payout menu
            context.user_data["payout_reply_mode"] = "menu"
            context.user_data["await_upi"] = False
            context.user_data["await_crypto_addr"] = False
            context.user_data["await_crypto_amt"] = False
            context.user_data["payout_amt"] = 0
            context.user_data["crypto_addr"] = ""
            await update.message.reply_text(
                tr(user.id, "choose_withdrawal"),
                reply_markup=payout_menu_kb(user.id)
            )
            return
        # 3-option payout menu BACK -> Balance menu
        context.user_data.pop("payout_type_select", None)
        context.user_data.pop("payout_reply_mode", None)
        await update.message.reply_text(tr(user.id, "balance_title"), reply_markup=balance_menu(user.id))
        return
        
    if txt_is(txt, "balance_history"):
        # Paginated balance history (Next/Back buttons)
        txt0, total_pages = balance_history_page_text(user.id, page=1, per_page=5)
        await update.message.reply_text(txt0, reply_markup=balance_history_kb(1, total_pages))
        return

    if txt_is(txt, "menu_referrals"):

        bot_username = context.bot.username
        link = referral_link(bot_username, user.id)

        total_ref, total_earned, rows = get_referral_overview(user.id, limit=10)

        commission_pct = get_referral_commission_pct()
        commission_per = round(float(PRE_CREDIT_AMOUNT) * commission_pct / 100.0, 2)

        lines = []
        for x in rows:
            uname = x.get("username") or str(x.get("user_id"))
            joined = fmt_ts(int(x["joined_at"])) if x.get("joined_at") else "-"
            approved = int(x.get("approved_count") or 0)
            paid = "✅" if int(x.get("bonus_paid") or 0) == 1 else "⏳"
            lines.append(tr(user.id, "referral_row", uname=uname, joined=joined, approved=approved, paid=paid))

        details = "\n\n" + "\n".join(lines) if lines else "\n\n" + tr(user.id, "no_referrals")

        # Top 3 referrals for leaderboard
        top3_lines = []
        medals = ["🥇", "🥈", "🥉"]
        for idx, x in enumerate(rows[:3]):
            uname = x.get("username") or str(x.get("user_id"))
            approved = int(x.get("approved_count") or 0)
            earned = round(approved * commission_per, 2)
            top3_lines.append(f"{medals[idx]} @{uname}  |  {approved} 🟢 Accepted  |  ₹{earned:.2f}")
        top3_str = "\n".join(top3_lines) if top3_lines else "— no referrals yet —"

        # Active = referred users with at least 1 approval
        active_count = sum(1 for x in rows if int(x.get("approved_count") or 0) > 0)

        msg = (
            f"💼 REFERRAL DASHBOARD\n"
            f"══════════════════════\n"
            f"👥  Referred     →  {total_ref} users\n"
            f"✅  Active       →  {active_count} users\n"
            f"💸  Earned       →  ₹{total_earned:.2f}\n"
            f"📈  Commission   →  {commission_pct:.1f}% / approval\n\n"
            f"Per approval earn: ₹{commission_per:.2f}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔗 Referral Link:\n{link}\n\n"
            f"Top referrals:\n{top3_str}"
        )
        await update.message.reply_text(msg)
        return


    if txt_is(txt, "menu_settings"):
        cur = get_user_currency(user.id)
        await update.message.reply_text(f"Settings:\n💱 Currency: {cur}", reply_markup=settings_menu())
        return

    if txt_is(txt, "settings_language"):
        await update.message.reply_text(tr(user.id, "choose_language"), reply_markup=language_menu(user.id))
        return

    if txt_is(txt, "settings_currency"):
        cur = get_user_currency(user.id)
        await update.message.reply_text(tr(user.id, "choose_currency", cur=cur), reply_markup=currency_kb())
        return

    # Currency selection
    if any(txt == label for _, label in CURRENCY_CHOICES):
        code = None
        for c, label in CURRENCY_CHOICES:
            if txt == label:
                code = c
                break
        set_user_currency(user.id, code or "INR")
        await update.message.reply_text(tr(user.id, "currency_set", code=code), reply_markup=settings_menu(user.id))
        return

    if txt in any_label("lang_en") + any_label("lang_hi") + any_label("lang_ur"):
        if txt in any_label("lang_en"):
            set_lang(user.id, "en")
            await update.message.reply_text(tr(user.id, "language_set_en"), reply_markup=main_menu_markup(user.id))
        elif txt in any_label("lang_hi"):
            set_lang(user.id, "hi")
            await update.message.reply_text(tr(user.id, "language_set_hi"), reply_markup=main_menu_markup(user.id))
        else:
            set_lang(user.id, "ur")
            await update.message.reply_text(tr(user.id, "language_set_ur"), reply_markup=main_menu_markup(user.id))
        return

    if txt_is(txt, "menu_task"):
        await update.message.reply_text(task_menu_text(user.id))
        return
 
    if txt_is(txt, "menu_help"):

        await update.message.reply_text(tr(user.id, "help_menu"), reply_markup=help_menu_kb(user.id))
        return

    if txt_is(txt, "back"):
        await update.message.reply_text(tr(user.id, "main_menu_text"), reply_markup=main_menu_markup(user.id))
        return


    if txt == "💳 PAYOUT REQUEST":
        # Show processing payouts and allow selection
        con = db()
        cur = con.cursor()
        cur.execute("SELECT id, user_id, amount, amount_usd, upi_or_qr, created_at, state FROM payouts WHERE state='processing' AND (method='upi' OR method IS NULL OR method='') ORDER BY id DESC LIMIT 10")
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

    
    if txt == "💳 CRYPTO REQUEST":
        # Show processing CRYPTO payouts and allow selection
        con = db()
        cur = con.cursor()
        cur.execute("""
            SELECT id, user_id, amount, amount_usd, upi_or_qr, created_at, state
            FROM payouts
            WHERE state='processing' AND method='crypto'
            ORDER BY id DESC LIMIT 10
        """)
        rows = cur.fetchall()
        con.close()
        if not rows:
            await update.message.reply_text("No PROCESSING crypto requests.", reply_markup=ADMIN_MENU_KB)
            return

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"#{r['id']} | ${float(r['amount_usd'] or 0.0):.2f} | {r['user_id']}", callback_data=f"PAY_SEL:{r['id']}")]
            for r in rows
        ])
        await update.message.reply_text("Select a CRYPTO request:", reply_markup=kb)
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
            generate_payout_proofs_pdf_from_db("payout_proofs.pdf", 200, None)
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
    
    if txt == "📄 PDF Last 30 Days":
        if update.effective_user.id != ADMIN_ID:
            return
        pdf_path = "payout_proofs.pdf"
        try:
            await asyncio.to_thread(generate_payout_proofs_pdf_from_db, pdf_path, None, 30)
            await update.message.reply_document(document=open(pdf_path, "rb"), filename="payout_proofs.pdf", reply_markup=ADMIN_MENU_KB)
        except Exception as e:
            await update.message.reply_text(f"❌ Failed to build/send PDF: {e}", reply_markup=ADMIN_MENU_KB)
        return

    if txt == "📄 PDF Last N Payouts":
        if update.effective_user.id != ADMIN_ID:
            return
        context.user_data["admin_mode"] = "pdf_lastn_wait"
        await update.message.reply_text("Send N (1-500):", reply_markup=ADMIN_MENU_KB)
        return
        try:
            await update.message.reply_document(
                document=open("payout_proofs.pdf", "rb"),
                reply_markup=ADMIN_MENU_KB
            )
        except Exception as e:
            await update.message.reply_text(f"❌ Failed to send PDF: {e}", reply_markup=ADMIN_MENU_KB)
        return


# ================ GENERATORS ===============
import random

# ----------------------------
# NAME GENERATION
# ----------------------------

VOWELS = "aeiou"
CONSONANTS = "bcdfghjklmnpqrstvwxyz"

def random_first_name():
    length = random.choice([4, 5, 6, 7])
    name = ""
    for i in range(length):
        if i % 2 == 0:
            name += random.choice(CONSONANTS)
        else:
            name += random.choice(VOWELS)
    return name.capitalize()

def random_last_name():
    length = random.choice([4, 5,])
    name = ""
    for i in range(length):
        if i % 2 == 0:
            name += random.choice(CONSONANTS)
        else:
            name += random.choice(VOWELS)
    return name.capitalize()


# ----------------------------
# PRONOUNCEABLE WORD GENERATOR
# ----------------------------

VOWELS = "aeiou"
CONSONANTS = "bcdfghjklmnpqrstvwxyz"


def make_pronounceable(length):
    word = ""
    use_consonant = random.choice([True, False])

    while len(word) < length:
        if use_consonant:
            word += random.choice(CONSONANTS)
        else:
            word += random.choice(VOWELS)

        use_consonant = not use_consonant

    return word[:length]


# ----------------------------
# EMAIL
# Word length: 8-18
# Number only at end
# ----------------------------

def random_email(first_name, last_name):

    def rand_num():
        return random.choice([
            str(random.randint(1, 9)),         # 1 digit
            str(random.randint(10, 99)),       # 2 digit
            str(random.randint(100, 999)),     # 3 digit
            str(random.randint(1000, 9999))    # 4 digit
        ])

    # ONLY word part length random
    word_length = random.randint(8, 15)

    # pronounceable username
    username = make_pronounceable(word_length)

    # add number only at end
    username += rand_num()

    return f"{username}@gmail.com"


# ----------------------------
# STRONG PASSWORD (no 0, no l)
# Length: 8 to 15
# ----------------------------

def strong_password():
    length = random.choice([10, 11, 12, 13, 14, 15])

    uppercase = "ABCDEFGHJKLMNPQRSTUVWXYZ"
    lowercase = "abcdefghijkmnopqrstuvwxyz"
    numbers = "123456789"
    symbols = "!@#$&+%₹?"

    all_chars = uppercase + lowercase + numbers + symbols

    pwd = [
        random.choice(uppercase),
        random.choice(uppercase),
        random.choice(lowercase),
        random.choice(lowercase),
        random.choice(numbers),
        random.choice(numbers),
        random.choice(symbols),
    ]

    pwd += random.choices(all_chars, k=length - len(pwd))

    random.shuffle(pwd)

    return "".join(pwd)


# ----------------------------
# RECOVERY EMAIL
# pronounceable + numbers
# ----------------------------

def random_recovery_email():

    def rand_num():
        return random.choice([
            str(random.randint(1, 99)),
            str(random.randint(100, 999)),
            str(random.randint(1000, 9999))
        ])

    word_length = random.randint(6, 14)

    username = make_pronounceable(word_length)

    username += rand_num()

    return f"{username}@xyzbaazar.com"


# =========================
# SELL FLOW — DUPLICATE CHECK HELPERS
# =========================

def _email_exists_in_db(email: str) -> bool:
    try:
        con = db(); cur = con.cursor()
        cur.execute("SELECT id FROM registrations WHERE email=?", (email.lower().strip(),))
        r = cur.fetchone(); con.close()
        return r is not None
    except Exception:
        return False

def _password_exists_in_db(password: str) -> bool:
    try:
        con = db(); cur = con.cursor()
        cur.execute("SELECT id FROM registrations WHERE password=?", (password,))
        r = cur.fetchone(); con.close()
        return r is not None
    except Exception:
        return False

def _recovery_email_exists_in_db(recovery_email: str) -> bool:
    try:
        con = db(); cur = con.cursor()
        cur.execute("SELECT id FROM registrations WHERE recovery_email=?", (recovery_email.lower().strip(),))
        r = cur.fetchone(); con.close()
        return r is not None
    except Exception:
        return False

# =========================
# SELL FLOW — KEYBOARDS
# =========================

def _sell_cancel_kb():
    return ReplyKeyboardMarkup([["⊖ Cancel registration"]], resize_keyboard=True)

async def _handle_qr_photo_for_2fa(update, context):
    """User ne 2FA QR code ki photo bheji — auto-extract secret aur save karo.
    Handles: plain QR, screenshot-in-screenshot, small QR inside Google UI, etc.
    """
    import io, urllib.parse
    user = update.effective_user
    try:
        # Download highest-res photo Telegram provides
        photo_file = await update.message.photo[-1].get_file()
        photo_bytes = await photo_file.download_as_bytearray()

        secret = None
        otpauth_uri = None

        # ── Helper: parse otpauth URI → secret ───────────────────────────────
        def _extract_secret_from_uri(uri):
            try:
                parsed = urllib.parse.urlparse(uri)
                qs = urllib.parse.parse_qs(parsed.query)
                vals = qs.get("secret", [])
                return vals[0].strip().upper() if vals else None
            except Exception:
                return None

        # ── Helper: scan one PIL image — opencv first, pyzbar fallback ─────
        def _pyzbar_scan(im):
            from PIL import ImageEnhance
            import numpy as np

            gray = im.convert("L")
            candidates = [
                im,
                gray,
                ImageEnhance.Contrast(gray).enhance(2.0),
                gray.point(lambda x: 0 if x < 128 else 255),
            ]
            if im.width < 800:
                candidates.append(im.resize((im.width * 2, im.height * 2), Image.LANCZOS))

            # ── Try opencv QRCodeDetector (no system lib needed) ─────────────
            try:
                import cv2
                detector = cv2.QRCodeDetector()
                for c in candidates:
                    arr = np.array(c.convert("RGB"))
                    bgr = arr[:, :, ::-1]  # RGB → BGR
                    data, _, _ = detector.detectAndDecode(bgr)
                    if data and data.strip():
                        # Return pyzbar-compatible object mock
                        class _R:
                            def __init__(self, d): self.data = d.encode()
                        return [_R(data)]
                # Also try WeChatQRCode if available (better for dense QR)
                try:
                    wechat = cv2.wechat_qrcode_WeChatQRCode()
                    for c in candidates:
                        arr = np.array(c.convert("RGB"))
                        bgr = arr[:, :, ::-1]
                        texts, _ = wechat.detectAndDecode(bgr)
                        for t in texts:
                            if t and t.strip():
                                class _R:
                                    def __init__(self, d): self.data = d.encode()
                                return [_R(t)]
                except Exception:
                    pass
            except ImportError:
                pass

            # ── Fallback: pyzbar (needs libzbar system lib) ───────────────────
            try:
                from pyzbar.pyzbar import decode as pyzbar_decode
                for c in candidates:
                    res = pyzbar_decode(c)
                    if res:
                        return res
            except Exception:
                pass

            return []

        # ── Helper: extract all QR candidate sub-regions from image ──────────
        def _get_scan_regions(img):
            """Return list of PIL images: full + quadrant crops + centre crop."""
            W, H = img.size
            regions = [img]
            # 4 quadrants
            for left, upper, right, lower in [
                (0,     0,     W//2, H//2),
                (W//2,  0,     W,    H//2),
                (0,     H//2,  W//2, H),
                (W//2,  H//2,  W,    H),
            ]:
                regions.append(img.crop((left, upper, right, lower)))
            # Centre 60%
            margin_x, margin_y = int(W * 0.2), int(H * 0.2)
            regions.append(img.crop((margin_x, margin_y, W - margin_x, H - margin_y)))
            # Bottom-centre (Google Auth QR is usually here in screenshots)
            regions.append(img.crop((int(W*0.2), int(H*0.4), int(W*0.8), int(H*0.85))))
            return regions

        # ── Helper: parse a single scan result text ───────────────────────────
        def _parse_result_text(text):
            nonlocal secret, otpauth_uri
            if "otpauth" in text.lower():
                otpauth_uri = text
                s = _extract_secret_from_uri(text)
                if s:
                    secret = s
                return True
            elif len(text) >= 16 and text.replace("=", "").replace(" ", "").isalnum():
                secret = text.strip().upper().replace(" ", "")
                return True
            return False

        from PIL import Image
        img = Image.open(io.BytesIO(bytes(photo_bytes)))
        regions = _get_scan_regions(img)

        # ── Pass 1: pyzbar across all regions ────────────────────────────────
        try:
            for region in regions:
                results = _pyzbar_scan(region)
                for r in results:
                    text = r.data.decode("utf-8", errors="ignore")
                    if _parse_result_text(text):
                        break
                if secret or otpauth_uri:
                    break
        except Exception:
            pass


        if secret:
            temp_data.setdefault(user.id, {})["twofa"] = secret
            context.user_data["sell_state"] = STATE_CONFIRM
            await update.message.reply_text(
                f"✅ 2FA secret extracted from QR!\n\n🔑 Key: `{secret}`\n\n"
                + _sell_confirmation_text(user.id),
                parse_mode="Markdown",
                reply_markup=_sell_after_2fa_kb()
            )
        else:
            await update.message.reply_text(
                "❌ Could not read QR code. Please enter your 2FA key manually:",
                reply_markup=_sell_cancel_kb()
            )
    except Exception as e:
        await update.message.reply_text(
            f"❌ QR scan error. Please enter your 2FA key manually:",
            reply_markup=_sell_cancel_kb()
        )

def _sell_entry_kb():
    return ReplyKeyboardMarkup([
        ["❤️Single Gmail", "📋 Bulk submit"],
        ["⊖ Cancel registration"],
    ], resize_keyboard=True)

def _sell_after_recovery_kb():
    return ReplyKeyboardMarkup([
        ["🔒 Add 2FA key"],
        ["✓ Done (without 2FA)"],
        ["⊖ Cancel registration"],
    ], resize_keyboard=True)

def _sell_after_2fa_kb():
    """Keyboard shown AFTER user has already entered 2FA key."""
    return ReplyKeyboardMarkup([
        ["✅ Submit"],
        ["🔒 Change 2FA key"],
        ["⊖ Cancel registration"],
    ], resize_keyboard=True)

def _sell_submit_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ SUBMIT", callback_data="FINAL_SUBMIT")],
    ])

def _sell_confirmation_text(user_id: int) -> str:
    d = temp_data.get(user_id, {})
    email    = d.get("email", "—")
    password = d.get("password", "—")
    recovery = d.get("recovery_email", "—")
    twofa    = d.get("twofa", "")
    lines = [
        "✅ Your account credentials have been saved",
        "",
        f"Email: {email}",
        f"Password: {password}",
        f"Recovery email: {recovery}",
    ]
    if twofa:
        lines.append(f"2FA Key: {twofa}")
    lines += [
        "",
        "🔒 Make sure your Gmail account exists and is accessible, otherwise the payment will not be processed.",
        "",
        "🔒 We suggest you set 2FA on the account to increase the chances of successful login on our side.",
        "",
        "You can submit your account with or without 2FA authentication key.",
    ]
    return "\n".join(lines)

# =========================
# REGISTER (SELL FLOW ENTRY)
# =========================

async def register(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Entry point when user taps Sell — ask single or bulk."""
    user = update.effective_user
    temp_data[user.id] = {}
    context.user_data["sell_state"] = STATE_WAIT_ENTRY
    context.user_data["reg_flow"] = True
    await update.message.reply_text(
        "Do you want to submit Single Gmail account or bulk submit?",
        reply_markup=_sell_entry_kb(),
    )

# =========================
# SELL FLOW — CANCEL HELPER
# =========================

async def _cancel_sell_flow(update, context):
    user = update.effective_user
    context.user_data.pop("sell_state", None)
    context.user_data.pop("reg_flow", None)
    temp_data.pop(user.id, None)
    await update.message.reply_text(
        "Registration cancelled.",
        reply_markup=main_menu_markup(user.id),
    )

# =========================
# SELL FLOW — BULK PARSER
# =========================

def _parse_bulk_accounts(text):
    results = []
    for line in text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        parts = None
        for sep in [":", ",", "	"]:
            p = line.split(sep)
            if len(p) >= 3:
                parts = [x.strip() for x in p]
                break
        if not parts or len(parts) < 3:
            continue
        results.append({
            "email": parts[0],
            "password": parts[1],
            "recovery_email": parts[2],
            "twofa": parts[3] if len(parts) >= 4 else "",
        })
    return results

# =========================
# SELL FLOW — FINAL SUBMIT HELPER
# =========================

async def _do_final_submit(update, context, twofa=""):
    user = update.effective_user
    # Support both message-based and callback-query-based triggers
    _reply_msg = update.message or (update.callback_query.message if update.callback_query else None)
    d = temp_data.get(user.id, {})
    email    = d.get("email", "").strip()
    password = d.get("password", "").strip()
    recovery = d.get("recovery_email", "").strip()
    if not twofa:
        twofa = d.get("twofa", "").strip()

    if not email or not password or not recovery:
        await _reply_msg.reply_text(
            "Missing data. Please start registration again.",
            reply_markup=main_menu_markup(user.id),
        )
        context.user_data.pop("sell_state", None)
        context.user_data.pop("reg_flow", None)
        temp_data.pop(user.id, None)
        return

    now = int(time.time())
    con = db()
    cur = con.cursor()
    try:
        cur.execute(
            "INSERT INTO registrations(user_id, email, password, recovery_email, twofa_key, created_at, state) VALUES(?,?,?,?,?,?,?)",
            (user.id, email, password, recovery, twofa if twofa else None, now, "created")
        )
        reg_id = cur.lastrowid
        expires_at = now + ACTION_TIMEOUT_HOURS * 3600
        cur.execute(
            "INSERT INTO actions(user_id, reg_id, created_at, expires_at, state) VALUES(?,?,?,?,?)",
            (user.id, reg_id, now, expires_at, "waiting_admin")
        )
        action_id = cur.lastrowid
        con.commit()
    except Exception as _e:
        try: con.rollback()
        except Exception: pass
        import traceback; traceback.print_exc()
        print(f"[SELL SUBMIT ERROR] {_e!r}", flush=True)
        con.close()
        await _reply_msg.reply_text(f"DB error: {_e}", reply_markup=main_menu_markup(user.id))
        return
    con.close()

    try:
        hid = add_hold_credit(user.id, float(PRE_CREDIT_AMOUNT))
        con2 = db(); cur2 = con2.cursor()
        cur2.execute(
            "INSERT INTO precredits(action_id, user_id, hold_credit_id, amount, created_at, reverted) VALUES(?,?,?,?,?,0)",
            (int(action_id), int(user.id), int(hid), float(PRE_CREDIT_AMOUNT), now)
        )
        con2.commit(); con2.close()
    except Exception:
        pass

    try:
        save_form_row(int(reg_id), int(user.id), "", email, password, recovery, now, twofa_key=twofa)
    except Exception:
        pass

    if ENABLE_SMTP_BOUNCE_CHECK:
        handle = (email.split("@")[0] if "@" in email else email).strip()
        ok = await asyncio.to_thread(_email_handle_exists, handle)
        if not ok:
            set_reg_state(reg_id, "failed")
            await _reply_msg.reply_text("Email verification failed. Please check your email.", reply_markup=main_menu_markup(user.id))
            context.user_data.pop("sell_state", None)
            context.user_data.pop("reg_flow", None)
            temp_data.pop(user.id, None)
            return

    context.user_data.pop("sell_state", None)
    context.user_data.pop("reg_flow", None)
    temp_data.pop(user.id, None)

    await _reply_msg.reply_text(
        "🎉Submitted successfully!\n\n💸Rs." + str(int(PRE_CREDIT_AMOUNT)) + " credited to HOLD balance.\n🏆Funds will be transferred to main balance after review.",
        reply_markup=main_menu_markup(user.id),
    )

# =========================
# SELL FLOW — TEXT INPUT HANDLER
# Called from menu_handler when sell_state is active
# =========================

async def _handle_sell_flow(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user = update.effective_user
    state = context.user_data.get("sell_state")
    if not state:
        return False

    txt = (update.message.text or "").strip()

    if txt == "Cancel registration" or txt == "⊖ Cancel registration":
        await _cancel_sell_flow(update, context)
        return True

    if state == STATE_WAIT_ENTRY:
        if txt == "❤️Single Gmail" or "Single Gmail" in txt:
            context.user_data["sell_state"] = STATE_WAIT_EMAIL
            await update.message.reply_text("Please enter your Gmail email address:", reply_markup=_sell_cancel_kb())
            return True
        if txt == "Bulk submit" or "Bulk submit" in txt:
            context.user_data["sell_state"] = STATE_WAIT_BULK
            msg = (
                "Bulk Account Submission\n\n"
                "Paste your accounts or send a .txt file, one account per line:\n"
                "email:password:recovery_email\n"
                "email:password:recovery_email:2fa_key (optional)\n\n"
                "Example:\n"
                "john@gmail.com:MyPass123:recovery1@mail.com\n"
                "jane@gmail.com:SecureP@ss:recovery2@mail.com:F7QZDZENI\n\n"
                "You can also use comma or tab as separator."
            )
            await update.message.reply_text(msg, reply_markup=_sell_cancel_kb())
            return True
        await update.message.reply_text("Please choose an option:", reply_markup=_sell_entry_kb())
        return True

    if state == STATE_WAIT_BULK:
        # .txt file support handled in document_handler; this handles pasted text
        accounts = _parse_bulk_accounts(txt)
        if not accounts:
            await update.message.reply_text("Could not parse any accounts. Format: email:password:recovery", reply_markup=_sell_cancel_kb())
            return True
        # Max 50 accounts per submission
        MAX_BULK = 50
        if len(accounts) > MAX_BULK:
            await update.message.reply_text(
                f"❌ Maximum {MAX_BULK} accounts allowed per bulk submission. You sent {len(accounts)}. Please split into smaller batches.",
                reply_markup=_sell_cancel_kb()
            )
            return True
        now = int(time.time())
        ok_count = 0
        skip_count = 0
        err_lines = []
        for acc in accounts:
            em = acc["email"].lower().strip()
            pw = acc["password"].strip()
            rv = acc["recovery_email"].lower().strip()
            tf = acc.get("twofa", "").strip()
            if not is_valid_email_syntax(em):
                err_lines.append("Invalid email: " + em); skip_count += 1; continue
            if not is_valid_email_syntax(rv):
                err_lines.append("Invalid recovery: " + rv); skip_count += 1; continue
            if em == rv:
                err_lines.append("Email same as recovery: " + em); skip_count += 1; continue
            if _email_exists_in_db(em):
                err_lines.append("Already exists: " + em); skip_count += 1; continue
            if _recovery_email_exists_in_db(rv):
                err_lines.append("Recovery exists: " + rv); skip_count += 1; continue
            try:
                con = db(); cur = con.cursor()
                cur.execute(
                    "INSERT INTO registrations(user_id, email, password, recovery_email, twofa_key, created_at, state) VALUES(?,?,?,?,?,?,?)",
                    (user.id, em, pw, rv, tf if tf else None, now, "created")
                )
                reg_id = cur.lastrowid
                expires_at = now + ACTION_TIMEOUT_HOURS * 3600
                cur.execute(
                    "INSERT INTO actions(user_id, reg_id, created_at, expires_at, state) VALUES(?,?,?,?,?)",
                    (user.id, reg_id, now, expires_at, "waiting_admin")
                )
                action_id = cur.lastrowid
                con.commit(); con.close()
                try:
                    hid = add_hold_credit(user.id, float(PRE_CREDIT_AMOUNT))
                    con2 = db(); cur2 = con2.cursor()
                    cur2.execute(
                        "INSERT INTO precredits(action_id, user_id, hold_credit_id, amount, created_at, reverted) VALUES(?,?,?,?,?,0)",
                        (int(action_id), int(user.id), int(hid), float(PRE_CREDIT_AMOUNT), now)
                    )
                    con2.commit(); con2.close()
                except Exception: pass
                try: save_form_row(int(reg_id), int(user.id), "", em, pw, rv, now)
                except Exception: pass
                ok_count += 1
            except Exception as be:
                err_lines.append("DB error for " + em); skip_count += 1
        msg = str(ok_count) + " account(s) submitted."
        if skip_count:
            msg += " " + str(skip_count) + " skipped."
        if err_lines:
            msg += "\n" + "\n".join(err_lines[:10])
        context.user_data.pop("sell_state", None)
        context.user_data.pop("reg_flow", None)
        temp_data.pop(user.id, None)
        await update.message.reply_text(msg, reply_markup=main_menu_markup(user.id))
        return True

    if state == STATE_WAIT_EMAIL:
        email = txt.lower().strip()
        if not is_valid_email_syntax(email):
            await update.message.reply_text("Invalid email format. Please enter a valid Gmail address:", reply_markup=_sell_cancel_kb())
            return True
        if _email_exists_in_db(email):
            await update.message.reply_text("This email already exists. Please enter a different email:", reply_markup=_sell_cancel_kb())
            return True
        temp_data.setdefault(user.id, {})["email"] = email
        context.user_data["sell_state"] = STATE_WAIT_PASSWORD
        await update.message.reply_text("Please enter your password:", reply_markup=_sell_cancel_kb())
        return True

    if state == STATE_WAIT_PASSWORD:
        password = txt.strip()
        if not password:
            await update.message.reply_text("Password cannot be empty. Please enter your password:", reply_markup=_sell_cancel_kb())
            return True
        temp_data.setdefault(user.id, {})["password"] = password
        context.user_data["sell_state"] = STATE_WAIT_RECOVERY
        await update.message.reply_text("Please enter your recovery email address:", reply_markup=_sell_cancel_kb())
        return True

    if state == STATE_WAIT_RECOVERY:
        recovery = txt.lower().strip()
        if not is_valid_email_syntax(recovery):
            await update.message.reply_text("Invalid recovery email format. Please enter a valid email:", reply_markup=_sell_cancel_kb())
            return True
        # Recovery email cannot be same as the main email
        main_email = temp_data.get(user.id, {}).get("email", "").lower().strip()
        if recovery == main_email:
            await update.message.reply_text("❌ Recovery email cannot be the same as your main email. Please enter a different recovery email:", reply_markup=_sell_cancel_kb())
            return True
        if _recovery_email_exists_in_db(recovery):
            await update.message.reply_text("This recovery email already exists. Please enter a different one:", reply_markup=_sell_cancel_kb())
            return True
        temp_data.setdefault(user.id, {})["recovery_email"] = recovery
        temp_data[user.id]["twofa"] = ""
        context.user_data["sell_state"] = STATE_CONFIRM
        await update.message.reply_text("Checking if credentials are unique...")
        await update.message.reply_text(_sell_confirmation_text(user.id), reply_markup=_sell_after_recovery_kb())
        return True

    if state == STATE_CONFIRM:
        if "Done (without 2FA)" in txt or "without 2FA" in txt.lower():
            await _do_final_submit(update, context, twofa="")
            return True
        if "Add 2FA" in txt or "2FA key" in txt.lower() or "Change 2FA" in txt.lower():
            context.user_data["sell_state"] = STATE_WAIT_2FA
            await update.message.reply_text("Please enter your 2FA authentication key: or QR:", reply_markup=_sell_cancel_kb())
            return True
        if "✅ Submit" in txt or "submit" in txt.lower():
            await _do_final_submit(update, context)
            return True
        return True

    if state == STATE_WAIT_2FA:
        twofa = txt.strip()
        if not twofa:
            await update.message.reply_text("2FA key cannot be empty. Please enter your 2FA key:", reply_markup=_sell_cancel_kb())
            return True
        temp_data.setdefault(user.id, {})["twofa"] = twofa
        context.user_data["sell_state"] = STATE_CONFIRM
        # Show confirmation text + inline Submit button + reply keyboard with change option
        await update.message.reply_text(
            "✅ 2FA key saved! Tap SUBMIT below to confirm:",
            reply_markup=_sell_after_2fa_kb()
        )
        return True

    return False
# =========================
# CALLBACKS
# =========================
def _admin_ev_set_verified(cur, action_id: int, admin_id: int):
    # Save decision only — state change sweeper handle karega hold mature hone par
    cur.execute(
        "INSERT OR REPLACE INTO admin_email_verify(action_id, decided_by, status, reason, decided_at) VALUES(?,?,?,?,?)",
        (int(action_id), int(admin_id), "VERIFIED", "", int(time.time())),
    )


def _admin_ev_set_not_verified(cur, action_id: int, admin_id: int, reason: str):
    # Revert provisional HOLD credit
    cur.execute("SELECT * FROM actions WHERE action_id=?", (int(action_id),))
    a = cur.fetchone()
    if a:
        cur.execute(
            "SELECT hold_credit_id, amount, reverted FROM precredits WHERE action_id=?",
            (int(action_id),),
        )
        pc = cur.fetchone()
        if pc and int(pc["reverted"] or 0) == 0:
            try:
                revert_hold_credit_cur(cur, int(pc["hold_credit_id"]), int(a["user_id"]), float(pc["amount"]))
            except Exception:
                pass
            cur.execute("UPDATE precredits SET reverted=1 WHERE action_id=?", (int(action_id),))

        set_action_state_cur(cur, int(action_id), "rejected")
        set_reg_state_cur(cur, int(a["reg_id"]), "rejected")

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

    try:
        if user.id != ADMIN_ID and is_blocked(user.id):
            return
    except Exception as e:
        dprint(f"[CB ERROR] is_blocked crashed: {e}", flush=True)

    try:
        ensure_user(user.id, user.username or user.full_name)
    except Exception as e:
        dprint(f"[CB ERROR] ensure_user crashed: {e}", flush=True)

    data = q.data or ""
    dprint(f"[CB] user={user.id} data={data}", flush=True)

    # ── Pending queue skip ───────────────────────────────────────────
    if data.startswith("ADM_QUEUE_SKIP:"):
        if not is_admin(user.id):
            return
        await q.answer("Skipped.", show_alert=False)
        try:
            await q.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
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

    # ── Reward toggle callbacks ──────────────────────────────────────
    if data in ("TASK_ON", "TASK_OFF", "REF_ON", "REF_OFF"):
        if not is_admin(user.id):
            return
        if data == "TASK_ON":
            set_setting("task_rewards", "on")
            msg = "✅ Task Rewards turned ON"
        elif data == "TASK_OFF":
            set_setting("task_rewards", "off")
            msg = "❌ Task Rewards turned OFF"
        elif data == "REF_ON":
            set_setting("referral_rewards", "on")
            msg = "✅ Referral Rewards turned ON"
        else:
            set_setting("referral_rewards", "off")
            msg = "❌ Referral Rewards turned OFF"
        task_st = get_setting("task_rewards", "on")
        ref_st  = get_setting("referral_rewards", "on")
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"TASK REWARD {'✅ ON' if task_st=='on' else '❌ OFF'}", callback_data="TASK_ON" if task_st!="on" else "TASK_OFF")],
            [InlineKeyboardButton(f"REFERRAL {'✅ ON' if ref_st=='on' else '❌ OFF'}", callback_data="REF_ON" if ref_st!="on" else "REF_OFF")],
        ])
        try:
            await q.edit_message_text(
                f"{msg}\n\nTask Rewards: {'ON ✅' if task_st=='on' else 'OFF ❌'}\n"
                f"Referral Rewards: {'ON ✅' if ref_st=='on' else 'OFF ❌'}",
                reply_markup=kb,
            )
        except Exception:
            await q.answer(msg, show_alert=True)
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
            await context.bot.send_message(chat_id=user.id, text=tr(user.id, "back_done"), reply_markup=main_menu_markup(user.id))
        except Exception:
            pass
        return

    # Help menu (9 buttons)
    if data == "HELP_BACK":
        await q.edit_message_text(tr(user.id, "help_menu"), reply_markup=help_menu_kb(user.id))
        return
    if re.fullmatch(r"HELP_[1-6]", data or ""):
        txt = tr(user.id, f"{data.lower()}_text") or tr(user.id, "help_not_found")
        await q.edit_message_text(txt, reply_markup=help_back_kb(user.id))
        return

    # Balance history pagination
    if data.startswith("BH:"):
        try:
            page = int((data.split(":")[1] or "1").strip())
        except Exception:
            page = 1
        txt0, total_pages = balance_history_page_text(user.id, page=page, per_page=5)
        try:
            await q.edit_message_text(txt0, reply_markup=balance_history_kb(page, total_pages))
        except Exception:
            try:
                await q.message.reply_text(txt0, reply_markup=balance_history_kb(page, total_pages))
            except Exception:
                pass
        return
        
    # Channel join check
    if data == "CHK_JOIN":
        # Always answer callback first (prevents timeout / stuck)
        try:
            await q.answer("⏳ Checking...", show_alert=False)
        except Exception:
            pass

        cache_invalidate_channel(user.id)  # Force fresh Telegram API check on join button
        ok = await user_in_required_channels(context, user.id)

        if not ok:
            try:
                await q.answer(tr(user.id, "🚫 You are not joined in required channel.\n\n"
    "📢 Please join all required channels first, then click on "
    '"Check Join" button again.\n\n'
    "After joining, you will get access instantly."), show_alert=True)
            except Exception:
                pass
            await q.message.reply_text(
                tr(user.id, "👋 Welcome!\n\n"
    "🔐 To use this bot, you must join our required channels first.\n\n"
    "📢 Join all channels below and then press the '✅ Joined' button.\n\n"
    "⚠️ Access will not be granted until all channels are joined."),
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
            tr(user.id, "channel_verified"),
            reply_markup=main_menu_markup(user.id)
        )
        return
    # Tutorial videos (always clickable)
    if data == "VID_CREATE":
        await send_create_account_video(context, user.id)
        return

    if data == "VID_LOGOUT":
        await send_logout_video(context, user.id)
        return

    if data == "VID_RECOVERY":
        # Recovery email tutorial — replace VIDEO_FILE_ID_RECOVERY with your actual file_id
        VIDEO_FILE_ID_RECOVERY = "BAACAgUAAxkBAAMpabgZhlyMgIysil7vxDtqo-RgdEkAAnMeAAJYKsFV6nSr2mV4xW46BA"
        try:
            await context.bot.send_video(chat_id=user.id, video=VIDEO_FILE_ID_RECOVERY, caption="📕 How to add recovery email")
        except Exception:
            await context.bot.send_message(chat_id=user.id, text="📕 Recovery email tutorial video coming soon.")
        return

    # Admin selects a payout to process
    
    # Export form_table PDF options (admin)
    if data.startswith("EXP_FORM:"):
        if not is_admin(user.id):
            return
        choice = data.split(":", 1)[1].strip()

        def _day_bounds(dt: datetime):
            start = datetime(dt.year, dt.month, dt.day)
            end = start + timedelta(days=1)
            return int(start.timestamp()), int(end.timestamp())

        if choice == "range":
            context.user_data["admin_mode"] = "export_form_range_wait"
            await q.message.reply_text("Send date range like: 2026-02-01 2026-02-15", reply_markup=ADMIN_MENU_KB)
            return

        def _resolve_ts(ch):
            start_ts = end_ts = None
            if ch == "today":
                start_ts, end_ts = _day_bounds(datetime.now())
            elif ch == "yesterday":
                y = datetime.now() - timedelta(days=1)
                start_ts, end_ts = _day_bounds(y)
            elif ch == "month":
                end_ts = int(datetime.now().timestamp())
                start_ts = end_ts - 30 * 86400
            elif ch == "all":
                start_ts = end_ts = None
            return start_ts, end_ts

        pdf_path = "form_data.pdf"
        await q.message.reply_text("⏳ Building export...", reply_markup=ADMIN_MENU_KB)

        try:
            start_ts, end_ts = _resolve_ts(choice)

            rows = _fetch_form_rows_range(start_ts=start_ts, end_ts=end_ts, limit=None)
            if len(rows) > 2000:
                rows = rows[:2000]

            await asyncio.to_thread(export_form_pdf, pdf_path, limit=None, rows=rows)
            # Also send CSV alongside PDF
            import io as _io
            csv_data = export_form_csv(rows)
            csv_bio = _io.BytesIO(csv_data.encode("utf-8-sig"))
            csv_bio.seek(0)
            label = choice.upper()
            await q.message.reply_document(document=open(pdf_path, "rb"), filename=f"form_{label}.pdf",
                                           caption=f"📄 PDF — {len(rows)} records", reply_markup=ADMIN_MENU_KB)
            await q.message.reply_document(document=csv_bio, filename=f"form_{label}_{datetime.now().strftime('%Y%m%d')}.csv",
                                           caption=f"📊 CSV — {len(rows)} records (Excel-ready)")
        except Exception as e:
            await q.message.reply_text(f"❌ Export failed: {e}", reply_markup=ADMIN_MENU_KB)
        return

    if data.startswith("PAY_SEL:"):
        if not is_admin(user.id):
            return
        pid = int(data.split(":")[1])

        # load method
        method = "upi"
        try:
            con = db()
            cur = con.cursor()
            cur.execute("SELECT method FROM payouts WHERE id=?", (pid,))
            row = cur.fetchone()
            con.close()
            if row:
                method = (row["method"] or "upi").lower()
        except Exception:
            try:
                con.close()
            except Exception:
                pass

        context.user_data["pay_selected"] = pid
        context.user_data["pay_selected_method"] = method

        if method == "crypto":
            context.user_data["admin_mode"] = "crypto_txid_wait"
            await q.message.reply_text(f"✅ Selected CRYPTO payout #{pid}.\n\nNow send Transaction ID (txid):")
            return

        # UPI/QR default flow (proof screenshot + UTR)
        await q.message.reply_text(f"✅ Selected payout #{pid}. Now choose an action:", reply_markup=PAYOUT_SUBMENU_KB)
        return



    # NOOP - ignore (page counter button)
    if data == "NOOP":
        await q.answer()
        return

    # Admin user list pagination
    if data.startswith("ADMIN_ACC:"):
        if user.id != ADMIN_ID:
            await q.answer("❌ Not authorized")
            return
        offset = int(data.split(":")[1])
        total = admin_total_users()
        await _send_admin_users_page(q, context, offset=offset, total=total)
        await q.answer()
        return
        
    # B) Accounts pagination
    if data.startswith("ACC:"):
        offset = int(data.split(":")[1])
        _ALL_STATES = "('waiting_admin','approved','rejected')"

        con = db()
        cur = con.cursor()
        try:
            cur.execute(f"""
                SELECT COUNT(*) FROM actions a
                JOIN registrations r ON r.id = a.reg_id
                WHERE a.user_id = ? AND a.state IN {_ALL_STATES}
            """, (user.id,))
            total_row = cur.fetchone()
            total = int(total_row[0] if total_row else 0)

            if total == 0:
                await q.edit_message_text(tr(user.id, "my_accounts_empty"))
                con.close()
                return

            # Clamp offset
            if offset >= total:
                offset = max(0, total - (total % 5 or 5))

            cur.execute(f"""
                SELECT
                    r.id AS reg_id,
                    r.email AS email,
                    a.action_id AS action_id,
                    a.state AS astate,
                    COALESCE(a.updated_at, a.created_at) AS stime,
                    a.created_at AS sub_ts,
                    ev.status AS ev_status,
                    ev.reason AS ev_reason,
                    r.state AS rstate,
                    hc.matured_at AS matured_at
                FROM actions a
                JOIN registrations r ON r.id = a.reg_id
                LEFT JOIN admin_email_verify ev ON ev.action_id = a.action_id
                LEFT JOIN precredits pc ON pc.action_id = a.action_id
                LEFT JOIN hold_credits hc ON hc.id = pc.hold_credit_id
                WHERE a.user_id = ? AND a.state IN {_ALL_STATES}
                ORDER BY COALESCE(a.updated_at, a.created_at) DESC
                LIMIT 5 OFFSET ?
            """, (user.id, offset))
            rows = cur.fetchall()
            con.commit()
        except Exception as e:
            try: con.rollback()
            except Exception: pass
            dprint(f"[ACCOUNTS] DB error: {e!r}", flush=True)
            await q.edit_message_text("❌ Error loading accounts. Please try again.")
            return
        finally:
            try: con.close()
            except Exception: pass

        if not rows:
            await q.edit_message_text(tr(user.id, "my_accounts_empty"))
            return

        page = offset // 5 + 1
        total_pages = max(1, (total + 4) // 5)
        now_ts = int(time.time())
        lines = _build_account_lines(rows, user.id, now_ts)
        header = tr(user.id, "my_accounts_title", page=page, total_pages=total_pages)
        msg = header + "\n\n" + "\n\n".join(lines)
        if len(msg) > 4000:
            msg = msg[:4000] + "\n..."
        await q.edit_message_text(msg, reply_markup=accounts_nav(offset, total))
        return
    
    
# B2) Payout type menu (UPI / CRYPTO)
    if data == "PAYOUT_TYPE:MENU":
        # Reset any in-progress withdraw input flow
        context.user_data["await_upi"] = False
        context.user_data["payout_amt"] = 0
        context.user_data["await_crypto_addr"] = False
        context.user_data["await_crypto_amt"] = False
        context.user_data["crypto_addr"] = ""
        context.user_data["payout_type_select"] = True
        context.user_data["payout_reply_mode"] = "menu"
        await q.message.reply_text(tr(user.id, "choose_withdrawal"), reply_markup=payout_menu_kb(user.id))
        return

    if data == "PAYOUT_TYPE:BACK_BALANCE":
        # Reset any in-progress withdraw input flow and return to Balance submenu (reply keyboard)
        context.user_data["await_upi"] = False
        context.user_data["payout_amt"] = 0
        context.user_data["await_crypto_addr"] = False
        context.user_data["await_crypto_amt"] = False
        context.user_data["crypto_addr"] = ""
        await q.message.reply_text("🔙 Back", reply_markup=balance_menu(user.id))
        return

    if data == "PAYOUT_TYPE:UPI":
        await q.message.reply_text(
            "CHOOSE AMOUNT\n10% FEES IS APPLICABLE",
            reply_markup=payout_amounts_kb()
        )
        return

    if data == "PAYOUT_TYPE:CRYPTO":
        context.user_data["await_crypto_addr"] = True
        context.user_data["await_crypto_amt"] = False
        context.user_data["crypto_addr"] = ""

        mainb, _holdb = get_balances(user.id)
        await q.message.reply_text(
            "Wallet address like: 0xb2450F5B107b4e04087cB70cDD8E6476385236B1\n"
            "Blockchain : BEP-20\n\n"
            f"Available balance: {inr_to_usd_fixed(float(mainb)):.2f} USD\n\n"
            "Now send your wallet address:",
            reply_markup=back_only_menu()
        )
        return

# C) Payout amount selection
    if data.startswith("PAY_AMT:"):
        # Disable amount buttons immediately after first click (prevents multiple selections)
        try:
            await q.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        amt = int(data.split(":")[1])

        allowed = (55, 110, 210, 310, 510, 1050)
        if amt not in allowed:
            await q.message.reply_text(tr(user.id, "invalid_amount"), reply_markup=back_only_menu())
            return

        mainb, _holdb = get_balances(user.id)

        # RULE:
        # - UPI payout = INR (direct)
        # - CRYPTO payout = USD (fixed 1 USD = ₹91), but crypto flow uses manual amount entry
        mode = (context.user_data.get("payout_reply_mode") or "upi").lower()

        if mode == "upi":
            amt_inr = int(amt)
            if float(mainb) + 1e-6 < float(amt_inr):
                await q.message.reply_text(tr(user.id, "insufficient_withdrawal"), reply_markup=back_only_menu())
                return
            context.user_data["await_upi"] = True
            context.user_data["payout_amt_inr"] = amt_inr
            context.user_data["payout_reply_mode"] = "upi"
            await q.message.reply_text("PLEASE ENTER YOUR UPI ID OR QR CODE", reply_markup=back_only_menu())
            return

        # Fallback (if ever used): treat as USD and check via fixed conversion
        amt_usd = float(amt)
        need_inr = usd_to_inr_fixed(amt_usd)
        if float(mainb) + 1e-6 < float(need_inr):
            await q.message.reply_text(tr(user.id, "insufficient_withdrawal"), reply_markup=back_only_menu())
            return

        context.user_data["await_upi"] = True
        context.user_data["payout_amt_inr"] = int(need_inr)
        context.user_data["payout_reply_mode"] = "upi"
        await q.message.reply_text("PLEASE ENTER YOUR UPI ID OR QR CODE", reply_markup=back_only_menu())
        return

    # A) Register buttons
    if (data.startswith("REG_DONE:") or data.startswith("REG_CANCEL:") or
        data.startswith("REG_CANCEL_SURE:") or data.startswith("REG_CONFIRM:") or
        data.startswith("REG_COMPLETE:") or data.startswith("REG_STEP1:") or
        data.startswith("REG_STEP2:")):
        action_id = int(data.split(":")[1])
        ok, a = action_valid(action_id)

        # timeout
        if not ok:
            # After 20 hours: show TIME OUT on the same message and remove buttons
            if a and int(time.time()) > int(a["expires_at"]):
                try:
                    txt0 = q.message.text or ""
                    if "TIME OUT" not in txt0:
                        txt0 = txt0 + "\n\n CANCELED REGISTRATION" 
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
           
            email = _safe_code(r["email"] if r else "")
            password = _safe_code(r["password"] if r else "")

            base_text = (
                "Register account using the specified\n"
                "data and get from ₹10 to ₹12\n\n"
                f"First Name: `{first_name}`\n"
                f"Last Name: `{last_name}`\n"
                f"Email: `{email}`\n"
                f"Password: `{password}`\n\n"
                "🔐 Be sure to use the specified data,\n"
                "otherwise the account will not be paid\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                "Age choose : 1990-2005\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                "Gender : Your choice,\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                "CANCELED REGISTRATION"
            )

            try:
                await q.edit_message_text(
                    text=base_text,
                    parse_mode="Markdown",
                    reply_markup=None,
                )
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
            # STEP 1 → STEP 2: Show Complete / Back buttons
            set_action_state(action_id, "done1")
            ts_key = f"confirm_ts_{action_id}"
            if not context.user_data.get(ts_key):
                context.user_data[ts_key] = int(time.time())

            con = db()
            cur = con.cursor()
            cur.execute("SELECT * FROM registrations WHERE id=?", (a["reg_id"],))
            r = cur.fetchone()
            con.close()

            def _safe_code(s: str) -> str:
                s = (s or "").strip()
                return s.replace("`", "'")

            first_name = _safe_code(r["first_name"] if r else "")
            last_name  = _safe_code(r["last_name"] if r else "")
            email = _safe_code(r["email"] if r else "")
            password = _safe_code(r["password"] if r else "")

            base_text = (
                "Register account using the specified\n"
                "data and get from ₹10 to ₹12\n\n"
                f"First Name: `{first_name}`\n"
                f"Last Name: `{last_name}`\n"
                f"Email: `{email}`\n"
                f"Password: `{password}`\n\n"
                "🔐 Be sure to use the specified data,\n"
                "otherwise the account will not be paid\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                "Age choose : 1990-2005\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                "Gender : Your choice\n"

            )
            try:
                await q.edit_message_text(text=base_text, parse_mode="Markdown",
                                          reply_markup=complete_back_buttons(action_id))
            except Exception:
                try:
                    await q.edit_message_reply_markup(reply_markup=complete_back_buttons(action_id))
                except Exception:
                    pass
            return

        if data.startswith("REG_COMPLETE:"):
            # STEP 2 → STEP 2(a): Show confirm + recovery video + back
            con = db()
            cur = con.cursor()
            cur.execute("SELECT * FROM registrations WHERE id=?", (a["reg_id"],))
            r = cur.fetchone()
            con.close()

            def _safe_code(s: str) -> str:
                s = (s or "").strip()
                return s.replace("`", "'")

            first_name = _safe_code(r["first_name"] if r else "")
            last_name  = _safe_code(r["last_name"] if r else "")   
            email = _safe_code(r["email"] if r else "")
            password = _safe_code(r["password"] if r else "")
            recovery_email = _safe_code(r["recovery_email"] if r else "")


            base_text = (
                "Register account using the specified\n"
                "data and get from ₹10 to ₹12\n\n"
                f"First Name: `{first_name}`\n"
                f"Last Name: `{last_name}`\n"
                f"Email: `{email}`\n"
                f"Password: `{password}`\n\n"
                "🔐 Be sure to use the specified data,\n"
                "otherwise the account will not be paid\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                "Age choose : 1990-2005\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                "Gender : Your choice\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                "🚦 You need to add Recovery email :\n"
                f"`{recovery_email}`\n"
            )
            try:
                await q.edit_message_text(text=base_text, parse_mode="Markdown",
                                          reply_markup=confirm_again_button(action_id))
            except Exception:
                try:
                    await q.edit_message_reply_markup(reply_markup=confirm_again_button(action_id))
                except Exception:
                    pass
            return

        if data.startswith("REG_STEP1:"):
            # Back from Step 2 → Step 1
            set_action_state(action_id, "shown")
            con = db()
            cur = con.cursor()
            cur.execute("SELECT * FROM registrations WHERE id=?", (a["reg_id"],))
            r = cur.fetchone()
            con.close()

            def _safe_code(s: str) -> str:
                s = (s or "").strip()
                return s.replace("`", "'")

            first_name = _safe_code(r["first_name"] if r else "")
            last_name  = _safe_code(r["last_name"] if r else "")
            email = _safe_code(r["email"] if r else "")
            password = _safe_code(r["password"] if r else "")

            base_text = (
                "Register account using the specified\n"
                "data and get from ₹10 to ₹12\n\n"
                f"First Name: `{first_name}`\n"
                f"Last Name: `{last_name}`\n"
                f"Email: `{email}`\n"
                f"Password: `{password}`\n\n"
                "🔐 Be sure to use the specified data,\n"
                "otherwise the account will not be paid\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                "Age choose : 1990-2005\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                "Gender : Your choice\n"
            )
            try:
                await q.edit_message_text(text=base_text, parse_mode="Markdown",
                                          reply_markup=reg_buttons(action_id))
            except Exception:
                try:
                    await q.edit_message_reply_markup(reply_markup=reg_buttons(action_id))
                except Exception:
                    pass
            return

        if data.startswith("REG_STEP2:"):
            # Back from Step 2(a) → Step 2
            set_action_state(action_id, "done1")
            con = db()
            cur = con.cursor()
            cur.execute("SELECT * FROM registrations WHERE id=?", (a["reg_id"],))
            r = cur.fetchone()
            con.close()

            def _safe_code(s: str) -> str:
                s = (s or "").strip()
                return s.replace("`", "'")

            first_name = _safe_code(r["first_name"] if r else "")
            last_name  = _safe_code(r["last_name"] if r else "")
            email = _safe_code(r["email"] if r else "")
            password = _safe_code(r["password"] if r else "")

            base_text = (
                "Register account using the specified\n"
                "data and get from ₹10 to ₹12\n\n"
                f"First Name: `{first_name}`\n"
                f"Last Name: `{last_name}`\n"
                f"Email: `{email}`\n"
                f"Password: `{password}`\n\n"
                "🔐 Be sure to use the specified data,\n"
                "otherwise the account will not be paid\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                "Age choose : 1990-2005\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                "Gender : Your choice\n"
                
                
            )
            try:
                await q.edit_message_text(text=base_text, parse_mode="Markdown",
                                          reply_markup=complete_back_buttons(action_id))
            except Exception:
                try:
                    await q.edit_message_reply_markup(reply_markup=complete_back_buttons(action_id))
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
                    text=tr(user.id, "email_checking", progress=_confirm_bar(0)),    
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
            # Animation will run during the real check
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
                            f"l⚠️ Please add a recovery email ."    
                        )    
                    except Exception:    
                        pass    
                    return    
                else:    
                    context.user_data[ready_key] = True    

    

            # After cooldown: perform REAL check
            handle = (email.split("@")[0] if "@" in email else email).strip()

            if confirm_msg_id:
                ok = await run_check_with_animation(
                    context.bot,
                    chat_id,
                    confirm_msg_id,
                    "",
                    action_id,
                    asyncio.to_thread(_email_handle_exists, handle)
                )
            else:
                ok = await asyncio.to_thread(_email_handle_exists, handle)

            if not ok:
                # keep action active for retry
                set_action_state(action_id, "done1")
                set_reg_state(a["reg_id"], "created")

                try:
                    await _edit_message_safe(
                        context.bot,
                        chat_id,
                        confirm_msg_id,
                        tr(user.id, "l⚠️ Please add a recovery email.")
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

            # ✅ Provisional reward: add HOLD immediately on RIGHT result (reverted if admin rejects)
            try:
                con_pc = db()
                cur_pc = con_pc.cursor()
                cur_pc.execute("SELECT hold_credit_id, amount, reverted FROM precredits WHERE action_id=?", (int(action_id),))
                pc = cur_pc.fetchone()
                _mature_delay = None
                if not pc:
                    hid, _mature_at = add_hold_credit_cur(cur_pc, int(user.id), float(PRE_CREDIT_AMOUNT))
                    _mature_delay = max(1, _mature_at - int(time.time()))
                    cur_pc.execute(
                        "INSERT INTO precredits(action_id, user_id, hold_credit_id, amount, created_at, reverted) VALUES(?,?,?,?,?,0)",
                        (int(action_id), int(user.id), int(hid), float(PRE_CREDIT_AMOUNT), int(time.time())),
                    )
                elif int(pc["reverted"] or 0) == 1:
                    # was reverted earlier; re-credit on new RIGHT result
                    hid, _mature_at = add_hold_credit_cur(cur_pc, int(user.id), float(pc["amount"]))
                    _mature_delay = max(1, _mature_at - int(time.time()))
                    cur_pc.execute(
                        "UPDATE precredits SET hold_credit_id=?, reverted=0 WHERE action_id=?",
                        (int(hid), int(action_id)),
                    )
                con_pc.commit()
                con_pc.close()
                # Exact time pe instant trigger schedule karo — sweeper pe depend nahi
                if _mature_delay is not None:
                    _uid_for_trigger = int(user.id)
                    async def _trigger_on_mature(ctx, _uid=_uid_for_trigger):
                        try:
                            def _do():
                                from db_pg import db as _db
                                return move_matured_hold_to_main(_uid)
                            result = await asyncio.to_thread(_do)
                            amt, task_paid, ref_id, commission_amt = result
                            if float(amt) > 0:
                                _commission_pct = get_referral_commission_pct()
                                _per = round(float(PRE_CREDIT_AMOUNT) * _commission_pct / 100.0, 2)
                                try:
                                    await ctx.bot.send_message(
                                        chat_id=_uid,
                                        text=f"{tr(_uid, 'accepted')}\n\n{tr(_uid, 'funds_accrual')}"
                                    )
                                except Exception:
                                    pass
                                if task_paid > 0:
                                    try:
                                        await ctx.bot.send_message(
                                            chat_id=_uid,
                                            text=f"✅ Task reward: ₹{task_paid:.2f} added to your main balance!"
                                        )
                                    except Exception:
                                        pass
                                if ref_id and commission_amt > 0:
                                    try:
                                        await ctx.bot.send_message(
                                            chat_id=int(ref_id),
                                            text=f"💸 Referral commission ₹{commission_amt:.2f} credited!"
                                        )
                                    except Exception:
                                        pass
                        except Exception as _e:
                            dprint(f"[INSTANT_TRIGGER] error for uid={_uid_for_trigger}: {_e!r}")
                    context.job_queue.run_once(_trigger_on_mature, when=_mature_delay, name=f"hold_mature_{user.id}_{action_id}")
            except Exception:
                try:
                    con_pc.close()
                except Exception:
                    pass

            try:    
                await _edit_message_safe(    
                    context.bot,    
                    chat_id,    
                    confirm_msg_id,    
                    tr(user.id, "BALANCE  UPDATED✅")    
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
           
            email = _safe_code(r["email"] if r else "")
            password = _safe_code(r["password"] if r else "")
            recovery_email = _safe_code(r["recovery_email"] if r else "")
            # ✅ Save to form_table ONLY after CONFIRM AGAIN + RIGHT result
            try:
                save_form_row(
                    int(a["reg_id"]),
                    int(user.id),
                    first_name,
                    email,
                    password,
                    recovery_email,
                    int(r["created_at"] or int(time.time())) if r else int(time.time()),
                )
            except Exception as _save_err:
                dprint(f"[save_form_row ERROR] {_save_err!r}")

            
            base_text = (
    "Register account using the specified data "
    "and get from ₹10 to ₹12\n\n"

    f"First name: `{first_name}`\n"
    f"Last name: `{last_name}`\n"
    f"Email: `{email}`\n"
    f"Password: `{password}`\n\n"

    "🔐 Be sure to use the specified data, "
    "otherwise the account will not be paid.\n"
    "━━━━━━━━━━━━━━━━━━━━\n\n"

    "🚦 You need to add Recovery email\n"
    f"`{recovery_email}`\n"
    "━━━━━━━━━━━━━━━━━━━━\n\n"

    "₹ 10 credited to hold\n\n"

    "Funds will be transferred to the main balance "
    "after 1-day hold.\n\n"

    "📛 Be sure to LOG OUT of account on your device"
)

            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("📲 How to logout of account ?", callback_data="VID_LOGOUT")
            ]])

            try:
                await q.edit_message_text(
                    text=base_text,
                    parse_mode="Markdown",
                    reply_markup=post_confirm_buttons()
                )
            except Exception:
                pass
                
            await send_logout_video(context, user.id)
            return

    # =========================
    # SELL FLOW INLINE CALLBACKS
    # =========================

    if data in ("NO_2FA", "WITH_2FA", "CONFIRM_2FA"):
        await q.answer()
        return

    if data == "FINAL_SUBMIT":
        await q.answer()
        await _do_final_submit(update, context)
        return
    # =========================
    # ADMIN: Manual email decision (💎 EMAIL)
    # =========================
    if data.startswith("ADMIN_EMAIL_REASON:"):
        if not is_admin(user.id):
            return
        parts = data.split(":")
        if len(parts) < 3:
            return
        try:
            action_id = int(parts[1])
            idx = int(parts[2])
        except Exception:
            return

        reason = MANUAL_EMAIL_REASONS[idx] if 0 <= idx < len(MANUAL_EMAIL_REASONS) else "UNKNOWN"

        def _op():
            con = db()
            cur = con.cursor()
            _admin_ev_set_not_verified(cur, action_id, user.id, reason)
            con.commit()
            con.close()

        # run in a thread to avoid blocking the bot while DB is busy
        await asyncio.to_thread(_db_write_retry, _op)

        await q.edit_message_text(f"❌ Rejected with reason: {reason}", reply_markup=None)
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

    # =========================
    # CRYPTO FLOW (USDT BEP-20)
    # =========================
    if context.user_data.get("await_crypto_addr"):
        addr = (update.message.text or "").strip()
        if addr in ("🔙 BACK", "🔙 Back", "⬅️ BACK", "⬅ BACK"):
            context.user_data["await_crypto_addr"] = False
            context.user_data["await_crypto_amt"] = False
            context.user_data["crypto_addr"] = ""
            context.user_data["payout_reply_mode"] = "menu"
            context.user_data["payout_type_select"] = True
            await update.message.reply_text(tr(user.id, "choose_withdrawal"), reply_markup=payout_menu_kb(user.id))
            return

        if not is_valid_bep20_address(addr):
            await update.message.reply_text(
                tr(user.id, "invalid_bep20"),
                reply_markup=back_only_menu(user.id),
            )
            return

        # Save address and ask for amount
        context.user_data["crypto_addr"] = addr
        context.user_data["await_crypto_addr"] = False
        context.user_data["await_crypto_amt"] = True

        mainb, _holdb = get_balances(user.id)
        await update.message.reply_text(
            tr(user.id, "crypto_address_saved", balance=f"{inr_to_usd_fixed(float(mainb)):.2f}"),
            reply_markup=back_only_menu(user.id),
        )
        return

    if context.user_data.get("await_crypto_amt"):
        raw_amt = (update.message.text or "").strip()
        if raw_amt in ("🔙 BACK", "🔙 Back", "⬅️ BACK", "⬅ BACK"):
            # BACK from amount → go back to wallet address entry
            context.user_data["await_crypto_amt"] = False
            context.user_data["await_crypto_addr"] = True
            context.user_data["crypto_addr"] = ""
            context.user_data["payout_reply_mode"] = "crypto"
            await update.message.reply_text(
                tr(user.id, "send_bep20"),
                reply_markup=back_only_menu(user.id),
            )
            return

        # Amount must be an integer USD (because payouts.amount is INTEGER in DB)
        # Amount can be decimal USD (min 0.25). Examples: 1.1  0.25  55
        try:
            amt_d = Decimal(raw_amt)
        except Exception:
            amt_d = None

        if amt_d is None or amt_d.is_nan() or amt_d <= 0:
            await update.message.reply_text(
                tr(user.id, "invalid_amount_usd"),
                reply_markup=back_only_menu(),
            )
            return

        # limit to 2 decimals
        amt_d = amt_d.quantize(Decimal("0.01"), rounding=ROUND_DOWN)

        if amt_d < Decimal("0.25"):
            await update.message.reply_text(
                tr(user.id, "min_crypto_withdraw"),
                reply_markup=back_only_menu(),
            )
            return

        amt = float(amt_d)


        # Check balance again (safety)
        mainb, _holdb = get_balances(user.id)
        need_inr = usd_to_inr_fixed(float(amt))
        if float(mainb) + 1e-6 < float(need_inr):
            await update.message.reply_text(tr(user.id, "insufficient_withdrawal"), reply_markup=back_only_menu())
            return

        addr = (context.user_data.get("crypto_addr") or "").strip()
        if not is_valid_bep20_address(addr):
            # Shouldn't happen, but keep safe
            context.user_data["await_crypto_amt"] = False
            context.user_data["await_crypto_addr"] = True
            await update.message.reply_text(tr(user.id, "address_missing"), reply_markup=back_only_menu())
            return

        inr_need = usd_to_inr_fixed(float(amt))

        now = int(time.time())
        con = db()
        cur = con.cursor()

        # Reserve funds immediately to prevent double-withdraw
        cur.execute("BEGIN")
        cur.execute(
            "UPDATE users SET main_balance = main_balance - ? WHERE user_id=? AND main_balance >= ?",
            (inr_need, user.id, inr_need),
        )
        if cur.rowcount != 1:
            con.rollback()
            con.close()
            await update.message.reply_text(tr(user.id, "insufficient_withdrawal"), reply_markup=back_only_menu())
            return

        meta = f"CRYPTO|USDT|BEP20|{addr}"
        cur.execute(
            "INSERT INTO payouts(user_id, amount, amount_usd, method, upi_or_qr, meta, created_at, state, reserved, refunded) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (user.id, inr_need, float(amt), 'crypto', addr, meta, now, "processing", 1, 0),
        )
        pid = cur.lastrowid
        con.commit()
        con.close()
        cache_invalidate_balance(user.id)  # Balance change hua — cache clear

        # ✅ AUTO-ACCEPT: notify admin + keep it in PROCESSING
        try:
            admin_kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("📌 OPEN CRYPTO REQUEST", callback_data=f"PAY_SEL:{pid}")]
            ])
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=(
                    "💳 NEW CRYPTO PAYOUT (AUTO ACCEPTED ✅)\n\n"
                    "Method: CRYPTO (USDT BEP-20)\n"
                    f"User ID: {user.id}\n"
                    f"Name: {user.username or user.full_name}\n"
                    f"Amount: ${float(amt):.2f}  (₹{int(inr_need)})\n"
                    f"Wallet: {addr}\n"
                    f"Time: {fmt_ts(now)}\n\n"
                    "➡️ Status: PROCESSING. Open and send Transaction ID."
                ),
                reply_markup=admin_kb,
            )
        except Exception:
            pass

        try:
            await update.message.reply_text(
                tr(user.id, "crypto_payout_sent"),
                reply_markup=main_menu_markup(user.id)
            )
        except Exception:
            pass


        # Reset crypto flow (only after success)
        context.user_data["await_crypto_amt"] = False
        context.user_data["await_crypto_addr"] = False
        context.user_data["crypto_addr"] = ""
        context.user_data.pop("payout_reply_mode", None)
        context.user_data.pop("payout_type_select", None)

     
    # =========================
    # UPI / QR FLOW (existing)
    # =========================
    if not context.user_data.get("await_upi"):
        return

    upi = (update.message.text or "").strip()
    if upi in ("🔙 BACK", "🔙 Back", "⬅️ BACK", "⬅ BACK"):
        context.user_data["await_upi"] = False
        context.user_data["payout_amt"] = 0
        context.user_data["payout_amt_inr"] = 0
        context.user_data["payout_reply_mode"] = "upi"
        context.user_data["payout_type_select"] = True
        await update.message.reply_text(
            tr(user.id, "choose_amount"),
            reply_markup=payout_amounts_kb()
        )
        return
    amt_inr = int(context.user_data.get("payout_amt_inr", 0) or 0)
    inr_need = int(amt_inr)

    if amt_inr not in (55,110,210,310,510,1050):
        await update.message.reply_text(tr(user.id, "invalid_amount"), reply_markup=back_only_menu())
        return

    # Check balance again (safety)
    mainb, _holdb = get_balances(user.id)
    if float(mainb) + 1e-6 < float(inr_need):
        await update.message.reply_text(tr(user.id, "insufficient_withdrawal"), reply_markup=back_only_menu())
        return

    kind = classify_upi_or_qr(upi)
    if kind == "upi":
        if not (upi.lower().startswith("upi://") or is_valid_upi_id(upi)):
            await update.message.reply_text(tr(user.id, "invalid_upi"), reply_markup=back_only_menu())
            return
        if is_upi_or_qr_used(upi, "upi", user.id):
            await update.message.reply_text(tr(user.id, "used_upi"), reply_markup=back_only_menu())
            return
    else:
        if len(upi) < 10:
            await update.message.reply_text(tr(user.id, "invalid_qr"), reply_markup=back_only_menu())
            return
        if is_upi_or_qr_used(upi, "qr", user.id):
            await update.message.reply_text(tr(user.id, "used_qr"), reply_markup=back_only_menu())
            return

    now = int(time.time())
    con = db()
    cur = con.cursor()

    # Reserve funds immediately to prevent double-withdraw
    cur.execute("BEGIN")
    cur.execute(
        "UPDATE users SET main_balance = main_balance - ? WHERE user_id=? AND main_balance >= ?",
        (inr_need, user.id, inr_need)
    )
    if cur.rowcount != 1:
        con.rollback()
        con.close()
        await update.message.reply_text(tr(user.id, "insufficient_withdrawal"), reply_markup=back_only_menu())
        return

    cur.execute(
        "INSERT INTO payouts(user_id, amount, amount_usd, method, upi_or_qr, meta, created_at, state, reserved, refunded) VALUES(?,?,?,?,?,?,?,?,?,?)",
        (user.id, inr_need, 0.0, 'upi', (upi.lower() if kind=="upi" else upi), '', now, "processing", 1, 0)
    )
    pid = cur.lastrowid
    con.commit()
    con.close()
    cache_invalidate_balance(user.id)  # Balance change hua — cache clear

    # Reset UPI flow ONLY after success
    context.user_data["await_upi"] = False
    context.user_data["payout_amt"] = 0
    context.user_data.pop("payout_reply_mode", None)
    context.user_data.pop("payout_type_select", None)

    # ✅ AUTO-ACCEPT: directly PROCESSING and notify admin with OPEN button
    try:
        admin_kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📌 OPEN REQUEST", callback_data=f"PAY_SEL:{pid}")]
        ])
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=(
                "💳 NEW UPI PAYOUT (AUTO ACCEPTED ✅)\n\n"
                "Method: UPI\n"
                f"User ID: {user.id}\n"
                f"Name: {user.username or user.full_name}\n"
                f"Amount: ₹{int(inr_need)}\n"
                f"UPI/QR: {upi}\n"
                f"Time: {fmt_ts(now)}\n\n"
                "➡️ Status: PROCESSING. Open and submit payment proof."
            ),
            reply_markup=admin_kb,
        )
    except Exception:
        pass

    await update.message.reply_text(tr(user.id, "payout_sent"), reply_markup=main_menu_markup(user.id))


# =========================
# ADMIN PANEL
# =========================
ADMIN_MENU_KB = ReplyKeyboardMarkup(
    [
        ["📢 Broadcast Text", "🔗 Broadcast Link"],
        ["🖼️ Broadcast Image", "🖼️ Image + Link"],
        ["🗃️ Broadcast File", "👤 Personal Message"],
        ["⛔ Block User", "✅ Unblock User"],
        ["💳 PAYOUT REQUEST", "💳 CRYPTO REQUEST"],
        ["📌 Pin Message"],
        ["📄 PDF Last 30 Days", "📄 PDF Last N Payouts"],
        ["📤 Export (Form)", "📥 Export Users CSV"],
        ["🔝 TOP 50 DAILY USER", "🔝 TOP 50 MONTHLY USER"],
        ["🎭ALL USER", "ADD OR DEDUCT BALANCE ♎"],
        ["💎 EMAIL", "🤖 Auto Reply"],
        ["📊 Bot Stats", "🔍 Search Email"],
        ["📣 Broadcast + Button"],
        ["⚙️ Reward Settings"],
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

AR_MENU_KB = ReplyKeyboardMarkup(
    [
        ["✅ ON", "❌ OFF"],
        ["✍️ Set Text", "📦 Set Any Message"],
        ["🔗 Set Link Button", "🗑 Clear Link Button"],
        ["📄 Status"],
        ["🔙 Back"],
    ],
    resize_keyboard=True
)


def get_autoreply_status_text() -> str:
    con = db()
    cur = con.cursor()
    cur.execute("SELECT enabled, text, reply_kind, media_type, file_id, caption, button_text, button_url FROM autoreply WHERE id=1")
    ar = cur.fetchone()
    con.close()
    enabled = 1 if (ar and int(ar["enabled"]) == 1) else 0
    reply_kind = (ar["reply_kind"] if ar and ar["reply_kind"] is not None else "text").strip() or "text"
    text_val = (ar["text"] if ar and ar["text"] is not None else "").strip()
    media_type = (ar["media_type"] if ar and ar["media_type"] is not None else "").strip()
    caption = (ar["caption"] if ar and ar["caption"] is not None else "").strip()
    button_text = (ar["button_text"] if ar and ar["button_text"] is not None else "").strip()
    button_url = (ar["button_url"] if ar and ar["button_url"] is not None else "").strip()

    if reply_kind == "text":
        body = text_val or "(empty)"
    else:
        body = f"{reply_kind}:{media_type}"
        if caption:
            body += f" | caption: {caption}"

    btn = f"{button_text} -> {button_url}" if (button_text and button_url) else "(none)"
    return (
        f"🤖 AUTO REPLY\n\n"
        f"Status: {'ON' if enabled else 'OFF'}\n"
        f"Type: {reply_kind}\n"
        f"Reply: {body}\n"
        f"Button: {btn}"
    )

async def send_configured_autoreply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    con = db()
    cur = con.cursor()
    cur.execute("SELECT enabled, text, reply_kind, media_type, file_id, caption, button_text, button_url FROM autoreply WHERE id=1")
    ar = cur.fetchone()
    con.close()
    if not ar or int(ar["enabled"] or 0) != 1:
        return False

    kb = None
    button_text = (ar["button_text"] or "").strip()
    button_url = (ar["button_url"] or "").strip()
    if button_text and button_url:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton(button_text, url=button_url)]])

    kind = (ar["reply_kind"] or "text").strip() or "text"
    media_type = (ar["media_type"] or "").strip()
    file_id = (ar["file_id"] or "").strip()
    caption = (ar["caption"] or "").strip()

    if kind == "copy" and file_id and media_type:
        if media_type == "photo":
            await context.bot.send_photo(chat_id=update.effective_chat.id, photo=file_id, caption=caption or None, reply_markup=kb)
            return True
        if media_type == "video":
            await context.bot.send_video(chat_id=update.effective_chat.id, video=file_id, caption=caption or None, reply_markup=kb)
            return True
        if media_type == "document":
            await context.bot.send_document(chat_id=update.effective_chat.id, document=file_id, caption=caption or None, reply_markup=kb)
            return True
        if media_type == "audio":
            await context.bot.send_audio(chat_id=update.effective_chat.id, audio=file_id, caption=caption or None, reply_markup=kb)
            return True
        if media_type == "voice":
            await context.bot.send_voice(chat_id=update.effective_chat.id, voice=file_id, caption=caption or None, reply_markup=kb)
            return True
        if media_type == "animation":
            await context.bot.send_animation(chat_id=update.effective_chat.id, animation=file_id, caption=caption or None, reply_markup=kb)
            return True
        if media_type == "sticker":
            await context.bot.send_sticker(chat_id=update.effective_chat.id, sticker=file_id)
            if kb:
                await context.bot.send_message(chat_id=update.effective_chat.id, text=" ", reply_markup=kb)
            return True

    await update.message.reply_text((ar["text"] or "").strip() or "(empty)", reply_markup=kb)
    return True

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
        WHERE a.created_at >= ? AND a.state IN ('waiting_admin','approved','rejected')
        GROUP BY a.user_id, u.username
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

async def _send_admin_users_page(update_or_q, context, offset: int, total: int):
    """Send/edit paginated user list for admin ALL USER view."""
    PAGE = 20
    con = db(); cur = con.cursor()
    cur.execute(
        "SELECT user_id, username, main_balance, hold_balance FROM users ORDER BY created_at DESC LIMIT ? OFFSET ?",
        (PAGE, offset)
    )
    rows = cur.fetchall(); con.close()

    page = offset // PAGE + 1
    total_pages = max(1, (total + PAGE - 1) // PAGE)

    import re
    def _esc(text: str) -> str:
        return re.sub(r'([_*`\[\]])', r'\\\1', str(text))

    lines = [f"🎭 ALL USER — Page {page}/{total_pages} (Total: {total})",
             "Type username/userid to search 🔍", ""]
    for i, r in enumerate(rows, start=offset + 1):
        uname = _esc((r["username"] or "").strip() or "-")
        lines.append(f"{i}. `{r['user_id']}` | {uname} | ₹{float(r['main_balance']):.0f} / ₹{float(r['hold_balance']):.0f}")

    msg = "\n".join(lines)
    if len(msg) > 4000:
        msg = msg[:4000] + "\n..."

    nav = []
    if offset - PAGE >= 0:
        nav.append(InlineKeyboardButton("◀️ PREV", callback_data=f"ADMIN_ACC:{offset - PAGE}"))
    nav.append(InlineKeyboardButton(f"📄 {page}/{total_pages}", callback_data="NOOP"))
    if offset + PAGE < total:
        nav.append(InlineKeyboardButton("NEXT ▶️", callback_data=f"ADMIN_ACC:{offset + PAGE}"))
    kb = InlineKeyboardMarkup([nav]) if nav else None

    # Called from message handler (txt == "🎭ALL USER") or callback
    if hasattr(update_or_q, "message") and update_or_q.message:
        await update_or_q.message.reply_text(msg, reply_markup=kb, parse_mode="Markdown")
    else:
        # It's a CallbackQuery
        await update_or_q.edit_message_text(msg, reply_markup=kb, parse_mode="Markdown")


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

    # PDF payout proofs
    if txt == "📄 PDF Last 30 Days":
        pdf_path = "payout_proofs.pdf"
        try:
            await asyncio.to_thread(generate_payout_proofs_pdf_from_db, pdf_path, None, 30)
            await update.message.reply_document(document=open(pdf_path, "rb"), filename="payout_proofs.pdf", reply_markup=ADMIN_MENU_KB)
        except Exception as e:
            await update.message.reply_text(f"❌ Failed to build/send PDF: {e}", reply_markup=ADMIN_MENU_KB)
        return

    if txt == "📄 PDF Last N Payouts":
        context.user_data["admin_mode"] = "pdf_lastn_wait"
        await update.message.reply_text("Send N (1-500):", reply_markup=ADMIN_MENU_KB)
        return


    # Export form_table as PDF with date filters
    if txt == "📤 Export (Form)":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("TODAY", callback_data="EXP_FORM:today"),
             InlineKeyboardButton("YESTERDAY", callback_data="EXP_FORM:yesterday")],
            [InlineKeyboardButton("ENTER DATE (FROM–TO)", callback_data="EXP_FORM:range")],
            [InlineKeyboardButton("1 MONTH AGO", callback_data="EXP_FORM:month")],
            [InlineKeyboardButton("ALL", callback_data="EXP_FORM:all")],
        ])
        await update.message.reply_text("📤 Export form_table to PDF. Choose range:", reply_markup=kb)
        return


    # Top users
    if txt == "🔝 TOP 50 DAILY USER":
        await update.message.reply_text(admin_top_users("daily", 50))
        return

    if txt == "🔝 TOP 50 MONTHLY USER":
        await update.message.reply_text(admin_top_users("monthly", 50))
        return

    if txt == "💎 EMAIL":
        # Manual email-based rejection with predefined reasons
        context.user_data["admin_mode"] = "manual_email_lookup"
        await update.message.reply_text("Enter email for manual decision:")
        return

        # All users list + search
    if txt == "🎭ALL USER":
        total = admin_total_users()
        if total == 0:
            await update.message.reply_text("No users found.")
            return
        await _send_admin_users_page(update, context, offset=0, total=total)
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
        await update.message.reply_text(get_autoreply_status_text(), reply_markup=AR_MENU_KB)
        return

    # ── NEW: Bot Stats ──────────────────────────────────────────────
    if txt == "📊 Bot Stats":
        try:
            stats_text = await asyncio.to_thread(admin_bot_stats)
            await update.message.reply_text(stats_text, parse_mode="Markdown", reply_markup=ADMIN_MENU_KB)
        except Exception as e:
            await update.message.reply_text(f"❌ Stats error: {e}", reply_markup=ADMIN_MENU_KB)
        return

    # ── NEW: Export Users CSV ───────────────────────────────────────
    if txt == "📥 Export Users CSV":
        try:
            import io
            csv_data = export_users_csv()
            bio = io.BytesIO(csv_data.encode("utf-8-sig"))  # utf-8-sig for Excel compatibility
            bio.name = "users_export.csv"
            bio.seek(0)
            total = csv_data.count("\n") - 1
            await update.message.reply_document(
                document=bio,
                filename=f"users_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                caption=f"👥 Users export — {total} records",
                reply_markup=ADMIN_MENU_KB
            )
        except Exception as e:
            await update.message.reply_text(f"❌ CSV export error: {e}", reply_markup=ADMIN_MENU_KB)
        return

    # ── NEW: Search by Email ────────────────────────────────────────
    if txt == "🔍 Search Email":
        context.user_data["admin_mode"] = "search_email_wait"
        await update.message.reply_text("🔍 Enter email (or partial) to search in form_table:", reply_markup=ADMIN_MENU_KB)
        return

    if context.user_data.get("admin_mode") == "search_email_wait":
        context.user_data.pop("admin_mode", None)
        rows = search_form_by_email(txt)
        if not rows:
            await update.message.reply_text(f"❌ No results for: `{txt}`", parse_mode="Markdown", reply_markup=ADMIN_MENU_KB)
            return
        lines = [f"🔍 *Search: {txt}* — {len(rows)} result(s)\n"]
        for r in rows:
            try:
                t = datetime.fromtimestamp(int(r["created_at"])).strftime("%Y-%m-%d %H:%M")
            except Exception:
                t = "-"
            lines.append(
                f"👤 UserID: `{r['user_id']}` | {r['first_name'] or '-'}\n"
                f"📧 `{r['email']}`\n"
                f"🔑 `{r['password']}`\n"
                f"📩 Recovery: `{r['recovery_email'] or '-'}`\n"
                f"🕐 {t}"
            )
        msg = "\n\n".join(lines)
        if len(msg) > 4000:
            msg = msg[:4000] + "\n...truncated"
        await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=ADMIN_MENU_KB)
        return

    if txt == "💳 Pending Payouts":
        con = db()
        cur = con.cursor()
        cur.execute("SELECT * FROM payouts WHERE state='pending' ORDER BY id DESC LIMIT 5")
        rows = cur.fetchall()
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

    # ── NEW: Block user with reason ─────────────────────────────────
    if txt == "⛔ Block User":
        context.user_data["admin_mode"] = "block_wait"
        await update.message.reply_text(
            "Send USER ID to block:\n(Optionally add reason after a space)\nExample: `123456789 spam`",
            parse_mode="Markdown", reply_markup=ADMIN_MENU_KB
        )
        return

    if txt == "✅ Unblock User":
        context.user_data["admin_mode"] = "unblock_wait"
        await update.message.reply_text("Send USER ID to UNBLOCK:", reply_markup=ADMIN_MENU_KB)
        return

    # ── NEW: Broadcast + Button ──────────────────────────────────────
    if txt == "📣 Broadcast + Button":
        context.user_data["admin_mode"] = "bc_btn_text"
        await update.message.reply_text(
            "📣 Broadcast with Button\n\nStep 1: Send the message text:",
            reply_markup=ADMIN_MENU_KB
        )
        return

    if txt == "⚙️ Reward Settings":
        task_st  = get_setting("task_rewards", "on")
        ref_st   = get_setting("referral_rewards", "on")
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"TASK REWARD {'✅ ON' if task_st=='on' else '❌ OFF'}", callback_data="TASK_ON" if task_st!="on" else "TASK_OFF")],
            [InlineKeyboardButton(f"REFERRAL {'✅ ON' if ref_st=='on' else '❌ OFF'}", callback_data="REF_ON" if ref_st!="on" else "REF_OFF")],
        ])
        await update.message.reply_text(
            f"⚙️ Reward Settings\n\nTask Rewards: {'ON ✅' if task_st=='on' else 'OFF ❌'}\n"
            f"Referral Rewards: {'ON ✅' if ref_st=='on' else 'OFF ❌'}\n\n"
            "Press button to toggle:",
            reply_markup=kb,
        )
        return

    # ── FIX: Admin Back → Main Menu ──────────────────────────────────────────
    if txt == "🔙 Back":
        context.user_data.pop("admin_mode", None)
        await update.message.reply_text(
            tr(user.id, "welcome_menu"),
            reply_markup=main_menu_markup(user.id)
        )
        return


async def admin_content_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_admin(user.id):
        return

    mode = context.user_data.get("admin_mode")
    # If admin pressed normal USER menu buttons, cancel any pending admin input mode
    try:
        txt = (update.message.text or '').strip() if update.message else ''
        if any(txt_is(txt, k) for k in ['menu_register','menu_accounts','menu_balance','menu_referrals','menu_settings','menu_task','menu_help']):
            context.user_data['admin_mode'] = None
            return
    except Exception:
        pass

    # ── Block user with reason ──────────────────────────────────────
    if mode == "block_wait":
        raw = (update.message.text or "").strip()
        context.user_data["admin_mode"] = None
        parts = raw.split(None, 1)
        uid_str = parts[0] if parts else ""
        reason = parts[1] if len(parts) > 1 else ""
        if not uid_str.isdigit():
            await update.message.reply_text("❌ Invalid User ID.", reply_markup=ADMIN_MENU_KB)
            return
        uid = int(uid_str)
        block_user_db(uid, reason)
        await update.message.reply_text(
            f"⛔ User `{uid}` blocked.\nReason: {reason or '(none)'}",
            parse_mode="Markdown", reply_markup=ADMIN_MENU_KB
        )
        try:
            await context.bot.send_message(chat_id=uid, text="⛔ You have been blocked from using this bot.")
        except Exception:
            pass
        return

    # ── Unblock user ────────────────────────────────────────────────
    if mode == "unblock_wait":
        raw = (update.message.text or "").strip()
        context.user_data["admin_mode"] = None
        if not raw.isdigit():
            await update.message.reply_text("❌ Invalid User ID.", reply_markup=ADMIN_MENU_KB)
            return
        uid = int(raw)
        info = get_block_info(uid)
        if not info:
            await update.message.reply_text(f"ℹ️ User `{uid}` is not blocked.", parse_mode="Markdown", reply_markup=ADMIN_MENU_KB)
            return
        unblock_user_db(uid)
        blocked_at, reason = info
        await update.message.reply_text(
            f"✅ User `{uid}` unblocked.\n"
            f"Was blocked: {fmt_ts(blocked_at)}\n"
            f"Reason: {reason or '(none)'}",
            parse_mode="Markdown", reply_markup=ADMIN_MENU_KB
        )
        try:
            await context.bot.send_message(chat_id=uid, text="✅ You have been unblocked. You can use the bot again.")
        except Exception:
            pass
        return

    # ── Broadcast + Button step 1: text ─────────────────────────────
    if mode == "bc_btn_text":
        context.user_data["bc_btn_msg_text"] = (update.message.text or "").strip()
        context.user_data["admin_mode"] = "bc_btn_label"
        await update.message.reply_text("Step 2: Send the button label (e.g. 'Join Now'):", reply_markup=ADMIN_MENU_KB)
        return

    # ── Broadcast + Button step 2: label ────────────────────────────
    if mode == "bc_btn_label":
        context.user_data["bc_btn_label"] = (update.message.text or "").strip()
        context.user_data["admin_mode"] = "bc_btn_url"
        await update.message.reply_text("Step 3: Send the button URL (https://...):", reply_markup=ADMIN_MENU_KB)
        return

    # ── Broadcast + Button step 3: url → send to all ────────────────
    if mode == "bc_btn_url":
        context.user_data["admin_mode"] = None
        bc_text = context.user_data.pop("bc_btn_msg_text", "")
        bc_label = context.user_data.pop("bc_btn_label", "Click here")
        bc_url = (update.message.text or "").strip()
        if not bc_url.startswith("http"):
            await update.message.reply_text("❌ Invalid URL. Must start with http/https.", reply_markup=ADMIN_MENU_KB)
            return
        kb = InlineKeyboardMarkup([[InlineKeyboardButton(bc_label, url=bc_url)]])
        con = db(); cur = con.cursor()
        cur.execute("SELECT user_id FROM users")
        all_users = [int(r["user_id"]) for r in cur.fetchall()]
        con.close()
        sent = 0; failed = 0
        await update.message.reply_text(f"📣 Broadcasting to {len(all_users)} users...", reply_markup=ADMIN_MENU_KB)
        for uid in all_users:
            try:
                await context.bot.send_message(chat_id=uid, text=bc_text, reply_markup=kb)
                sent += 1
            except Exception:
                failed += 1
        await update.message.reply_text(
            f"✅ Broadcast done!\nSent: {sent}  |  Failed: {failed}",
            reply_markup=ADMIN_MENU_KB
        )
        return



    
    # PDF (payout proofs) - Last N payouts
    if mode == "pdf_lastn_wait":
        raw = (update.message.text or "").strip()
        try:
            n = int(raw)
        except Exception:
            await update.message.reply_text("❌ Please send a number like 50 (1-500).", reply_markup=ADMIN_MENU_KB)
            return
        if n < 1:
            n = 1
        if n > 500:
            n = 500

        pdf_path = "payout_proofs.pdf"
        try:
            await asyncio.to_thread(generate_payout_proofs_pdf_from_db, pdf_path, n, None)
            await update.message.reply_document(document=open(pdf_path, "rb"), filename="payout_proofs.pdf", reply_markup=ADMIN_MENU_KB)
            context.user_data["admin_mode"] = None
        except Exception as e:
            await update.message.reply_text(f"❌ Failed to build/send PDF: {e}", reply_markup=ADMIN_MENU_KB)
        return



    if mode == "export_form_range_wait":
        raw = (update.message.text or "").strip()
        # Accept formats:
        # 2026-02-01 2026-02-15
        # 2026-02-01 to 2026-02-15
        m = re.findall(r"(\d{4}-\d{2}-\d{2})", raw)
        if len(m) != 2:
            await update.message.reply_text("❌ Send dates like: 2026-02-01 2026-02-15", reply_markup=ADMIN_MENU_KB)
            return
        d1, d2 = m[0], m[1]
        try:
            start_dt = datetime.strptime(d1, "%Y-%m-%d")
            end_dt = datetime.strptime(d2, "%Y-%m-%d") + timedelta(days=1)  # inclusive end date
            start_ts = int(start_dt.timestamp())
            end_ts = int(end_dt.timestamp())
        except Exception:
            await update.message.reply_text("❌ Invalid date format. Use YYYY-MM-DD.", reply_markup=ADMIN_MENU_KB)
            return

        context.user_data["admin_mode"] = None
        await update.message.reply_text("⏳ Building form_data.pdf...", reply_markup=ADMIN_MENU_KB)
        pdf_path = "form_data.pdf"
        try:
            rows = _fetch_form_rows_range(start_ts=start_ts, end_ts=end_ts, limit=None)
            # Safety limit to avoid huge PDFs
            if len(rows) > 2000:
                rows = rows[:2000]
            await asyncio.to_thread(export_form_pdf, pdf_path, limit=None, rows=rows)
            await update.message.reply_document(document=open(pdf_path, "rb"), filename="form_data.pdf", reply_markup=ADMIN_MENU_KB)
        except Exception as e:
            await update.message.reply_text(f"❌ Export failed: {e}", reply_markup=ADMIN_MENU_KB)
        return

# Manual email decision flow
    if mode == "manual_email_lookup":
        email = (update.message.text or "").strip()
        if not email:
            await update.message.reply_text("Enter email:")
            return

        con = db()
        cur = con.cursor()
        cur.execute(
            """
            SELECT a.action_id, a.user_id, r.id AS reg_id
            FROM actions a
            JOIN registrations r ON r.id = a.reg_id
            WHERE r.email=?
            ORDER BY a.action_id DESC
            LIMIT 1
            """,
            (email,),
        )
        row = cur.fetchone()
        con.close()

        if not row:
            await update.message.reply_text("No registration found for this email.")
            context.user_data["admin_mode"] = None
            return

        action_id = int(row["action_id"])
        context.user_data["manual_email_target"] = action_id
        context.user_data["admin_mode"] = None

        # Show predefined reasons as inline buttons
        kb_rows = [
            [InlineKeyboardButton(r, callback_data=f"ADMIN_EMAIL_REASON:{action_id}:{i}")]
            for i, r in enumerate(MANUAL_EMAIL_REASONS)
        ]

        await update.message.reply_text(
            f"Select reason for:\n{email}",
            reply_markup=InlineKeyboardMarkup(kb_rows),
        )
        return

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
                lines.append(
                    f"- {r['user_id']} | {r['username'] or ''} | "
                    f"MAIN ₹{float(r['main_balance']):.2f} | HOLD ₹{float(r['hold_balance']):.2f}"
                )
            await update.message.reply_text("\n".join(lines))
        else:
            r = res
            await update.message.reply_text(
                f"Found: {r['user_id']} | {r['username'] or ''}\n"
                f"MAIN ₹{float(r['main_balance']):.2f} | HOLD ₹{float(r['hold_balance']):.2f}"
            )

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
            "+100 main\n-50 hold\n+10 hold\n-25 main\n\n"
            "Optional reason (add after amount):\n"
            "+100 main Bonus reward\n-50 main Penalty"
        )
        return

    if mode == "bal_apply":
        uid = context.user_data.get("bal_user_id")
        if not uid:
            context.user_data["admin_mode"] = None
            await update.message.reply_text("No user selected.", reply_markup=ADMIN_MENU_KB)
            return
        raw_input = (update.message.text or "").strip()
        txt = raw_input.lower()
        m2 = re.match(r'^([\+\-])\s*(\d+(?:\.\d+)?)\s*(main|hold)(?:\s+(.+))?$', raw_input, re.IGNORECASE)
        if not m2:
            await update.message.reply_text("Format: +100 main OR -50 hold OR +100 main Bonus reward")
            return
        sign, amt_s, which, custom_reason = m2.groups()
        which = which.lower()
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
        # ── Ledger entry — reason ke saath ──────────────────────────────────
        base_reason = f"Admin {'added' if amt >= 0 else 'deducted'} ₹{abs(amt):.2f} {which.upper()}"
        reason_str = f"{base_reason} — {custom_reason.strip()}" if custom_reason else base_reason
        if which == "main":
            add_ledger_entry_cur(cur, int(uid), delta_main=float(amt), reason=reason_str)
        else:
            add_ledger_entry_cur(cur, int(uid), delta_hold=float(amt), reason=reason_str)

        con.commit()
        # show updated
        cur.execute("SELECT username, main_balance, hold_balance FROM users WHERE user_id=?", (int(uid),))
        r2 = cur.fetchone()
        con.close()
        cache_invalidate_balance(uid)  # Admin ne balance change kiya — cache clear

        sign_word = "Added" if amt >= 0 else "Deducted"
        await update.message.reply_text(
            f"✅ {sign_word} ₹{abs(amt):.2f} from {which.upper()} for user {uid} ({r2['username'] or ''})\n"
            f"MAIN ₹{float(r2['main_balance']):.2f} | HOLD ₹{float(r2['hold_balance']):.2f}\n"
            f"📒 Ledger entry saved.",
            reply_markup=ADMIN_MENU_KB
        )
        # Notify user
        try:
            notif = (
                f"💰 Your balance has been {'credited' if amt >= 0 else 'debited'} by admin.\n"
                f"Amount: ₹{abs(amt):.2f} ({which.upper()})\n"
                f"New MAIN balance: ₹{float(r2['main_balance']):.2f}"
            )
            await context.bot.send_message(chat_id=int(uid), text=notif)
        except Exception:
            pass
        # keep mode for more edits
        return


    
    # CRYPTO TXID flow (admin)
    if mode == "crypto_txid_wait":
        pid = context.user_data.get("pay_selected")
        if not pid:
            context.user_data["admin_mode"] = None
            await update.message.reply_text("First select a crypto payout from CRYPTO REQUEST.", reply_markup=ADMIN_MENU_KB)
            return

        txid = (update.message.text or "").strip()
        if len(txid) < 10:
            await update.message.reply_text("❌ Transaction ID invalid. Send again:")
            return

        # Load payout
        con = db()
        cur = con.cursor()
        cur.execute("SELECT * FROM payouts WHERE id=?", (int(pid),))
        p = cur.fetchone()
        if not p:
            con.close()
            context.user_data["admin_mode"] = None
            await update.message.reply_text("Payout not found.", reply_markup=ADMIN_MENU_KB)
            return

        if (p["method"] or "").lower() != "crypto":
            con.close()
            context.user_data["admin_mode"] = None
            await update.message.reply_text("This payout is not CRYPTO.", reply_markup=ADMIN_MENU_KB)
            return

        now = int(time.time())

        # Save txid as proof (store in utr field)
        try:
            cur.execute(
                "INSERT OR REPLACE INTO payout_proofs(payout_id, user_id, amount, upi_or_qr, utr, proof_file_id, created_at) VALUES(?,?,?,?,?,?,?)",
                (int(pid), int(p["user_id"]), int(p["amount"]), p["upi_or_qr"], txid, "", now),
            )
        except Exception:
            pass

        # Mark completed
        try:
            cur.execute("UPDATE payouts SET state='completed', reserved=0 WHERE id=?", (int(pid),))
        except Exception:
            cur.execute("UPDATE payouts SET state='completed' WHERE id=?", (int(pid),))

        con.commit()
        con.close()

        # User message (photo 5 style)
        amt_usd = float(p["amount_usd"] or 0.0)
        if amt_usd <= 0:
            amt_usd = inr_to_usd_fixed(float(p["amount"] or 0.0))
        wallet = (p["upi_or_qr"] or "").strip()

        share_text = (
            "YOUR WITHDRAWAL💲 IS SUCCESSFUL.\n"
            "TELL YOUR FRIENDS ABOUT YOUR WITHDRAWAL 💲"
        )
        share_url = "https://t.me/share/url?text=" + urllib.parse.quote(share_text)

        user_kb = InlineKeyboardMarkup([[InlineKeyboardButton("TELL YOUR FRIENDS 🫂", url=share_url)]])

        msg = (
            "💸 Withdrawal Processed!\n\n"
            "Your withdrawal has been successfully sent!\n\n"
            f"💰 Amount: ${amt_usd:.2f}\n"
            f"📮 Wallet: {wallet}\n"
            "✅ Status: Paid\n"
            f"📄 Reference: {pid}\n\n"
            f"🔗 Transaction ID:\n{txid}\n\n"
            "Your funds have been sent to your wallet address.\n"
            "Thank you for using our service! 🎉"
        )

        try:
            await context.bot.send_message(chat_id=int(p["user_id"]), text=msg, reply_markup=user_kb)
        except Exception:
            pass

        # Rebuild PDF (optional)
        try:
            generate_payout_proofs_pdf_from_db("payout_proofs.pdf", 200, None)
        except Exception:
            pass

        context.user_data["admin_mode"] = None
        context.user_data["pay_selected"] = None
        context.user_data["pay_selected_method"] = None
        await update.message.reply_text("✅ Crypto payout completed and sent to user.", reply_markup=ADMIN_MENU_KB)
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
        if msg == "🔙 Back":
            context.user_data["admin_mode"] = None
            await update.message.reply_text("ADMIN PANEL:", reply_markup=ADMIN_MENU_KB)
            return

        if msg == "✅ ON":
            con = db()
            cur = con.cursor()
            cur.execute("UPDATE autoreply SET enabled=1 WHERE id=1")
            con.commit()
            con.close()
            await update.message.reply_text(get_autoreply_status_text(), reply_markup=AR_MENU_KB)
            return

        if msg == "❌ OFF":
            con = db()
            cur = con.cursor()
            cur.execute("UPDATE autoreply SET enabled=0 WHERE id=1")
            con.commit()
            con.close()
            await update.message.reply_text(get_autoreply_status_text(), reply_markup=AR_MENU_KB)
            return

        if msg == "📄 Status":
            await update.message.reply_text(get_autoreply_status_text(), reply_markup=AR_MENU_KB)
            return

        if msg == "✍️ Set Text":
            context.user_data["admin_mode"] = "ar_wait_text"
            await update.message.reply_text("Send new auto reply text:", reply_markup=ReplyKeyboardMarkup([["🔙 Back"]], resize_keyboard=True))
            return

        await update.message.reply_text("Choose an option from Auto Reply menu.", reply_markup=AR_MENU_KB)
        return

    if mode == "ar_wait_text":
        msg = (update.message.text or "").strip()
        if msg == "🔙 Back":
            context.user_data["admin_mode"] = "ar_menu"
            await update.message.reply_text(get_autoreply_status_text(), reply_markup=AR_MENU_KB)
            return
        con = db()
        cur = con.cursor()
        cur.execute("UPDATE autoreply SET text=? WHERE id=1", (msg,))
        con.commit()
        con.close()
        context.user_data["admin_mode"] = "ar_menu"
        await update.message.reply_text("✅ Auto reply text updated.\n\n" + get_autoreply_status_text(), reply_markup=AR_MENU_KB)
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
# DRIVE BACKUP / RESTORE (SQLite DB)
# - Restores DB from Drive on boot (if available)
# - Auto-backup to Drive every N seconds (default 60 = 1 min)
# - Admin-only: /backupnow /backupstat
# =========================

DRIVE_FOLDER_ID = (os.environ.get("DRIVE_FOLDER_ID") or "").strip()
# Fixed FILE_ID mode (as requested):
# - Always overwrite / restore the exact Drive file ID provided here.
# - No auto-create, no name-search fallback.
DRIVE_FILE_ID = (os.environ.get("DRIVE_FILE_ID") or "").strip()
DRIVE_DB_NAME = (os.environ.get("DRIVE_DB_NAME") or "bot.db").strip()
DRIVE_BACKUP_SEC = int(os.environ.get("DRIVE_BACKUP_SEC") or "60")
DRIVE_SA_JSON = (os.environ.get("DRIVE_SA_JSON") or "").strip()
DRIVE_SA_JSON_B64 = (os.environ.get("DRIVE_SA_JSON_B64") or "").strip()

_drive_stats = {
    "enabled": False,
    "started": False,
    "last_ok": 0,
    "runs": 0,
    "last_error": "",
    "last_uploaded_file_id": "",
}

# NOTE: For fixed FILE_ID mode, the service account must be able to access an
# existing file shared from a user's Drive. The narrower `drive.file` scope can
# still lead to "notFound" (404) in some cases even when the file is shared.
# Use full Drive scope for reliability.
DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive"]

def _drive_enabled() -> bool:
    # Fixed FILE_ID mode: enabled only when Service Account JSON + DRIVE_FILE_ID are set.
    return bool((DRIVE_SA_JSON or DRIVE_SA_JSON_B64) and DRIVE_FILE_ID)

def _gdrive_service():
    """Build Google Drive API client using a Service Account JSON (never expires).
    Env options:
      - DRIVE_SA_JSON: raw JSON content
      - DRIVE_SA_JSON_B64: base64(JSON)
    """
    if not (DRIVE_SA_JSON or DRIVE_SA_JSON_B64):
        return None
    try:
        import base64 as _b64
        import json as _json
        from google.oauth2 import service_account
        from googleapiclient.discovery import build as _build
        raw = DRIVE_SA_JSON
        if (not raw) and DRIVE_SA_JSON_B64:
            raw = _b64.b64decode(DRIVE_SA_JSON_B64.encode()).decode()
        info = _json.loads(raw)
        creds = service_account.Credentials.from_service_account_info(info, scopes=DRIVE_SCOPES)
        return _build("drive", "v3", credentials=creds, cache_discovery=False)
    except Exception as e:
        _drive_stats["last_error"] = f"service: {repr(e)}"
        return None

def _gdrive_find_file_id_by_name(svc, filename: str) -> str:
    """Search file by exact name inside DRIVE_FOLDER_ID."""
    if not DRIVE_FOLDER_ID:
        return ""
    try:
        safe_name = filename.replace("'", "''")
        q = (
            f"name='{safe_name}' "
            f"and '{DRIVE_FOLDER_ID}' in parents "
            f"and trashed=false"
        )
        res = svc.files().list(
            q=q,
            spaces="drive",
            fields="files(id,name,modifiedTime)",
            pageSize=10,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        files = res.get("files") or []
        return files[0]["id"] if files else ""
    except Exception as e:
        _drive_stats["last_error"] = f"find: {repr(e)}"
        return ""

def _gdrive_upload_db() -> bool:
    """
    pg_dump → SQL file → overwrite exact DRIVE_FILE_ID on Google Drive.
    Same file ID har baar — naya file nahi banta.
    """
    svc = _gdrive_service()
    if not svc:
        return False
    if not DRIVE_FILE_ID:
        _drive_stats["last_error"] = "upload: DRIVE_FILE_ID missing"
        return False
    db_url = os.environ.get("DATABASE_URL", "").strip()
    if not db_url:
        _drive_stats["last_error"] = "upload: DATABASE_URL not set"
        return False

    tmp_path = None
    try:
        from googleapiclient.http import MediaFileUpload
        import tempfile, subprocess

        # pg_dump → temp SQL file
        with tempfile.NamedTemporaryFile(suffix=".sql", delete=False) as tf:
            tmp_path = tf.name

        result = subprocess.run(
            ["pg_dump", "--no-password", "--format=plain", "--file", tmp_path, db_url],
            capture_output=True, text=True, timeout=120,
        )
        if result.returncode != 0:
            _drive_stats["last_error"] = f"pg_dump failed: {result.stderr[:300]}"
            return False

        # Overwrite same DRIVE_FILE_ID — file Drive pe wohi rehti hai
        media = MediaFileUpload(tmp_path, mimetype="text/plain", resumable=True)
        upd = svc.files().update(
            fileId=DRIVE_FILE_ID,
            media_body=media,
            fields="id",
            supportsAllDrives=True,
        ).execute()
        _drive_stats["last_uploaded_file_id"] = upd.get("id", "") or DRIVE_FILE_ID
        _drive_stats["last_ok"] = int(time.time())
        _drive_stats["last_error"] = ""
        return True

    except Exception as e:
        _drive_stats["last_error"] = f"upload: {repr(e)}"
        return False
    finally:
        try:
            if tmp_path:
                os.remove(tmp_path)
        except Exception:
            pass


def _gdrive_download_to_db() -> bool:
    """
    Download SQL dump from exact DRIVE_FILE_ID → psql restore.
    Boot pe call hota hai — existing DB ke upar restore karta hai.
    """
    svc = _gdrive_service()
    if not svc:
        return False
    if not DRIVE_FILE_ID:
        _drive_stats["last_error"] = "restore: DRIVE_FILE_ID missing"
        return False
    db_url = os.environ.get("DATABASE_URL", "").strip()
    if not db_url:
        _drive_stats["last_error"] = "restore: DATABASE_URL not set"
        return False

    tmp_path = None
    try:
        from googleapiclient.http import MediaIoBaseDownload
        import tempfile, subprocess

        # Drive se SQL dump download karo
        with tempfile.NamedTemporaryFile(suffix=".sql", delete=False) as tf:
            tmp_path = tf.name

        request = svc.files().get_media(fileId=DRIVE_FILE_ID, supportsAllDrives=True)
        with open(tmp_path, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()

        # Restore via psql
        result = subprocess.run(
            ["psql", "--no-password", "--file", tmp_path, db_url],
            capture_output=True, text=True, timeout=180,
        )
        if result.returncode != 0:
            _drive_stats["last_error"] = f"psql restore failed: {result.stderr[:300]}"
            return False

        return True

    except Exception as e:
        _drive_stats["last_error"] = f"restore: {repr(e)}"
        return False
    finally:
        try:
            if tmp_path:
                os.remove(tmp_path)
        except Exception:
            pass

async def _drive_backup_loop():
    _drive_stats["started"] = True
    while True:
        try:
            await asyncio.sleep(max(30, int(DRIVE_BACKUP_SEC)))
            _drive_stats["runs"] += 1
            await asyncio.to_thread(_gdrive_upload_db)
        except Exception as e:
            _drive_stats["last_error"] = f"loop: {repr(e)}"

async def backupnow_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if not is_admin(u.id):
        return
    ok = await asyncio.to_thread(_gdrive_upload_db)
    fid = _drive_stats.get("last_uploaded_file_id") or DRIVE_FILE_ID
    await update.message.reply_text(
        f"☁️ Drive backup: {'✅ OK' if ok else '❌ FAIL'}\n"
        f"FileId: {fid}\n"
        f"Last error: {_drive_stats.get('last_error', '')}"
    )

async def restore_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin-only: Drive ke DRIVE_FILE_ID se PostgreSQL restore karo."""
    u = update.effective_user
    if not is_admin(u.id):
        return
    await update.message.reply_text(
        f"⏳ Restore ho raha hai Drive file `{DRIVE_FILE_ID}` se...",
        parse_mode="Markdown"
    )
    ok = await asyncio.to_thread(_gdrive_download_to_db)
    err = _drive_stats.get("last_error", "")
    await update.message.reply_text(
        f"{'✅ Restore OK' if ok else '❌ Restore FAILED'}\n"
        f"FileId: {DRIVE_FILE_ID}\n"
        f"{('Error: ' + err) if err else ''}"
    )

async def backupstat_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if not is_admin(u.id):
        return
    last_ok = _drive_stats.get("last_ok", 0)
    ts = datetime.fromtimestamp(last_ok).isoformat() if last_ok else "0"
    await update.message.reply_text(
        "☁️ Drive Backup Status\n"
        f"Enabled: {_drive_stats.get('enabled')}\n"
        f"Started: {_drive_stats.get('started')}\n"
        f"Runs: {_drive_stats.get('runs')}\n"
        f"Last OK: {ts}\n"
        f"Last error: {_drive_stats.get('last_error')}\n"
        f"Config FileId: {DRIVE_FILE_ID}\n"
        f"Last Uploaded FileId: {_drive_stats.get('last_uploaded_file_id')}\n"
        f"FolderId: {DRIVE_FOLDER_ID}\n"
        f"DB: {DB}"
    )
async def dbstat_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin-only: show email_cache rows count + last few handles (SQLite)."""
    if not update.effective_user or int(update.effective_user.id) != int(ADMIN_ID):
        return
    try:
        _email_sqlite_init()
        con = db()
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) FROM email_cache")
        total = int(cur.fetchone()[0] or 0)
        cur.execute("SELECT handle, last_seen FROM email_cache ORDER BY last_seen DESC LIMIT 5")
        rows = cur.fetchall()

        lines = [f"📦 email_cache rows: {total}"]
        for h, ts in rows:
            try:
                t = datetime.fromtimestamp(int(ts)).isoformat(sep=" ", timespec="seconds")
            except Exception:
                t = str(ts)
            lines.append(f"- `{h}` @ {t}")

        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"❌ dbstat failed: {e!r}")


async def syncstat_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin-only: show last sync tick + counts + last error."""
    if not update.effective_user or int(update.effective_user.id) != int(ADMIN_ID):
        return
    try:
        st = SYNC_STATE.copy()
        msg = (
            f"🛰️ Sync started: {st.get('started')}\n"
            f"⏱️ Last tick: {st.get('last_tick')}\n"
            f"📥 Last list count: {st.get('last_list_count')}\n"
            f"💾 Handles saved (last tick): {st.get('last_handles_saved')}\n"
            f"⚠️ Last error: {st.get('last_error') or '-'}"
        )
        await update.message.reply_text(msg)
    except Exception as e:
        await update.message.reply_text(f"❌ syncstat failed: {e!r}")

_sweeper_notified = set()  # {hold_credit_id, ...} — processed hold ids track karo

async def hold_sweeper_job(context: ContextTypes.DEFAULT_TYPE):
    """Automatically move matured HOLD -> MAIN (runs in background).
    Also triggers task rewards + referral commission at Accepted moment.

    FIXES:
    - Bug 2: commission_pct sweeper mein alag se fetch nahi, move_matured se aata hai
    - Error logging add kiya taaki silent failures na hon
    """
    def _work():
        now = int(time.time())
        con = db()
        cur = con.cursor()
        cur.execute(
            "SELECT DISTINCT user_id FROM hold_credits WHERE moved=0 AND matured_at<=? LIMIT 200",
            (now,),
        )
        uids = [int(x["user_id"]) for x in cur.fetchall()]
        con.close()
        results = []
        for uid in uids:
            try:
                result = _db_write_retry(lambda u=uid: move_matured_hold_to_main(u))
                amt, task_paid, ref_id, commission_amt = result
                if float(amt) > 0:
                    results.append((uid, float(amt), float(task_paid), ref_id, float(commission_amt)))
            except Exception as _e:
                import traceback
                dprint(f"[SWEEPER] uid={uid} failed: {_e!r}\n{traceback.format_exc()}")
                continue
        return results

    results = await asyncio.to_thread(_work)

    # Is sweeper run mein already notify kiye uids — same run mein spam rokne ke liye
    _this_run_notified = set()

    for uid, amt, task_paid, ref_id, commission_amt in results:
        # Same sweeper run mein duplicate rokna
        if uid in _this_run_notified:
            continue
        _this_run_notified.add(uid)
        # User ko: balance accrual notification (sirf sweeper se — ek baar)
        try:
            await context.bot.send_message(
                chat_id=int(uid),
                text=f"{tr(uid, 'accepted')}\n\n{tr(uid, 'funds_accrual')}"
            )
        except Exception:
            pass
        # User ko: task milestone reward notification
        if task_paid > 0:
            try:
                await context.bot.send_message(
                    chat_id=int(uid),
                    text=f"🎁 Task reward added to MAIN: ₹{int(task_paid)}"
                )
            except Exception:
                pass
        # Referrer ko: commission notification (commission_pct move_matured se hi aata hai)
        if ref_id and commission_amt > 0:
            try:
                await context.bot.send_message(
                    chat_id=int(ref_id),
                    text=(
                        f"💸 Commission Earned!\n\n"
                        f"Tumhara referral ka account 🟢 Accepted hua.\n"
                        f"Commission: +₹{commission_amt:.2f} "
                        f"credited to your Main Balance! 🎉"
                    ),
                )
            except Exception:
                pass


async def _post_init(application: Application):
    # Start Gmail sync after the application is ready (works for webhook + polling)
    try:
        _email_sqlite_init()
    except Exception as e:
        SYNC_STATE["last_error"] = f"sqlite_init: {e!r}"
        dprint(f"[SYNC] sqlite init failed: {e!r}")
    # Start Drive auto-backup loop (every DRIVE_BACKUP_SEC)
    try:
        if _drive_enabled():
            _drive_stats['enabled'] = True
            application.job_queue.run_once(lambda *_: asyncio.create_task(_drive_backup_loop()), when=1)

    except Exception:
        pass

    # Auto move matured HOLD -> MAIN (always)
    # job_queue ki jagah direct asyncio loop — apscheduler dependency nahi chahiye
    async def _sweeper_loop():
        await asyncio.sleep(30)  # bot start hone ke 30 sec baad pehli baar
        while True:
            try:
                await hold_sweeper_job(application)
            except Exception:
                pass
            await asyncio.sleep(300)  # har 5 min mein

    asyncio.create_task(_sweeper_loop())

    try:
        if os.path.exists("token.json") or os.environ.get("GMAIL_TOKEN_JSON"):
            poll_sec = int(os.environ.get("POLL_SEC", "5"))
            max_list = int(os.environ.get("MAX_LIST", "200"))
            asyncio.create_task(_gmail_sync_loop(poll_sec=poll_sec, max_list=max_list))
            dprint(f"[SYNC] scheduled gmail sync task (poll_sec={poll_sec}, max_list={max_list})")


        else:
            dprint("[SYNC] not scheduled (missing GMAIL_TOKEN_JSON/token.json)")
    except Exception as e:
        SYNC_STATE["last_error"] = f"schedule: {e!r}"
        dprint(f"[SYNC] schedule failed: {e!r}")

async def setcommission_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin only. /setcommission → current dikhao. /setcommission 15 → 15% set karo."""
    user = update.effective_user
    if not user or int(user.id) != int(ADMIN_ID):
        return
    args = context.args or []
    if not args:
        current = get_referral_commission_pct()
        per = round(float(PRE_CREDIT_AMOUNT) * current / 100.0, 2)
        await update.message.reply_text(
            f"📊 Referral Commission Settings\n\n"
            f"Current rate: {current:.2f}%\n"
            f"Per approval earn: ₹{per}\n\n"
            f"Change karne ke liye:\n"
            f"/setcommission 10   → 10%\n"
            f"/setcommission 15.5 → 15.5%\n"
            f"/setcommission 0    → band karo"
        )
        return
    try:
        new_pct = float(args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid. Example: /setcommission 10")
        return
    if new_pct < 0 or new_pct > 100:
        await update.message.reply_text("❌ 0 se 100 ke beech hona chahiye.")
        return
    set_admin_setting("referral_commission_pct", str(new_pct))
    per = round(float(PRE_CREDIT_AMOUNT) * new_pct / 100.0, 2)
    if new_pct == 0:
        await update.message.reply_text("✅ Referral commission band kar diya (0%).")
    else:
        await update.message.reply_text(
            f"✅ Referral commission set: {new_pct:.2f}%\n"
            f"Ab har approval pe referrer ko ₹{per} milega (MAIN balance)."
        )



def _ensure_core_tables():
    try:
        import db_pg

        if hasattr(db_pg, "init_db"):
            db_pg.init_db()

    except Exception:
        pass



async def bulk_txt_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle .txt file upload for bulk Gmail submission."""
    user = update.effective_user
    if not user:
        return
    if user.id != ADMIN_ID and is_blocked(user.id):
        return

    state = context.user_data.get("sell_state")
    if state != STATE_WAIT_BULK:
        return  # Only handle in bulk state

    try:
        doc = update.message.document
        if not doc or not doc.file_name.lower().endswith(".txt"):
            return

        if doc.file_size and doc.file_size > 500 * 1024:
            await update.message.reply_text("❌ File too large. Max 500KB.", reply_markup=_sell_cancel_kb())
            return

        file = await doc.get_file()
        file_bytes = await file.download_as_bytearray()
        txt_content = file_bytes.decode("utf-8", errors="ignore")

        accounts = _parse_bulk_accounts(txt_content)
        if not accounts:
            await update.message.reply_text(
                "❌ Could not parse any accounts from file.\nFormat: email:password:recovery_email",
                reply_markup=_sell_cancel_kb()
            )
            return

        MAX_BULK = 50
        if len(accounts) > MAX_BULK:
            await update.message.reply_text(
                f"❌ Maximum {MAX_BULK} accounts allowed. Your file has {len(accounts)}. Please split into smaller batches.",
                reply_markup=_sell_cancel_kb()
            )
            return

        now = int(time.time())
        ok_count = 0
        skip_count = 0
        err_lines = []

        for acc in accounts:
            em = acc["email"].lower().strip()
            pw = acc["password"].strip()
            rv = acc["recovery_email"].lower().strip()
            tf = acc.get("twofa", "").strip()

            if not is_valid_email_syntax(em):
                err_lines.append("Invalid email: " + em); skip_count += 1; continue
            if not is_valid_email_syntax(rv):
                err_lines.append("Invalid recovery: " + rv); skip_count += 1; continue
            if em == rv:
                err_lines.append("Email same as recovery: " + em); skip_count += 1; continue
            if _email_exists_in_db(em):
                err_lines.append("Already exists: " + em); skip_count += 1; continue
            if _recovery_email_exists_in_db(rv):
                err_lines.append("Recovery exists: " + rv); skip_count += 1; continue

            try:
                con = db(); cur = con.cursor()
                cur.execute(
                    "INSERT INTO registrations(user_id, email, password, recovery_email, twofa_key, created_at, state) VALUES(?,?,?,?,?,?,?)",
                    (user.id, em, pw, rv, tf if tf else None, now, "created")
                )
                reg_id = cur.lastrowid
                expires_at = now + ACTION_TIMEOUT_HOURS * 3600
                cur.execute(
                    "INSERT INTO actions(user_id, reg_id, created_at, expires_at, state) VALUES(?,?,?,?,?)",
                    (user.id, reg_id, now, expires_at, "waiting_admin")
                )
                action_id = cur.lastrowid
                con.commit(); con.close()
                try:
                    hid = add_hold_credit(user.id, float(PRE_CREDIT_AMOUNT))
                    con2 = db(); cur2 = con2.cursor()
                    cur2.execute(
                        "INSERT INTO precredits(action_id, user_id, hold_credit_id, amount, created_at, reverted) VALUES(?,?,?,?,?,0)",
                        (int(action_id), int(user.id), int(hid), float(PRE_CREDIT_AMOUNT), now)
                    )
                    con2.commit(); con2.close()
                except Exception:
                    pass
                try:
                    save_form_row(int(reg_id), int(user.id), "", em, pw, rv, now)
                except Exception:
                    pass
                ok_count += 1
            except Exception:
                err_lines.append("DB error for " + em); skip_count += 1

        msg = f"✅ {ok_count} account(s) submitted from file."
        if skip_count:
            msg += f"\n⚠️ {skip_count} skipped."
        if err_lines:
            msg += "\n" + "\n".join(err_lines[:10])

        context.user_data.pop("sell_state", None)
        context.user_data.pop("reg_flow", None)
        temp_data.pop(user.id, None)
        await update.message.reply_text(msg, reply_markup=main_menu_markup(user.id))

    except Exception as e:
        await update.message.reply_text(f"❌ Error reading file: {e}", reply_markup=_sell_cancel_kb())


def main():
    print("[BOT] Starting...", flush=True)
    # Restore DB from Google Drive before initializing (best-effort)
    try:
        if _drive_enabled():
            _drive_stats["enabled"] = True
            _gdrive_download_to_db()
    except Exception:
        pass

    print("[BOT] Initializing DB...", flush=True)
    try:
        init_db()
    except Exception as _db_err:
        print(f"[BOT] FATAL: init_db failed: {_db_err!r}", flush=True)
        import traceback; traceback.print_exc()
        raise
    print(f"[BOT] DB OK: {DB[:40]}...", flush=True)

    # Migration: add new columns to registrations table (twofa_key)
    try:
        _mig_con2 = db()
        _mig_cur2 = _mig_con2.cursor()
        for _col, _type in [("twofa_key", "TEXT")]:
            try:
                _mig_cur2.execute(f"ALTER TABLE registrations ADD COLUMN {_col} {_type}")
                _mig_con2.commit()
                dprint(f"[MIGRATION] registrations.{_col} added")
            except Exception:
                pass  # column already exists
        _mig_con2.close()
    except Exception as _mig2_err:
        dprint(f"[MIGRATION] registrations migration error: {_mig2_err!r}")

    # Migration: admin_settings defaults for reward toggles
    try:
        _mig_con3 = db()
        _mig_cur3 = _mig_con3.cursor()
        _mig_cur3.execute(
            "INSERT OR IGNORE INTO admin_settings(key, value) VALUES('task_rewards','on')"
        )
        _mig_cur3.execute(
            "INSERT OR IGNORE INTO admin_settings(key, value) VALUES('referral_rewards','on')"
        )
        _mig_con3.commit()
        _mig_con3.close()
    except Exception as _mig3_err:
        dprint(f"[MIGRATION] admin_settings defaults error: {_mig3_err!r}")

    # Migration: referral_bonuses mein reg_id column add karo (agar nahi hai)
    # Yeh column per-registration deduplication ke liye zaroori hai
    try:
        _mig_con = db()
        _mig_cur = _mig_con.cursor()
        try:
            _mig_cur.execute("ALTER TABLE referral_bonuses ADD COLUMN reg_id INTEGER DEFAULT NULL")
            _mig_con.commit()
            dprint("[MIGRATION] referral_bonuses.reg_id column added")
        except Exception:
            pass  # column already exists
        # Remove old amount=0 placeholder rows (were inserted by ensure_user, caused bonus skipping)
        _mig_cur.execute("DELETE FROM referral_bonuses WHERE amount=0")
        _mig_con.commit()
        deleted = _mig_cur.rowcount
        if deleted:
            dprint(f"[MIGRATION] Removed {deleted} amount=0 placeholder rows from referral_bonuses")
        _mig_con.close()
    except Exception as mig_err:
        dprint(f"[MIGRATION] referral_bonuses migration error: {mig_err!r}")

    print("[BOT] Building application...", flush=True)
    app = Application.builder().token(BOT_TOKEN).post_init(_post_init).build()

    # Commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("dbstat", dbstat_cmd))
    app.add_handler(CommandHandler("syncstat", syncstat_cmd))
    app.add_handler(CommandHandler("admin", admin_cmd))
    app.add_handler(CommandHandler("export", export_cmd))
    # Admin-only Drive backup commands
    app.add_handler(CommandHandler("backupnow", backupnow_cmd))
    app.add_handler(CommandHandler("backupstat", backupstat_cmd))
    app.add_handler(CommandHandler("restore", restore_cmd))
    app.add_handler(CommandHandler("formimg", formimg_cmd))
    app.add_handler(CommandHandler("referral", referral_cmd))
    app.add_handler(CommandHandler("setcommission", setcommission_cmd))

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
    # QR photo handler for sell flow (2FA secret auto-extract)
    app.add_handler(MessageHandler(filters.PHOTO, menu_handler), group=3)
    # .txt file handler for bulk submission
    app.add_handler(MessageHandler(filters.Document.FileExtension("txt"), bulk_txt_handler), group=3)

    # ✅ Gmail sync is scheduled in _post_init()

    # =========================
    # WEBHOOK (Railway)
    # =========================
    dprint("✅ Bot started (WEBHOOK)...")
    dprint("[VERSION] main.py = v4-reg-updated-at-fix-2026-05-12", flush=True)

    port = int(os.environ.get("PORT", "8080"))

    # Railway built-in public domain (preferred). Fallback to manual variable if you set it.
    public_domain = (os.environ.get("RAILWAY_PUBLIC_DOMAIN") or os.environ.get("RAILWAY_STATIC_URL") or "").strip()
    print(f"[BOT] public_domain={public_domain!r}", flush=True)
    if not public_domain:
        print("[BOT] No public domain — running polling mode", flush=True)
        app.run_polling()
        return

    url_path = (os.environ.get("WEBHOOK_PATH") or "").strip().lstrip("/")
    if not url_path:
        url_path = str(BOT_TOKEN).strip().lstrip("/")

    webhook_url = f"https://{public_domain}/{url_path}"
    print(f"[BOT] Webhook URL: https://{public_domain}/***", flush=True)

    app.run_webhook(
        listen="0.0.0.0",
        port=port,
        url_path=url_path,
        webhook_url=webhook_url,
    )

if __name__ == "__main__":
    main()
