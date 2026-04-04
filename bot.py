import logging
import asyncio
import sqlite3
import re
import os
from datetime import datetime, date, timedelta
import uuid
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from telegram.constants import ParseMode

# Настройка логирования
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# ==================== ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ ====================
BOT_TOKEN = os.environ.get('BOT_TOKEN')
ADMIN_IDS = [int(id.strip()) for id in os.environ.get('ADMIN_IDS', '').split(',') if id.strip()]
GROUP_CHAT_ID = int(os.environ.get('GROUP_CHAT_ID'))
SUPPORT_CHAT_ID = int(os.environ.get('SUPPORT_CHAT_ID', os.environ.get('GROUP_CHAT_ID')))
CHANNEL_ID = os.environ.get('CHANNEL_ID')

# Проверка обязательных переменных
if not BOT_TOKEN:
    logger.error("❌ BOT_TOKEN не задан!")
    exit(1)
if not ADMIN_IDS:
    logger.error("❌ ADMIN_IDS не задан!")
    exit(1)
if not GROUP_CHAT_ID:
    logger.error("❌ GROUP_CHAT_ID не задан!")
    exit(1)
if not CHANNEL_ID:
    logger.error("❌ CHANNEL_ID не задан!")
    exit(1)

DB_FILE = 'bot_data.db'

def escape_markdown(text: str) -> str:
    if not text:
        return ""
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', str(text))

LEVELS = [
    {"name": "🌱 Новичок", "points": 0},
    {"name": "🌿 Начинающий", "points": 5},
    {"name": "⭐ Любитель", "points": 15},
    {"name": "🔥 Эксперт", "points": 30},
    {"name": "💎 Профи", "points": 50},
    {"name": "👑 Мастер", "points": 80},
    {"name": "🏆 ЛЕГЕНДА", "points": 120}
]

def get_level(rating):
    current_level = LEVELS[0]
    next_level = LEVELS[1] if len(LEVELS) > 1 else None
    for i, level in enumerate(LEVELS):
        if rating >= level["points"]:
            current_level = level
            next_level = LEVELS[i + 1] if i + 1 < len(LEVELS) else None
    if next_level:
        points_for_next = next_level["points"] - rating
        total_needed = next_level["points"] - current_level["points"]
        current_progress = rating - current_level["points"]
        progress_percent = int(current_progress / total_needed * 20) if total_needed > 0 else 20
        progress_bar = "▰" * progress_percent + "▱" * (20 - progress_percent)
    else:
        points_for_next = 0
        progress_bar = "▰" * 20
    return current_level, next_level, points_for_next, progress_bar

# ==================== БАЗА ДАННЫХ ====================
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY, 
        username TEXT, 
        first_name TEXT, 
        registration_date TEXT, 
        ads_sent INTEGER DEFAULT 0, 
        ads_published INTEGER DEFAULT 0, 
        rating INTEGER DEFAULT 0, 
        is_blocked INTEGER DEFAULT 0, 
        is_admin INTEGER DEFAULT 0, 
        last_ad_time TEXT,
        last_ticket_time TEXT
    )''')
    
    try:
        c.execute("ALTER TABLE users ADD COLUMN last_ticket_time TEXT")
    except:
        pass
    
    c.execute('''CREATE TABLE IF NOT EXISTS daily_stats (
        user_id INTEGER, 
        date TEXT, 
        ads_count INTEGER DEFAULT 0, 
        PRIMARY KEY (user_id, date)
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS stats (
        date TEXT PRIMARY KEY, 
        published_count INTEGER DEFAULT 0
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS bot_settings (
        key TEXT PRIMARY KEY, 
        value TEXT
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS tickets (
        ticket_id TEXT PRIMARY KEY, 
        user_id INTEGER, 
        username TEXT, 
        first_name TEXT, 
        message TEXT, 
        status TEXT DEFAULT 'open', 
        created_at TEXT, 
        closed_at TEXT, 
        closed_by INTEGER, 
        admin_reply TEXT, 
        admin_reply_sent INTEGER DEFAULT 0
    )''')
    
    c.execute("INSERT OR IGNORE INTO bot_settings (key, value) VALUES ('bot_enabled', '1')")
    c.execute("INSERT OR IGNORE INTO bot_settings (key, value) VALUES ('maintenance_message', '🔧 Бот временно недоступен. Ведутся технические работы.')")
    c.execute("INSERT OR IGNORE INTO bot_settings (key, value) VALUES ('channel_id', ?)", (CHANNEL_ID,))
    c.execute("INSERT OR IGNORE INTO bot_settings (key, value) VALUES ('cooldown_minutes', '10')")
    c.execute("INSERT OR IGNORE INTO bot_settings (key, value) VALUES ('cooldown_enabled', '1')")
    c.execute("INSERT OR IGNORE INTO bot_settings (key, value) VALUES ('ticket_cooldown_minutes', '5')")
    c.execute("INSERT OR IGNORE INTO bot_settings (key, value) VALUES ('ticket_cooldown_enabled', '1')")
    
    default_welcome = "✋ Здравствуйте, {name}!\n\nЭто бот канала Купи/Продай Rostov\n\n🌴 Сюда ты можешь кидать свои объявления!\n\n💥 Обязательно указывайте свой username!"
    c.execute("INSERT OR IGNORE INTO bot_settings (key, value) VALUES ('welcome_message', ?)", (default_welcome,))
    
    conn.commit()
    conn.close()
    logger.info("✅ База данных инициализирована")

init_db()

# ==================== ФУНКЦИИ ДЛЯ КД ТИКЕТОВ ====================
def get_ticket_cooldown_minutes():
    try:
        val = get_bot_setting('ticket_cooldown_minutes')
        return int(val) if val else 5
    except:
        return 5

def set_ticket_cooldown_minutes(minutes):
    set_bot_setting('ticket_cooldown_minutes', str(minutes))

def is_ticket_cooldown_enabled():
    val = get_bot_setting('ticket_cooldown_enabled')
    return val == '1'

def set_ticket_cooldown_enabled(enabled):
    set_bot_setting('ticket_cooldown_enabled', '1' if enabled else '0')

def can_send_ticket(user_id):
    if is_user_blocked(user_id):
        return False, None
    if not is_ticket_cooldown_enabled():
        return True, None
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT last_ticket_time FROM users WHERE user_id = ?", (user_id,))
        result = c.fetchone()
        conn.close()
        if not result or not result[0]:
            return True, None
        last_time = datetime.fromisoformat(result[0])
        cooldown = get_ticket_cooldown_minutes()
        next_time = last_time + timedelta(minutes=cooldown)
        now = datetime.now()
        if now >= next_time:
            return True, None
        else:
            wait_seconds = int((next_time - now).total_seconds())
            wait_minutes = wait_seconds // 60
            wait_seconds = wait_seconds % 60
            return False, (wait_minutes, wait_seconds)
    except Exception as e:
        logger.error(f"Ошибка can_send_ticket: {e}")
        return True, None

def update_last_ticket_time(user_id):
    try:
        now = datetime.now().isoformat()
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("UPDATE users SET last_ticket_time = ? WHERE user_id = ?", (now, user_id))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Ошибка update_last_ticket_time: {e}")

# ==================== ФУНКЦИИ БД ====================
def get_user_stats(user_id):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        reg_date = datetime.now().isoformat()
        c.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
        if not c.fetchone():
            c.execute("INSERT INTO users (user_id, username, first_name, registration_date, ads_sent, ads_published, rating, is_blocked, is_admin, last_ad_time, last_ticket_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                      (user_id, None, None, reg_date, 0, 0, 0, 0, 1 if user_id in ADMIN_IDS else 0, None, None))
            conn.commit()
        c.execute("SELECT username, first_name, registration_date, ads_sent, ads_published, rating, is_blocked, is_admin, last_ad_time, last_ticket_time FROM users WHERE user_id = ?", (user_id,))
        result = c.fetchone()
        conn.close()
        return result if result else (None, None, reg_date, 0, 0, 0, 0, 1 if user_id in ADMIN_IDS else 0, None, None)
    except Exception as e:
        logger.error(f"Ошибка get_user_stats: {e}")
        return (None, None, datetime.now().isoformat(), 0, 0, 0, 0, 0, None, None)

def update_user_ads(user_id, username, first_name):
    try:
        today = date.today().isoformat()
        now = datetime.now().isoformat()
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("UPDATE users SET username = ?, first_name = ?, last_ad_time = ? WHERE user_id = ?", (username, first_name, now, user_id))
        c.execute("INSERT INTO daily_stats (user_id, date, ads_count) VALUES (?, ?, 1) ON CONFLICT(user_id, date) DO UPDATE SET ads_count = ads_count + 1", (user_id, today))
        c.execute("UPDATE users SET ads_sent = ads_sent + 1 WHERE user_id = ?", (user_id,))
        c.execute("UPDATE users SET rating = ads_sent + ads_published WHERE user_id = ?", (user_id,))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Ошибка update_user_ads: {e}")

def increment_published(user_id):
    try:
        today = date.today().isoformat()
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("UPDATE users SET ads_published = ads_published + 1 WHERE user_id = ?", (user_id,))
        c.execute("UPDATE users SET rating = ads_sent + ads_published WHERE user_id = ?", (user_id,))
        c.execute("INSERT INTO stats (date, published_count) VALUES (?, 1) ON CONFLICT(date) DO UPDATE SET published_count = published_count + 1", (today,))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Ошибка increment_published: {e}")

def get_daily_stats():
    try:
        today = date.today().isoformat()
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT COALESCE(published_count, 0) FROM stats WHERE date = ?", (today,))
        result = c.fetchone()
        conn.close()
        return result[0] if result else 0
    except Exception as e:
        logger.error(f"Ошибка get_daily_stats: {e}")
        return 0

def get_total_ads_sent():
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT COALESCE(SUM(ads_sent), 0) FROM users")
        return c.fetchone()[0]
    except Exception as e:
        logger.error(f"Ошибка get_total_ads_sent: {e}")
        return 0

def get_total_ads_published():
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT COALESCE(SUM(ads_published), 0) FROM users")
        return c.fetchone()[0]
    except Exception as e:
        logger.error(f"Ошибка get_total_ads_published: {e}")
        return 0

def block_user(user_id):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("UPDATE users SET is_blocked = 1 WHERE user_id = ?", (user_id,))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Ошибка block_user: {e}")

def unblock_user(user_id):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("UPDATE users SET is_blocked = 0 WHERE user_id = ?", (user_id,))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Ошибка unblock_user: {e}")

def is_user_blocked(user_id):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT is_blocked FROM users WHERE user_id = ?", (user_id,))
        result = c.fetchone()
        conn.close()
        return result[0] == 1 if result else False
    except Exception as e:
        logger.error(f"Ошибка is_user_blocked: {e}")
        return False

def get_total_users():
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM users")
        return c.fetchone()[0]
    except Exception as e:
        logger.error(f"Ошибка get_total_users: {e}")
        return 0

def get_active_users_today():
    try:
        today = date.today().isoformat()
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT COUNT(DISTINCT user_id) FROM daily_stats WHERE date = ?", (today,))
        return c.fetchone()[0]
    except Exception as e:
        logger.error(f"Ошибка get_active_users_today: {e}")
        return 0

def get_blocked_users_count():
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM users WHERE is_blocked = 1")
        return c.fetchone()[0]
    except Exception as e:
        logger.error(f"Ошибка get_blocked_users_count: {e}")
        return 0

def get_all_users(limit=100, offset=0):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT user_id, username, first_name, registration_date, ads_sent, ads_published, rating, is_blocked, is_admin FROM users ORDER BY rating DESC LIMIT ? OFFSET ?", (limit, offset))
        return c.fetchall()
    except Exception as e:
        logger.error(f"Ошибка get_all_users: {e}")
        return []

def get_top_users(limit=10):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT username, first_name, rating, ads_sent, ads_published FROM users WHERE is_blocked = 0 ORDER BY rating DESC LIMIT ?", (limit,))
        return c.fetchall()
    except Exception as e:
        logger.error(f"Ошибка get_top_users: {e}")
        return []

def get_bot_setting(key):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT value FROM bot_settings WHERE key = ?", (key,))
        result = c.fetchone()
        conn.close()
        return result[0] if result else None
    except Exception as e:
        logger.error(f"Ошибка get_bot_setting: {e}")
        return None

def set_bot_setting(key, value):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("UPDATE bot_settings SET value = ? WHERE key = ?", (value, key))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Ошибка set_bot_setting: {e}")

def is_bot_enabled():
    return get_bot_setting('bot_enabled') == '1'

def get_welcome_message():
    return get_bot_setting('welcome_message')

def set_welcome_message(message):
    set_bot_setting('welcome_message', message)

def get_channel():
    return get_bot_setting('channel_id')

def set_channel(channel_id):
    set_bot_setting('channel_id', channel_id)

def get_cooldown_minutes():
    try:
        val = get_bot_setting('cooldown_minutes')
        return int(val) if val else 10
    except:
        return 10

def set_cooldown_minutes(minutes):
    set_bot_setting('cooldown_minutes', str(minutes))

def is_cooldown_enabled():
    val = get_bot_setting('cooldown_enabled')
    return val == '1'

def set_cooldown_enabled(enabled):
    set_bot_setting('cooldown_enabled', '1' if enabled else '0')

def can_send_ad(user_id):
    if is_user_blocked(user_id):
        return False, None
    if not is_cooldown_enabled():
        return True, None
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT last_ad_time FROM users WHERE user_id = ?", (user_id,))
        result = c.fetchone()
        conn.close()
        if not result or not result[0]:
            return True, None
        last_time = datetime.fromisoformat(result[0])
        cooldown = get_cooldown_minutes()
        next_time = last_time + timedelta(minutes=cooldown)
        now = datetime.now()
        if now >= next_time:
            return True, None
        else:
            wait_seconds = int((next_time - now).total_seconds())
            wait_minutes = wait_seconds // 60
            wait_seconds = wait_seconds % 60
            return False, (wait_minutes, wait_seconds)
    except Exception as e:
        logger.error(f"Ошибка can_send_ad: {e}")
        return True, None

def is_admin(user_id):
    return user_id in ADMIN_IDS

def set_admin(user_id, is_admin_val):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("UPDATE users SET is_admin = ? WHERE user_id = ?", (1 if is_admin_val else 0, user_id))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Ошибка set_admin: {e}")

def close_ticket(ticket_id, closed_by):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        closed_at = datetime.now().isoformat()
        c.execute("UPDATE tickets SET status = 'closed', closed_at = ?, closed_by = ? WHERE ticket_id = ?",
                  (closed_at, closed_by, ticket_id))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Ошибка close_ticket: {e}")
        return False

def create_ticket(user_id, username, first_name, message):
    try:
        if is_user_blocked(user_id):
            return None
        
        can_send, wait_time = can_send_ticket(user_id)
        if not can_send:
            return None
        
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        ticket_id = str(uuid.uuid4())[:8]
        created_at = datetime.now().isoformat()
        c.execute("INSERT INTO tickets (ticket_id, user_id, username, first_name, message, status, created_at, admin_reply_sent) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                  (ticket_id, user_id, username, first_name, message, 'open', created_at, 0))
        conn.commit()
        conn.close()
        
        update_last_ticket_time(user_id)
        
        logger.info(f"✅ Создан тикет {ticket_id} от пользователя {user_id}")
        return ticket_id
    except Exception as e:
        logger.error(f"Ошибка create_ticket: {e}")
        return None

def get_ticket_user_id(ticket_id):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT user_id FROM tickets WHERE ticket_id = ?", (ticket_id,))
        result = c.fetchone()
        conn.close()
        return result[0] if result else None
    except Exception as e:
        logger.error(f"Ошибка get_ticket_user_id: {e}")
        return None

def get_ticket_reply_sent(ticket_id):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT admin_reply_sent FROM tickets WHERE ticket_id = ?", (ticket_id,))
        result = c.fetchone()
        conn.close()
        return result[0] if result else 0
    except Exception as e:
        logger.error(f"Ошибка get_ticket_reply_sent: {e}")
        return 0

def update_ticket_reply(ticket_id, reply_text):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("UPDATE tickets SET admin_reply = ?, admin_reply_sent = 1 WHERE ticket_id = ?", (reply_text, ticket_id))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Ошибка update_ticket_reply: {e}")
        return False

def get_open_tickets_count():
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM tickets WHERE status = 'open'")
        return c.fetchone()[0]
    except Exception as e:
        logger.error(f"Ошибка get_open_tickets_count: {e}")
        return 0

def get_all_users_stats():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    c.execute("SELECT user_id, username, first_name, ads_published FROM users WHERE ads_published > 0 ORDER BY ads_published DESC LIMIT 15")
    all_time_stats = c.fetchall()
    
    today = date.today().isoformat()
    c.execute("SELECT u.user_id, u.username, u.first_name, COALESCE(SUM(da.ads_count), 0) FROM users u LEFT JOIN daily_stats da ON u.user_id = da.user_id AND da.date = ? WHERE u.ads_published > 0 GROUP BY u.user_id HAVING COALESCE(SUM(da.ads_count), 0) > 0 ORDER BY COALESCE(SUM(da.ads_count), 0) DESC LIMIT 15", (today,))
    today_stats = c.fetchall()
    
    week_ago = (date.today() - timedelta(days=7)).isoformat()
    c.execute("SELECT u.user_id, u.username, u.first_name, COALESCE(SUM(da.ads_count), 0) FROM users u LEFT JOIN daily_stats da ON u.user_id = da.user_id AND da.date >= ? WHERE u.ads_published > 0 GROUP BY u.user_id HAVING COALESCE(SUM(da.ads_count), 0) > 0 ORDER BY COALESCE(SUM(da.ads_count), 0) DESC LIMIT 15", (week_ago,))
    week_stats = c.fetchall()
    
    c.execute("SELECT COALESCE(SUM(ads_published), 0) FROM users")
    total_posts = c.fetchone()[0]
    
    c.execute("SELECT COALESCE(SUM(ads_count), 0) FROM daily_stats WHERE date = ?", (today,))
    today_posts = c.fetchone()[0]
    
    c.execute("SELECT COALESCE(SUM(ads_count), 0) FROM daily_stats WHERE date >= ?", (week_ago,))
    week_posts = c.fetchone()[0]
    
    c.execute("SELECT COUNT(DISTINCT user_id) FROM users WHERE ads_published > 0")
    total_authors = c.fetchone()[0]
    
    c.execute("SELECT COUNT(DISTINCT user_id) FROM daily_stats WHERE date = ?", (today,))
    today_authors = c.fetchone()[0]
    
    c.execute("SELECT COUNT(DISTINCT user_id) FROM daily_stats WHERE date >= ?", (week_ago,))
    week_authors = c.fetchone()[0]
    
    conn.close()
    return all_time_stats, today_stats, week_stats, total_posts, today_posts, week_posts, total_authors, today_authors, week_authors

def find_user_by_username_or_id(search_term):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        try:
            user_id = int(search_term)
            c.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
            result = c.fetchone()
            if result:
                conn.close()
                return result
        except:
            pass
        username = search_term.replace('@', '').lower()
        c.execute("SELECT * FROM users WHERE LOWER(username) = ?", (username,))
        result = c.fetchone()
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Ошибка find_user: {e}")
        return None

# ==================== КЛАВИАТУРЫ ====================
def get_main_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("📋 Отправить объявление"), KeyboardButton("👤 Мой профиль ⭐")],
        [KeyboardButton("📊 Статистика"), KeyboardButton("❓ Помощь / Тикет")]
    ], resize_keyboard=True)

def get_group_keyboard(user_id, is_published=False):
    if is_published:
        return InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Выложить еще раз", callback_data=f"republish_{user_id}")]])
    
    is_blocked = is_user_blocked(user_id)
    
    if is_blocked:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Разблокировать", callback_data=f"unblock_{user_id}")]
        ])
    else:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Выложить в канал", callback_data=f"publish_{user_id}"),
             InlineKeyboardButton("💬 Ответить", callback_data=f"reply_to_user_{user_id}")],
            [InlineKeyboardButton("❌ Удалить", callback_data=f"delete_{user_id}"),
             InlineKeyboardButton("🚫 Заблокировать", callback_data=f"block_{user_id}")]
        ])

def get_admin_keyboard():
    open_tickets = get_open_tickets_count()
    tickets_button = f"🎫 Тикеты ({open_tickets})" if open_tickets > 0 else "🎫 Тикеты"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton("👥 Список пользователей", callback_data="admin_users")],
        [InlineKeyboardButton("🔍 Поиск пользователя", callback_data="admin_search")],
        [InlineKeyboardButton("🏆 Топ", callback_data="admin_top")],
        [InlineKeyboardButton(tickets_button, callback_data="admin_tickets")],
        [InlineKeyboardButton("📨 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton("💣 СПАМ РАССЫЛКА", callback_data="admin_spam")],
        [InlineKeyboardButton("💬 Отправить сообщение", callback_data="admin_send_message")],
        [InlineKeyboardButton("⚙️ Настройки", callback_data="admin_settings")],
        [InlineKeyboardButton("🔙 Выход", callback_data="admin_exit")]
    ])

def get_user_action_keyboard(user_id, is_blocked):
    keyboard = []
    if is_blocked:
        keyboard.append([InlineKeyboardButton("✅ Разблокировать", callback_data=f"admin_unblock_{user_id}")])
    else:
        keyboard.append([InlineKeyboardButton("❌ Заблокировать", callback_data=f"admin_block_{user_id}")])
    keyboard.append([InlineKeyboardButton("📊 Статистика", callback_data=f"user_stats_{user_id}")])
    keyboard.append([InlineKeyboardButton("💬 Отправить сообщение", callback_data=f"send_msg_{user_id}")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_back")])
    return InlineKeyboardMarkup(keyboard)

def get_bot_settings_keyboard():
    enabled = is_bot_enabled()
    status_text = "🟢 Включен" if enabled else "🔴 Выключен"
    cooldown = get_cooldown_minutes()
    cooldown_enabled = is_cooldown_enabled()
    cooldown_status = "🟢 Вкл" if cooldown_enabled else "🔴 Выкл"
    
    ticket_cooldown = get_ticket_cooldown_minutes()
    ticket_cooldown_enabled = is_ticket_cooldown_enabled()
    ticket_cooldown_status = "🟢 Вкл" if ticket_cooldown_enabled else "🔴 Выкл"
    
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🤖 Статус: {status_text}", callback_data="admin_toggle_bot")],
        [InlineKeyboardButton("📢 Изменить канал", callback_data="admin_edit_channel")],
        [InlineKeyboardButton(f"⏱️ КД на посты: {cooldown} мин [{cooldown_status}]", callback_data="admin_edit_cooldown")],
        [InlineKeyboardButton("⏱️ Вкл/Выкл КД (посты)", callback_data="admin_toggle_cooldown")],
        [InlineKeyboardButton(f"🎫 КД на тикеты: {ticket_cooldown} мин [{ticket_cooldown_status}]", callback_data="admin_edit_ticket_cooldown")],
        [InlineKeyboardButton("🎫 Вкл/Выкл КД (тикеты)", callback_data="admin_toggle_ticket_cooldown")],
        [InlineKeyboardButton("✏️ Изменить приветствие", callback_data="admin_edit_welcome")],
        [InlineKeyboardButton("✏️ Сообщение о тех.работах", callback_data="admin_edit_maintenance")],
        [InlineKeyboardButton("👑 Управление админами", callback_data="admin_manage_admins")],
        [InlineKeyboardButton("🔙 Назад", callback_data="admin_back")]
    ])

def get_manage_admins_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Назначить админа", callback_data="admin_add_admin")],
        [InlineKeyboardButton("➖ Снять с админа", callback_data="admin_remove_admin")],
        [InlineKeyboardButton("📋 Список админов", callback_data="admin_list_admins")],
        [InlineKeyboardButton("🔙 Назад", callback_data="admin_back")]
    ])

def get_users_navigation_keyboard(page, total_pages):
    keyboard = []
    if page > 0:
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data=f"users_page_{page - 1}")])
    if page < total_pages - 1:
        keyboard.append([InlineKeyboardButton("Вперед ▶️", callback_data=f"users_page_{page + 1}")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_back")])
    return InlineKeyboardMarkup(keyboard)

def get_broadcast_confirm_keyboard(total_users):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Отправить", callback_data="broadcast_confirm"), 
         InlineKeyboardButton("❌ Отмена", callback_data="broadcast_cancel")],
        [InlineKeyboardButton(f"👥 Всего: {total_users} пользователей", callback_data="ignore")]
    ])

def get_ticket_keyboard(ticket_id, user_id):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ Ответить", callback_data=f"ticket_reply_{ticket_id}"),
         InlineKeyboardButton("✅ Закрыть", callback_data=f"ticket_close_{ticket_id}")],
        [InlineKeyboardButton("🚫 Заблокировать", callback_data=f"ticket_block_{user_id}_{ticket_id}")]
    ])

# ==================== ОБРАБОТЧИКИ ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    if chat.type in ["group", "supergroup"]:
        await update.message.reply_text("👋 Бот активен!\n\n📋 Сюда будут приходить объявления от пользователей.\n🔑 Используйте /admin в личке с ботом для управления.", reply_markup=ReplyKeyboardRemove())
        return
    get_user_stats(user.id)
    welcome = get_welcome_message().replace("{name}", user.first_name)
    await update.message.reply_text(welcome, reply_markup=get_main_keyboard())

async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        await update.message.reply_text("❌ Админ-панель доступна только в личных сообщениях с ботом!")
        return
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Нет прав")
        return
    await update.message.reply_text("🔑 **Админ-панель**", parse_mode=ParseMode.MARKDOWN, reply_markup=get_admin_keyboard())

async def admin_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.id != GROUP_CHAT_ID and chat.id != SUPPORT_CHAT_ID:
        await update.message.reply_text("❌ Эта команда доступна только в чате админов!")
        return
    await update.message.reply_text("📊 Собираю статистику...")
    all_time_stats, today_stats, week_stats, total_posts, today_posts, week_posts, total_authors, today_authors, week_authors = get_all_users_stats()
    text = "📊 **СТАТИСТИКА ПОСТОВ**\n\n🏆 **ВСЕГО (за всё время):**\n"
    for i, (uid, username, first_name, count) in enumerate(all_time_stats[:10], 1):
        name = escape_markdown(first_name or username or f"ID:{uid}")
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
        admin_mark = "👑 " if is_admin(uid) else ""
        text += f"{medal} {admin_mark}**{name}** — {count} 📤\n"
    text += f"\n📅 **ЗА СЕГОДНЯ:** {today_posts} постов\n📆 **ЗА НЕДЕЛЮ:** {week_posts} постов"
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

async def levels_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = "🏆 **ТАБЛИЦА УРОВНЕЙ** 🏆\n\n"
    for i, level in enumerate(LEVELS):
        if i == 0:
            text += f"{level['name']} — от {level['points']} очков\n"
        else:
            text += f"{level['name']} — {LEVELS[i-1]['points']+1}–{level['points']} очков\n"
    text += f"\n🏆 **ЛЕГЕНДА** — {LEVELS[-1]['points']}+ очков\n\n💡 +1 очко за отправку, +1 за публикацию"
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

async def get_chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"Chat ID: `{update.effective_chat.id}`", parse_mode=ParseMode.MARKDOWN)

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text("✅ Отменено", reply_markup=get_main_keyboard())

# ==================== АДМИН КОЛБЭКИ ====================
async def admin_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user = query.from_user
    if not is_admin(user.id):
        await query.edit_message_text("❌ Нет прав")
        return

    if data == "admin_stats":
        text = f"📊 **СТАТИСТИКА**\n\n👥 Всего: {get_total_users()}\n📊 Активных сегодня: {get_active_users_today()}\n🚫 Заблокировано: {get_blocked_users_count()}\n📤 Отправлено: {get_total_ads_sent()}\n📥 Опубликовано: {get_total_ads_published()}\n✅ За сегодня: {get_daily_stats()}\n🎫 Тикетов: {get_open_tickets_count()}\n📅 {date.today().strftime('%d.%m.%Y')}"
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_admin_keyboard())
    elif data == "admin_settings":
        await show_bot_settings(query)
    elif data == "admin_back":
        await query.edit_message_text("🔑 **Админ-панель**", parse_mode=ParseMode.MARKDOWN, reply_markup=get_admin_keyboard())
    elif data == "admin_exit":
        await query.edit_message_text("👋 Выход", reply_markup=None)
    elif data == "admin_toggle_bot":
        current = is_bot_enabled()
        set_bot_setting('bot_enabled', '0' if current else '1')
        await query.answer(f"Бот {'выключен' if current else 'включен'}")
        await show_bot_settings(query)
    elif data == "admin_toggle_cooldown":
        current = is_cooldown_enabled()
        set_cooldown_enabled(not current)
        await query.answer(f"КД на посты {'ВЫКЛЮЧЕНО' if current else 'ВКЛЮЧЕНО'}")
        await show_bot_settings(query)
    elif data == "admin_toggle_ticket_cooldown":
        current = is_ticket_cooldown_enabled()
        set_ticket_cooldown_enabled(not current)
        await query.answer(f"КД на тикеты {'ВЫКЛЮЧЕНО' if current else 'ВКЛЮЧЕНО'}")
        await show_bot_settings(query)
    elif data == "admin_edit_cooldown":
        context.user_data['edit_cooldown'] = True
        await query.edit_message_text(f"⏱️ Текущее КД на посты: {get_cooldown_minutes()} минут\n\nВведите новое значение (в минутах):")
    elif data == "admin_edit_ticket_cooldown":
        context.user_data['edit_ticket_cooldown'] = True
        await query.edit_message_text(f"🎫 Текущее КД на тикеты: {get_ticket_cooldown_minutes()} минут\n\nВведите новое значение (в минутах):")
    elif data == "admin_edit_welcome":
        context.user_data['edit_welcome'] = True
        await query.edit_message_text("✏️ Отправьте новое приветствие (используйте {name})")
    elif data == "admin_edit_maintenance":
        context.user_data['edit_maintenance'] = True
        await query.edit_message_text("✏️ Отправьте новое сообщение о тех.работах")
    elif data == "admin_edit_channel":
        context.user_data['edit_channel'] = True
        await query.edit_message_text(f"📢 Текущий канал: {get_channel()}\n\nВведите новый @username канала:")
    elif data == "admin_broadcast":
        context.user_data['broadcast_mode'] = True
        await query.edit_message_text(f"📨 Введите сообщение для рассылки\n\n👥 Будет отправлено: {get_total_users()} пользователям")
    elif data == "admin_spam":
        context.user_data['spam_mode'] = True
        await query.edit_message_text("💣 **СПАМ РАССЫЛКА**\n\nВведите: `ID количество текст`\nПример: `123456789 500 Привет!`")
    elif data == "admin_send_message":
        context.user_data['send_message_mode'] = True
        await query.edit_message_text("💬 Введите ID пользователя и сообщение через пробел\n\nПример: `123456789 Привет!`")
    elif data == "admin_users":
        await show_users_page(query, context, 0)
    elif data == "admin_search":
        context.user_data['search_mode'] = True
        await query.edit_message_text("🔍 Введите ID или @username:")
    elif data == "admin_top":
        top = get_top_users(10)
        text = "🏆 **ТОП-10**\n\n"
        for i, (username, first_name, rating, sent, published) in enumerate(top, 1):
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
            name = escape_markdown(first_name or "без имени")
            text += f"{medal} **{name}**\n⭐ {rating} очков | 📤{sent} 📥{published}\n\n"
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_admin_keyboard())
    elif data == "admin_tickets":
        await show_tickets_page(query, context)
    elif data == "admin_manage_admins":
        await query.edit_message_text("👑 **Управление админами**", parse_mode=ParseMode.MARKDOWN, reply_markup=get_manage_admins_keyboard())
    elif data == "admin_add_admin":
        context.user_data['add_admin_mode'] = True
        await query.edit_message_text("➕ Введите ID пользователя:")
    elif data == "admin_remove_admin":
        context.user_data['remove_admin_mode'] = True
        await query.edit_message_text("➖ Введите ID пользователя:")
    elif data == "admin_list_admins":
        text = "👑 **Список администраторов**\n\n"
        for uid in ADMIN_IDS:
            text += f"• `{uid}` (главный)\n"
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_manage_admins_keyboard())
    elif data.startswith("admin_block_"):
        user_id = int(data.split("_")[2])
        block_user(user_id)
        await query.answer("Заблокирован")
        await show_user_info(query, user_id)
    elif data.startswith("admin_unblock_"):
        user_id = int(data.split("_")[2])
        unblock_user(user_id)
        await query.answer("Разблокирован")
        await show_user_info(query, user_id)
    elif data.startswith("user_stats_"):
        user_id = int(data.split("_")[2])
        await show_user_info(query, user_id)
    elif data.startswith("send_msg_"):
        user_id = int(data.split("_")[2])
        context.user_data['send_message_target'] = user_id
        await query.edit_message_text(f"💬 Введите сообщение для пользователя ID `{user_id}`:")
    elif data.startswith("users_page_"):
        page = int(data.split("_")[2])
        await show_users_page(query, context, page)
    elif data == "broadcast_confirm":
        if 'broadcast_message' in context.user_data:
            await start_broadcast(update, context)
        else:
            await query.edit_message_text("❌ Сообщение не найдено")
    elif data == "broadcast_cancel":
        context.user_data.pop('broadcast_mode', None)
        context.user_data.pop('broadcast_message', None)
        await query.edit_message_text("❌ Отменено", reply_markup=get_admin_keyboard())
    elif data.startswith("ticket_reply_"):
        ticket_id = data.split("_")[2]
        if get_ticket_reply_sent(ticket_id):
            await query.edit_message_text("ℹ️ На этот тикет уже был отправлен ответ.")
            return
        context.user_data['reply_to_ticket'] = ticket_id
        await query.edit_message_text(f"✏️ **Ответ на тикет #{ticket_id}**\n\nНапишите ваш ответ.\n\nДля отмены /cancel")
    elif data.startswith("ticket_close_"):
        ticket_id = data.split("_")[2]
        close_ticket(ticket_id, user.id)
        
        target_user_id = get_ticket_user_id(ticket_id)
        if target_user_id:
            try:
                await context.bot.send_message(
                    chat_id=target_user_id,
                    text=f"✅ Ваш тикет #{ticket_id} был закрыт администратором.\nСпасибо за обращение!"
                )
            except:
                pass
        
        await query.edit_message_text(f"✅ Тикет #{ticket_id} закрыт", reply_markup=get_admin_keyboard())
    elif data.startswith("ticket_block_"):
        parts = data.split("_")
        if len(parts) >= 4:
            user_id = int(parts[2])
            ticket_id = parts[3]
            block_user(user_id)
            close_ticket(ticket_id, user.id)
            
            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text="🚫 Вы были заблокированы администратором за нарушение правил!"
                )
            except:
                pass
            
            await query.edit_message_text(f"🚫 Пользователь {user_id} заблокирован\n✅ Тикет #{ticket_id} закрыт", reply_markup=get_admin_keyboard())
        else:
            await query.edit_message_text("❌ Ошибка: неверный формат данных")

async def show_bot_settings(query):
    enabled = is_bot_enabled()
    cooldown_enabled = is_cooldown_enabled()
    cooldown_status = "🟢 Вкл" if cooldown_enabled else "🔴 Выкл"
    ticket_cooldown_enabled = is_ticket_cooldown_enabled()
    ticket_cooldown_status = "🟢 Вкл" if ticket_cooldown_enabled else "🔴 Выкл"
    text = f"⚙️ **НАСТРОЙКИ**\n\n🔹 Статус: {'🟢 Включен' if enabled else '🔴 Выключен'}\n📢 Канал: {get_channel()}\n⏱️ КД на посты: {get_cooldown_minutes()} мин [{cooldown_status}]\n🎫 КД на тикеты: {get_ticket_cooldown_minutes()} мин [{ticket_cooldown_status}]"
    await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_bot_settings_keyboard())

async def show_user_info(query, user_id):
    user_data = find_user_by_username_or_id(str(user_id))
    if not user_data:
        await query.edit_message_text("❌ Пользователь не найден")
        return
    uid, username, first_name, reg_date, sent, published, rating, is_blocked, is_admin_user, last_ad_time, last_ticket_time = user_data
    text = f"👤 **{escape_markdown(first_name or 'Без имени')}**\nID: `{uid}`\n@{escape_markdown(username or 'нет')}\n📤 {sent} | 📥 {published}\n⭐ {rating} очков\n{'❌ Заблокирован' if is_blocked else '✅ Активен'}"
    await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_user_action_keyboard(uid, is_blocked))

async def show_users_page(query, context, page):
    users_per_page = 5
    users = get_all_users(users_per_page, page * users_per_page)
    total = get_total_users()
    pages = (total + users_per_page - 1) // users_per_page
    text = f"👥 **Пользователи** (стр {page + 1}/{pages})\n\n"
    for u in users:
        uid, username, first_name, reg_date, sent, published, rating, is_blocked, is_admin_user = u
        status = "❌" if is_blocked else "✅"
        admin = "👑" if is_admin_user else ""
        name = escape_markdown(first_name or "без имени")
        text += f"{status}{admin} **{name}**\nID: `{uid}` | ⭐ {rating}\n\n"
    await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_users_navigation_keyboard(page, pages))

async def show_tickets_page(query, context):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT ticket_id, user_id, username, first_name, message, created_at FROM tickets WHERE status = 'open' ORDER BY created_at DESC")
    tickets = c.fetchall()
    conn.close()
    
    if not tickets:
        await query.edit_message_text("✅ Нет открытых тикетов", reply_markup=get_admin_keyboard())
        return
    
    text = "🎫 **ОТКРЫТЫЕ ТИКЕТЫ**\n\n"
    buttons = []
    
    for t in tickets:
        ticket_id, user_id, username, first_name, message, created_at = t
        short_msg = escape_markdown(message[:40] + "..." if len(message) > 40 else message)
        created = datetime.fromisoformat(created_at).strftime('%d.%m %H:%M')
        username_str = f"@{escape_markdown(username)}" if username else f"ID:{user_id}"
        
        text += f"**#{ticket_id}** от {username_str}\n📝 {short_msg}\n🕐 {created}\n\n"
        buttons.append([InlineKeyboardButton(f"📝 #{ticket_id} - {username_str[:20]}", callback_data=f"ticket_view_{ticket_id}")])
    
    buttons.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_back")])
    
    await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(buttons))

async def ticket_view_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user = query.from_user
    
    if not is_admin(user.id):
        await query.edit_message_text("❌ Нет прав")
        return
    
    ticket_id = data.split("_")[2]
    
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT ticket_id, user_id, username, first_name, message, created_at, status FROM tickets WHERE ticket_id = ?", (ticket_id,))
    ticket = c.fetchone()
    conn.close()
    
    if not ticket:
        await query.edit_message_text("❌ Тикет не найден", reply_markup=get_admin_keyboard())
        return
    
    ticket_id, user_id, username, first_name, message, created_at, status = ticket
    
    text = f"🎫 **ТИКЕТ #{ticket_id}**\n\n"
    text += f"👤 Пользователь: @{escape_markdown(username or 'нет')} (ID: `{user_id}`)\n"
    text += f"📝 Сообщение: {escape_markdown(message)}\n"
    text += f"🕐 Создан: {datetime.fromisoformat(created_at).strftime('%d.%m.%Y %H:%M')}\n"
    text += f"📊 Статус: {'🟢 Открыт' if status == 'open' else '🔴 Закрыт'}"
    
    keyboard = [
        [InlineKeyboardButton("✏️ Ответить", callback_data=f"ticket_reply_{ticket_id}"),
         InlineKeyboardButton("✅ Закрыть", callback_data=f"ticket_close_{ticket_id}")],
        [InlineKeyboardButton("🚫 Заблокировать", callback_data=f"ticket_block_{user_id}_{ticket_id}"),
         InlineKeyboardButton("🔙 Назад", callback_data="admin_tickets")]
    ]
    
    await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(keyboard))

async def start_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    msg = context.user_data['broadcast_message']
    users = get_all_users(limit=1000)
    total = len(users)
    await query.edit_message_text(f"📨 Начинаю рассылку {total} пользователям...")
    sent = 0
    failed = 0
    for u in users:
        try:
            if msg.text:
                await context.bot.send_message(chat_id=u[0], text=msg.text)
            sent += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            failed += 1
            logger.error(f"Ошибка отправки {u[0]}: {e}")
    result_text = f"📊 **ОТЧЕТ**\n\n👥 Всего: {total}\n✅ Отправлено: {sent}\n❌ Ошибок: {failed}"
    await context.bot.send_message(chat_id=update.effective_user.id, text=result_text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_admin_keyboard())
    context.user_data.pop('broadcast_mode', None)
    context.user_data.pop('broadcast_message', None)

# ==================== ОСНОВНОЙ ОБРАБОТЧИК ====================
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    text = update.message.text if update.message.text else ""

    if chat.type == "private":
        # Режимы редактирования
        if context.user_data.get('edit_cooldown'):
            try:
                new_cooldown = int(text)
                if new_cooldown >= 1:
                    set_cooldown_minutes(new_cooldown)
                    await update.message.reply_text(f"✅ КД на посты изменено на {new_cooldown} минут", reply_markup=get_main_keyboard())
                else:
                    await update.message.reply_text("❌ КД должно быть не менее 1 минуты")
            except:
                await update.message.reply_text("❌ Введите число")
            context.user_data.pop('edit_cooldown')
            return

        if context.user_data.get('edit_ticket_cooldown'):
            try:
                new_cooldown = int(text)
                if new_cooldown >= 1:
                    set_ticket_cooldown_minutes(new_cooldown)
                    await update.message.reply_text(f"✅ КД на тикеты изменено на {new_cooldown} минут", reply_markup=get_main_keyboard())
                else:
                    await update.message.reply_text("❌ КД должно быть не менее 1 минуты")
            except:
                await update.message.reply_text("❌ Введите число")
            context.user_data.pop('edit_ticket_cooldown')
            return

        if context.user_data.get('edit_welcome'):
            set_welcome_message(text)
            context.user_data.pop('edit_welcome')
            await update.message.reply_text("✅ Приветствие обновлено", reply_markup=get_main_keyboard())
            return

        if context.user_data.get('edit_maintenance'):
            set_bot_setting('maintenance_message', text)
            context.user_data.pop('edit_maintenance')
            await update.message.reply_text("✅ Сообщение обновлено", reply_markup=get_main_keyboard())
            return

        if context.user_data.get('edit_channel'):
            if not text.startswith('@'):
                text = '@' + text
            set_channel(text)
            context.user_data.pop('edit_channel')
            await update.message.reply_text(f"✅ Канал изменен на {text}", reply_markup=get_main_keyboard())
            return

        if context.user_data.get('add_admin_mode'):
            try:
                set_admin(int(text), True)
                await update.message.reply_text(f"✅ Админ добавлен", reply_markup=get_main_keyboard())
            except:
                await update.message.reply_text("❌ Ошибка! Введите ID пользователя")
            context.user_data.pop('add_admin_mode')
            return

        if context.user_data.get('remove_admin_mode'):
            try:
                uid = int(text)
                if uid in ADMIN_IDS:
                    await update.message.reply_text("❌ Нельзя удалить главного админа")
                else:
                    set_admin(uid, False)
                    await update.message.reply_text(f"✅ Админ удален", reply_markup=get_main_keyboard())
            except:
                await update.message.reply_text("❌ Ошибка! Введите ID пользователя")
            context.user_data.pop('remove_admin_mode')
            return

        if context.user_data.get('search_mode'):
            found = find_user_by_username_or_id(text)
            if found:
                uid, username, first_name, reg_date, sent, published, rating, is_blocked, is_admin_user, last_ad_time, last_ticket_time = found
                await update.message.reply_text(f"👤 **{escape_markdown(first_name or 'Без имени')}**\nID: `{uid}`\n⭐ {rating} очков\n{'❌ Заблокирован' if is_blocked else '✅ Активен'}", parse_mode=ParseMode.MARKDOWN, reply_markup=get_user_action_keyboard(uid, is_blocked))
            else:
                await update.message.reply_text("❌ Не найден")
            context.user_data.pop('search_mode')
            return

        if context.user_data.get('reply_to_ticket'):
            ticket_id = context.user_data['reply_to_ticket']
            target_user_id = get_ticket_user_id(ticket_id)
            if target_user_id and not get_ticket_reply_sent(ticket_id):
                update_ticket_reply(ticket_id, text)
                await context.bot.send_message(chat_id=target_user_id, text=f"✏️ **Ответ на тикет #{ticket_id}**\n\n{text}", parse_mode=ParseMode.MARKDOWN)
                await update.message.reply_text(f"✅ Ответ отправлен!", reply_markup=get_main_keyboard())
            context.user_data.pop('reply_to_ticket')
            return

        if context.user_data.get('send_message_target'):
            target_id = context.user_data['send_message_target']
            try:
                await context.bot.send_message(chat_id=target_id, text=f"📩 **Сообщение от администратора:**\n\n{text}", parse_mode=ParseMode.MARKDOWN)
                await update.message.reply_text(f"✅ Сообщение отправлено пользователю ID `{target_id}`", reply_markup=get_main_keyboard())
            except Exception as e:
                await update.message.reply_text(f"❌ Ошибка: {e}")
            context.user_data.pop('send_message_target')
            return

        if context.user_data.get('send_message_mode'):
            parts = text.split(' ', 1)
            if len(parts) >= 2:
                try:
                    target_id = int(parts[0])
                    msg_text = parts[1]
                    await context.bot.send_message(chat_id=target_id, text=f"📩 **Сообщение от администратора:**\n\n{msg_text}", parse_mode=ParseMode.MARKDOWN)
                    await update.message.reply_text(f"✅ Сообщение отправлено пользователю ID `{target_id}`", reply_markup=get_main_keyboard())
                except:
                    await update.message.reply_text("❌ Ошибка! Неверный ID")
            else:
                await update.message.reply_text("❌ Используйте: ID сообщение")
            context.user_data.pop('send_message_mode')
            return

        # ===== СПАМ РАССЫЛКА =====
        if context.user_data.get('spam_mode'):
            parts = text.split(' ', 2)
            if len(parts) >= 3:
                try:
                    target_id = int(parts[0])
                    count = int(parts[1])
                    spam_text = parts[2]

                    if count < 1 or count > 1000:
                        await update.message.reply_text("❌ Количество сообщений должно быть от 1 до 1000!")
                        return

                    await update.message.reply_text(f"💣 Начинаю спам-рассылку пользователю {target_id}\n📨 Количество: {count}\n⏳ Это может занять время...")

                    sent = 0
                    failed = 0

                    for i in range(count):
                        try:
                            await context.bot.send_message(
                                chat_id=target_id,
                                text=f"📨 **Сообщение {i+1} из {count}**\n\n{spam_text}",
                                parse_mode=ParseMode.MARKDOWN
                            )
                            sent += 1
                            if count > 100:
                                await asyncio.sleep(0.1)
                        except Exception as e:
                            failed += 1
                            logger.error(f"Ошибка отправки {i+1}: {e}")
                            if failed > 10:
                                await update.message.reply_text("❌ Слишком много ошибок, рассылка прервана!")
                                break

                    result_text = f"✅ **СПАМ-РАССЫЛКА ЗАВЕРШЕНА**\n\n👤 Пользователь: `{target_id}`\n📨 Отправлено: {sent}\n❌ Ошибок: {failed}"
                    await update.message.reply_text(result_text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_keyboard())

                except ValueError:
                    await update.message.reply_text("❌ Ошибка! ID и количество должны быть числами.")
                except Exception as e:
                    await update.message.reply_text(f"❌ Ошибка: {e}")
            else:
                await update.message.reply_text("❌ Используйте: ID количество текст\nПример: `123456789 500 Привет!`")

            context.user_data.pop('spam_mode')
            return

        if context.user_data.get('broadcast_mode'):
            context.user_data['broadcast_message'] = update.message
            await update.message.reply_text("📨 Подтвердите рассылку", reply_markup=get_broadcast_confirm_keyboard(get_total_users()))
            return

        if not is_bot_enabled() and not is_admin(user.id):
            await update.message.reply_text("🔧 Бот временно недоступен.")
            return

        # ===== ОБРАБОТКА КНОПОК =====
        if text == "📋 Отправить объявление":
            if is_user_blocked(user.id):
                await update.message.reply_text("❌ Вы заблокированы")
                return
            can_send, wait = can_send_ad(user.id)
            if not can_send:
                await update.message.reply_text(f"⏱️ Подождите {wait[0]} мин {wait[1]} сек")
                return
            await update.message.reply_text("📝 Отправьте объявление (текст + фото/видео)")
            context.user_data['awaiting_ad'] = True

        elif text == "👤 Мой профиль ⭐":
            stats = get_user_stats(user.id)
            if stats:
                username, first_name, reg_date, sent, published, rating, is_blocked, is_admin_user, last_ad_time, last_ticket_time = stats
                level, next_level, points_next, progress = get_level(rating)
                profile = f"👤 **{escape_markdown(first_name or 'Не указано')}**\n🆔 ID: `{user.id}`\n📤 Отправлено: {sent}\n📥 Опубликовано: {published}\n⭐ Рейтинг: {rating}\n📊 Уровень: {level['name']}\n📈 Прогресс: {progress}\n🔒 Статус: {'❌ Заблокирован' if is_blocked else '✅ Активен'}"
                await update.message.reply_text(profile, parse_mode=ParseMode.MARKDOWN)

        elif text == "📊 Статистика":
            stat = f"📊 **СТАТИСТИКА БОТА**\n\n👥 Пользователей: {get_total_users()}\n📊 Активных сегодня: {get_active_users_today()}\n🚫 Заблокировано: {get_blocked_users_count()}\n📤 Отправлено: {get_total_ads_sent()}\n📥 Опубликовано: {get_total_ads_published()}\n✅ За сегодня: {get_daily_stats()}\n📅 {date.today().strftime('%d.%m.%Y')}"
            await update.message.reply_text(stat, parse_mode=ParseMode.MARKDOWN)

        elif text == "❓ Помощь / Тикет":
            context.user_data['ticket_mode'] = True
            await update.message.reply_text("📝 Опишите вашу проблему одним сообщением:")

        elif context.user_data.get('ticket_mode'):
            if is_user_blocked(user.id):
                await update.message.reply_text("❌ Вы заблокированы и не можете создавать тикеты!")
                context.user_data.pop('ticket_mode')
                return
            
            can_send, wait_time = can_send_ticket(user.id)
            if not can_send:
                minutes, seconds = wait_time
                await update.message.reply_text(f"⏱️ Вы слишком часто создаете тикеты!\n\nСледующий тикет можно создать через **{minutes} мин {seconds} сек**")
                context.user_data.pop('ticket_mode')
                return
            
            ticket_id = create_ticket(user.id, user.username, user.first_name, text)
            if ticket_id:
                ticket_info = f"🎫 **НОВЫЙ ТИКЕТ** #{ticket_id}\n\n👤 @{escape_markdown(user.username or 'нет')} (ID: {user.id})\n📝 {escape_markdown(text)}\n🕐 {datetime.now().strftime('%d.%m.%Y %H:%M')}"
                await context.bot.send_message(SUPPORT_CHAT_ID, ticket_info, parse_mode=ParseMode.MARKDOWN, reply_markup=get_ticket_keyboard(ticket_id, user.id))
                await update.message.reply_text(f"✅ Тикет #{ticket_id} создан!\n\nАдминистратор ответит вам в ближайшее время.", reply_markup=get_main_keyboard())
            else:
                await update.message.reply_text("❌ Ошибка создания тикета")
            context.user_data.pop('ticket_mode')

        elif context.user_data.get('awaiting_ad'):
            try:
                await update.message.copy(chat_id=GROUP_CHAT_ID)
                info = f"📋 **НОВОЕ ОБЪЯВЛЕНИЕ**\n\n👤 @{escape_markdown(user.username or 'нет')}\n🆔 ID: `{user.id}`\n🕐 {datetime.now().strftime('%d.%m.%Y %H:%M')}"
                await context.bot.send_message(chat_id=GROUP_CHAT_ID, text=info, parse_mode=ParseMode.MARKDOWN, reply_markup=get_group_keyboard(user.id, False))
                update_user_ads(user.id, user.username, user.first_name)
                await update.message.reply_text("✅ Объявление отправлено на модерацию!", reply_markup=get_main_keyboard())
            except Exception as e:
                logger.error(f"Ошибка: {e}")
                await update.message.reply_text(f"❌ Ошибка: {e}")
            context.user_data.pop('awaiting_ad', None)

        # ===== ОБРАБОТКА НЕИЗВЕСТНЫХ КОМАНД =====
        else:
            if text.startswith('/'):
                await update.message.reply_text(
                    f"❌ **Неизвестная команда!**\n\n"
                    f"Команда `{escape_markdown(text)}` не распознана.\n\n"
                    f"📋 **Доступные команды:**\n"
                    f"• `/start` - запустить бота\n"
                    f"• `/admin` - админ-панель\n"
                    f"• `/stats` - статистика\n"
                    f"• `/levels` - таблица уровней\n"
                    f"• `/getid` - узнать ID чата\n"
                    f"• `/cancel` - отменить действие\n\n"
                    f"🔄 **Перезапустите бота командой /start**",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await update.message.reply_text(
                    f"❌ **Я вас не понял!**\n\n"
                    f"Пожалуйста, используйте кнопки меню для навигации.\n\n"
                    f"📋 **Доступные кнопки:**\n"
                    f"• 📋 Отправить объявление\n"
                    f"• 👤 Мой профиль ⭐\n"
                    f"• 📊 Статистика\n"
                    f"• ❓ Помощь / Тикет\n\n"
                    f"🔄 **Перезапустите бота командой /start**",
                    parse_mode=ParseMode.MARKDOWN
                )

    elif chat.type in ["group", "supergroup"]:
        if chat.id == GROUP_CHAT_ID or chat.id == SUPPORT_CHAT_ID:
            if context.user_data.get('reply_to_user'):
                target = context.user_data['reply_to_user']
                try:
                    await context.bot.send_message(chat_id=target, text=f"📩 **Сообщение от администратора:**\n\n{text}", parse_mode=ParseMode.MARKDOWN)
                    await update.message.reply_text(f"✅ Сообщение отправлено пользователю!")
                except Exception as e:
                    await update.message.reply_text(f"❌ Ошибка: {e}")
                context.user_data.pop('reply_to_user')

            if context.user_data.get('reply_to_ticket'):
                ticket_id = context.user_data['reply_to_ticket']
                target = get_ticket_user_id(ticket_id)
                if target:
                    update_ticket_reply(ticket_id, text)
                    try:
                        await context.bot.send_message(chat_id=target, text=f"✏️ **Ответ на ваш тикет #{ticket_id}**\n\n{text}\n\n— Администратор", parse_mode=ParseMode.MARKDOWN)
                        await update.message.reply_text(f"✅ Ответ на тикет #{ticket_id} отправлен!")
                    except Exception as e:
                        await update.message.reply_text(f"❌ Ошибка: {e}")
                context.user_data.pop('reply_to_ticket')
        else:
            await update.message.reply_text(
                "❌ **Этот бот работает только в определенной группе!**\n\n"
                f"🔑 Используйте /start в личных сообщениях с ботом.\n\n"
                f"🔄 **Перезапустите бота командой /start**",
                parse_mode=ParseMode.MARKDOWN
            )

# ==================== ГРУППОВОЙ ОБРАБОТЧИК ====================
async def group_action_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    channel = get_channel()
    admin = query.from_user

    if not channel:
        channel = "@канал_не_указан"

    channel_link = channel.replace('@', 'https://t.me/') if channel.startswith('@') else f"https://t.me/{channel}"

    parts = data.split("_")

    if data.startswith("publish_"):
        user_id = int(parts[1])
        author_data = get_user_stats(user_id)
        author_username = author_data[0] if author_data else None
        author_first_name = author_data[1] if author_data else None
        try:
            original_id = query.message.message_id - 1
            await context.bot.copy_message(chat_id=channel, from_chat_id=GROUP_CHAT_ID, message_id=original_id)
            increment_published(user_id)
            await query.edit_message_text(
                text=f"📋 **ОБЪЯВЛЕНИЕ ОПУБЛИКОВАНО**\n\n👤 @{escape_markdown(author_username or 'нет')}\n👑 Админ: @{escape_markdown(admin.username or admin.first_name)}\n📢 Канал: [{channel}]({channel_link})",
                parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True, reply_markup=get_group_keyboard(user_id, is_published=True)
            )
        except Exception as e:
            await query.edit_message_text(f"❌ Ошибка: {e}")

    elif data.startswith("republish_"):
        user_id = int(parts[1])
        author_data = get_user_stats(user_id)
        author_username = author_data[0] if author_data else None
        author_first_name = author_data[1] if author_data else None
        try:
            original_id = query.message.message_id - 1
            await context.bot.copy_message(chat_id=channel, from_chat_id=GROUP_CHAT_ID, message_id=original_id)
            increment_published(user_id)
            await query.answer("✅ Пост опубликован повторно!")
            await query.edit_message_text(
                text=f"📋 **ОБЪЯВЛЕНИЕ ОПУБЛИКОВАНО (повторно)**\n\n👤 @{escape_markdown(author_username or 'нет')}\n👑 Админ: @{escape_markdown(admin.username or admin.first_name)}\n📢 Канал: [{channel}]({channel_link})",
                parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True, reply_markup=get_group_keyboard(user_id, is_published=True)
            )
        except Exception as e:
            await query.answer("❌ Ошибка")

    elif data.startswith("delete_"):
        user_id = int(parts[1])
        author_data = get_user_stats(user_id)
        author_username = author_data[0] if author_data else None
        await query.edit_message_text(
            text=f"❌ **УДАЛЕНО**\n\n👤 Автор: @{escape_markdown(author_username or 'нет')}\n👑 Админ: @{escape_markdown(admin.username or admin.first_name)}",
            parse_mode=ParseMode.MARKDOWN
        )

    elif data.startswith("block_"):
        user_id = int(parts[1])
        block_user(user_id)
        await query.edit_message_text(
            text=f"🚫 **ЗАБЛОКИРОВАН**\n\n👑 Админ: @{escape_markdown(admin.username or admin.first_name)}",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_group_keyboard(user_id, is_published=False)
        )
        
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text="🚫 Вы были заблокированы администратором!\n\n❌ Вы не можете отправлять объявления и создавать тикеты.\n\nДля разблокировки обратитесь к администратору."
            )
        except:
            pass

    elif data.startswith("unblock_"):
        user_id = int(parts[1])
        unblock_user(user_id)
        await query.edit_message_text(
            text=f"✅ **РАЗБЛОКИРОВАН**\n\n👑 Админ: @{escape_markdown(admin.username or admin.first_name)}",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_group_keyboard(user_id, is_published=False)
        )
        
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text="✅ Вы были разблокированы администратором!\n\n✨ Вы снова можете отправлять объявления и создавать тикеты."
            )
        except:
            pass

    elif data.startswith("reply_to_user_"):
        user_id = int(parts[3])
        context.user_data['reply_to_user'] = user_id
        await query.edit_message_text(
            text=f"💬 **Ответ пользователю**\n\n👤 Пользователь: ID `{user_id}`\n\nНапишите ваше сообщение. Для отмены /cancel",
            parse_mode=ParseMode.MARKDOWN
        )

    elif data == "cancel_reply":
        context.user_data.pop('reply_to_user', None)
        await query.edit_message_text("❌ Отменено", reply_markup=None)

# ==================== ЗАПУСК ====================
async def run_bot():
    logger.info("=" * 50)
    logger.info("ЗАПУСК БОТА")
    logger.info("=" * 50)

    app = Application.builder().token(BOT_TOKEN).connect_timeout(60).read_timeout(60).write_timeout(60).pool_timeout(60).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin", admin_command))
    app.add_handler(CommandHandler("stats", admin_stats_command))
    app.add_handler(CommandHandler("levels", levels_command))
    app.add_handler(CommandHandler("getid", get_chat_id))
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(CallbackQueryHandler(admin_callback_handler, pattern="^(admin_|users_page_|user_stats_|broadcast_|ticket_|send_msg_)"))
    app.add_handler(CallbackQueryHandler(group_action_handler, pattern="^(publish_|delete_|block_|republish_|unblock_|reply_to_user_|cancel_reply)"))
    app.add_handler(CallbackQueryHandler(ticket_view_handler, pattern="^ticket_view_"))
    app.add_handler(MessageHandler(filters.ALL, message_handler))

    await app.initialize()
    await app.start()
    
    try:
        await app.bot.delete_webhook(drop_pending_updates=True)
        logger.info("✅ Webhook удален")
    except Exception as e:
        logger.warning(f"⚠️ Не удалось удалить webhook: {e}")
    
    await app.updater.start_polling(drop_pending_updates=True, timeout=60)

    logger.info("✅ Бот успешно запущен!")

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Останавливаем...")
    finally:
        await app.updater.stop()
        await app.stop()
        await app.shutdown()

def main():
    try:
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        logger.info("Бот остановлен")

if __name__ == "__main__":
    main()
