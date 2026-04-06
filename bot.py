import logging
import asyncio
import os
import sqlite3
import re
from datetime import datetime, date, timedelta
import uuid
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes

# Настройка логирования
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)

# ==================== КОНФИГУРАЦИЯ ====================
BOT_TOKEN = os.environ.get('BOT_TOKEN')
ADMIN_IDS = [int(id) for id in os.environ.get('ADMIN_IDS', '').split(',') if id]
GROUP_CHAT_ID = int(os.environ.get('GROUP_CHAT_ID', '0'))
SUPPORT_CHAT_ID = int(os.environ.get('SUPPORT_CHAT_ID', '0'))
CHANNEL_ID = os.environ.get('CHANNEL_ID')


DB_FILE = 'bot_data.db'

# Уровни и очки
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


# ==================== БАЗА ДАННЫХ SQLITE ====================
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            registration_date TEXT,
            ads_sent INTEGER DEFAULT 0,
            ads_published INTEGER DEFAULT 0,
            rating INTEGER DEFAULT 0,
            is_blocked INTEGER DEFAULT 0,
            is_admin INTEGER DEFAULT 0,
            last_ad_time TEXT
        )
    ''')

    try:
        c.execute("ALTER TABLE users ADD COLUMN last_ad_time TEXT")
    except:
        pass

    c.execute('''
        CREATE TABLE IF NOT EXISTS daily_stats (
            user_id INTEGER,
            date TEXT,
            ads_count INTEGER DEFAULT 0,
            PRIMARY KEY (user_id, date)
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS stats (
            date TEXT PRIMARY KEY,
            published_count INTEGER DEFAULT 0
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS bot_settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS tickets (
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
        )
    ''')

    try:
        c.execute("ALTER TABLE tickets ADD COLUMN admin_reply TEXT")
    except:
        pass
    
    try:
        c.execute("ALTER TABLE tickets ADD COLUMN admin_reply_sent INTEGER DEFAULT 0")
    except:
        pass

    c.execute("INSERT OR IGNORE INTO bot_settings (key, value) VALUES ('bot_enabled', '1')")
    c.execute("INSERT OR IGNORE INTO bot_settings (key, value) VALUES ('maintenance_message', '🔧 Бот временно недоступен. Ведутся технические работы.')")
    c.execute("INSERT OR IGNORE INTO bot_settings (key, value) VALUES ('channel_id', ?)", (CHANNEL_ID,))
    c.execute("INSERT OR IGNORE INTO bot_settings (key, value) VALUES ('cooldown_minutes', '10')")
    c.execute("INSERT OR IGNORE INTO bot_settings (key, value) VALUES ('cooldown_enabled', '1')")
    c.execute("INSERT OR IGNORE INTO bot_settings (key, value) VALUES ('ai_pepel_enabled', '0')")  # Новая настройка

    default_welcome = """✋ Здравствуйте, {name}!

Это бот канала Купи/Продай Rostov

🌴 Сюда ты можешь кидать свои объявления!

💥 Обязательно указывайте свой username!"""

    c.execute("INSERT OR IGNORE INTO bot_settings (key, value) VALUES ('welcome_message', ?)", (default_welcome,))

    conn.commit()
    conn.close()
    logger.info("✅ База данных SQLite инициализирована")


init_db()


# ==================== ФУНКЦИИ РАБОТЫ С БД ====================
def get_user_stats(user_id: int):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        reg_date = datetime.now().isoformat()

        c.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
        if not c.fetchone():
            c.execute("""
                INSERT INTO users (user_id, username, first_name, registration_date, ads_sent, ads_published, rating, is_blocked, is_admin, last_ad_time) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (user_id, None, None, reg_date, 0, 0, 0, 0, 1 if user_id in ADMIN_IDS else 0, None))
            conn.commit()

        c.execute("""
            SELECT username, first_name, registration_date, ads_sent, ads_published, rating, is_blocked, is_admin, last_ad_time 
            FROM users WHERE user_id = ?
        """, (user_id,))
        result = c.fetchone()
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Ошибка get_user_stats: {e}")
        return None


def update_user_ads(user_id: int, username: str, first_name: str):
    try:
        today = date.today().isoformat()
        now = datetime.now().isoformat()
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()

        c.execute("UPDATE users SET username = ?, first_name = ?, last_ad_time = ? WHERE user_id = ?",
                  (username, first_name, now, user_id))

        c.execute("""
            INSERT INTO daily_stats (user_id, date, ads_count) VALUES (?, ?, 1) 
            ON CONFLICT(user_id, date) DO UPDATE SET ads_count = ads_count + 1
        """, (user_id, today))

        c.execute("UPDATE users SET ads_sent = ads_sent + 1 WHERE user_id = ?", (user_id,))
        c.execute("UPDATE users SET rating = ads_sent + ads_published WHERE user_id = ?", (user_id,))

        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Ошибка update_user_ads: {e}")


def increment_published(user_id: int):
    try:
        today = date.today().isoformat()
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("UPDATE users SET ads_published = ads_published + 1 WHERE user_id = ?", (user_id,))
        c.execute("UPDATE users SET rating = ads_sent + ads_published WHERE user_id = ?", (user_id,))
        c.execute("""
            INSERT INTO stats (date, published_count) VALUES (?, 1) 
            ON CONFLICT(date) DO UPDATE SET published_count = published_count + 1
        """, (today,))
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
        return result[0] if result and result[0] is not None else 0
    except Exception as e:
        logger.error(f"Ошибка get_daily_stats: {e}")
        return 0


def get_total_ads_sent():
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT COALESCE(SUM(ads_sent), 0) FROM users")
        result = c.fetchone()[0]
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Ошибка get_total_ads_sent: {e}")
        return 0


def get_total_ads_published():
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT COALESCE(SUM(ads_published), 0) FROM users")
        result = c.fetchone()[0]
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Ошибка get_total_ads_published: {e}")
        return 0


def block_user(user_id: int):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("UPDATE users SET is_blocked = 1 WHERE user_id = ?", (user_id,))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Ошибка block_user: {e}")


def unblock_user(user_id: int):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("UPDATE users SET is_blocked = 0 WHERE user_id = ?", (user_id,))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Ошибка unblock_user: {e}")


def is_user_blocked(user_id: int):
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
        result = c.fetchone()[0]
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Ошибка get_total_users: {e}")
        return 0


def get_active_users_today():
    try:
        today = date.today().isoformat()
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT COUNT(DISTINCT user_id) FROM daily_stats WHERE date = ?", (today,))
        result = c.fetchone()[0]
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Ошибка get_active_users_today: {e}")
        return 0


def get_blocked_users_count():
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM users WHERE is_blocked = 1")
        result = c.fetchone()[0]
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Ошибка get_blocked_users_count: {e}")
        return 0


def get_all_users(limit=100, offset=0):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("""
            SELECT user_id, username, first_name, registration_date, ads_sent, ads_published, rating, is_blocked, is_admin 
            FROM users ORDER BY rating DESC LIMIT ? OFFSET ?
        """, (limit, offset))
        users = c.fetchall()
        conn.close()
        return users
    except Exception as e:
        logger.error(f"Ошибка get_all_users: {e}")
        return []


def get_top_users(limit=10):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("""
            SELECT username, first_name, rating, ads_sent, ads_published 
            FROM users WHERE is_blocked = 0 ORDER BY rating DESC LIMIT ?
        """, (limit,))
        top = c.fetchall()
        conn.close()
        return top
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


def is_ai_pepel_enabled():
    val = get_bot_setting('ai_pepel_enabled')
    return val == '1'


def set_ai_pepel_enabled(enabled):
    set_bot_setting('ai_pepel_enabled', '1' if enabled else '0')


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
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT is_admin FROM users WHERE user_id = ?", (user_id,))
    result = c.fetchone()
    conn.close()
    if result and result[0] == 1:
        return True
    return user_id in ADMIN_IDS


def set_admin(user_id, is_admin_val):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("UPDATE users SET is_admin = ? WHERE user_id = ?", (1 if is_admin_val else 0, user_id))
    conn.commit()
    conn.close()


def update_ticket_reply(ticket_id, reply_text):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("UPDATE tickets SET admin_reply = ?, admin_reply_sent = 1 WHERE ticket_id = ?", (reply_text, ticket_id))
        conn.commit()
        conn.close()
        logger.info(f"✅ Ответ на тикет {ticket_id} сохранен в БД")
        return True
    except Exception as e:
        logger.error(f"Ошибка update_ticket_reply: {e}")
        return False


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


def get_ai_pepel_stats():
    """Получает статистику ИИ PEPEL для отображения в /stats"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    # Считаем сколько объявлений опубликовано через ИИ PEPEL сегодня
    today = date.today().isoformat()
    c.execute("SELECT COUNT(*) FROM stats WHERE date = ?", (today,))
    today_posts = c.fetchone()[0] or 0
    
    # Считаем общее количество опубликованных объявлений
    c.execute("SELECT COALESCE(SUM(ads_published), 0) FROM users")
    total_posts = c.fetchone()[0] or 0
    
    conn.close()
    
    return {
        'enabled': is_ai_pepel_enabled(),
        'today_posts': today_posts,
        'total_posts': total_posts
    }


# ==================== ИИ PEPEL - ПРОВЕРКА ОБЪЯВЛЕНИЙ ====================
def check_username_in_text(text: str, expected_username: str) -> tuple[bool, str]:
    """
    Проверяет:
    1) Есть ли @username в тексте
    2) Совпадает ли он с юзернеймом отправителя
    """
    if not expected_username:
        return False, "❌ У вас не установлен username в Telegram!"
    
    found_usernames = re.findall(r'@([a-zA-Z0-9_]+)', text)
    
    if not found_usernames:
        return False, "❌ В объявлении не указан ваш @username!"
    
    if found_usernames[0].lower() != expected_username.lower():
        return False, f"❌ Username в объявлении (@{found_usernames[0]}) не совпадает с вашим (@{expected_username})"
    
    return True, "✅ Username найден и совпадает"


def check_keywords_or_numbers(text: str) -> tuple[bool, str]:
    """
    Проверяет наличие ключевых слов или любой цифры
    Ключевые слова: продам, селл, сел, sel, sell, куплю, продаже, продаю, вылетает, торг, цена
    """
    keywords = ['продам', 'селл', 'сел', 'sel', 'sell', 'куплю', 'продаже', 'продаю', 'вылетает', 'торг', 'цена']
    text_lower = text.lower()
    
    # Проверка ключевых слов
    for keyword in keywords:
        if keyword in text_lower:
            return True, f"✅ Найдено ключевое слово: {keyword}"
    
    # Проверка наличия любой цифры
    if re.search(r'\d', text):
        return True, "✅ Найдена цифра в тексте"
    
    return False, "❌ Нет ключевых слов (продам, куплю, цена и т.д.) и нет цифр"


def check_text_length(text: str) -> tuple[bool, str]:
    """Проверяет длину текста от 15 до 2000 символов"""
    length = len(text)
    if length < 15:
        return False, f"❌ Текст слишком короткий ({length} символов). Минимум 15 символов"
    if length > 2000:
        return False, f"❌ Текст слишком длинный ({length} символов). Максимум 2000 символов"
    return True, f"✅ Длина текста: {length} символов"


def validate_ad_for_ai_pepel(text: str, username: str) -> tuple[bool, str, dict]:
    """
    Полная проверка объявления для ИИ PEPEL
    Возвращает (прошла_проверку, сообщение, детали)
    """
    details = {}
    errors = []
    
    # 1. Проверка длины
    length_ok, length_msg = check_text_length(text)
    if not length_ok:
        return False, length_msg, {}
    details['length'] = len(text)
    
    # 2. Проверка username в тексте и совпадение
    username_ok, username_msg = check_username_in_text(text, username)
    if not username_ok:
        return False, username_msg, {}
    details['username_match'] = True
    
    # 3. Проверка ключевых слов или цифр
    keywords_ok, keywords_msg = check_keywords_or_numbers(text)
    if not keywords_ok:
        return False, keywords_msg, {}
    details['has_keyword_or_number'] = True
    
    return True, "✅ Все критерии пройдены!", details


# ==================== ТИКЕТЫ ====================
def create_ticket(user_id, username, first_name, message):
    try:
        if is_user_blocked(user_id):
            return None
        
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        ticket_id = str(uuid.uuid4())[:8]
        created_at = datetime.now().isoformat()
        c.execute("""
            INSERT INTO tickets (ticket_id, user_id, username, first_name, message, status, created_at, admin_reply_sent) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (ticket_id, user_id, username, first_name, message, 'open', created_at, 0))
        conn.commit()
        conn.close()
        logger.info(f"✅ Создан тикет {ticket_id} от пользователя {user_id}")
        return ticket_id
    except Exception as e:
        logger.error(f"Ошибка create_ticket: {e}")
        return None


def get_ticket(ticket_id):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT * FROM tickets WHERE ticket_id = ?", (ticket_id,))
        ticket = c.fetchone()
        conn.close()
        return ticket
    except Exception as e:
        logger.error(f"Ошибка get_ticket: {e}")
        return None


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


def get_open_tickets_count():
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM tickets WHERE status = 'open'")
        result = c.fetchone()[0]
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Ошибка get_open_tickets_count: {e}")
        return 0


# ==================== СТАТИСТИКА ВСЕХ ПОЛЬЗОВАТЕЛЕЙ ====================
def get_all_users_stats():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    c.execute("""
        SELECT user_id, username, first_name, ads_published 
        FROM users 
        WHERE ads_published > 0
        ORDER BY ads_published DESC
        LIMIT 15
    """)
    all_time_stats = c.fetchall()
    
    today = date.today().isoformat()
    c.execute("""
        SELECT u.user_id, u.username, u.first_name, COALESCE(SUM(da.ads_count), 0) 
        FROM users u
        LEFT JOIN daily_stats da ON u.user_id = da.user_id AND da.date = ?
        WHERE u.ads_published > 0
        GROUP BY u.user_id
        HAVING COALESCE(SUM(da.ads_count), 0) > 0
        ORDER BY COALESCE(SUM(da.ads_count), 0) DESC
        LIMIT 15
    """, (today,))
    today_stats = c.fetchall()
    
    week_ago = (date.today() - timedelta(days=7)).isoformat()
    c.execute("""
        SELECT u.user_id, u.username, u.first_name, COALESCE(SUM(da.ads_count), 0) 
        FROM users u
        LEFT JOIN daily_stats da ON u.user_id = da.user_id AND da.date >= ?
        WHERE u.ads_published > 0
        GROUP BY u.user_id
        HAVING COALESCE(SUM(da.ads_count), 0) > 0
        ORDER BY COALESCE(SUM(da.ads_count), 0) DESC
        LIMIT 15
    """, (week_ago,))
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


# ==================== КОМАНДА /levels ====================
async def levels_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = "🏆 **ТАБЛИЦА УРОВНЕЙ** 🏆\n\n"
    
    for i, level in enumerate(LEVELS):
        if i == 0:
            text += f"{level['name']} — от {level['points']} очков\n"
        else:
            text += f"{level['name']} — {LEVELS[i-1]['points']+1}–{level['points']} очков\n"
    
    text += f"\n🏆 **ЛЕГЕНДА** — {LEVELS[-1]['points']}+ очков\n\n"
    text += "💡 **Как получить очки:**\n"
    text += "• +1 очко за отправку объявления\n"
    text += "• +1 очко за публикацию объявления в канале"
    
    await update.message.reply_text(text, parse_mode='Markdown')


# ==================== КЛАВИАТУРЫ ====================
def get_main_keyboard():
    keyboard = [
        [KeyboardButton("📋 Отправить объявление"), KeyboardButton("👤 Мой профиль ⭐")],
        [KeyboardButton("📊 Статистика"), KeyboardButton("❓ Помощь / Тикет")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def get_group_keyboard(user_id: int, is_published: bool = False):
    if is_published:
        keyboard = [
            [
                InlineKeyboardButton("🔄 Выложить еще раз", callback_data=f"republish_{user_id}")
            ]
        ]
    else:
        keyboard = [
            [
                InlineKeyboardButton("✅ Выложить в канал", callback_data=f"publish_{user_id}"),
                InlineKeyboardButton("💬 Ответить", callback_data=f"reply_to_user_{user_id}")
            ],
            [
                InlineKeyboardButton("❌ Удалить", callback_data=f"delete_{user_id}"),
                InlineKeyboardButton("🚫 Заблокировать", callback_data=f"block_{user_id}")
            ]
        ]
    return InlineKeyboardMarkup(keyboard)


def get_admin_keyboard():
    open_tickets = get_open_tickets_count()
    tickets_button = f"🎫 Тикеты ({open_tickets})" if open_tickets > 0 else "🎫 Тикеты"
    ai_pepel_status = "🟢 Вкл" if is_ai_pepel_enabled() else "🔴 Выкл"
    keyboard = [
        [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton("👥 Список пользователей", callback_data="admin_users")],
        [InlineKeyboardButton("🔍 Поиск пользователя", callback_data="admin_search")],
        [InlineKeyboardButton("🏆 Топ", callback_data="admin_top")],
        [InlineKeyboardButton(tickets_button, callback_data="admin_tickets")],
        [InlineKeyboardButton("📨 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton("💬 Отправить сообщение", callback_data="admin_send_message")],
        [InlineKeyboardButton(f"🤖 ИИ PEPEL [{ai_pepel_status}]", callback_data="admin_toggle_ai_pepel")],
        [InlineKeyboardButton("⚙️ Настройки", callback_data="admin_settings")],
        [InlineKeyboardButton("🔙 Выход", callback_data="admin_exit")]
    ]
    return InlineKeyboardMarkup(keyboard)


def get_user_action_keyboard(user_id: int, is_blocked: bool):
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
    ai_pepel_status = "🟢 Вкл" if is_ai_pepel_enabled() else "🔴 Выкл"

    keyboard = [
        [InlineKeyboardButton(f"Статус: {status_text}", callback_data="admin_toggle_bot")],
        [InlineKeyboardButton("📢 Изменить канал", callback_data="admin_edit_channel")],
        [InlineKeyboardButton(f"⏱️ КД на посты: {cooldown} мин [{cooldown_status}]", callback_data="admin_edit_cooldown")],
        [InlineKeyboardButton("⏱️ Вкл/Выкл КД", callback_data="admin_toggle_cooldown")],
        [InlineKeyboardButton(f"🤖 ИИ PEPEL [{ai_pepel_status}]", callback_data="admin_toggle_ai_pepel")],
        [InlineKeyboardButton("✏️ Изменить приветствие", callback_data="admin_edit_welcome")],
        [InlineKeyboardButton("✏️ Сообщение о тех.работах", callback_data="admin_edit_maintenance")],
        [InlineKeyboardButton("👑 Управление админами", callback_data="admin_manage_admins")],
        [InlineKeyboardButton("🔙 Назад", callback_data="admin_back")]
    ]
    return InlineKeyboardMarkup(keyboard)


def get_manage_admins_keyboard():
    keyboard = [
        [InlineKeyboardButton("➕ Назначить админа", callback_data="admin_add_admin")],
        [InlineKeyboardButton("➖ Снять с админа", callback_data="admin_remove_admin")],
        [InlineKeyboardButton("📋 Список админов", callback_data="admin_list_admins")],
        [InlineKeyboardButton("🔙 Назад", callback_data="admin_back")]
    ]
    return InlineKeyboardMarkup(keyboard)


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


def get_ticket_keyboard(ticket_id: str, user_id: int):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ Ответить", callback_data=f"ticket_reply_{ticket_id}"),
         InlineKeyboardButton("✅ Закрыть", callback_data=f"ticket_close_{ticket_id}")],
        [InlineKeyboardButton("🚫 Заблокировать", callback_data=f"ticket_block_{user_id}_{ticket_id}")]
    ])


# ==================== ПОИСК ПОЛЬЗОВАТЕЛЯ ====================
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
        logger.error(f"Ошибка поиска: {e}")
        return None


# ==================== ОБРАБОТЧИКИ ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat

    if chat.type in ["group", "supergroup"]:
        await update.message.reply_text(
            f"👋 Бот активен!\n\n"
            f"📋 Сюда будут приходить объявления от пользователей.\n"
            f"🔑 Используйте /admin в личке с ботом для управления.\n"
            f"📊 Используйте /stats для просмотра статистики.",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    get_user_stats(user.id)
    welcome = get_welcome_message().replace("{name}", user.first_name)
    await update.message.reply_text(welcome, reply_markup=get_main_keyboard())


# ==================== КОМАНДА /stats В ЧАТЕ АДМИНОВ ====================
async def admin_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    
    if chat.id != GROUP_CHAT_ID and chat.id != SUPPORT_CHAT_ID:
        await update.message.reply_text("❌ Эта команда доступна только в чате админов!")
        return
    
    await update.message.reply_text("📊 Собираю статистику...")
    
    all_time_stats, today_stats, week_stats, total_posts, today_posts, week_posts, total_authors, today_authors, week_authors = get_all_users_stats()
    ai_pepel_stats = get_ai_pepel_stats()
    
    text = "📊 **СТАТИСТИКА ПОСТОВ**\n\n"
    
    text += "🏆 **ВСЕГО (за всё время):**\n"
    if all_time_stats:
        for i, (uid, username, first_name, count) in enumerate(all_time_stats[:10], 1):
            name = first_name or username or f"ID:{uid}"
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
            admin_mark = "👑 " if is_admin(uid) else ""
            text += f"{medal} {admin_mark}**{name}** — {count} 📤\n"
    else:
        text += "Нет данных\n"
    
    # Добавляем ИИ PEPEL в топ
    if ai_pepel_stats['total_posts'] > 0:
        text += f"🤖 **ИИ PEPEL** — {ai_pepel_stats['total_posts']} 📤\n"
    
    text += "\n📅 **ЗА СЕГОДНЯ:**\n"
    if today_stats:
        for i, (uid, username, first_name, count) in enumerate(today_stats[:10], 1):
            name = first_name or username or f"ID:{uid}"
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
            admin_mark = "👑 " if is_admin(uid) else ""
            text += f"{medal} {admin_mark}**{name}** — {count} 📤\n"
    else:
        text += "Нет данных\n"
    
    # Добавляем ИИ PEPEL в топ за сегодня
    if ai_pepel_stats['today_posts'] > 0:
        text += f"🤖 **ИИ PEPEL** — {ai_pepel_stats['today_posts']} 📤\n"
    
    text += "\n📆 **ЗА НЕДЕЛЮ:**\n"
    if week_stats:
        for i, (uid, username, first_name, count) in enumerate(week_stats[:10], 1):
            name = first_name or username or f"ID:{uid}"
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
            admin_mark = "👑 " if is_admin(uid) else ""
            text += f"{medal} {admin_mark}**{name}** — {count} 📤\n"
    else:
        text += "Нет данных\n"
    
    text += "\n━━━━━━━━━━━━━━━━━━━━\n"
    text += f"📊 **ВСЕГО ПОСТОВ:** {total_posts}\n"
    text += f"👥 **Авторов:** {total_authors}\n\n"
    text += f"📅 **ЗА СЕГОДНЯ:** {today_posts} постов\n"
    text += f"👥 **Авторов сегодня:** {today_authors}\n\n"
    text += f"📆 **ЗА НЕДЕЛЮ:** {week_posts} постов\n"
    text += f"👥 **Авторов за неделю:** {week_authors}\n\n"
    text += f"━━━━━━━━━━━━━━━━━━━━\n"
    text += f"🤖 **ИИ PEPEL:** {'🟢 Включен' if ai_pepel_stats['enabled'] else '🔴 Выключен'}\n"
    
    await update.message.reply_text(text, parse_mode='Markdown')


async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        await update.message.reply_text("❌ Админ-панель доступна только в личных сообщениях с ботом!")
        return

    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Нет прав")
        return

    await update.message.reply_text("🔑 **Админ-панель**", parse_mode='Markdown', reply_markup=get_admin_keyboard())


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    for mode in ['broadcast_mode', 'edit_welcome', 'edit_maintenance', 'awaiting_ad', 'ticket_mode', 'search_mode',
                 'edit_channel', 'add_admin_mode', 'remove_admin_mode', 'edit_cooldown', 'reply_to_ticket',
                 'send_message_mode', 'reply_to_user']:
        context.user_data.pop(mode, None)
    await update.message.reply_text("✅ Отменено", reply_markup=get_main_keyboard())


async def get_chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"Chat ID: `{update.effective_chat.id}`", parse_mode='Markdown')


# ==================== АДМИН-ПАНЕЛЬ ====================
async def admin_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user = query.from_user

    if not is_admin(user.id):
        await query.edit_message_text("❌ Нет прав")
        return

    if data == "admin_stats":
        text = (f"📊 **СТАТИСТИКА**\n\n"
                f"👥 Всего: {get_total_users()}\n"
                f"📊 Активных сегодня: {get_active_users_today()}\n"
                f"🚫 Заблокировано: {get_blocked_users_count()}\n"
                f"📤 Отправлено: {get_total_ads_sent()}\n"
                f"📥 Опубликовано: {get_total_ads_published()}\n"
                f"✅ За сегодня: {get_daily_stats()}\n"
                f"🎫 Тикетов: {get_open_tickets_count()}\n"
                f"🤖 ИИ PEPEL: {'🟢 Включен' if is_ai_pepel_enabled() else '🔴 Выключен'}\n"
                f"📅 {date.today().strftime('%d.%m.%Y')}")
        await query.edit_message_text(text, parse_mode='Markdown', reply_markup=get_admin_keyboard())

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
            name = first_name or "без имени"
            text += f"{medal} **{name}**\n⭐ {rating} очков | 📤{sent} 📥{published}\n\n"
        await query.edit_message_text(text, parse_mode='Markdown', reply_markup=get_admin_keyboard())

    elif data == "admin_tickets":
        await show_tickets_page(query, context, 0)

    elif data == "admin_broadcast":
        total_users = get_total_users()
        context.user_data['broadcast_mode'] = True
        await query.edit_message_text(
            f"📨 Введите сообщение для рассылки\n\n👥 Будет отправлено: {total_users} пользователям")

    elif data == "admin_send_message":
        context.user_data['send_message_mode'] = True
        await query.edit_message_text(
            "💬 Введите ID пользователя и сообщение через пробел\n\nПример: `123456789 Привет! Как дела?`")

    elif data == "admin_toggle_ai_pepel":
        current = is_ai_pepel_enabled()
        set_ai_pepel_enabled(not current)
        await query.answer(f"ИИ PEPEL {'ВЫКЛЮЧЕН' if current else 'ВКЛЮЧЕН'}")
        await show_bot_settings(query)

    elif data == "admin_settings":
        await show_bot_settings(query)

    elif data == "admin_toggle_cooldown":
        current = is_cooldown_enabled()
        set_cooldown_enabled(not current)
        await query.answer(f"КД {'ВЫКЛЮЧЕНО' if current else 'ВКЛЮЧЕНО'}")
        await show_bot_settings(query)

    elif data == "admin_manage_admins":
        await query.edit_message_text("👑 **Управление админами**\n\nВыберите действие:", parse_mode='Markdown',
                                      reply_markup=get_manage_admins_keyboard())

    elif data == "admin_add_admin":
        context.user_data['add_admin_mode'] = True
        await query.edit_message_text("➕ Введите ID пользователя, которого хотите назначить администратором:")

    elif data == "admin_remove_admin":
        context.user_data['remove_admin_mode'] = True
        await query.edit_message_text("➖ Введите ID пользователя, которого хотите снять с администратора:")

    elif data == "admin_list_admins":
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT user_id, username, first_name FROM users WHERE is_admin = 1")
        admins = c.fetchall()
        conn.close()

        if not admins:
            text = "👑 **Список администраторов**\n\nНет администраторов"
        else:
            text = "👑 **Список администраторов**\n\n"
            for a in admins:
                uid, username, first_name = a
                text += f"• {first_name or 'Без имени'} (@{username or 'нет'}) - ID: `{uid}`\n"
        await query.edit_message_text(text, parse_mode='Markdown', reply_markup=get_manage_admins_keyboard())

    elif data == "admin_toggle_bot":
        current = is_bot_enabled()
        set_bot_setting('bot_enabled', '0' if current else '1')
        await query.answer(f"Бот {'выключен' if current else 'включен'}")
        await show_bot_settings(query)

    elif data == "admin_edit_channel":
        context.user_data['edit_channel'] = True
        current_channel = get_channel()
        await query.edit_message_text(
            f"📢 **Текущий канал:** {current_channel}\n\nВведите новый @username канала (например @mychannel):")

    elif data == "admin_edit_cooldown":
        context.user_data['edit_cooldown'] = True
        current = get_cooldown_minutes()
        await query.edit_message_text(f"⏱️ **Текущее КД:** {current} минут\n\nВведите новое значение (в минутах):")

    elif data == "admin_edit_welcome":
        context.user_data['edit_welcome'] = True
        await query.edit_message_text("✏️ Отправьте новое приветствие (используйте {name})")

    elif data == "admin_edit_maintenance":
        context.user_data['edit_maintenance'] = True
        await query.edit_message_text("✏️ Отправьте новое сообщение о тех.работах")

    elif data == "admin_back":
        await query.edit_message_text("🔑 **Админ-панель**", parse_mode='Markdown', reply_markup=get_admin_keyboard())

    elif data == "admin_exit":
        await query.edit_message_text("👋 Выход", reply_markup=None)

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
        await query.edit_message_text(f"💬 Введите сообщение для пользователя ID `{user_id}`:", parse_mode='Markdown')

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
        logger.info(f"🔘 Админ {user.id} нажал 'Ответить' на тикет {ticket_id}")
        
        if get_ticket_reply_sent(ticket_id):
            await query.edit_message_text("ℹ️ На этот тикет уже был отправлен ответ.")
            return
        
        context.user_data['reply_to_ticket'] = ticket_id
        await query.edit_message_text(
            f"✏️ **Ответ на тикет #{ticket_id}**\n\n"
            f"Напишите ваш ответ. Он будет отправлен пользователю.\n\n"
            f"Для отмены отправьте /cancel",
            parse_mode='Markdown'
        )
        logger.info(f"✅ Админ {user.id} переведен в режим ответа на тикет {ticket_id}")

    elif data.startswith("ticket_close_"):
        ticket_id = data.split("_")[2]
        admin_id = user.id
        close_ticket(ticket_id, admin_id)
        await query.edit_message_text(f"✅ Тикет #{ticket_id} закрыт", reply_markup=get_admin_keyboard())

        ticket = get_ticket(ticket_id)
        if ticket:
            try:
                await context.bot.send_message(
                    chat_id=ticket[1],
                    text=f"✅ Ваш тикет #{ticket_id} был закрыт администратором.\nСпасибо за обращение!"
                )
            except:
                pass

    elif data.startswith("ticket_block_"):
        parts = data.split("_")
        user_id = int(parts[2])
        ticket_id = parts[3]
        block_user(user_id)
        close_ticket(ticket_id, user.id)
        await query.edit_message_text(f"🚫 Пользователь {user_id} заблокирован\n✅ Тикет #{ticket_id} закрыт",
                                      reply_markup=get_admin_keyboard())


async def show_bot_settings(query):
    enabled = is_bot_enabled()
    cooldown_enabled = is_cooldown_enabled()
    cooldown_status = "🟢 ВКЛ" if cooldown_enabled else "🔴 ВЫКЛ"
    ai_pepel_status = "🟢 ВКЛ" if is_ai_pepel_enabled() else "🔴 ВЫКЛ"
    text = (f"⚙️ **НАСТРОЙКИ**\n\n"
            f"🔹 Статус бота: {'🟢 Включен' if enabled else '🔴 Выключен'}\n"
            f"📢 Канал: {get_channel()}\n"
            f"⏱️ КД на посты: {get_cooldown_minutes()} минут [{cooldown_status}]\n"
            f"🤖 ИИ PEPEL: {ai_pepel_status}\n"
            f"📝 Приветствие: {get_welcome_message()[:50]}...")
    await query.edit_message_text(text, parse_mode='Markdown', reply_markup=get_bot_settings_keyboard())


async def show_user_info(query, user_id):
    user_data = find_user_by_username_or_id(str(user_id))
    if not user_data:
        await query.edit_message_text("❌ Пользователь не найден")
        return
    uid, username, first_name, reg_date, sent, published, rating, is_blocked, is_admin_user, last_ad_time = user_data
    text = (f"👤 **{first_name or 'Без имени'}**\n"
            f"ID: `{uid}`\n"
            f"@{username or 'нет'}\n"
            f"📤 {sent} | 📥 {published}\n"
            f"⭐ {rating} очков\n"
            f"👑 Админ: {'Да' if is_admin_user else 'Нет'}\n"
            f"{'❌ Заблокирован' if is_blocked else '✅ Активен'}")
    await query.edit_message_text(text, parse_mode='Markdown', reply_markup=get_user_action_keyboard(uid, is_blocked))


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
        name = first_name or "без имени"
        text += f"{status}{admin} **{name}**\nID: `{uid}` | ⭐ {rating} очков\n\n"
    await query.edit_message_text(text, parse_mode='Markdown', reply_markup=get_users_navigation_keyboard(page, pages))


async def show_tickets_page(query, context, page):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        "SELECT ticket_id, user_id, username, first_name, message, created_at FROM tickets WHERE status = 'open' ORDER BY created_at DESC")
    tickets = c.fetchall()
    conn.close()

    if not tickets:
        await query.edit_message_text("✅ Нет открытых тикетов", reply_markup=get_admin_keyboard())
        return

    text = "🎫 **ОТКРЫТЫЕ ТИКЕТЫ**\n\n"
    for t in tickets:
        ticket_id, user_id, username, first_name, message, created_at = t
        short_msg = message[:40] + "..." if len(message) > 40 else message
        created = datetime.fromisoformat(created_at).strftime('%d.%m %H:%M')
        username_str = f"@{username}" if username else "нет"
        text += f"**#{ticket_id}** от {username_str}\n📝 {short_msg}\n🕐 {created} | ID: {user_id}\n\n"

    await query.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup([[
        InlineKeyboardButton("🔙 Назад", callback_data="admin_back")
    ]]))


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

    result_text = (f"📊 **ОТЧЕТ О РАССЫЛКЕ**\n\n"
                   f"👥 Всего: {total}\n"
                   f"✅ Отправлено: {sent}\n"
                   f"❌ Не отправлено: {failed}")

    await context.bot.send_message(chat_id=update.effective_user.id, text=result_text, parse_mode='Markdown',
                                   reply_markup=get_admin_keyboard())
    context.user_data.pop('broadcast_mode', None)
    context.user_data.pop('broadcast_message', None)


# ==================== АВТОПУБЛИКАЦИЯ ИИ PEPEL ====================
async def auto_publish_with_ai_pepel(context: ContextTypes.DEFAULT_TYPE, message_id: int, user_id: int, text: str, username: str):
    """Функция автопубликации через 5 секунд после поста в чат админов"""
    await asyncio.sleep(5)
    
    # Проверяем, включен ли ИИ PEPEL
    if not is_ai_pepel_enabled():
        logger.info("ИИ PEPEL выключен, автопубликация не выполнена")
        return
    
    # Проверяем объявление
    is_valid, validation_msg, details = validate_ad_for_ai_pepel(text, username)
    
    if is_valid:
        # Публикуем в канал
        channel = get_channel()
        try:
            # Копируем сообщение в канал
            await context.bot.copy_message(
                chat_id=channel,
                from_chat_id=GROUP_CHAT_ID,
                message_id=message_id
            )
            
            # Увеличиваем счетчик публикаций
            increment_published(user_id)
            
            # Отправляем сообщение в чат админов о публикации
            await context.bot.send_message(
                chat_id=GROUP_CHAT_ID,
                text=f"🤖 **ИИ PEPEL** автоматически опубликовал объявление в канале!\n\n"
                     f"👤 Автор: @{username}\n"
                     f"🆔 ID: `{user_id}`\n"
                     f"✅ Проверка пройдена: {validation_msg}\n"
                     f"📏 Длина: {len(text)} символов\n"
                     f"🕐 {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}",
                parse_mode='Markdown'
            )
            
            logger.info(f"🤖 ИИ PEPEL опубликовал объявление от пользователя {user_id}")
            
        except Exception as e:
            logger.error(f"Ошибка автопубликации ИИ PEPEL: {e}")
            await context.bot.send_message(
                chat_id=GROUP_CHAT_ID,
                text=f"❌ **Ошибка автопубликации ИИ PEPEL**\n\n"
                     f"👤 Автор: @{username}\n"
                     f"🆔 ID: `{user_id}`\n"
                     f"❌ Ошибка: {e}\n"
                     f"📝 Объявление отправлено на ручную модерацию.",
                parse_mode='Markdown'
            )
    else:
        # Объявление не прошло проверку - ничего не делаем, оставляем на модерацию
        logger.info(f"🤖 ИИ PEPEL: объявление от {user_id} НЕ ПРОШЛО проверку: {validation_msg}")


# ==================== ОСНОВНОЙ ОБРАБОТЧИК ====================
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    text = update.message.text if update.message.text else ""

    # ===== ЛИЧНЫЕ СООБЩЕНИЯ =====
    if chat.type == "private":
        logger.info(f"📩 Личное сообщение от {user.id}: {text[:50] if text else 'медиа'}")

        # Режим отправки сообщения пользователю (админ)
        if context.user_data.get('send_message_mode'):
            parts = text.split(' ', 1)
            if len(parts) >= 2:
                try:
                    target_id = int(parts[0])
                    msg_text = parts[1]
                    await context.bot.send_message(chat_id=target_id,
                                                   text=f"📩 **Сообщение от администратора:**\n\n{msg_text}",
                                                   parse_mode='Markdown')
                    await update.message.reply_text(f"✅ Сообщение отправлено пользователю ID `{target_id}`",
                                                    parse_mode='Markdown')
                except ValueError:
                    await update.message.reply_text("❌ Неверный формат ID. Введите число.")
                except Exception as e:
                    await update.message.reply_text(f"❌ Ошибка: {e}")
            else:
                await update.message.reply_text("❌ Неверный формат. Используйте: ID сообщение")
            context.user_data.pop('send_message_mode')
            return

        # Режим отправки сообщения выбранному пользователю
        if context.user_data.get('send_message_target'):
            target_id = context.user_data['send_message_target']
            try:
                await context.bot.send_message(chat_id=target_id, text=f"📩 **Сообщение от администратора:**\n\n{text}",
                                               parse_mode='Markdown')
                await update.message.reply_text(f"✅ Сообщение отправлено пользователю ID `{target_id}`",
                                                parse_mode='Markdown')
            except Exception as e:
                await update.message.reply_text(f"❌ Ошибка: {e}")
            context.user_data.pop('send_message_target')
            return

        # ===== ОТВЕТ ПОЛЬЗОВАТЕЛЮ ОТ АДМИНА =====
        if context.user_data.get('reply_to_user'):
            target_user_id = context.user_data['reply_to_user']
            reply_text = text
            
            logger.info(f"📝 Получен ответ от админа {user.id} для пользователя {target_user_id}: {reply_text[:50]}")
            
            try:
                await context.bot.send_message(
                    chat_id=target_user_id,
                    text=f"📩 **Сообщение от администратора:**\n\n{reply_text}",
                    parse_mode='Markdown'
                )
                await update.message.reply_text(
                    f"✅ **Сообщение отправлено пользователю!**\n\n"
                    f"👤 ID: `{target_user_id}`\n"
                    f"📝 Текст: {reply_text[:100]}...",
                    reply_markup=get_main_keyboard()
                )
                logger.info(f"✅ Сообщение отправлено пользователю {target_user_id}")
            except Exception as e:
                logger.error(f"❌ Ошибка отправки сообщения: {e}")
                await update.message.reply_text(f"❌ Не удалось отправить сообщение: {e}")
            
            context.user_data.pop('reply_to_user')
            return

        if context.user_data.get('broadcast_mode'):
            context.user_data['broadcast_message'] = update.message
            total_users = get_total_users()
            await update.message.reply_text(
                f"📨 Подтвердите рассылку\n\n👥 Будет отправлено: {total_users} пользователям",
                reply_markup=get_broadcast_confirm_keyboard(total_users))
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

        if context.user_data.get('edit_cooldown'):
            try:
                new_cooldown = int(text)
                if new_cooldown < 1:
                    await update.message.reply_text("❌ КД должно быть не менее 1 минуты")
                else:
                    set_cooldown_minutes(new_cooldown)
                    await update.message.reply_text(f"✅ КД изменено на {new_cooldown} минут",
                                                    reply_markup=get_main_keyboard())
            except:
                await update.message.reply_text("❌ Введите число (минуты)")
            context.user_data.pop('edit_cooldown')
            return

        if context.user_data.get('add_admin_mode'):
            try:
                user_id = int(text)
                user_data = get_user_stats(user_id)
                if user_data:
                    set_admin(user_id, True)
                    await update.message.reply_text(f"✅ Пользователь {user_id} назначен администратором!",
                                                    reply_markup=get_main_keyboard())
                else:
                    await update.message.reply_text("❌ Пользователь не найден", reply_markup=get_main_keyboard())
            except:
                await update.message.reply_text("❌ Введите корректный ID пользователя",
                                                reply_markup=get_main_keyboard())
            context.user_data.pop('add_admin_mode')
            return

        if context.user_data.get('remove_admin_mode'):
            try:
                user_id = int(text)
                if user_id in ADMIN_IDS:
                    await update.message.reply_text("❌ Нельзя снять права с главного администратора!",
                                                    reply_markup=get_main_keyboard())
                else:
                    set_admin(user_id, False)
                    await update.message.reply_text(f"✅ Пользователь {user_id} снят с администратора!",
                                                    reply_markup=get_main_keyboard())
            except:
                await update.message.reply_text("❌ Введите корректный ID пользователя",
                                                reply_markup=get_main_keyboard())
            context.user_data.pop('remove_admin_mode')
            return

        if context.user_data.get('search_mode'):
            found = find_user_by_username_or_id(text)
            if found:
                uid, username, first_name, reg_date, sent, published, rating, is_blocked, is_admin_user, last_ad_time = found
                info = (f"👤 **{first_name or 'Без имени'}**\n"
                        f"ID: `{uid}`\n"
                        f"@{username or 'нет'}\n"
                        f"📤 {sent} | 📥 {published}\n"
                        f"⭐ {rating} очков\n"
                        f"👑 Админ: {'Да' if is_admin_user else 'Нет'}\n"
                        f"{'❌ Заблокирован' if is_blocked else '✅ Активен'}")
                await update.message.reply_text(info, parse_mode='Markdown',
                                                reply_markup=get_user_action_keyboard(uid, is_blocked))
            else:
                await update.message.reply_text("❌ Не найден")
            context.user_data.pop('search_mode')
            return

        # ===== ОТВЕТ НА ТИКЕТ В ЛИЧКЕ =====
        if context.user_data.get('reply_to_ticket'):
            ticket_id = context.user_data['reply_to_ticket']
            reply_text = text
            
            logger.info(f"📝 Получен ответ на тикет {ticket_id} от админа {user.id} в личке: {reply_text[:50]}")
            
            target_user_id = get_ticket_user_id(ticket_id)
            logger.info(f"👤 Пользователь для тикета {ticket_id}: {target_user_id}")
            
            if target_user_id:
                if get_ticket_reply_sent(ticket_id):
                    await update.message.reply_text("ℹ️ На этот тикет уже был отправлен ответ.")
                    context.user_data.pop('reply_to_ticket')
                    return

                update_ticket_reply(ticket_id, reply_text)

                try:
                    await context.bot.send_message(
                        chat_id=target_user_id,
                        text=f"✏️ **Ответ на ваш тикет #{ticket_id}**\n\n{reply_text}\n\n— Администратор",
                        parse_mode='Markdown'
                    )
                    await update.message.reply_text(
                        f"✅ **Ответ на тикет #{ticket_id} отправлен пользователю!**\n\n"
                        f"Вы можете закрыть тикет в админ-панели.",
                        reply_markup=get_main_keyboard()
                    )
                    logger.info(f"✅ Ответ на тикет {ticket_id} отправлен пользователю {target_user_id}")
                except Exception as e:
                    logger.error(f"❌ Ошибка отправки ответа: {e}")
                    await update.message.reply_text(f"❌ Не удалось отправить ответ: {e}")
            else:
                logger.error(f"❌ Тикет {ticket_id} не найден")
                await update.message.reply_text("❌ Тикет не найден")

            context.user_data.pop('reply_to_ticket')
            return

        if not is_bot_enabled() and not is_admin(user.id):
            await update.message.reply_text(get_bot_setting('maintenance_message'))
            return

        if text == "📋 Отправить объявление":
            stats = get_user_stats(user.id)
            if stats and stats[6] == 1:
                await update.message.reply_text("❌ Вы заблокированы")
                return

            can_send, wait_time = can_send_ad(user.id)
            if not can_send:
                minutes, seconds = wait_time
                await update.message.reply_text(
                    f"⏱️ **Вы слишком часто отправляете объявления!**\n\nСледующее объявление можно отправить через **{minutes} мин {seconds} сек**")
                return

            context.user_data['awaiting_ad'] = True
            cooldown_status = f"\n\n⏱️ Вы сможете отправить следующее через {get_cooldown_minutes()} минут" if is_cooldown_enabled() else "\n\n♾️ КД отключено"
            await update.message.reply_text(f"📝 Отправьте объявление{cooldown_status}")

        elif text == "👤 Мой профиль ⭐":
            stats = get_user_stats(user.id)
            if not stats:
                await update.message.reply_text("❌ Ошибка загрузки профиля")
                return
            username, first_name, reg_date, sent, published, rating, is_blocked, is_admin_user, last_ad_time = stats
            today = date.today().isoformat()
            conn = sqlite3.connect(DB_FILE)
            c = conn.cursor()
            c.execute("SELECT ads_count FROM daily_stats WHERE user_id = ? AND date = ?", (user.id, today))
            daily = c.fetchone()
            conn.close()
            daily_count = daily[0] if daily else 0

            next_ad_text = ""
            if is_cooldown_enabled() and last_ad_time:
                last = datetime.fromisoformat(last_ad_time)
                cooldown = get_cooldown_minutes()
                next_time = last + timedelta(minutes=cooldown)
                now = datetime.now()
                if now < next_time:
                    wait = int((next_time - now).total_seconds())
                    minutes = wait // 60
                    seconds = wait % 60
                    next_ad_text = f"\n⏱️ Следующий пост через: {minutes}м {seconds}с"
                else:
                    next_ad_text = "\n✅ Вы можете отправить пост"
            elif not is_cooldown_enabled():
                next_ad_text = "\n♾️ КД отключено"

            current_level, next_level, points_for_next, progress_bar = get_level(rating)

            profile = (f"     👤 **МОЙ ПРОФИЛЬ**     \n\n"
                       f"✨ **Имя:** {first_name}\n"
                       f"👥 **Ник:** @{username or 'нет'}\n"
                       f"🆔 **ID:** `{user.id}`\n"
                       f"📅 **Зарегистрирован:** {reg_date.split('T')[0]}\n\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n\n"
                       f"📊 **СТАТИСТИКА**\n\n"
                       f"📤 **Отправлено:** {sent}\n"
                       f"📥 **Опубликовано:** {published}\n"
                       f"📈 **Сегодня:** {daily_count}{next_ad_text}\n\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n\n"
                       f"🏆 **РЕЙТИНГ**\n\n"
                       f"⭐ **Очки:** {rating}\n"
                       f"📊 **Уровень:** {current_level['name']}\n"
                       f"📈 **Прогресс:** {progress_bar}\n")
            
            if next_level:
                profile += f"🎯 **До {next_level['name']}:** {points_for_next} очков\n"
            
            profile += f"\n━━━━━━━━━━━━━━━━━━━━\n\n"
            profile += f"🔒 **Статус:** {'❌ Заблокирован' if is_blocked else '✅ Активен'}\n"
            profile += f"👑 **Админ:** {'Да' if is_admin_user else 'Нет'}"
            
            await update.message.reply_text(profile, parse_mode='Markdown')

        elif text == "📊 Статистика":
            cooldown_status = f"⏱️ КД на посты: {get_cooldown_minutes()} минут" if is_cooldown_enabled() else "⏱️ КД на посты: ♾️ ВЫКЛЮЧЕНО"
            ai_status = f"🤖 ИИ PEPEL: {'🟢 Включен' if is_ai_pepel_enabled() else '🔴 Выключен'}"
            stat = (f"     📊 **СТАТИСТИКА БОТА**     \n\n"
                    f"👥 **Пользователей:** {get_total_users()}\n"
                    f"📊 **Активных сегодня:** {get_active_users_today()}\n"
                    f"🚫 **Заблокировано:** {get_blocked_users_count()}\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"📤 **Всего отправлено:** {get_total_ads_sent()}\n"
                    f"📥 **Всего опубликовано:** {get_total_ads_published()}\n"
                    f"✅ **За сегодня:** {get_daily_stats()}\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"{cooldown_status}\n"
                    f"{ai_status}\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"📅 **Дата:** {date.today().strftime('%d.%m.%Y')}")
            await update.message.reply_text(stat, parse_mode='Markdown')

        elif text == "❓ Помощь / Тикет":
            context.user_data['ticket_mode'] = True
            await update.message.reply_text("📝 Опишите вашу проблему одним сообщением:")

        elif context.user_data.get('awaiting_ad'):
            try:
                if is_user_blocked(user.id):
                    await update.message.reply_text("❌ Вы заблокированы и не можете отправлять объявления!")
                    context.user_data.pop('awaiting_ad')
                    return
                    
                can_send, wait_time = can_send_ad(user.id)
                if not can_send:
                    minutes, seconds = wait_time
                    await update.message.reply_text(
                        f"⏱️ **Вы слишком часто отправляете объявления!**\n\nСледующее объявление можно отправить через **{minutes} мин {seconds} сек**")
                    context.user_data.pop('awaiting_ad')
                    return

                # Отправляем объявление в чат админов
                sent_message = await update.message.forward(GROUP_CHAT_ID)
                
                user_data = get_user_stats(user.id)
                username = user_data[0] if user_data else user.username
                first_name = user_data[1] if user_data else user.first_name
                
                info = f"📋 **НОВОЕ ОБЪЯВЛЕНИЕ**\n\n👤 @{username or user.username or 'нет'} ({first_name or user.first_name or 'Без имени'})\n🆔 ID: `{user.id}`\n🕐 {datetime.now().strftime('%d.%m.%Y %H:%M')}"
                
                await context.bot.send_message(
                    chat_id=GROUP_CHAT_ID,
                    text=info,
                    parse_mode='Markdown',
                    reply_markup=get_group_keyboard(user.id, is_published=False)
                )
                
                update_user_ads(user.id, user.username, user.first_name)
                
                await update.message.reply_text("✅ Объявление отправлено на модерацию!",
                                                reply_markup=get_main_keyboard())
                
                # Запускаем автопубликацию ИИ PEPEL через 5 секунд
                asyncio.create_task(auto_publish_with_ai_pepel(
                    context, 
                    sent_message.message_id, 
                    user.id, 
                    text, 
                    user.username or ""
                ))
                
            except Exception as e:
                logger.error(f"Ошибка: {e}")
                await update.message.reply_text(f"❌ Ошибка: {e}")
            context.user_data.pop('awaiting_ad')

        elif context.user_data.get('ticket_mode'):
            if is_user_blocked(user.id):
                await update.message.reply_text("❌ Вы заблокированы и не можете создавать тикеты!")
                context.user_data.pop('ticket_mode')
                return
                
            ticket_id = create_ticket(user.id, user.username, user.first_name, text)
            if ticket_id:
                ticket_info = f"🎫 **НОВЫЙ ТИКЕТ** #{ticket_id}\n\n👤 @{user.username or 'нет'} (ID: {user.id})\n📝 {text}\n🕐 {datetime.now().strftime('%d.%m.%Y %H:%M')}"
                await context.bot.send_message(SUPPORT_CHAT_ID, ticket_info, parse_mode='Markdown',
                                               reply_markup=get_ticket_keyboard(ticket_id, user.id))
                await update.message.reply_text(
                    f"✅ Тикет #{ticket_id} создан!\n\nАдминистратор ответит вам в ближайшее время.",
                    reply_markup=get_main_keyboard())
            else:
                await update.message.reply_text("❌ Ошибка создания тикета")
            context.user_data.pop('ticket_mode')

        else:
            pass

    # ===== ГРУППЫ (чат админов) =====
    elif chat.type in ["group", "supergroup"]:
        if chat.id == GROUP_CHAT_ID or chat.id == SUPPORT_CHAT_ID:
            logger.info(f"📢 Сообщение в чате админов от {user.id}: {text[:50] if text else 'медиа'}")
            
            # ===== ОТВЕТ ПОЛЬЗОВАТЕЛЮ ОТ АДМИНА В ЧАТЕ АДМИНОВ =====
            if context.user_data.get('reply_to_user'):
                target_user_id = context.user_data['reply_to_user']
                reply_text = text
                
                logger.info(f"📝 Получен ответ от админа {user.id} для пользователя {target_user_id} в чате админов: {reply_text[:50]}")
                
                try:
                    await context.bot.send_message(
                        chat_id=target_user_id,
                        text=f"📩 **Сообщение от администратора:**\n\n{reply_text}",
                        parse_mode='Markdown'
                    )
                    await update.message.reply_text(
                        f"✅ **Сообщение отправлено пользователю!**\n\n"
                        f"👤 ID: `{target_user_id}`\n"
                        f"📝 Текст: {reply_text[:100]}..."
                    )
                    logger.info(f"✅ Сообщение отправлено пользователю {target_user_id}")
                except Exception as e:
                    logger.error(f"❌ Ошибка отправки сообщения: {e}")
                    await update.message.reply_text(f"❌ Не удалось отправить сообщение: {e}")
                
                context.user_data.pop('reply_to_user')
                return
            
            # Проверяем, находится ли админ в режиме ответа на тикет
            if context.user_data.get('reply_to_ticket'):
                ticket_id = context.user_data['reply_to_ticket']
                reply_text = text
                
                logger.info(f"📝 Получен ответ на тикет {ticket_id} от админа {user.id} в чате админов: {reply_text[:50]}")
                
                target_user_id = get_ticket_user_id(ticket_id)
                logger.info(f"👤 Пользователь для тикета {ticket_id}: {target_user_id}")
                
                if target_user_id:
                    if get_ticket_reply_sent(ticket_id):
                        await update.message.reply_text("ℹ️ На этот тикет уже был отправлен ответ.")
                        context.user_data.pop('reply_to_ticket')
                        return

                    update_ticket_reply(ticket_id, reply_text)

                    try:
                        await context.bot.send_message(
                            chat_id=target_user_id,
                            text=f"✏️ **Ответ на ваш тикет #{ticket_id}**\n\n{reply_text}\n\n— Администратор",
                            parse_mode='Markdown'
                        )
                        await update.message.reply_text(
                            f"✅ **Ответ на тикет #{ticket_id} отправлен пользователю!**"
                        )
                        logger.info(f"✅ Ответ на тикет {ticket_id} отправлен пользователю {target_user_id}")
                    except Exception as e:
                        logger.error(f"❌ Ошибка отправки ответа: {e}")
                        await update.message.reply_text(f"❌ Не удалось отправить ответ: {e}")
                else:
                    logger.error(f"❌ Тикет {ticket_id} не найден")
                    await update.message.reply_text("❌ Тикет не найден")

                context.user_data.pop('reply_to_ticket')
                return
            
            # Если сообщение не является ответом, игнорируем
            logger.info(f"Сообщение в чате админов не является ответом, игнорируем")


# ==================== ГРУППОВОЙ ОБРАБОТЧИК ====================
async def group_action_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    channel = get_channel()
    admin = query.from_user
    channel_link = channel.replace('@', 'https://t.me/')
    
    parts = data.split("_")
    
    if data.startswith("publish_"):
        user_id = int(parts[1])
    elif data.startswith("republish_"):
        user_id = int(parts[1])
    elif data.startswith("delete_"):
        user_id = int(parts[1])
    elif data.startswith("block_"):
        user_id = int(parts[1])
    elif data.startswith("unblock_"):
        user_id = int(parts[1])
    elif data.startswith("reply_to_user_"):
        user_id = int(parts[3])
    else:
        return
    
    author_data = get_user_stats(user_id)
    author_username = author_data[0] if author_data else None
    author_first_name = author_data[1] if author_data else None

    if data.startswith("publish_"):
        try:
            original_id = query.message.message_id - 1

            await context.bot.copy_message(
                chat_id=channel,
                from_chat_id=GROUP_CHAT_ID,
                message_id=original_id
            )

            increment_published(user_id)

            await query.edit_message_text(
                text=f"📋 **ОБЪЯВЛЕНИЕ ОПУБЛИКОВАНО**\n\n"
                     f"👤 @{author_username or 'нет'} ({author_first_name or 'Без имени'})\n"
                     f"🆔 ID: `{user_id}`\n"
                     f"👑 Выложил админ: @{admin.username or admin.first_name}\n"
                     f"📢 Канал: [{channel}]({channel_link})\n"
                     f"🕐 {datetime.now().strftime('%d.%m.%Y %H:%M')}",
                parse_mode='Markdown',
                disable_web_page_preview=True,
                reply_markup=get_group_keyboard(user_id, is_published=True)
            )
            
            await context.bot.send_message(
                chat_id=GROUP_CHAT_ID,
                text=f"✅ **Пост опубликован в канале**\n\n"
                     f"👤 Автор: @{author_username or 'нет'} (ID: `{user_id}`)\n"
                     f"👑 Админ: @{admin.username or admin.first_name}\n"
                     f"📢 Канал: [{channel}]({channel_link})",
                parse_mode='Markdown',
                disable_web_page_preview=True
            )
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            await query.edit_message_text(f"❌ Ошибка: {e}")

    elif data.startswith("republish_"):
        try:
            original_id = query.message.message_id - 1

            await context.bot.copy_message(
                chat_id=channel,
                from_chat_id=GROUP_CHAT_ID,
                message_id=original_id
            )

            increment_published(user_id)

            await query.answer("✅ Пост опубликован повторно!")
            
            await query.edit_message_text(
                text=f"📋 **ОБЪЯВЛЕНИЕ ОПУБЛИКОВАНО (повторно)**\n\n"
                     f"👤 @{author_username or 'нет'} ({author_first_name or 'Без имени'})\n"
                     f"🆔 ID: `{user_id}`\n"
                     f"👑 Выложил админ: @{admin.username or admin.first_name}\n"
                     f"📢 Канал: [{channel}]({channel_link})\n"
                     f"🕐 {datetime.now().strftime('%d.%m.%Y %H:%M')}",
                parse_mode='Markdown',
                disable_web_page_preview=True,
                reply_markup=get_group_keyboard(user_id, is_published=True)
            )
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            await query.answer("❌ Ошибка публикации")

    elif data.startswith("reply_to_user_"):
        context.user_data['reply_to_user'] = user_id
        await query.edit_message_text(
            text=f"💬 **Ответ пользователю**\n\n"
                 f"👤 Пользователь: @{author_username or 'нет'} (ID: `{user_id}`)\n\n"
                 f"Напишите ваше сообщение. Оно будет отправлено пользователю.\n\n"
                 f"Для отмены отправьте /cancel",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Отмена", callback_data="cancel_reply")
            ]])
        )
        logger.info(f"🔘 Админ {admin.id} начал ответ пользователю {user_id}")

    elif data == "cancel_reply":
        context.user_data.pop('reply_to_user', None)
        await query.edit_message_text(
            text="❌ Ответ отменен",
            reply_markup=None
        )

    elif data.startswith("delete_"):
        await query.edit_message_text(
            f"❌ **УДАЛЕНО**\n\n"
            f"👤 Автор: @{author_username or 'нет'} (ID: `{user_id}`)\n"
            f"👑 Админ: @{admin.username or admin.first_name}",
            parse_mode='Markdown'
        )

    elif data.startswith("block_"):
        block_user(user_id)
        await query.edit_message_text(
            f"🚫 **ЗАБЛОКИРОВАН**\n\n"
            f"👤 Пользователь: @{author_username or 'нет'} (ID: `{user_id}`)\n"
            f"👑 Админ: @{admin.username or admin.first_name}\n\n"
            f"⚠️ Пользователь не может отправлять объявления и создавать тикеты",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Разблокировать", callback_data=f"unblock_{user_id}")
            ]])
        )

    elif data.startswith("unblock_"):
        user_id = int(parts[1])
        unblock_user(user_id)
        await query.edit_message_text(
            f"✅ **РАЗБЛОКИРОВАН**\n\n"
            f"👤 Пользователь: ID `{user_id}`\n"
            f"👑 Админ: @{admin.username or admin.first_name}\n\n"
            f"✨ Пользователь снова может отправлять объявления и создавать тикеты",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🚫 Заблокировать", callback_data=f"block_{user_id}")
            ]])
        )


# ==================== ЗАПУСК ====================
async def run_bot():
    logger.info("=" * 50)
    logger.info("ЗАПУСК БОТА")
    logger.info("=" * 50)

    app = Application.builder().token(BOT_TOKEN).connect_timeout(120).read_timeout(120).write_timeout(120).pool_timeout(120).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin", admin_command))
    app.add_handler(CommandHandler("stats", admin_stats_command))
    app.add_handler(CommandHandler("levels", levels_command))
    app.add_handler(CommandHandler("getid", get_chat_id))
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(CallbackQueryHandler(admin_callback_handler,
                                         pattern="^(admin_|users_page_|user_stats_|broadcast_|ticket_|send_msg_)"))
    app.add_handler(CallbackQueryHandler(group_action_handler, pattern="^(publish_|delete_|block_|republish_|unblock_|reply_to_user_|cancel_reply)"))
    app.add_handler(MessageHandler(filters.ALL, message_handler))

    await app.initialize()
    await app.start()
    
    try:
        await app.bot.delete_webhook(drop_pending_updates=True)
        logger.info("✅ Webhook удален")
    except Exception as e:
        logger.warning(f"⚠️ Не удалось удалить webhook: {e}")
    
    await app.updater.start_polling(drop_pending_updates=True, timeout=120)

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
