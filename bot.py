import os
import json
import math
import copy
import time
import logging
import re
import requests
import telebot
from telebot import types
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import quote
import threading

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    PSYCOPG2_IMPORT_ERROR = None
except Exception as e:
    psycopg2 = None
    RealDictCursor = None
    PSYCOPG2_IMPORT_ERROR = e

# =========================
# НАСТРОЙКИ
# =========================

TOKEN = os.getenv("TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
RAILWAY_ENVIRONMENT = os.getenv("RAILWAY_ENVIRONMENT")

BOT_USERNAME = "KakMakDak_bot"
PHONE_NUMBER = "+79789927572"

ADMIN_CHAT_ID = -1003701168467
PUBLIC_GROUP_ID = -1003817058763

YANDEX_LINK = "https://yandex.ru/maps/org/kakmakdak/161927095302"
YANDEX_REVIEWS_LINK = "https://yandex.ru/maps/org/kakmakdak/161927095302/reviews"
WORKING_HOURS = "Ежедневно 15:00-03:00"

DATA_FILE = "bot_storage.json"

INFO_DELIVERY_TEXT = "ℹ️ Важно: доставка до подъезда/входа. В квартиру не заносим."

def validate_startup_config():
    if not TOKEN:
        raise ValueError("Не задан TOKEN")
    if not DATABASE_URL:
        raise ValueError("Не задан DATABASE_URL")
    if psycopg2 is None:
        raise ImportError(f"Не установлена зависимость psycopg2/psycopg2-binary: {PSYCOPG2_IMPORT_ERROR}")

validate_startup_config()
bot = telebot.TeleBot(TOKEN)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# =========================
# РЕЖИМ ХРАНЕНИЯ
# =========================
USE_POSTGRES = True

# =========================
# МЕНЮ
# =========================
MENU = {
    "1": {
        "title": "🌯 Шаурма и Хот-доги",
        "items": {
            "Шаурма Классик": 350,
            "Шаурма с тигровой креветкой": 450,
            "Шаурма с говядиной КакМакДак": 500,
            "Шаурма сытная (сыр+карт.по-дер.)": 500,
            "Хот-дог Датский": 300,
            "Хот-дог в лаваше (2 колбаски)": 450,
        },
    },
    "2": {
        "title": "🍔 Бургеры и Сэндвичи",
        "items": {
            "Гамбургер": 350,
            "Чизбургер": 350,
            "Дабл Чизбургер": 500,
            "Чикен Чиз": 500,
            "Бургер КакМакДак": 650,
            "Фишбургер": 500,
            "Крабсбургер": 650,
            "Сэндвич с красной рыбой": 300,
            "Сэндвич с курицей": 250,
        },
    },
    "3": {
        "title": "🍜 Лапша WOK",
        "items": {
            "Удон с курицей": 550,
            "Удон с говядиной": 650,
            "Удон с креветкой": 600,
            "Удон сливочный с креветкой": 650,
            "Соба с курицей": 550,
            "Соба с говядиной": 650,
            "Соба с креветкой": 600,
            "Соба сливочная с креветкой": 650,
            "Тяхан с курицей": 600,
            "Тяхан с тунцом и креветкой": 750,
        },
    },
    "4": {
        "title": "🍲 Супы",
        "items": {
            "Том-ям": 650,
        },
    },
    "5": {
        "title": "🍣 Роллы и Суши",
        "items": {
            "Филадельфия": 650,
            "Филадельфия с креветкой": 750,
            "Аляска Лосось": 550,
            "Аляска угорь": 550,
            "Сливочный угорь": 750,
            "Сливочный с угрём": 750,
            "Сливочная креветка": 700,
            "Сливочная с тигровой креветкой": 700,
            "Чиз-ролл": 450,
            "Салат чука с ореховым соусом": 300,
            "Маки с рыбой": 300,
            "Маки огурец": 200,
        },
    },
    "6": {
        "title": "🍟 Закуски",
        "items": {
            "Картофель фри": 250,
            "Картофель по-деревенски": 250,
            "Сырные палочки": 400,
            "Наггетсы": 350,
            "Луковые кольца": 250,
            "Креветки к пиву": 450,
            "Креветки в медово-горчичном соусе": 500,
        },
    },
    "7": {
        "title": "🥤 Лимонады",
        "items": {
            "Кока-Кола 1 л.": 200,
            "Волчок Кола": 150,
            "Волчок Цитрус микс": 150,
            "Волчок Грейпфрут гибискус": 150,
            "Волчок Манго кокос": 150,
            "Волчок Личи груша": 150,
            "Волчок Арбуз": 150,
            "Волчок Персик": 150,
        },
    },
}

MAKI_FISH_OPTIONS = {
    "Маки тунец": 300,
    "Маки лосось": 300,
    "Маки угорь": 300,
    "Маки креветка": 300,
}


SAUCES = {
    "Сырный": 50,
    "Чесночный": 50,
    "Горчичный": 50,
    "Кисло-сладкий": 50,
    "Шрирача": 50,
    "Терияки": 50,
    "Сладкий чили": 50,
}

FRYER_ITEMS = {
    "Картофель фри",
    "Картофель по-деревенски",
    "Наггетсы",
    "Луковые кольца",
}

LOCALITIES = {
    "Ялта": (44.4952, 34.1663),
    "Гурзуф": (44.5462, 34.2784),
    "Массандра": (44.5170, 34.2029),
    "Ливадия": (44.4672, 34.1426),
    "Ореанда": (44.4573, 34.1296),
    "Гаспра": (44.4337, 34.1032),
    "Кореиз": (44.4330, 34.0856),
    "Мисхор": (44.4287, 34.0798),
    "Алупка": (44.4181, 34.0453),
    "Симеиз": (44.4013, 33.9984),
}

SUSHI_CART_ITEMS = {
    "Филадельфия",
    "Филадельфия с креветкой",
    "Аляска Лосось",
    "Аляска угорь",
    "Сливочный угорь",
    "Сливочный с угрём",
    "Сливочная креветка",
    "Сливочная с тигровой креветкой",
    "Чиз-ролл",
    "Маки огурец",
    "Маки тунец",
    "Маки лосось",
    "Маки угорь",
    "Маки креветка",
}

DRINK_CART_ITEMS = {
    "Кока-Кола 1 л.",
    "Волчок Кола",
    "Волчок Цитрус микс",
    "Волчок Грейпфрут гибискус",
    "Волчок Манго кокос",
    "Волчок Личи груша",
    "Волчок Арбуз",
    "Волчок Персик",
}

CHUKA_NAME = "Салат чука с ореховым соусом"
CHUKA_PRICE = 300
PUBLIC_POST_HOUR = 16
PUBLIC_POST_MINUTE = 0
PUBLIC_POST_TIMEZONE = "Europe/Moscow"
ORDER_STALE_MINUTES = 10
MOSCOW_TZ = ZoneInfo(PUBLIC_POST_TIMEZONE)

# =========================
# СОСТОЯНИЯ
# =========================
STATE_NONE = "none"
STATE_WAIT_ADDRESS_CHOICE = "wait_address_choice"
STATE_WAIT_ADDRESS_MANUAL = "wait_address_manual"
STATE_WAIT_PHONE = "wait_phone"
STATE_WAIT_PAYMENT = "wait_payment"
STATE_WAIT_COMMENT_OR_SKIP = "wait_comment_or_skip"
STATE_WAIT_CONFIRM = "wait_confirm"

# =========================
# JSON STORAGE
# =========================
storage = {
    "user_cart": {},
    "user_state": {},
    "user_data": {},
    "preview_message_ids": {},
    "order_counters_by_date": {},
    "order_history": {},
    "last_public_post_date": "",
    "admin_order_statuses": {},
    "admin_order_meta": {},
}


def load_json_storage():
    global storage
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                for key in storage:
                    if key in data and isinstance(data[key], type(storage[key])):
                        storage[key] = data[key]
        except Exception:
            pass


def save_json_storage():
    try:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(storage, f, ensure_ascii=False, indent=2)
    except Exception:
        pass



# =========================
# POSTGRES STORAGE
# =========================
def pg_get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def pg_init_db():
    with pg_get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    state TEXT NOT NULL DEFAULT 'none',
                    data_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    preview_message_id BIGINT NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS carts (
                    user_id BIGINT NOT NULL,
                    item_name TEXT NOT NULL,
                    price INTEGER NOT NULL,
                    qty INTEGER NOT NULL,
                    PRIMARY KEY (user_id, item_name)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS order_counters (
                    order_date_key TEXT PRIMARY KEY,
                    counter_value INTEGER NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    order_id_text TEXT NOT NULL,
                    total INTEGER NOT NULL,
                    payment TEXT NOT NULL,
                    address TEXT NOT NULL,
                    locality TEXT NOT NULL,
                    comment TEXT NOT NULL DEFAULT '',
                    cart_json JSONB NOT NULL,
                    accepted_for_history BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                ALTER TABLE orders
                ADD COLUMN IF NOT EXISTS accepted_for_history BOOLEAN NOT NULL DEFAULT FALSE
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS app_meta (
                    meta_key TEXT PRIMARY KEY,
                    meta_value TEXT NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS order_statuses (
                    order_id_text TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    admin_message_chat_id BIGINT NULL,
                    admin_message_id BIGINT NULL,
                    client_chat_id BIGINT NULL,
                    accepted_at TIMESTAMP NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                ALTER TABLE order_statuses
                ADD COLUMN IF NOT EXISTS accepted_at TIMESTAMP NULL
            """)
            cur.execute("""
                ALTER TABLE order_statuses
                ADD COLUMN IF NOT EXISTS client_chat_id BIGINT NULL
            """)


def pg_ensure_user_row(user_id: int):
    with pg_get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO users (user_id)
                VALUES (%s)
                ON CONFLICT (user_id) DO NOTHING
            """, (user_id,))


# =========================
# УНИВЕРСАЛЬНЫЙ СЛОЙ ДАННЫХ
# =========================
def ukey(user_id):
    return str(user_id)


def normalize_phone(phone):
    phone = str(phone).strip()
    phone = re.sub(r"[^\d+]", "", phone)

    if phone.startswith("8") and len(phone) == 11:
        phone = "+7" + phone[1:]
    elif phone.startswith("7") and len(phone) == 11:
        phone = "+" + phone

    if not re.fullmatch(r"\+7\d{10}", phone):
        return None

    return phone


def normalize_payment_choice(text):
    if not text:
        return None
    value = text.strip().lower()
    value = value.replace("ё", "е")
    value = re.sub(r"[^a-zа-я0-9+ ]", " ", value)
    value = re.sub(r"\s+", " ", value).strip()

    card_keywords = {
        "картой", "карта", "по карте", "терминал"
    }
    transfer_keywords = {
        "перевод", "переводом", "сбп", "онлайн", "онлайном", "безнал", "безналом"
    }
    cash_keywords = {
        "наличными", "наличные", "нал", "наличка", "налом"
    }

    if value in card_keywords:
        return "Картой"
    if value in transfer_keywords:
        return "Переводом"
    if value in cash_keywords:
        return "Наличными"

    if "перевод" in value or "сбп" in value or "безнал" in value or "онлайн" in value:
        return "Переводом"
    if "карт" in value or "терминал" in value:
        return "Картой"
    if "нал" in value:
        return "Наличными"

    return None


def escape_callback_text(text):
    return text.replace("|", "¦")


def unescape_callback_text(text):
    return text.replace("¦", "|")


def extract_phone_from_text(text):
    if not text:
        return PHONE_NUMBER
    match = re.search(r"📞 Телефон: (\+7\d{10})", text)
    return match.group(1) if match else PHONE_NUMBER


# ---------- user state ----------
def get_user_state(user_id):
    if USE_POSTGRES:
        pg_ensure_user_row(user_id)
        with pg_get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT state FROM users WHERE user_id = %s", (user_id,))
                row = cur.fetchone()
                return row["state"] if row else STATE_NONE
    return storage["user_state"].get(ukey(user_id), STATE_NONE)


def set_user_state(user_id, state):
    if USE_POSTGRES:
        pg_ensure_user_row(user_id)
        with pg_get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE users
                    SET state = %s, updated_at = NOW()
                    WHERE user_id = %s
                """, (state, user_id))
        return
    storage["user_state"][ukey(user_id)] = state
    save_json_storage()


# ---------- user data ----------
def get_user_data(user_id):
    if USE_POSTGRES:
        pg_ensure_user_row(user_id)
        with pg_get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT data_json FROM users WHERE user_id = %s", (user_id,))
                row = cur.fetchone()
                return dict(row["data_json"]) if row and row["data_json"] else {}
    key = ukey(user_id)
    if key not in storage["user_data"] or not isinstance(storage["user_data"][key], dict):
        storage["user_data"][key] = {}
    return storage["user_data"][key]


def save_user_data(user_id, data):
    if USE_POSTGRES:
        pg_ensure_user_row(user_id)
        with pg_get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE users
                    SET data_json = %s::jsonb, updated_at = NOW()
                    WHERE user_id = %s
                """, (json.dumps(data, ensure_ascii=False), user_id))
        return
    storage["user_data"][ukey(user_id)] = data
    save_json_storage()


def append_undo_action(user_id, action):
    user_data = get_user_data(user_id)
    stack = user_data.get("undo_stack", [])
    if not isinstance(stack, list):
        stack = []
    stack.append(action)
    user_data["undo_stack"] = stack[-30:]
    save_user_data(user_id, user_data)


def push_undo_restore(user_id):
    current_data = copy.deepcopy(get_user_data(user_id))
    current_data.pop("undo_stack", None)
    append_undo_action(user_id, {
        "type": "restore_flow",
        "prev_state": get_user_state(user_id),
        "prev_data": current_data,
    })


def push_undo_add_item(user_id, item_name, qty=1):
    append_undo_action(user_id, {
        "type": "add_item",
        "item_name": item_name,
        "qty": int(qty),
    })


def apply_cancel_last_action(chat_id, user_id):
    user_data = get_user_data(user_id)
    stack = user_data.get("undo_stack", [])
    if not isinstance(stack, list) or not stack:
        safe_send_message(chat_id, "Отменять нечего.", reply_markup=get_main_keyboard())
        return

    action = stack.pop()
    user_data["undo_stack"] = stack
    save_user_data(user_id, user_data)

    if action.get("type") == "add_item":
        item_name = action.get("item_name")
        qty_to_remove = int(action.get("qty", 1))
        cart = get_user_cart(user_id)
        if item_name and item_name in cart:
            new_qty = int(cart[item_name]["qty"]) - qty_to_remove
            set_cart_item_qty(user_id, item_name, new_qty)
        refresh_preview_after_cart_change(chat_id, user_id, f"Отмена: {item_name}")
        safe_send_message(chat_id, "❌ Последнее действие отменено.", reply_markup=get_main_keyboard())
        return

    if action.get("type") == "restore_flow":
        prev_data = action.get("prev_data", {})
        prev_data["undo_stack"] = stack
        save_user_data(user_id, prev_data)
        set_user_state(user_id, action.get("prev_state", STATE_NONE))
        safe_send_message(chat_id, "❌ Последнее действие отменено.", reply_markup=get_main_keyboard())
        return

    safe_send_message(chat_id, "Отменять нечего.", reply_markup=get_main_keyboard())


def reset_user_flow(user_id):
    if USE_POSTGRES:
        pg_ensure_user_row(user_id)
        with pg_get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE users
                    SET state = %s,
                        data_json = '{}'::jsonb,
                        updated_at = NOW()
                    WHERE user_id = %s
                """, (STATE_NONE, user_id))
        return
    storage["user_state"][ukey(user_id)] = STATE_NONE
    storage["user_data"][ukey(user_id)] = {}
    save_json_storage()


# ---------- preview ----------
def get_preview_message_id(user_id):
    if USE_POSTGRES:
        pg_ensure_user_row(user_id)
        with pg_get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT preview_message_id FROM users WHERE user_id = %s", (user_id,))
                row = cur.fetchone()
                return row["preview_message_id"] if row else None
    return storage["preview_message_ids"].get(ukey(user_id))


def set_preview_message_id(user_id, message_id):
    if USE_POSTGRES:
        pg_ensure_user_row(user_id)
        with pg_get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE users
                    SET preview_message_id = %s, updated_at = NOW()
                    WHERE user_id = %s
                """, (message_id, user_id))
        return
    storage["preview_message_ids"][ukey(user_id)] = message_id
    save_json_storage()


def clear_preview_message_id(user_id):
    if USE_POSTGRES:
        pg_ensure_user_row(user_id)
        with pg_get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE users
                    SET preview_message_id = NULL, updated_at = NOW()
                    WHERE user_id = %s
                """, (user_id,))
        return
    storage["preview_message_ids"].pop(ukey(user_id), None)
    save_json_storage()


# ---------- cart ----------
def get_user_cart(user_id):
    if USE_POSTGRES:
        with pg_get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT item_name, price, qty
                    FROM carts
                    WHERE user_id = %s
                    ORDER BY item_name
                """, (user_id,))
                rows = cur.fetchall()
        cart = {}
        for row in rows:
            cart[row["item_name"]] = {
                "price": int(row["price"]),
                "qty": int(row["qty"]),
            }
        return cart

    key = ukey(user_id)
    if key not in storage["user_cart"] or not isinstance(storage["user_cart"][key], dict):
        storage["user_cart"][key] = {}
    return storage["user_cart"][key]


def add_to_cart(user_id, item_name, price, qty=1):
    if USE_POSTGRES:
        with pg_get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO carts (user_id, item_name, price, qty)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (user_id, item_name)
                    DO UPDATE SET
                        qty = carts.qty + EXCLUDED.qty,
                        price = EXCLUDED.price
                """, (user_id, item_name, int(price), int(qty)))
        return

    cart = get_user_cart(user_id)
    if item_name not in cart:
        cart[item_name] = {"price": int(price), "qty": 0}
    cart[item_name]["qty"] += int(qty)
    save_json_storage()


def set_cart_item_qty(user_id, item_name, qty):
    if USE_POSTGRES:
        if qty <= 0:
            delete_cart_item(user_id, item_name)
            return
        with pg_get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE carts
                    SET qty = %s
                    WHERE user_id = %s AND item_name = %s
                """, (int(qty), user_id, item_name))
        return

    cart = get_user_cart(user_id)
    if qty <= 0:
        cart.pop(item_name, None)
    else:
        if item_name in cart:
            cart[item_name]["qty"] = int(qty)
    save_json_storage()


def delete_cart_item(user_id, item_name):
    if USE_POSTGRES:
        with pg_get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM carts
                    WHERE user_id = %s AND item_name = %s
                """, (user_id, item_name))
        return

    cart = get_user_cart(user_id)
    cart.pop(item_name, None)
    save_json_storage()


def clear_user_cart(user_id):
    if USE_POSTGRES:
        with pg_get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM carts WHERE user_id = %s", (user_id,))
        return

    storage["user_cart"][ukey(user_id)] = {}
    save_json_storage()


def get_cart_total(user_id):
    cart = get_user_cart(user_id)
    total_qty = 0
    total_sum = 0
    for item_data in cart.values():
        qty = int(item_data["qty"])
        price = int(item_data["price"])
        total_qty += qty
        total_sum += qty * price
    return total_qty, total_sum


# ---------- orders ----------
def get_next_order_id():
    now = datetime.now()
    today_key = now.strftime("%Y%m%d")
    today_view = now.strftime("%d-%m-%y")

    if USE_POSTGRES:
        with pg_get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO order_counters (order_date_key, counter_value)
                    VALUES (%s, 1)
                    ON CONFLICT (order_date_key)
                    DO UPDATE SET counter_value = order_counters.counter_value + 1
                    RETURNING counter_value
                """, (today_key,))
                new_counter = int(cur.fetchone()[0])
        return f"дата {today_view}  #заказа {new_counter:04d}"

    counters = storage["order_counters_by_date"]
    counters[today_key] = int(counters.get(today_key, 0)) + 1
    save_json_storage()
    return f"дата {today_view}  #заказа {counters[today_key]:04d}"



def create_pg_order(user_id, total, payment, address, locality, comment, cart):
    now = datetime.now()
    today_view = now.strftime("%d-%m-%y")

    with pg_get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                INSERT INTO orders (
                    user_id,
                    order_id_text,
                    total,
                    payment,
                    address,
                    locality,
                    comment,
                    cart_json,
                    accepted_for_history
                )
                VALUES (%s, '', %s, %s, %s, %s, %s, %s::jsonb, FALSE)
                RETURNING id
            """, (
                user_id,
                int(total),
                payment,
                address,
                locality,
                comment,
                json.dumps(cart, ensure_ascii=False),
            ))
            row = cur.fetchone()
            order_db_id = int(row["id"])
            order_id_text = f"дата {today_view}  #заказа {order_db_id:04d}"
            cur.execute(
                "UPDATE orders SET order_id_text = %s WHERE id = %s",
                (order_id_text, order_db_id)
            )
    return order_id_text, order_db_id


def save_order_history(user_id, order_record):
    if USE_POSTGRES:
        with pg_get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO orders (
                        user_id,
                        order_id_text,
                        total,
                        payment,
                        address,
                        locality,
                        comment,
                        cart_json,
                        accepted_for_history
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                """, (
                    user_id,
                    order_record["order_id"],
                    int(order_record["total"]),
                    order_record["payment"],
                    order_record["address"],
                    order_record["locality"],
                    order_record["comment"],
                    json.dumps(order_record["cart"], ensure_ascii=False),
                    bool(order_record.get("accepted_for_history", False)),
                ))
        return

    key = ukey(user_id)
    if key not in storage["order_history"] or not isinstance(storage["order_history"][key], list):
        storage["order_history"][key] = []
    storage["order_history"][key].append(order_record)
    storage["order_history"][key] = storage["order_history"][key][-20:]
    save_json_storage()




def mark_order_accepted_for_history(user_id, order_id):
    if USE_POSTGRES:
        with pg_get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE orders
                    SET accepted_for_history = TRUE
                    WHERE order_id_text = %s
                """, (order_id,))
        return

    history = storage.get("order_history", {}).get(ukey(user_id), [])
    for order in history:
        if isinstance(order, dict) and order.get("order_id") == order_id:
            order["accepted_for_history"] = True
    save_json_storage()


def get_user_orders(user_id, limit=5):
    if USE_POSTGRES:
        with pg_get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT order_id_text, total, payment, address, locality, comment, cart_json, created_at
                    FROM (
                        SELECT
                            o.order_id_text,
                            o.total,
                            o.payment,
                            o.address,
                            o.locality,
                            o.comment,
                            o.cart_json,
                            o.created_at
                        FROM orders o
                        WHERE o.user_id = %s
                          AND o.accepted_for_history = TRUE
                        ORDER BY o.created_at DESC
                        LIMIT %s
                    ) accepted_orders
                    ORDER BY created_at ASC
                """, (user_id, limit))
                rows = cur.fetchall()

        result = []
        for row in rows:
            result.append({
                "order_id": row["order_id_text"],
                "total": int(row["total"]),
                "payment": row["payment"],
                "address": row["address"],
                "locality": row["locality"],
                "comment": row["comment"] or "",
                "cart": row["cart_json"] or {},
                "created_at": row["created_at"].strftime("%Y-%m-%d %H:%M:%S"),
            })
        return result

    history = storage["order_history"].get(ukey(user_id), [])
    accepted_history = [
        order for order in history
        if bool(order.get("accepted_for_history", False))
    ]
    return accepted_history[-limit:]


def get_last_user_order(user_id):
    if USE_POSTGRES:
        with pg_get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT
                        o.order_id_text,
                        o.total,
                        o.payment,
                        o.address,
                        o.locality,
                        o.comment,
                        o.cart_json,
                        o.created_at
                    FROM orders o
                    WHERE o.user_id = %s
                      AND o.accepted_for_history = TRUE
                    ORDER BY o.created_at DESC
                    LIMIT 1
                """, (user_id,))
                row = cur.fetchone()

        if not row:
            return None

        return {
            "order_id": row["order_id_text"],
            "total": int(row["total"]),
            "payment": row["payment"],
            "address": row["address"],
            "locality": row["locality"],
            "comment": row["comment"] or "",
            "cart": row["cart_json"] or {},
            "created_at": row["created_at"].strftime("%Y-%m-%d %H:%M:%S"),
        }

    history = storage["order_history"].get(ukey(user_id), [])
    accepted_history = [
        order for order in history
        if bool(order.get("accepted_for_history", False))
    ]
    return accepted_history[-1] if accepted_history else None


# ---------- meta ----------
def get_meta_value(key, default=""):

    if USE_POSTGRES:
        with pg_get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT meta_value FROM app_meta WHERE meta_key = %s", (key,))
                row = cur.fetchone()
                return row["meta_value"] if row else default
    return storage.get(key, default)


def set_meta_value(key, value):
    if USE_POSTGRES:
        with pg_get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO app_meta (meta_key, meta_value)
                    VALUES (%s, %s)
                    ON CONFLICT (meta_key)
                    DO UPDATE SET meta_value = EXCLUDED.meta_value
                """, (key, value))
        return
    storage[key] = value
    save_json_storage()


def get_order_meta(order_id):
    meta_allowed_keys = {"admin_base_text", "phone"}

    if USE_POSTGRES:
        result = {}
        with pg_get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT status, admin_message_chat_id, admin_message_id, client_chat_id, accepted_at
                    FROM order_statuses
                    WHERE order_id_text = %s
                """, (order_id,))
                row = cur.fetchone()
                if row:
                    result.update({
                        "status": row["status"],
                        "admin_message_chat_id": row["admin_message_chat_id"],
                        "admin_message_id": row["admin_message_id"],
                        "client_chat_id": row.get("client_chat_id"),
                        "accepted_at": row["accepted_at"].strftime("%Y-%m-%d %H:%M:%S") if row.get("accepted_at") else None,
                    })

                cur.execute("SELECT meta_value FROM app_meta WHERE meta_key = %s", (f"order_meta::{order_id}",))
                meta_row = cur.fetchone()
                if meta_row and meta_row.get("meta_value"):
                    try:
                        stored_meta = json.loads(meta_row["meta_value"])
                        if isinstance(stored_meta, dict):
                            filtered_meta = {
                                key: value
                                for key, value in stored_meta.items()
                                if key in meta_allowed_keys
                            }
                            result.update(filtered_meta)
                    except Exception:
                        pass
        return result

    current = storage.get("admin_order_meta", {}).get(order_id, {}).copy()
    if not isinstance(current, dict):
        return {}
    return {
        key: value
        for key, value in current.items()
        if key in {"status", "admin_message_chat_id", "admin_message_id", "client_chat_id", "accepted_at", "admin_base_text", "phone"}
    }


def save_order_meta(order_id, meta):
    meta_allowed_keys = {"admin_base_text", "phone"}
    filtered_meta = {}
    if isinstance(meta, dict):
        filtered_meta = {
            key: value
            for key, value in meta.items()
            if key in meta_allowed_keys
        }

    if USE_POSTGRES:
        existing = get_order_meta(order_id)
        merged = {}
        if isinstance(existing, dict):
            merged.update({
                key: value
                for key, value in existing.items()
                if key in meta_allowed_keys
            })
        merged.update(filtered_meta)
        with pg_get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO app_meta (meta_key, meta_value)
                    VALUES (%s, %s)
                    ON CONFLICT (meta_key)
                    DO UPDATE SET meta_value = EXCLUDED.meta_value
                """, (f"order_meta::{order_id}", json.dumps(merged, ensure_ascii=False)))
        return
    storage.setdefault("admin_order_meta", {})
    current = storage["admin_order_meta"].get(order_id, {})
    if not isinstance(current, dict):
        current = {}
    current = {
        key: value
        for key, value in current.items()
        if key in meta_allowed_keys or key in {"status", "admin_message_chat_id", "admin_message_id", "client_chat_id", "accepted_at"}
    }
    current.update(filtered_meta)
    storage["admin_order_meta"][order_id] = current
    save_json_storage()


def format_status_time(value):
    if not value:
        return ""
    if isinstance(value, str):
        try:
            value = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return ""
    return value.strftime("%d.%m | %H:%M")


def extract_order_number(order_id):
    match = re.search(r"#заказа\s*(\d+)", order_id or "")
    return match.group(1) if match else (order_id or "")


def get_moscow_now():
    return datetime.now(MOSCOW_TZ)


def get_stale_threshold():
    return get_moscow_now().replace(tzinfo=None) - timedelta(minutes=ORDER_STALE_MINUTES)


def mark_order_stale_if_needed(order_id, chat_id=None, message_id=None, client_chat_id=None):
    meta = get_order_meta(order_id)
    status = get_order_status(order_id)
    accepted_at_raw = meta.get("accepted_at")
    if status != "accepted" or not accepted_at_raw:
        return False

    if isinstance(accepted_at_raw, str):
        try:
            accepted_at = datetime.strptime(accepted_at_raw, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return False
    else:
        accepted_at = accepted_at_raw

    if accepted_at > get_stale_threshold():
        return False

    admin_chat_id = chat_id or meta.get("admin_message_chat_id")
    admin_message_id = message_id or meta.get("admin_message_id")
    client_chat_id = client_chat_id or meta.get("client_chat_id")
    base_text = meta.get("admin_base_text", "")
    phone = meta.get("phone") or extract_phone_from_text(base_text)

    if not admin_chat_id or not admin_message_id or not base_text:
        return False

    ok = safe_edit_message_text(
        admin_chat_id,
        admin_message_id,
        build_admin_order_text(base_text, "stale", accepted_at),
        reply_markup=build_admin_status_keyboard(order_id, client_chat_id or admin_chat_id, phone, "stale"),
        disable_web_page_preview=True
    )
    if ok:
        set_order_status(order_id, "stale", admin_chat_id, admin_message_id, client_chat_id)
    return ok


def stale_orders_watcher():
    while True:
        try:
            if USE_POSTGRES:
                threshold = get_stale_threshold()
                with pg_get_conn() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("""
                            SELECT order_id_text, admin_message_chat_id, admin_message_id, client_chat_id
                            FROM order_statuses
                            WHERE status = 'accepted'
                              AND accepted_at IS NOT NULL
                              AND accepted_at <= %s
                              AND admin_message_chat_id IS NOT NULL
                              AND admin_message_id IS NOT NULL
                        """, (threshold,))
                        rows = cur.fetchall()
                for row in rows:
                    mark_order_stale_if_needed(
                        row["order_id_text"],
                        row["admin_message_chat_id"],
                        row["admin_message_id"],
                        row.get("client_chat_id")
                    )
            else:
                for order_id, meta in list(storage.get("admin_order_meta", {}).items()):
                    mark_order_stale_if_needed(
                        order_id,
                        meta.get("admin_message_chat_id"),
                        meta.get("admin_message_id"),
                        meta.get("client_chat_id")
                    )
        except Exception:
            logger.exception("stale_orders_watcher failed")
        time.sleep(30)


def get_order_status(order_id):
    if USE_POSTGRES:
        with pg_get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT status FROM order_statuses WHERE order_id_text = %s", (order_id,))
                row = cur.fetchone()
                return row["status"] if row else "new"
    return storage["admin_order_statuses"].get(order_id, "new")


def set_order_status(order_id, status, admin_message_chat_id=None, admin_message_id=None, client_chat_id=None):
    accepted_at_value = None
    if status == "accepted":
        accepted_at_value = get_moscow_now().replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")

    if USE_POSTGRES:
        with pg_get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO order_statuses (order_id_text, status, admin_message_chat_id, admin_message_id, client_chat_id, accepted_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (order_id_text)
                    DO UPDATE SET
                        status = EXCLUDED.status,
                        admin_message_chat_id = COALESCE(EXCLUDED.admin_message_chat_id, order_statuses.admin_message_chat_id),
                        admin_message_id = COALESCE(EXCLUDED.admin_message_id, order_statuses.admin_message_id),
                        client_chat_id = COALESCE(EXCLUDED.client_chat_id, order_statuses.client_chat_id),
                        accepted_at = CASE
                            WHEN EXCLUDED.status = 'accepted' THEN EXCLUDED.accepted_at
                            ELSE order_statuses.accepted_at
                        END,
                        updated_at = NOW()
                """, (order_id, status, admin_message_chat_id, admin_message_id, client_chat_id, accepted_at_value))
        return

    storage["admin_order_statuses"][order_id] = status
    meta = storage.setdefault("admin_order_meta", {}).get(order_id, {})
    if admin_message_chat_id is not None:
        meta["admin_message_chat_id"] = admin_message_chat_id
    if admin_message_id is not None:
        meta["admin_message_id"] = admin_message_id
    if client_chat_id is not None:
        meta["client_chat_id"] = client_chat_id
    if accepted_at_value:
        meta["accepted_at"] = accepted_at_value
    storage["admin_order_meta"][order_id] = meta

    for history_items in storage.get("order_history", {}).values():
        if isinstance(history_items, list):
            for order in history_items:
                if isinstance(order, dict) and order.get("order_id") == order_id:
                    order["status"] = status
    save_json_storage()


def build_admin_status_keyboard(order_id, client_chat_id, phone, status):
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("📞 Позвонить клиенту", callback_data=f"call_{phone}"),
        types.InlineKeyboardButton("💬 Написать в Telegram", url=f"tg://user?id={client_chat_id}")
    )

    if status == "new":
        kb.row(
            types.InlineKeyboardButton("✅ Принят", callback_data=f"admin_accept|{order_id}|{client_chat_id}"),
            types.InlineKeyboardButton("🚚 Уже в пути", callback_data=f"admin_way|{order_id}|{client_chat_id}")
        )
        kb.add(types.InlineKeyboardButton("❌ Отменить заказ", callback_data=f"admin_cancel|{order_id}|{client_chat_id}"))
        return kb

    if status == "accepted":
        kb.row(
            types.InlineKeyboardButton("✅ Принят •", callback_data="noop"),
            types.InlineKeyboardButton("🚚 Уже в пути", callback_data=f"admin_way|{order_id}|{client_chat_id}")
        )
        kb.add(types.InlineKeyboardButton("❌ Отменить заказ", callback_data=f"admin_cancel|{order_id}|{client_chat_id}"))
        return kb

    if status == "stale":
        kb.row(
            types.InlineKeyboardButton("⛔ Устарел •", callback_data="noop"),
            types.InlineKeyboardButton("🚚 Уже в пути", callback_data=f"admin_way|{order_id}|{client_chat_id}")
        )
        kb.add(types.InlineKeyboardButton("❌ Отменить заказ", callback_data=f"admin_cancel|{order_id}|{client_chat_id}"))
        return kb

    if status == "cancelled":
        kb.row(
            types.InlineKeyboardButton("❌ Заказ отменён •", callback_data="noop")
        )
        return kb

    kb.row(
        types.InlineKeyboardButton("✅ Принят", callback_data=f"admin_accept|{order_id}|{client_chat_id}"),
        types.InlineKeyboardButton("🚚 Уже в пути", callback_data=f"admin_way|{order_id}|{client_chat_id}")
    )
    kb.add(types.InlineKeyboardButton("❌ Отменить заказ", callback_data=f"admin_cancel|{order_id}|{client_chat_id}"))
    return kb


def build_admin_order_text(base_text, status, accepted_at=None):
    if status == "new":
        return f"{base_text}\n\nСтатус: 🕓 Новый заказ"
    if status == "accepted":
        time_line = f"\n🕒 {format_status_time(accepted_at)}" if accepted_at else ""
        return f"{base_text}\n\nСтатус: ✅ Заказ принят{time_line}"
    if status == "stale":
        time_line = f"\n🕒 {format_status_time(accepted_at)}" if accepted_at else ""
        return f"{base_text}\n\nСтатус: ⛔ Заказ устарел{time_line}"
    if status == "cancelled":
        return f"{base_text}\n\nСтатус: ❌ Заказ отменён"
    return f"{base_text}\n\nСтатус: {status}"
def update_admin_order_message(call, order_id, client_chat_id, phone, status):
    current_text = call.message.text or f"📦 ЗАКАЗ #{extract_order_number(order_id)}"
    base_text = current_text.split("\n\nСтатус:")[0]
    accepted_at = get_order_meta(order_id).get("accepted_at")
    new_text = build_admin_order_text(base_text, status, accepted_at)
    ok = safe_edit_message_text(
        call.message.chat.id,
        call.message.message_id,
        new_text,
        reply_markup=build_admin_status_keyboard(order_id, client_chat_id, phone, status),
        disable_web_page_preview=True
    )
    return ok
def format_cart(cart):
    if not cart:
        return "🛒 Корзина пуста."

    total = 0
    lines = ["🛒 Ваша корзина:\n"]
    for item_name, item_data in cart.items():
        qty = int(item_data["qty"])
        price = int(item_data["price"])
        line_total = qty * price
        total += line_total
        lines.append(f"• {item_name} x{qty} — {line_total}₽")

    lines.append(f"\n💰 Итого: {total}₽")
    return "\n".join(lines)


def format_order_history_item(order):
    comment = order.get("comment", "")
    comment_text = f"\n💬 Комментарий: {comment}" if comment else ""

    return (
        "🧾 Заказ\n"
        f"{order['order_id']}\n"
        f"💰 Сумма: {order['total']}₽\n"
        f"📍 Населённый пункт: {order.get('locality', 'Не определён')}\n"
        f"🏠 Адрес: {order.get('address', 'Не указан')}\n"
        f"💳 Оплата: {order.get('payment', 'Не указана')}\n"
        f"{INFO_DELIVERY_TEXT}"
        f"{comment_text}"
    )


def get_item_by_id(category_id, item_id):
    items_list = list(MENU[category_id]["items"].items())
    item_name, price = items_list[int(item_id) - 1]
    return item_name, int(price)


def haversine_distance_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = (
        math.sin(d_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


LOCALITY_ALIASES = {
    "Ялта": ["ялта", "городской округ ялта", "ялтинский район", "ялтинский городской округ"],
    "Гурзуф": ["гурзуф"],
    "Массандра": ["массандра"],
    "Ливадия": ["ливадия"],
    "Ореанда": ["ореанда", "нижняя ореанда", "верхняя ореанда"],
    "Гаспра": ["гаспра"],
    "Кореиз": ["кореиз"],
    "Мисхор": ["мисхор"],
    "Алупка": ["алупка"],
    "Симеиз": ["симеиз"],
}

MANUAL_ADDRESS_PRIORITY = ["Ялта", "Массандра", "Ливадия"]
NEAREST_LOCALITY_RADIUS_KM = 18


def normalize_text_for_match(text):
    value = (text or "").lower().replace("ё", "е")
    value = re.sub(r"[^a-zа-я0-9\s,-]", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def detect_locality_from_text(text):
    normalized = normalize_text_for_match(text)
    if not normalized:
        return "Не определён"

    for locality, aliases in LOCALITY_ALIASES.items():
        for alias in aliases:
            if alias in normalized:
                return locality

    return "Не определён"


def get_nearest_locality(lat, lon):
    nearest_name = "Не определён"
    nearest_distance = None
    for name, (loc_lat, loc_lon) in LOCALITIES.items():
        distance = haversine_distance_km(lat, lon, loc_lat, loc_lon)
        if nearest_distance is None or distance < nearest_distance:
            nearest_distance = distance
            nearest_name = name
    return nearest_name, nearest_distance


def build_locality_search_order(explicit_locality=None):
    ordered = []
    if explicit_locality and explicit_locality in LOCALITIES:
        ordered.append(explicit_locality)
    for locality in MANUAL_ADDRESS_PRIORITY:
        if locality not in ordered:
            ordered.append(locality)
    for locality in LOCALITIES.keys():
        if locality not in ordered:
            ordered.append(locality)
    return ordered


def geocode_address_with_locality(address_text, locality):
    query_text = f"{locality}, Крым, {address_text}"
    try:
        url = "https://nominatim.openstreetmap.org/search"
        params = {
            "format": "json",
            "q": query_text,
            "limit": 1,
            "addressdetails": 1,
        }
        headers = {"User-Agent": "telegram-food-bot"}
        response = requests.get(url, params=params, headers=headers, timeout=6)
        response.raise_for_status()
        data = response.json()

        if isinstance(data, list) and data:
            row = data[0]
            lat = row.get("lat")
            lon = row.get("lon")
            if lat and lon:
                return {
                    "locality": locality,
                    "lat": float(lat),
                    "lon": float(lon),
                    "map_link": f"https://yandex.ru/maps/?pt={lon},{lat}&z=17&l=map",
                    "display_name": row.get("display_name", ""),
                }
    except Exception:
        pass
    return None


def build_short_address(address_data, detected_locality, map_link, display_name):
    road = address_data.get("road", "")
    house_number = address_data.get("house_number", "")
    pedestrian = address_data.get("pedestrian", "")
    footway = address_data.get("footway", "")

    street_name = road or pedestrian or footway
    parts = []

    if street_name:
        if house_number:
            parts.append(f"{street_name}, {house_number}")
        else:
            parts.append(street_name)

    if detected_locality != "Не определён" and detected_locality not in parts:
        parts.append(detected_locality)

    if parts:
        return ", ".join(parts)

    if display_name:
        return display_name

    return f"{address_data.get('state', '')}, {address_data.get('county', '')}".strip(", ").strip() or "Адрес не определён"


def extract_clean_address_for_maps(address_text):
    value = (address_text or "").strip()
    if not value:
        return ""

    value = value.replace("\n", " ")
    value = re.sub(r"\s+", " ", value).strip(" ,.;")

    detail_patterns = [
        r"\bпод[ъь]езд\b",
        r"\bэтаж\b",
        r"\bквартир[аыеу]?\b",
        r"\bкв\.?\b",
        r"\bдомофон\b",
        r"\bориентир\b",
        r"\bкод\b",
        r"\bвход\b",
        r"\bсправа\b",
        r"\bслева\b",
        r"\bнапротив\b",
        r"\bвозле\b",
        r"\bрядом\b",
        r"\bоколо\b",
        r"\bот\b",
    ]

    cut_index = len(value)
    lowered = value.lower().replace("ё", "е")
    for pattern in detail_patterns:
        match = re.search(pattern, lowered, flags=re.IGNORECASE)
        if match:
            cut_index = min(cut_index, match.start())

    cleaned = value[:cut_index].strip(" ,.;") if cut_index < len(value) else value
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,.;")

    return cleaned or value


def build_yandex_maps_link_from_text(address_text, locality="Не определён"):
    clean_address_text = extract_clean_address_for_maps(address_text)
    explicit_locality = locality if locality in LOCALITIES else detect_locality_from_text(clean_address_text)

    for candidate_locality in build_locality_search_order(explicit_locality if explicit_locality != "Не определён" else None):
        match = geocode_address_with_locality(clean_address_text, candidate_locality)
        if match:
            return match["map_link"]

    fallback_query = f"Ялта, Крым, {clean_address_text or address_text}"
    query = quote(fallback_query)
    return f"https://yandex.ru/maps/?text={query}"


def get_address_and_locality_from_coords(lat, lon):
    map_link = f"https://yandex.ru/maps/?pt={lon},{lat}&z=17&l=map"
    try:
        url = (
            "https://nominatim.openstreetmap.org/reverse"
            f"?format=json&lat={lat}&lon={lon}&zoom=18&addressdetails=1"
        )
        headers = {"User-Agent": "telegram-food-bot"}
        response = requests.get(url, headers=headers, timeout=6)
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise ValueError("Некорректный ответ геокодера")

        display_name = data.get("display_name", "")
        address_data = data.get("address", {})

        raw_text = " ".join([
            display_name,
            address_data.get("city", ""),
            address_data.get("town", ""),
            address_data.get("village", ""),
            address_data.get("municipality", ""),
            address_data.get("suburb", ""),
            address_data.get("neighbourhood", ""),
            address_data.get("county", ""),
            address_data.get("state", ""),
            address_data.get("road", ""),
            address_data.get("house_number", ""),
        ])

        detected_locality = detect_locality_from_text(raw_text)

        if detected_locality == "Не определён":
            nearest_locality, nearest_distance = get_nearest_locality(lat, lon)
            if nearest_distance is not None and nearest_distance <= NEAREST_LOCALITY_RADIUS_KM:
                detected_locality = nearest_locality

        short_address = build_short_address(address_data, detected_locality, map_link, display_name)

        has_street = bool(
            address_data.get("road")
            or address_data.get("pedestrian")
            or address_data.get("footway")
        )
        has_house = bool(address_data.get("house_number"))
        needs_manual_address = not (has_street and has_house)

        return short_address, detected_locality, map_link, needs_manual_address

    except Exception:
        nearest_locality, nearest_distance = get_nearest_locality(lat, lon)
        locality = nearest_locality if nearest_distance is not None and nearest_distance <= NEAREST_LOCALITY_RADIUS_KM else "Не определён"
        fallback_address = f"{lat}, {lon}"
        return fallback_address, locality, map_link, True


def safe_send_message(chat_id, text, **kwargs):
    try:
        return bot.send_message(chat_id, text, **kwargs)
    except Exception:
        logger.exception("send_message failed chat_id=%s", chat_id)
        return None


def safe_delete_message(chat_id, message_id):
    try:
        bot.delete_message(chat_id, message_id)
        return True
    except Exception:
        return False


def safe_edit_message_text(chat_id, message_id, text, **kwargs):
    try:
        bot.edit_message_text(text, chat_id, message_id, **kwargs)
        return True
    except Exception as e:
        error_text = str(e).lower()
        if "message is not modified" in error_text:
            return True
        if "message to edit not found" in error_text:
            return False
        logger.exception("edit_message_text failed chat_id=%s message_id=%s", chat_id, message_id)
        return False



# =========================
# КЛАВИАТУРЫ
# =========================
def get_main_keyboard():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("📋 Меню", "🛒 Корзина")
    kb.row("✅ Оформить заказ", "🗑 Очистить корзину")
    kb.row("🔁 Повторить прошлый заказ", "📦 Мои заказы")
    kb.row("⭐ Отзывы", "🔄 Начать заново")
    return kb


def get_link_keyboard():
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("📍 Мы на Яндекс Картах", url=YANDEX_LINK))
    return kb


def get_reviews_keyboard():
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("⭐ Смотреть / оставить отзыв на Яндекс", url=YANDEX_REVIEWS_LINK))
    return kb


def get_group_buttons_keyboard():
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton(
        "🍔 Оформить заказ через бот",
        url=f"https://t.me/{BOT_USERNAME}"
    ))
    return kb


def get_categories_keyboard():
    kb = types.InlineKeyboardMarkup()
    for category_id, category_data in MENU.items():
        kb.add(types.InlineKeyboardButton(
            category_data["title"],
            callback_data=f"cat|{category_id}"
        ))
    kb.add(types.InlineKeyboardButton("🛒 Корзина", callback_data="cart"))
    return kb


def get_items_keyboard(category_id):
    kb = types.InlineKeyboardMarkup()
    for item_id, (item_name, price) in enumerate(MENU[category_id]["items"].items(), start=1):
        kb.add(types.InlineKeyboardButton(
            f"{item_name} — {price}₽",
            callback_data=f"add|{category_id}|{item_id}"
        ))
    kb.row(
        types.InlineKeyboardButton("⬅️ Категории", callback_data="back_categories"),
        types.InlineKeyboardButton("🛒 Корзина", callback_data="cart")
    )
    return kb


def get_sauces_keyboard(category_id):
    kb = types.InlineKeyboardMarkup()
    for sauce_name, sauce_price in SAUCES.items():
        safe_name = escape_callback_text(sauce_name)
        kb.add(types.InlineKeyboardButton(
            f"{sauce_name} — {sauce_price}₽",
            callback_data=f"sauce|{category_id}|{safe_name}"
        ))
    kb.add(types.InlineKeyboardButton("Без соуса", callback_data=f"sauce_none|{category_id}"))
    kb.row(
        types.InlineKeyboardButton("⬅️ Назад", callback_data=f"cat|{category_id}"),
        types.InlineKeyboardButton("🛒 Корзина", callback_data="cart")
    )
    return kb


def get_maki_fish_keyboard(category_id):
    kb = types.InlineKeyboardMarkup()
    for item_name, price in MAKI_FISH_OPTIONS.items():
        safe_name = escape_callback_text(item_name)
        kb.add(types.InlineKeyboardButton(
            f"{item_name} — {price}₽",
            callback_data=f"maki_fish|{category_id}|{safe_name}"
        ))
    kb.row(
        types.InlineKeyboardButton("⬅️ Назад", callback_data=f"cat|{category_id}"),
        types.InlineKeyboardButton("🛒 Корзина", callback_data="cart")
    )
    return kb


def get_chuka_offer_keyboard():
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton(f"➕ Добавить {CHUKA_NAME} — {CHUKA_PRICE}₽", callback_data="chuka_add"),
        types.InlineKeyboardButton("➡️ Продолжить без чуки", callback_data="chuka_skip")
    )
    return kb


def get_drinks_offer_keyboard():
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("🥤 Выбрать напиток", callback_data="drinks_offer_open"),
        types.InlineKeyboardButton("➡️ Без напитка", callback_data="drinks_offer_skip")
    )
    return kb


def get_address_choice_keyboard():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    location_btn = types.KeyboardButton("📍 Отправить геолокацию", request_location=True)
    kb.row(location_btn)
    kb.row("✏️ Ввести вручную")
    return kb


def get_phone_keyboard():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    contact_btn = types.KeyboardButton("📞 Отправить номер", request_contact=True)
    kb.row(contact_btn)
    return kb


def get_payment_keyboard():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("Картой", "Наличными")
    kb.row("Переводом")
    return kb


def get_comment_skip_keyboard():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("Пропустить")
    return kb


def get_preview_keyboard():
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("🛒 Открыть корзину", callback_data="cart"),
        types.InlineKeyboardButton("📋 Категории", callback_data="back_categories")
    )
    return kb


def get_cart_item_name_by_index(user_id, index):
    cart = get_user_cart(user_id)
    item_names = list(cart.keys())
    if 0 <= index < len(item_names):
        return item_names[index]
    return None


def get_cart_keyboard(user_id):
    kb = types.InlineKeyboardMarkup(row_width=4)
    cart = get_user_cart(user_id)

    if not cart:
        kb.add(types.InlineKeyboardButton("📋 К категориям", callback_data="back_categories"))
        return kb

    for idx, (item_name, item_data) in enumerate(cart.items()):
        qty = int(item_data["qty"])
        line_total = qty * int(item_data["price"])

        kb.row(
            types.InlineKeyboardButton(
                f"{item_name} x{qty} — {line_total}₽",
                callback_data=f"cart_line|{idx}"
            ),
            types.InlineKeyboardButton("➖", callback_data=f"cart_dec|{idx}"),
            types.InlineKeyboardButton("➕", callback_data=f"cart_inc|{idx}"),
            types.InlineKeyboardButton("✖", callback_data=f"cart_del|{idx}")
        )

    kb.add(types.InlineKeyboardButton("✅ Оформить заказ", callback_data="checkout"))
    kb.add(types.InlineKeyboardButton("🗑 Очистить корзину", callback_data="clear_cart_inline"))
    kb.add(types.InlineKeyboardButton("📋 К категориям", callback_data="back_categories"))
    return kb


def get_confirm_order_keyboard():
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("✅ Подтвердить заказ", callback_data="confirm_order"),
        types.InlineKeyboardButton("✏️ Вернуться в корзину", callback_data="return_to_cart")
    )
    return kb


def get_comment_confirm_keyboard():
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("✏️ Изменить комментарий", callback_data="edit_comment"),
        types.InlineKeyboardButton("➡️ Дальше", callback_data="comment_next")
    )
    return kb


def get_admin_order_keyboard(order_id, client_chat_id, phone):
    return build_admin_status_keyboard(order_id, client_chat_id, phone, get_order_status(order_id))

def show_or_update_preview(chat_id, user_id, last_added_text="—"):
    total_qty, total_sum = get_cart_total(user_id)

    if total_qty == 0:
        preview_text = "🛒 Предкорзина пуста."
    else:
        preview_text = (
            "🛒 Предкорзина\n\n"
            f"✅ Последнее добавлено: {last_added_text}\n"
            f"📦 Позиций в заказе: {total_qty}\n"
            f"💰 Сумма сейчас: {total_sum}₽"
        )

    preview_message_id = get_preview_message_id(user_id)

    if preview_message_id:
        ok = safe_edit_message_text(
            chat_id,
            preview_message_id,
            preview_text,
            reply_markup=get_preview_keyboard()
        )
        if ok:
            return

        safe_delete_message(chat_id, preview_message_id)
        clear_preview_message_id(user_id)

    msg = safe_send_message(chat_id, preview_text, reply_markup=get_preview_keyboard())
    if msg:
        set_preview_message_id(user_id, msg.message_id)


def clear_preview(chat_id, user_id):
    preview_message_id = get_preview_message_id(user_id)
    if preview_message_id:
        safe_delete_message(chat_id, preview_message_id)
    clear_preview_message_id(user_id)


def refresh_preview_after_cart_change(chat_id, user_id, last_added_text="—"):
    total_qty, _ = get_cart_total(user_id)
    if total_qty <= 0:
        clear_preview(chat_id, user_id)
        return
    show_or_update_preview(chat_id, user_id, last_added_text)


# =========================
# ОТРИСОВКА
# =========================
def show_cart_message(chat_id, user_id):
    safe_send_message(
        chat_id,
        format_cart(get_user_cart(user_id)),
        reply_markup=get_cart_keyboard(user_id)
    )


def edit_category_message(call, category_id):
    category_title = MENU[category_id]["title"]
    ok = safe_edit_message_text(
        call.message.chat.id,
        call.message.message_id,
        f"Категория: {category_title}",
        reply_markup=get_items_keyboard(category_id)
    )
    if not ok:
        safe_send_message(
            call.message.chat.id,
            f"Категория: {category_title}",
            reply_markup=get_items_keyboard(category_id)
        )


def edit_cart_message(call, user_id):
    ok = safe_edit_message_text(
        call.message.chat.id,
        call.message.message_id,
        format_cart(get_user_cart(user_id)),
        reply_markup=get_cart_keyboard(user_id)
    )
    if not ok:
        safe_send_message(
            call.message.chat.id,
            format_cart(get_user_cart(user_id)),
            reply_markup=get_cart_keyboard(user_id)
        )


def cart_needs_chuka_offer(user_id):
    cart = get_user_cart(user_id)
    if CHUKA_NAME in cart:
        return False
    return any(item_name in SUSHI_CART_ITEMS for item_name in cart.keys())


def cart_needs_drinks_offer(user_id):
    cart = get_user_cart(user_id)
    return not any(item_name in DRINK_CART_ITEMS for item_name in cart.keys())


def continue_checkout_after_offers(chat_id, user_id):
    if cart_needs_drinks_offer(user_id):
        user_data = get_user_data(user_id)
        user_data["awaiting_drink_offer_choice"] = True
        save_user_data(user_id, user_data)
        safe_send_message(
            chat_id,
            "🥤 Хотите добавить лимонад к заказу?",
            reply_markup=get_drinks_offer_keyboard()
        )
        return

    user_data = get_user_data(user_id)
    if user_data.get("awaiting_drink_offer_choice"):
        user_data.pop("awaiting_drink_offer_choice", None)
        save_user_data(user_id, user_data)
    start_address_step(chat_id, user_id)


def begin_checkout_flow(chat_id, user_id):
    cart = get_user_cart(user_id)
    if not cart:
        safe_send_message(chat_id, "Корзина пуста.", reply_markup=get_main_keyboard())
        return

    safe_send_message(chat_id, format_cart(cart))

    if cart_needs_chuka_offer(user_id):
        safe_send_message(
            chat_id,
            f"🍣 Хотите добавить {CHUKA_NAME} — {CHUKA_PRICE}₽?",
            reply_markup=get_chuka_offer_keyboard()
        )
        return

    continue_checkout_after_offers(chat_id, user_id)


def start_address_step(chat_id, user_id):
    push_undo_restore(user_id)
    set_user_state(user_id, STATE_WAIT_ADDRESS_CHOICE)
    safe_send_message(
        chat_id,
        f"Выбери способ указать адрес:\n\n{INFO_DELIVERY_TEXT}",
        reply_markup=get_address_choice_keyboard()
    )


def build_confirmation_text(user_id):
    user_data = get_user_data(user_id)
    cart = get_user_cart(user_id)
    total = 0
    items_lines = []

    for item_name, item_data in cart.items():
        qty = int(item_data["qty"])
        price = int(item_data["price"])
        line_total = qty * price
        total += line_total
        items_lines.append(f"• {item_name} x{qty} — {line_total}₽")

    comment = user_data.get("comment", "")
    comment_text = f"\n💬 Комментарий: {comment}" if comment else "\n💬 Комментарий: —"

    return (
        "📋 Проверь заказ перед отправкой:\n\n"
        f"{chr(10).join(items_lines)}\n\n"
        f"📍 Населённый пункт: {user_data.get('locality', 'Не определён')}\n"
        f"🏠 Адрес: {user_data.get('address', 'Не указан')}\n"
        f"📞 Телефон: {user_data.get('phone', 'Не указан')}\n"
        f"💳 Оплата: {user_data.get('payment', 'Не указана')}\n"
        f"{INFO_DELIVERY_TEXT}"
        f"{comment_text}\n"
        f"💰 Итого к оплате: {total}₽"
    )


def build_public_info_text():
    return (
        "🍔 Добро пожаловать!\n"
        "📦 Теперь заказ можно оформить через нашего бота КакМакДак.\n\n"
        "Нажми кнопку ниже, чтобы перейти к оформлению заказа.\n\n"
        f"📞 Телефон для связи: {PHONE_NUMBER}\n"
        f"🕒 Время работы: {WORKING_HOURS}\n\n"
        "💬 По вопросам можно писать прямо в этот чат."
    )


def post_public_info_if_needed():
    today = datetime.now().strftime("%Y-%m-%d")
    last_date = get_meta_value("last_public_post_date", "")
    if last_date == today:
        return

    msg = safe_send_message(
        PUBLIC_GROUP_ID,
        build_public_info_text(),
        reply_markup=get_group_buttons_keyboard()
    )
    if msg:
        set_meta_value("last_public_post_date", today)
        try:
            bot.pin_chat_message(PUBLIC_GROUP_ID, msg.message_id, disable_notification=True)
        except Exception:
            pass


def send_public_post(meta_key):
    today = get_moscow_now().strftime("%Y-%m-%d")
    last_date = get_meta_value(meta_key, "")
    if last_date == today:
        return False

    msg = safe_send_message(
        PUBLIC_GROUP_ID,
        build_public_info_text(),
        reply_markup=get_group_buttons_keyboard()
    )
    if not msg:
        return False

    set_meta_value(meta_key, today)

    try:
        bot.pin_chat_message(PUBLIC_GROUP_ID, msg.message_id, disable_notification=True)
    except Exception:
        pass

    return True


def public_post_scheduler():
    last_checked_minute = None

    while True:
        now = get_moscow_now()
        current_minute = now.strftime("%Y-%m-%d %H:%M")

        if current_minute != last_checked_minute:
            last_checked_minute = current_minute
            if now.hour == PUBLIC_POST_HOUR and now.minute == PUBLIC_POST_MINUTE:
                send_public_post("last_public_scheduled_post_date")

        time.sleep(5)

@bot.message_handler(commands=["start"])
def start(message):
    if message.chat.type != "private":
        return

    user_id = message.from_user.id
    clear_user_cart(user_id)
    clear_preview(message.chat.id, user_id)
    reset_user_flow(user_id)

    welcome_text = (
        "Привет! 👋\n\n"
        "🍔 Чтобы оформить заказ:\n"
        "1. Открой меню\n"
        "2. Добавь блюда в корзину\n"
        "3. Подтверди заказ\n\n"
        "🏠 Адрес вручную лучше писать так:\n"
        "улица, дом, подъезд\n\n"
        f"{INFO_DELIVERY_TEXT}\n\n"
        "💬 Если есть вопросы — пиши в группу, администраторы ответят."
    )

    safe_send_message(message.chat.id, welcome_text, reply_markup=get_main_keyboard())
    safe_send_message(message.chat.id, "📍 Наше местоположение:", reply_markup=get_link_keyboard())


@bot.message_handler(commands=["groupbuttons"])
def send_group_buttons(message):
    if message.chat.type not in ["group", "supergroup"]:
        safe_send_message(message.chat.id, "Команда работает только в группе.")
        return

    msg = safe_send_message(
        message.chat.id,
        build_public_info_text(),
        reply_markup=get_group_buttons_keyboard()
    )
    if msg:
        try:
            bot.pin_chat_message(message.chat.id, msg.message_id, disable_notification=True)
        except Exception:
            pass


@bot.message_handler(commands=["postbuttons"])
def post_buttons_to_group(message):
    if message.chat.type != "private":
        return

    msg = safe_send_message(
        PUBLIC_GROUP_ID,
        build_public_info_text(),
        reply_markup=get_group_buttons_keyboard()
    )

    if msg:
        set_meta_value("last_public_post_date", datetime.now().strftime("%Y-%m-%d"))
        try:
            bot.pin_chat_message(PUBLIC_GROUP_ID, msg.message_id, disable_notification=True)
        except Exception:
            pass
        safe_send_message(message.chat.id, "✅ Сообщение с кнопкой отправлено в группу.")
    else:
        safe_send_message(message.chat.id, "Не удалось отправить сообщение в группу.")


# =========================
# ОБЩАЯ ОТМЕНА / СБРОС
# =========================
@bot.message_handler(func=lambda m: m.chat.type == "private" and m.text == "🔄 Начать заново")
def restart_flow(message):
    user_id = message.from_user.id
    clear_user_cart(user_id)
    clear_preview(message.chat.id, user_id)
    reset_user_flow(user_id)
    user_data = get_user_data(user_id)
    user_data.pop("pending_fryer_item", None)
    user_data.pop("undo_stack", None)
    save_user_data(user_id, user_data)
    safe_send_message(message.chat.id, "🔄 Всё сброшено. Можешь начать заново.", reply_markup=get_main_keyboard())


# =========================
# ОСНОВНЫЕ КНОПКИ
# =========================
@bot.message_handler(func=lambda m: m.chat.type == "private" and m.text == "📋 Меню")
def show_menu(message):
    safe_send_message(message.chat.id, "Выбери категорию:", reply_markup=get_categories_keyboard())


@bot.message_handler(func=lambda m: m.chat.type == "private" and m.text == "⭐ Отзывы")
def show_reviews(message):
    safe_send_message(
        message.chat.id,
        "⭐ Отзывы\nВы можете ознакомиться с отзывами наших гостей на Яндекс.Картах.\nПри желании оставить свой отзыв — будем благодарны.",
        reply_markup=get_reviews_keyboard()
    )


@bot.message_handler(func=lambda m: m.chat.type == "private" and m.text == "🛒 Корзина")
def show_cart(message):
    show_cart_message(message.chat.id, message.from_user.id)


@bot.message_handler(func=lambda m: m.chat.type == "private" and m.text == "🗑 Очистить корзину")
def clear_cart_handler(message):
    user_id = message.from_user.id
    clear_user_cart(user_id)
    clear_preview(message.chat.id, user_id)
    user_data = get_user_data(user_id)
    user_data.pop("pending_fryer_item", None)
    user_data.pop("undo_stack", None)
    save_user_data(user_id, user_data)
    if get_user_state(user_id) != STATE_NONE:
        set_user_state(user_id, STATE_NONE)
    safe_send_message(message.chat.id, "Корзина очищена.", reply_markup=get_main_keyboard())


@bot.message_handler(func=lambda m: m.chat.type == "private" and m.text == "🔁 Повторить прошлый заказ")
def repeat_last_order(message):
    user_id = message.from_user.id
    last_order = get_last_user_order(user_id)

    if not last_order:
        safe_send_message(message.chat.id, "У тебя пока нет принятых прошлых заказов.", reply_markup=get_main_keyboard())
        return

    clear_user_cart(user_id)

    cart = last_order.get("cart", {})
    for item_name, item_data in cart.items():
        add_to_cart(user_id, item_name, int(item_data["price"]), int(item_data["qty"]))

    safe_send_message(message.chat.id, "Последний заказ добавлен в корзину.", reply_markup=get_main_keyboard())
    refresh_preview_after_cart_change(message.chat.id, user_id, "Повтор прошлого заказа")
    show_cart_message(message.chat.id, user_id)


@bot.message_handler(func=lambda m: m.chat.type == "private" and m.text == "📦 Мои заказы")
def my_orders(message):
    history = get_user_orders(message.from_user.id, limit=5)

    if not history:
        safe_send_message(message.chat.id, "У тебя пока нет принятых заказов.", reply_markup=get_main_keyboard())
        return

    texts = [format_order_history_item(order) for order in history]
    safe_send_message(
        message.chat.id,
        "📦 Последние заказы:\n\n" + "\n\n".join(texts),
        reply_markup=get_main_keyboard()
    )


@bot.message_handler(func=lambda m: m.chat.type == "private" and m.text == "✅ Оформить заказ")
def checkout_from_main(message):
    begin_checkout_flow(message.chat.id, message.from_user.id)


def notify_client_about_status(client_chat_id, order_id, status):
    if status == "accepted":
        safe_send_message(client_chat_id, f"✅ Заказ принят\n{order_id}", reply_markup=get_main_keyboard())
        return
    if status == "on_the_way":
        safe_send_message(client_chat_id, f"🚚 Заказ уже в пути\n{order_id}", reply_markup=get_main_keyboard())
        return
    if status == "cancelled":
        safe_send_message(
            client_chat_id,
            f"❌ Заказ отменён\n{order_id}\nСвяжитесь с администратором для уточнения.",
            reply_markup=get_main_keyboard()
        )


def notify_admin_group_about_status(chat_id, order_id, status):
    messages = {
        "accepted": "✅ Статус заказа «Принят» отправлен клиенту.",
        "on_the_way": "🚚 Статус заказа «Уже в пути» отправлен клиенту.",
        "cancelled": "❌ Статус заказа «Отменён» отправлен клиенту.",
    }
    text = messages.get(status)
    if text:
        safe_send_message(chat_id, text)


def apply_admin_status_change(call, order_id, client_chat_id, status_to_store, visible_status=None):
    phone = extract_phone_from_text(call.message.text or "")
    set_order_status(order_id, status_to_store, call.message.chat.id, call.message.message_id, int(client_chat_id))
    if visible_status:
        update_admin_order_message(call, order_id, int(client_chat_id), phone, visible_status)
    notify_client_about_status(int(client_chat_id), order_id, status_to_store)
    notify_admin_group_about_status(call.message.chat.id, order_id, status_to_store)



# =========================
# CALLBACK'И
# =========================
@bot.callback_query_handler(func=lambda call: True)
def callbacks(call):
    user_id = call.from_user.id
    data = call.data

    if data == "edit_comment":
        bot.answer_callback_query(call.id, "Можешь изменить комментарий")
        set_user_state(user_id, STATE_WAIT_COMMENT_OR_SKIP)
        safe_send_message(
            call.message.chat.id,
            "✏️ Введите комментарий заново или нажмите Пропустить:",
            reply_markup=get_comment_skip_keyboard()
        )
        return

    if data == "comment_next":
        bot.answer_callback_query(call.id)
        set_user_state(user_id, STATE_WAIT_CONFIRM)
        safe_send_message(
            call.message.chat.id,
            build_confirmation_text(user_id),
            reply_markup=get_confirm_order_keyboard()
        )
        return

    if data == "noop":
        bot.answer_callback_query(call.id)
        return

    if data.startswith("call_"):
        phone = data.split("_", 1)[1]
        safe_send_message(
            call.message.chat.id,
            f"📞 Нажми, чтобы позвонить:\n<a href='tel:{phone}'>{phone}</a>",
            parse_mode="HTML"
        )
        bot.answer_callback_query(call.id)
        return

    bot.answer_callback_query(call.id)

    if data == "back_categories":
        ok = safe_edit_message_text(
            call.message.chat.id,
            call.message.message_id,
            "Выбери категорию:",
            reply_markup=get_categories_keyboard()
        )
        if not ok:
            safe_send_message(call.message.chat.id, "Выбери категорию:", reply_markup=get_categories_keyboard())
        return

    if data == "cart":
        edit_cart_message(call, user_id)
        return

    if data == "checkout":
        begin_checkout_flow(call.message.chat.id, user_id)
        return

    if data == "clear_cart_inline":
        clear_user_cart(user_id)
        clear_preview(call.message.chat.id, user_id)
        user_data = get_user_data(user_id)
        user_data.pop("pending_fryer_item", None)
        user_data.pop("undo_stack", None)
        save_user_data(user_id, user_data)
        if get_user_state(user_id) != STATE_NONE:
            set_user_state(user_id, STATE_NONE)
        edit_cart_message(call, user_id)
        return

    if data == "confirm_order":
        phone = get_user_data(user_id).get("phone", "")
        finish_order(call.message, phone, user_id)
        return

    if data == "return_to_cart":
        set_user_state(user_id, STATE_NONE)
        if not get_user_cart(user_id):
            clear_preview(call.message.chat.id, user_id)
            safe_send_message(
                call.message.chat.id,
                "Корзина пуста.",
                reply_markup=get_main_keyboard()
            )
            return
        safe_send_message(
            call.message.chat.id,
            "Вернулись в корзину. Можешь изменить заказ.",
            reply_markup=get_main_keyboard()
        )
        show_cart_message(call.message.chat.id, user_id)
        return

    if data.startswith("cat|"):
        category_id = data.split("|")[1]
        edit_category_message(call, category_id)
        return

    if data.startswith("add|"):
        _, category_id, item_id = data.split("|")
        item_name, item_price = get_item_by_id(category_id, item_id)

        if item_name == "Маки с рыбой":
            ok = safe_edit_message_text(
                call.message.chat.id,
                call.message.message_id,
                "Выбери вид маки:",
                reply_markup=get_maki_fish_keyboard(category_id)
            )
            if not ok:
                safe_send_message(
                    call.message.chat.id,
                    "Выбери вид маки:",
                    reply_markup=get_maki_fish_keyboard(category_id)
                )
            return

        if item_name in FRYER_ITEMS:
            push_undo_add_item(user_id, item_name)
            add_to_cart(user_id, item_name, item_price)
            refresh_preview_after_cart_change(call.message.chat.id, user_id, item_name)

            user_data = get_user_data(user_id)
            user_data["pending_fryer_item"] = {
                "name": item_name,
                "price": item_price,
                "category_id": category_id
            }
            save_user_data(user_id, user_data)

            ok = safe_edit_message_text(
                call.message.chat.id,
                call.message.message_id,
                f"Для позиции «{item_name}» выбери соус:",
                reply_markup=get_sauces_keyboard(category_id)
            )
            if not ok:
                safe_send_message(
                    call.message.chat.id,
                    f"Для позиции «{item_name}» выбери соус:",
                    reply_markup=get_sauces_keyboard(category_id)
                )
            return

        push_undo_add_item(user_id, item_name)
        add_to_cart(user_id, item_name, item_price)
        refresh_preview_after_cart_change(call.message.chat.id, user_id, item_name)

        if category_id == "7":
            user_data = get_user_data(user_id)
            if user_data.get("awaiting_drink_offer_choice"):
                user_data.pop("awaiting_drink_offer_choice", None)
                save_user_data(user_id, user_data)
                safe_send_message(call.message.chat.id, format_cart(get_user_cart(user_id)))
                start_address_step(call.message.chat.id, user_id)
                return

        return

    if data.startswith("sauce|"):
        _, category_id, safe_sauce_name = data.split("|")
        sauce_name = unescape_callback_text(safe_sauce_name)

        user_data = get_user_data(user_id)
        pending_item = user_data.get("pending_fryer_item")
        if not pending_item:
            safe_send_message(call.message.chat.id, "Ошибка: позиция для соуса не найдена.")
            return

        push_undo_add_item(user_id, f"Соус: {sauce_name}")
        add_to_cart(user_id, f"Соус: {sauce_name}", SAUCES[sauce_name])

        user_data.pop("pending_fryer_item", None)
        save_user_data(user_id, user_data)

        show_or_update_preview(
            call.message.chat.id,
            user_id,
            f"{pending_item['name']} + соус {sauce_name}"
        )
        edit_category_message(call, category_id)
        return

    if data.startswith("sauce_none|"):
        _, category_id = data.split("|")

        user_data = get_user_data(user_id)
        pending_item = user_data.get("pending_fryer_item")
        if not pending_item:
            safe_send_message(call.message.chat.id, "Ошибка: позиция для соуса не найдена.")
            return

        user_data.pop("pending_fryer_item", None)
        save_user_data(user_id, user_data)

        show_or_update_preview(
            call.message.chat.id,
            user_id,
            f"{pending_item['name']} без соуса"
        )
        edit_category_message(call, category_id)
        return

    if data.startswith("maki_fish|"):
        _, category_id, safe_item_name = data.split("|")
        item_name = unescape_callback_text(safe_item_name)
        item_price = MAKI_FISH_OPTIONS.get(item_name)
        if item_price is None:
            safe_send_message(call.message.chat.id, "Ошибка: вид маки не найден.")
            return
        push_undo_add_item(user_id, item_name)
        add_to_cart(user_id, item_name, item_price)
        refresh_preview_after_cart_change(call.message.chat.id, user_id, item_name)
        edit_category_message(call, category_id)
        return

    if data == "chuka_add":
        push_undo_add_item(user_id, CHUKA_NAME)
        add_to_cart(user_id, CHUKA_NAME, CHUKA_PRICE)
        refresh_preview_after_cart_change(call.message.chat.id, user_id, CHUKA_NAME)
        safe_send_message(call.message.chat.id, format_cart(get_user_cart(user_id)))
        continue_checkout_after_offers(call.message.chat.id, user_id)
        return

    if data == "chuka_skip":
        safe_send_message(call.message.chat.id, format_cart(get_user_cart(user_id)))
        continue_checkout_after_offers(call.message.chat.id, user_id)
        return

    if data == "drinks_offer_open":
        user_data = get_user_data(user_id)
        user_data["awaiting_drink_offer_choice"] = True
        save_user_data(user_id, user_data)
        edit_category_message(call, "7")
        return

    if data == "drinks_offer_skip":
        user_data = get_user_data(user_id)
        user_data.pop("awaiting_drink_offer_choice", None)
        save_user_data(user_id, user_data)
        safe_send_message(call.message.chat.id, format_cart(get_user_cart(user_id)))
        start_address_step(call.message.chat.id, user_id)
        return

    if data.startswith("cart_line|"):
        return

    if data.startswith("cart_inc|"):
        try:
            item_index = int(data.split("|", 1)[1])
        except ValueError:
            return
        item_name = get_cart_item_name_by_index(user_id, item_index)
        cart = get_user_cart(user_id)
        if item_name and item_name in cart:
            set_cart_item_qty(user_id, item_name, int(cart[item_name]["qty"]) + 1)
            edit_cart_message(call, user_id)
            refresh_preview_after_cart_change(call.message.chat.id, user_id, item_name)
        return

    if data.startswith("cart_dec|"):
        try:
            item_index = int(data.split("|", 1)[1])
        except ValueError:
            return
        item_name = get_cart_item_name_by_index(user_id, item_index)
        cart = get_user_cart(user_id)
        if item_name and item_name in cart:
            set_cart_item_qty(user_id, item_name, int(cart[item_name]["qty"]) - 1)
            edit_cart_message(call, user_id)
            refresh_preview_after_cart_change(call.message.chat.id, user_id, item_name)
        return

    if data.startswith("cart_del|"):
        try:
            item_index = int(data.split("|", 1)[1])
        except ValueError:
            return
        item_name = get_cart_item_name_by_index(user_id, item_index)
        if item_name:
            delete_cart_item(user_id, item_name)
            edit_cart_message(call, user_id)
            refresh_preview_after_cart_change(call.message.chat.id, user_id, f"Удалено: {item_name}")
        return
    if data.startswith("admin_accept|"):
        _, order_id, client_chat_id = data.split("|")
        mark_order_accepted_for_history(int(client_chat_id), order_id)
        apply_admin_status_change(call, order_id, client_chat_id, "accepted", visible_status="accepted")
        return
    if data.startswith("admin_way|"):
        _, order_id, client_chat_id = data.split("|")
        apply_admin_status_change(call, order_id, client_chat_id, "on_the_way", visible_status=None)
        return
    if data.startswith("admin_cancel|"):
        _, order_id, client_chat_id = data.split("|")
        apply_admin_status_change(call, order_id, client_chat_id, "cancelled", visible_status="cancelled")
        return


# =========================
# ЛОКАЦИЯ / КОНТАКТ


# =========================
# ЛОКАЦИЯ / КОНТАКТ
# =========================
@bot.message_handler(content_types=["location"])
def handle_location(message):
    if message.chat.type != "private":
        return

    user_id = message.from_user.id
    current_state = get_user_state(user_id)

    if current_state not in [STATE_WAIT_ADDRESS_CHOICE, STATE_WAIT_ADDRESS_MANUAL]:
        safe_send_message(
            message.chat.id,
            "Сначала начни оформление заново через кнопку «✅ Оформить заказ».",
            reply_markup=get_main_keyboard()
        )
        return

    lat = message.location.latitude
    lon = message.location.longitude

    push_undo_restore(user_id)
    safe_send_message(message.chat.id, "Геолокация получена, определяю адрес...")

    address, locality, map_link, needs_manual_address = get_address_and_locality_from_coords(lat, lon)

    user_data = get_user_data(user_id)
    user_data["locality"] = locality
    user_data["map_link"] = map_link

    if needs_manual_address:
        user_data["address"] = address
        user_data["need_manual_clarification"] = True
        save_user_data(user_id, user_data)

        set_user_state(user_id, STATE_WAIT_ADDRESS_MANUAL)
        safe_send_message(
            message.chat.id,
            f"📍 Населённый пункт: {locality}\n"
            f"🏠 Адрес: {address}\n\n"
            f"{map_link}",
            reply_markup=types.ReplyKeyboardRemove()
        )
        safe_send_message(
            message.chat.id,
            "⚠️ Адрес определился неточно.\n\n"
            "Пожалуйста, введите адрес вручную в формате:\n"
            "улица, дом, подъезд\n\n"
            f"{INFO_DELIVERY_TEXT}"
        )
        return

    user_data["address"] = address
    user_data["need_manual_clarification"] = False
    save_user_data(user_id, user_data)

    set_user_state(user_id, STATE_WAIT_PHONE)
    safe_send_message(
        message.chat.id,
        f"📍 Населённый пункт: {locality}\n"
        f"🏠 Адрес: {address}\n\n"
        f"{INFO_DELIVERY_TEXT}\n\n"
        "Нажми кнопку, чтобы отправить номер:",
        reply_markup=get_phone_keyboard()
    )


@bot.message_handler(content_types=["contact"])
def handle_contact(message):
    if message.chat.type != "private":
        return

    user_id = message.from_user.id
    if get_user_state(user_id) != STATE_WAIT_PHONE:
        return

    push_undo_restore(user_id)
    phone = normalize_phone(message.contact.phone_number)
    if not phone:
        safe_send_message(message.chat.id, "❌ Не удалось распознать номер. Отправь его ещё раз.", reply_markup=get_phone_keyboard())
        return
    user_data = get_user_data(user_id)
    user_data["phone"] = phone
    save_user_data(user_id, user_data)

    set_user_state(user_id, STATE_WAIT_PAYMENT)
    safe_send_message(
        message.chat.id,
        "Номер получен. Выбери способ оплаты: Картой, Наличными или Переводом:",
        reply_markup=get_payment_keyboard()
    )


# =========================
# ЭТАПЫ ОФОРМЛЕНИЯ
# =========================
@bot.message_handler(func=lambda m: m.chat.type == "private" and get_user_state(m.from_user.id) == STATE_WAIT_ADDRESS_CHOICE and m.text in ["✏️ Ввести вручную", "Ввести вручную"])
def choose_manual_address(message):
    push_undo_restore(message.from_user.id)
    set_user_state(message.from_user.id, STATE_WAIT_ADDRESS_MANUAL)
    safe_send_message(
        message.chat.id,
        "Напиши адрес вручную в формате:\n\nулица, дом, подъезд\n\n" + INFO_DELIVERY_TEXT,
        reply_markup=types.ReplyKeyboardRemove()
    )


@bot.message_handler(func=lambda m: m.chat.type == "private" and get_user_state(m.from_user.id) in [STATE_WAIT_ADDRESS_CHOICE, STATE_WAIT_ADDRESS_MANUAL] and m.content_type == "text" and m.text not in ["🔄 Начать заново", "⭐ Отзывы", "📞 Отправить номер", "📍 Отправить геолокацию", "Картой", "Наличными", "Пропустить", "✅ Оформить заказ", "📋 Меню", "🛒 Корзина", "🗑 Очистить корзину", "🔁 Повторить прошлый заказ", "📦 Мои заказы"])
def process_manual_address(message):
    user_id = message.from_user.id
    text = message.text.strip()

    if len(text) < 6:
        safe_send_message(
            message.chat.id,
            "Адрес слишком короткий. Напиши адрес в формате:\nулица, дом, подъезд"
        )
        return

    push_undo_restore(user_id)
    user_data = get_user_data(user_id)

    clean_address_text = extract_clean_address_for_maps(text)
    explicit_locality = detect_locality_from_text(clean_address_text)
    search_order = build_locality_search_order(explicit_locality if explicit_locality != "Не определён" else None)

    detected_locality = explicit_locality if explicit_locality != "Не определён" else "Не определён"
    map_link = ""

    for locality_name in search_order:
        match = geocode_address_with_locality(clean_address_text, locality_name)
        if match:
            detected_locality = locality_name
            map_link = match["map_link"]
            break

    if not map_link:
        map_link = build_yandex_maps_link_from_text(clean_address_text, detected_locality)

    user_data["address"] = text
    user_data["need_manual_clarification"] = False
    user_data["locality"] = detected_locality
    user_data["map_link"] = map_link
    save_user_data(user_id, user_data)

    set_user_state(user_id, STATE_WAIT_PHONE)
    safe_send_message(
        message.chat.id,
        f"📍 Населённый пункт: {detected_locality}\n"
        f"🏠 Адрес: {text}\n\n"
        "Адрес получен. Нажмите кнопку, чтобы отправить номер телефона, или введите его вручную для связи с вами.",
        reply_markup=get_phone_keyboard()
    )


@bot.message_handler(func=lambda m: m.chat.type == "private" and get_user_state(m.from_user.id) == STATE_WAIT_PHONE)
def process_phone_text(message):
    if message.text == "📞 Отправить номер":
        safe_send_message(
            message.chat.id,
            "Нажмите системную кнопку отправки номера или введите номер вручную в формате +7XXXXXXXXXX или 8XXXXXXXXXX.",
            reply_markup=get_phone_keyboard()
        )
        return

    user_id = message.from_user.id
    phone = normalize_phone(message.text.strip())
    if not phone:
        safe_send_message(message.chat.id, "❌ Введи корректный номер в формате +7XXXXXXXXXX или 8XXXXXXXXXX")
        return
    user_data = get_user_data(user_id)
    user_data["phone"] = phone
    save_user_data(user_id, user_data)

    set_user_state(user_id, STATE_WAIT_PAYMENT)
    safe_send_message(
        message.chat.id,
        "Номер получен. Выбери способ оплаты: Картой, Наличными или Переводом:",
        reply_markup=get_payment_keyboard()
    )


@bot.message_handler(func=lambda m: m.chat.type == "private" and get_user_state(m.from_user.id) == STATE_WAIT_PAYMENT)
def process_payment(message):
    payment_value = normalize_payment_choice(message.text)
    if not payment_value:
        safe_send_message(message.chat.id, "Выбери: Картой, Наличными или Переводом.")
        return

    user_id = message.from_user.id
    push_undo_restore(user_id)
    user_data = get_user_data(user_id)
    user_data["payment"] = payment_value
    save_user_data(user_id, user_data)

    set_user_state(user_id, STATE_WAIT_COMMENT_OR_SKIP)
    safe_send_message(
        message.chat.id,
        "Если хочешь, напиши комментарий к заказу или нажми Пропустить:",
        reply_markup=get_comment_skip_keyboard()
    )


@bot.message_handler(func=lambda m: m.chat.type == "private" and get_user_state(m.from_user.id) == STATE_WAIT_COMMENT_OR_SKIP and m.text == "Пропустить")
def skip_comment(message):
    user_id = message.from_user.id
    push_undo_restore(user_id)
    user_data = get_user_data(user_id)
    user_data["comment"] = ""
    save_user_data(user_id, user_data)

    set_user_state(user_id, STATE_WAIT_CONFIRM)
    safe_send_message(
        message.chat.id,
        build_confirmation_text(user_id),
        reply_markup=get_confirm_order_keyboard()
    )


@bot.message_handler(func=lambda m: m.chat.type == "private" and get_user_state(m.from_user.id) == STATE_WAIT_COMMENT_OR_SKIP)
def save_comment_and_confirm(message):
    user_id = message.from_user.id
    push_undo_restore(user_id)
    user_data = get_user_data(user_id)
    user_data["comment"] = message.text.strip()
    save_user_data(user_id, user_data)

    safe_send_message(
        message.chat.id,
        (
            f"💬 Ваш комментарий:\n"
            f"«{user_data['comment']}»\n\n"
            f"Всё верно?"
        ),
        reply_markup=get_comment_confirm_keyboard()
    )


# =========================
# ЗАВЕРШЕНИЕ ЗАКАЗА
# =========================
def finish_order(message, phone, user_id):
    cart = get_user_cart(user_id)

    if not cart:
        safe_send_message(message.chat.id, "Корзина пуста.", reply_markup=get_main_keyboard())
        reset_user_flow(user_id)
        return

    user_data = get_user_data(user_id)
    address = user_data.get("address", "Не указан")
    locality = user_data.get("locality", "Не определён")
    payment = user_data.get("payment", "Не указана")
    comment = user_data.get("comment", "")

    total = 0
    items_lines = []

    for item_name, item_data in cart.items():
        qty = int(item_data["qty"])
        price = int(item_data["price"])
        line_total = qty * price
        total += line_total
        items_lines.append(f"• {item_name} x{qty} — {line_total}₽")

    items_text = "\n".join(items_lines)
    client_name = message.chat.first_name or "Без имени"

    if USE_POSTGRES:
        order_id, _order_db_id = create_pg_order(
            user_id=user_id,
            total=total,
            payment=payment,
            address=address,
            locality=locality,
            comment=comment,
            cart=copy.deepcopy(cart),
        )
    else:
        order_id = get_next_order_id()

    comment_text_admin = f"\n💬 Комментарий: {comment}" if comment else ""
    comment_text_user = f"\n💬 Комментарий: {comment}" if comment else ""
    map_link = user_data.get("map_link", "")
    map_link_text_admin = f"\n🗺 Маршрут: {map_link}" if map_link else ""

    admin_text = (
        f"📦 ЗАКАЗ #{extract_order_number(order_id)}\n\n"
        f"👤 Клиент: {client_name}\n"
        f"📞 Телефон: {phone}\n\n"
        f"📋 Состав заказа:\n{items_text}\n\n"
        f"📍 Населённый пункт: {locality}\n"
        f"🏠 Адрес: {address}"
        f"{map_link_text_admin}\n"
        f"💳 Оплата: {payment}"
        f"{comment_text_admin}\n"
        f"💰 Итого: {total}₽"
    )

    admin_text_with_status = build_admin_order_text(admin_text, "new")
    sent_to_admin = safe_send_message(
        ADMIN_CHAT_ID,
        admin_text_with_status,
        reply_markup=get_admin_order_keyboard(order_id, message.chat.id, phone),
        disable_web_page_preview=True
    )

    if not sent_to_admin:
        safe_send_message(
            message.chat.id,
            "Не удалось завершить заказ. Попробуй ещё раз.",
            reply_markup=get_main_keyboard()
        )
        return

    set_order_status(order_id, "new", ADMIN_CHAT_ID, sent_to_admin.message_id, message.chat.id)
    save_order_meta(order_id, {
        "admin_message_chat_id": ADMIN_CHAT_ID,
        "admin_message_id": sent_to_admin.message_id,
        "client_chat_id": message.chat.id,
        "admin_base_text": admin_text,
        "phone": phone,
    })

    if not USE_POSTGRES:
        order_record = {
            "order_id": order_id,
            "total": total,
            "payment": payment,
            "address": address,
            "locality": locality,
            "comment": comment,
            "cart": copy.deepcopy(cart),
            "status": "new",
            "accepted_for_history": False,
        }
        save_order_history(user_id, order_record)

    safe_send_message(
        message.chat.id,
        "✅ Заказ оформлен\n"
        f"{order_id}\n"
        f"📞 Телефон: {phone}\n"
        f"📍 Населённый пункт: {locality}\n"
        f"🏠 Адрес: {address}\n"
        f"💳 Оплата: {payment}\n"
        f"💰 Итого к оплате: {total}₽\n"
        f"{INFO_DELIVERY_TEXT}"
        f"{comment_text_user}",
        reply_markup=get_main_keyboard()
    )

    safe_send_message(
        message.chat.id,
        "В ближайшее время вы получите сообщение о статусе заказа (принят / отменён)."
    )

    clear_preview(message.chat.id, user_id)
    clear_user_cart(user_id)
    reset_user_flow(user_id)

def run_bot():
    if USE_POSTGRES:
        pg_init_db()

    scheduler_thread = threading.Thread(target=public_post_scheduler, daemon=True)
    scheduler_thread.start()

    stale_thread = threading.Thread(target=stale_orders_watcher, daemon=True)
    stale_thread.start()

    logger.info("Бот запущен. Режим хранения: %s", "PostgreSQL" if USE_POSTGRES else "JSON")

    while True:
        try:
            bot.infinity_polling(timeout=60, long_polling_timeout=60, skip_pending=True)
        except Exception:
            logger.exception("Polling crashed, restarting in 3 seconds")
            time.sleep(3)


if __name__ == "__main__":
    run_bot()
