import os
import json
import math
import copy
import requests
import telebot
from telebot import types
from datetime import datetime

# =========================
# НАСТРОЙКИ
# =========================

BOT_TOKEN = os.getenv("BOT_TOKEN")

bot = telebot.TeleBot(BOT_TOKEN)
BOT_USERNAME = "KakMakDak_bot"
PHONE_NUMBER = "+79789927572"

ADMIN_CHAT_ID = -1003701168467
PUBLIC_GROUP_ID = -1003817058763

YANDEX_LINK = "https://share.google/Tqn6l2X7GmhaTTcC2"
WORKING_HOURS = "Ежедневно 15:00-03:00"

DATA_FILE = "bot_storage.json"

INFO_DELIVERY_TEXT = "ℹ️ Важно: доставка до подъезда/входа. В квартиру не заносим."

bot = telebot.TeleBot(TOKEN)

# =========================
# МЕНЮ
# =========================
MENU = {
    "1": {
        "title": "🌯 Шаурма и Хот-доги",
        "items": {
            "Шаурма Классик": 350,
            "Шаурма с тигровой креветкой": 450,
            "Шаурма с говядиной": 500,
            "Шаурма сытная (сыр + картофель)": 500,
            "Хот-дог Датский": 300,
            "Хот-дог в лаваше": 450,
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
            "Фишбургер": 450,
            "Крабсбургер": 650,
            "Сэндвич с красной рыбой": 300,
            "Сэндвич с курицей": 250,
        },
    },
    "3": {
        "title": "🍜 Лапша WOK",
        "items": {
            "Удон с курицей": 500,
            "Удон с говядиной": 600,
            "Удон с креветкой": 550,
            "Удон сливочный с креветкой": 600,
            "Соба с курицей": 500,
            "Соба с говядиной": 600,
            "Соба с креветкой": 550,
            "Тяхан с курицей": 550,
            "Тяхан с тунцом и креветкой": 650,
        },
    },
    "4": {
        "title": "🍣 Роллы и Суши",
        "items": {
            "Филадельфия": 550,
            "Филадельфия с креветкой": 600,
            "Аляска Лосось": 400,
            "Сливочный угорь": 600,
            "Сливочная креветка": 600,
            "Чиз-ролл": 450,
            "Маки огурец/лосось": 300,
            "Салат Чука": 250,
        },
    },
    "5": {
        "title": "🍟 Закуски",
        "items": {
            "Картофель фри": 250,
            "Картофель по-деревенски": 250,
            "Сырные палочки": 400,
            "Наггетсы": 350,
            "Луковые кольца": 250,
            "Креветки к пиву": 450,
        },
    },
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
# ХРАНИЛИЩЕ
# =========================
storage = {
    "user_cart": {},
    "user_state": {},
    "user_data": {},
    "preview_message_ids": {},
    "order_counters_by_date": {},
    "order_history": {},
    "last_public_post_date": "",
}


def load_storage():
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


def save_storage():
    try:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(storage, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


load_storage()

# =========================
# БАЗОВЫЕ ФУНКЦИИ
# =========================
def ukey(user_id):
    return str(user_id)


def get_user_cart(user_id):
    key = ukey(user_id)
    if key not in storage["user_cart"] or not isinstance(storage["user_cart"][key], dict):
        storage["user_cart"][key] = {}
    return storage["user_cart"][key]


def clear_user_cart(user_id):
    storage["user_cart"][ukey(user_id)] = {}
    save_storage()


def get_user_state(user_id):
    return storage["user_state"].get(ukey(user_id), STATE_NONE)


def set_user_state(user_id, state):
    storage["user_state"][ukey(user_id)] = state
    save_storage()


def get_user_data(user_id):
    key = ukey(user_id)
    if key not in storage["user_data"] or not isinstance(storage["user_data"][key], dict):
        storage["user_data"][key] = {}
    return storage["user_data"][key]


def reset_user_flow(user_id):
    storage["user_state"][ukey(user_id)] = STATE_NONE
    storage["user_data"][ukey(user_id)] = {}
    save_storage()


def normalize_phone(phone):
    phone = str(phone).strip()
    phone = phone.replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    if phone.startswith("+"):
        return phone
    if phone.startswith("8") and len(phone) == 11:
        return "+7" + phone[1:]
    if phone.startswith("7") and len(phone) == 11:
        return "+" + phone
    return "+" + phone


def escape_callback_text(text):
    return text.replace("|", "¦")


def unescape_callback_text(text):
    return text.replace("¦", "|")


def add_to_cart(user_id, item_name, price, qty=1):
    cart = get_user_cart(user_id)
    if item_name not in cart:
        cart[item_name] = {"price": int(price), "qty": 0}
    cart[item_name]["qty"] += int(qty)
    save_storage()


def get_item_by_id(category_id, item_id):
    items_list = list(MENU[category_id]["items"].items())
    item_name, price = items_list[int(item_id) - 1]
    return item_name, int(price)


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


def detect_locality_from_text(text):
    if not text:
        return "Не определён"
    text_lower = text.lower()
    for locality in LOCALITIES.keys():
        if locality.lower() in text_lower:
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
        return ", ".join(parts) + f"\n{map_link}"

    if display_name:
        return display_name + f"\n{map_link}"

    return map_link


def get_address_and_locality_from_coords(lat, lon):
    map_link = f"https://yandex.ru/maps/?pt={lon},{lat}&z=17&l=map"
    try:
        url = (
            "https://nominatim.openstreetmap.org/reverse"
            f"?format=json&lat={lat}&lon={lon}&zoom=18&addressdetails=1"
        )
        headers = {"User-Agent": "telegram-food-bot"}
        response = requests.get(url, headers=headers, timeout=4)
        data = response.json()

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
            if nearest_distance is not None and nearest_distance <= 12:
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
        locality = nearest_locality if nearest_distance is not None and nearest_distance <= 12 else "Не определён"
        fallback_address = f"{lat}, {lon}\n{map_link}"
        return fallback_address, locality, map_link, True


def safe_send_message(chat_id, text, **kwargs):
    try:
        return bot.send_message(chat_id, text, **kwargs)
    except Exception:
        return None


def safe_edit_message_text(chat_id, message_id, text, **kwargs):
    try:
        bot.edit_message_text(text, chat_id, message_id, **kwargs)
        return True
    except Exception as e:
        if "message is not modified" in str(e).lower():
            return True
        return False

# =========================
# КЛАВИАТУРЫ
# =========================
def get_main_keyboard():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("📋 Меню", "🛒 Корзина")
    kb.row("✅ Оформить заказ", "🗑 Очистить корзину")
    kb.row("🔁 Повторить прошлый заказ", "📦 Мои заказы")
    return kb


def get_link_keyboard():
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("📍 Мы на Яндекс Картах", url=YANDEX_LINK))
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


def get_cart_keyboard(user_id):
    kb = types.InlineKeyboardMarkup(row_width=4)
    cart = get_user_cart(user_id)

    if not cart:
        kb.add(types.InlineKeyboardButton("📋 К категориям", callback_data="back_categories"))
        return kb

    for item_name, item_data in cart.items():
        safe_item = escape_callback_text(item_name)
        qty = int(item_data["qty"])
        line_total = qty * int(item_data["price"])

        kb.row(
            types.InlineKeyboardButton(
                f"{item_name} x{qty} — {line_total}₽",
                callback_data="noop"
            ),
            types.InlineKeyboardButton("➖", callback_data=f"cart_dec|{safe_item}"),
            types.InlineKeyboardButton("➕", callback_data=f"cart_inc|{safe_item}"),
            types.InlineKeyboardButton("✖", callback_data=f"cart_del|{safe_item}")
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


def get_admin_order_keyboard(order_id, client_chat_id, phone):
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("📞 Позвонить клиенту", callback_data=f"call_{phone}"),
        types.InlineKeyboardButton("💬 Написать в Telegram", url=f"tg://user?id={client_chat_id}")
    )
    kb.row(
        types.InlineKeyboardButton("✅ Принят", callback_data=f"admin_accept|{order_id}|{client_chat_id}"),
        types.InlineKeyboardButton("🚚 Уже в пути", callback_data=f"admin_way|{order_id}|{client_chat_id}")
    )
    kb.add(
        types.InlineKeyboardButton("❌ Отменить заказ", callback_data=f"admin_cancel|{order_id}|{client_chat_id}")
    )
    return kb

# =========================
# ПРЕДКОРЗИНА
# =========================
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

    key = ukey(user_id)
    preview_message_ids = storage["preview_message_ids"]

    if key in preview_message_ids:
        ok = safe_edit_message_text(
            chat_id,
            preview_message_ids[key],
            preview_text,
            reply_markup=get_preview_keyboard()
        )
        if ok:
            return

    msg = safe_send_message(chat_id, preview_text, reply_markup=get_preview_keyboard())
    if msg:
        preview_message_ids[key] = msg.message_id
        save_storage()


def clear_preview(chat_id, user_id):
    key = ukey(user_id)
    preview_message_ids = storage["preview_message_ids"]
    if key in preview_message_ids:
        safe_edit_message_text(
            chat_id,
            preview_message_ids[key],
            "🛒 Предкорзина пуста.",
            reply_markup=get_preview_keyboard()
        )

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


def start_address_step(chat_id, user_id):
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
        f"💰 Итого: {total}₽"
    )


def get_next_order_id():
    now = datetime.now()
    today_key = now.strftime("%Y%m%d")
    today_view = now.strftime("%d-%m-%y")

    counters = storage["order_counters_by_date"]
    counters[today_key] = counters.get(today_key, 0) + 1
    save_storage()

    return f"дата {today_view}  #заказа {counters[today_key]:04d}"


def save_order_history(user_id, order_record):
    key = ukey(user_id)
    if key not in storage["order_history"] or not isinstance(storage["order_history"][key], list):
        storage["order_history"][key] = []
    storage["order_history"][key].append(order_record)
    storage["order_history"][key] = storage["order_history"][key][-20:]
    save_storage()


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
    if storage.get("last_public_post_date") == today:
        return

    msg = safe_send_message(
        PUBLIC_GROUP_ID,
        build_public_info_text(),
        reply_markup=get_group_buttons_keyboard()
    )
    if msg:
        storage["last_public_post_date"] = today
        save_storage()
        try:
            bot.pin_chat_message(PUBLIC_GROUP_ID, msg.message_id, disable_notification=True)
        except Exception:
            pass

# =========================
# СТАРТ
# =========================
@bot.message_handler(commands=["start"])
def start(message):
    if message.chat.type != "private":
        return

    user_id = message.from_user.id
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
        storage["last_public_post_date"] = datetime.now().strftime("%Y-%m-%d")
        save_storage()
        try:
            bot.pin_chat_message(PUBLIC_GROUP_ID, msg.message_id, disable_notification=True)
        except Exception:
            pass
        safe_send_message(message.chat.id, "✅ Сообщение с кнопкой отправлено в группу.")
    else:
        safe_send_message(message.chat.id, "Не удалось отправить сообщение в группу.")

# =========================
# ОСНОВНЫЕ КНОПКИ
# =========================
@bot.message_handler(func=lambda m: m.chat.type == "private" and m.text == "📋 Меню")
def show_menu(message):
    safe_send_message(message.chat.id, "Выбери категорию:", reply_markup=get_categories_keyboard())


@bot.message_handler(func=lambda m: m.chat.type == "private" and m.text == "🛒 Корзина")
def show_cart(message):
    show_cart_message(message.chat.id, message.from_user.id)


@bot.message_handler(func=lambda m: m.chat.type == "private" and m.text == "🗑 Очистить корзину")
def clear_cart_handler(message):
    clear_user_cart(message.from_user.id)
    clear_preview(message.chat.id, message.from_user.id)
    safe_send_message(message.chat.id, "Корзина очищена.", reply_markup=get_main_keyboard())


@bot.message_handler(func=lambda m: m.chat.type == "private" and m.text == "🔁 Повторить прошлый заказ")
def repeat_last_order(message):
    user_id = message.from_user.id
    history = storage["order_history"].get(ukey(user_id), [])

    if not history:
        safe_send_message(message.chat.id, "У тебя пока нет прошлых заказов.", reply_markup=get_main_keyboard())
        return

    last_order = history[-1]
    storage["user_cart"][ukey(user_id)] = copy.deepcopy(last_order.get("cart", {}))
    save_storage()

    safe_send_message(message.chat.id, "Последний заказ добавлен в корзину.", reply_markup=get_main_keyboard())
    show_or_update_preview(message.chat.id, user_id, "Повтор прошлого заказа")
    show_cart_message(message.chat.id, user_id)


@bot.message_handler(func=lambda m: m.chat.type == "private" and m.text == "📦 Мои заказы")
def my_orders(message):
    history = storage["order_history"].get(ukey(message.from_user.id), [])

    if not history:
        safe_send_message(message.chat.id, "История заказов пуста.", reply_markup=get_main_keyboard())
        return

    texts = [format_order_history_item(order) for order in history[-5:]]
    safe_send_message(
        message.chat.id,
        "📦 Последние заказы:\n\n" + "\n\n".join(texts),
        reply_markup=get_main_keyboard()
    )


@bot.message_handler(func=lambda m: m.chat.type == "private" and m.text == "✅ Оформить заказ")
def checkout_from_main(message):
    user_id = message.from_user.id
    cart = get_user_cart(user_id)

    if not cart:
        safe_send_message(message.chat.id, "Корзина пуста.", reply_markup=get_main_keyboard())
        return

    safe_send_message(message.chat.id, format_cart(cart))
    start_address_step(message.chat.id, user_id)

# =========================
# CALLBACK'И
# =========================
@bot.callback_query_handler(func=lambda call: True)
def callbacks(call):
    user_id = call.from_user.id
    data = call.data

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
        cart = get_user_cart(user_id)
        if not cart:
            return
        safe_send_message(call.message.chat.id, format_cart(cart))
        start_address_step(call.message.chat.id, user_id)
        return

    if data == "clear_cart_inline":
        clear_user_cart(user_id)
        clear_preview(call.message.chat.id, user_id)
        edit_cart_message(call, user_id)
        return

    if data == "confirm_order":
        phone = get_user_data(user_id).get("phone", "")
        finish_order(call.message, phone, user_id)
        return

    if data == "return_to_cart":
        set_user_state(user_id, STATE_NONE)
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

        if item_name in FRYER_ITEMS:
            data_user = get_user_data(user_id)
            data_user["pending_fryer_item"] = {
                "name": item_name,
                "price": item_price,
                "category_id": category_id
            }
            save_storage()

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

        add_to_cart(user_id, item_name, item_price)
        show_or_update_preview(call.message.chat.id, user_id, item_name)
        return

    if data.startswith("sauce|"):
        _, category_id, safe_sauce_name = data.split("|")
        sauce_name = unescape_callback_text(safe_sauce_name)

        pending_item = get_user_data(user_id).get("pending_fryer_item")
        if not pending_item:
            safe_send_message(call.message.chat.id, "Ошибка: позиция для соуса не найдена.")
            return

        add_to_cart(user_id, pending_item["name"], pending_item["price"])
        add_to_cart(user_id, f"Соус: {sauce_name}", SAUCES[sauce_name])

        get_user_data(user_id).pop("pending_fryer_item", None)
        save_storage()

        show_or_update_preview(
            call.message.chat.id,
            user_id,
            f"{pending_item['name']} + соус {sauce_name}"
        )
        edit_category_message(call, category_id)
        return

    if data.startswith("sauce_none|"):
        _, category_id = data.split("|")

        pending_item = get_user_data(user_id).get("pending_fryer_item")
        if not pending_item:
            safe_send_message(call.message.chat.id, "Ошибка: позиция для соуса не найдена.")
            return

        add_to_cart(user_id, pending_item["name"], pending_item["price"])
        get_user_data(user_id).pop("pending_fryer_item", None)
        save_storage()

        show_or_update_preview(
            call.message.chat.id,
            user_id,
            f"{pending_item['name']} без соуса"
        )
        edit_category_message(call, category_id)
        return

    if data.startswith("cart_inc|"):
        item_name = unescape_callback_text(data.split("|", 1)[1])
        cart = get_user_cart(user_id)
        if item_name in cart:
            cart[item_name]["qty"] += 1
            save_storage()
        edit_cart_message(call, user_id)
        show_or_update_preview(call.message.chat.id, user_id, item_name)
        return

    if data.startswith("cart_dec|"):
        item_name = unescape_callback_text(data.split("|", 1)[1])
        cart = get_user_cart(user_id)
        if item_name in cart:
            cart[item_name]["qty"] -= 1
            if cart[item_name]["qty"] <= 0:
                del cart[item_name]
            save_storage()
        edit_cart_message(call, user_id)
        show_or_update_preview(call.message.chat.id, user_id, item_name)
        return

    if data.startswith("cart_del|"):
        item_name = unescape_callback_text(data.split("|", 1)[1])
        cart = get_user_cart(user_id)
        if item_name in cart:
            del cart[item_name]
            save_storage()
        edit_cart_message(call, user_id)
        show_or_update_preview(call.message.chat.id, user_id, f"Удалено: {item_name}")
        return

    if data.startswith("admin_accept|"):
        _, order_id, client_chat_id = data.split("|")
        safe_send_message(
            int(client_chat_id),
            f"✅ Заказ принят\n{order_id}",
            reply_markup=get_main_keyboard()
        )
        return

    if data.startswith("admin_way|"):
        _, order_id, client_chat_id = data.split("|")
        safe_send_message(
            int(client_chat_id),
            f"🚚 Заказ уже в пути\n{order_id}",
            reply_markup=get_main_keyboard()
        )
        return

    if data.startswith("admin_cancel|"):
        _, order_id, client_chat_id = data.split("|")
        safe_send_message(
            int(client_chat_id),
            f"❌ Заказ отменён\n{order_id}\nСвяжитесь с администратором для уточнения.",
            reply_markup=get_main_keyboard()
        )
        return

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

    safe_send_message(message.chat.id, "Геолокация получена, определяю адрес...")

    address, locality, map_link, needs_manual_address = get_address_and_locality_from_coords(lat, lon)

    user_data = get_user_data(user_id)
    user_data["locality"] = locality
    user_data["map_link"] = map_link

    if needs_manual_address:
        user_data["address"] = address
        user_data["need_manual_clarification"] = True
        save_storage()

        set_user_state(user_id, STATE_WAIT_ADDRESS_MANUAL)
        safe_send_message(
            message.chat.id,
            f"📍 Населённый пункт: {locality}\n"
            f"🏠 Адрес определился неточно:\n{address}\n\n"
            "Напиши адрес вручную в формате:\n"
            "улица, дом, подъезд\n\n"
            f"{INFO_DELIVERY_TEXT}",
            reply_markup=types.ReplyKeyboardRemove()
        )
        return

    user_data["address"] = address
    user_data["need_manual_clarification"] = False
    save_storage()

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

    phone = normalize_phone(message.contact.phone_number)
    get_user_data(user_id)["phone"] = phone
    save_storage()

    set_user_state(user_id, STATE_WAIT_PAYMENT)
    safe_send_message(
        message.chat.id,
        "Номер получен. Выбери способ оплаты:",
        reply_markup=get_payment_keyboard()
    )

# =========================
# ЭТАПЫ ОФОРМЛЕНИЯ
# =========================
@bot.message_handler(func=lambda m: m.chat.type == "private" and get_user_state(m.from_user.id) == STATE_WAIT_ADDRESS_CHOICE and m.text in ["✏️ Ввести вручную", "Ввести вручную"])
def choose_manual_address(message):
    set_user_state(message.from_user.id, STATE_WAIT_ADDRESS_MANUAL)
    safe_send_message(
        message.chat.id,
        "Напиши адрес вручную в формате:\n\nулица, дом, подъезд\n\n" + INFO_DELIVERY_TEXT,
        reply_markup=types.ReplyKeyboardRemove()
    )


@bot.message_handler(func=lambda m: m.chat.type == "private" and get_user_state(m.from_user.id) == STATE_WAIT_ADDRESS_MANUAL)
def process_manual_address(message):
    user_id = message.from_user.id
    text = message.text.strip()

    if len(text) < 6:
        safe_send_message(
            message.chat.id,
            "Адрес слишком короткий. Напиши адрес в формате:\nулица, дом, подъезд"
        )
        return

    user_data = get_user_data(user_id)
    user_data["address"] = text
    user_data["need_manual_clarification"] = False
    save_storage()

    set_user_state(user_id, STATE_WAIT_PHONE)
    safe_send_message(
        message.chat.id,
        "Адрес получен. Нажми кнопку, чтобы отправить номер:",
        reply_markup=get_phone_keyboard()
    )


@bot.message_handler(func=lambda m: m.chat.type == "private" and get_user_state(m.from_user.id) == STATE_WAIT_PHONE)
def process_phone_text(message):
    if message.text == "📞 Отправить номер":
        safe_send_message(message.chat.id, "Нажми системную кнопку отправки номера.")
        return

    user_id = message.from_user.id
    phone = normalize_phone(message.text.strip())
    get_user_data(user_id)["phone"] = phone
    save_storage()

    set_user_state(user_id, STATE_WAIT_PAYMENT)
    safe_send_message(
        message.chat.id,
        "Номер получен. Выбери способ оплаты:",
        reply_markup=get_payment_keyboard()
    )


@bot.message_handler(func=lambda m: m.chat.type == "private" and get_user_state(m.from_user.id) == STATE_WAIT_PAYMENT)
def process_payment(message):
    if message.text not in ["Картой", "Наличными"]:
        safe_send_message(message.chat.id, "Выбери: Картой или Наличными.")
        return

    user_id = message.from_user.id
    get_user_data(user_id)["payment"] = message.text
    save_storage()

    set_user_state(user_id, STATE_WAIT_COMMENT_OR_SKIP)
    safe_send_message(
        message.chat.id,
        "Если хочешь, напиши комментарий к заказу или нажми Пропустить:",
        reply_markup=get_comment_skip_keyboard()
    )


@bot.message_handler(func=lambda m: m.chat.type == "private" and get_user_state(m.from_user.id) == STATE_WAIT_COMMENT_OR_SKIP and m.text == "Пропустить")
def skip_comment(message):
    user_id = message.from_user.id
    get_user_data(user_id)["comment"] = ""
    save_storage()
    set_user_state(user_id, STATE_WAIT_CONFIRM)

    safe_send_message(
        message.chat.id,
        build_confirmation_text(user_id),
        reply_markup=get_confirm_order_keyboard()
    )


@bot.message_handler(func=lambda m: m.chat.type == "private" and get_user_state(m.from_user.id) == STATE_WAIT_COMMENT_OR_SKIP)
def save_comment_and_confirm(message):
    user_id = message.from_user.id
    comment_text = message.text.strip()
    get_user_data(user_id)["comment"] = comment_text
    save_storage()
    set_user_state(user_id, STATE_WAIT_CONFIRM)

    safe_send_message(
        message.chat.id,
        build_confirmation_text(user_id),
        reply_markup=get_confirm_order_keyboard()
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
    username = message.chat.username if getattr(message.chat, "username", None) else None
    username_text = f"@{username}" if username else "нет"

    order_id = get_next_order_id()

    comment_text_admin = f"\n💬 Комментарий: {comment}" if comment else ""
    comment_text_user = f"\n💬 Комментарий: {comment}" if comment else ""

    admin_text = (
        "📥 НОВЫЙ ЗАКАЗ\n"
        f"{order_id}\n\n"
        f"👤 Клиент: {client_name}\n"
        f"🔗 Username: {username_text}\n"
        f"🆔 ID клиента: {message.chat.id}\n"
        f"📞 Телефон: {phone}\n\n"
        f"📋 Состав заказа:\n{items_text}\n\n"
        f"📍 Населённый пункт: {locality}\n"
        f"🏠 Адрес: {address}\n"
        f"💳 Оплата: {payment}"
        f"{comment_text_admin}\n"
        f"💰 Итого: {total}₽"
    )

    sent_to_admin = safe_send_message(
        ADMIN_CHAT_ID,
        admin_text,
        reply_markup=get_admin_order_keyboard(order_id, message.chat.id, phone)
    )

    if not sent_to_admin:
        safe_send_message(
            message.chat.id,
            "Не удалось завершить заказ. Попробуй ещё раз.",
            reply_markup=get_main_keyboard()
        )
        return

    order_record = {
        "order_id": order_id,
        "total": total,
        "payment": payment,
        "address": address,
        "locality": locality,
        "comment": comment,
        "cart": copy.deepcopy(cart),
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
        f"{INFO_DELIVERY_TEXT}"
        f"{comment_text_user}",
        reply_markup=get_main_keyboard()
    )

    clear_preview(message.chat.id, user_id)
    clear_user_cart(user_id)
    reset_user_flow(user_id)

post_public_info_if_needed()

print("Бот запущен...")
bot.infinity_polling(timeout=30, long_polling_timeout=30)