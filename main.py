import os
import json
import asyncio
import logging
from typing import Dict, Set, List, Optional

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from dotenv import load_dotenv

load_dotenv()  # без override, чтобы Railway env не перетирались

logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID_RAW = os.getenv("OWNER_ID", "0")

try:
    OWNER_ID = int(OWNER_ID_RAW)
except Exception:
    OWNER_ID = 0

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN missing in env")

DATA_FILE = "chats.json"

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

# ==========================
# Dictionaries / Tags
# ==========================

AGE_TAGS = [
    ("baby", "👶 Бейби"),
    ("kids", "🧒 Дети"),
    ("teen", "🧑‍🎓 Подростки"),
    ("adult", "🧑 Взрослые"),
    ("mom", "🤱 Мамочки"),
]

LEVEL_TAGS = [
    ("beginner", "🟢 Начинающие"),
    ("middle", "🟡 Продолжающие"),
    ("pro", "🔴 Профи"),
]

# ==========================
# Persistent chat storage
# ==========================

def load_chats() -> Dict[str, dict]:
    if not os.path.exists(DATA_FILE):
        return {}
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_chats(data: Dict[str, dict]):
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

CHATS: Dict[str, dict] = load_chats()

def upsert_chat(chat: types.Chat):
    if chat.type not in ("group", "supergroup"):
        return
    cid = str(chat.id)
    old = CHATS.get(cid, {})
    CHATS[cid] = {
        "id": chat.id,
        "title": chat.title or str(chat.id),
        "type": chat.type,
        "age": old.get("age"),      # сохраняем уже назначенные теги
        "level": old.get("level"),
    }
    save_chats(CHATS)

def get_chat(chat_id: int) -> Optional[dict]:
    return CHATS.get(str(chat_id))

# ==========================
# Helpers
# ==========================

def is_owner_user_id(user_id: int) -> bool:
    return OWNER_ID != 0 and user_id == OWNER_ID

def is_owner(message: types.Message) -> bool:
    return message.from_user and is_owner_user_id(message.from_user.id)

async def send_to_chat(chat_id: int, origin: types.Message):
    # text
    if origin.text:
        await bot.send_message(chat_id, origin.text)

    # photo
    elif origin.photo:
        file_id = origin.photo[-1].file_id
        caption = origin.caption or ""
        await bot.send_photo(chat_id, file_id, caption=caption)

    # video
    elif origin.video:
        file_id = origin.video.file_id
        caption = origin.caption or ""
        await bot.send_video(chat_id, file_id, caption=caption)

    # document
    elif origin.document:
        file_id = origin.document.file_id
        caption = origin.caption or ""
        await bot.send_document(chat_id, file_id, caption=caption)

    else:
        await bot.send_message(chat_id, "⚠️ Этот тип сообщения пока не поддерживается.")

# ==========================
# Broadcast states (simple)
# ==========================

STATE: Dict[int, str] = {}  # "bc_filter" | "bc_manual" | "bc_wait_msg"
SELECTED_CHATS: Dict[int, Set[int]] = {}  # manual selected chats

BC_SELECTED_AGES: Dict[int, Set[str]] = {}   # age tags
BC_SELECTED_LEVELS: Dict[int, Set[str]] = {} # level tags

# ==========================
# Tag states (simple)
# ==========================

TAG_STATE: Dict[int, str] = {}  # "tag_choose_chat" | "tag_choose_age" | "tag_choose_level"
TAG_TARGET_CHAT: Dict[int, int] = {}  # user_id -> chat_id

# ==========================
# Keyboards
# ==========================

def kb_main_admin() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("📣 Рассылка", callback_data="menu_broadcast"),
        InlineKeyboardButton("🏷 Разметить чат (/tag)", callback_data="menu_tag"),
        InlineKeyboardButton("🧩 Список чатов", callback_data="menu_chats"),
    )
    return kb

def kb_chat_list_for_tag(user_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)

    chats = list(CHATS.values())
    if not chats:
        kb.add(InlineKeyboardButton("⚠️ Нет чатов (добавь бота в группы)", callback_data="noop"))
        kb.add(InlineKeyboardButton("❌ Отмена", callback_data="tag_cancel"))
        return kb

    # максимум 40, чтобы не было слишком много кнопок
    chats = chats[:40]
    for ch in chats:
        cid = ch["id"]
        title = ch.get("title", str(cid))
        age = ch.get("age")
        level = ch.get("level")
        tags = []
        if age: tags.append(age)
        if level: tags.append(level)
        suffix = f" ({', '.join(tags)})" if tags else ""
        kb.add(InlineKeyboardButton(f"{title}{suffix}", callback_data=f"tag_chat_{cid}"))

    kb.add(InlineKeyboardButton("❌ Отмена", callback_data="tag_cancel"))
    return kb

def kb_age_picker(user_id: int) -> InlineKeyboardMarkup:
    selected = TAG_TARGET_CHAT.get(user_id)
    kb = InlineKeyboardMarkup(row_width=1)

    # возрастные кнопки + "выбрать все" в конце
    for tag, label in AGE_TAGS:
        kb.add(InlineKeyboardButton(label, callback_data=f"tag_age_{tag}"))

    kb.add(InlineKeyboardButton("✅ Выбрать все (возраст)", callback_data="tag_age_all"))
    kb.add(InlineKeyboardButton("❌ Отмена", callback_data="tag_cancel"))
    return kb

def kb_level_picker(user_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)

    for tag, label in LEVEL_TAGS:
        kb.add(InlineKeyboardButton(label, callback_data=f"tag_level_{tag}"))

    kb.add(InlineKeyboardButton("✅ Выбрать все (уровни)", callback_data="tag_level_all"))
    kb.add(InlineKeyboardButton("❌ Отмена", callback_data="tag_cancel"))
    return kb

def kb_bc_filter(user_id: int) -> InlineKeyboardMarkup:
    ages = BC_SELECTED_AGES.get(user_id, set())
    levels = BC_SELECTED_LEVELS.get(user_id, set())
    kb = InlineKeyboardMarkup(row_width=1)

    kb.add(InlineKeyboardButton("—— Возраст (можно несколько) ——", callback_data="noop"))

    for tag, label in AGE_TAGS:
        mark = "✅" if tag in ages else "⬜"
        kb.add(InlineKeyboardButton(f"{mark} {label}", callback_data=f"bc_age_{tag}"))

    # ✅ выбрать все — в конце
    kb.add(InlineKeyboardButton("✅ Выбрать все возраста", callback_data="bc_age_all"))

    kb.add(InlineKeyboardButton("—— Уровень (можно несколько) ——", callback_data="noop"))

    for tag, label in LEVEL_TAGS:
        mark = "✅" if tag in levels else "⬜"
        kb.add(InlineKeyboardButton(f"{mark} {label}", callback_data=f"bc_level_{tag}"))

    # ✅ выбрать все — в конце
    kb.add(InlineKeyboardButton("✅ Выбрать все уровни", callback_data="bc_level_all"))

    kb.add(
        InlineKeyboardButton("➡️ Далее", callback_data="bc_filter_next"),
        InlineKeyboardButton("⚙️ Выбрать вручную чаты", callback_data="bc_manual_start"),
        InlineKeyboardButton("❌ Отмена", callback_data="bc_cancel"),
    )
    return kb

def kb_bc_manual(user_id: int) -> InlineKeyboardMarkup:
    selected = SELECTED_CHATS.get(user_id, set())
    kb = InlineKeyboardMarkup(row_width=1)

    chats_list = list(CHATS.values())[:40]
    if not chats_list:
        kb.add(InlineKeyboardButton("⚠️ Нет чатов (добавь бота в группы)", callback_data="noop"))
        kb.add(InlineKeyboardButton("❌ Отмена", callback_data="bc_cancel"))
        return kb

    for ch in chats_list:
        cid = ch["id"]
        title = ch.get("title", str(cid))
        mark = "✅" if cid in selected else "⬜"
        kb.add(InlineKeyboardButton(f"{mark} {title}", callback_data=f"bc_t_{cid}"))

    kb.add(
        InlineKeyboardButton("➡️ Далее", callback_data="bc_manual_next"),
        InlineKeyboardButton("❌ Отмена", callback_data="bc_cancel"),
    )
    return kb

# ==========================
# Chat filtering
# ==========================

def match_chat(chat: dict, ages: Set[str], levels: Set[str]) -> bool:
    # если не выбраны ages/levels -> не матч
    if not ages or not levels:
        return False
    return (chat.get("age") in ages) and (chat.get("level") in levels)

def get_chats_by_filter(ages: Set[str], levels: Set[str]) -> List[int]:
    result = []
    for ch in CHATS.values():
        if match_chat(ch, ages, levels):
            result.append(ch["id"])
    return result

# ==========================
# Startup
# ==========================

async def on_startup(dp: Dispatcher):
    await bot.delete_webhook(drop_pending_updates=True)
    logging.info("✅ Bot started polling")
    logging.info(f"OWNER_ID parsed = {OWNER_ID}")
    logging.info(f"Loaded chats: {len(CHATS)}")

# ==========================
# Commands
# ==========================

@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    await message.reply(
        "✅ Бот работает.\n"
        f"Ваш ID: <code>{message.from_user.id}</code>\n\n"
        "Команды:\n"
        "/broadcast — рассылка\n"
        "/tag — разметка чатов (возраст+уровень)\n"
        "/chats — список чатов\n",
        parse_mode="HTML",
        reply_markup=kb_main_admin()
    )

@dp.message_handler(commands=["chats"])
async def cmd_chats(message: types.Message):
    if not is_owner(message):
        await message.reply("⛔ Только владелец может смотреть список чатов.")
        return
    if not CHATS:
        await message.reply("Чатов пока нет. Добавь бота в группы.")
        return

    lines = ["📌 Чаты:"]
    for ch in CHATS.values():
        age = ch.get("age") or "-"
        level = ch.get("level") or "-"
        lines.append(f"- {ch['title']} | age={age} | level={level}")
    await message.reply("\n".join(lines))

@dp.message_handler(commands=["tag"])
async def cmd_tag(message: types.Message):
    if not is_owner(message):
        await message.reply("⛔ Эта команда только для владельца.")
        return

    TAG_STATE[message.from_user.id] = "tag_choose_chat"
    await message.reply(
        "🏷 Выбери чат, который нужно разметить:",
        reply_markup=kb_chat_list_for_tag(message.from_user.id)
    )

@dp.message_handler(commands=["broadcast"])
async def cmd_broadcast(message: types.Message):
    if not is_owner(message):
        await message.reply(
            "⛔ Эта команда только для владельца.\n\n"
            f"Ваш id: {message.from_user.id}\n"
            f"OWNER_ID в Railway сейчас: {OWNER_ID}"
        )
        return

    uid = message.from_user.id
    STATE[uid] = "bc_filter"
    BC_SELECTED_AGES[uid] = set()
    BC_SELECTED_LEVELS[uid] = set()

    await message.reply(
        "📣 Выбери фильтры для рассылки (можно комбо):",
        reply_markup=kb_bc_filter(uid)
    )

# ==========================
# Menu callbacks
# ==========================

@dp.callback_query_handler(lambda c: c.data == "menu_broadcast")
async def menu_broadcast(call: types.CallbackQuery):
    fake = types.Message(message_id=0, date=None, chat=call.message.chat, from_user=call.from_user)
    await cmd_broadcast(fake)

@dp.callback_query_handler(lambda c: c.data == "menu_tag")
async def menu_tag(call: types.CallbackQuery):
    fake = types.Message(message_id=0, date=None, chat=call.message.chat, from_user=call.from_user)
    await cmd_tag(fake)

@dp.callback_query_handler(lambda c: c.data == "menu_chats")
async def menu_chats(call: types.CallbackQuery):
    fake = types.Message(message_id=0, date=None, chat=call.message.chat, from_user=call.from_user)
    await cmd_chats(fake)

@dp.callback_query_handler(lambda c: c.data == "noop")
async def noop(call: types.CallbackQuery):
    await call.answer()

# ==========================
# TAG callbacks
# ==========================

@dp.callback_query_handler(lambda c: c.data == "tag_cancel")
async def tag_cancel(call: types.CallbackQuery):
    uid = call.from_user.id
    TAG_STATE.pop(uid, None)
    TAG_TARGET_CHAT.pop(uid, None)
    await call.message.edit_text("❌ Разметка отменена.")
    await call.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("tag_chat_"))
async def tag_choose_chat(call: types.CallbackQuery):
    uid = call.from_user.id
    if TAG_STATE.get(uid) != "tag_choose_chat":
        await call.answer("Неактуально")
        return

    chat_id = int(call.data.split("_")[-1])
    TAG_TARGET_CHAT[uid] = chat_id
    TAG_STATE[uid] = "tag_choose_age"

    ch = get_chat(chat_id)
    title = ch.get("title") if ch else str(chat_id)

    await call.message.edit_text(
        f"Чат: {title}\n\nВыбери возраст:",
        reply_markup=kb_age_picker(uid)
    )
    await call.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("tag_age_"))
async def tag_set_age(call: types.CallbackQuery):
    uid = call.from_user.id
    if TAG_STATE.get(uid) != "tag_choose_age":
        await call.answer("Неактуально")
        return

    chat_id = TAG_TARGET_CHAT.get(uid)
    if not chat_id:
        await call.answer("Ошибка: чат не выбран")
        return

    age_tag = call.data.split("_")[-1]

    if age_tag == "all":
        await call.answer("Выбери конкретный возраст", show_alert=True)
        return

    ch = get_chat(chat_id)
    if not ch:
        await call.answer("Чат не найден")
        return

    ch["age"] = age_tag
    CHATS[str(chat_id)] = ch
    save_chats(CHATS)

    TAG_STATE[uid] = "tag_choose_level"
    await call.message.edit_text(
        f"✅ Возраст сохранён: {age_tag}\n\nТеперь выбери уровень:",
        reply_markup=kb_level_picker(uid)
    )
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "tag_age_all")
async def tag_age_all(call: types.CallbackQuery):
    # На самом деле для одного чата нельзя "все возраста"
    await call.answer("Для разметки чата нужно выбрать один возраст.", show_alert=True)

@dp.callback_query_handler(lambda c: c.data.startswith("tag_level_"))
async def tag_set_level(call: types.CallbackQuery):
    uid = call.from_user.id
    if TAG_STATE.get(uid) != "tag_choose_level":
        await call.answer("Неактуально")
        return

    chat_id = TAG_TARGET_CHAT.get(uid)
    if not chat_id:
        await call.answer("Ошибка: чат не выбран")
        return

    level_tag = call.data.split("_")[-1]

    if level_tag == "all":
        await call.answer("Выбери конкретный уровень", show_alert=True)
        return

    ch = get_chat(chat_id)
    if not ch:
        await call.answer("Чат не найден")
        return

    ch["level"] = level_tag
    CHATS[str(chat_id)] = ch
    save_chats(CHATS)

    TAG_STATE.pop(uid, None)
    TAG_TARGET_CHAT.pop(uid, None)

    await call.message.edit_text(
        f"✅ Разметка сохранена!\n\n"
        f"age={ch.get('age')}\n"
        f"level={ch.get('level')}"
    )
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "tag_level_all")
async def tag_level_all(call: types.CallbackQuery):
    await call.answer("Для разметки чата нужно выбрать один уровень.", show_alert=True)

# ==========================
# BROADCAST FILTER callbacks
# ==========================

@dp.callback_query_handler(lambda c: c.data == "bc_cancel")
async def bc_cancel(call: types.CallbackQuery):
    uid = call.from_user.id
    STATE.pop(uid, None)
    SELECTED_CHATS.pop(uid, None)
    BC_SELECTED_AGES.pop(uid, None)
    BC_SELECTED_LEVELS.pop(uid, None)
    await call.message.edit_text("❌ Рассылка отменена.")
    await call.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("bc_age_"))
async def bc_toggle_age(call: types.CallbackQuery):
    uid = call.from_user.id
    if STATE.get(uid) != "bc_filter":
        await call.answer("Неактуально")
        return

    tag = call.data.split("_")[-1]
    ages = BC_SELECTED_AGES.setdefault(uid, set())

    if tag == "all":
        return

    if tag in ages:
        ages.remove(tag)
    else:
        ages.add(tag)

    await call.message.edit_reply_markup(reply_markup=kb_bc_filter(uid))
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "bc_age_all")
async def bc_age_all(call: types.CallbackQuery):
    uid = call.from_user.id
    if STATE.get(uid) != "bc_filter":
        await call.answer("Неактуально")
        return

    ages = BC_SELECTED_AGES.setdefault(uid, set())
    all_tags = {t for t, _ in AGE_TAGS}

    # если уже выбраны все — сбрасываем, иначе ставим все
    if ages == all_tags:
        ages.clear()
    else:
        ages.clear()
        ages.update(all_tags)

    await call.message.edit_reply_markup(reply_markup=kb_bc_filter(uid))
    await call.answer("Ок")

@dp.callback_query_handler(lambda c: c.data.startswith("bc_level_"))
async def bc_toggle_level(call: types.CallbackQuery):
    uid = call.from_user.id
    if STATE.get(uid) != "bc_filter":
        await call.answer("Неактуально")
        return

    tag = call.data.split("_")[-1]
    levels = BC_SELECTED_LEVELS.setdefault(uid, set())

    if tag == "all":
        return

    if tag in levels:
        levels.remove(tag)
    else:
        levels.add(tag)

    await call.message.edit_reply_markup(reply_markup=kb_bc_filter(uid))
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "bc_level_all")
async def bc_level_all(call: types.CallbackQuery):
    uid = call.from_user.id
    if STATE.get(uid) != "bc_filter":
        await call.answer("Неактуально")
        return

    levels = BC_SELECTED_LEVELS.setdefault(uid, set())
    all_tags = {t for t, _ in LEVEL_TAGS}

    if levels == all_tags:
        levels.clear()
    else:
        levels.clear()
        levels.update(all_tags)

    await call.message.edit_reply_markup(reply_markup=kb_bc_filter(uid))
    await call.answer("Ок")

@dp.callback_query_handler(lambda c: c.data == "bc_filter_next")
async def bc_filter_next(call: types.CallbackQuery):
    uid = call.from_user.id
    if STATE.get(uid) != "bc_filter":
        await call.answer("Неактуально")
        return

    ages = BC_SELECTED_AGES.get(uid, set())
    levels = BC_SELECTED_LEVELS.get(uid, set())

    if not ages or not levels:
        await call.answer("Выбери минимум 1 возраст и 1 уровень", show_alert=True)
        return

    targets = get_chats_by_filter(ages, levels)

    if not targets:
        await call.answer("Нет чатов под выбранные фильтры. Разметь /tag", show_alert=True)
        return

    STATE[uid] = "bc_wait_msg"
    SELECTED_CHATS[uid] = set(targets)

    await call.message.edit_text(
        f"✅ Под фильтры подходит чатов: {len(targets)}\n\n"
        "Теперь пришли ОДНО сообщение для рассылки:\n"
        "💬 текст / 🖼 фото / 🎬 видео / 📎 файл"
    )
    await call.answer()

# ==========================
# BROADCAST MANUAL callbacks
# ==========================

@dp.callback_query_handler(lambda c: c.data == "bc_manual_start")
async def bc_manual_start(call: types.CallbackQuery):
    uid = call.from_user.id
    if STATE.get(uid) != "bc_filter":
        await call.answer("Неактуально")
        return

    STATE[uid] = "bc_manual"
    SELECTED_CHATS[uid] = set()

    await call.message.edit_text("⚙️ Выбери чаты вручную:", reply_markup=kb_bc_manual(uid))
    await call.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("bc_t_"))
async def bc_manual_toggle(call: types.CallbackQuery):
    uid = call.from_user.id
    if STATE.get(uid) != "bc_manual":
        await call.answer("Неактуально")
        return

    chat_id = int(call.data.split("_")[-1])
    selected = SELECTED_CHATS.setdefault(uid, set())

    if chat_id in selected:
        selected.remove(chat_id)
    else:
        selected.add(chat_id)

    await call.message.edit_reply_markup(reply_markup=kb_bc_manual(uid))
    await call.answer("Ок")

@dp.callback_query_handler(lambda c: c.data == "bc_manual_next")
async def bc_manual_next(call: types.CallbackQuery):
    uid = call.from_user.id
    if STATE.get(uid) != "bc_manual":
        await call.answer("Неактуально")
        return

    selected = list(SELECTED_CHATS.get(uid, set()))
    if not selected:
        await call.answer("Выбери хотя бы один чат", show_alert=True)
        return

    STATE[uid] = "bc_wait_msg"
    await call.message.edit_text(
        f"✅ Выбрано чатов: {len(selected)}\n\n"
        "Теперь пришли ОДНО сообщение для рассылки:\n"
        "💬 текст / 🖼 фото / 🎬 видео / 📎 файл"
    )
    await call.answer()

# ==========================
# Any message: store chats + broadcast send
# ==========================

@dp.message_handler(content_types=types.ContentTypes.ANY)
async def any_message(message: types.Message):
    # сохраняем чаты при любом сообщении в группе
    if message.chat.type in ("group", "supergroup"):
        upsert_chat(message.chat)

    # если не owner — игнор
    if not message.from_user or not is_owner_user_id(message.from_user.id):
        return

    uid = message.from_user.id

    # ждём сообщение для рассылки
    if STATE.get(uid) != "bc_wait_msg":
        return

    chat_ids = list(SELECTED_CHATS.get(uid, set()))

    # очищаем состояние
    STATE.pop(uid, None)
    SELECTED_CHATS.pop(uid, None)
    BC_SELECTED_AGES.pop(uid, None)
    BC_SELECTED_LEVELS.pop(uid, None)

    await message.reply(f"🚀 Начинаю рассылку в {len(chat_ids)} чатов...")

    ok = 0
    fail = 0

    for cid in chat_ids:
        try:
            await send_to_chat(cid, message)
            ok += 1
        except Exception as e:
            fail += 1
            logging.error(f"Failed to send to {cid}: {e}")

        # антифлуд
        await asyncio.sleep(1.0)

    await message.reply(f"✅ Готово!\nУспешно: {ok}\nОшибок: {fail}")

# ==========================
# Run
# ==========================

if __name__ == "__main__":
    executor.start_polling(dp, on_startup=on_startup)
