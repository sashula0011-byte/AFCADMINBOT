import os
import json
import asyncio
import logging
from typing import Dict, List, Set

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from dotenv import load_dotenv

# Не перетираем переменные Railway
load_dotenv()

logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID_RAW = os.getenv("OWNER_ID", "0")

try:
    OWNER_ID = int(OWNER_ID_RAW)
except:
    OWNER_ID = 0

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN missing in env")

DATA_FILE = "chats.json"  # хранение списка чатов в репозитории Railway container

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)


# ==========================
# Storage for chats
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
    # сохраняем только группы/супергруппы
    if chat.type not in ("group", "supergroup"):
        return
    cid = str(chat.id)
    CHATS[cid] = {
        "id": chat.id,
        "title": chat.title or str(chat.id),
        "type": chat.type,
    }
    save_chats(CHATS)


# ==========================
# Broadcast FSM (простая)
# ==========================

# user_id -> state
STATE: Dict[int, str] = {}  # choosing | waiting_message
SELECTED: Dict[int, Set[int]] = {}  # user_id -> set(chat_id)

def is_owner(message: types.Message) -> bool:
    return OWNER_ID != 0 and message.from_user and message.from_user.id == OWNER_ID


def kb_chat_picker(user_id: int) -> InlineKeyboardMarkup:
    selected = SELECTED.get(user_id, set())
    kb = InlineKeyboardMarkup(row_width=1)

    # показываем максимум 30 чатов, чтобы Telegram не ругался
    chats_list = list(CHATS.values())[:30]

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
        InlineKeyboardButton("➡️ Далее", callback_data="bc_next"),
        InlineKeyboardButton("❌ Отмена", callback_data="bc_cancel"),
    )
    return kb


async def send_to_chat(chat_id: int, origin: types.Message):
    # Текст
    if origin.text:
        await bot.send_message(chat_id, origin.text)

    # Фото (берём самое большое)
    elif origin.photo:
        file_id = origin.photo[-1].file_id
        caption = origin.caption or ""
        await bot.send_photo(chat_id, file_id, caption=caption)

    # Видео
    elif origin.video:
        file_id = origin.video.file_id
        caption = origin.caption or ""
        await bot.send_video(chat_id, file_id, caption=caption)

    # Документ
    elif origin.document:
        file_id = origin.document.file_id
        caption = origin.caption or ""
        await bot.send_document(chat_id, file_id, caption=caption)

    # Иначе
    else:
        await bot.send_message(chat_id, "⚠️ Этот тип сообщения пока не поддерживается.")


# ==========================
# Startup
# ==========================

async def on_startup(dp: Dispatcher):
    await bot.delete_webhook(drop_pending_updates=True)
    logging.info("✅ Bot started polling")
    logging.info(f"OWNER_ID parsed = {OWNER_ID}")
    logging.info(f"Loaded chats: {len(CHATS)}")


# ==========================
# Handlers
# ==========================

@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    await message.reply(
        "✅ Бот работает.\n"
        f"Ваш ID: <code>{message.from_user.id}</code>\n\n"
        "Команды:\n"
        "/broadcast — рассылка\n"
        "/chats — показать чаты\n",
        parse_mode="HTML"
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
        lines.append(f"- {ch['title']} ({ch['id']})")
    await message.reply("\n".join(lines))


@dp.message_handler(commands=["broadcast"])
async def cmd_broadcast(message: types.Message):
    if not is_owner(message):
        await message.reply(
            "⛔ Эта команда только для владельца.\n\n"
            f"Ваш id: {message.from_user.id}\n"
            f"OWNER_ID в Railway сейчас: {OWNER_ID}"
        )
        return

    STATE[message.from_user.id] = "choosing"
    SELECTED[message.from_user.id] = set()

    await message.reply(
        "📣 Выбери чаты для рассылки:",
        reply_markup=kb_chat_picker(message.from_user.id)
    )


@dp.callback_query_handler(lambda c: c.data.startswith("bc_t_"))
async def cb_toggle_chat(call: types.CallbackQuery):
    user_id = call.from_user.id

    if STATE.get(user_id) != "choosing":
        await call.answer("Неактуально", show_alert=False)
        return

    chat_id = int(call.data.split("_")[-1])
    selected = SELECTED.setdefault(user_id, set())

    if chat_id in selected:
        selected.remove(chat_id)
    else:
        selected.add(chat_id)

    await call.answer("Ок")
    await call.message.edit_reply_markup(reply_markup=kb_chat_picker(user_id))


@dp.callback_query_handler(lambda c: c.data == "bc_cancel")
async def cb_cancel(call: types.CallbackQuery):
    user_id = call.from_user.id
    STATE.pop(user_id, None)
    SELECTED.pop(user_id, None)
    await call.message.edit_text("❌ Рассылка отменена.")
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "bc_next")
async def cb_next(call: types.CallbackQuery):
    user_id = call.from_user.id
    selected = list(SELECTED.get(user_id, set()))

    if not selected:
        await call.answer("Выбери хотя бы один чат", show_alert=True)
        return

    STATE[user_id] = "waiting_message"
    await call.message.edit_text(
        f"✅ Выбрано чатов: {len(selected)}\n\n"
        "Теперь пришли ОДНО сообщение для рассылки:\n"
        "💬 текст / 🖼 фото / 🎬 видео / 📎 файл"
    )
    await call.answer()


@dp.message_handler(content_types=types.ContentTypes.ANY)
async def any_message(message: types.Message):
    # сохраняем чаты при любом сообщении из группы
    if message.chat.type in ("group", "supergroup"):
        upsert_chat(message.chat)

    # если это не owner — игнор
    if not message.from_user or message.from_user.id != OWNER_ID:
        return

    # ждём сообщение для рассылки
    if STATE.get(message.from_user.id) != "waiting_message":
        return

    chat_ids = list(SELECTED.get(message.from_user.id, set()))
    STATE.pop(message.from_user.id, None)
    SELECTED.pop(message.from_user.id, None)

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


if __name__ == "__main__":
    executor.start_polling(dp, on_startup=on_startup)
