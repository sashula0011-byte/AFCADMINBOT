import os
import logging
from typing import List, Tuple

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from dotenv import load_dotenv

# Важно: НЕ override=True, чтобы Railway Variables не затирались локальным .env
load_dotenv()

logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OWNER_ID_RAW = os.getenv("OWNER_ID", "0")

try:
    OWNER_ID = int(OWNER_ID_RAW)
except Exception:
    OWNER_ID = 0

# Диагностика (без вывода токена целиком)
print("BOT_TOKEN exists:", "BOT_TOKEN" in os.environ, "length:", len(BOT_TOKEN or ""))
print("OWNER_ID from env =", repr(OWNER_ID_RAW), "parsed =", OWNER_ID)

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing. Add BOT_TOKEN variable in Railway.")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

# ======= HELPERS =======

def is_owner(message: types.Message) -> bool:
    return OWNER_ID != 0 and message.from_user and message.from_user.id == OWNER_ID


# ======= MIDDLEWARE / LOGGING =======

@dp.message_handler(content_types=types.ContentTypes.ANY)
async def log_all_messages(message: types.Message):
    # Логируем всё входящее, чтобы не гадать (можно потом убрать)
    try:
        uname = message.from_user.username if message.from_user else None
        uid = message.from_user.id if message.from_user else None
        chat_id = message.chat.id
        text = message.text or ""
        logging.info(f"INCOMING: chat={chat_id} user={uid} @{uname} text={text[:120]!r}")
    except Exception as e:
        logging.error(f"Logging error: {e}")

    # дальше дадим обработчикам команд сработать
    # чтобы не "съесть" команды, выходим:
    return


# ======= COMMANDS =======

@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    await message.reply(
        "✅ Бот запущен!\n"
        f"Ваш user_id: <code>{message.from_user.id}</code>\n\n"
        "Команды:\n"
        "/help\n"
        "/id\n"
        "/broadcast <текст>\n",
        parse_mode="HTML"
    )


@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    await message.reply(
        "📌 Доступные команды:\n"
        "/start — проверка работы\n"
        "/id — показать ваш Telegram ID\n"
        "/broadcast <текст> — рассылка (только владелец)\n"
    )


@dp.message_handler(commands=["id"])
async def cmd_id(message: types.Message):
    await message.reply(f"Ваш user_id: <code>{message.from_user.id}</code>", parse_mode="HTML")


@dp.message_handler(commands=["broadcast"])
async def cmd_broadcast(message: types.Message):
    # Владелец
    if not is_owner(message):
        await message.reply(
            "⛔ Эта команда только для владельца.\n\n"
            f"Ваш id: {message.from_user.id}\n"
            f"OWNER_ID в Railway сейчас: {OWNER_ID}\n"
            "➡️ Исправь переменную OWNER_ID и сделай Redeploy."
        )
        return

    text = message.get_args().strip()
    if not text:
        await message.reply("Напиши так:\n<code>/broadcast Всем привет!</code>", parse_mode="HTML")
        return

    # ПОКА: заглушка — отправляем только в этот же чат
    # Следующим шагом сделаем выбор чатов и настоящую рассылку.
    await message.reply(f"✅ Принял broadcast:\n\n{text}")


# ======= STARTUP =======

async def on_startup(dp: Dispatcher):
    # Обязательный фикс: если был webhook — polling не получал апдейты
    await bot.delete_webhook(drop_pending_updates=True)
    logging.info("✅ on_startup: webhook deleted, bot started polling")


if __name__ == "__main__":
    executor.start_polling(dp, on_startup=on_startup)
