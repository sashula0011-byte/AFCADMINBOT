import os
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor

BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing. Add BOT_TOKEN variable in Railway.")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

def is_owner(message: types.Message) -> bool:
    return OWNER_ID != 0 and message.from_user and message.from_user.id == OWNER_ID

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
    await message.reply(
        f"Ваш user_id: <code>{message.from_user.id}</code>",
        parse_mode="HTML"
    )

@dp.message_handler(commands=["broadcast"])
async def cmd_broadcast(message: types.Message):
    if not is_owner(message):
        await message.reply(
            "⛔ Эта команда только для владельца.\n\n"
            f"Ваш id: {message.from_user.id}\n"
            f"OWNER_ID в Railway сейчас: {OWNER_ID}\n"
            "➡️ Исправьте переменную OWNER_ID и сделайте Redeploy."
        )
        return
    text = message.get_args().strip()
    if not text:
        await message.reply(
            "Напишите так:\n<code>/broadcast Всем привет!</code>",
            parse_mode="HTML"
        )
        return
    await message.reply(f"✅ Принял broadcast:\n\n{text}")

async def on_startup(dp):
    await bot.delete_webhook(drop_pending_updates=True)
    logging.info("✅ on_startup: webhook deleted, bot started polling")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    executor.start_polling(dp, on_startup=on_startup)
