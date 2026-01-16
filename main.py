import os
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor

BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))

# Debug prints to check environment variables
print("BOT_TOKEN exists:", BOT_TOKEN is not None, "length:", len(BOT_TOKEN) if BOT_TOKEN else 0)
print("OWNER_ID:", OWNER_ID)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

@dp.message_handler(commands=["start"])
async def start_handler(message: types.Message):
    await message.reply(f"Ваш user_id: {message.from_user.id}")

@dp.message_handler(commands=["broadcast"])
async def broadcast_handler(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    text = message.get_args()
    await message.reply("Рассылка пока не настроена в коде")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    executor.start_polling(dp)
