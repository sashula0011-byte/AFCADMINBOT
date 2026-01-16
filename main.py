import os
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor

BOT_TOKEN = os.getenv('BOT_TOKEN')
OWNER_ID = int(os.getenv('OWNER_ID', '0'))

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

@dp.message_handler(commands=['start'])
async def start_handler(message: types.Message):
    await message.reply(f"Ваш user_id: {message.from_user.id}")

@dp.message_handler(commands=['id'])
async def id_handler(message: types.Message):
    await message.reply(f"Ваш user_id: {message.from_user.id}")

@dp.message_handler(commands=['help'])
async def help_handler(message: types.Message):
    await message.reply(
        "/start — получить ID\n"
        "/id — вывести ваш ID\n"
        "/broadcast <текст> — разослать сообщение\n"
        "/help — список команд"
    )

@dp.message_handler(commands=['broadcast'])
async def broadcast_handler(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("Эта команда только для владельца.")
        return
    text = message.get_args()
    if not text:
        await message.reply("Укажите текст после команды, например: /broadcast Всем привет!")
        return
    # Здесь пока простая заглушка — отправка в тот же чат
    await message.reply(f"Готов к рассылке: {text}")

async def on_startup(dp):
    # Сбрасываем вебхук, если он установлен, чтобы бот перешёл в polling
    await bot.delete_webhook(drop_pending_updates=True)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    executor.start_polling(dp, on_startup=on_startup)
