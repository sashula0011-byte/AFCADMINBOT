import os
import asyncio
import logging
from typing import Dict, Set, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
)
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID_RAW = os.getenv("OWNER_ID", "0")
DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("DATABASE_PUBLIC_URL")

try:
    OWNER_ID = int(OWNER_ID_RAW)
except Exception:
    OWNER_ID = 0

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN missing in env")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL missing in env (add reference from Postgres service)")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

# ==========================
# TAGS
# ==========================

AGE_TAGS: List[Tuple[str, str]] = [
    ("baby", "👶 Бейби"),
    ("kids", "🧒 Дети"),
    ("junior", "🧑‍🎓 Юниоры"),
    ("adult", "🧑 Взрослые"),
    ("mom", "🤱 Мамочки"),
]

LEVEL_TAGS: List[Tuple[str, str]] = [
    ("beginner", "🟢 Начинающие"),
    ("middle", "🟡 Продолжающие"),
    ("pro", "🔴 Профи"),
]

BRANCH_TAGS: List[Tuple[str, str]] = [
    ("krylatskoe", "📍 Крылатское"),
    ("odintsovo", "📍 Одинцово"),
]

ALL_AGE_TAGS = {t for t, _ in AGE_TAGS}
ALL_LEVEL_TAGS = {t for t, _ in LEVEL_TAGS}
ALL_BRANCH_TAGS = {t for t, _ in BRANCH_TAGS}

# ==========================
# DB
# ==========================

def db_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor, sslmode="require")


def db_init():
    """Создание таблицы + авто-миграции."""
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS chats (
                    chat_id BIGINT PRIMARY KEY,
                    title TEXT NOT NULL,
                    chat_type TEXT NOT NULL,
                    updated_at TIMESTAMP DEFAULT NOW()
                );
            """)
            cur.execute("ALTER TABLE chats ADD COLUMN IF NOT EXISTS branch TEXT;")
            cur.execute("ALTER TABLE chats ADD COLUMN IF NOT EXISTS age TEXT;")
            cur.execute("ALTER TABLE chats ADD COLUMN IF NOT EXISTS level TEXT;")
        conn.commit()


def db_upsert_chat(chat: types.Chat):
    if chat.type not in ("group", "supergroup"):
        return
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO chats (chat_id, title, chat_type, updated_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (chat_id) DO UPDATE
                SET title = EXCLUDED.title,
                    chat_type = EXCLUDED.chat_type,
                    updated_at = NOW();
            """, (chat.id, chat.title or str(chat.id), chat.type))
        conn.commit()


def db_get_chat(chat_id: int) -> Optional[dict]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM chats WHERE chat_id=%s;", (chat_id,))
            return cur.fetchone()


def db_get_all_chats() -> List[dict]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM chats ORDER BY title ASC;")
            return cur.fetchall()


def db_get_chats_by_branch(branch: str) -> List[dict]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM chats
                WHERE branch=%s
                ORDER BY title ASC;
            """, (branch,))
            return cur.fetchall()


def db_set_field(chat_id: int, field: str, value: Optional[str]):
    if field not in ("branch", "age", "level"):
        raise ValueError("bad field")
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE chats SET {field}=%s, updated_at=NOW() WHERE chat_id=%s;", (value, chat_id))
        conn.commit()


def db_get_next_missing_branch_chat() -> Optional[dict]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM chats
                WHERE branch IS NULL
                ORDER BY title ASC
                LIMIT 1;
            """)
            return cur.fetchone()


def db_get_next_missing_age_or_level_chat() -> Optional[dict]:
    """Для комбо-сценария: чат, где не заполнен возраст ИЛИ уровень."""
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM chats
                WHERE age IS NULL OR level IS NULL
                ORDER BY title ASC
                LIMIT 1;
            """)
            return cur.fetchone()


def db_get_chats_by_filter(branch: str, ages: Set[str], levels: Set[str]) -> List[int]:
    if not ages or not levels:
        return []
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT chat_id FROM chats
                WHERE branch=%s AND age = ANY(%s) AND level = ANY(%s)
                ORDER BY title ASC;
            """, (branch, list(ages), list(levels)))
            rows = cur.fetchall()
            return [int(r["chat_id"]) for r in rows]


# ==========================
# Helpers
# ==========================

def is_owner_user_id(user_id: int) -> bool:
    return OWNER_ID != 0 and user_id == OWNER_ID


def safe_title(chat_id: int) -> str:
    ch = db_get_chat(int(chat_id))
    return ch["title"] if ch and ch.get("title") else str(chat_id)


async def send_to_chat(chat_id: int, origin: types.Message):
    if origin.text:
        await bot.send_message(chat_id, origin.text)
    elif origin.photo:
        file_id = origin.photo[-1].file_id
        caption = origin.caption or ""
        await bot.send_photo(chat_id, file_id, caption=caption)
    elif origin.video:
        file_id = origin.video.file_id
        caption = origin.caption or ""
        await bot.send_video(chat_id, file_id, caption=caption)
    elif origin.document:
        file_id = origin.document.file_id
        caption = origin.caption or ""
        await bot.send_document(chat_id, file_id, caption=caption)
    else:
        await bot.send_message(chat_id, "⚠️ Этот тип сообщения пока не поддерживается.")


def chunk_list(items: List[dict], size: int) -> List[List[dict]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


# ==========================
# STATES
# ==========================

STATE: Dict[int, str] = {}

# broadcast
BC_SELECTED_BRANCH: Dict[int, str] = {}
BC_SELECTED_AGES: Dict[int, Set[str]] = {}
BC_SELECTED_LEVELS: Dict[int, Set[str]] = {}
BC_TARGET_CHATS: Dict[int, Set[int]] = {}
BC_MANUAL_SELECTED: Dict[int, Set[int]] = {}
BC_MANUAL_PAGE: Dict[int, int] = {}

# branch tagging sequential
BR_STATE: Dict[int, str] = {}
BR_TARGET_CHAT: Dict[int, int] = {}
BR_AUTO_NEXT: Dict[int, bool] = {}

# combo tagging (age+level)
AL_STATE: Dict[int, str] = {}              # "al_choose_age" | "al_choose_level"
AL_TARGET_CHAT: Dict[int, int] = {}
AL_TEMP_AGE: Dict[int, str] = {}
AL_AUTO_NEXT: Dict[int, bool] = {}

# edit tags flow
EDIT_STATE: Dict[int, str] = {}            # "edit_choose_branch" | "edit_pick_chat" | "edit_menu" | ...
EDIT_BRANCH: Dict[int, str] = {}           # "all" or branch tag
EDIT_PAGE: Dict[int, int] = {}
EDIT_CHAT: Dict[int, int] = {}


# ==========================
# Keyboards: Reply (bottom)
# ==========================

def kb_bottom_menu() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton("📣 Рассылка"))
    kb.add(KeyboardButton("🏢 Разметка филиала"))
    kb.add(KeyboardButton("🏷 Разметка возраст+уровень"))
    kb.add(KeyboardButton("🛠 Изменить теги"))
    kb.add(KeyboardButton("🔄 Обновить меню"))
    kb.add(KeyboardButton("🙈 Скрыть меню"))
    return kb


# ==========================
# Keyboards: Inline
# ==========================

def kb_main_admin() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("📣 Рассылка", callback_data="menu_broadcast"),
        InlineKeyboardButton("🏢 Разметка филиала (следующий)", callback_data="menu_branch_next_missing"),
        InlineKeyboardButton("🏷 Разметка возраст+уровень (следующий)", callback_data="menu_agelevel_next_missing"),
        InlineKeyboardButton("🛠 Изменить теги чата", callback_data="menu_edit_tags"),
    )
    return kb


def kb_branch_picker(prefix: str, cancel_cb: str, include_all: bool = False) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    if include_all:
        kb.add(InlineKeyboardButton("🌐 Все филиалы", callback_data=f"{prefix}_all"))
    for tag, label in BRANCH_TAGS:
        kb.add(InlineKeyboardButton(label, callback_data=f"{prefix}_{tag}"))
    kb.add(InlineKeyboardButton("❌ Отмена", callback_data=cancel_cb))
    return kb


def kb_age_picker(prefix: str, cancel_cb: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    for tag, label in AGE_TAGS:
        kb.add(InlineKeyboardButton(label, callback_data=f"{prefix}_{tag}"))
    kb.add(InlineKeyboardButton("❌ Отмена", callback_data=cancel_cb))
    return kb


def kb_level_picker(prefix: str, cancel_cb: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    for tag, label in LEVEL_TAGS:
        kb.add(InlineKeyboardButton(label, callback_data=f"{prefix}_{tag}"))
    kb.add(InlineKeyboardButton("❌ Отмена", callback_data=cancel_cb))
    return kb


def kb_broadcast_mode() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("🔎 Выбрать чаты по названию", callback_data="bc_mode_manual"),
        InlineKeyboardButton("🏷 Выбрать чаты по тегу", callback_data="bc_mode_tags"),
        InlineKeyboardButton("❌ Отмена", callback_data="bc_cancel"),
    )
    return kb


def kb_bc_confirm() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("✅ Подтвердить", callback_data="bc_confirm_send"),
        InlineKeyboardButton("❌ Отмена", callback_data="bc_cancel"),
    )
    return kb


def kb_bc_wait_cancel() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("❌ Отменить рассылку", callback_data="bc_cancel"))
    return kb


def kb_bc_age(user_id: int) -> InlineKeyboardMarkup:
    selected = BC_SELECTED_AGES.get(user_id, set())
    kb = InlineKeyboardMarkup(row_width=1)

    for tag, label in AGE_TAGS:
        mark = "✅" if tag in selected else "⬜"
        kb.add(InlineKeyboardButton(f"{mark} {label}", callback_data=f"bc_age_{tag}"))

    all_mark = "✅" if selected == ALL_AGE_TAGS else "⬜"
    kb.add(InlineKeyboardButton(f"{all_mark} ✅ Выбрать все возраста", callback_data="bc_age_all"))

    kb.add(
        InlineKeyboardButton("➡️ Далее", callback_data="bc_age_next"),
        InlineKeyboardButton("❌ Отмена", callback_data="bc_cancel"),
    )
    return kb


def kb_bc_level(user_id: int) -> InlineKeyboardMarkup:
    selected = BC_SELECTED_LEVELS.get(user_id, set())
    kb = InlineKeyboardMarkup(row_width=1)

    for tag, label in LEVEL_TAGS:
        mark = "✅" if tag in selected else "⬜"
        kb.add(InlineKeyboardButton(f"{mark} {label}", callback_data=f"bc_level_{tag}"))

    all_mark = "✅" if selected == ALL_LEVEL_TAGS else "⬜"
    kb.add(InlineKeyboardButton(f"{all_mark} ✅ Выбрать все уровни", callback_data="bc_level_all"))

    kb.add(
        InlineKeyboardButton("⬅️ Назад", callback_data="bc_level_back"),
        InlineKeyboardButton("➡️ Далее", callback_data="bc_level_next"),
        InlineKeyboardButton("❌ Отмена", callback_data="bc_cancel"),
    )
    return kb


def kb_bc_manual_pick(user_id: int) -> InlineKeyboardMarkup:
    branch = BC_SELECTED_BRANCH.get(user_id)
    chats = db_get_chats_by_branch(branch) if branch else []
    selected = BC_MANUAL_SELECTED.get(user_id, set())
    page = BC_MANUAL_PAGE.get(user_id, 0)

    per_page = 15
    pages = chunk_list(chats, per_page)
    if not pages:
        pages = [[]]

    page = max(0, min(page, len(pages) - 1))
    BC_MANUAL_PAGE[user_id] = page

    kb = InlineKeyboardMarkup(row_width=1)

    for ch in pages[page]:
        cid = int(ch["chat_id"])
        title = ch.get("title") or str(cid)
        mark = "✅" if cid in selected else "⬜"
        kb.add(InlineKeyboardButton(f"{mark} {title}", callback_data=f"bc_mpick_{cid}"))

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data="bc_mpage_prev"))
    nav.append(InlineKeyboardButton(f"📄 {page + 1}/{len(pages)}", callback_data="noop"))
    if page < len(pages) - 1:
        nav.append(InlineKeyboardButton("➡️", callback_data="bc_mpage_next"))
    kb.row(*nav)

    all_mark = "✅" if len(chats) > 0 and len(selected) == len(chats) else "⬜"
    kb.add(
        InlineKeyboardButton(f"{all_mark} ✅ Выбрать все", callback_data="bc_mpick_all"),
        InlineKeyboardButton("➡️ Далее", callback_data="bc_mpick_next"),
        InlineKeyboardButton("❌ Отмена", callback_data="bc_cancel"),
    )
    return kb


def kb_edit_chat_list(user_id: int) -> InlineKeyboardMarkup:
    branch = EDIT_BRANCH.get(user_id, "all")
    chats = db_get_all_chats() if branch == "all" else db_get_chats_by_branch(branch)

    page = EDIT_PAGE.get(user_id, 0)
    per_page = 15
    pages = chunk_list(chats, per_page)
    if not pages:
        pages = [[]]

    page = max(0, min(page, len(pages) - 1))
    EDIT_PAGE[user_id] = page

    kb = InlineKeyboardMarkup(row_width=1)
    for ch in pages[page]:
        cid = int(ch["chat_id"])
        title = ch.get("title") or str(cid)
        kb.add(InlineKeyboardButton(title, callback_data=f"edit_chat_{cid}"))

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data="edit_page_prev"))
    nav.append(InlineKeyboardButton(f"📄 {page + 1}/{len(pages)}", callback_data="noop"))
    if page < len(pages) - 1:
        nav.append(InlineKeyboardButton("➡️", callback_data="edit_page_next"))
    kb.row(*nav)

    kb.add(
        InlineKeyboardButton("⬅️ Назад", callback_data="edit_back_to_branch"),
        InlineKeyboardButton("❌ Отмена", callback_data="edit_cancel"),
    )
    return kb


def kb_edit_menu(user_id: int, chat_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("🏢 Изменить филиал", callback_data="edit_change_branch"),
        InlineKeyboardButton("🎂 Изменить возраст", callback_data="edit_change_age"),
        InlineKeyboardButton("🎯 Изменить уровень", callback_data="edit_change_level"),
        InlineKeyboardButton("🧹 Очистить возраст+уровень", callback_data="edit_clear_agelvl"),
        InlineKeyboardButton("🧹 Очистить филиал", callback_data="edit_clear_branch"),
        InlineKeyboardButton("⬅️ К списку чатов", callback_data="edit_back_to_list"),
        InlineKeyboardButton("❌ Закрыть", callback_data="edit_cancel"),
    )
    return kb


# ==========================
# Startup
# ==========================

async def on_startup(dp: Dispatcher):
    await bot.delete_webhook(drop_pending_updates=True)
    db_init()
    logging.info("✅ Bot started polling")
    logging.info(f"OWNER_ID parsed = {OWNER_ID}")


# ==========================
# Commands + Reply menu show/hide
# ==========================

@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    await message.reply(
        "✅ Бот работает.\n"
        f"Ваш ID: <code>{message.from_user.id}</code>\n\n"
        "Меню всегда внизу 👇",
        parse_mode="HTML",
        reply_markup=kb_bottom_menu()
    )
    await message.answer("Быстрые действия (inline):", reply_markup=kb_main_admin())


@dp.message_handler(commands=["menu"])
async def cmd_menu(message: types.Message):
    await message.reply("✅ Меню показано 👇", reply_markup=kb_bottom_menu())


@dp.message_handler(commands=["hide"])
async def cmd_hide(message: types.Message):
    await message.reply("🙈 Меню скрыто. Вернуть: /menu", reply_markup=ReplyKeyboardRemove())


@dp.message_handler(lambda m: m.chat.type == "private" and m.text == "🙈 Скрыть меню")
async def hide_menu_button(m: types.Message):
    await m.reply("🙈 Меню скрыто. Вернуть: /menu", reply_markup=ReplyKeyboardRemove())


@dp.message_handler(lambda m: m.chat.type == "private" and m.text == "🔄 Обновить меню")
async def refresh_menu_button(m: types.Message):
    await m.reply("🔄 Меню обновлено 👇", reply_markup=kb_bottom_menu())


# ==========================
# Bottom menu actions (ReplyKeyboard)
# ==========================

@dp.message_handler(lambda m: m.chat.type == "private" and m.text == "📣 Рассылка")
async def bottom_broadcast(m: types.Message):
    if not is_owner_user_id(m.from_user.id):
        await m.reply("⛔ Только владелец.")
        return

    uid = m.from_user.id
    STATE[uid] = "bc_choose_branch"
    BC_SELECTED_BRANCH.pop(uid, None)
    BC_SELECTED_AGES.pop(uid, None)
    BC_SELECTED_LEVELS.pop(uid, None)
    BC_TARGET_CHATS.pop(uid, None)
    BC_MANUAL_SELECTED.pop(uid, None)
    BC_MANUAL_PAGE.pop(uid, None)

    await m.reply(
        "📣 Выбери филиал для рассылки:",
        reply_markup=kb_branch_picker("bc_branch", "bc_cancel", include_all=False)
    )


@dp.message_handler(lambda m: m.chat.type == "private" and m.text == "🏢 Разметка филиала")
async def bottom_tag_branch(m: types.Message):
    if not is_owner_user_id(m.from_user.id):
        await m.reply("⛔ Только владелец.")
        return

    row = db_get_next_missing_branch_chat()
    if not row:
        await m.reply("✅ Нет групп без филиала.")
        return

    uid = m.from_user.id
    chat_id = int(row["chat_id"])
    title = row.get("title") or str(chat_id)

    BR_AUTO_NEXT[uid] = True
    BR_TARGET_CHAT[uid] = chat_id
    BR_STATE[uid] = "br_choose_branch"

    await m.reply(
        f"🏢 Назначаем филиал\nЧат: {title}\n\nВыбери филиал:",
        reply_markup=kb_branch_picker("br_branch", "br_cancel", include_all=False)
    )


@dp.message_handler(lambda m: m.chat.type == "private" and m.text == "🏷 Разметка возраст+уровень")
async def bottom_tag_agelvl(m: types.Message):
    if not is_owner_user_id(m.from_user.id):
        await m.reply("⛔ Только владелец.")
        return

    row = db_get_next_missing_age_or_level_chat()
    if not row:
        await m.reply("✅ Нет групп без разметки возраста/уровня.")
        return

    uid = m.from_user.id
    chat_id = int(row["chat_id"])
    title = row.get("title") or str(chat_id)

    AL_AUTO_NEXT[uid] = True
    AL_TARGET_CHAT[uid] = chat_id
    AL_TEMP_AGE.pop(uid, None)
    AL_STATE[uid] = "al_choose_age"

    await m.reply(
        f"🏷 Разметка возраст+уровень\nЧат: {title}\n\nСначала выбери возраст:",
        reply_markup=kb_age_picker("al_age", "al_cancel")
    )


@dp.message_handler(lambda m: m.chat.type == "private" and m.text == "🛠 Изменить теги")
async def bottom_edit_tags(m: types.Message):
    if not is_owner_user_id(m.from_user.id):
        await m.reply("⛔ Только владелец.")
        return

    uid = m.from_user.id
    EDIT_STATE[uid] = "edit_choose_branch"
    EDIT_BRANCH[uid] = "all"
    EDIT_PAGE[uid] = 0
    EDIT_CHAT.pop(uid, None)

    await m.reply(
        "🛠 Выбери филиал, в котором искать чат (или Все филиалы):",
        reply_markup=kb_branch_picker("edit_branch", "edit_cancel", include_all=True)
    )


# ==========================
# Common noop
# ==========================

@dp.callback_query_handler(lambda c: c.data == "noop")
async def noop(call: types.CallbackQuery):
    await call.answer()


# ==========================
# MENU INLINE: Broadcast start
# ==========================

@dp.callback_query_handler(lambda c: c.data == "menu_broadcast")
async def menu_broadcast(call: types.CallbackQuery):
    uid = call.from_user.id
    if not is_owner_user_id(uid):
        await call.answer("⛔ Только владелец", show_alert=True)
        return

    STATE[uid] = "bc_choose_branch"
    BC_SELECTED_BRANCH.pop(uid, None)
    BC_SELECTED_AGES.pop(uid, None)
    BC_SELECTED_LEVELS.pop(uid, None)
    BC_TARGET_CHATS.pop(uid, None)
    BC_MANUAL_SELECTED.pop(uid, None)
    BC_MANUAL_PAGE.pop(uid, None)

    await call.message.answer(
        "📣 Выбери филиал для рассылки:",
        reply_markup=kb_branch_picker("bc_branch", "bc_cancel", include_all=False)
    )
    await call.answer()


# ==========================
# MENU INLINE: Branch tagging next missing
# ==========================

@dp.callback_query_handler(lambda c: c.data == "menu_branch_next_missing")
async def menu_branch_next_missing(call: types.CallbackQuery):
    uid = call.from_user.id
    if not is_owner_user_id(uid):
        await call.answer("⛔ Только владелец", show_alert=True)
        return

    row = db_get_next_missing_branch_chat()
    if not row:
        await call.message.answer("✅ Нет групп без филиала.")
        await call.answer()
        return

    chat_id = int(row["chat_id"])
    title = row.get("title") or str(chat_id)

    BR_AUTO_NEXT[uid] = True
    BR_TARGET_CHAT[uid] = chat_id
    BR_STATE[uid] = "br_choose_branch"

    await call.message.answer(
        f"🏢 Назначаем филиал\nЧат: {title}\n\nВыбери филиал:",
        reply_markup=kb_branch_picker("br_branch", "br_cancel", include_all=False)
    )
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "br_cancel")
async def br_cancel(call: types.CallbackQuery):
    uid = call.from_user.id
    BR_STATE.pop(uid, None)
    BR_TARGET_CHAT.pop(uid, None)
    BR_AUTO_NEXT.pop(uid, None)
    try:
        await call.message.edit_text("❌ Разметка филиала отменена.")
    except Exception:
        await call.message.answer("❌ Разметка филиала отменена.")
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("br_branch_"))
async def br_set_branch(call: types.CallbackQuery):
    uid = call.from_user.id
    if BR_STATE.get(uid) != "br_choose_branch":
        await call.answer("Неактуально")
        return

    chat_id = BR_TARGET_CHAT.get(uid)
    if chat_id is None:
        await call.answer("Ошибка: чат не выбран", show_alert=True)
        return

    branch = call.data.replace("br_branch_", "").strip()
    if branch not in ALL_BRANCH_TAGS:
        await call.answer("Неизвестный филиал", show_alert=True)
        return

    db_set_field(int(chat_id), "branch", branch)

    title = safe_title(int(chat_id))

    BR_STATE.pop(uid, None)
    BR_TARGET_CHAT.pop(uid, None)

    await call.message.edit_text(f"✅ Филиал назначен!\n\n{title}\nbranch={branch}")
    await call.answer()

    if BR_AUTO_NEXT.get(uid):
        row = db_get_next_missing_branch_chat()
        if not row:
            await call.message.answer("✅ Все группы теперь имеют филиал!")
            BR_AUTO_NEXT.pop(uid, None)
            return

        chat_id2 = int(row["chat_id"])
        title2 = row.get("title") or str(chat_id2)

        BR_TARGET_CHAT[uid] = chat_id2
        BR_STATE[uid] = "br_choose_branch"

        await call.message.answer(
            f"🏢 Следующий чат:\n{title2}\n\nВыбери филиал:",
            reply_markup=kb_branch_picker("br_branch", "br_cancel", include_all=False)
        )


# ==========================
# MENU INLINE: Age+Level combo tagging
# ==========================

@dp.callback_query_handler(lambda c: c.data == "menu_agelevel_next_missing")
async def menu_agelevel_next_missing(call: types.CallbackQuery):
    uid = call.from_user.id
    if not is_owner_user_id(uid):
        await call.answer("⛔ Только владелец", show_alert=True)
        return

    row = db_get_next_missing_age_or_level_chat()
    if not row:
        await call.message.answer("✅ Нет групп без разметки возраста/уровня.")
        await call.answer()
        return

    chat_id = int(row["chat_id"])
    title = row.get("title") or str(chat_id)

    AL_AUTO_NEXT[uid] = True
    AL_TARGET_CHAT[uid] = chat_id
    AL_TEMP_AGE.pop(uid, None)
    AL_STATE[uid] = "al_choose_age"

    await call.message.answer(
        f"🏷 Разметка возраст+уровень\nЧат: {title}\n\nСначала выбери возраст:",
        reply_markup=kb_age_picker("al_age", "al_cancel")
    )
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "al_cancel")
async def al_cancel(call: types.CallbackQuery):
    uid = call.from_user.id
    AL_STATE.pop(uid, None)
    AL_TARGET_CHAT.pop(uid, None)
    AL_TEMP_AGE.pop(uid, None)
    AL_AUTO_NEXT.pop(uid, None)
    try:
        await call.message.edit_text("❌ Разметка возраст+уровень отменена.")
    except Exception:
        await call.message.answer("❌ Разметка возраст+уровень отменена.")
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("al_age_"))
async def al_pick_age(call: types.CallbackQuery):
    uid = call.from_user.id
    if AL_STATE.get(uid) != "al_choose_age":
        await call.answer("Неактуально")
        return

    age = call.data.replace("al_age_", "").strip()
    if age not in ALL_AGE_TAGS:
        await call.answer("Неизвестный возраст", show_alert=True)
        return

    AL_TEMP_AGE[uid] = age
    AL_STATE[uid] = "al_choose_level"

    chat_id = AL_TARGET_CHAT.get(uid)
    title = safe_title(int(chat_id)) if chat_id is not None else "чат"

    await call.message.edit_text(
        f"🏷 Разметка возраст+уровень\nЧат: {title}\n\nТеперь выбери уровень:",
        reply_markup=kb_level_picker("al_level", "al_cancel")
    )
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("al_level_"))
async def al_pick_level(call: types.CallbackQuery):
    uid = call.from_user.id
    if AL_STATE.get(uid) != "al_choose_level":
        await call.answer("Неактуально")
        return

    level = call.data.replace("al_level_", "").strip()
    if level not in ALL_LEVEL_TAGS:
        await call.answer("Неизвестный уровень", show_alert=True)
        return

    chat_id = AL_TARGET_CHAT.get(uid)
    age = AL_TEMP_AGE.get(uid)

    if chat_id is None or not age:
        await call.answer("Ошибка состояния", show_alert=True)
        return

    # сохраняем оба
    db_set_field(int(chat_id), "age", age)
    db_set_field(int(chat_id), "level", level)

    title = safe_title(int(chat_id))

    AL_STATE.pop(uid, None)
    AL_TARGET_CHAT.pop(uid, None)
    AL_TEMP_AGE.pop(uid, None)

    await call.message.edit_text(f"✅ Сохранено!\n\n{title}\nage={age}\nlevel={level}")
    await call.answer()

    if AL_AUTO_NEXT.get(uid):
        row = db_get_next_missing_age_or_level_chat()
        if not row:
            await call.message.answer("✅ Все группы теперь имеют возраст и уровень!")
            AL_AUTO_NEXT.pop(uid, None)
            return

        chat_id2 = int(row["chat_id"])
        title2 = row.get("title") or str(chat_id2)

        AL_TARGET_CHAT[uid] = chat_id2
        AL_STATE[uid] = "al_choose_age"
        AL_TEMP_AGE.pop(uid, None)

        await call.message.answer(
            f"🏷 Следующий чат:\n{title2}\n\nСначала выбери возраст:",
            reply_markup=kb_age_picker("al_age", "al_cancel")
        )


# ==========================
# MENU INLINE: Edit tags
# ==========================

@dp.callback_query_handler(lambda c: c.data == "menu_edit_tags")
async def menu_edit_tags(call: types.CallbackQuery):
    uid = call.from_user.id
    if not is_owner_user_id(uid):
        await call.answer("⛔ Только владелец", show_alert=True)
        return

    EDIT_STATE[uid] = "edit_choose_branch"
    EDIT_BRANCH[uid] = "all"
    EDIT_PAGE[uid] = 0
    EDIT_CHAT.pop(uid, None)

    await call.message.answer(
        "🛠 Выбери филиал, в котором искать чат (или Все филиалы):",
        reply_markup=kb_branch_picker("edit_branch", "edit_cancel", include_all=True)
    )
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "edit_cancel")
async def edit_cancel(call: types.CallbackQuery):
    uid = call.from_user.id
    EDIT_STATE.pop(uid, None)
    EDIT_BRANCH.pop(uid, None)
    EDIT_PAGE.pop(uid, None)
    EDIT_CHAT.pop(uid, None)
    try:
        await call.message.edit_text("❌ Редактирование тегов отменено.")
    except Exception:
        await call.message.answer("❌ Редактирование тегов отменено.")
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("edit_branch_"))
async def edit_choose_branch(call: types.CallbackQuery):
    uid = call.from_user.id
    if EDIT_STATE.get(uid) != "edit_choose_branch":
        await call.answer("Неактуально")
        return

    branch = call.data.replace("edit_branch_", "").strip()
    if branch != "all" and branch not in ALL_BRANCH_TAGS:
        await call.answer("Неизвестный филиал", show_alert=True)
        return

    EDIT_BRANCH[uid] = branch
    EDIT_STATE[uid] = "edit_pick_chat"
    EDIT_PAGE[uid] = 0

    await call.message.edit_text(
        "🛠 Выбери чат для изменения тегов:",
        reply_markup=kb_edit_chat_list(uid)
    )
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "edit_page_prev")
async def edit_page_prev(call: types.CallbackQuery):
    uid = call.from_user.id
    EDIT_PAGE[uid] = max(0, EDIT_PAGE.get(uid, 0) - 1)
    await call.message.edit_reply_markup(reply_markup=kb_edit_chat_list(uid))
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "edit_page_next")
async def edit_page_next(call: types.CallbackQuery):
    uid = call.from_user.id
    EDIT_PAGE[uid] = EDIT_PAGE.get(uid, 0) + 1
    await call.message.edit_reply_markup(reply_markup=kb_edit_chat_list(uid))
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "edit_back_to_branch")
async def edit_back_to_branch(call: types.CallbackQuery):
    uid = call.from_user.id
    EDIT_STATE[uid] = "edit_choose_branch"
    EDIT_CHAT.pop(uid, None)
    await call.message.edit_text(
        "🛠 Выбери филиал, в котором искать чат (или Все филиалы):",
        reply_markup=kb_branch_picker("edit_branch", "edit_cancel", include_all=True)
    )
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("edit_chat_"))
async def edit_pick_chat(call: types.CallbackQuery):
    uid = call.from_user.id
    if EDIT_STATE.get(uid) != "edit_pick_chat":
        await call.answer("Неактуально")
        return

    chat_id_str = call.data.replace("edit_chat_", "").strip()
    if not chat_id_str.lstrip("-").isdigit():
        await call.answer("Ошибка id", show_alert=True)
        return

    chat_id = int(chat_id_str)
    ch = db_get_chat(chat_id)
    if not ch:
        await call.answer("Чат не найден в базе", show_alert=True)
        return

    EDIT_CHAT[uid] = chat_id
    EDIT_STATE[uid] = "edit_menu"

    title = ch.get("title") or str(chat_id)
    branch = ch.get("branch") or "—"
    age = ch.get("age") or "—"
    level = ch.get("level") or "—"

    await call.message.edit_text(
        f"🛠 Редактирование тегов\n\n"
        f"Чат: {title}\n"
        f"Филиал: {branch}\n"
        f"Возраст: {age}\n"
        f"Уровень: {level}\n\n"
        f"Что меняем?",
        reply_markup=kb_edit_menu(uid, chat_id)
    )
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "edit_back_to_list")
async def edit_back_to_list(call: types.CallbackQuery):
    uid = call.from_user.id
    EDIT_STATE[uid] = "edit_pick_chat"
    await call.message.edit_text(
        "🛠 Выбери чат для изменения тегов:",
        reply_markup=kb_edit_chat_list(uid)
    )
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "edit_change_branch")
async def edit_change_branch(call: types.CallbackQuery):
    uid = call.from_user.id
    if EDIT_STATE.get(uid) != "edit_menu":
        await call.answer("Неактуально")
        return
    EDIT_STATE[uid] = "edit_set_branch"
    await call.message.edit_text(
        "🏢 Выбери новый филиал:",
        reply_markup=kb_branch_picker("edit_setbranch", "edit_cancel", include_all=False)
    )
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "edit_change_age")
async def edit_change_age(call: types.CallbackQuery):
    uid = call.from_user.id
    if EDIT_STATE.get(uid) != "edit_menu":
        await call.answer("Неактуально")
        return
    EDIT_STATE[uid] = "edit_set_age"
    await call.message.edit_text(
        "🎂 Выбери новый возраст:",
        reply_markup=kb_age_picker("edit_setage", "edit_cancel")
    )
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "edit_change_level")
async def edit_change_level(call: types.CallbackQuery):
    uid = call.from_user.id
    if EDIT_STATE.get(uid) != "edit_menu":
        await call.answer("Неактуально")
        return
    EDIT_STATE[uid] = "edit_set_level"
    await call.message.edit_text(
        "🎯 Выбери новый уровень:",
        reply_markup=kb_level_picker("edit_setlevel", "edit_cancel")
    )
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "edit_clear_agelvl")
async def edit_clear_agelvl(call: types.CallbackQuery):
    uid = call.from_user.id
    chat_id = EDIT_CHAT.get(uid)
    if EDIT_STATE.get(uid) != "edit_menu" or chat_id is None:
        await call.answer("Неактуально")
        return

    db_set_field(int(chat_id), "age", None)
    db_set_field(int(chat_id), "level", None)

    title = safe_title(int(chat_id))
    await call.message.edit_text(
        f"✅ Очищено возраст+уровень\n\nЧат: {title}\n\nЧто дальше?",
        reply_markup=kb_edit_menu(uid, int(chat_id))
    )
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "edit_clear_branch")
async def edit_clear_branch(call: types.CallbackQuery):
    uid = call.from_user.id
    chat_id = EDIT_CHAT.get(uid)
    if EDIT_STATE.get(uid) != "edit_menu" or chat_id is None:
        await call.answer("Неактуально")
        return

    db_set_field(int(chat_id), "branch", None)

    title = safe_title(int(chat_id))
    await call.message.edit_text(
        f"✅ Очищен филиал\n\nЧат: {title}\n\nЧто дальше?",
        reply_markup=kb_edit_menu(uid, int(chat_id))
    )
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("edit_setbranch_"))
async def edit_set_branch(call: types.CallbackQuery):
    uid = call.from_user.id
    if EDIT_STATE.get(uid) != "edit_set_branch":
        await call.answer("Неактуально")
        return
    chat_id = EDIT_CHAT.get(uid)
    if chat_id is None:
        await call.answer("Чат не выбран", show_alert=True)
        return

    branch = call.data.replace("edit_setbranch_", "").strip()
    if branch not in ALL_BRANCH_TAGS:
        await call.answer("Неизвестный филиал", show_alert=True)
        return

    db_set_field(int(chat_id), "branch", branch)

    EDIT_STATE[uid] = "edit_menu"
    title = safe_title(int(chat_id))
    await call.message.edit_text(
        f"✅ Филиал обновлён\n\nЧат: {title}\n\nЧто дальше?",
        reply_markup=kb_edit_menu(uid, int(chat_id))
    )
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("edit_setage_"))
async def edit_set_age(call: types.CallbackQuery):
    uid = call.from_user.id
    if EDIT_STATE.get(uid) != "edit_set_age":
        await call.answer("Неактуально")
        return
    chat_id = EDIT_CHAT.get(uid)
    if chat_id is None:
        await call.answer("Чат не выбран", show_alert=True)
        return

    age = call.data.replace("edit_setage_", "").strip()
    if age not in ALL_AGE_TAGS:
        await call.answer("Неизвестный возраст", show_alert=True)
        return

    db_set_field(int(chat_id), "age", age)

    EDIT_STATE[uid] = "edit_menu"
    title = safe_title(int(chat_id))
    await call.message.edit_text(
        f"✅ Возраст обновлён\n\nЧат: {title}\n\nЧто дальше?",
        reply_markup=kb_edit_menu(uid, int(chat_id))
    )
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("edit_setlevel_"))
async def edit_set_level(call: types.CallbackQuery):
    uid = call.from_user.id
    if EDIT_STATE.get(uid) != "edit_set_level":
        await call.answer("Неактуально")
        return
    chat_id = EDIT_CHAT.get(uid)
    if chat_id is None:
        await call.answer("Чат не выбран", show_alert=True)
        return

    level = call.data.replace("edit_setlevel_", "").strip()
    if level not in ALL_LEVEL_TAGS:
        await call.answer("Неизвестный уровень", show_alert=True)
        return

    db_set_field(int(chat_id), "level", level)

    EDIT_STATE[uid] = "edit_menu"
    title = safe_title(int(chat_id))
    await call.message.edit_text(
        f"✅ Уровень обновлён\n\nЧат: {title}\n\nЧто дальше?",
        reply_markup=kb_edit_menu(uid, int(chat_id))
    )
    await call.answer()


# ==========================
# Broadcast flow
# ==========================

@dp.callback_query_handler(lambda c: c.data == "bc_cancel")
async def bc_cancel(call: types.CallbackQuery):
    uid = call.from_user.id
    STATE.pop(uid, None)
    BC_SELECTED_BRANCH.pop(uid, None)
    BC_SELECTED_AGES.pop(uid, None)
    BC_SELECTED_LEVELS.pop(uid, None)
    BC_TARGET_CHATS.pop(uid, None)
    BC_MANUAL_SELECTED.pop(uid, None)
    BC_MANUAL_PAGE.pop(uid, None)

    try:
        await call.message.edit_text("❌ Рассылка отменена.")
    except Exception:
        await call.message.answer("❌ Рассылка отменена.")
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("bc_branch_"))
async def bc_choose_branch(call: types.CallbackQuery):
    uid = call.from_user.id
    if STATE.get(uid) != "bc_choose_branch":
        await call.answer("Неактуально")
        return

    branch = call.data.replace("bc_branch_", "").strip()
    if branch not in ALL_BRANCH_TAGS:
        await call.answer("Неизвестный филиал", show_alert=True)
        return

    BC_SELECTED_BRANCH[uid] = branch
    STATE[uid] = "bc_choose_mode"

    await call.message.edit_text(
        f"✅ Филиал выбран: {branch}\n\nТеперь выбери как выбирать чаты:",
        reply_markup=kb_broadcast_mode()
    )
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "bc_mode_manual")
async def bc_mode_manual(call: types.CallbackQuery):
    uid = call.from_user.id
    if not is_owner_user_id(uid):
        await call.answer("⛔", show_alert=True)
        return
    if not BC_SELECTED_BRANCH.get(uid):
        await call.answer("Сначала выбери филиал", show_alert=True)
        return

    STATE[uid] = "bc_manual_pick"
    BC_MANUAL_SELECTED[uid] = set()
    BC_MANUAL_PAGE[uid] = 0

    await call.message.edit_text(
        "🔎 Выбор чатов вручную.\n\nОтмечай нужные чаты ✅",
        reply_markup=kb_bc_manual_pick(uid)
    )
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "bc_mode_tags")
async def bc_mode_tags(call: types.CallbackQuery):
    uid = call.from_user.id
    if not is_owner_user_id(uid):
        await call.answer("⛔", show_alert=True)
        return
    if not BC_SELECTED_BRANCH.get(uid):
        await call.answer("Сначала выбери филиал", show_alert=True)
        return

    STATE[uid] = "bc_age"
    BC_SELECTED_AGES[uid] = set()
    BC_SELECTED_LEVELS[uid] = set()

    await call.message.edit_text("🏷 Выбери возраст:", reply_markup=kb_bc_age(uid))
    await call.answer()


@dp.callback_query_handler(
    lambda c: c.data.startswith("bc_mpick_") and c.data.split("_")[-1].lstrip("-").isdigit()
)
async def bc_mpick_toggle(call: types.CallbackQuery):
    uid = call.from_user.id
    if STATE.get(uid) != "bc_manual_pick":
        await call.answer("Неактуально")
        return

    chat_id = int(call.data.split("_")[-1])
    selected = BC_MANUAL_SELECTED.setdefault(uid, set())

    if chat_id in selected:
        selected.remove(chat_id)
    else:
        selected.add(chat_id)

    await call.message.edit_reply_markup(reply_markup=kb_bc_manual_pick(uid))
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "bc_mpage_prev")
async def bc_mpage_prev(call: types.CallbackQuery):
    uid = call.from_user.id
    BC_MANUAL_PAGE[uid] = max(0, BC_MANUAL_PAGE.get(uid, 0) - 1)
    await call.message.edit_reply_markup(reply_markup=kb_bc_manual_pick(uid))
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "bc_mpage_next")
async def bc_mpage_next(call: types.CallbackQuery):
    uid = call.from_user.id
    BC_MANUAL_PAGE[uid] = BC_MANUAL_PAGE.get(uid, 0) + 1
    await call.message.edit_reply_markup(reply_markup=kb_bc_manual_pick(uid))
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "bc_mpick_all")
async def bc_mpick_all(call: types.CallbackQuery):
    uid = call.from_user.id
    if STATE.get(uid) != "bc_manual_pick":
        await call.answer("Неактуально")
        return

    branch = BC_SELECTED_BRANCH.get(uid)
    chats = db_get_chats_by_branch(branch) if branch else []
    all_ids = {int(ch["chat_id"]) for ch in chats}

    selected = BC_MANUAL_SELECTED.setdefault(uid, set())
    if selected == all_ids:
        selected.clear()
    else:
        selected.clear()
        selected.update(all_ids)

    await call.message.edit_reply_markup(reply_markup=kb_bc_manual_pick(uid))
    await call.answer("Ок")


@dp.callback_query_handler(lambda c: c.data == "bc_mpick_next")
async def bc_mpick_next(call: types.CallbackQuery):
    uid = call.from_user.id
    if STATE.get(uid) != "bc_manual_pick":
        await call.answer("Неактуально")
        return

    selected = BC_MANUAL_SELECTED.get(uid, set())
    if not selected:
        await call.answer("Выбери хотя бы 1 чат", show_alert=True)
        return

    BC_TARGET_CHATS[uid] = set(selected)
    STATE[uid] = "bc_confirm"

    lines = [f"• {safe_title(cid)}" for cid in selected]
    shown = lines[:30]
    extra = len(lines) - len(shown)
    list_text = "\n".join(shown)
    if extra > 0:
        list_text += f"\n… и ещё {extra} чатов"

    await call.message.edit_text(
        f"✅ Выбрано чатов: {len(selected)}\n\n"
        f"📋 Чаты:\n{list_text}\n\n"
        "Нажми ✅ Подтвердить, чтобы перейти к отправке сообщения.",
        reply_markup=kb_bc_confirm()
    )
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("bc_age_") and c.data not in ("bc_age_all", "bc_age_next"))
async def bc_toggle_age(call: types.CallbackQuery):
    uid = call.from_user.id
    if STATE.get(uid) != "bc_age":
        await call.answer("Неактуально")
        return
    tag = call.data.split("_")[-1]
    if tag not in ALL_AGE_TAGS:
        await call.answer("Неизвестный возраст", show_alert=True)
        return
    selected = BC_SELECTED_AGES.setdefault(uid, set())
    selected.remove(tag) if tag in selected else selected.add(tag)
    await call.message.edit_reply_markup(reply_markup=kb_bc_age(uid))
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "bc_age_all")
async def bc_age_all(call: types.CallbackQuery):
    uid = call.from_user.id
    if STATE.get(uid) != "bc_age":
        await call.answer("Неактуально")
        return
    selected = BC_SELECTED_AGES.setdefault(uid, set())
    if selected == ALL_AGE_TAGS:
        selected.clear()
    else:
        selected.clear()
        selected.update(ALL_AGE_TAGS)
    await call.message.edit_reply_markup(reply_markup=kb_bc_age(uid))
    await call.answer("Ок")


@dp.callback_query_handler(lambda c: c.data == "bc_age_next")
async def bc_age_next(call: types.CallbackQuery):
    uid = call.from_user.id
    if STATE.get(uid) != "bc_age":
        await call.answer("Неактуально")
        return
    ages = BC_SELECTED_AGES.get(uid, set())
    if not ages:
        await call.answer("Выбери минимум 1 возраст", show_alert=True)
        return
    STATE[uid] = "bc_level"
    await call.message.edit_text("🏷 Выбери уровень:", reply_markup=kb_bc_level(uid))
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("bc_level_") and c.data not in ("bc_level_all", "bc_level_back", "bc_level_next"))
async def bc_toggle_level(call: types.CallbackQuery):
    uid = call.from_user.id
    if STATE.get(uid) != "bc_level":
        await call.answer("Неактуально")
        return
    tag = call.data.split("_")[-1]
    if tag not in ALL_LEVEL_TAGS:
        await call.answer("Неизвестный уровень", show_alert=True)
        return
    selected = BC_SELECTED_LEVELS.setdefault(uid, set())
    selected.remove(tag) if tag in selected else selected.add(tag)
    await call.message.edit_reply_markup(reply_markup=kb_bc_level(uid))
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "bc_level_all")
async def bc_level_all(call: types.CallbackQuery):
    uid = call.from_user.id
    if STATE.get(uid) != "bc_level":
        await call.answer("Неактуально")
        return
    selected = BC_SELECTED_LEVELS.setdefault(uid, set())
    if selected == ALL_LEVEL_TAGS:
        selected.clear()
    else:
        selected.clear()
        selected.update(ALL_LEVEL_TAGS)
    await call.message.edit_reply_markup(reply_markup=kb_bc_level(uid))
    await call.answer("Ок")


@dp.callback_query_handler(lambda c: c.data == "bc_level_back")
async def bc_level_back(call: types.CallbackQuery):
    uid = call.from_user.id
    if STATE.get(uid) != "bc_level":
        await call.answer("Неактуально")
        return
    STATE[uid] = "bc_age"
    await call.message.edit_text("🏷 Выбери возраст:", reply_markup=kb_bc_age(uid))
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "bc_level_next")
async def bc_level_next(call: types.CallbackQuery):
    uid = call.from_user.id
    if STATE.get(uid) != "bc_level":
        await call.answer("Неактуально")
        return

    branch = BC_SELECTED_BRANCH.get(uid)
    if not branch:
        await call.answer("Сначала выбери филиал", show_alert=True)
        return

    ages = BC_SELECTED_AGES.get(uid, set())
    levels = BC_SELECTED_LEVELS.get(uid, set())
    if not levels:
        await call.answer("Выбери минимум 1 уровень", show_alert=True)
        return

    targets = db_get_chats_by_filter(branch, ages, levels)
    if not targets:
        await call.answer("Нет чатов под фильтр. Разметь группы.", show_alert=True)
        return

    BC_TARGET_CHATS[uid] = set(targets)
    STATE[uid] = "bc_confirm"

    lines = [f"• {safe_title(cid)}" for cid in targets]
    shown = lines[:30]
    extra = len(lines) - len(shown)
    list_text = "\n".join(shown)
    if extra > 0:
        list_text += f"\n… и ещё {extra} чатов"

    await call.message.edit_text(
        f"✅ Чатов подходит: {len(targets)}\n\n"
        f"📋 Чаты:\n{list_text}\n\n"
        "Нажми ✅ Подтвердить, чтобы перейти к отправке сообщения.",
        reply_markup=kb_bc_confirm()
    )
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "bc_confirm_send")
async def bc_confirm_send(call: types.CallbackQuery):
    uid = call.from_user.id
    if STATE.get(uid) != "bc_confirm":
        await call.answer("Неактуально")
        return

    STATE[uid] = "bc_wait_msg"
    await call.message.edit_text(
        "✅ Подтверждено!\n\n"
        "Теперь пришли ОДНО сообщение для рассылки:\n"
        "💬 текст / 🖼 фото / 🎬 видео / 📎 файл\n\n"
        "Если передумал — нажми отмену ниже.",
        reply_markup=kb_bc_wait_cancel()
    )
    await call.answer()


# ==========================
# Any message: save chats + send broadcast if waiting
# ==========================

@dp.message_handler(content_types=types.ContentTypes.ANY)
async def any_message(message: types.Message):
    # save group chat in db
    if message.chat.type in ("group", "supergroup"):
        db_upsert_chat(message.chat)

    # broadcast send (only owner)
    if not message.from_user or not is_owner_user_id(message.from_user.id):
        return

    uid = message.from_user.id
    if STATE.get(uid) != "bc_wait_msg":
        return

    chat_ids = list(BC_TARGET_CHATS.get(uid, set()))

    # clear broadcast state
    STATE.pop(uid, None)
    BC_SELECTED_BRANCH.pop(uid, None)
    BC_SELECTED_AGES.pop(uid, None)
    BC_SELECTED_LEVELS.pop(uid, None)
    BC_TARGET_CHATS.pop(uid, None)
    BC_MANUAL_SELECTED.pop(uid, None)
    BC_MANUAL_PAGE.pop(uid, None)

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
        await asyncio.sleep(1.0)

    await message.reply(f"✅ Готово!\nУспешно: {ok}\nОшибок: {fail}")


if __name__ == "__main__":
    executor.start_polling(dp, on_startup=on_startup)
