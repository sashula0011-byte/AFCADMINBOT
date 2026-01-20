import os
import asyncio
import logging
from typing import Dict, Set, List, Optional

import psycopg2
from psycopg2.extras import RealDictCursor

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
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

AGE_TAGS = [
    ("baby", "👶 Бейби"),
    ("kids", "🧒 Дети"),
    ("junior", "🧑‍🎓 Юниоры"),
    ("adult", "🧑 Взрослые"),
    ("mom", "🤱 Мамочки"),
]

LEVEL_TAGS = [
    ("beginner", "🟢 Начинающие"),
    ("middle", "🟡 Продолжающие"),
    ("pro", "🔴 Профи"),
]

BRANCH_TAGS = [
    ("krylatskoe", "📍 Крылатское"),
    ("odintsovo", "📍 Одинцово"),
]

ALL_AGE_TAGS = {t for t, _ in AGE_TAGS}
ALL_LEVEL_TAGS = {t for t, _ in LEVEL_TAGS}


# ==========================
# DB
# ==========================

def db_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor, sslmode="require")


def db_init():
    """
    Создание таблицы + авто-миграции.
    """
    with db_conn() as conn:
        with conn.cursor() as cur:
            # базовая таблица
            cur.execute("""
                CREATE TABLE IF NOT EXISTS chats (
                    chat_id BIGINT PRIMARY KEY,
                    title TEXT NOT NULL,
                    chat_type TEXT NOT NULL,
                    updated_at TIMESTAMP DEFAULT NOW()
                );
            """)
            # миграции колонок
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


def db_get_chats_by_branch(branch: str) -> List[dict]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM chats
                WHERE branch=%s
                ORDER BY title ASC;
            """, (branch,))
            return cur.fetchall()


def db_set_chat_branch(chat_id: int, branch: str):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE chats SET branch=%s, updated_at=NOW() WHERE chat_id=%s;", (branch, chat_id))
        conn.commit()


def db_set_chat_age(chat_id: int, age: str):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE chats SET age=%s, updated_at=NOW() WHERE chat_id=%s;", (age, chat_id))
        conn.commit()


def db_set_chat_level(chat_id: int, level: str):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE chats SET level=%s, updated_at=NOW() WHERE chat_id=%s;", (level, chat_id))
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


def db_get_next_missing_age_chat() -> Optional[dict]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM chats
                WHERE age IS NULL
                ORDER BY title ASC
                LIMIT 1;
            """)
            return cur.fetchone()


def db_get_next_missing_level_chat() -> Optional[dict]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM chats
                WHERE level IS NULL
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

# tagging (branch/age/level sequential)
TAG_STATE: Dict[int, str] = {}         # "tag_branch" | "tag_age" | "tag_level"
TAG_TARGET_CHAT: Dict[int, int] = {}   # chat_id being tagged
TAG_AUTO_NEXT: Dict[int, bool] = {}    # continue automatically


# ==========================
# Keyboards
# ==========================

def kb_main_admin() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("📣 Рассылка", callback_data="menu_broadcast"),
        InlineKeyboardButton("🏢 Разметка филиала (следующий)", callback_data="menu_tag_branch_next"),
        InlineKeyboardButton("🎂 Разметка возраста (следующий)", callback_data="menu_tag_age_next"),
        InlineKeyboardButton("🎯 Разметка уровня (следующий)", callback_data="menu_tag_level_next"),
    )
    return kb


def kb_branch_picker(prefix: str, cancel_cb: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
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
        title = ch["title"]
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


# ==========================
# Startup
# ==========================

async def on_startup(dp: Dispatcher):
    await bot.delete_webhook(drop_pending_updates=True)
    db_init()
    logging.info("✅ Bot started polling")
    logging.info(f"OWNER_ID parsed = {OWNER_ID}")
    logging.info("✅ Postgres initialized + migrated")


# ==========================
# Commands
# ==========================

@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    await message.reply(
        "✅ Бот работает.\n"
        f"Ваш ID: <code>{message.from_user.id}</code>",
        parse_mode="HTML",
        reply_markup=kb_main_admin()
    )


# ==========================
# MENU CALLBACKS
# ==========================

@dp.callback_query_handler(lambda c: c.data == "noop")
async def noop(call: types.CallbackQuery):
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "menu_broadcast")
async def menu_broadcast(call: types.CallbackQuery):
    uid = call.from_user.id
    if not is_owner_user_id(uid):
        await call.answer("⛔ Только владелец", show_alert=True)
        return

    # reset broadcast
    STATE[uid] = "bc_choose_branch"
    BC_SELECTED_BRANCH.pop(uid, None)
    BC_SELECTED_AGES.pop(uid, None)
    BC_SELECTED_LEVELS.pop(uid, None)
    BC_TARGET_CHATS.pop(uid, None)
    BC_MANUAL_SELECTED.pop(uid, None)
    BC_MANUAL_PAGE.pop(uid, None)

    await call.message.answer(
        "📣 Выбери филиал для рассылки:",
        reply_markup=kb_branch_picker("bc_branch", "bc_cancel")
    )
    await call.answer()


# ==========================
# TAGGING: branch/age/level
# ==========================

async def tag_show_next(uid: int, kind: str, chat_row: Optional[dict], message: types.Message):
    """
    kind: "branch" | "age" | "level"
    """
    if not chat_row:
        await message.answer(
            f"✅ Нет групп без разметки ({kind}).\n\n"
            "⚠️ Если групп нет в базе — напиши любое сообщение (можно '.') в каждую группу, "
            "где есть бот, чтобы он сохранил чаты."
        )
        return

    chat_id = int(chat_row["chat_id"])
    title = chat_row.get("title") or str(chat_id)

    TAG_AUTO_NEXT[uid] = True
    TAG_TARGET_CHAT[uid] = chat_id
    TAG_STATE[uid] = f"tag_{kind}"

    if kind == "branch":
        await message.answer(
            f"🏢 Назначаем филиал\nЧат: {title}\n\nВыбери филиал:",
            reply_markup=kb_branch_picker("tag_branch", "tag_cancel")
        )
    elif kind == "age":
        await message.answer(
            f"🎂 Назначаем возраст\nЧат: {title}\n\nВыбери возраст:",
            reply_markup=kb_age_picker("tag_age", "tag_cancel")
        )
    else:
        await message.answer(
            f"🎯 Назначаем уровень\nЧат: {title}\n\nВыбери уровень:",
            reply_markup=kb_level_picker("tag_level", "tag_cancel")
        )


@dp.callback_query_handler(lambda c: c.data == "menu_tag_branch_next")
async def menu_tag_branch_next(call: types.CallbackQuery):
    uid = call.from_user.id
    if not is_owner_user_id(uid):
        await call.answer("⛔ Только владелец", show_alert=True)
        return

    row = db_get_next_missing_branch_chat()
    await tag_show_next(uid, "branch", row, call.message)
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "menu_tag_age_next")
async def menu_tag_age_next(call: types.CallbackQuery):
    uid = call.from_user.id
    if not is_owner_user_id(uid):
        await call.answer("⛔ Только владелец", show_alert=True)
        return

    row = db_get_next_missing_age_chat()
    await tag_show_next(uid, "age", row, call.message)
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "menu_tag_level_next")
async def menu_tag_level_next(call: types.CallbackQuery):
    uid = call.from_user.id
    if not is_owner_user_id(uid):
        await call.answer("⛔ Только владелец", show_alert=True)
        return

    row = db_get_next_missing_level_chat()
    await tag_show_next(uid, "level", row, call.message)
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "tag_cancel")
async def tag_cancel(call: types.CallbackQuery):
    uid = call.from_user.id
    TAG_STATE.pop(uid, None)
    TAG_TARGET_CHAT.pop(uid, None)
    TAG_AUTO_NEXT.pop(uid, None)
    try:
        await call.message.edit_text("❌ Разметка отменена.")
    except Exception:
        await call.message.answer("❌ Разметка отменена.")
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("tag_branch_"))
async def tag_set_branch(call: types.CallbackQuery):
    uid = call.from_user.id
    if TAG_STATE.get(uid) != "tag_branch":
        await call.answer("Неактуально")
        return

    chat_id = TAG_TARGET_CHAT.get(uid)
    if chat_id is None:
        await call.answer("Ошибка: чат не выбран", show_alert=True)
        return

    branch = call.data.replace("tag_branch_", "").strip()
    db_set_chat_branch(int(chat_id), branch)

    title = safe_title(int(chat_id))

    TAG_STATE.pop(uid, None)
    TAG_TARGET_CHAT.pop(uid, None)

    await call.message.edit_text(f"✅ Филиал назначен!\n\n{title}\nbranch={branch}")
    await call.answer()

    if TAG_AUTO_NEXT.get(uid):
        row = db_get_next_missing_branch_chat()
        if not row:
            await call.message.answer("✅ Все группы теперь имеют филиал!")
            TAG_AUTO_NEXT.pop(uid, None)
            return
        await tag_show_next(uid, "branch", row, call.message)


@dp.callback_query_handler(lambda c: c.data.startswith("tag_age_"))
async def tag_set_age(call: types.CallbackQuery):
    uid = call.from_user.id
    if TAG_STATE.get(uid) != "tag_age":
        await call.answer("Неактуально")
        return

    chat_id = TAG_TARGET_CHAT.get(uid)
    if chat_id is None:
        await call.answer("Ошибка: чат не выбран", show_alert=True)
        return

    age = call.data.replace("tag_age_", "").strip()
    db_set_chat_age(int(chat_id), age)

    title = safe_title(int(chat_id))

    TAG_STATE.pop(uid, None)
    TAG_TARGET_CHAT.pop(uid, None)

    await call.message.edit_text(f"✅ Возраст назначен!\n\n{title}\nage={age}")
    await call.answer()

    if TAG_AUTO_NEXT.get(uid):
        row = db_get_next_missing_age_chat()
        if not row:
            await call.message.answer("✅ Все группы теперь имеют возраст!")
            TAG_AUTO_NEXT.pop(uid, None)
            return
        await tag_show_next(uid, "age", row, call.message)


@dp.callback_query_handler(lambda c: c.data.startswith("tag_level_"))
async def tag_set_level(call: types.CallbackQuery):
    uid = call.from_user.id
    if TAG_STATE.get(uid) != "tag_level":
        await call.answer("Неактуально")
        return

    chat_id = TAG_TARGET_CHAT.get(uid)
    if chat_id is None:
        await call.answer("Ошибка: чат не выбран", show_alert=True)
        return

    level = call.data.replace("tag_level_", "").strip()
    db_set_chat_level(int(chat_id), level)

    title = safe_title(int(chat_id))

    TAG_STATE.pop(uid, None)
    TAG_TARGET_CHAT.pop(uid, None)

    await call.message.edit_text(f"✅ Уровень назначен!\n\n{title}\nlevel={level}")
    await call.answer()

    if TAG_AUTO_NEXT.get(uid):
        row = db_get_next_missing_level_chat()
        if not row:
            await call.message.answer("✅ Все группы теперь имеют уровень!")
            TAG_AUTO_NEXT.pop(uid, None)
            return
        await tag_show_next(uid, "level", row, call.message)


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


# Manual pick (supports negative chat_id)
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


# Age/Level selection for tagged broadcast
@dp.callback_query_handler(lambda c: c.data.startswith("bc_age_") and c.data not in ("bc_age_all", "bc_age_next"))
async def bc_toggle_age(call: types.CallbackQuery):
    uid = call.from_user.id
    if STATE.get(uid) != "bc_age":
        await call.answer("Неактуально")
        return
    tag = call.data.split("_")[-1]
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
# Any message
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
