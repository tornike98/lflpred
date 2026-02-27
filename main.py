import asyncio
import logging
import os
import re
from datetime import datetime, time
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple
from zoneinfo import ZoneInfo

import asyncpg
from dotenv import load_dotenv

from aiogram import BaseMiddleware, Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton

# -------------------- ENV --------------------
load_dotenv()

API_TOKEN = os.getenv("BOT_TOKEN")
if not API_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")

ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()]

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST")

if not all([DB_USER, DB_PASSWORD, DB_NAME, DB_HOST]):
    raise RuntimeError("DB_USER/DB_PASSWORD/DB_NAME/DB_HOST must be set")

MOSCOW_TZ = ZoneInfo("Europe/Moscow")

# -------------------- TEAMS --------------------
TEAMS = [
    "Анвизор", "Анжи", "Видное", "Войтек", "Восточное Бутово", "Галеон", "Джей-Уан", "Дискавер",
    "Иствуд", "Карт Бланш", "Кентавр", "Керамик Чувашия", "Кони", "Корсары", "Линия-ТБН",
    "ЛФК Тройка", "Маяк", "Медина", "Молоково", "Норвич", "ПЫХТим", "Развилка", "Родина Внутри",
    "Русский Стандарт", "Сбербанк", "Северо-Запад", "СКЛФ", "Смартавиа", "Такси Ритм", "Фора",
    "Хаджиме", "Царицыно",
]
FAN_TEAM = "Болельщик"


def _norm_team(s: str) -> str:
    s = re.sub(r"\s+", " ", (s or "").strip())
    return s.casefold()


TEAM_MAP = {_norm_team(FAN_TEAM): FAN_TEAM}
TEAM_MAP.update({_norm_team(t): t for t in TEAMS})

# -------------------- PROFANITY FILTER (ONLY MAT, >=4 letters) --------------------
# Только мат/обсценная лексика. Оскорбления (идиот, дебил...) НЕ проверяем.
# - проверяем по словам (токенам)
# - игнорируем токены короче 4
# - НЕ используем короткие корни типа "еб"

MAT_PREFIXES_4PLUS = {
    # матные корни
    "бляд", "блят",
    "пизд", "пезд",
    "хуйн",
    # еб-ветка (только >=4)
    "ебан", "ёбан", "уеб", "уёб", "заеб", "заёб", "ебуч", "ёбуч", "ебищ", "ёбищ", "еблан", "ёблан",
    # обсценка, обычно считаем вместе с матом
    "дроч", "онан",
    "залуп", "манда",
    "шлюх", "шалав", "простит",  # проститут*
    "минет",
    "аналь", "анал",
    "сосат", "сосал",
    # фекальная обсценка (если хочешь убрать — можно удалить эти корни)
    "говн", "дерьм", "сран", "срат", "срак",
    # спорно, но обычно относят к мату/обсценному
    "сука", "сучк", "сучь",
}

MAT_WORDS_EXACT_4PLUS = {
    "блядь", "бляди", "блядство", "блядина",
    "пизда", "пиздец", "пиздюк", "пиздун",
    "хуйня", "херня",
    "ебаный", "ебаная", "ебаное", "ебаные",
    "уебок", "уебки",
    "еблан", "ебланы",
    "дрочить", "дрочка",
    "залупа",
    "шлюха", "шлюхи",
    "шалава", "шалавы",
    "проститутка", "проститутки",
    "минет",
    "анальный", "анально",
    "сосать", "сосал",
    # фекальная обсценка
    "говно", "дерьмо", "срань", "срака", "сраный",
    # спорно
    "сука", "сучка",
}


def _norm_text_for_filter(s: str) -> str:
    s = (s or "").casefold().replace("ё", "е")
    s = re.sub(r"[^0-9a-zа-я]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


_MAT_PREFIXES_N = {p.casefold().replace("ё", "е") for p in MAT_PREFIXES_4PLUS if len(p) >= 4}
_MAT_WORDS_N = {w.casefold().replace("ё", "е") for w in MAT_WORDS_EXACT_4PLUS if len(w) >= 4}


def contains_profanity_or_insult(text: str) -> bool:
    """Теперь проверяет ТОЛЬКО мат (без оскорблений)."""
    t = _norm_text_for_filter(text)
    if not t:
        return False

    for tok in t.split():
        if len(tok) < 4:
            continue

        if tok in _MAT_WORDS_N:
            return True

        for pref in _MAT_PREFIXES_N:
            if tok.startswith(pref):
                return True

    return False


# -------------------- GLOBALS --------------------
db_pool: Optional[asyncpg.Pool] = None
router = Router()

MENU_BUTTONS = [
    "Мой профиль",
    "Сделать прогноз",
    "Таблица лидеров",
    "Таблица лидеров команд",
    "Таблица месяц",
    "Посмотреть мой прогноз",
    "Посмотреть мои очки",
]
ADMIN_BUTTONS = [
    "Внести результаты",
    "Изменить результаты",
    "Внести новые матчи",
    "Опубликовать результаты",
    "Удалить все таблицы",
    "Таблица АДМИН",
    "Месяц АДМИН",
    # NEW:
    "Забанить пользователя",
    "Разблокировать пользователя",
    "Черный список",
]
ALL_BUTTONS = MENU_BUTTONS + ADMIN_BUTTONS


# -------------------- FSM --------------------
class RegisterStates(StatesGroup):
    waiting_for_name = State()
    waiting_for_team = State()


class ForecastStates(StatesGroup):
    waiting_for_score = State()


class NewMatchesStates(StatesGroup):
    waiting_for_match = State()


class EnterResultsStates(StatesGroup):
    waiting_for_result = State()


class DeleteTablesStates(StatesGroup):
    waiting_for_confirmation = State()


class BanStates(StatesGroup):
    waiting_for_telegram_id = State()
    waiting_for_confirmation = State()


class UnbanStates(StatesGroup):
    waiting_for_telegram_id = State()
    waiting_for_nickname = State()
    waiting_for_points = State()
    waiting_for_team = State()


# -------------------- TIME HELPERS --------------------
def moscow_now() -> datetime:
    return datetime.now(MOSCOW_TZ)


def current_isoyear_week() -> Tuple[int, int]:
    iso = moscow_now().isocalendar()
    return int(iso.year), int(iso.week)


def is_forecast_open() -> bool:
    t = moscow_now()
    weekday = t.weekday()  # Пн=0..Вс=6
    current_time = t.time()

    if weekday == 1:  # вторник
        return current_time >= time(18, 0)
    elif weekday == 4:  # пятница
        return current_time < time(21, 0)
    elif weekday in (2, 3):  # среда/четверг
        return True
    return False


# -------------------- UI HELPERS --------------------
def build_main_menu(user_id: int) -> ReplyKeyboardMarkup:
    buttons = list(MENU_BUTTONS)
    if user_id in ADMIN_IDS:
        buttons.extend(ADMIN_BUTTONS)

    rows: list[list[KeyboardButton]] = []
    row: list[KeyboardButton] = []
    for b in buttons:
        row.append(KeyboardButton(text=b))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


async def send_main_menu(message: Message) -> None:
    await message.answer("Выберите действие:", reply_markup=build_main_menu(message.from_user.id))


# -------------------- DB INIT (safe migration) --------------------
async def _maybe_rename_legacy(conn: asyncpg.Connection, table: str, required_cols: set[str]) -> None:
    reg = await conn.fetchval("SELECT to_regclass($1)", f"public.{table}")
    if not reg:
        return

    cols = await conn.fetch(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name=$1
        """,
        table,
    )
    colset = {r["column_name"] for r in cols}
    if not required_cols.issubset(colset):
        suffix = moscow_now().strftime("%Y%m%d%H%M%S")
        legacy_name = f"{table}_legacy_{suffix}"
        await conn.execute(f'ALTER TABLE "{table}" RENAME TO "{legacy_name}"')


async def init_db() -> None:
    global db_pool
    db_pool = await asyncpg.create_pool(
        user=DB_USER, password=DB_PASSWORD, database=DB_NAME, host=DB_HOST
    )

    async with db_pool.acquire() as conn:
        # users + team
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                telegram_id BIGINT UNIQUE,
                name TEXT UNIQUE,
                nickname TEXT,
                team TEXT,
                points INTEGER DEFAULT 0
            );
            """
        )
        await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS team TEXT;")

        # monthleaders + team
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS monthleaders (
                id SERIAL PRIMARY KEY,
                telegram_id BIGINT UNIQUE,
                name TEXT,
                nickname TEXT,
                team TEXT,
                points INTEGER DEFAULT 0
            );
            """
        )
        await conn.execute("ALTER TABLE monthleaders ADD COLUMN IF NOT EXISTS team TEXT;")

        # matches/forecasts
        await _maybe_rename_legacy(conn, "matches", {"iso_year", "week", "match_index", "match_name"})
        await _maybe_rename_legacy(conn, "forecasts", {"iso_year", "week", "match_index", "telegram_id", "forecast"})

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS matches (
                id SERIAL PRIMARY KEY,
                iso_year INTEGER NOT NULL,
                week INTEGER NOT NULL,
                match_index INTEGER NOT NULL,
                match_name TEXT NOT NULL,
                result TEXT,
                UNIQUE (iso_year, week, match_index)
            );
            """
        )

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS forecasts (
                id SERIAL PRIMARY KEY,
                telegram_id BIGINT NOT NULL,
                iso_year INTEGER NOT NULL,
                week INTEGER NOT NULL,
                match_index INTEGER NOT NULL,
                forecast TEXT NOT NULL,
                UNIQUE (telegram_id, iso_year, week, match_index)
            );
            """
        )

        # idempotent apply marker + ledger for rollback
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS points_applied (
                iso_year INTEGER NOT NULL,
                week INTEGER NOT NULL,
                applied_at TIMESTAMP NOT NULL DEFAULT NOW(),
                PRIMARY KEY (iso_year, week)
            );
            """
        )

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS match_points (
                iso_year INTEGER NOT NULL,
                week INTEGER NOT NULL,
                telegram_id BIGINT NOT NULL,
                match_index INTEGER NOT NULL,
                points INTEGER NOT NULL,
                PRIMARY KEY (iso_year, week, telegram_id, match_index)
            );
            """
        )

        # BLACKLIST
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS blacklist (
                telegram_id BIGINT PRIMARY KEY,
                nickname TEXT,
                points INTEGER DEFAULT 0,
                banned_at TIMESTAMP NOT NULL DEFAULT NOW()
            );
            """
        )


# -------------------- ADMIN HELPERS --------------------
def _is_admin(message: Message) -> bool:
    return message.from_user.id in ADMIN_IDS


async def get_latest_matches_set(conn: asyncpg.Connection) -> Optional[Tuple[int, int]]:
    row = await conn.fetchrow(
        """
        SELECT iso_year, week
        FROM matches
        GROUP BY iso_year, week
        ORDER BY iso_year DESC, week DESC
        LIMIT 1
        """
    )
    if not row:
        return None
    return int(row["iso_year"]), int(row["week"])


async def get_latest_applied_set(conn: asyncpg.Connection) -> Optional[Tuple[int, int]]:
    row = await conn.fetchrow(
        """
        SELECT iso_year, week
        FROM points_applied
        ORDER BY iso_year DESC, week DESC
        LIMIT 1
        """
    )
    if not row:
        return None
    return int(row["iso_year"]), int(row["week"])


async def set_stats(conn: asyncpg.Connection, iso_year: int, week: int) -> Tuple[int, int]:
    total = int(await conn.fetchval("SELECT COUNT(*) FROM matches WHERE iso_year=$1 AND week=$2", iso_year, week))
    missing = int(await conn.fetchval(
        "SELECT COUNT(*) FROM matches WHERE iso_year=$1 AND week=$2 AND result IS NULL", iso_year, week
    ))
    return total, missing


async def is_applied(conn: asyncpg.Connection, iso_year: int, week: int) -> bool:
    v = await conn.fetchval("SELECT 1 FROM points_applied WHERE iso_year=$1 AND week=$2", iso_year, week)
    return v is not None


async def clear_applied_markers(conn: asyncpg.Connection, iso_year: int, week: int) -> None:
    await conn.execute("DELETE FROM points_applied WHERE iso_year=$1 AND week=$2", iso_year, week)
    await conn.execute("DELETE FROM match_points WHERE iso_year=$1 AND week=$2", iso_year, week)


# -------------------- MIDDLEWARE (registration + blacklist) --------------------
class RegistrationCheckMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
        event: Message,
        data: Dict[str, Any],
    ) -> Any:
        if db_pool is None:
            await event.answer("База данных ещё не инициализирована, попробуйте позже.")
            return

        # Admins: allow always (and cannot be banned by logic)
        if event.from_user and event.from_user.id in ADMIN_IDS:
            return await handler(event, data)

        # First: blacklist block (even for /start)
        async with db_pool.acquire() as conn:
            banned = await conn.fetchval("SELECT 1 FROM blacklist WHERE telegram_id=$1", event.from_user.id)
        if banned:
            await event.answer("Ваш аккаунт заблокирован, вы не можете участвовать в конкурсе")
            return

        # /start passes
        if event.text and event.text.startswith("/start"):
            return await handler(event, data)

        # FSM registration passes
        state: Optional[FSMContext] = data.get("state")
        if state:
            cur = await state.get_state()
            if cur and cur.startswith("RegisterStates"):
                return await handler(event, data)

        # must be registered
        async with db_pool.acquire() as conn:
            user = await conn.fetchval("SELECT 1 FROM users WHERE telegram_id=$1", event.from_user.id)

        if not user:
            await event.answer("Вы не зарегистрированы. Пожалуйста, зарегистрируйтесь, введя /start")
            return

        return await handler(event, data)


router.message.middleware(RegistrationCheckMiddleware())


# -------------------- BUSINESS --------------------
def compute_points(actual: str, forecast: str) -> int:
    try:
        if actual.lower() == "тп":
            return 0

        a = actual.split("-")
        f = forecast.split("-")
        if len(a) != 2 or len(f) != 2:
            return 0

        ah, aa = int(a[0]), int(a[1])
        fh, fa = int(f[0]), int(f[1])

        actual_outcome = 1 if ah > aa else (0 if ah == aa else -1)
        forecast_outcome = 1 if fh > fa else (0 if fh == fa else -1)
        if actual_outcome != forecast_outcome:
            return 0

        points = 1
        if abs(ah - aa) == abs(fh - fa):
            points = 3
            if ah == fh and aa == fa:
                points = 5
        return points
    except Exception:
        return 0


async def broadcast_new_matches(bot: Bot, iso_year: int, week: int) -> None:
    async with db_pool.acquire() as conn:
        matches = await conn.fetch(
            """
            SELECT match_name FROM matches
            WHERE iso_year=$1 AND week=$2
            ORDER BY match_index
            """,
            iso_year, week
        )
        users = await conn.fetch("SELECT telegram_id FROM users")

    if matches:
        matches_text = "\n".join(m["match_name"] for m in matches)
        text = f"Новые матчи на выходные добавлены 😃, не забудь оставить прогноз:\n{matches_text}"
    else:
        text = "Новые матчи на выходные добавлены 😃, не забудь оставить прогноз."

    for u in users:
        try:
            await bot.send_message(u["telegram_id"], text)
        except Exception as e:
            logging.error("Ошибка при отправке пользователю %s: %s", u["telegram_id"], e)


async def clear_forecasts_for_week(iso_year: int, week: int) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM forecasts WHERE iso_year=$1 AND week=$2", iso_year, week)


async def apply_points_for_week(conn: asyncpg.Connection, iso_year: int, week: int) -> None:
    if await is_applied(conn, iso_year, week):
        return

    total, missing = await set_stats(conn, iso_year, week)
    if total >= 10 and missing > 0:
        return

    matches = await conn.fetch(
        "SELECT match_index, result FROM matches WHERE iso_year=$1 AND week=$2",
        iso_year, week
    )
    results_by_idx = {int(m["match_index"]): m["result"] for m in matches}

    forecasts = await conn.fetch(
        """
        SELECT telegram_id, match_index, forecast
        FROM forecasts
        WHERE iso_year=$1 AND week=$2
        """,
        iso_year, week
    )

    totals: dict[int, int] = {}

    for fc in forecasts:
        tid = int(fc["telegram_id"])
        midx = int(fc["match_index"])
        forecast = str(fc["forecast"])
        actual = results_by_idx.get(midx)

        if not actual:
            continue

        pts = compute_points(str(actual), forecast)

        await conn.execute(
            """
            INSERT INTO match_points (iso_year, week, telegram_id, match_index, points)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (iso_year, week, telegram_id, match_index)
            DO UPDATE SET points=EXCLUDED.points
            """,
            iso_year, week, tid, midx, pts
        )
        totals[tid] = totals.get(tid, 0) + pts

    await conn.execute(
        "INSERT INTO points_applied (iso_year, week) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        iso_year, week
    )

    if not totals:
        return

    tids = list(totals.keys())
    users = await conn.fetch(
        "SELECT telegram_id, name, nickname, team FROM users WHERE telegram_id = ANY($1::bigint[])",
        tids
    )
    user_map = {
        int(u["telegram_id"]): (str(u["name"]), str(u["nickname"]), (u["team"] or ""))
        for u in users
    }

    for tid, pts in totals.items():
        await conn.execute("UPDATE users SET points = points + $1 WHERE telegram_id=$2", pts, tid)

        name, nickname, team = user_map.get(tid, ("", "", ""))
        exists_ml = await conn.fetchval("SELECT 1 FROM monthleaders WHERE telegram_id=$1", tid)
        if exists_ml:
            await conn.execute(
                "UPDATE monthleaders SET points = points + $1, nickname=$2, team=$3 WHERE telegram_id=$4",
                pts, nickname, team, tid
            )
        else:
            await conn.execute(
                "INSERT INTO monthleaders (telegram_id, name, nickname, team, points) VALUES ($1, $2, $3, $4, $5)",
                tid, name, nickname, team, pts
            )


async def rollback_points_for_week(conn: asyncpg.Connection, iso_year: int, week: int) -> None:
    rows = await conn.fetch(
        """
        SELECT telegram_id, SUM(points)::int AS pts
        FROM match_points
        WHERE iso_year=$1 AND week=$2
        GROUP BY telegram_id
        """,
        iso_year, week
    )

    for r in rows:
        tid = int(r["telegram_id"])
        pts = int(r["pts"] or 0)
        if pts == 0:
            continue

        await conn.execute("UPDATE users SET points = GREATEST(points - $1, 0) WHERE telegram_id=$2", pts, tid)
        await conn.execute("UPDATE monthleaders SET points = GREATEST(points - $1, 0) WHERE telegram_id=$2", pts, tid)

    await clear_applied_markers(conn, iso_year, week)


# -------------------- USER HANDLERS --------------------
@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext) -> None:
    # middleware already blocks blacklist
    async with db_pool.acquire() as conn:
        user = await conn.fetchrow("SELECT 1 FROM users WHERE telegram_id=$1", message.from_user.id)

    if not user:
        await message.answer("Привет!👋 Введите, пожалуйста, ваше имя:")
        await state.set_state(RegisterStates.waiting_for_name)
    else:
        await send_main_menu(message)


@router.message(RegisterStates.waiting_for_name)
async def process_name(message: Message, state: FSMContext) -> None:
    name = (message.text or "").strip()
    if not name:
        await message.answer("Имя не может быть пустым, введите другое")
        return

    # мат-фильтр
    if contains_profanity_or_insult(name):
        await message.answer("Имя содержит мат. Введите другое имя:")
        return

    async with db_pool.acquire() as conn:
        existing = await conn.fetchrow("SELECT 1 FROM users WHERE name=$1", name)
        if existing:
            await message.answer("Имя уже занято, введите другое")
            return

    nickname = message.from_user.username if message.from_user.username else "ник скрыт"
    await state.update_data(reg_name=name, reg_nickname=nickname)

    await message.answer(
        "Укажите название вашей команды.\n"
        f"Если вы не являетесь членом команды, укажите «{FAN_TEAM}».\n\n"
 )
    await state.set_state(RegisterStates.waiting_for_team)


@router.message(RegisterStates.waiting_for_team)
async def process_team(message: Message, state: FSMContext) -> None:
    raw = (message.text or "").strip()
    team = TEAM_MAP.get(_norm_team(raw))

    if not team:
        await message.answer(
            "Неверное название команды.\n"
            f"Введите «{FAN_TEAM}» или одно из названий из списка (регистр не важен)."
        )
        return

    data = await state.get_data()
    name = data.get("reg_name")
    nickname = data.get("reg_nickname")

    async with db_pool.acquire() as conn:
        # на всякий случай повторная проверка
        banned = await conn.fetchval("SELECT 1 FROM blacklist WHERE telegram_id=$1", message.from_user.id)
        if banned:
            await message.answer("Ваш аккаунт заблокирован, вы не можете участвовать в конкурсе")
            await state.clear()
            return

        existing = await conn.fetchrow("SELECT 1 FROM users WHERE name=$1", name)
        if existing:
            await message.answer("Имя уже занято, введите другое имя через /start")
            await state.clear()
            return

        await conn.execute(
            "INSERT INTO users (telegram_id, name, nickname, team) VALUES ($1, $2, $3, $4)",
            message.from_user.id, name, nickname, team
        )

    await state.clear()

    rules_text = (
        "Вы успешно зарегистрировались, желаем удачи! 🤝\n\n"
        "📜 Правила конкурса:\n"
        "1. Каждую неделю для прогноза дается 10 матчей.\n"
        "2. Прогнозы принимаются со вторника 18:00 и до пятницы 21:00.\n"
        "3. Если прогноз уже внесен, поменять его больше нельзя.\n"
        "4. Очки начисляются по следующим правилам:\n"
        "   - 5 очков за точный счет;\n"
        "   - 3 очка за угаданный исход и разницу мячей;\n"
        "   - 1 очко за угаданный исход матча.\n"
        "5. Таблица лидеров обновляется после внесения результатов администратором.\n"
        "6. Если матч не состоялся или одной из команд присвоен статус 'Техническое поражение', пользователи не получают очки.\n"
        "Удачи!"
    )
    await message.answer(rules_text)
    await send_main_menu(message)


@router.message(StateFilter(None), F.text.in_(ALL_BUTTONS))
async def main_menu_handler(message: Message, state: FSMContext) -> None:
    text = message.text

    if text in ADMIN_BUTTONS and message.from_user.id not in ADMIN_IDS:
        await message.answer("Недостаточно прав.")
        return

    if text == "Мой профиль":
        await handle_my_profile(message)
    elif text == "Сделать прогноз":
        await handle_make_forecast(message, state)
    elif text == "Таблица лидеров":
        await handle_leaderboard(message)
    elif text == "Таблица лидеров команд":
        await handle_team_leaderboard(message)
    elif text == "Таблица месяц":
        await handle_month_leaderboard(message)
    elif text == "Посмотреть мой прогноз":
        await handle_view_forecast(message)
    elif text == "Посмотреть мои очки":
        await handle_view_points(message)

    # ADMIN
    elif text == "Внести результаты":
        await admin_enter_results(message, state)
    elif text == "Изменить результаты":
        await admin_edit_results(message, state)
    elif text == "Внести новые матчи":
        await admin_new_matches(message, state)
    elif text == "Опубликовать результаты":
        await admin_publish_results(message)
    elif text == "Удалить все таблицы":
        await prompt_delete_tables(message, state)
    elif text == "Таблица АДМИН":
        await handle_admin_table(message)
    elif text == "Месяц АДМИН":
        await handle_month_admin_table(message)

    # BAN/UNBAN
    elif text == "Забанить пользователя":
        await admin_ban_start(message, state)
    elif text == "Черный список":
        await admin_blacklist_show(message)
    elif text == "Разблокировать пользователя":
        await admin_unban_start(message, state)

    else:
        await message.answer("Команда не распознана")


@router.message(StateFilter(None), F.text)
async def fallback_text(message: Message) -> None:
    await message.answer("Команда не распознана. Выберите действие из меню или нажмите /start")


# -------------------- USER FEATURES --------------------
async def handle_my_profile(message: Message) -> None:
    async with db_pool.acquire() as conn:
        user = await conn.fetchrow(
            "SELECT telegram_id, name, nickname, team, points FROM users WHERE telegram_id=$1",
            message.from_user.id
        )
        if not user:
            await message.answer("Пользователь не найден. Используйте /start для регистрации.")
            return

        rows = await conn.fetch("SELECT telegram_id FROM users ORDER BY points DESC")
        overall_rank = 1
        for row in rows:
            if row["telegram_id"] == message.from_user.id:
                break
            overall_rank += 1

        monthly_row = await conn.fetchrow(
            """
            SELECT telegram_id, name, team, points, rank FROM (
                SELECT telegram_id, name, team, points, RANK() OVER (ORDER BY points DESC) as rank
                FROM monthleaders
            ) sub
            WHERE telegram_id = $1
            """,
            message.from_user.id,
        )

    team = user["team"] or "не указана"
    response = (
        f"<b>{user['name']}</b>\n"
        f"<b>{team}</b>\n"
        "Полная таблица лидеров:\n"
        f"<b>{overall_rank} место - {user['points']} очков.</b>\n"
        "Таблица лидеров за месяц:\n"
    )
    if monthly_row:
        response += f"<b>{monthly_row['rank']} место - {monthly_row['points']} очков.</b>"
    else:
        response += "<b>Нет записи в месячной таблице лидеров.</b>"

    await message.answer(response)


async def handle_make_forecast(message: Message, state: FSMContext) -> None:
    if not is_forecast_open():
        await message.answer("Прием прогнозов остановлен. Дождитесь вторника 18:00.")
        return

    iso_year, week = current_isoyear_week()

    async with db_pool.acquire() as conn:
        matches = await conn.fetch(
            """
            SELECT match_index, match_name
            FROM matches
            WHERE iso_year=$1 AND week=$2
            ORDER BY match_index
            """,
            iso_year, week
        )
        if not matches:
            await message.answer("Матчей еще нет")
            return

        existing = await conn.fetchval(
            """
            SELECT 1
            FROM forecasts
            WHERE telegram_id=$1 AND iso_year=$2 AND week=$3
            LIMIT 1
            """,
            message.from_user.id, iso_year, week
        )
        if existing:
            await message.answer("Прогноз на эту неделю уже сделан, дождитесь следующей недели")
            return

    await state.update_data(forecast_iso_year=iso_year, forecast_week=week, current_match_index=1)
    await send_next_match(message, state)


async def send_next_match(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    iso_year = int(data["forecast_iso_year"])
    week = int(data["forecast_week"])
    current_match_index = int(data.get("current_match_index", 1))

    async with db_pool.acquire() as conn:
        match = await conn.fetchrow(
            """
            SELECT match_name
            FROM matches
            WHERE iso_year=$1 AND week=$2 AND match_index=$3
            """,
            iso_year, week, current_match_index
        )

    if match:
        await state.set_state(ForecastStates.waiting_for_score)
        await message.answer(
            f"Прогноз для матча {current_match_index}: {match['match_name']}\nВведите счет в формате '2-1'"
        )
    else:
        await message.answer("Прогноз принят, желаем удачи!")
        await state.clear()
        await send_main_menu(message)


@router.message(ForecastStates.waiting_for_score)
async def process_forecast_score(message: Message, state: FSMContext) -> None:
    if not is_forecast_open():
        await message.answer("Время для внесения прогнозов истекло. Прогноз не сохранён.")
        await state.clear()
        await send_main_menu(message)
        return

    score = (message.text or "").strip()
    if score.count("-") != 1:
        await message.answer("Неверный формат. Введите счет в формате '2-1'")
        return
    a, b = score.split("-", 1)
    if not (a.isdigit() and b.isdigit()):
        await message.answer("Неверный формат. Введите счет в формате '2-1'")
        return

    data = await state.get_data()
    iso_year = int(data["forecast_iso_year"])
    week = int(data["forecast_week"])
    current_match_index = int(data["current_match_index"])

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO forecasts (telegram_id, iso_year, week, match_index, forecast)
            VALUES ($1, $2, $3, $4, $5)
            """,
            message.from_user.id, iso_year, week, current_match_index, score
        )

    current_match_index += 1
    await state.update_data(current_match_index=current_match_index)
    await send_next_match(message, state)


async def handle_view_forecast(message: Message) -> None:
    iso_year, week = current_isoyear_week()

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT m.match_name, f.forecast
            FROM forecasts f
            JOIN matches m
              ON m.iso_year=f.iso_year AND m.week=f.week AND m.match_index=f.match_index
            WHERE f.telegram_id=$1 AND f.iso_year=$2 AND f.week=$3
            ORDER BY f.match_index
            """,
            message.from_user.id, iso_year, week
        )

    if not rows:
        await message.answer("Прогноз не найден")
        return

    response = "\n".join(f"{r['match_name']} {r['forecast']}" for r in rows)
    await message.answer(response)


async def handle_leaderboard(message: Message) -> None:
    async with db_pool.acquire() as conn:
        top_rows = await conn.fetch(
            "SELECT telegram_id, name, team, points FROM users ORDER BY points DESC LIMIT 10"
        )
        if not top_rows:
            await message.answer("Таблица лидеров пуста.")
            return

        user_row = await conn.fetchrow(
            """
            SELECT telegram_id, name, team, points, rank FROM (
                SELECT telegram_id, name, team, points, RANK() OVER (ORDER BY points DESC) AS rank
                FROM users
            ) sub
            WHERE telegram_id = $1
            """,
            message.from_user.id,
        )

    response = "Таблица лидеров:\n"
    for i, row in enumerate(top_rows, start=1):
        team = row["team"] or "—"
        response += f"{i}. {row['name']} - {team} - {row['points']} очков\n"

    if user_row:
        response += (
            f"\n<b>Ваш результат:</b> {user_row['rank']}. - {user_row['name']} - "
            f"{(user_row['team'] or '—')} - {user_row['points']} очков"
        )

    await message.answer(response)


async def handle_month_leaderboard(message: Message) -> None:
    async with db_pool.acquire() as conn:
        top_rows = await conn.fetch(
            "SELECT telegram_id, name, team, points FROM monthleaders ORDER BY points DESC LIMIT 10"
        )
        if not top_rows:
            await message.answer("Месячная таблица лидеров пуста.")
            return

        user_row = await conn.fetchrow(
            """
            SELECT telegram_id, name, team, points, rank FROM (
                SELECT telegram_id, name, team, points, RANK() OVER (ORDER BY points DESC) as rank
                FROM monthleaders
            ) sub
            WHERE telegram_id = $1
            """,
            message.from_user.id,
        )

    response = "Топ-10 за этот месяц:\n"
    for i, row in enumerate(top_rows, start=1):
        team = row["team"] or "—"
        response += f"{i}. {row['name']} - {team} - {row['points']} очков\n"

    if user_row:
        response += (
            f"\n<b>Ваш результат:</b> {user_row['rank']}. {user_row['name']} - "
            f"{(user_row['team'] or '—')} - {user_row['points']} очков"
        )

    await message.answer(response)


async def handle_team_leaderboard(message: Message) -> None:
    """
    Топ команд по сумме users.points, кроме "Болельщик".
    Внизу:
      <i>Ваша команда:</i>
      - если болельщик: "Болельщики не учитываются при подсчете очков команд."
      - иначе: "4. Анвизор - 52 очков."
    """
    async with db_pool.acquire() as conn:
        user = await conn.fetchrow(
            "SELECT team FROM users WHERE telegram_id=$1",
            message.from_user.id
        )
        if not user:
            await message.answer("Пользователь не найден.")
            return

        top = await conn.fetch(
            """
            SELECT team, points FROM (
                SELECT team,
                       SUM(points)::int AS points,
                       RANK() OVER (ORDER BY SUM(points) DESC) AS rnk
                FROM users
                WHERE team IS NOT NULL
                  AND team <> $1
                GROUP BY team
            ) x
            ORDER BY points DESC, team ASC
            LIMIT 10
            """,
            FAN_TEAM
        )

        if not top:
            await message.answer("Пока нет данных для командной таблицы лидеров.")
            return

        response = "Таблица лидеров команд:\n"
        for idx, row in enumerate(top, start=1):
            response += f"{idx}. {row['team']} - {row['points']} очков\n"

        my_team = (user["team"] or FAN_TEAM)

        my_row = None
        if my_team != FAN_TEAM:
            my_row = await conn.fetchrow(
                """
                SELECT team, points, rnk FROM (
                    SELECT team,
                           SUM(points)::int AS points,
                           RANK() OVER (ORDER BY SUM(points) DESC) AS rnk
                    FROM users
                    WHERE team IS NOT NULL
                      AND team <> $1
                    GROUP BY team
                ) x
                WHERE team = $2
                """,
                FAN_TEAM, my_team
            )

    response += "\n\n<i>Ваша команда:</i>\n\n"
    if my_team == FAN_TEAM:
        response += "Болельщики не учитываются при подсчете очков команд."
    elif my_row:
        response += f"{my_row['rnk']}. {my_row['team']} - {my_row['points']} очков."
    else:
        response += f"— {my_team} - 0 очков."

    await message.answer(response)


async def handle_view_points(message: Message) -> None:
    """
    Показывает очки по последнему набору матчей, по которому уже начислялись очки.
    """
    async with db_pool.acquire() as conn:
        applied_set = await get_latest_applied_set(conn)
        if not applied_set:
            await message.answer("Пока нет данных для отображения — результаты еще не внесены.")
            return

        iso_year, week = applied_set

        cnt = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM forecasts
            WHERE telegram_id=$1 AND iso_year=$2 AND week=$3
            """,
            message.from_user.id, iso_year, week
        )
        if int(cnt) == 0:
            await message.answer("Вы не делали прогноз на этих матчах")
            return

        rows = await conn.fetch(
            """
            SELECT m.match_name, m.result, f.forecast
            FROM forecasts f
            JOIN matches m
              ON m.iso_year=f.iso_year AND m.week=f.week AND m.match_index=f.match_index
            WHERE f.telegram_id=$1 AND f.iso_year=$2 AND f.week=$3 AND m.result IS NOT NULL
            ORDER BY f.match_index
            """,
            message.from_user.id, iso_year, week
        )

    if not rows:
        await message.answer("Пока нет данных для отображения — результаты еще не внесены.")
        return

    response_lines = []
    for idx, row in enumerate(rows, start=1):
        points = compute_points(str(row["result"]), str(row["forecast"]))
        response_lines.append(
            f"{idx}. <b>{row['match_name']} {row['result']}</b>\n"
            f"Ваш прогноз {row['forecast']} ({points} очков)"
        )

    await message.answer("\n\n".join(response_lines))


# -------------------- ADMIN HANDLERS (matches/results) --------------------
async def admin_new_matches(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        await message.answer("Недостаточно прав.")
        return

    async with db_pool.acquire() as conn:
        latest = await get_latest_matches_set(conn)
        if latest:
            iso_year_old, week_old = latest
            total, missing = await set_stats(conn, iso_year_old, week_old)
            if total >= 10 and missing > 0:
                await message.answer("Сначала внесите результаты по старым матчам")
                return

    iso_year, week = current_isoyear_week()
    await state.update_data(new_match_index=1, new_match_iso_year=iso_year, new_match_week=week)
    await state.set_state(NewMatchesStates.waiting_for_match)
    await message.answer("Введите название матча 1 из 10:")


@router.message(NewMatchesStates.waiting_for_match)
async def process_new_match(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        await message.answer("Недостаточно прав.")
        return

    match_name = (message.text or "").strip()
    if not match_name:
        await message.answer("Название матча не может быть пустым, введите ещё раз:")
        return

    data = await state.get_data()
    idx = int(data.get("new_match_index", 1))
    iso_year = int(data["new_match_iso_year"])
    week = int(data["new_match_week"])

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO matches (iso_year, week, match_index, match_name, result)
            VALUES ($1, $2, $3, $4, NULL)
            ON CONFLICT (iso_year, week, match_index)
            DO UPDATE SET match_name=EXCLUDED.match_name, result=NULL
            """,
            iso_year, week, idx, match_name
        )

    if idx < 10:
        idx += 1
        await state.update_data(new_match_index=idx)
        await message.answer(f"Готово, внесите следующий матч ({idx} из 10):")
        return

    await message.answer("Готово, все матчи добавлены.")
    await state.clear()

    # new set => allow results again for this iso_year/week (clear markers/ledger)
    async with db_pool.acquire() as conn:
        await clear_applied_markers(conn, iso_year, week)

    # original behavior: clear forecasts for this week
    await clear_forecasts_for_week(iso_year, week)

    # broadcast
    await broadcast_new_matches(message.bot, iso_year, week)


async def admin_enter_results(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        await message.answer("Недостаточно прав.")
        return

    async with db_pool.acquire() as conn:
        latest = await get_latest_matches_set(conn)
        if not latest:
            await message.answer("Матчей еще нет")
            return

        iso_year, week = latest
        total, _missing = await set_stats(conn, iso_year, week)

        if total < 10:
            await message.answer("Матчи добавлены не полностью. Сначала внесите все 10 матчей.")
            return

        if await is_applied(conn, iso_year, week):
            await message.answer("Результаты уже внесены. Используйте кнопку «Изменить результаты».")
            return

        await state.update_data(result_index=1, result_iso_year=iso_year, result_week=week)

    await state.set_state(EnterResultsStates.waiting_for_result)
    await send_result_entry(message, state)


async def admin_edit_results(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        await message.answer("Недостаточно прав.")
        return

    async with db_pool.acquire() as conn:
        latest = await get_latest_matches_set(conn)
        if not latest:
            await message.answer("Матчей еще нет")
            return

        iso_year, week = latest
        if not await is_applied(conn, iso_year, week):
            await message.answer("Новые результаты еще не внесены")
            return

        await rollback_points_for_week(conn, iso_year, week)
        await state.update_data(result_index=1, result_iso_year=iso_year, result_week=week)

    await state.set_state(EnterResultsStates.waiting_for_result)
    await message.answer("Ок. Введите результаты заново (1–10).")
    await send_result_entry(message, state)


async def send_result_entry(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    idx = int(data.get("result_index", 1))
    iso_year = int(data["result_iso_year"])
    week = int(data["result_week"])

    async with db_pool.acquire() as conn:
        match = await conn.fetchrow(
            """
            SELECT match_name
            FROM matches
            WHERE iso_year=$1 AND week=$2 AND match_index=$3
            """,
            iso_year, week, idx
        )

    if match:
        await message.answer(
            f"Введите результат для матча {idx}: {match['match_name']} (в формате '3-4' или 'тп')"
        )
        return

    async with db_pool.acquire() as conn:
        await apply_points_for_week(conn, iso_year, week)

    await message.answer("Готово. Очки пересчитаны и таблицы обновлены.")
    await state.clear()
    await send_main_menu(message)


@router.message(EnterResultsStates.waiting_for_result)
async def process_result_entry(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        await message.answer("Недостаточно прав.")
        return

    result = (message.text or "").strip().lower()
    if result != "тп":
        if result.count("-") != 1:
            await message.answer("Неверный формат. Введите результат в формате '3-4' или 'тп'")
            return
        a, b = result.split("-", 1)
        if not (a.isdigit() and b.isdigit()):
            await message.answer("Неверный формат. Введите результат в формате '3-4' или 'тп'")
            return

    data = await state.get_data()
    idx = int(data.get("result_index", 1))
    iso_year = int(data["result_iso_year"])
    week = int(data["result_week"])

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE matches
            SET result=$1
            WHERE iso_year=$2 AND week=$3 AND match_index=$4
            """,
            result, iso_year, week, idx
        )

    idx += 1
    await state.update_data(result_index=idx)
    await send_result_entry(message, state)


async def admin_publish_results(message: Message) -> None:
    if not _is_admin(message):
        await message.answer("Недостаточно прав.")
        return

    async with db_pool.acquire() as conn:
        top10 = await conn.fetch("SELECT name, team, points FROM monthleaders ORDER BY points DESC LIMIT 10")
        all_users = await conn.fetch("SELECT telegram_id FROM users")

    leaderboard_text = "Месячная таблица лидеров:\n"
    rank = 1
    for row in top10:
        team = row["team"] or "—"
        leaderboard_text += f"{rank}. {row['name']} - {team} - {row['points']} очков\n"
        rank += 1

    for user in all_users:
        try:
            await message.bot.send_message(user["telegram_id"], leaderboard_text)
        except Exception as e:
            logging.error("Ошибка при отправке пользователю %s: %s", user["telegram_id"], e)

    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE monthleaders SET points = 0")

    await message.answer("Данные месячной таблицы лидеров сброшены и результаты отправлены всем пользователям.")


# -------------------- ADMIN: Delete tables --------------------
async def prompt_delete_tables(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        await message.answer("Недостаточно прав.")
        return
    await message.answer("Вы уверены, что хотите удалить все таблицы? (Да/Нет)")
    await state.set_state(DeleteTablesStates.waiting_for_confirmation)


@router.message(DeleteTablesStates.waiting_for_confirmation)
async def process_delete_tables_confirmation(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        await message.answer("Недостаточно прав.")
        return

    if (message.text or "").strip().lower() == "да":
        async with db_pool.acquire() as conn:
            await conn.execute("DROP TABLE IF EXISTS match_points CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS points_applied CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS forecasts CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS matches CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS users CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS monthleaders CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS blacklist CASCADE;")

            legacy = await conn.fetch(
                """
                SELECT tablename
                FROM pg_tables
                WHERE schemaname='public'
                  AND (tablename LIKE 'matches_legacy_%' OR tablename LIKE 'forecasts_legacy_%')
                """
            )
            for r in legacy:
                await conn.execute(f'DROP TABLE IF EXISTS "{r["tablename"]}" CASCADE;')

        await init_db()
        await message.answer("Все таблицы удалены и созданы заново без данных!")
    else:
        await message.answer("Удаление таблиц отменено")

    await state.clear()


# -------------------- ADMIN: Full tables --------------------
async def handle_admin_table(message: Message) -> None:
    if not _is_admin(message):
        await message.answer("Недостаточно прав.")
        return

    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT telegram_id, name, nickname, team, points FROM users ORDER BY points DESC")

    if not rows:
        await message.answer("Таблица лидеров пуста.")
        return

    response = "Полная таблица лидеров:\n"
    rank = 1
    for row in rows:
        team = row["team"] or "—"
        response += (
            f"{rank}. {row['name']} - {team} - {row['points']} очков, "
            f"TelegramID: {row['telegram_id']}, Ник: {row['nickname']}\n"
        )
        rank += 1
    await message.answer(response)


async def handle_month_admin_table(message: Message) -> None:
    if not _is_admin(message):
        await message.answer("Недостаточно прав.")
        return

    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT telegram_id, name, nickname, team, points FROM monthleaders ORDER BY points DESC")

    if not rows:
        await message.answer("Месячная таблица лидеров пуста.")
        return

    response = "Месячная таблица лидеров (АДМИН):\n"
    rank = 1
    for row in rows:
        team = row["team"] or "—"
        response += (
            f"{rank}. {row['name']} - {team} - {row['points']} очков, "
            f"TelegramID: {row['telegram_id']}, Ник: {row['nickname']}\n"
        )
        rank += 1
    await message.answer(response)


# -------------------- ADMIN: Ban / Blacklist / Unban --------------------
async def admin_ban_start(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        return
    await message.answer("Укажите TelegramID пользователя")
    await state.set_state(BanStates.waiting_for_telegram_id)


@router.message(BanStates.waiting_for_telegram_id)
async def admin_ban_receive_id(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        await state.clear()
        return

    raw = (message.text or "").strip()
    if not raw.isdigit():
        await message.answer("Введите TelegramID числом")
        return

    tid = int(raw)

    if tid in ADMIN_IDS:
        await message.answer("Администратора забанить нельзя")
        await state.clear()
        await send_main_menu(message)
        return

    async with db_pool.acquire() as conn:
        user = await conn.fetchrow("SELECT telegram_id, nickname, points FROM users WHERE telegram_id=$1", tid)
        if not user:
            await message.answer("Такого пользователя не существует")
            await state.clear()
            await send_main_menu(message)
            return

        rank_row = await conn.fetchrow(
            """
            SELECT rank FROM (
                SELECT telegram_id, RANK() OVER (ORDER BY points DESC) AS rank
                FROM users
            ) sub
            WHERE telegram_id=$1
            """,
            tid
        )
        rank = int(rank_row["rank"]) if rank_row else 0

    await state.update_data(ban_tid=tid, ban_rank=rank, ban_nickname=str(user["nickname"] or "ник скрыт"))
    await message.answer(
        f"Забанить пользователя ({rank} место, {user['nickname']}, {tid}) (Да/Нет)"
    )
    await state.set_state(BanStates.waiting_for_confirmation)


@router.message(BanStates.waiting_for_confirmation)
async def admin_ban_confirm(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        await state.clear()
        return

    ans = (message.text or "").strip().lower()
    data = await state.get_data()
    tid = int(data.get("ban_tid", 0))

    if ans == "нет":
        await message.answer("Отменено")
        await state.clear()
        await send_main_menu(message)
        return

    if ans != "да":
        await message.answer("Ответьте Да или Нет")
        return

    if tid in ADMIN_IDS:
        await message.answer("Администратора забанить нельзя")
        await state.clear()
        await send_main_menu(message)
        return

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            user = await conn.fetchrow("SELECT telegram_id, nickname, points FROM users WHERE telegram_id=$1", tid)
            if not user:
                await message.answer("Такого пользователя не существует")
                await state.clear()
                await send_main_menu(message)
                return

            # add to blacklist
            await conn.execute(
                """
                INSERT INTO blacklist (telegram_id, nickname, points)
                VALUES ($1, $2, $3)
                ON CONFLICT (telegram_id)
                DO UPDATE SET nickname=EXCLUDED.nickname, points=EXCLUDED.points
                """,
                tid, user["nickname"], int(user["points"] or 0)
            )

            # delete from other tables
            await conn.execute("DELETE FROM forecasts WHERE telegram_id=$1", tid)
            await conn.execute("DELETE FROM monthleaders WHERE telegram_id=$1", tid)
            await conn.execute("DELETE FROM match_points WHERE telegram_id=$1", tid)
            await conn.execute("DELETE FROM users WHERE telegram_id=$1", tid)

    await message.answer("Пользователь забанен и удалён из таблиц.")
    await state.clear()
    await send_main_menu(message)


async def admin_blacklist_show(message: Message) -> None:
    if not _is_admin(message):
        return

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT nickname, telegram_id, points
            FROM blacklist
            ORDER BY lower(coalesce(nickname,'')) ASC, telegram_id ASC
            """
        )

    if not rows:
        await message.answer("Черный список пуст.")
        return

    lines = []
    for r in rows:
        nick = r["nickname"] or "ник скрыт"
        lines.append(f"{nick}, {r['telegram_id']}, {r['points']} очков.")
    await message.answer("\n".join(lines))


async def admin_unban_start(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        return
    await message.answer("Введите TelegramID пользователя, которого хотите разблокировать")
    await state.set_state(UnbanStates.waiting_for_telegram_id)


@router.message(UnbanStates.waiting_for_telegram_id)
async def admin_unban_receive_id(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        await state.clear()
        return

    raw = (message.text or "").strip()
    if not raw.isdigit():
        await message.answer("Введите TelegramID числом")
        return

    tid = int(raw)

    if tid in ADMIN_IDS:
        await message.answer("Администратора забанить нельзя, разблокировка не требуется.")
        await state.clear()
        await send_main_menu(message)
        return

    async with db_pool.acquire() as conn:
        exists = await conn.fetchval("SELECT 1 FROM blacklist WHERE telegram_id=$1", tid)
        if not exists:
            await message.answer("Пользователь не находится в черном списке")
            await state.clear()
            await send_main_menu(message)
            return

    await state.update_data(unban_tid=tid)
    await message.answer("Какой никнейм присвоить пользователю?")
    await state.set_state(UnbanStates.waiting_for_nickname)


@router.message(UnbanStates.waiting_for_nickname)
async def admin_unban_receive_nickname(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        await state.clear()
        return

    nick = (message.text or "").strip()
    if not nick:
        await message.answer("Никнейм не может быть пустым. Введите никнейм:")
        return

    # мат-фильтр
    if contains_profanity_or_insult(nick):
        await message.answer("Никнейм содержит мат. Введите другой никнейм:")
        return

    async with db_pool.acquire() as conn:
        # никнейм не должен быть занят (проверяем и name, и nickname)
        taken = await conn.fetchval(
            "SELECT 1 FROM users WHERE name=$1 OR nickname=$1 LIMIT 1",
            nick
        )
        if taken:
            await message.answer("Никнейм уже занят. Введите другой никнейм:")
            return

    await state.update_data(unban_nickname=nick)
    await message.answer("Сколько очков присвоить пользователю?")
    await state.set_state(UnbanStates.waiting_for_points)


@router.message(UnbanStates.waiting_for_points)
async def admin_unban_receive_points(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        await state.clear()
        return

    raw = (message.text or "").strip()
    if not raw.isdigit():
        await message.answer("Введите числовое значение")
        await message.answer("Сколько очков присвоить пользователю?")
        return

    pts = int(raw)
    await state.update_data(unban_points=pts)

    await message.answer(
        "За какую команду играет пользователь?\n"
        f"Введите «{FAN_TEAM}» или одно из названий (регистр не важен)."
    )
    await state.set_state(UnbanStates.waiting_for_team)


@router.message(UnbanStates.waiting_for_team)
async def admin_unban_receive_team(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        await state.clear()
        return

    raw = (message.text or "").strip()
    team = TEAM_MAP.get(_norm_team(raw))
    if not team:
        await message.answer("Неверное название команды. Введите еще раз:")
        return

    data = await state.get_data()
    tid = int(data["unban_tid"])
    nickname = str(data["unban_nickname"])
    points = int(data["unban_points"])

    # Мы трактуем введенный "никнейм" как display-name и как nickname в базе,
    # чтобы не было пустого name (он нужен для таблиц/уникальности).
    name = nickname

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            # ещё раз проверка на существование в black list
            exists = await conn.fetchval("SELECT 1 FROM blacklist WHERE telegram_id=$1", tid)
            if not exists:
                await message.answer("Пользователь не находится в черном списке")
                await state.clear()
                await send_main_menu(message)
                return

            # защитим от гонок по имени
            taken = await conn.fetchval("SELECT 1 FROM users WHERE name=$1 OR nickname=$1 LIMIT 1", nickname)
            if taken:
                await message.answer("Никнейм уже занят. Начните заново: Разблокировать пользователя")
                await state.clear()
                await send_main_menu(message)
                return

            await conn.execute(
                """
                INSERT INTO users (telegram_id, name, nickname, team, points)
                VALUES ($1, $2, $3, $4, $5)
                """,
                tid, name, nickname, team, points
            )

            # Добавляем в monthleaders тоже (как просили "во все таблицы")
            await conn.execute(
                """
                INSERT INTO monthleaders (telegram_id, name, nickname, team, points)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (telegram_id)
                DO UPDATE SET name=EXCLUDED.name, nickname=EXCLUDED.nickname, team=EXCLUDED.team, points=EXCLUDED.points
                """,
                tid, name, nickname, team, points
            )

            # удаляем из blacklist
            await conn.execute("DELETE FROM blacklist WHERE telegram_id=$1", tid)

    await message.answer("Пользователь разблокирован и восстановлен.")
    await state.clear()
    await send_main_menu(message)


# -------------------- MAIN --------------------
async def main() -> None:
    logging.basicConfig(level=logging.INFO)

    await init_db()

    bot = Bot(
        token=API_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    await bot.delete_webhook(drop_pending_updates=True)

    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        if db_pool is not None:
            await db_pool.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
