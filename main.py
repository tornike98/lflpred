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

# ==================== ENV ====================
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

# ==================== DEFAULT TEAMS (seed only) ====================
DEFAULT_TEAMS = [
    "Анвизор", "Анжи", "Видное", "Войтек", "Восточное Бутово", "Галеон", "Джей-Уан", "Дискавер",
    "Иствуд", "Карт Бланш", "Кентавр", "Керамик Чувашия", "Кони", "Корсары", "Линия-ТБН",
    "ЛФК Тройка", "Маяк", "Медина", "Молоково", "Норвич", "ПЫХТим", "Развилка", "Родина Внутри",
    "Русский Стандарт", "Сбербанк", "Северо-Запад", "СКЛФ", "Смартавиа", "Такси Ритм", "Фора",
    "Хаджиме", "Царицыно", "Боавишта", "Вейрус", "Геофак", "Годзилла Крю", "Горки парк", "Грут",
    "Нагатино", "Нефть", "Нижний Новгород", "Орехово", "Спарта Москва", "СТИЛ", "Фарвартер",
    "Форвард", "БРВ", "Вежливые люди", "Джей-Уан-2", "Квантум", "Кони-Д", "Олимпик", "Орион",
    "Рестарт", "Рубеж", "Стандард", "Туристы", "Фиора", "Фортуна", "Эдельвейс", "Восточное Бутово-Д",
    "Греймстаун", "Дерби", "Квантум-Д", "Кони-Д", "Корсары-Д", "Маяк-Д", "Молоково-Д", "Рубеж-Д",
    "Русский Стандард-Д", "Спарта Москва-Д", "Фора-2",
]
FAN_TEAM = "Болельщик"

# ==================== HTML ESCAPE ====================
def html_escape(s: str) -> str:
    s = "" if s is None else str(s)
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# ==================== PROFANITY FILTER (ONLY MAT, >=4 letters) ====================
MAT_PREFIXES_4PLUS = {
    "бляд",
    "блят",
    "пизд",
    "пезд",
    "хуйн",
    "ебан",
    "ёбан",
    "уеб",
    "уёб",
    "заеб",
    "заёб",
    "ебуч",
    "ёбуч",
    "ебищ",
    "ёбищ",
    "еблан",
    "ёблан",
    "дроч",
    "онан",
    "залуп",
    "манда",
    "шлюх",
    "шалав",
    "простит",
    "минет",
    "аналь",
    "анал",
    "сосат",
    "сосал",
    "говн",
    "дерьм",
    "сран",
    "срат",
    "срак",
    "сука",
    "сучк",
    "сучь",
}

MAT_WORDS_EXACT_4PLUS = {
    "блядь",
    "бляди",
    "блядство",
    "блядина",
    "пизда",
    "пиздец",
    "пиздюк",
    "пиздун",
    "хуйня",
    "херня",
    "ебаный",
    "ебаная",
    "ебаное",
    "ебаные",
    "уебок",
    "уебки",
    "еблан",
    "ебланы",
    "дрочить",
    "дрочка",
    "залупа",
    "шлюха",
    "шлюхи",
    "шалава",
    "шалавы",
    "проститутка",
    "проститутки",
    "минет",
    "анальный",
    "анально",
    "сосать",
    "сосал",
    "говно",
    "дерьмо",
    "срань",
    "срака",
    "сраный",
    "сука",
    "сучка",
}


def _norm_text_for_filter(s: str) -> str:
    s = (s or "").casefold().replace("ё", "е")
    s = re.sub(r"[^0-9a-zа-я]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


_MAT_PREFIXES_N = {p.casefold().replace("ё", "е") for p in MAT_PREFIXES_4PLUS if len(p) >= 4}
_MAT_WORDS_N = {w.casefold().replace("ё", "е") for w in MAT_WORDS_EXACT_4PLUS if len(w) >= 4}


def contains_profanity_or_insult(text: str) -> bool:
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


# ==================== GLOBALS ====================
db_pool: Optional[asyncpg.Pool] = None
router = Router()

MENU_BUTTONS = [
    "Мой профиль",
    "Сделать прогноз",
    "Поменять команду",
    "Таблица лидеров",
    "Топ болельщиков",
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
    "Посмотреть очки команды",
    "Добавить или удалить команду",
    "Забанить пользователя",
    "Разблокировать пользователя",
    "Черный список",
    "Открыть прием прогнозов",
    "Закрыть прием прогнозов",
    "Авто прием прогнозов",
    "Отправить сообщение",
]
ALL_BUTTONS = MENU_BUTTONS + ADMIN_BUTTONS


# ==================== FSM ====================
class RegisterStates(StatesGroup):
    waiting_for_name = State()
    waiting_for_team = State()


class ChangeTeamStates(StatesGroup):
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
    waiting_for_name = State()
    waiting_for_points = State()
    waiting_for_team = State()


class BroadcastStates(StatesGroup):
    waiting_for_text = State()
    waiting_for_confirmation = State()


class TeamPointsStates(StatesGroup):
    waiting_for_team = State()


class TeamManageStates(StatesGroup):
    waiting_for_action = State()  # Добавить / Удалить
    waiting_for_team_name = State()


# ==================== TIME HELPERS ====================
def moscow_now() -> datetime:
    return datetime.now(MOSCOW_TZ)


def current_isoyear_week() -> Tuple[int, int]:
    iso = moscow_now().isocalendar()
    return int(iso.year), int(iso.week)


def is_forecast_open_schedule() -> bool:
    t = moscow_now()
    weekday = t.weekday()
    current_time = t.time()

    if weekday == 1:  # Tuesday
        return current_time >= time(18, 0)
    elif weekday == 4:  # Friday
        return current_time < time(21, 0)
    elif weekday in (2, 3):  # Wed/Thu
        return True
    return False


# ==================== UI HELPERS ====================
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


async def send_text_in_chunks(message: Message, text: str, chunk_limit: int = 3500) -> None:
    if len(text) <= chunk_limit:
        await message.answer(text)
        return

    lines = text.split("\n")
    buf = ""
    for line in lines:
        add = line + "\n"
        if len(buf) + len(add) > chunk_limit:
            if buf.strip():
                await message.answer(buf.rstrip("\n"))
            buf = add
        else:
            buf += add

    if buf.strip():
        await message.answer(buf.rstrip("\n"))


# ==================== DB INIT ====================
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
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        host=DB_HOST,
        min_size=1,
        max_size=10,
    )

    async with db_pool.acquire() as conn:
        # Users tables
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
        await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS nickname TEXT;")
        await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS team TEXT;")

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
        await conn.execute("ALTER TABLE monthleaders ADD COLUMN IF NOT EXISTS nickname TEXT;")
        await conn.execute("ALTER TABLE monthleaders ADD COLUMN IF NOT EXISTS team TEXT;")

        # Allowed teams (dynamic)
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS allowed_teams (
                name TEXT PRIMARY KEY
            );
            """
        )
        # Seed defaults + FAN_TEAM
        for t in [FAN_TEAM] + DEFAULT_TEAMS:
            await conn.execute(
                "INSERT INTO allowed_teams (name) VALUES ($1) ON CONFLICT (name) DO NOTHING",
                t,
            )

        # Matches/forecasts
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

        # Blacklist
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

        # Forecast control
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS forecast_control (
                id SMALLINT PRIMARY KEY,
                mode TEXT NOT NULL CHECK (mode IN ('auto','open','closed')),
                updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                updated_by BIGINT
            );
            """
        )
        await conn.execute(
            """
            INSERT INTO forecast_control (id, mode)
            VALUES (1, 'auto')
            ON CONFLICT (id) DO NOTHING;
            """
        )

        # Admin actions log
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS admin_actions (
                id SERIAL PRIMARY KEY,
                admin_id BIGINT NOT NULL,
                action TEXT NOT NULL,
                details TEXT,
                created_at TIMESTAMP NOT NULL DEFAULT NOW()
            );
            """
        )


# ==================== HELPERS ====================
def _is_admin(message: Message) -> bool:
    return message.from_user.id in ADMIN_IDS


def nickname_from_message(message: Message) -> str:
    username = getattr(message.from_user, "username", None)
    if not username:
        return "ник скрыт"
    if contains_profanity_or_insult(username):
        return "ник скрыт"
    return username


async def sync_nickname_if_registered(conn: asyncpg.Connection, telegram_id: int, nickname: str) -> None:
    await conn.execute("UPDATE users SET nickname=$1 WHERE telegram_id=$2", nickname, telegram_id)
    await conn.execute("UPDATE monthleaders SET nickname=$1 WHERE telegram_id=$2", nickname, telegram_id)


async def log_admin_action(admin_id: int, action: str, details: str = "") -> None:
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO admin_actions (admin_id, action, details) VALUES ($1, $2, $3)",
                admin_id,
                action,
                details[:2000],
            )
    except Exception as e:
        logging.error("admin_actions log failed: %s", e)


async def resolve_team_name(raw: str) -> Optional[str]:
    """
    Возвращает каноническое название команды из allowed_teams (case-insensitive) или None.
    """
    name = (raw or "").strip()
    if not name:
        return None
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT name FROM allowed_teams WHERE LOWER(name)=LOWER($1) LIMIT 1",
            name,
        )
    return str(row["name"]) if row else None


async def team_exists(raw: str) -> bool:
    return (await resolve_team_name(raw)) is not None


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
    missing = int(
        await conn.fetchval("SELECT COUNT(*) FROM matches WHERE iso_year=$1 AND week=$2 AND result IS NULL", iso_year, week)
    )
    return total, missing


async def validate_match_set_1_to_10(conn: asyncpg.Connection, iso_year: int, week: int) -> Tuple[bool, str]:
    rows = await conn.fetch(
        """
        SELECT match_index
        FROM matches
        WHERE iso_year=$1 AND week=$2
        ORDER BY match_index
        """,
        iso_year,
        week,
    )
    idxs = [int(r["match_index"]) for r in rows]
    if len(idxs) != 10:
        return False, "Матчи добавлены не полностью. Должно быть ровно 10 матчей (1-10)."
    if idxs != list(range(1, 11)):
        return False, "Матчи добавлены некорректно: индексы матчей должны быть 1-10 без пропусков."
    return True, ""


async def is_applied(conn: asyncpg.Connection, iso_year: int, week: int) -> bool:
    v = await conn.fetchval("SELECT 1 FROM points_applied WHERE iso_year=$1 AND week=$2", iso_year, week)
    return v is not None


async def clear_applied_markers(conn: asyncpg.Connection, iso_year: int, week: int) -> None:
    await conn.execute("DELETE FROM points_applied WHERE iso_year=$1 AND week=$2", iso_year, week)
    await conn.execute("DELETE FROM match_points WHERE iso_year=$1 AND week=$2", iso_year, week)


async def get_forecast_mode(conn: asyncpg.Connection) -> str:
    mode = await conn.fetchval("SELECT mode FROM forecast_control WHERE id=1")
    return (mode or "auto").lower()


async def set_forecast_mode(conn: asyncpg.Connection, mode: str, updated_by: int) -> None:
    mode = mode.lower()
    if mode not in ("auto", "open", "closed"):
        mode = "auto"
    await conn.execute(
        """
        UPDATE forecast_control
        SET mode=$1, updated_at=NOW(), updated_by=$2
        WHERE id=1
        """,
        mode,
        updated_by,
    )


async def is_forecast_open_effective() -> bool:
    async with db_pool.acquire() as conn:
        mode = await get_forecast_mode(conn)
    if mode == "open":
        return True
    if mode == "closed":
        return False
    return is_forecast_open_schedule()


# ==================== SAFE SEND / RATE LIMIT ====================
async def safe_send_message(bot: Bot, chat_id: int, text: str) -> bool:
    for attempt in range(2):
        try:
            await bot.send_message(chat_id, text)
            return True
        except Exception as e:
            retry_after = getattr(e, "retry_after", None)
            if retry_after is not None and attempt == 0:
                await asyncio.sleep(float(retry_after) + 0.2)
                continue
            logging.error("send_message failed to %s: %s", chat_id, e)
            return False
    return False


# ==================== MIDDLEWARE ====================
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

        # Admin bypass blacklist & registration requirement, but keep nickname sync if exists in users.
        if event.from_user and event.from_user.id in ADMIN_IDS:
            async with db_pool.acquire() as conn:
                exists = await conn.fetchval("SELECT 1 FROM users WHERE telegram_id=$1", event.from_user.id)
                if exists:
                    await sync_nickname_if_registered(conn, event.from_user.id, nickname_from_message(event))
            return await handler(event, data)

        # blacklist check (even for /start)
        async with db_pool.acquire() as conn:
            banned = await conn.fetchval("SELECT 1 FROM blacklist WHERE telegram_id=$1", event.from_user.id)
        if banned:
            await event.answer("Ваш аккаунт заблокирован, вы не можете участвовать в конкурсе")
            return

        # nickname sync if registered
        async with db_pool.acquire() as conn:
            exists = await conn.fetchval("SELECT 1 FROM users WHERE telegram_id=$1", event.from_user.id)
            if exists:
                await sync_nickname_if_registered(conn, event.from_user.id, nickname_from_message(event))

        # /start passes
        if event.text and event.text.startswith("/start"):
            return await handler(event, data)

        # registration FSM passes
        state: Optional[FSMContext] = data.get("state")
        if state:
            cur = await state.get_state()
            if cur and cur.startswith("RegisterStates"):
                return await handler(event, data)

        # require registration for everything else
        async with db_pool.acquire() as conn:
            user = await conn.fetchval("SELECT 1 FROM users WHERE telegram_id=$1", event.from_user.id)
        if not user:
            await event.answer("Вы не зарегистрированы. Пожалуйста, зарегистрируйтесь, введя /start")
            return

        return await handler(event, data)


router.message.middleware(RegistrationCheckMiddleware())

# ==================== BUSINESS ====================
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
            iso_year,
            week,
        )
        users = await conn.fetch("SELECT telegram_id FROM users")

    if matches:
        matches_text = "\n".join(m["match_name"] for m in matches)
        text = f"Новые матчи на выходные добавлены 😃, не забудь оставить прогноз:\n{matches_text}"
    else:
        text = "Новые матчи на выходные добавлены 😃, не забудь оставить прогноз."

    for u in users:
        await safe_send_message(bot, int(u["telegram_id"]), text)
        await asyncio.sleep(0.03)


async def clear_forecasts_for_week(iso_year: int, week: int) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM forecasts WHERE iso_year=$1 AND week=$2", iso_year, week)


async def apply_points_for_week(conn: asyncpg.Connection, iso_year: int, week: int) -> None:
    if await is_applied(conn, iso_year, week):
        return

    ok, _msg = await validate_match_set_1_to_10(conn, iso_year, week)
    if not ok:
        return

    total, missing = await set_stats(conn, iso_year, week)
    if total >= 10 and missing > 0:
        return

    matches = await conn.fetch(
        "SELECT match_index, result FROM matches WHERE iso_year=$1 AND week=$2",
        iso_year,
        week,
    )
    results_by_idx = {int(m["match_index"]): m["result"] for m in matches}

    forecasts = await conn.fetch(
        """
        SELECT telegram_id, match_index, forecast
        FROM forecasts
        WHERE iso_year=$1 AND week=$2
        """,
        iso_year,
        week,
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
            iso_year,
            week,
            tid,
            midx,
            pts,
        )
        totals[tid] = totals.get(tid, 0) + pts

    await conn.execute(
        "INSERT INTO points_applied (iso_year, week) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        iso_year,
        week,
    )

    if not totals:
        return

    tids = list(totals.keys())
    users = await conn.fetch(
        "SELECT telegram_id, name, nickname, team FROM users WHERE telegram_id = ANY($1::bigint[])",
        tids,
    )
    user_map = {
        int(u["telegram_id"]): (str(u["name"]), str(u["nickname"] or "ник скрыт"), (u["team"] or ""))
        for u in users
    }

    for tid, pts in totals.items():
        await conn.execute("UPDATE users SET points = points + $1 WHERE telegram_id=$2", pts, tid)

        name, nickname, team = user_map.get(tid, ("", "ник скрыт", ""))
        exists_ml = await conn.fetchval("SELECT 1 FROM monthleaders WHERE telegram_id=$1", tid)
        if exists_ml:
            await conn.execute(
                "UPDATE monthleaders SET points = points + $1, nickname=$2, team=$3 WHERE telegram_id=$4",
                pts,
                nickname,
                team,
                tid,
            )
        else:
            await conn.execute(
                "INSERT INTO monthleaders (telegram_id, name, nickname, team, points) VALUES ($1, $2, $3, $4, $5)",
                tid,
                name,
                nickname,
                team,
                pts,
            )


async def rollback_points_for_week(conn: asyncpg.Connection, iso_year: int, week: int) -> None:
    rows = await conn.fetch(
        """
        SELECT telegram_id, SUM(points)::int AS pts
        FROM match_points
        WHERE iso_year=$1 AND week=$2
        GROUP BY telegram_id
        """,
        iso_year,
        week,
    )

    for r in rows:
        tid = int(r["telegram_id"])
        pts = int(r["pts"] or 0)
        if pts == 0:
            continue
        await conn.execute("UPDATE users SET points = GREATEST(points - $1, 0) WHERE telegram_id=$2", pts, tid)
        await conn.execute("UPDATE monthleaders SET points = GREATEST(points - $1, 0) WHERE telegram_id=$2", pts, tid)

    await clear_applied_markers(conn, iso_year, week)


# ==================== START / REGISTRATION ====================
@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext) -> None:
    async with db_pool.acquire() as conn:
        user = await conn.fetchval("SELECT 1 FROM users WHERE telegram_id=$1", message.from_user.id)

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

    if contains_profanity_or_insult(name):
        await message.answer("Имя содержит мат. Введите другое имя:")
        return

    async with db_pool.acquire() as conn:
        existing = await conn.fetchval("SELECT 1 FROM users WHERE name=$1", name)
        if existing:
            await message.answer("Имя уже занято, введите другое")
            return

    await state.update_data(reg_name=name)
    await message.answer(
        "Укажите название вашей команды.\n"
        f"Если вы не являетесь членом команды, укажите «{FAN_TEAM}»."
    )
    await state.set_state(RegisterStates.waiting_for_team)


@router.message(RegisterStates.waiting_for_team)
async def process_team(message: Message, state: FSMContext) -> None:
    raw = (message.text or "").strip()
    team = await resolve_team_name(raw)
    if not team:
        await message.answer("Такой команды не существует. Введите снова.")
        return

    data = await state.get_data()
    name = str(data.get("reg_name") or "")
    nickname = nickname_from_message(message)

    async with db_pool.acquire() as conn:
        existing = await conn.fetchval("SELECT 1 FROM users WHERE name=$1", name)
        if existing:
            await message.answer("Имя уже занято, введите другое имя через /start")
            await state.clear()
            return

        await conn.execute(
            "INSERT INTO users (telegram_id, name, nickname, team) VALUES ($1, $2, $3, $4)",
            message.from_user.id,
            name,
            nickname,
            team,
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
        "6. В случае нарушения правил или использования неподобающих никнеймов, пользователь попадает в бан.\n"
        "7. В случае указывания команды, за которую не заявлен пользователь - штраф 10 очков.\n"
        "8. Если матч не состоялся или одной из команд присвоен статус 'Техническое поражение', пользователи не получают очки.\n"
        "Удачи!"
    )
    await message.answer(rules_text)
    await send_main_menu(message)


# ==================== MAIN MENU ROUTER ====================
@router.message(StateFilter(None), F.text.in_(ALL_BUTTONS))
async def main_menu_handler(message: Message, state: FSMContext) -> None:
    text = message.text

    if text in ADMIN_BUTTONS and not _is_admin(message):
        await message.answer("Недостаточно прав.")
        return

    if text == "Мой профиль":
        await handle_my_profile(message)
    elif text == "Сделать прогноз":
        await handle_make_forecast(message, state)
    elif text == "Поменять команду":
        await start_change_team(message, state)
    elif text == "Таблица лидеров":
        await handle_leaderboard(message)
    elif text == "Топ болельщиков":
        await handle_fans_top(message)
    elif text == "Таблица лидеров команд":
        await handle_team_leaderboard(message)
    elif text == "Таблица месяц":
        await handle_month_leaderboard(message)
    elif text == "Посмотреть мой прогноз":
        await handle_view_forecast(message)
    elif text == "Посмотреть мои очки":
        await handle_view_points(message)

    # ADMIN
    elif text == "Посмотреть очки команды":
        await admin_team_points_start(message, state)
    elif text == "Добавить или удалить команду":
        await admin_manage_team_start(message, state)
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

    elif text == "Забанить пользователя":
        await admin_ban_start(message, state)
    elif text == "Черный список":
        await admin_blacklist_show(message)
    elif text == "Разблокировать пользователя":
        await admin_unban_start(message, state)

    elif text == "Открыть прием прогнозов":
        await admin_set_forecast_mode_button(message, "open")
    elif text == "Закрыть прием прогнозов":
        await admin_set_forecast_mode_button(message, "closed")
    elif text == "Авто прием прогнозов":
        await admin_set_forecast_mode_button(message, "auto")

    elif text == "Отправить сообщение":
        await admin_broadcast_start(message, state)
    else:
        await message.answer("Команда не распознана")


@router.message(StateFilter(None), F.text)
async def fallback_text(message: Message) -> None:
    await message.answer("Команда не распознана. Выберите действие из меню или нажмите /start")


# ==================== USER FEATURES ====================
async def handle_my_profile(message: Message) -> None:
    async with db_pool.acquire() as conn:
        user = await conn.fetchrow(
            "SELECT telegram_id, name, nickname, team, points FROM users WHERE telegram_id=$1",
            message.from_user.id,
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

    name = html_escape(user["name"])
    team = html_escape(user["team"] or "не указана")
    nick = html_escape(user["nickname"] or "ник скрыт")
    points = int(user["points"] or 0)

    response = (
        f"<b>{name}</b>\n"
        f"<b>{team}</b>\n"
        f"Ник: {nick}\n"
        "Полная таблица лидеров:\n"
        f"<b>{overall_rank} место - {points} очков.</b>\n"
        "Таблица лидеров за месяц:\n"
    )
    if monthly_row:
        response += f"<b>{int(monthly_row['rank'])} место - {int(monthly_row['points'] or 0)} очков.</b>"
    else:
        response += "<b>Нет записи в месячной таблице лидеров.</b>"

    await message.answer(response)


async def handle_make_forecast(message: Message, state: FSMContext) -> None:
    if not await is_forecast_open_effective():
        await message.answer("Прием прогнозов остановлен.")
        return

    iso_year, week = current_isoyear_week()

    async with db_pool.acquire() as conn:
        ok, msg = await validate_match_set_1_to_10(conn, iso_year, week)
        if not ok:
            await message.answer(msg)
            return

        existing = await conn.fetchval(
            """
            SELECT 1
            FROM forecasts
            WHERE telegram_id=$1 AND iso_year=$2 AND week=$3
            LIMIT 1
            """,
            message.from_user.id,
            iso_year,
            week,
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
            iso_year,
            week,
            current_match_index,
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
    if not await is_forecast_open_effective():
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
        try:
            await conn.execute(
                """
                INSERT INTO forecasts (telegram_id, iso_year, week, match_index, forecast)
                VALUES ($1, $2, $3, $4, $5)
                """,
                message.from_user.id,
                iso_year,
                week,
                current_match_index,
                score,
            )
        except Exception as e:
            logging.error("insert forecast failed: %s", e)
            await message.answer("Не удалось сохранить прогноз (возможно, он уже был внесен).")
            await state.clear()
            await send_main_menu(message)
            return

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
            message.from_user.id,
            iso_year,
            week,
        )

    if not rows:
        await message.answer("Прогноз не найден")
        return

    response = "\n".join(f"{r['match_name']} {r['forecast']}" for r in rows)
    await message.answer(response)


async def handle_leaderboard(message: Message) -> None:
    async with db_pool.acquire() as conn:
        top_rows = await conn.fetch("SELECT telegram_id, name, team, points FROM users ORDER BY points DESC LIMIT 10")
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
        response += f"{i}. {row['name']} - {(row['team'] or '—')} - {row['points']} очков\n"

    if user_row:
        response += (
            f"\n<b>Ваш результат:</b> {int(user_row['rank'])}. - {html_escape(user_row['name'])} - "
            f"{html_escape(user_row['team'] or '—')} - {int(user_row['points'] or 0)} очков"
        )

    await message.answer(response)


async def handle_fans_top(message: Message) -> None:
    async with db_pool.acquire() as conn:
        top_rows = await conn.fetch(
            """
            SELECT telegram_id, name, points
            FROM users
            WHERE team = $1
            ORDER BY points DESC, name ASC
            LIMIT 10
            """,
            FAN_TEAM,
        )
        if not top_rows:
            await message.answer("Топ болельщиков пуст.")
            return

        me = await conn.fetchrow("SELECT team FROM users WHERE telegram_id=$1", message.from_user.id)
        me_rank_row = None
        if me and me["team"] == FAN_TEAM:
            me_rank_row = await conn.fetchrow(
                """
                SELECT rank, name, points FROM (
                    SELECT telegram_id, name, points,
                           RANK() OVER (ORDER BY points DESC) AS rank
                    FROM users
                    WHERE team = $1
                ) sub
                WHERE telegram_id = $2
                """,
                FAN_TEAM,
                message.from_user.id,
            )

    response = "Топ-10 болельщиков:\n\n"
    for i, row in enumerate(top_rows, start=1):
        response += f"{i}. {row['name']} - {int(row['points'] or 0)} очков\n"

    if me_rank_row:
        response += (
            f"\n<b>{int(me_rank_row['rank'])}. {html_escape(str(me_rank_row['name']))} - "
            f"{int(me_rank_row['points'] or 0)} очков</b>"
        )

    await message.answer(response)


async def handle_team_leaderboard(message: Message) -> None:
    async with db_pool.acquire() as conn:
        user = await conn.fetchrow("SELECT team FROM users WHERE telegram_id=$1", message.from_user.id)
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
            FAN_TEAM,
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
                FAN_TEAM,
                my_team,
            )

    response += "\n\n<i>Ваша команда:</i>\n\n"
    if my_team == FAN_TEAM:
        response += "Болельщики не учитываются при подсчете очков команд."
    elif my_row:
        response += f"{int(my_row['rnk'])}. {my_row['team']} - {int(my_row['points'] or 0)} очков."
    else:
        response += f"— {my_team} - 0 очков."

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
        response += f"{i}. {row['name']} - {(row['team'] or '—')} - {row['points']} очков\n"

    if user_row:
        response += (
            f"\n<b>Ваш результат:</b> {int(user_row['rank'])}. {html_escape(user_row['name'])} - "
            f"{html_escape(user_row['team'] or '—')} - {int(user_row['points'] or 0)} очков"
        )

    await message.answer(response)


async def handle_view_points(message: Message) -> None:
    async with db_pool.acquire() as conn:
        applied_set = await get_latest_applied_set(conn)
        if not applied_set:
            await message.answer("Пока нет данных для отображения — результаты еще не внесены.")
            return

        iso_year, week = applied_set
        cnt = await conn.fetchval(
            "SELECT COUNT(*) FROM forecasts WHERE telegram_id=$1 AND iso_year=$2 AND week=$3",
            message.from_user.id,
            iso_year,
            week,
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
            message.from_user.id,
            iso_year,
            week,
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


# ==================== CHANGE TEAM (USER) ====================
async def start_change_team(message: Message, state: FSMContext) -> None:
    await message.answer("В какой команде вы играете?")
    await state.set_state(ChangeTeamStates.waiting_for_team)


@router.message(ChangeTeamStates.waiting_for_team)
async def process_change_team(message: Message, state: FSMContext) -> None:
    raw = (message.text or "").strip()
    team = await resolve_team_name(raw)
    if not team:
        await message.answer("Такой команды не существует")
        await state.clear()
        await send_main_menu(message)
        return

    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE users SET team=$1 WHERE telegram_id=$2", team, message.from_user.id)
        await conn.execute("UPDATE monthleaders SET team=$1 WHERE telegram_id=$2", team, message.from_user.id)

    await message.answer(f"Команда обновлена: <b>{html_escape(team)}</b>")
    await state.clear()
    await send_main_menu(message)


# ==================== ADMIN: TEAM POINTS ====================
async def admin_team_points_start(message: Message, state: FSMContext) -> None:
    await message.answer("Очки какой команды вы хотите посмотреть?")
    await state.set_state(TeamPointsStates.waiting_for_team)


@router.message(TeamPointsStates.waiting_for_team)
async def admin_team_points_show(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        await state.clear()
        return

    raw = (message.text or "").strip()
    team = await resolve_team_name(raw)
    if not team:
        await message.answer("Такой команды не существует")
        await state.clear()
        await send_main_menu(message)
        return

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT name, nickname, telegram_id, points
            FROM users
            WHERE team = $1
            ORDER BY points DESC, name ASC
            """,
            team,
        )

    header = f"<b>{html_escape(team)}</b>\n\n"
    if not rows:
        await message.answer(header + "В этой команде нет игроков.")
        await state.clear()
        await send_main_menu(message)
        return

    lines = []
    for i, r in enumerate(rows, start=1):
        name = html_escape(r["name"])
        nick = html_escape(r["nickname"] or "ник скрыт")
        tid = int(r["telegram_id"])
        pts = int(r["points"] or 0)
        lines.append(f"{i}. {name}, {nick}, {tid} - {pts} очков")

    await send_text_in_chunks(message, header + "\n".join(lines))
    await log_admin_action(message.from_user.id, "team_points", f"team={team}, count={len(rows)}")

    await state.clear()
    await send_main_menu(message)


# ==================== ADMIN: ADD/REMOVE TEAM ====================
async def admin_manage_team_start(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        await message.answer("Недостаточно прав.")
        return
    await message.answer("Какое действие выполнить? (Добавить/Удалить)")
    await state.set_state(TeamManageStates.waiting_for_action)


@router.message(TeamManageStates.waiting_for_action)
async def admin_manage_team_action(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        await state.clear()
        return

    action = (message.text or "").strip().casefold()
    if action not in ("добавить", "удалить"):
        await message.answer("Введите только: Добавить или Удалить")
        return

    await state.update_data(team_manage_action=action)
    await message.answer("Введите название команды")
    await state.set_state(TeamManageStates.waiting_for_team_name)


@router.message(TeamManageStates.waiting_for_team_name)
async def admin_manage_team_name(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        await state.clear()
        return

    name = (message.text or "").strip()
    if not name:
        await message.answer("Название команды не может быть пустым. Введите снова:")
        return
    if len(name) < 2:
        await message.answer("Слишком короткое название команды. Введите снова:")
        return
    if contains_profanity_or_insult(name):
        await message.answer("Название содержит мат. Введите другое название:")
        return

    data = await state.get_data()
    action = str(data.get("team_manage_action") or "")

    async with db_pool.acquire() as conn:
        existing = await conn.fetchrow("SELECT name FROM allowed_teams WHERE LOWER(name)=LOWER($1) LIMIT 1", name)

        if action == "добавить":
            if existing:
                await message.answer("Такая команда уже существует")
                await state.clear()
                await send_main_menu(message)
                return

            await conn.execute("INSERT INTO allowed_teams (name) VALUES ($1)", name)
            await log_admin_action(message.from_user.id, "team_add", f"name={name}")
            await message.answer(f"Команда добавлена: <b>{html_escape(name)}</b>")
            await state.clear()
            await send_main_menu(message)
            return

        # delete
        if not existing:
            await message.answer("Такой команды не существует")
            await state.clear()
            await send_main_menu(message)
            return

        canonical = str(existing["name"])
        if canonical.lower() == FAN_TEAM.lower():
            await message.answer("Нельзя удалить команду «Болельщик»")
            await state.clear()
            await send_main_menu(message)
            return

        # move members to FAN_TEAM then delete
        await conn.execute("UPDATE users SET team=$1 WHERE LOWER(team)=LOWER($2)", FAN_TEAM, canonical)
        await conn.execute("UPDATE monthleaders SET team=$1 WHERE LOWER(team)=LOWER($2)", FAN_TEAM, canonical)
        await conn.execute("DELETE FROM allowed_teams WHERE LOWER(name)=LOWER($1)", canonical)

        await log_admin_action(message.from_user.id, "team_delete", f"name={canonical}")
        await message.answer(f"Команда удалена: <b>{html_escape(canonical)}</b>\nИгроки переведены в «{FAN_TEAM}».")
        await state.clear()
        await send_main_menu(message)


# ==================== ADMIN: MATCHES / RESULTS ====================
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
        await state.clear()
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
            iso_year,
            week,
            idx,
            match_name,
        )

    if idx < 10:
        idx += 1
        await state.update_data(new_match_index=idx)
        await message.answer(f"Готово, внесите следующий матч ({idx} из 10):")
        return

    await message.answer("Готово, все матчи добавлены.")
    await state.clear()

    async with db_pool.acquire() as conn:
        await clear_applied_markers(conn, iso_year, week)

    await clear_forecasts_for_week(iso_year, week)
    await broadcast_new_matches(message.bot, iso_year, week)
    await log_admin_action(message.from_user.id, "new_matches", f"set={iso_year}-W{week}")

    await send_main_menu(message)


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
        ok, msg = await validate_match_set_1_to_10(conn, iso_year, week)
        if not ok:
            await message.answer(msg)
            return

        if await is_applied(conn, iso_year, week):
            await message.answer("Результаты уже внесены. Используйте кнопку «Изменить результаты».")
            return

        await state.update_data(result_index=1, result_iso_year=iso_year, result_week=week, results_mode="enter")

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
        ok, msg = await validate_match_set_1_to_10(conn, iso_year, week)
        if not ok:
            await message.answer(msg)
            return

        if not await is_applied(conn, iso_year, week):
            await message.answer("Новые результаты еще не внесены")
            return

        await rollback_points_for_week(conn, iso_year, week)
        await state.update_data(result_index=1, result_iso_year=iso_year, result_week=week, results_mode="edit")

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
            "SELECT match_name FROM matches WHERE iso_year=$1 AND week=$2 AND match_index=$3",
            iso_year,
            week,
            idx,
        )

    if match:
        await message.answer(
            f"Введите результат для матча {idx}: {match['match_name']} (в формате '3-4' или 'тп')"
        )
        return

    async with db_pool.acquire() as conn:
        await apply_points_for_week(conn, iso_year, week)

    mode = str(data.get("results_mode") or "enter")
    await log_admin_action(message.from_user.id, "results_apply", f"mode={mode}, set={iso_year}-W{week}")

    await message.answer("Готово. Очки пересчитаны и таблицы обновлены.")
    await state.clear()
    await send_main_menu(message)


@router.message(EnterResultsStates.waiting_for_result)
async def process_result_entry(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        await state.clear()
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
            "UPDATE matches SET result=$1 WHERE iso_year=$2 AND week=$3 AND match_index=$4",
            result,
            iso_year,
            week,
            idx,
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
    for i, row in enumerate(top10, start=1):
        leaderboard_text += f"{i}. {row['name']} - {(row['team'] or '—')} - {row['points']} очков\n"

    sent = 0
    for user in all_users:
        uid = int(user["telegram_id"])
        ok = await safe_send_message(message.bot, uid, leaderboard_text)
        if ok:
            sent += 1
        await asyncio.sleep(0.03)

    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE monthleaders SET points = 0")

    await log_admin_action(message.from_user.id, "publish_month", f"sent={sent}")
    await message.answer("Данные месячной таблицы лидеров сброшены и результаты отправлены всем пользователям.")


# ==================== ADMIN: DELETE TABLES ====================
async def prompt_delete_tables(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        await message.answer("Недостаточно прав.")
        return
    await message.answer("Вы уверены, что хотите удалить все таблицы? (Да/Нет)")
    await state.set_state(DeleteTablesStates.waiting_for_confirmation)


@router.message(DeleteTablesStates.waiting_for_confirmation)
async def process_delete_tables_confirmation(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        await state.clear()
        return

    if (message.text or "").strip().lower() == "да":
        await log_admin_action(message.from_user.id, "delete_tables", "drop all")
        async with db_pool.acquire() as conn:
            await conn.execute("DROP TABLE IF EXISTS admin_actions CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS match_points CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS points_applied CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS forecasts CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS matches CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS users CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS monthleaders CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS blacklist CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS forecast_control CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS allowed_teams CASCADE;")

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


# ==================== ADMIN: TABLES ====================
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
    for rank, row in enumerate(rows, start=1):
        response += (
            f"{rank}. {row['name']} - {(row['team'] or '—')} - {row['points']} очков, "
            f"TelegramID: {row['telegram_id']}, Ник: {row['nickname']}\n"
        )
    await send_text_in_chunks(message, response)


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
    for rank, row in enumerate(rows, start=1):
        response += (
            f"{rank}. {row['name']} - {(row['team'] or '—')} - {row['points']} очков, "
            f"TelegramID: {row['telegram_id']}, Ник: {row['nickname']}\n"
        )
    await send_text_in_chunks(message, response)


# ==================== ADMIN: BAN / BLACKLIST / UNBAN ====================
async def admin_ban_start(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        await message.answer("Недостаточно прав.")
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
            tid,
        )
        rank = int(rank_row["rank"]) if rank_row else 0

    await state.update_data(ban_tid=tid)
    await message.answer(f"Забанить пользователя ({rank} место, {user['nickname']}, {tid}) (Да/Нет)")
    await state.set_state(BanStates.waiting_for_confirmation)


@router.message(BanStates.waiting_for_confirmation)
async def admin_ban_confirm(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        await state.clear()
        return

    ans = (message.text or "").strip().lower()
    tid = int((await state.get_data()).get("ban_tid", 0))

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

            await conn.execute(
                """
                INSERT INTO blacklist (telegram_id, nickname, points)
                VALUES ($1, $2, $3)
                ON CONFLICT (telegram_id)
                DO UPDATE SET nickname=EXCLUDED.nickname, points=EXCLUDED.points
                """,
                tid,
                str(user["nickname"] or "ник скрыт"),
                int(user["points"] or 0),
            )

            await conn.execute("DELETE FROM forecasts WHERE telegram_id=$1", tid)
            await conn.execute("DELETE FROM monthleaders WHERE telegram_id=$1", tid)
            await conn.execute("DELETE FROM match_points WHERE telegram_id=$1", tid)
            await conn.execute("DELETE FROM users WHERE telegram_id=$1", tid)

    await log_admin_action(message.from_user.id, "ban_user", f"tid={tid}")
    await message.answer("Пользователь забанен и удалён из таблиц.")
    await state.clear()
    await send_main_menu(message)


async def admin_blacklist_show(message: Message) -> None:
    if not _is_admin(message):
        await message.answer("Недостаточно прав.")
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
        nick = html_escape(r["nickname"] or "ник скрыт")
        lines.append(f"{nick}, {r['telegram_id']}, {int(r['points'] or 0)} очков.")
    await send_text_in_chunks(message, "\n".join(lines))


async def admin_unban_start(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        await message.answer("Недостаточно прав.")
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
    await message.answer("Какое имя присвоить пользователю?")
    await state.set_state(UnbanStates.waiting_for_name)


@router.message(UnbanStates.waiting_for_name)
async def admin_unban_receive_name(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        await state.clear()
        return

    name = (message.text or "").strip()
    if not name:
        await message.answer("Имя не может быть пустым. Введите имя:")
        return

    if contains_profanity_or_insult(name):
        await message.answer("Имя содержит мат. Введите другое имя:")
        return

    async with db_pool.acquire() as conn:
        taken = await conn.fetchval("SELECT 1 FROM users WHERE name=$1 LIMIT 1", name)
        if taken:
            await message.answer("Имя уже занято. Введите другое имя:")
            return

    await state.update_data(unban_name=name)
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

    await state.update_data(unban_points=int(raw))
    await message.answer(
        "За какую команду играет пользователь?\n"
        f"Введите «{FAN_TEAM}» или название команды (регистр не важен)."
    )
    await state.set_state(UnbanStates.waiting_for_team)


@router.message(UnbanStates.waiting_for_team)
async def admin_unban_receive_team(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        await state.clear()
        return

    raw = (message.text or "").strip()
    team = await resolve_team_name(raw)
    if not team:
        await message.answer("Такой команды не существует. Введите еще раз:")
        return

    data = await state.get_data()
    tid = int(data["unban_tid"])
    name = str(data["unban_name"])
    points = int(data["unban_points"])

    nickname = "ник скрыт"
    try:
        chat = await message.bot.get_chat(tid)
        username = getattr(chat, "username", None)
        if username and not contains_profanity_or_insult(username):
            nickname = username
    except Exception:
        nickname = "ник скрыт"

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            exists = await conn.fetchval("SELECT 1 FROM blacklist WHERE telegram_id=$1", tid)
            if not exists:
                await message.answer("Пользователь не находится в черном списке")
                await state.clear()
                await send_main_menu(message)
                return

            taken = await conn.fetchval("SELECT 1 FROM users WHERE name=$1 LIMIT 1", name)
            if taken:
                await message.answer("Имя уже занято. Начните заново: Разблокировать пользователя")
                await state.clear()
                await send_main_menu(message)
                return

            await conn.execute(
                "INSERT INTO users (telegram_id, name, nickname, team, points) VALUES ($1, $2, $3, $4, $5)",
                tid,
                name,
                nickname,
                team,
                points,
            )
            await conn.execute(
                """
                INSERT INTO monthleaders (telegram_id, name, nickname, team, points)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (telegram_id)
                DO UPDATE SET name=EXCLUDED.name, nickname=EXCLUDED.nickname, team=EXCLUDED.team, points=EXCLUDED.points
                """,
                tid,
                name,
                nickname,
                team,
                points,
            )
            await conn.execute("DELETE FROM blacklist WHERE telegram_id=$1", tid)

    await log_admin_action(message.from_user.id, "unban_user", f"tid={tid}, name={name}, team={team}, points={points}")
    await message.answer(f"Пользователь разблокирован ({html_escape(name)}, {tid}, {html_escape(nickname)}, {points} очков).")
    await state.clear()
    await send_main_menu(message)


# ==================== ADMIN: FORECAST CONTROL ====================
async def admin_set_forecast_mode_button(message: Message, mode: str) -> None:
    if not _is_admin(message):
        await message.answer("Недостаточно прав.")
        return

    async with db_pool.acquire() as conn:
        await set_forecast_mode(conn, mode, message.from_user.id)
        current = await get_forecast_mode(conn)

    await log_admin_action(message.from_user.id, "forecast_mode", f"mode={current}")

    if current == "open":
        await message.answer("✅ Прием прогнозов ОТКРЫТ вручную (игнорируя расписание).")
    elif current == "closed":
        await message.answer("⛔ Прием прогнозов ЗАКРЫТ вручную (игнорируя расписание).")
    else:
        await message.answer("🕒 Прием прогнозов переведен в АВТО режим (по расписанию).")

    await send_main_menu(message)


# ==================== ADMIN: BROADCAST ====================
async def admin_broadcast_start(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        return
    await message.answer("Введите сообщение для рассылки всем пользователям:")
    await state.set_state(BroadcastStates.waiting_for_text)


@router.message(BroadcastStates.waiting_for_text)
async def admin_broadcast_receive_text(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        await state.clear()
        return

    text = (message.text or "").strip()
    if not text:
        await message.answer("Сообщение не может быть пустым. Введите текст:")
        return

    await state.update_data(broadcast_text=text)
    await message.answer("Отправить сообщение? (Да/Нет)")
    await state.set_state(BroadcastStates.waiting_for_confirmation)


@router.message(BroadcastStates.waiting_for_confirmation)
async def admin_broadcast_confirm(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        await state.clear()
        return

    ans = (message.text or "").strip().lower()
    data = await state.get_data()
    text = str(data.get("broadcast_text") or "")

    if ans == "нет":
        await message.answer("Отменено.")
        await state.clear()
        await send_main_menu(message)
        return

    if ans != "да":
        await message.answer("Ответьте Да или Нет")
        return

    async with db_pool.acquire() as conn:
        users = await conn.fetch("SELECT telegram_id FROM users")

    sent = 0
    for u in users:
        uid = int(u["telegram_id"])
        ok = await safe_send_message(message.bot, uid, text)
        if ok:
            sent += 1
        await asyncio.sleep(0.03)

    await log_admin_action(message.from_user.id, "broadcast", f"sent={sent}, text={text[:200]}")
    await message.answer(f"Сообщение отправлено. Получателей: {sent}")
    await state.clear()
    await send_main_menu(message)


# ==================== MAIN ====================
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
