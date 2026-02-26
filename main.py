import asyncio
import logging
import os
from datetime import datetime, time, timedelta
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

# -------------------- GLOBALS --------------------
db_pool: Optional[asyncpg.Pool] = None
router = Router()

MENU_BUTTONS = [
    "Мой профиль",
    "Сделать прогноз",
    "Таблица лидеров",
    "Таблица месяц",
    "Посмотреть мой прогноз",
    "Посмотреть мои очки",
]
ADMIN_BUTTONS = [
    "Внести результаты",
    "Внести новые матчи",
    "Опубликовать результаты",
    "Удалить все таблицы",
    "Таблица АДМИН",
    "Месяц АДМИН",
]
ALL_BUTTONS = MENU_BUTTONS + ADMIN_BUTTONS


# -------------------- FSM --------------------
class RegisterStates(StatesGroup):
    waiting_for_name = State()


class ForecastStates(StatesGroup):
    waiting_for_score = State()


class NewMatchesStates(StatesGroup):
    waiting_for_match = State()


class EnterResultsStates(StatesGroup):
    waiting_for_result = State()


class DeleteTablesStates(StatesGroup):
    waiting_for_confirmation = State()


# -------------------- TIME HELPERS --------------------
def moscow_now() -> datetime:
    return datetime.now(MOSCOW_TZ)


def current_isoyear_week() -> Tuple[int, int]:
    iso = moscow_now().isocalendar()
    return int(iso.year), int(iso.week)


def previous_isoyear_week() -> Tuple[int, int]:
    d = moscow_now().date() - timedelta(days=7)
    iso = d.isocalendar()
    return int(iso.year), int(iso.week)


def is_forecast_open() -> bool:
    t = moscow_now()
    weekday = t.weekday()  # Пн=0..Вс=6
    current_time = t.time()

    # вторник — с 18:00
    if weekday == 1:
        return current_time >= time(18, 0)
    # пятница — до 21:00
    elif weekday == 4:
        return current_time < time(21, 0)
    # среда/четверг — открыто
    elif weekday in (2, 3):
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


# -------------------- MIDDLEWARE --------------------
class RegistrationCheckMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
        event: Message,
        data: Dict[str, Any],
    ) -> Any:
        if event.text and event.text.startswith("/start"):
            return await handler(event, data)

        state: Optional[FSMContext] = data.get("state")
        if state:
            current_state = await state.get_state()
            if current_state == RegisterStates.waiting_for_name.state:
                return await handler(event, data)

        if db_pool is None:
            await event.answer("База данных ещё не инициализирована, попробуйте позже.")
            return

        async with db_pool.acquire() as conn:
            user = await conn.fetchrow("SELECT 1 FROM users WHERE telegram_id=$1", event.from_user.id)

        if not user:
            await event.answer("Вы не зарегистрированы. Пожалуйста, зарегистрируйтесь, введя /start")
            return

        return await handler(event, data)


router.message.middleware(RegistrationCheckMiddleware())


# -------------------- DB (with safe migration) --------------------
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
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                telegram_id BIGINT UNIQUE,
                name TEXT UNIQUE,
                nickname TEXT,
                points INTEGER DEFAULT 0
            );
            """
        )

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS monthleaders (
                id SERIAL PRIMARY KEY,
                telegram_id BIGINT UNIQUE,
                name TEXT,
                nickname TEXT,
                points INTEGER DEFAULT 0
            );
            """
        )

        # если в БД остались старые matches/forecasts без iso_year/week — переименуем их
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


# -------------------- ADMIN HELPERS (NEW) --------------------
async def get_latest_matches_set(conn: asyncpg.Connection) -> Optional[Tuple[int, int]]:
    """
    Возвращает (iso_year, week) самого свежего набора матчей, который есть в matches.
    """
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


async def latest_set_stats(conn: asyncpg.Connection, iso_year: int, week: int) -> Tuple[int, int]:
    """
    total = сколько матчей в наборе
    missing = сколько матчей без результата
    """
    total = int(
        await conn.fetchval(
            "SELECT COUNT(*) FROM matches WHERE iso_year=$1 AND week=$2",
            iso_year, week
        )
    )
    missing = int(
        await conn.fetchval(
            "SELECT COUNT(*) FROM matches WHERE iso_year=$1 AND week=$2 AND result IS NULL",
            iso_year, week
        )
    )
    return total, missing


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
    if db_pool is None:
        return

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
    if db_pool is None:
        return
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM forecasts WHERE iso_year=$1 AND week=$2", iso_year, week)


# -------------------- USER HANDLERS --------------------
@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext) -> None:
    if db_pool is None:
        await message.answer("База данных ещё не инициализирована, попробуйте позже.")
        return

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

    async with db_pool.acquire() as conn:
        existing = await conn.fetchrow("SELECT 1 FROM users WHERE name=$1", name)
        if existing:
            await message.answer("Имя уже занято, введите другое")
            return

        nickname = message.from_user.username if message.from_user.username else "ник скрыт"
        await conn.execute(
            "INSERT INTO users (telegram_id, name, nickname) VALUES ($1, $2, $3)",
            message.from_user.id, name, nickname
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


# меню — только когда нет FSM состояния
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
    elif text == "Таблица месяц":
        await handle_month_leaderboard(message)
    elif text == "Посмотреть мой прогноз":
        await handle_view_forecast(message)
    elif text == "Посмотреть мои очки":
        await handle_view_points(message)
    elif text == "Внести результаты":
        await admin_enter_results(message, state)
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
    else:
        await message.answer("Команда не распознана")


@router.message(StateFilter(None), F.text)
async def fallback_text(message: Message) -> None:
    await message.answer("Команда не распознана. Выберите действие из меню или нажмите /start")


async def handle_my_profile(message: Message) -> None:
    async with db_pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM users WHERE telegram_id=$1", message.from_user.id)
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
            SELECT telegram_id, name, points, rank FROM (
                SELECT telegram_id, name, points, RANK() OVER (ORDER BY points DESC) as rank
                FROM monthleaders
            ) sub
            WHERE telegram_id = $1
            """,
            message.from_user.id,
        )

    response = (
        f"<b>{user['name']}</b>. Ник: {user['nickname']}\n"
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


async def handle_leaderboard(message: Message) -> None:
    async with db_pool.acquire() as conn:
        top_rows = await conn.fetch(
            "SELECT telegram_id, name, points FROM users ORDER BY points DESC LIMIT 10"
        )
        if not top_rows:
            await message.answer("Таблица лидеров пуста.")
            return

        user_row = await conn.fetchrow(
            """
            SELECT telegram_id, name, points, rank FROM (
                SELECT telegram_id, name, points, RANK() OVER (ORDER BY points DESC) AS rank
                FROM users
            ) sub
            WHERE telegram_id = $1
            """,
            message.from_user.id,
        )

    response = "Таблица лидеров:\n"
    for i, row in enumerate(top_rows, start=1):
        response += f"{i}. {row['name']} - {row['points']} очков\n"

    if user_row:
        response += f"\n<b>Ваш результат:</b> {user_row['rank']}. - {user_row['name']} - {user_row['points']} очков"

    await message.answer(response)


async def handle_month_leaderboard(message: Message) -> None:
    async with db_pool.acquire() as conn:
        top_rows = await conn.fetch("SELECT name, points FROM monthleaders ORDER BY points DESC LIMIT 10")
        if not top_rows:
            await message.answer("Месячная таблица лидеров пуста.")
            return

        user_row = await conn.fetchrow(
            """
            SELECT telegram_id, name, points, rank FROM (
                SELECT telegram_id, name, points, RANK() OVER (ORDER BY points DESC) as rank
                FROM monthleaders
            ) sub
            WHERE telegram_id = $1
            """,
            message.from_user.id,
        )

    response = "Топ-10 за этот месяц:\n"
    for i, row in enumerate(top_rows, start=1):
        response += f"{i}. {row['name']} - {row['points']} очков\n"

    if user_row:
        response += f"\n<b>Ваш результат:</b> {user_row['rank']}. {user_row['name']} - {user_row['points']} очков"

    await message.answer(response)


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


async def handle_view_points(message: Message) -> None:
    iso_year, week = previous_isoyear_week()

    async with db_pool.acquire() as conn:
        cnt = await conn.fetchval(
            "SELECT COUNT(*) FROM forecasts WHERE telegram_id=$1 AND iso_year=$2 AND week=$3",
            message.from_user.id, iso_year, week
        )
        if int(cnt) == 0:
            await message.answer("Вы не делали прогноз на прошлой неделе")
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
        points = compute_points(row["result"], row["forecast"])
        response_lines.append(
            f"{idx}. <b>{row['match_name']} {row['result']}</b>\n"
            f"Ваш прогноз {row['forecast']} ({points} очков)"
        )

    await message.answer("\n\n".join(response_lines))


# -------------------- ADMIN HANDLERS --------------------
def _is_admin(message: Message) -> bool:
    return message.from_user.id in ADMIN_IDS


# ✅ ПРАВКА #1: Внести результаты — ориентируемся на "нынешние матчи" (последний внесённый набор)
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
        total, _missing = await latest_set_stats(conn, iso_year, week)

        # если набор не полный — лучше не начинать внесение результатов
        if total < 10:
            await message.answer("Матчи добавлены не полностью. Сначала внесите все 10 матчей.")
            return

    await state.update_data(result_index=1, result_iso_year=iso_year, result_week=week)
    await state.set_state(EnterResultsStates.waiting_for_result)
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
    else:
        await calculate_points_for_week(message, iso_year, week)
        await message.answer("Внесите новые матчи, нажав кнопку 'Внести новые матчи'")
        await state.clear()


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


async def calculate_points_for_week(message: Message, iso_year: int, week: int) -> None:
    async with db_pool.acquire() as conn:
        forecasts = await conn.fetch(
            """
            SELECT telegram_id, match_index, forecast
            FROM forecasts
            WHERE iso_year=$1 AND week=$2
            """,
            iso_year, week
        )

        for fc in forecasts:
            match = await conn.fetchrow(
                """
                SELECT result
                FROM matches
                WHERE iso_year=$1 AND week=$2 AND match_index=$3
                """,
                iso_year, week, fc["match_index"]
            )
            if not (match and match["result"]):
                continue

            points = compute_points(match["result"], fc["forecast"])

            await conn.execute(
                "UPDATE users SET points = points + $1 WHERE telegram_id=$2",
                points, fc["telegram_id"]
            )

            user = await conn.fetchrow(
                "SELECT name, nickname FROM users WHERE telegram_id=$1",
                fc["telegram_id"]
            )
            if not user:
                continue

            exists_ml = await conn.fetchval(
                "SELECT 1 FROM monthleaders WHERE telegram_id=$1",
                fc["telegram_id"]
            )
            if exists_ml:
                await conn.execute(
                    "UPDATE monthleaders SET points = points + $1, nickname=$2 WHERE telegram_id=$3",
                    points, user["nickname"], fc["telegram_id"]
                )
            else:
                await conn.execute(
                    "INSERT INTO monthleaders (telegram_id, name, nickname, points) VALUES ($1, $2, $3, $4)",
                    fc["telegram_id"], user["name"], user["nickname"], points
                )

    await message.answer("Результаты внесены, таблица лидеров обновлена.")


# ✅ ПРАВКА #2: запрет "Внести новые матчи", если по прошлому набору не внесены результаты
async def admin_new_matches(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        await message.answer("Недостаточно прав.")
        return

    async with db_pool.acquire() as conn:
        latest = await get_latest_matches_set(conn)
        if latest:
            iso_year_old, week_old = latest
            total, missing = await latest_set_stats(conn, iso_year_old, week_old)

            # блокируем только если есть полный набор (10) и хотя бы один результат не внесён
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
    else:
        await message.answer("Готово, все матчи добавлены.")
        await state.clear()

        # как в исходной логике: очищаем прогнозы этой недели, чтобы могли ввести новые
        await clear_forecasts_for_week(iso_year, week)

        # рассылка
        await broadcast_new_matches(message.bot, iso_year, week)


async def admin_publish_results(message: Message) -> None:
    if not _is_admin(message):
        await message.answer("Недостаточно прав.")
        return

    async with db_pool.acquire() as conn:
        top10 = await conn.fetch(
            "SELECT telegram_id, name, points FROM monthleaders ORDER BY points DESC LIMIT 10"
        )
        all_users = await conn.fetch("SELECT telegram_id FROM users")

    leaderboard_text = "Месячная таблица лидеров:\n"
    for i, row in enumerate(top10, start=1):
        leaderboard_text += f"{i}. {row['name']} - {row['points']} очков\n"

    for u in all_users:
        try:
            await message.bot.send_message(u["telegram_id"], leaderboard_text)
        except Exception as e:
            logging.error("Ошибка при отправке пользователю %s: %s", u["telegram_id"], e)

    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE monthleaders SET points = 0")

    await message.answer("Данные месячной таблицы лидеров сброшены и результаты отправлены всем пользователям.")


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
            await conn.execute("DROP TABLE IF EXISTS forecasts CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS matches CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS users CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS monthleaders CASCADE;")

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


async def handle_admin_table(message: Message) -> None:
    if not _is_admin(message):
        await message.answer("Недостаточно прав.")
        return

    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT telegram_id, name, nickname, points FROM users ORDER BY points DESC")

    if not rows:
        await message.answer("Таблица лидеров пуста.")
        return

    response = "Полная таблица лидеров:\n"
    for i, row in enumerate(rows, start=1):
        response += (
            f"{i}. {row['name']} - {row['points']} очков, "
            f"TelegramID: {row['telegram_id']}, Ник: {row['nickname']}\n"
        )
    await message.answer(response)


async def handle_month_admin_table(message: Message) -> None:
    if not _is_admin(message):
        await message.answer("Недостаточно прав.")
        return

    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT telegram_id, name, nickname, points FROM monthleaders ORDER BY points DESC")

    if not rows:
        await message.answer("Месячная таблица лидеров пуста.")
        return

    response = "Месячная таблица лидеров (АДМИН):\n"
    for i, row in enumerate(rows, start=1):
        response += (
            f"{i}. {row['name']} - {row['points']} очков, "
            f"TelegramID: {row['telegram_id']}, Ник: {row['nickname']}\n"
        )
    await message.answer(response)


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

    # чтобы polling не конфликтовал с webhook
    await bot.delete_webhook(drop_pending_updates=True)

    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        if db_pool is not None:
            await db_pool.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
