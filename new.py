import asyncio
import asyncpg
import logging
import os
from datetime import datetime, time
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, types
from aiogram.types import ReplyKeyboardMarkup
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage

# Загружаем переменные окружения
load_dotenv()

API_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = list(map(int, os.getenv("ADMIN_IDS").split(',')))

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST")

logging.basicConfig(level=logging.INFO)
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

db_pool: asyncpg.Pool = None

# --- Состояния для FSM ---
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

# --- Вспомогательные функции ---
def is_forecast_open() -> bool:
    now = datetime.now()
    # Прогнозы принимаются до пятницы 23:00
    if now.weekday() < 4:
        return True
    elif now.weekday() == 4 and now.time() <= time(23, 0):
        return True
    return False

async def init_db():
    """Инициализация базы данных и создание таблиц."""
    global db_pool
    db_pool = await asyncpg.create_pool(
        user=DB_USER, password=DB_PASSWORD, database=DB_NAME, host=DB_HOST
    )
    async with db_pool.acquire() as conn:
        # Таблица пользователей: id, telegram_id, имя, nickname и очки
        await conn.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT UNIQUE,
            name TEXT,
            nickname TEXT,
            points INTEGER DEFAULT 0
        );
        ''')
        # Таблица прогнозов: храним прогнозы для каждого пользователя по текущей неделе
        await conn.execute('''
        CREATE TABLE IF NOT EXISTS forecasts (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT,
            week INTEGER,
            match_index INTEGER,
            forecast TEXT,
            UNIQUE(telegram_id, week, match_index)
        );
        ''')
        # Таблица матчей: match_index (от 1 до 10), название матча и итоговый счет (результат)
        await conn.execute('''
        CREATE TABLE IF NOT EXISTS matches (
            match_index SERIAL PRIMARY KEY,
            match_name TEXT,
            result TEXT
        );
        ''')
        # Таблица месячной таблицы лидеров
        await conn.execute('''
        CREATE TABLE IF NOT EXISTS monthleaders (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT UNIQUE,
            name TEXT,
            nickname TEXT,
            points INTEGER DEFAULT 0
        );
        ''')

# Функция рассылки новых матчей всем пользователям
async def broadcast_new_matches(message: types.Message):
    async with db_pool.acquire() as conn:
        matches = await conn.fetch("SELECT match_name FROM matches ORDER BY match_index")
        if matches:
            matches_text = "\n".join(match["match_name"] for match in matches)
            broadcast_text = f"Новые матчи на выходные добавлены 😃, не забудь оставить прогноз:\n{matches_text}"
        else:
            broadcast_text = "Новые матчи на выходные добавлены 😃, не забудь оставить прогноз."
        users = await conn.fetch("SELECT telegram_id FROM users")
    for user in users:
        try:
            await bot.send_message(user["telegram_id"], broadcast_text)
        except Exception as e:
            logging.error(f"Ошибка при отправке сообщения пользователю {user['telegram_id']}: {e}")

# Основное меню
async def send_main_menu(message: types.Message):
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
    buttons = ["Мой профиль", "Сделать прогноз", "Таблица лидеров", "Таблица месяц", "Посмотреть мой прогноз", "Посмотреть мои очки"]
    # Для админа добавляем дополнительные кнопки
    if message.from_user.id in ADMIN_IDS:
        buttons.extend(["Внести результаты", "Внести новые матчи", "Опубликовать результаты", "Удалить все таблицы", "Таблица АДМИН", "Месяц АДМИН"])
    keyboard.add(*buttons)
    await message.answer("Выберите действие:", reply_markup=keyboard)

# --- Хэндлеры пользователя ---

# Регистрация пользователя через команду /start
@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    async with db_pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM users WHERE telegram_id=$1", message.from_user.id)
        if not user:
            await message.answer("Привет!👋 Введите, пожалуйста, ваше имя:")
            await RegisterStates.waiting_for_name.set()
        else:
            await send_main_menu(message)

# Обработчик ввода имени
@dp.message_handler(state=RegisterStates.waiting_for_name)
async def process_name(message: types.Message, state: FSMContext):
    name = message.text.strip()
    async with db_pool.acquire() as conn:
        # Проверяем, существует ли уже пользователь с таким именем
        existing = await conn.fetchrow("SELECT * FROM users WHERE name=$1", name)
        if existing:
            await message.answer("Имя уже занято, введите другое")
            return
        # Сохраняем запись с nickname из Telegram (message.from_user.username)
        await conn.execute("""
            INSERT INTO users (telegram_id, name, nickname)
            VALUES ($1, $2, $3)
        """, message.from_user.id, name, message.from_user.username)
    await state.finish()
    rules_text = (
        "Вы успешно зарегистрировались, желаем удачи!\n\n"
        "Правила конкурса:\n"
        "1. Прогнозы принимаются до пятницы 23:00.\n"
        "2. Если прогноз уже внесен, поменять его больше нельзя.\n"
        "3. Очки начисляются по следующим правилам:\n"
        "   - 5 очков за точный счет;\n"
        "   - 3 очка за угаданный исход и разницу мячей;\n"
        "   - 1 очко за угаданный исход матча.\n"
        "4. Таблица лидеров обновляется после внесения результатов администратором.\n"
        "5. Если матч не состоялся или одной из команд присвоен статус "Техническое поражение", пользователи не получают очки.\n"
        "Удачи!"
    )
    await message.answer(rules_text)
    await send_main_menu(message)

# Обработка нажатия кнопок главного меню
@dp.message_handler(lambda message: message.text in ["Мой профиль", "Сделать прогноз", "Таблица лидеров", "Таблица месяц", "Посмотреть мой прогноз", "Посмотреть мои очки", "Внести результаты", "Внести новые матчи", "Опубликовать результаты", "Удалить все таблицы", "Таблица АДМИН", "Месяц АДМИН"])
async def main_menu_handler(message: types.Message, state: FSMContext):
    if message.text == "Мой профиль":
        await handle_my_profile(message)
    elif message.text == "Сделать прогноз":
        await handle_make_forecast(message, state)
    elif message.text == "Таблица лидеров":
        await handle_leaderboard(message)
    elif message.text == "Таблица месяц":
        await handle_month_leaderboard(message)
    elif message.text == "Посмотреть мой прогноз":
        await handle_view_forecast(message)
    elif message.text == "Посмотреть мои очки":
        await handle_view_points(message)
    elif message.text == "Внести результаты" and message.from_user.id in ADMIN_IDS:
        await admin_enter_results(message, state)
    elif message.text == "Внести новые матчи" and message.from_user.id in ADMIN_IDS:
        await admin_new_matches(message, state)
    elif message.text == "Опубликовать результаты" and message.from_user.id in ADMIN_IDS:
        await admin_publish_results(message)
    elif message.text == "Удалить все таблицы" and message.from_user.id in ADMIN_IDS:
        await prompt_delete_tables(message, state)
    elif message.text == "Таблица АДМИН" and message.from_user.id in ADMIN_IDS:
        await handle_admin_table(message)
    elif message.text == "Месяц АДМИН" and message.from_user.id in ADMIN_IDS:
        await handle_month_admin_table(message)
    else:
        await message.answer("Команда не распознана")

# 1. Мой профиль – показывает имя, позицию, очки и никнейм
async def handle_my_profile(message: types.Message):
    async with db_pool.acquire() as conn:
        # Получаем данные пользователя из таблицы users
        user = await conn.fetchrow("SELECT * FROM users WHERE telegram_id=$1", message.from_user.id)
        if not user:
            await message.answer("Пользователь не найден. Используйте /start для регистрации.")
            return

        # Определяем позицию пользователя в полной таблице лидеров (таблица users)
        rows = await conn.fetch("SELECT telegram_id FROM users ORDER BY points DESC")
        overall_rank = 1
        for row in rows:
            if row["telegram_id"] == message.from_user.id:
                break
            overall_rank += 1

        # Определяем позицию пользователя в месячной таблице лидеров (таблица monthleaders)
        monthly_row = await conn.fetchrow("""
            SELECT telegram_id, name, points, rank FROM (
                SELECT telegram_id, name, points, RANK() OVER (ORDER BY points DESC) as rank
                FROM monthleaders
            ) sub
            WHERE telegram_id = $1
        """, message.from_user.id)

        if monthly_row:
            monthly_rank = monthly_row["rank"]
            monthly_points = monthly_row["points"]
        else:
            monthly_rank = None

        # Формируем итоговое сообщение
        response = (
            f"<b>{user['name']}</b>. Ник: {user['nickname']}\n"
            "Полная таблица лидеров:\n"
            f"<b>{overall_rank} место - {user['points']} очков.</b>\n"
            "Таблица лидеров за месяц:\n"
        )
        if monthly_rank is not None:
            response += f"<b>{monthly_rank} место - {monthly_points} очков.</b>"
        else:
            response += "<b>Нет записи в месячной таблице лидеров.</b>"

        await message.answer(response, parse_mode='HTML')


# 2. Сделать прогноз – пользователь вводит прогнозы по матчам
async def handle_make_forecast(message: types.Message, state: FSMContext):
    if not is_forecast_open():
        await message.answer("Прием прогнозов остановлен")
        return
    async with db_pool.acquire() as conn:
        matches = await conn.fetch("SELECT * FROM matches ORDER BY match_index")
        if not matches:
            await message.answer("Матчей еще нет")
            return
        week = datetime.now().isocalendar()[1]
        existing = await conn.fetch("SELECT * FROM forecasts WHERE telegram_id=$1 AND week=$2", message.from_user.id, week)
        if existing:
            await message.answer("Прогноз на эту неделю уже сделан, дождитесь следующей недели")
            return
    await state.update_data(forecast_week=week, current_match_index=1)
    await send_next_match(message, state)

async def send_next_match(message: types.Message, state: FSMContext):
    data = await state.get_data()
    current_match_index = data.get("current_match_index")
    async with db_pool.acquire() as conn:
        match = await conn.fetchrow("SELECT * FROM matches WHERE match_index=$1", current_match_index)
    if match:
        await ForecastStates.waiting_for_score.set()
        await message.answer(
            f"Прогноз для матча {current_match_index}: {match['match_name']}\nВведите счет в формате '2-1'"
        )
    else:
        await message.answer("Прогноз принят, желаем удачи!")
        await state.finish()
        await send_main_menu(message)

@dp.message_handler(state=ForecastStates.waiting_for_score)
async def process_forecast_score(message: types.Message, state: FSMContext):
    # Дополнительная проверка времени перед обработкой ввода
    if not is_forecast_open():
        await message.answer("Время для внесения прогнозов истекло. Прогноз не сохранён.")
        await state.finish()
        await send_main_menu(message)
        return

    score = message.text.strip()
    if score.count('-') != 1:
        await message.answer("Неверный формат. Введите счет в формате '2-1'")
        return
    parts = score.split('-')
    if not (parts[0].isdigit() and parts[1].isdigit()):
        await message.answer("Неверный формат. Введите счет в формате '2-1'")
        return
    data = await state.get_data()
    week = data.get("forecast_week")
    current_match_index = data.get("current_match_index")
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO forecasts (telegram_id, week, match_index, forecast)
            VALUES ($1, $2, $3, $4)
        """, message.from_user.id, week, current_match_index, score)
    current_match_index += 1
    await state.update_data(current_match_index=current_match_index)
    async with db_pool.acquire() as conn:
        next_match = await conn.fetchrow("SELECT * FROM matches WHERE match_index=$1", current_match_index)
    if next_match:
        # Проверка времени перед переходом к следующему матчу
        if not is_forecast_open():
            await message.answer("Время для внесения прогнозов истекло. Прогноз сохранён.")
            await state.finish()
            await send_main_menu(message)
        else:
            await send_next_match(message, state)
    else:
        await message.answer("Прогноз принят, желаем удачи!")
        await state.finish()
        await send_main_menu(message)

# 3. Таблица лидеров – вывод топ-10 пользователей
@dp.message_handler(lambda message: message.text == "Таблица лидеров")
async def handle_leaderboard(message: types.Message):
    async with db_pool.acquire() as conn:
        # Получаем топ-10 пользователей
        top_rows = await conn.fetch("SELECT telegram_id, name, points FROM users ORDER BY points DESC LIMIT 10")
        if not top_rows:
            await message.answer("Таблица лидеров пуста.")
            return

        response = "Таблица лидеров:\n"
        rank = 1
        for row in top_rows:
            response += f"{rank}. {row['name']} - {row['points']} очков\n"
            rank += 1

        # Определяем позицию текущего пользователя с использованием оконной функции
        user_row = await conn.fetchrow("""
            SELECT telegram_id, name, points, rank FROM (
                SELECT telegram_id, name, points, RANK() OVER (ORDER BY points DESC) AS rank
                FROM users
            ) sub
            WHERE telegram_id = $1
        """, message.from_user.id)

        if user_row:
            response += f"\n<b>Ваш результат:</b> {user_row['rank']}. - {user_row['name']} - {user_row['points']} очков"
        
        await message.answer(response, parse_mode='HTML')


# 4. Посмотреть мой прогноз – показывает введенные прогнозы
async def handle_view_forecast(message: types.Message):
    week = datetime.now().isocalendar()[1]
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT m.match_name, f.forecast FROM forecasts f
            JOIN matches m ON f.match_index = m.match_index
            WHERE f.telegram_id=$1 AND f.week=$2
            ORDER BY f.match_index
        ''', message.from_user.id, week)
        if not rows:
            await message.answer("Прогноз не найден")
            return
        response = ""
        for row in rows:
            response += f"{row['match_name']} {row['forecast']}\n"
        await message.answer(response)

# 5. Таблица лидеров за месяц
@dp.message_handler(lambda message: message.text == "Таблица месяц")
async def handle_month_leaderboard(message: types.Message):
    async with db_pool.acquire() as conn:
        # Получаем топ-10 пользователей из таблицы monthleaders
        top_rows = await conn.fetch("SELECT name, points FROM monthleaders ORDER BY points DESC LIMIT 10")
        if not top_rows:
            await message.answer("Месячная таблица лидеров пуста.")
            return
        response = "Топ-10 за этот месяц:\n"
        rank = 1
        for row in top_rows:
            response += f"{rank}. {row['name']} - {row['points']} очков\n"
            rank += 1

        # Запрос для определения позиции текущего пользователя с использованием оконной функции
        user_row = await conn.fetchrow("""
            SELECT telegram_id, name, points, rank FROM (
                SELECT telegram_id, name, points, RANK() OVER (ORDER BY points DESC) as rank
                FROM monthleaders
            ) sub
            WHERE telegram_id = $1
        """, message.from_user.id)

        if user_row:
            # Жирным выделяем номер места
            response += f"\n<b>Ваш результат:</b> {user_row['rank']}. {user_row['name']} - {user_row['points']} очков"

        await message.answer(response, parse_mode='HTML')


def compute_points(actual: str, forecast: str) -> int:
    """Вычисляет очки за прогноз по результату матча.
    Если реальный результат - 'тп' (технический проигрыш), возвращает 0 очков."""
    try:
        # Если результат матча технический проигрыш, начисляем 0 очков для всех
        if actual.lower() == "тп":
            return 0

        actual_parts = actual.split('-')
        forecast_parts = forecast.split('-')
        if len(actual_parts) != 2 or len(forecast_parts) != 2:
            return 0
        actual_home, actual_away = int(actual_parts[0]), int(actual_parts[1])
        forecast_home, forecast_away = int(forecast_parts[0]), int(forecast_parts[1])
        # Определяем исход матча: 1 — победа хозяев, 0 — ничья, -1 — победа гостей
        actual_outcome = 1 if actual_home > actual_away else (0 if actual_home == actual_away else -1)
        forecast_outcome = 1 if forecast_home > forecast_away else (0 if forecast_home == forecast_away else -1)
        if actual_outcome != forecast_outcome:
            return 0
        # Угадан исход матча
        points = 1
        # Если угадана разница мячей, то 3 очка
        if abs(actual_home - actual_away) == abs(forecast_home - forecast_away):
            points = 3
            # Если угадан точный счёт, то 5 очков
            if actual_home == forecast_home and actual_away == forecast_away:
                points = 5
        return points
    except Exception as e:
        return 0


# 6. Посмотреть мои очки – показывает для каждого матча результат, прогноз и начисленные очки
@dp.message_handler(lambda message: message.text == "Посмотреть мои очки")
async def handle_view_points(message: types.Message):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT m.match_name, m.result, f.forecast 
            FROM forecasts f
            JOIN matches m ON f.match_index = m.match_index
            WHERE f.telegram_id=$1 AND m.result IS NOT NULL
            ORDER BY f.match_index DESC
            LIMIT 10
        ''', message.from_user.id)

    if not rows:
        await message.answer("Пока нет данных для отображения. Возможно, результаты ещё не внесены.")
        return

    rows = list(reversed(rows))

    response_lines = []
    for idx, row in enumerate(rows, start=1):
        points = compute_points(row['result'], row['forecast'])
        line = (
            f"{idx}. <b>{row['match_name']} {row['result']}</b>\n"
            f"Ваш прогноз {row['forecast']} ({points} очков)"
        )
        response_lines.append(line)

    response = "\n\n".join(response_lines)
    await message.answer(response, parse_mode='HTML')









# --- Хэндлеры для администратора ---

# 1. Внести результаты – ввод результатов матчей и пересчет очков
@dp.message_handler(lambda message: message.from_user.id in ADMIN_IDS and message.text == "Внести результаты")
async def admin_enter_results(message: types.Message, state: FSMContext):
    async with db_pool.acquire() as conn:
        matches = await conn.fetch("SELECT * FROM matches ORDER BY match_index")
        if not matches:
            await message.answer("Матчей еще нет")
            return
    await state.update_data(result_index=1)
    await EnterResultsStates.waiting_for_result.set()
    await send_result_entry(message, state)

async def send_result_entry(message: types.Message, state: FSMContext):
    data = await state.get_data()
    result_index = data.get("result_index")
    async with db_pool.acquire() as conn:
        match = await conn.fetchrow("SELECT * FROM matches WHERE match_index=$1", result_index)
    if match:
        await message.answer(
            f"Введите результат для матча {result_index}: {match['match_name']} (в формате '3-4')"
        )
    else:
        await calculate_points(message)
        await message.answer("Внесите новые матчи, нажав кнопку 'Внести новые матчи'")
        await state.finish()

@dp.message_handler(lambda message: message.from_user.id in ADMIN_IDS, state=EnterResultsStates.waiting_for_result)
async def process_result_entry(message: types.Message, state: FSMContext):
    result = message.text.strip().lower()
    # Если введён «тп», принимаем его как корректное значение
    if result == "тп":
        pass  # Будет обработано как технический проигрыш (0 очков для всех)
    else:
        # Проверка формата для обычного результата, например "3-4"
        if result.count('-') != 1:
            await message.answer("Неверный формат. Введите результат в формате '3-4' или 'тп'")
            return
        parts = result.split('-')
        if not (parts[0].isdigit() and parts[1].isdigit()):
            await message.answer("Неверный формат. Введите результат в формате '3-4' или 'тп'")
            return
    data = await state.get_data()
    result_index = data.get("result_index")
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE matches SET result=$1 WHERE match_index=$2", result, result_index)
    result_index += 1
    await state.update_data(result_index=result_index)
    await send_result_entry(message, state)


async def calculate_points(message: types.Message):
    week = datetime.now().isocalendar()[1]
    async with db_pool.acquire() as conn:
        forecasts = await conn.fetch("SELECT * FROM forecasts WHERE week=$1", week)
        for forecast in forecasts:
            match = await conn.fetchrow("SELECT result FROM matches WHERE match_index=$1", forecast["match_index"])
            if match and match["result"]:
                actual = match["result"]
                user_forecast = forecast["forecast"]
                # Здесь, если actual == "тп", compute_points вернет 0
                points = compute_points(actual, user_forecast)
                # Обновляем очки в таблице users
                await conn.execute("UPDATE users SET points = points + $1 WHERE telegram_id=$2", points, forecast["telegram_id"])
                # Обновляем или вставляем запись в таблицу monthleaders
                user = await conn.fetchrow("SELECT name, nickname FROM users WHERE telegram_id=$1", forecast["telegram_id"])
                existing_ml = await conn.fetchrow("SELECT * FROM monthleaders WHERE telegram_id=$1", forecast["telegram_id"])
                if existing_ml:
                    await conn.execute(
                        "UPDATE monthleaders SET points = points + $1, nickname = $2 WHERE telegram_id=$3",
                        points, user["nickname"], forecast["telegram_id"]
                    )
                else:
                    await conn.execute(
                        "INSERT INTO monthleaders (telegram_id, name, nickname, points) VALUES ($1, $2, $3, $4)",
                        forecast["telegram_id"], user["name"], user["nickname"], points
                    )
    await message.answer("Результаты внесены, таблица лидеров обновлена.")


# 2. Внести новые матчи – ввод 10 матчей по одному
@dp.message_handler(lambda message: message.from_user.id in ADMIN_IDS and message.text == "Внести новые матчи")
async def admin_new_matches(message: types.Message, state: FSMContext):
    await state.update_data(new_match_index=1)
    await NewMatchesStates.waiting_for_match.set()
    await message.answer("Введите название матча 1 из 10:")

@dp.message_handler(lambda message: message.from_user.id in ADMIN_IDS, state=NewMatchesStates.waiting_for_match)
async def process_new_match(message: types.Message, state: FSMContext):
    match_name = message.text.strip()
    data = await state.get_data()
    new_match_index = data.get("new_match_index")
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO matches (match_index, match_name, result)
            VALUES ($1, $2, NULL)
            ON CONFLICT (match_index)
            DO UPDATE SET match_name = EXCLUDED.match_name, result = NULL
        """, new_match_index, match_name)
    if new_match_index < 10:
        new_match_index += 1
        await state.update_data(new_match_index=new_match_index)
        await message.answer(f"Готово, внесите следующий матч ({new_match_index} из 10):")
    else:
        await message.answer("Готово, все матчи добавлены.")
        await state.finish()
        # Удаляем прогнозы для текущей недели
        await clear_forecasts()
        # После внесения новых матчей рассылаем сообщение всем пользователям
        await broadcast_new_matches(message)

async def clear_forecasts():
    """Удаляем прогнозы для текущей недели, чтобы пользователи могли ввести новые."""
    week = datetime.now().isocalendar()[1]
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM forecasts WHERE week=$1", week)

# 3. Опубликовать результаты – выгрузка топ-10 из таблицы monthleaders и сброс очков
@dp.message_handler(lambda message: message.from_user.id in ADMIN_IDS and message.text == "Опубликовать результаты")
async def admin_publish_results(message: types.Message):
    async with db_pool.acquire() as conn:
        # Получаем топ-10 пользователей из таблицы monthleaders
        top10 = await conn.fetch("SELECT telegram_id, name, points FROM monthleaders ORDER BY points DESC LIMIT 10")
        leaderboard_text = "Месячная таблица лидеров:\n"
        rank = 1
        for row in top10:
            leaderboard_text += f"{rank}. {row['name']} - {row['points']} очков\n"
            rank += 1

        # Получаем список всех пользователей
        all_users_rows = await conn.fetch("SELECT telegram_id FROM users")
    # Отправляем сообщение каждому пользователю
    for user in all_users_rows:
        try:
            await bot.send_message(user["telegram_id"], leaderboard_text, parse_mode=types.ParseMode.MARKDOWN)
        except Exception as e:
            logging.error(f"Ошибка при отправке сообщения пользователю {user['telegram_id']}: {e}")
    # Сбрасываем очки в таблице monthleaders
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE monthleaders SET points = 0")
    await message.answer("Данные месячной таблицы лидеров сброшены и результаты отправлены всем пользователям.")

# 4. Удаление всех таблиц и создание новых
@dp.message_handler(lambda message: message.from_user.id in ADMIN_IDS and message.text == "Удалить все таблицы")
async def prompt_delete_tables(message: types.Message, state: FSMContext):
    await message.answer("Вы уверены, что хотите удалить все таблицы? (Да/Нет)")
    await DeleteTablesStates.waiting_for_confirmation.set()

@dp.message_handler(lambda message: message.from_user.id in ADMIN_IDS, state=DeleteTablesStates.waiting_for_confirmation)
async def process_delete_tables_confirmation(message: types.Message, state: FSMContext):
    if message.text.strip().lower() == "да":
        async with db_pool.acquire() as conn:
            # Удаляем таблицы
            await conn.execute("DROP TABLE IF EXISTS forecasts CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS matches CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS users CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS monthleaders CASCADE;")
            # Пересоздаем таблицы без данных
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    telegram_id BIGINT UNIQUE,
                    name TEXT UNIQUE,
                    nickname TEXT,
                    points INTEGER DEFAULT 0
                );
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS forecasts (
                    id SERIAL PRIMARY KEY,
                    telegram_id BIGINT,
                    week INTEGER,
                    match_index INTEGER,
                    forecast TEXT,
                    UNIQUE(telegram_id, week, match_index)
                );
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS matches (
                    match_index SERIAL PRIMARY KEY,
                    match_name TEXT,
                    result TEXT
                );
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS monthleaders (
                    id SERIAL PRIMARY KEY,
                    telegram_id BIGINT UNIQUE,
                    name TEXT,
                    nickname TEXT,
                    points INTEGER DEFAULT 0
                );
            ''')
        await message.answer("Все таблицы удалены и созданы заново без данных!")
    else:
        await message.answer("Удаление таблиц отменено")
    await state.finish()

# 5. Таблица АДМИН – полная таблица лидеров с данными из таблицы users
@dp.message_handler(lambda message: message.from_user.id in ADMIN_IDS and message.text == "Таблица АДМИН")
async def handle_admin_table(message: types.Message):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT telegram_id, name, nickname, points FROM users ORDER BY points DESC")
        if not rows:
            await message.answer("Таблица лидеров пуста.")
            return
        response = "Полная таблица лидеров:\n"
        rank = 1
        for row in rows:
            response += f"{rank}. {row['name']} - {row['points']} очков, TelegramID: {row['telegram_id']}, Ник: {row['nickname']}\n"
            rank += 1
        await message.answer(response)

# 6. Месяц АДМИН – полная таблица лидеров из таблицы monthleaders
@dp.message_handler(lambda message: message.from_user.id in ADMIN_IDS and message.text == "Месяц АДМИН")
async def handle_month_admin_table(message: types.Message):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT telegram_id, name, nickname, points FROM monthleaders ORDER BY points DESC")
        if not rows:
            await message.answer("Месячная таблица лидеров пуста.")
            return
        response = "Месячная таблица лидеров (АДМИН):\n"
        rank = 1
        for row in rows:
            response += f"{rank}. {row['name']} - {row['points']} очков, TelegramID: {row['telegram_id']}, Ник: {row['nickname']}\n"
            rank += 1
        await message.answer(response)

async def on_startup(dispatcher):
    # Инициализация базы данных, создание или миграция таблиц
    await init_db()
    # Можно добавить рассылку сообщений о новых изменениях или иные операции
    logging.info("Бот запущен и обновления применены")
    
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(init_db())
    from aiogram import executor
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)

