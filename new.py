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
    waiting_for_phone = State()

class ForecastStates(StatesGroup):
    waiting_for_score = State()

class NewMatchesStates(StatesGroup):
    waiting_for_match = State()

class EnterResultsStates(StatesGroup):
    waiting_for_result = State()

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
        # Таблица пользователей: id, telegram_id, имя и очки
        await conn.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT UNIQUE,
            name TEXT,
            phone TEXT UNIQUE,
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
            phone TEXT,
            points INTEGER DEFAULT 0
        );
        ''')

# В обновлённом основном меню для администратора добавляем кнопку "Удалить все таблицы"
async def send_main_menu(message: types.Message):
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
    buttons = ["Мой профиль", "Сделать прогноз", "Таблица лидеров", "Таблица лидеров за этот месяц", "Посмотреть мой прогноз"]
    # Для админа добавляем кнопки
    if message.from_user.id in ADMIN_IDS:
        buttons.extend(["Внести результаты", "Внести новые матчи", "Опубликовать результаты", "Удалить все таблицы", "Таблица АДМИН"])
    keyboard.add(*buttons)
    await message.answer("Выберите действие:", reply_markup=keyboard)

# --- Хэндлеры пользователя ---

# Регистрация пользователя через команду /start
# При нажатии на /start проверяем регистрацию пользователя
@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    async with db_pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM users WHERE telegram_id=$1", message.from_user.id)
        if not user:
            await message.answer("Привет! Введите, пожалуйста, ваше имя:")
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
            return  # Остаёмся в этом состоянии для повторного ввода
        # Если имя свободно, создаём запись с phone=NULL
        await conn.execute("""
            INSERT INTO users (telegram_id, name, phone)
            VALUES ($1, $2, NULL)
        """, message.from_user.id, name)
    await message.answer("Введите номер телефона в формате 9ХХХХХХХХХ (ровно 10 цифр, без кода страны):")
    await RegisterStates.waiting_for_phone.set()

# Обработчик ввода номера телефона
@dp.message_handler(state=RegisterStates.waiting_for_phone)
async def process_phone(message: types.Message, state: FSMContext):
    phone = message.text.strip()
    # Проверяем, что номер состоит ровно из 10 цифр
    if not (phone.isdigit() and len(phone) == 10):
        await message.answer("Неверный формат. Введите номер телефона ровно из 10 цифр (например, 9123456789)")
        return
    async with db_pool.acquire() as conn:
        # Проверяем, существует ли уже пользователь с таким номером телефона
        existing_phone = await conn.fetchrow("SELECT * FROM users WHERE phone=$1", phone)
        if existing_phone:
            await message.answer("Пользователь с таким номером телефона уже существует. Введите другой номер:")
            return
        # Обновляем запись пользователя, добавляя номер телефона, используя telegram_id
        await conn.execute("""
            UPDATE users SET phone = $1 WHERE telegram_id = $2
        """, phone, message.from_user.id)
    await state.finish()
    await message.answer("Вы успешно зарегистрировались, желаем удачи!")
    await send_main_menu(message)

# Обработка нажатия кнопок главного меню
@dp.message_handler(lambda message: message.text in ["Мой профиль", "Сделать прогноз", "Таблица лидеров", "Посмотреть мой прогноз", "Внести результаты", "Внести новые матчи", "Опубликовать результаты", "Удалить все таблицы", "Таблица АДМИН"])
async def main_menu_handler(message: types.Message, state: FSMContext):
    if message.text == "Мой профиль":
        await handle_my_profile(message)
    elif message.text == "Сделать прогноз":
        await handle_make_forecast(message, state)
    elif message.text == "Таблица лидеров":
        await handle_leaderboard(message)
    elif message.text == "Таблица лидеров за этот месяц":
        await handle_month_leaderboard(message)
    elif message.text == "Посмотреть мой прогноз":
        await handle_view_forecast(message)
    elif message.text == "Внести результаты" and message.from_user.id in ADMIN_IDS:
        await admin_enter_results(message, state)
    elif message.text == "Внести новые матчи" and message.from_user.id in ADMIN_IDS:
        await admin_new_matches(message, state)
    elif message.text == "Опубликовать результаты" and message.from_user.id in ADMIN_IDS:
        await admin_publish_results(message)
    elif message.text == "Удалить все таблицы" and message.from_user.id in ADMIN_IDS:
        await delete_all_tables(message, state)
    elif message.text == "Таблица АДМИН" and message.from_user.id in ADMIN_IDS:
        await handle_admin_table(message)
    else:
        await message.answer("Команда не распознана")

# 1. Мой профиль – показывает имя, позицию и очки
async def handle_my_profile(message: types.Message):
    async with db_pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM users WHERE telegram_id=$1", message.from_user.id)
        if user:
            rows = await conn.fetch("SELECT telegram_id FROM users ORDER BY points DESC")
            position = 1
            for row in rows:
                if row["telegram_id"] == message.from_user.id:
                    break
                position += 1
            await message.answer(f"{position}. {user['name']} - {user['points']} очков. {user['phone']}")
        else:
            await message.answer("Пользователь не найден. Используйте /start для регистрации.")

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
        await send_next_match(message, state)
    else:
        await message.answer("Прогноз принят, желаем удачи!")
        await state.finish()
        await send_main_menu(message)

# 3. Таблица лидеров – вывод топ-10 пользователей
async def handle_leaderboard(message: types.Message):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT name, points FROM users ORDER BY points DESC LIMIT 10")
        response = "Таблица лидеров:\n"
        rank = 1
        for row in rows:
            response += f"{rank}. {row['name']} - {row['points']} очков\n"
            rank += 1
        await message.answer(response)

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
@dp.message_handler(lambda message: message.text == "Таблица лидеров за этот месяц")
async def handle_month_leaderboard(message: types.Message):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT name, points FROM monthleaders ORDER BY points DESC LIMIT 10")
        if not rows:
            await message.answer("Месячная таблица лидеров пуста.")
            return
        response = "Топ-10 за этот месяц:\n"
        rank = 1
        for row in rows:
            response += f"{rank}. {row['name']} - {row['points']} очков\n"
            rank += 1
        await message.answer(response)


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
    result = message.text.strip()
    if result.count('-') != 1:
        await message.answer("Неверный формат. Введите результат в формате '3-4'")
        return
    parts = result.split('-')
    if not (parts[0].isdigit() and parts[1].isdigit()):
        await message.answer("Неверный формат. Введите результат в формате '3-4'")
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
                points = 0
                actual_parts = actual.split('-')
                forecast_parts = user_forecast.split('-')
                if len(actual_parts) == 2 and len(forecast_parts) == 2:
                    actual_home, actual_away = int(actual_parts[0]), int(actual_parts[1])
                    forecast_home, forecast_away = int(forecast_parts[0]), int(forecast_parts[1])
                    actual_outcome = 1 if actual_home > actual_away else (0 if actual_home == actual_away else -1)
                    forecast_outcome = 1 if forecast_home > forecast_away else (0 if forecast_home == forecast_away else -1)
                    if actual_outcome == forecast_outcome:
                        points += 1
                    if actual_home == forecast_home and actual_away == forecast_away:
                        points += 3
                await conn.execute("UPDATE users SET points = points + $1 WHERE telegram_id=$2", points, forecast["telegram_id"])
                # Обновляем очки в таблице monthleaders
                existing_ml = await conn.fetchrow("SELECT * FROM monthleaders WHERE telegram_id=$1", forecast["telegram_id"])
                if existing_ml:
                    await conn.execute("UPDATE monthleaders SET points = points + $1 WHERE telegram_id=$2", points, forecast["telegram_id"])
                else:
                    user = await conn.fetchrow("SELECT name FROM users WHERE telegram_id=$1", forecast["telegram_id"])
                    await conn.execute("INSERT INTO monthleaders (telegram_id, name, points) VALUES ($1, $2, $3)", forecast["telegram_id"], user["name"], points)
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

        # После внесения новых матчей разрешаем прогнозы
        await open_forecast(message)

async def clear_forecasts():
    """Удаляем прогнозы для текущей недели, чтобы пользователи могли ввести новые."""
    week = datetime.now().isocalendar()[1]
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM forecasts WHERE week=$1", week)

async def open_forecast(message: types.Message):
    """Разрешаем делать прогнозы после внесения новых матчей."""
    async with db_pool.acquire() as conn:
        week = datetime.now().isocalendar()[1]
        existing = await conn.fetch("SELECT * FROM forecasts WHERE telegram_id=$1 AND week=$2", message.from_user.id, week)
        if existing:
            await message.answer("Прогноз на эту неделю уже сделан, дождитесь следующей недели")
        else:
            await message.answer("Теперь вы можете сделать прогнозы на эту неделю.")

async def open_forecast(message: types.Message):
    """Разрешаем делать прогнозы после внесения новых матчей."""
    async with db_pool.acquire() as conn:
        week = datetime.now().isocalendar()[1]
        existing = await conn.fetch("SELECT * FROM forecasts WHERE telegram_id=$1 AND week=$2", message.from_user.id, week)
        if existing:
            await message.answer("Прогноз на эту неделю уже сделан, дождитесь следующей недели")
        else:
            await message.answer("Теперь вы можете сделать прогнозы на эту неделю.") 

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
async def delete_all_tables(message: types.Message, state: FSMContext):
    async with db_pool.acquire() as conn:
        # Удаляем все таблицы с зависимостями
        await conn.execute("DROP TABLE IF EXISTS forecasts CASCADE;")
        await conn.execute("DROP TABLE IF EXISTS matches CASCADE;")
        await conn.execute("DROP TABLE IF EXISTS users CASCADE;")
        await conn.execute("DROP TABLE IF EXISTS monthleaders CASCADE;")
        # Пересоздаём таблицы без данных
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                telegram_id BIGINT UNIQUE,
                name TEXT UNIQUE,
                phone TEXT UNIQUE,
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
                phone TEXT,
                points INTEGER DEFAULT 0
            );
        ''')
    await message.answer("Все таблицы удалены и созданы заново без данных!")


# 5. Таблица АДМИН – полная таблица лидеров с именем, telegramID и номером телефона
@dp.message_handler(lambda message: message.from_user.id in ADMIN_IDS and message.text == "Таблица АДМИН")
async def handle_admin_table(message: types.Message):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT telegram_id, name, phone, points FROM users ORDER BY points DESC")
        if not rows:
            await message.answer("Месячная таблица лидеров пуста.")
            return
        response = "Полная таблица лидеров:\n"
        rank = 1
        for row in rows:
            response += f"{rank}. {row['name']} - {row['points']} очков, TelegramID: {row['telegram_id']}, Телефон: {row['phone']}\n"
            rank += 1
        await message.answer(response)



# --- Запуск бота ---
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(init_db())
    from aiogram import executor
    executor.start_polling(dp, skip_updates=True)
