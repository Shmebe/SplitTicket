import os
import asyncio
import logging
import requests
import aiosqlite
from datetime import datetime, timedelta, timezone, time as dt_time
from itertools import zip_longest
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery, 
    ReplyKeyboardMarkup, KeyboardButton, 
    InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

# Завантаження змінних
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TRAFIK_KEY = os.getenv("TRAFIK_KEY")
API_URL = "https://api.trafikinfo.trafikverket.se/v2/data.json"
DB_NAME = "bot_data.db"

# ================= СТАН БОТА (Пам'ять сесії) =================
active_messages = {}      # date_str -> set of (chat_id, message_id)
last_user_messages = {}   # chat_id -> message_id

bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()

# Машина станів для додавання квитка
class TicketState(StatesGroup):
    waiting_for_date = State()

# ================= БАЗА ДАНИХ =================
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, full_name TEXT)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS routes (route_id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, owner_id INTEGER)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS route_members (route_id INTEGER, user_id INTEGER, PRIMARY KEY (route_id, user_id))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS tickets (ticket_id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, route_id INTEGER, valid_until DATE)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS bookings (date TEXT, train_id TEXT, route_id INTEGER, user_id INTEGER, PRIMARY KEY (date, train_id, route_id, user_id))""")
        
        # Дефолтний маршрут
        await db.execute("INSERT OR IGNORE INTO routes (route_id, name, owner_id) VALUES (1, 'Загальний', 0)")
        await db.commit()

# ================= ЛОГІКА API =================
def fetch_trains(target_date: datetime.date, direction: str):
    start_dt = datetime.combine(target_date, dt_time.min).astimezone(timezone.utc).isoformat()
    end_dt = datetime.combine(target_date, dt_time.max).astimezone(timezone.utc).isoformat()
    xml = f"""<REQUEST>
      <LOGIN authenticationkey="{TRAFIK_KEY}" />
      <QUERY objecttype="TrainAnnouncement" schemaversion="1.9">
        <FILTER>
          <AND>
            <IN name="LocationSignature" value="Uåö, U, Vns" />
            <GT name="AdvertisedTimeAtLocation" value="{start_dt}" />
            <LT name="AdvertisedTimeAtLocation" value="{end_dt}" />
          </AND>
        </FILTER>
        <INCLUDE>AdvertisedTrainIdent</INCLUDE><INCLUDE>LocationSignature</INCLUDE>
        <INCLUDE>ActivityType</INCLUDE><INCLUDE>AdvertisedTimeAtLocation</INCLUDE><INCLUDE>Canceled</INCLUDE>
      </QUERY>
    </REQUEST>"""
    resp = requests.post(API_URL, data=xml.encode("utf-8"), headers={"Content-Type": "text/xml"})
    if resp.status_code != 200: return []
    data = resp.json().get("RESPONSE", {}).get("RESULT", [{}])[0].get("TrainAnnouncement", [])
    
    train_map = {}
    for event in data:
        tid, loc, act = event.get("AdvertisedTrainIdent"), event.get("LocationSignature"), event.get("ActivityType")
        if not tid or event.get("Canceled"): continue
        if tid not in train_map: train_map[tid] = {}
        if loc not in train_map[tid]: train_map[tid][loc] = {}
        train_map[tid][loc][act] = datetime.fromisoformat(event.get("AdvertisedTimeAtLocation"))

    valid_routes = []
    for tid, stations in train_map.items():
        try:
            if direction == "to_vns":
                dep_station = "U" if "U" in stations and "Avgang" in stations["U"] else "Uåö"
                dep_time, arr_time = stations[dep_station]["Avgang"], stations["Vns"]["Ankomst"]
                if dep_time < arr_time: valid_routes.append({"id": tid, "dep_time": dep_time})
            elif direction == "to_umea":
                dep_time = stations["Vns"]["Avgang"]
                arr_station = "U" if "U" in stations and "Ankomst" in stations["U"] else "Uåö"
                arr_time = stations[arr_station]["Ankomst"]
                if dep_time < arr_time: valid_routes.append({"id": tid, "dep_time": dep_time})
        except KeyError: continue

    valid_routes.sort(key=lambda x: x["dep_time"])
    return valid_routes

# ================= КЛАВІАТУРИ =================
def get_main_keyboard():
    kb = [
        [KeyboardButton(text="Розклад на Сьогодні"), KeyboardButton(text="Розклад на Завтра")],
        [KeyboardButton(text="🎫 Мої квитки")]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

async def build_dual_schedule_keyboard(routes_to_umea, routes_to_vns, date_str):
    builder = [[
        InlineKeyboardButton(text="Vns ➡️ Umeå", callback_data="ignore"),
        InlineKeyboardButton(text="Umeå ➡️ Vns", callback_data="ignore")
    ]]
    
    async with aiosqlite.connect(DB_NAME) as db:
        # 1. Рахуємо місткість (кількість валідних квитків на цю дату)
        async with db.execute("SELECT COUNT(*) FROM tickets WHERE route_id = 1 AND valid_until >= ?", (date_str,)) as cursor:
            capacity = (await cursor.fetchone())[0]

        # 2. Витягуємо списки пасажирів на кожен рейс
        date_bookings = {}
        async with db.execute(
            "SELECT b.train_id, u.full_name FROM bookings b JOIN users u ON b.user_id = u.user_id WHERE b.date = ?", (date_str,)
        ) as cursor:
            async for row in cursor:
                tid, name = row
                if tid not in date_bookings: date_bookings[tid] = []
                date_bookings[tid].append(name[:5]) # Скорочуємо ім'я для економії місця
                    
    def make_btn(route_data, direction):
        if not route_data: return InlineKeyboardButton(text="—", callback_data="ignore")
        tid, dep_str = route_data["id"], route_data["dep_time"].astimezone().strftime("%H:%M")
        passengers = date_bookings.get(tid, [])
        booked_count = len(passengers)
        available = capacity - booked_count
        
        if capacity == 0:
            text = f"{dep_str} | 🚫 0 квитків"
        elif booked_count == 0:
            text = f"{dep_str} | ✅ Вільно: {capacity}"
        else:
            names = ",".join(passengers)
            text = f"{dep_str} | 👤 {names} (+{available})" if available > 0 else f"{dep_str} | 🛑 {names}"
            
        return InlineKeyboardButton(text=text, callback_data=f"book:{tid}:{date_str}:{direction}")

    for r_umea, r_vns in zip_longest(routes_to_umea, routes_to_vns):
        builder.append([make_btn(r_umea, "to_umea"), make_btn(r_vns, "to_vns")])
        
    return InlineKeyboardMarkup(inline_keyboard=builder)

# ================= ОБРОБНИКИ: КВИТКИ ТА ПРОФІЛЬ =================
@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    user_id, full_name = message.from_user.id, message.from_user.first_name
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR IGNORE INTO users (user_id, full_name) VALUES (?, ?)", (user_id, full_name))
        await db.commit()
    await message.answer(f"Привіт, {full_name}.\nОбирай дію в меню:", reply_markup=get_main_keyboard())

@dp.message(F.text == "🎫 Мої квитки")
async def my_tickets_handler(message: Message, state: FSMContext):
    user_id = message.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT valid_until FROM tickets WHERE user_id = ? ORDER BY valid_until DESC", (user_id,)) as cursor:
            tickets = await cursor.fetchall()
            
    if tickets:
        tickets_str = "\n".join([f"✅ Дійсний до: {t[0]}" for t in tickets])
        text = f"Твої квитки:\n{tickets_str}\n\nЩоб додати новий квиток, напиши дату його закінчення у форматі РРРР-ММ-ДД (наприклад, 2026-04-15):"
    else:
        text = "У тебе ще немає доданих квитків.\nНапиши дату закінчення квитка у форматі РРРР-ММ-ДД (наприклад, 2026-04-15):"
        
    await message.answer(text)
    await state.set_state(TicketState.waiting_for_date)

@dp.message(TicketState.waiting_for_date)
async def process_ticket_date(message: Message, state: FSMContext):
    date_str = message.text.strip()
    try:
        valid_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        user_id = message.from_user.id
        
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("INSERT INTO tickets (user_id, route_id, valid_until) VALUES (?, ?, ?)", (user_id, 1, valid_date.isoformat()))
            await db.commit()
            
        await message.answer(f"🎉 Квиток до {valid_date} успішно додано!", reply_markup=get_main_keyboard())
        await state.clear()
    except ValueError:
        await message.answer("❌ Неправильний формат. Напиши дату як РРРР-ММ-ДД (наприклад, 2026-04-15) або натисни /start для скасування.")

# ================= ОБРОБНИКИ: РОЗКЛАД =================
@dp.message(F.text.in_(["Розклад на Сьогодні", "Розклад на Завтра"]))
async def handle_schedule_request(message: Message):
    target_date = datetime.now().date() + timedelta(days=1 if "Завтра" in message.text else 0)
    date_str = target_date.isoformat()
    loading_msg = await message.answer("🔄 Завантажую розклад...")
    
    loop = asyncio.get_running_loop()
    routes_to_umea, routes_to_vns = await asyncio.gather(
        loop.run_in_executor(None, fetch_trains, target_date, "to_umea"),
        loop.run_in_executor(None, fetch_trains, target_date, "to_vns")
    )
    
    if not routes_to_umea and not routes_to_vns:
        await loading_msg.edit_text("Не знайдено рейсів на цей день.")
        return
        
    markup = await build_dual_schedule_keyboard(routes_to_umea, routes_to_vns, date_str)
    chat_id = message.chat.id
    if chat_id in last_user_messages:
        try: await bot.delete_message(chat_id, last_user_messages[chat_id])
        except: pass 
            
    await loading_msg.delete()
    msg = await message.answer(f"🗓 Розклад на {date_str}", reply_markup=markup)
    last_user_messages[chat_id] = msg.message_id

    if date_str not in active_messages: active_messages[date_str] = set()
    active_messages[date_str].add((msg.chat.id, msg.message_id))

@dp.callback_query(F.data == "ignore")
async def ignore_callback(callback: CallbackQuery):
    await callback.answer()

@dp.callback_query(F.data.startswith("book:"))
async def process_booking(callback: CallbackQuery):
    _, train_id, date_str, direction = callback.data.split(":")
    user_id = callback.from_user.id
    
    async with aiosqlite.connect(DB_NAME) as db:
        # Перевіряємо ліміт квитків
        async with db.execute("SELECT COUNT(*) FROM tickets WHERE route_id = 1 AND valid_until >= ?", (date_str,)) as cursor:
            capacity = (await cursor.fetchone())[0]

        if capacity == 0:
            return await callback.answer("На цю дату немає дійсних квитків у групі!", show_alert=True)

        # Перевіряємо поточні бронювання
        async with db.execute("SELECT user_id FROM bookings WHERE date = ? AND train_id = ?", (date_str, train_id)) as cursor:
            current_bookings = [row[0] for row in await cursor.fetchall()]

        if user_id in current_bookings:
            await db.execute("DELETE FROM bookings WHERE date = ? AND train_id = ? AND user_id = ?", (date_str, train_id, user_id))
            await db.commit()
            await callback.answer("Бронювання скасовано.")
        elif len(current_bookings) >= capacity:
            return await callback.answer("Всі місця на цей час вже зайняті!", show_alert=True)
        else:
            await db.execute("INSERT INTO bookings (date, train_id, route_id, user_id) VALUES (?, ?, ?, ?)", (date_str, train_id, 1, user_id))
            await db.commit()
            await callback.answer("Успішно закріплено!")

    # Оновлення UI
    target_date = datetime.fromisoformat(date_str).date()
    loop = asyncio.get_running_loop()
    routes_to_umea, routes_to_vns = await asyncio.gather(
        loop.run_in_executor(None, fetch_trains, target_date, "to_umea"),
        loop.run_in_executor(None, fetch_trains, target_date, "to_vns")
    )
    markup = await build_dual_schedule_keyboard(routes_to_umea, routes_to_vns, date_str)
    
    dead_messages = set()
    for chat_id, msg_id in active_messages.get(date_str, []):
        try: await bot.edit_message_reply_markup(chat_id=chat_id, message_id=msg_id, reply_markup=markup)
        except Exception as e:
            if "message is not modified" not in str(e).lower(): dead_messages.add((chat_id, msg_id))

    for dead in dead_messages: active_messages[date_str].discard(dead)

async def main():
    logging.basicConfig(level=logging.INFO)
    await init_db()
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())