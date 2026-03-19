import os
import asyncio
import logging
import requests
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

# Завантаження змінних
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TRAFIK_KEY = os.getenv("TRAFIK_KEY")
API_URL = "https://api.trafikinfo.trafikverket.se/v2/data.json"

# ================= СТАН БОТА =================
bookings = {}             # date -> train_id -> user_name
active_messages = {}      # date_str -> set of (chat_id, message_id)
last_user_messages = {}   # chat_id -> message_id

bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()

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
        <INCLUDE>AdvertisedTrainIdent</INCLUDE>
        <INCLUDE>LocationSignature</INCLUDE>
        <INCLUDE>ActivityType</INCLUDE>
        <INCLUDE>AdvertisedTimeAtLocation</INCLUDE>
        <INCLUDE>Canceled</INCLUDE>
      </QUERY>
    </REQUEST>"""

    resp = requests.post(API_URL, data=xml.encode("utf-8"), headers={"Content-Type": "text/xml"})
    if resp.status_code != 200:
        return []

    data = resp.json().get("RESPONSE", {}).get("RESULT", [{}])[0].get("TrainAnnouncement", [])
    
    train_map = {}
    for event in data:
        tid = event.get("AdvertisedTrainIdent")
        if not tid or event.get("Canceled"):
            continue
            
        if tid not in train_map:
            train_map[tid] = {}
            
        loc = event.get("LocationSignature")
        act = event.get("ActivityType")
        time_str = event.get("AdvertisedTimeAtLocation")
        
        if loc not in train_map[tid]:
            train_map[tid][loc] = {}
        train_map[tid][loc][act] = datetime.fromisoformat(time_str)

    valid_routes = []
    for tid, stations in train_map.items():
        try:
            if direction == "to_vns":
                dep_station = "U" if "U" in stations and "Avgang" in stations["U"] else "Uåö"
                if dep_station not in stations or "Avgang" not in stations[dep_station]: continue
                dep_time = stations[dep_station]["Avgang"]
                
                if "Vns" not in stations or "Ankomst" not in stations["Vns"]: continue
                arr_time = stations["Vns"]["Ankomst"]

                if dep_time < arr_time:
                    valid_routes.append({"id": tid, "dep_time": dep_time})

            elif direction == "to_umea":
                if "Vns" not in stations or "Avgang" not in stations["Vns"]: continue
                dep_time = stations["Vns"]["Avgang"]
                
                arr_station = "U" if "U" in stations and "Ankomst" in stations["U"] else "Uåö"
                if arr_station not in stations or "Ankomst" not in stations[arr_station]: continue
                arr_time = stations[arr_station]["Ankomst"]

                if dep_time < arr_time:
                    valid_routes.append({"id": tid, "dep_time": dep_time})
        except KeyError:
            continue

    valid_routes.sort(key=lambda x: x["dep_time"])
    return valid_routes

# ================= КЛАВІАТУРИ =================
def get_main_keyboard():
    kb = [[KeyboardButton(text="Розклад на Сьогодні"), KeyboardButton(text="Розклад на Завтра")]]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

def build_dual_schedule_keyboard(routes_to_umea, routes_to_vns, date_str):
    builder = []
    
    # Заголовки (щоб було зрозуміло, де яка колонка)
    builder.append([
        InlineKeyboardButton(text="Vns ➡️ Umeå", callback_data="ignore"),
        InlineKeyboardButton(text="Umeå ➡️ Vns", callback_data="ignore")
    ])
    
    date_bookings = bookings.get(date_str, {})

    for r_umea, r_vns in zip_longest(routes_to_umea, routes_to_vns):
        row = []
        
        # Ліва колонка: Vännäs -> Umeå
        if r_umea:
            dep_str = r_umea["dep_time"].astimezone().strftime("%H:%M")
            passenger = date_bookings.get(r_umea["id"])
            text = f"{dep_str} | 👤 {passenger[:6]}" if passenger else f"{dep_str} | ✅"
            row.append(InlineKeyboardButton(text=text, callback_data=f"book:{r_umea['id']}:{date_str}:to_umea"))
        else:
            row.append(InlineKeyboardButton(text="—", callback_data="ignore"))

        # Права колонка: Umeå -> Vännäs
        if r_vns:
            dep_str = r_vns["dep_time"].astimezone().strftime("%H:%M")
            passenger = date_bookings.get(r_vns["id"])
            text = f"{dep_str} | 👤 {passenger[:6]}" if passenger else f"{dep_str} | ✅"
            row.append(InlineKeyboardButton(text=text, callback_data=f"book:{r_vns['id']}:{date_str}:to_vns"))
        else:
            row.append(InlineKeyboardButton(text="—", callback_data="ignore"))

        builder.append(row)
        
    return InlineKeyboardMarkup(inline_keyboard=builder)

# ================= ОБРОБНИКИ =================
@dp.message(CommandStart())
async def cmd_start(message: Message):
    await message.answer("Обирай день:", reply_markup=get_main_keyboard())

@dp.message(F.text.in_(["Розклад на Сьогодні", "Розклад на Завтра"]))
async def handle_schedule_request(message: Message):
    target_date = datetime.now().date()
    if "Завтра" in message.text:
        target_date += timedelta(days=1)
        
    date_str = target_date.isoformat()
    loading_msg = await message.answer("🔄 Завантажую розклад...")
    
    loop = asyncio.get_running_loop()
    task_umea = loop.run_in_executor(None, fetch_trains, target_date, "to_umea")
    task_vns = loop.run_in_executor(None, fetch_trains, target_date, "to_vns")
    
    routes_to_umea, routes_to_vns = await asyncio.gather(task_umea, task_vns)
    
    if not routes_to_umea and not routes_to_vns:
        await loading_msg.edit_text("Не знайдено рейсів на цей день.")
        return
        
    markup = build_dual_schedule_keyboard(routes_to_umea, routes_to_vns, date_str)
    
    # Видаляємо старе повідомлення, щоб не засмічувати чат
    chat_id = message.chat.id
    if chat_id in last_user_messages:
        try:
            await bot.delete_message(chat_id, last_user_messages[chat_id])
        except Exception:
            pass 
            
    await loading_msg.delete()
    msg = await message.answer(f"🗓 Розклад на {date_str}", reply_markup=markup)
    
    last_user_messages[chat_id] = msg.message_id

    # Реєструємо для масових оновлень
    if date_str not in active_messages:
        active_messages[date_str] = set()
    active_messages[date_str].add((msg.chat.id, msg.message_id))

@dp.callback_query(F.data == "ignore")
async def ignore_callback(callback: CallbackQuery):
    await callback.answer()

@dp.callback_query(F.data.startswith("book:"))
async def process_booking(callback: CallbackQuery):
    _, train_id, date_str, direction = callback.data.split(":")
    user_name = callback.from_user.first_name
    
    if date_str not in bookings:
        bookings[date_str] = {}
        
    current_passenger = bookings[date_str].get(train_id)
    
    if current_passenger == user_name:
        del bookings[date_str][train_id]
        await callback.answer("Бронювання скасовано.")
    elif current_passenger:
        await callback.answer(f"Зайнято: {current_passenger}", show_alert=True)
        return
    else:
        bookings[date_str][train_id] = user_name
        await callback.answer("Успішно закріплено!")
        
    # Оновлюємо розклад для обох напрямків, щоб перемалювати дві колонки
    target_date = datetime.fromisoformat(date_str).date()
    loop = asyncio.get_running_loop()
    task_umea = loop.run_in_executor(None, fetch_trains, target_date, "to_umea")
    task_vns = loop.run_in_executor(None, fetch_trains, target_date, "to_vns")
    routes_to_umea, routes_to_vns = await asyncio.gather(task_umea, task_vns)
    
    markup = build_dual_schedule_keyboard(routes_to_umea, routes_to_vns, date_str)
    
    # Синхронно оновлюємо ВСІ повідомлення за цю дату
    dead_messages = set()
    for chat_id, msg_id in active_messages.get(date_str, []):
        try:
            await bot.edit_message_reply_markup(chat_id=chat_id, message_id=msg_id, reply_markup=markup)
        except Exception as e:
            if "message is not modified" not in str(e).lower():
                dead_messages.add((chat_id, msg_id))

    for dead in dead_messages:
        active_messages[date_str].discard(dead)

async def main():
    logging.basicConfig(level=logging.INFO)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())