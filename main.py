import os
import io
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
    InlineKeyboardMarkup, InlineKeyboardButton,
    BufferedInputFile
)
from aiogram.filters import CommandStart, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TRAFIK_KEY = os.getenv("TRAFIK_KEY")
API_URL = "https://api.trafikinfo.trafikverket.se/v2/data.json"
DB_NAME = "bot_data.db"

active_messages = {}      
last_user_messages = {}   

bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()

class TicketState(StatesGroup):
    waiting_for_photo = State()
    waiting_for_date = State()

# ================= БАЗА ДАНИХ ТА ХЕЛПЕРИ =================
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, full_name TEXT, active_route_id INTEGER)")
        try: await db.execute("ALTER TABLE users ADD COLUMN active_route_id INTEGER")
        except Exception: pass
        await db.execute("CREATE TABLE IF NOT EXISTS routes (route_id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, owner_id INTEGER)")
        await db.execute("CREATE TABLE IF NOT EXISTS route_members (route_id INTEGER, user_id INTEGER, PRIMARY KEY (route_id, user_id))")
        await db.execute("CREATE TABLE IF NOT EXISTS tickets (ticket_id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, route_id INTEGER, image_data BLOB, valid_until DATE)")
        await db.execute("CREATE TABLE IF NOT EXISTS bookings (date TEXT, train_id TEXT, route_id INTEGER, user_id INTEGER, dep_ts INTEGER, dispatched INTEGER DEFAULT 0, PRIMARY KEY (date, train_id, route_id, user_id))")
        await db.execute("CREATE TABLE IF NOT EXISTS dispatches (rowid INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, chat_id INTEGER, message_id INTEGER, delete_at_ts INTEGER, date TEXT, train_id TEXT)")
        await db.commit()

async def get_user_routes(user_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(
            "SELECT r.route_id, r.name FROM route_members rm JOIN routes r ON rm.route_id = r.route_id WHERE rm.user_id = ?", 
            (user_id,)
        ) as cursor:
            return await cursor.fetchall()

# ================= ФОНОВІ РОБІТНИКИ =================
async def ticket_worker(bot_instance: Bot):
    """Фоновий процес для розсилки та видалення квитків"""
    while True:
        try:
            now_ts = int(datetime.now().timestamp())
            async with aiosqlite.connect(DB_NAME) as db:
                async with db.execute("SELECT rowid, chat_id, message_id FROM dispatches WHERE delete_at_ts <= ?", (now_ts,)) as cursor:
                    rows = await cursor.fetchall()
                    for rowid, chat_id, msg_id in rows:
                        try: await bot_instance.delete_message(chat_id, msg_id)
                        except Exception: pass 
                        await db.execute("DELETE FROM dispatches WHERE rowid = ?", (rowid,))
                
                target_ts = now_ts + (60 * 60)
                async with db.execute(
                    "SELECT date, train_id, route_id, dep_ts FROM bookings WHERE dep_ts <= ? AND dispatched = 0 GROUP BY date, train_id, route_id",
                    (target_ts,)
                ) as cursor:
                    trains_to_dispatch = await cursor.fetchall()

                for b_date, t_id, r_id, dep_ts in trains_to_dispatch:
                    async with db.execute("SELECT rowid, user_id, dispatched FROM bookings WHERE date=? AND train_id=? AND route_id=? ORDER BY rowid", (b_date, t_id, r_id)) as cur:
                        all_passengers = await cur.fetchall()
                    
                    async with db.execute("SELECT image_data FROM tickets WHERE route_id=? AND valid_until >= ? ORDER BY ticket_id", (r_id, b_date)) as cur:
                        all_tickets = [r[0] for r in await cur.fetchall()]

                    for index, (b_rowid, u_id, is_dispatched) in enumerate(all_passengers):
                        if is_dispatched == 0 and index < len(all_tickets):
                            try:
                                photo_file = BufferedInputFile(all_tickets[index], filename="ticket.jpg")
                                msg = await bot_instance.send_photo(
                                    chat_id=u_id,
                                    photo=photo_file,
                                    caption="🎫 Час рушати! Ось твій унікальний квиток.\n⏳ Він буде автоматично видалений через 60 хвилин після відправлення.",
                                    protect_content=True
                                )
                                delete_at = dep_ts + (60 * 60)
                                await db.execute(
                                    "INSERT INTO dispatches (user_id, chat_id, message_id, delete_at_ts, date, train_id) VALUES (?, ?, ?, ?, ?, ?)",
                                    (u_id, u_id, msg.message_id, delete_at, b_date, t_id)
                                )
                                await db.execute("UPDATE bookings SET dispatched = 1 WHERE rowid=?", (b_rowid,))
                            except Exception as e:
                                logging.error(f"Не вдалося надіслати квиток: {e}")

                await db.commit()
        except Exception as e:
            logging.error(f"Помилка у ticket_worker: {e}")

        await asyncio.sleep(20)

async def schedule_update_worker(bot_instance: Bot):
    """Фоновий процес для моніторингу змін у розкладі кожні 5 хвилин"""
    while True:
        await asyncio.sleep(300) # 5 хвилин
        try:
            today = datetime.now(timezone.utc).astimezone().date()
            dates_to_check = {today.isoformat(), (today + timedelta(days=1)).isoformat()}
            dates_to_check.update(date_str for date_str, _ in active_messages.keys())

            async with aiosqlite.connect(DB_NAME) as db:
                for date_str in dates_to_check:
                    target_date = datetime.fromisoformat(date_str).date()
                    loop = asyncio.get_running_loop()
                    routes_to_umea, routes_to_vns = await asyncio.gather(
                        loop.run_in_executor(None, fetch_trains, target_date, "to_umea"),
                        loop.run_in_executor(None, fetch_trains, target_date, "to_vns")
                    )

                    # Маппінг актуального часу для кожного потяга
                    train_updates = {}
                    for r in routes_to_umea + routes_to_vns:
                        train_updates[r["id"]] = int(r["dep_time"].timestamp())

                    async with db.execute("SELECT rowid, train_id, user_id, dep_ts FROM bookings WHERE date = ?", (date_str,)) as cursor:
                        bookings = await cursor.fetchall()

                    for b_rowid, t_id, u_id, old_dep_ts in bookings:
                        new_dep_ts = train_updates.get(t_id)
                        
                        # Якщо розклад змістився
                        if new_dep_ts and new_dep_ts != old_dep_ts:
                            await db.execute("UPDATE bookings SET dep_ts = ? WHERE rowid = ?", (new_dep_ts, b_rowid))
                            
                            old_time_str = datetime.fromtimestamp(old_dep_ts).astimezone().strftime("%H:%M")
                            new_time_str = datetime.fromtimestamp(new_dep_ts).astimezone().strftime("%H:%M")
                            
                            try:
                                await bot_instance.send_message(
                                    u_id,
                                    f"⚠️ **Увага! Зміни у розкладі.**\nЧас відправлення твого потяга **{t_id}** на {date_str} змінився!\n"
                                    f"Було: {old_time_str} ➡️ **Стало: {new_time_str}**",
                                    parse_mode="Markdown"
                                )
                            except Exception: pass

                    await db.commit()

                    # Оновлюємо візуальне відображення для всіх відкритих клавіатур
                    for cache_key in list(active_messages.keys()):
                        if cache_key[0] == date_str:
                            route_id = cache_key[1]
                            markup = await build_dual_schedule_keyboard(routes_to_umea, routes_to_vns, date_str, route_id)
                            dead_messages = set()
                            for chat_id, msg_id in active_messages.get(cache_key, []):
                                try:
                                    await bot_instance.edit_message_reply_markup(chat_id=chat_id, message_id=msg_id, reply_markup=markup)
                                except Exception as e:
                                    if "message is not modified" not in str(e).lower(): 
                                        dead_messages.add((chat_id, msg_id))

                            for dead in dead_messages:
                                active_messages[cache_key].discard(dead)
        except Exception as e:
            logging.error(f"Помилка в schedule_update_worker: {e}")


# ================= ЛОГІКА API Trafikverket =================
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
        [KeyboardButton(text="👥 Мої групи"), KeyboardButton(text="🎫 Мої квитки")]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

async def build_dual_schedule_keyboard(routes_to_umea, routes_to_vns, date_str, route_id):
    builder = []
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT name FROM routes WHERE route_id = ?", (route_id,)) as cursor:
            group_name = (await cursor.fetchone())[0]

        async with db.execute("SELECT COUNT(*) FROM tickets WHERE route_id = ? AND valid_until >= ?", (route_id, date_str)) as cursor:
            capacity = (await cursor.fetchone())[0]

        date_bookings = {}
        async with db.execute(
            "SELECT b.train_id, u.full_name FROM bookings b JOIN users u ON b.user_id = u.user_id WHERE b.date = ? AND b.route_id = ?", 
            (date_str, route_id)
        ) as cursor:
            async for row in cursor:
                tid, name = row
                if tid not in date_bookings: date_bookings[tid] = []
                date_bookings[tid].append(name[:5])

    builder.append([InlineKeyboardButton(text=f"🗓 {date_str} | 👥 {group_name}", callback_data="ignore")])
    builder.append([
        InlineKeyboardButton(text="Vns ➡️ Umeå", callback_data="ignore"),
        InlineKeyboardButton(text="Umeå ➡️ Vns", callback_data="ignore")
    ])
                    
    def make_btn(route_data, direction):
        if not route_data: return InlineKeyboardButton(text="—", callback_data="ignore")
        tid, dep_str = route_data["id"], route_data["dep_time"].astimezone().strftime("%H:%M")
        passengers = date_bookings.get(tid, [])
        booked_count = len(passengers)
        available = capacity - booked_count
        
        dir_char = "u" if direction == "to_umea" else "v"
        cb_data = f"bk:{route_id}:{tid}:{date_str}:{dir_char}"

        if capacity == 0: text = f"{dep_str} | 🚫 0"
        elif booked_count == 0: text = f"{dep_str} | ✅ {capacity}"
        else:
            names = ",".join(passengers)
            text = f"{dep_str} | 👤 {names} (+{available})" if available > 0 else f"{dep_str} | 🛑 {names}"
            
        return InlineKeyboardButton(text=text, callback_data=cb_data)

    for r_umea, r_vns in zip_longest(routes_to_umea, routes_to_vns):
        builder.append([make_btn(r_umea, "to_umea"), make_btn(r_vns, "to_vns")])
        
    return InlineKeyboardMarkup(inline_keyboard=builder)

# ================= ХЕЛПЕР ВИВОДУ ГРУП =================
async def send_groups_list(chat_id: int, user_id: int):
    user_routes = await get_user_routes(user_id)
    
    if not user_routes:
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➕ Створити нову групу", callback_data="create_group")]])
        return await bot.send_message(chat_id, "У тебе ще немає спільної групи для поїздок.", reply_markup=kb)

    bot_info = await bot.get_me()
    text = "👥 **Твої групи для поїздок:**\n\n"
    kb_builder = []

    for rid, rname in user_routes:
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT owner_id FROM routes WHERE route_id = ?", (rid,)) as cursor:
                owner_id = (await cursor.fetchone())[0]
            async with db.execute("SELECT u.full_name FROM route_members rm JOIN users u ON rm.user_id = u.user_id WHERE rm.route_id = ?", (rid,)) as cursor:
                members = [row[0] for row in await cursor.fetchall()]
            async with db.execute("SELECT COUNT(*) FROM tickets WHERE route_id = ? AND valid_until >= date('now')", (rid,)) as cursor:
                valid_tickets = (await cursor.fetchone())[0]

        invite_link = f"https://t.me/{bot_info.username}?start=join_{rid}"
        text += f"🔹 **{rname}**\n🎫 Дійсних квитків: {valid_tickets}\n👤 Учасники: {', '.join(members)}\n🔗 Запросити: `{invite_link}`\n\n"
        
        if owner_id == user_id:
            kb_builder.append([
                InlineKeyboardButton(text=f"⚙️ Учасники «{rname[:10]}»", callback_data=f"manage_grp:{rid}"),
                InlineKeyboardButton(text=f"🗑 Видалити «{rname[:10]}»", callback_data=f"del_grp:{rid}")
            ])
        else:
            kb_builder.append([InlineKeyboardButton(text=f"🚪 Вийти з «{rname[:15]}»", callback_data=f"leave_grp:{rid}")])

    kb_builder.append([InlineKeyboardButton(text="➕ Створити нову групу", callback_data="create_group")])
    await bot.send_message(chat_id, text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_builder))

# ================= ОБРОБНИКИ: ПРОФІЛЬ ТА ГРУПИ =================
@dp.message(CommandStart())
async def cmd_start(message: Message, command: CommandObject, state: FSMContext):
    await state.clear()
    user_id, full_name = message.from_user.id, message.from_user.first_name
    
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR IGNORE INTO users (user_id, full_name) VALUES (?, ?)", (user_id, full_name))
        
        args = command.args
        if args and args.startswith("join_"):
            route_id = int(args.split("_")[1])
            await db.execute("INSERT OR IGNORE INTO route_members (route_id, user_id) VALUES (?, ?)", (route_id, user_id))
            await db.commit()
            return await message.answer("✅ Ти успішно приєднався до групи!", reply_markup=get_main_keyboard())
            
        await db.commit()
    await message.answer(f"Привіт, {full_name}.\nОбирай дію в меню:", reply_markup=get_main_keyboard())

@dp.message(F.text == "👥 Мої групи")
async def my_groups_handler(message: Message):
    await send_groups_list(message.chat.id, message.from_user.id)

@dp.callback_query(F.data == "create_group")
async def create_group_callback(callback: CallbackQuery):
    user_id, name = callback.from_user.id, f"Група {callback.from_user.first_name}"
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("INSERT INTO routes (name, owner_id) VALUES (?, ?)", (name, user_id))
        route_id = cursor.lastrowid
        await db.execute("INSERT INTO route_members (route_id, user_id) VALUES (?, ?)", (route_id, user_id))
        await db.commit()
    
    await callback.answer("✅ Нову групу створено!")
    await callback.message.delete()
    await send_groups_list(callback.message.chat.id, callback.from_user.id)

@dp.callback_query(F.data.startswith("manage_grp:"))
async def manage_group_callback(callback: CallbackQuery):
    route_id = int(callback.data.split(":")[1])
    user_id = callback.from_user.id
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT name, owner_id FROM routes WHERE route_id = ?", (route_id,)) as cursor:
            row = await cursor.fetchone()
            if not row or row[1] != user_id:
                return await callback.answer("Тільки власник може керувати групою!", show_alert=True)
            route_name = row[0]
        
        async with db.execute("SELECT u.user_id, u.full_name FROM route_members rm JOIN users u ON rm.user_id = u.user_id WHERE rm.route_id = ? AND rm.user_id != ?", (route_id, user_id)) as cursor:
            members = await cursor.fetchall()

    if not members:
        await callback.answer("У групі більше немає інших учасників.", show_alert=True)
        if callback.message.text and "Керування учасниками" in callback.message.text:
            await callback.message.delete()
            await send_groups_list(callback.message.chat.id, user_id)
        return

    kb_builder = []
    for m_id, m_name in members:
        kb_builder.append([InlineKeyboardButton(text=f"❌ Видалити: {m_name}", callback_data=f"kick_usr:{route_id}:{m_id}")])
    
    kb_builder.append([InlineKeyboardButton(text="🔙 Назад до груп", callback_data="back_to_grps")])
    text = f"⚙️ **Керування учасниками групи «{route_name}»**\n\nОбери, кого хочеш видалити з групи:"
    
    try:
        if callback.message.text and "Керування учасниками" in callback.message.text:
            await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_builder))
        else:
            await callback.message.delete()
            await callback.message.answer(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_builder))
    except Exception: pass

@dp.callback_query(F.data.startswith("kick_usr:"))
async def kick_user_callback(callback: CallbackQuery):
    _, route_id_str, target_user_id_str = callback.data.split(":")
    route_id, target_user_id = int(route_id_str), int(target_user_id_str)
    user_id = callback.from_user.id

    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT owner_id FROM routes WHERE route_id = ?", (route_id,)) as cursor:
            row = await cursor.fetchone()
            if not row or row[0] != user_id:
                return await callback.answer("Помилка доступу!", show_alert=True)
        
        await db.execute("DELETE FROM route_members WHERE route_id = ? AND user_id = ?", (route_id, target_user_id))
        await db.execute("DELETE FROM bookings WHERE route_id = ? AND user_id = ?", (route_id, target_user_id))
        await db.execute("DELETE FROM tickets WHERE route_id = ? AND user_id = ?", (route_id, target_user_id))
        await db.commit()
        
    await callback.answer("✅ Учасника успішно видалено!")
    callback.data = f"manage_grp:{route_id}"
    await manage_group_callback(callback)

@dp.callback_query(F.data == "back_to_grps")
async def back_to_groups_callback(callback: CallbackQuery):
    await callback.message.delete()
    await send_groups_list(callback.message.chat.id, callback.from_user.id)

@dp.callback_query(F.data.startswith("del_grp:"))
async def delete_group_callback(callback: CallbackQuery):
    route_id = int(callback.data.split(":")[1])
    user_id = callback.from_user.id
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT owner_id FROM routes WHERE route_id = ?", (route_id,)) as cursor:
            row = await cursor.fetchone()
            if not row or row[0] != user_id:
                return await callback.answer("Тільки власник може видалити групу!", show_alert=True)
        
        await db.execute("DELETE FROM bookings WHERE route_id = ?", (route_id,))
        await db.execute("DELETE FROM tickets WHERE route_id = ?", (route_id,))
        await db.execute("DELETE FROM route_members WHERE route_id = ?", (route_id,))
        await db.execute("DELETE FROM routes WHERE route_id = ?", (route_id,))
        await db.commit()
        
    await callback.answer("✅ Групу повністю видалено!", show_alert=True)
    await callback.message.delete()
    await send_groups_list(callback.message.chat.id, callback.from_user.id)

@dp.callback_query(F.data.startswith("leave_grp:"))
async def leave_group_callback(callback: CallbackQuery):
    route_id = int(callback.data.split(":")[1])
    user_id = callback.from_user.id
    
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM route_members WHERE route_id = ? AND user_id = ?", (route_id, user_id))
        await db.execute("DELETE FROM bookings WHERE route_id = ? AND user_id = ?", (route_id, user_id))
        await db.commit()
        
    await callback.answer("✅ Ти вийшов з групи!", show_alert=True)
    await callback.message.delete()
    await send_groups_list(callback.message.chat.id, callback.from_user.id)

# ================= ОБРОБНИКИ: КВИТКИ =================
@dp.message(F.text == "🎫 Мої квитки")
async def my_tickets_handler(message: Message, state: FSMContext):
    user_id = message.from_user.id
    user_routes = await get_user_routes(user_id)
    
    if not user_routes:
        return await message.answer("Спершу створи групу або приєднайся до існуючої (меню «👥 Мої групи»).")

    text = "🎫 **Квитки по групах:**\n\n"
    kb_builder = []

    for rid, rname in user_routes:
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT valid_until FROM tickets WHERE route_id = ? ORDER BY valid_until DESC", (rid,)) as cursor:
                tickets = await cursor.fetchall()
        
        text += f"🔹 **{rname}**:\n"
        if tickets:
            text += "\n".join([f"  ✅ До: {t[0]}" for t in tickets]) + "\n\n"
        else:
            text += "  ❌ Немає квитків\n\n"
            
        kb_builder.append([InlineKeyboardButton(text=f"➕ Додати квиток у «{rname[:15]}»", callback_data=f"add_tkt:{rid}")])

    await message.answer(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_builder))

@dp.callback_query(F.data.startswith("add_tkt:"))
async def add_tkt_callback(callback: CallbackQuery, state: FSMContext):
    route_id = int(callback.data.split(":")[1])
    await state.update_data(route_id=route_id)
    await callback.message.answer("📸 Надішли мені **ФОТО (скріншот) QR-коду** для цієї групи:")
    await state.set_state(TicketState.waiting_for_photo)
    await callback.answer()

@dp.message(TicketState.waiting_for_photo, F.photo)
async def process_ticket_photo(message: Message, state: FSMContext):
    file_id = message.photo[-1].file_id 
    loading_msg = await message.answer("⬇️ Завантажую зображення в базу...")
    
    file = await bot.get_file(file_id)
    img_bytes = io.BytesIO()
    await bot.download_file(file.file_path, destination=img_bytes)
    
    await state.update_data(image_data=img_bytes.getvalue())
    await loading_msg.edit_text("✅ QR-код збережено.\nТепер напиши дату його закінчення у форматі РРРР-ММ-ДД (наприклад, 2026-04-15):")
    await state.set_state(TicketState.waiting_for_date)

@dp.message(TicketState.waiting_for_date)
async def process_ticket_date(message: Message, state: FSMContext):
    date_str = message.text.strip()
    try:
        valid_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        user_id = message.from_user.id
        
        data = await state.get_data()
        route_id = data.get("route_id")
        
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute(
                "INSERT INTO tickets (user_id, route_id, image_data, valid_until) VALUES (?, ?, ?, ?)", 
                (user_id, route_id, data.get("image_data"), valid_date.isoformat())
            )
            await db.commit()
            
        await message.answer(f"🎉 Квиток до {valid_date} успішно додано!", reply_markup=get_main_keyboard())
        await state.clear()
    except ValueError:
        await message.answer("❌ Неправильний формат. Напиши дату як РРРР-ММ-ДД або натисни /start для скасування.")

# ================= ОБРОБНИКИ: РОЗКЛАД ТА БРОНЮВАННЯ =================
@dp.message(F.text.in_(["Розклад на Сьогодні", "Розклад на Завтра"]))
async def handle_schedule_request(message: Message):
    user_id = message.from_user.id
    user_routes = await get_user_routes(user_id)
    
    if not user_routes: 
        return await message.answer("Спершу створи або приєднайся до групи (меню «👥 Мої групи»).")

    target_date = datetime.now().date() + timedelta(days=1 if "Завтра" in message.text else 0)
    date_str = target_date.isoformat()
    loading_msg = await message.answer("🔄 Завантажую розклад з бази Trafikverket...")
    
    loop = asyncio.get_running_loop()
    routes_to_umea, routes_to_vns = await asyncio.gather(
        loop.run_in_executor(None, fetch_trains, target_date, "to_umea"),
        loop.run_in_executor(None, fetch_trains, target_date, "to_vns")
    )
    
    chat_id = message.chat.id
    if chat_id in last_user_messages:
        for msg_id in last_user_messages[chat_id]:
            try: await bot.delete_message(chat_id, msg_id)
            except: pass
    last_user_messages[chat_id] = []
    
    await loading_msg.delete()

    if not routes_to_umea and not routes_to_vns: 
        msg = await message.answer("Не знайдено рейсів на цей день.")
        last_user_messages[chat_id].append(msg.message_id)
        return
        
    for rid, rname in user_routes:
        markup = await build_dual_schedule_keyboard(routes_to_umea, routes_to_vns, date_str, rid)
        msg = await message.answer(f"⚡ Розклад", reply_markup=markup)
        
        last_user_messages[chat_id].append(msg.message_id)
        cache_key = (date_str, rid)
        if cache_key not in active_messages: active_messages[cache_key] = set()
        active_messages[cache_key].add((msg.chat.id, msg.message_id))

@dp.callback_query(F.data == "ignore")
async def ignore_callback(callback: CallbackQuery):
    await callback.answer()

@dp.callback_query(F.data.startswith("bk:"))
async def process_booking(callback: CallbackQuery):
    _, route_id_str, train_id, date_str, dir_char = callback.data.split(":")
    user_id = callback.from_user.id
    route_id = int(route_id_str)
    direction = "to_umea" if dir_char == "u" else "to_vns"
    
    target_date = datetime.fromisoformat(date_str).date()
    loop = asyncio.get_running_loop()
    routes_to_umea, routes_to_vns = await asyncio.gather(
        loop.run_in_executor(None, fetch_trains, target_date, "to_umea"),
        loop.run_in_executor(None, fetch_trains, target_date, "to_vns")
    )
    
    relevant_routes = routes_to_vns if direction == "to_vns" else routes_to_umea
    train_data = next((r for r in relevant_routes if r["id"] == train_id), None)
    if not train_data: return await callback.answer("Помилка: рейс не знайдено.", show_alert=True)
    
    dep_ts = int(train_data["dep_time"].timestamp())
    now_ts = int(datetime.now().timestamp())

    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT COUNT(*) FROM tickets WHERE route_id = ? AND valid_until >= ?", (route_id, date_str)) as cursor:
            capacity = (await cursor.fetchone())[0]

        if capacity == 0: return await callback.answer("На цю дату немає дійсних квитків у цій групі!", show_alert=True)

        async with db.execute("SELECT user_id FROM bookings WHERE date = ? AND train_id = ? AND route_id = ? ORDER BY rowid", (date_str, train_id, route_id)) as cursor:
            current_bookings = [row[0] for row in await cursor.fetchall()]

        if user_id in current_bookings:
            await db.execute("DELETE FROM bookings WHERE date = ? AND train_id = ? AND user_id = ? AND route_id = ?", (date_str, train_id, user_id, route_id))
            async with db.execute("SELECT rowid, chat_id, message_id FROM dispatches WHERE user_id=? AND date=? AND train_id=?", (user_id, date_str, train_id)) as cur:
                dispatched_msgs = await cur.fetchall()
                for r in dispatched_msgs:
                    try: await bot.delete_message(r[1], r[2])
                    except: pass
                    await db.execute("DELETE FROM dispatches WHERE rowid=?", (r[0],))
            await db.commit()
            await callback.answer("Бронювання скасовано. Квиток відкликано з чату.")
        
        elif len(current_bookings) >= capacity:
            return await callback.answer("Всі місця вже зайняті!", show_alert=True)
        
        else:
            is_urgent = (dep_ts - now_ts) <= 3600
            dispatched_flag = 1 if is_urgent else 0
            
            await db.execute(
                "INSERT INTO bookings (date, train_id, route_id, user_id, dep_ts, dispatched) VALUES (?, ?, ?, ?, ?, ?)", 
                (date_str, train_id, route_id, user_id, dep_ts, dispatched_flag)
            )

            if is_urgent:
                async with db.execute("SELECT user_id FROM bookings WHERE date=? AND train_id=? AND route_id=? ORDER BY rowid", (date_str, train_id, route_id)) as cur:
                    all_booked = [r[0] for r in await cur.fetchall()]
                user_index = all_booked.index(user_id)
                
                async with db.execute("SELECT image_data FROM tickets WHERE route_id=? AND valid_until >= ? ORDER BY ticket_id", (route_id, date_str)) as cur:
                    all_tickets = [r[0] for r in await cur.fetchall()]
                
                if user_index < len(all_tickets):
                    try:
                        photo_file = BufferedInputFile(all_tickets[user_index], filename="ticket.jpg")
                        msg = await bot.send_photo(
                            chat_id=user_id,
                            photo=photo_file,
                            caption="🎫 Рейс менш ніж за годину! Ось твій квиток.\n⏳ Він зникне через 60 хвилин після відправлення.",
                            protect_content=True
                        )
                        delete_at = dep_ts + (60 * 60)
                        await db.execute(
                            "INSERT INTO dispatches (user_id, chat_id, message_id, delete_at_ts, date, train_id) VALUES (?, ?, ?, ?, ?, ?)",
                            (user_id, user_id, msg.message_id, delete_at, date_str, train_id)
                        )
                    except Exception: pass
            
            await db.commit()
            if is_urgent:
                await callback.answer("Закріплено! Квиток вже відправлено в чат.")
            else:
                await callback.answer("Закріплено! Квиток прийде за 60 хв до відправлення.")

    markup = await build_dual_schedule_keyboard(routes_to_umea, routes_to_vns, date_str, route_id)
    
    dead_messages = set()
    cache_key = (date_str, route_id)
    for chat_id, msg_id in active_messages.get(cache_key, []):
        try: await bot.edit_message_reply_markup(chat_id=chat_id, message_id=msg_id, reply_markup=markup)
        except Exception as e:
            if "message is not modified" not in str(e).lower(): dead_messages.add((chat_id, msg_id))

    for dead in dead_messages: active_messages[cache_key].discard(dead)

async def main():
    logging.basicConfig(level=logging.INFO)
    await init_db()
    asyncio.create_task(ticket_worker(bot))
    asyncio.create_task(schedule_update_worker(bot))
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())