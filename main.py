import asyncio
import logging
import os
from datetime import datetime, timedelta
import pytz

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Bot
from telegram.error import TelegramError

from getcourse import fetch_payments
from analytics import build_excel, build_analytics_text

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BOT_TOKEN = os.environ["BOT_TOKEN"]
CHAT_ID = os.environ["CHAT_ID"]
MOSCOW_TZ = pytz.timezone("Europe/Moscow")

async def daily_job():
    bot = Bot(token=BOT_TOKEN)
    yesterday = (datetime.now(MOSCOW_TZ) - timedelta(days=1)).date()
    date_str = yesterday.strftime("%d.%m")
    filename = f"оплаты {date_str} вб2.xlsx"
    logger.info(f"Запуск выгрузки за {date_str}")
    try:
        df = await fetch_payments(yesterday)
    except Exception as e:
        logger.error(f"Ошибка при получении данных из GetCourse: {e}")
        await bot.send_message(chat_id=CHAT_ID, text=f"❌ Ошибка выгрузки из GetCourse:\n{e}")
        return
    if df is None or df.empty:
        await bot.send_message(chat_id=CHAT_ID, text=f"⚠️ Нет данных об оплатах за {date_str}")
        return
    excel_bytes = build_excel(df, filename)
    analytics_text = build_analytics_text(df, date_str)
    try:
        await bot.send_document(chat_id=CHAT_ID, document=excel_bytes, filename=filename, caption=f"📁 {filename}")
        await bot.send_message(chat_id=CHAT_ID, text=analytics_text, parse_mode="HTML")
        logger.info("Отправка завершена успешно")
    except TelegramError as e:
        logger.error(f"Ошибка отправки в Telegram: {e}")

async def main():
    scheduler = AsyncIOScheduler(timezone=MOSCOW_TZ)
    scheduler.add_job(daily_job, "cron", hour=9, minute=0)
    scheduler.start()
    logger.info("Бот запущен. Следующая выгрузка в 09:00 МСК.")
    if os.environ.get("RUN_NOW") == "1":
        logger.info("RUN_NOW=1 — запускаю выгрузку немедленно")
        await daily_job()
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
