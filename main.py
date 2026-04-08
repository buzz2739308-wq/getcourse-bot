import asyncio
import logging
import os
from datetime import datetime, timedelta
import pytz

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Bot
from telegram.error import TelegramError

from getcourse import fetch_payments, build_dataframe
from analytics import build_excel, build_analytics_text
from wednesday import (
    get_wednesday_dates, fetch_users_by_group,
    fetch_deals_wednesday, analytics_users,
    analytics_deals_wednesday
)

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
    logger.info(f"Ежедневная выгрузка за {date_str}")
    try:
        df = await fetch_payments(yesterday)
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await bot.send_message(chat_id=CHAT_ID, text=f"❌ Ошибка выгрузки оплат:\n{e}")
        return
    if df is None or df.empty:
        await bot.send_message(chat_id=CHAT_ID, text=f"⚠️ Нет оплат за {date_str}")
        return
    excel_bytes = build_excel(df, filename)
    analytics_text = build_analytics_text(df, date_str)
    await bot.send_document(chat_id=CHAT_ID, document=excel_bytes, filename=filename, caption=f"📁 {filename}")
    await bot.send_message(chat_id=CHAT_ID, text=analytics_text, parse_mode="HTML")
    logger.info("Ежедневная выгрузка отправлена")


async def wednesday_job():
    bot = Bot(token=BOT_TOKEN)
    today = datetime.now(MOSCOW_TZ).date()
    dates = get_wednesday_dates(today)
    mon = dates["mon"]
    wed = dates["wed"]
    label = f"{mon.strftime('%d')}-{wed.strftime('%d.%m')}"
    logger.info(f"Среда: выгрузки за {label}")

    # 1. Регистрации
    try:
        await bot.send_message(chat_id=CHAT_ID, text=f"⏳ Выгружаю регистрации {label}...")
        df_reg = await fetch_users_by_group(dates["reg_group_name"])
        filename = f"регистрации {label} вб2.xlsx"
        excel = build_excel(df_reg, filename)
        text = analytics_users(df_reg, label, "Регистрации")
        await bot.send_document(chat_id=CHAT_ID, document=excel, filename=filename, caption=f"📁 {filename}")
        await bot.send_message(chat_id=CHAT_ID, text=text, parse_mode="HTML")
        logger.info("Регистрации отправлены")
    except Exception as e:
        logger.error(f"Ошибка регистраций: {e}")
        await bot.send_message(chat_id=CHAT_ID, text=f"❌ Ошибка регистраций:\n{e}")

    await asyncio.sleep(60)

    # 2. Входы
    try:
        await bot.send_message(chat_id=CHAT_ID, text=f"⏳ Выгружаю входы {mon.strftime('%d.%m')}...")
        df_entry = await fetch_users_by_group(dates["entry_group_name"])
        filename = f"входы {mon.strftime('%d.%m')} вб2.xlsx"
        excel = build_excel(df_entry, filename)
        await bot.send_document(chat_id=CHAT_ID, document=excel, filename=filename, caption=f"📁 {filename}")
        logger.info("Входы отправлены")
    except Exception as e:
        logger.error(f"Ошибка входов: {e}")
        await bot.send_message(chat_id=CHAT_ID, text=f"❌ Ошибка входов:\n{e}")

    await asyncio.sleep(60)

    # 3. Записи
    try:
        await bot.send_message(chat_id=CHAT_ID, text=f"⏳ Выгружаю записи {mon.strftime('%d.%m')}...")
        df_views = await fetch_users_by_group(
            dates["views_group_name"],
            date_from=dates["views_date_from"],
            date_to=dates["views_date_to"]
        )
        filename = f"записи {mon.strftime('%d.%m')} вб2.xlsx"
        excel = build_excel(df_views, filename)

        # Аналитика входы + записи вместе
        total_entry = len(df_entry) if 'df_entry' in dir() else 0
        total_views = len(df_views)
        entry_label = mon.strftime('%d.%m')
        text = (
            f"📊 <b>Входы + Записи {entry_label}:</b>\n"
            f"Входов: <b>{total_entry}</b>\n"
            f"Записей: <b>{total_views}</b>\n"
            f"Итого: <b>{total_entry + total_views}</b>\n\n"
            f"🎯 <b>Входы по источникам:</b>\n"
        )
        if 'df_entry' in dir() and not df_entry.empty:
            if "utm_source" in df_entry.columns and "utm_medium" in df_entry.columns:
                grouped = (
                    df_entry.assign(
                        source=df_entry["utm_source"].fillna("—").replace("", "—"),
                        medium=df_entry["utm_medium"].fillna("—").replace("", "—")
                    )
                    .groupby(["source", "medium"]).size()
                    .reset_index(name="count")
                    .sort_values("count", ascending=False)
                )
                for _, row in grouped.iterrows():
                    text += f"— {row['source']} / {row['medium']}: {row['count']}\n"
        text += "\n🎯 <b>Записи по источникам:</b>\n"
        if not df_views.empty:
            if "utm_source" in df_views.columns and "utm_medium" in df_views.columns:
                grouped = (
                    df_views.assign(
                        source=df_views["utm_source"].fillna("—").replace("", "—"),
                        medium=df_views["utm_medium"].fillna("—").replace("", "—")
                    )
                    .groupby(["source", "medium"]).size()
                    .reset_index(name="count")
                    .sort_values("count", ascending=False)
                )
                for _, row in grouped.iterrows():
                    text += f"— {row['source']} / {row['medium']}: {row['count']}\n"

        await bot.send_document(chat_id=CHAT_ID, document=excel, filename=filename, caption=f"📁 {filename}")
        await bot.send_message(chat_id=CHAT_ID, text=text, parse_mode="HTML")
        logger.info("Записи отправлены")
    except Exception as e:
        logger.error(f"Ошибка записей: {e}")
        await bot.send_message(chat_id=CHAT_ID, text=f"❌ Ошибка записей:\n{e}")

    await asyncio.sleep(60)

    # 4. Заказы прошлый трёхдневник
    try:
        await bot.send_message(chat_id=CHAT_ID, text=f"⏳ Выгружаю заказы {dates['deals1_label']}...")
        df_deals1 = await fetch_deals_wednesday(dates["deals1_from"], dates["deals1_to"])
        filename = f"заказы {dates['deals1_label']} вб2.xlsx"
        excel = build_excel(df_deals1, filename)
        text = analytics_deals_wednesday(df_deals1, dates["deals1_label"])
        await bot.send_document(chat_id=CHAT_ID, document=excel, filename=filename, caption=f"📁 {filename}")
        await bot.send_message(chat_id=CHAT_ID, text=text, parse_mode="HTML")
        logger.info("Заказы 1 отправлены")
    except Exception as e:
        logger.error(f"Ошибка заказов 1: {e}")
        await bot.send_message(chat_id=CHAT_ID, text=f"❌ Ошибка заказов {dates['deals1_label']}:\n{e}")

    await asyncio.sleep(60)

    # 5. Заказы позапрошлый трёхдневник
    try:
        await bot.send_message(chat_id=CHAT_ID, text=f"⏳ Выгружаю заказы {dates['deals2_label']}...")
        df_deals2 = await fetch_deals_wednesday(dates["deals2_from"], dates["deals2_to"])
        filename = f"заказы {dates['deals2_label']} вб2.xlsx"
        excel = build_excel(df_deals2, filename)
        text = analytics_deals_wednesday(df_deals2, dates["deals2_label"])
        await bot.send_document(chat_id=CHAT_ID, document=excel, filename=filename, caption=f"📁 {filename}")
        await bot.send_message(chat_id=CHAT_ID, text=text, parse_mode="HTML")
        logger.info("Заказы 2 отправлены")
    except Exception as e:
        logger.error(f"Ошибка заказов 2: {e}")
        await bot.send_message(chat_id=CHAT_ID, text=f"❌ Ошибка заказов {dates['deals2_label']}:\n{e}")

    logger.info("Все среда выгрузки завершены")


async def main():
    scheduler = AsyncIOScheduler(timezone=MOSCOW_TZ)
    scheduler.add_job(daily_job, "cron", hour=9, minute=0)
    scheduler.add_job(wednesday_job, "cron", day_of_week="wed", hour=9, minute=5)
    scheduler.start()
    logger.info("Бот запущен. Ежедневно 09:00, по средам доп. выгрузки в 09:05 МСК.")

    if os.environ.get("RUN_NOW") == "1":
        await daily_job()
    if os.environ.get("RUN_WEDNESDAY") == "1":
        await wednesday_job()

    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
