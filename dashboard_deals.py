"""Обновление колонки Заказы в Google Sheets дашборде.

Запуск: ежедневно в 00:01 МСК.

Логика:
- Каждый день «принадлежит» предыдущему веб-трёхдневнику (Пн-Ср).
    Чт-Вс → Пн этой недели; Пн-Ср → Пн прошлой недели.
- Окно заказов: Ср (Mw+2) ... Вт следующей недели (Mw+8), 7 дней.
- Выгружаем нарастающим итогом: от Mw+2 до min(вчера, Mw+8).
- Label недели = «{сб} и {вс} {месяц_в_род.}», где сб=Mw-2, вс=Mw-1.
- Суммируем по каналам (CHANNELS из dashboard_regs). UTM берём из
  user_utm_source / user_utm_medium — переименовываем перед подсчётом.
- Перезаписываем колонку J (Заказы) в листе ДАННЫЕ.
"""
import asyncio
import logging
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
load_dotenv()

from wednesday import fetch_deals_wednesday
from dashboard_regs import MONTHS_GEN, count_by_channel, update_sheet

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("dashboard_deals")

DEALS_COLUMN_LETTER = "J"  # Заказы


async def main():
    now_msk = datetime.now(ZoneInfo("Europe/Moscow"))
    today = now_msk.date()
    dow = today.weekday()  # Пн=0 ... Вс=6

    if dow >= 3:                    # Чт-Вс → Пн этой недели
        mw = today - timedelta(days=dow)
    else:                           # Пн-Ср → Пн прошлой недели
        mw = today - timedelta(days=dow + 7)

    deals_from = mw + timedelta(days=2)     # Ср недели веба
    deals_to_max = mw + timedelta(days=8)   # Вт следующей недели
    yesterday = today - timedelta(days=1)
    date_to = min(yesterday, deals_to_max)

    sat = mw - timedelta(days=2)
    sun = mw - timedelta(days=1)
    week_label = f"{sat.day} и {sun.day} {MONTHS_GEN[sun.month]}"

    logger.info("МСК: %s", now_msk.isoformat())
    logger.info("Трёхдневник Пн=%s, окно заказов %s..%s", mw, deals_from, deals_to_max)
    logger.info("Выгружаем %s..%s (нарастающий итог до вчера)", deals_from, date_to)
    logger.info("Неделя в таблице: %r", week_label)

    if date_to < deals_from:
        logger.info("Нечего выгружать: date_to < deals_from")
        return

    df = await fetch_deals_wednesday(deals_from, date_to)
    logger.info("Заказов (стоимость>0): %d", len(df))

    # Для заказов UTM берём из пользователя, а не из сделки.
    rename_map = {}
    if "user_utm_source" in df.columns:
        rename_map["user_utm_source"] = "utm_source"
    if "user_utm_medium" in df.columns:
        rename_map["user_utm_medium"] = "utm_medium"
    if "utm_source" in df.columns and "user_utm_source" in df.columns:
        # дропаем сделочный utm_source чтобы не перезатёрся при rename
        df = df.drop(columns=["utm_source"])
    if "utm_medium" in df.columns and "user_utm_medium" in df.columns:
        df = df.drop(columns=["utm_medium"])
    if rename_map:
        df = df.rename(columns=rename_map)

    counts = count_by_channel(df)
    logger.info("Заказы по каналам: %s", counts)

    result = update_sheet(week_label, counts, col_letter=DEALS_COLUMN_LETTER)
    logger.info("Результат обновления:")
    for name, (row, value) in result.items():
        logger.info("  %-15s строка=%s заказов=%d", name, row, value)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.exception("Ошибка: %s", e)
        sys.exit(1)
