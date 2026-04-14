"""Обновление колонки Участники в Google Sheets дашборде.

Запуск: вт-пт в 00:01 МСК (в пн/сб/вс — ранний return).

Логика:
- Берём Пн текущей ISO-недели (через wednesday.get_wednesday_dates).
- Источники:
    * Входы: группа "{Месяц} | Курс 2-26 | Вход на веб. {ПН.ММ.ГГ}",
      период date_from=Пн, date_to=Пн.
    * Записи: группа "КУРС 1.1 | Просмотр записи вебинара. День 1. 2025",
      период date_from=Пн, date_to=Пн+6.
- Суммируем по каналам (CHANNELS из dashboard_regs) через count_by_channel.
- Перезаписываем колонку I (Участники) в листе ДАННЫЕ для строк
  Неделя={сб} и {вс} <месяц в род. падеже>, Канал=<имя канала>.
"""
import asyncio
import logging
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
load_dotenv()

import pandas as pd

from wednesday import get_wednesday_dates, fetch_users_by_group
from dashboard_regs import CHANNELS, MONTHS_GEN, count_by_channel, update_sheet

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("dashboard_participants")

PART_COLUMN_LETTER = "J"  # Участники (после вставки колонки План)


async def main():
    now_msk = datetime.now(ZoneInfo("Europe/Moscow"))
    today = now_msk.date()
    dow = today.weekday()  # Пн=0 ... Вс=6

    logger.info("МСК: %s (день недели: %d)", now_msk.isoformat(), dow)
    if dow not in (1, 2, 3, 4):  # вт-пт
        logger.info("Запуск только вт-пт, пропускаем")
        return

    dates = get_wednesday_dates(today)
    mon = dates["mon"]
    sat = mon - timedelta(days=2)
    sun = mon - timedelta(days=1)
    week_label = f"{sat.day} и {sun.day} {MONTHS_GEN[sun.month]}"

    logger.info("Пн трёхдневника: %s, label: %r", mon, week_label)
    logger.info("Группа входов:  %r", dates["entry_group_name"])
    logger.info("Группа записей: %r", dates["views_group_name"])

    # 1. Входы — только за понедельник
    df_entry = await fetch_users_by_group(
        dates["entry_group_name"], date_from=mon, date_to=mon
    )
    logger.info("Входов: %d", len(df_entry))

    # 2. Записи — пн..пн+6
    df_views = await fetch_users_by_group(
        dates["views_group_name"], date_from=mon, date_to=mon + timedelta(days=6)
    )
    logger.info("Записей: %d", len(df_views))

    # Склеиваем и считаем по каналам
    df = pd.concat([df_entry, df_views], ignore_index=True)
    logger.info("Всего для учёта: %d", len(df))

    counts = count_by_channel(df)
    logger.info("Участники по каналам: %s", counts)

    result = update_sheet(week_label, counts, col_letter=PART_COLUMN_LETTER)
    logger.info("Результат обновления:")
    for name, (row, value) in result.items():
        logger.info("  %-15s строка=%s участников=%d", name, row, value)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.exception("Ошибка: %s", e)
        sys.exit(1)
