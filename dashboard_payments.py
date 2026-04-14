"""Обновление колонок Выручка и Оплаты в Google Sheets дашборде.

Запуск: ежедневно в 00:01 МСК.

Логика (та же что у заказов):
- Каждый день «принадлежит» предыдущему веб-трёхдневнику (Пн-Ср).
    Чт-Вс → Пн этой недели; Пн-Ср → Пн прошлой недели.
- Окно оплат: Ср (Mw+2) ... Вт следующей недели (Mw+8).
- Выгружаем нарастающим итогом: от Mw+2 до min(вчера, Mw+8)
  через fetch_deals_payed (payed_at, status=payed).
- Label недели = «{сб} и {вс} {месяц_в_род.}», сб=Mw-2, вс=Mw-1.
- UTM берём из user_utm_source / user_utm_medium → переименовываем.
- Оплаты = count по каналам (колонка K).
  Выручка = сумма «Заработано, RUB» по каналам (колонка D).
"""
import asyncio
import logging
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
load_dotenv()

import pandas as pd

from wednesday import fetch_deals_payed
from dashboard_regs import (
    MONTHS_GEN,
    count_by_channel,
    sum_by_channel,
    update_sheet,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("dashboard_payments")

REVENUE_COL_LETTER = "E"   # Выручка (после вставки колонки План)
PAYMENTS_COL_LETTER = "L"  # Оплаты (после вставки колонки План)
EARNED_FIELD = "Заработано"


async def main():
    now_msk = datetime.now(ZoneInfo("Europe/Moscow"))
    today = now_msk.date()
    dow = today.weekday()

    if dow >= 3:
        mw = today - timedelta(days=dow)
    else:
        mw = today - timedelta(days=dow + 7)

    pay_from = mw + timedelta(days=2)
    pay_to_max = mw + timedelta(days=8)
    yesterday = today - timedelta(days=1)
    date_to = min(yesterday, pay_to_max)

    sat = mw - timedelta(days=2)
    sun = mw - timedelta(days=1)
    week_label = f"{sat.day} и {sun.day} {MONTHS_GEN[sun.month]}"

    logger.info("МСК: %s", now_msk.isoformat())
    logger.info("Трёхдневник Пн=%s, окно оплат %s..%s", mw, pay_from, pay_to_max)
    logger.info("Выгружаем %s..%s (нарастающий итог до вчера)", pay_from, date_to)
    logger.info("Неделя в таблице: %r", week_label)

    if date_to < pay_from:
        logger.info("Нечего выгружать: date_to < pay_from")
        return

    df = await fetch_deals_payed(pay_from, date_to)
    logger.info("Оплат (стоимость>0): %d", len(df))

    # UTM из пользователя, а не из сделки
    rename_map = {}
    if "utm_source" in df.columns and "user_utm_source" in df.columns:
        df = df.drop(columns=["utm_source"])
    if "utm_medium" in df.columns and "user_utm_medium" in df.columns:
        df = df.drop(columns=["utm_medium"])
    if "user_utm_source" in df.columns:
        rename_map["user_utm_source"] = "utm_source"
    if "user_utm_medium" in df.columns:
        rename_map["user_utm_medium"] = "utm_medium"
    if rename_map:
        df = df.rename(columns=rename_map)

    # Нормализуем «Заработано, RUB» к числу
    if EARNED_FIELD in df.columns:
        df[EARNED_FIELD] = pd.to_numeric(
            df[EARNED_FIELD].astype(str).str.replace(r"[^0-9.]", "", regex=True),
            errors="coerce",
        ).fillna(0)
    else:
        logger.warning("Нет колонки %r в выгрузке — выручка будет 0", EARNED_FIELD)

    payments = count_by_channel(df)
    revenue = sum_by_channel(df, EARNED_FIELD)

    logger.info("Оплаты по каналам:  %s", payments)
    logger.info("Выручка по каналам: %s", {k: round(v) for k, v in revenue.items()})

    # Округляем выручку до целых рублей
    revenue_int = {k: int(round(v)) for k, v in revenue.items()}

    res_pay = update_sheet(week_label, payments, col_letter=PAYMENTS_COL_LETTER)
    res_rev = update_sheet(week_label, revenue_int, col_letter=REVENUE_COL_LETTER)

    logger.info("Результат обновления:")
    for name in payments:
        row_p, val_p = res_pay.get(name, ("?", 0))
        row_r, val_r = res_rev.get(name, ("?", 0))
        logger.info(
            "  %-15s оплат=%d (строка %s)  выручка=%d ₽ (строка %s)",
            name, val_p, row_p, val_r, row_r
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.exception("Ошибка: %s", e)
        sys.exit(1)
