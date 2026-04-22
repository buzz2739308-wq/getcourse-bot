"""Обновление регистраций по каналам в Google Sheets дашборде.

Запуск: ежедневно в 00:01 МСК.
Cron пример: 1 0 * * * cd ~/Documents/getcourse_bot && /usr/bin/env python3 dashboard_regs.py

Логика:
- Берём ближайший трёхдневник (Пн-Ср) по МСК:
    Пн/Вт → этой недели; Ср-Вс → следующей недели.
- Трафик: Ср прошлой недели (Mw-5) ... Вт недели веба (Mw+1).
- Название недели в таблице: "{Сб} и {Вс} {месяц_в_родительном}".
- Выгружаем пользователей группы:
    "{Месяц} | Стажировка | Регистрация на веб. {ДД(Пн)}-{ДД.ММ.ГГГГ(Ср)}"
  за период от первой Ср трафика до вчера (нарастающий итог).
- Считаем по каналам → обновляем колонку Регистрации в листе ДАННЫЕ.
"""
import asyncio
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
load_dotenv()  # до импорта wednesday — он читает GC_API_KEY на уровне модуля

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

from wednesday import MONTHS_RU, fetch_users_by_group
from dashboard_utils import sanitize

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("dashboard_regs")

SHEET_ID = "1V7sVTvvpWB3ejHdTpu4plpyq8uFU5x99gW_IJs0508w"
TAB_NAME = "ДАННЫЕ"
# На проде (Railway) креды передаются через env GOOGLE_CREDENTIALS_JSON.
# Локально — ищем файл по CREDS_PATH.
CREDS_PATH = os.environ.get("GOOGLE_CREDENTIALS_PATH", "/Users/qwerty/wildmanager/credentials.json")
REG_COLUMN_LETTER = "I"  # Регистрации (после вставки колонки План: Неделя,Канал,План,Расход,Выручка,Охваты,Клики,Клики на лендинг,Регистрации)


def _escape_controls_inside_strings(s: str) -> str:
    """Экранирует control-символы только внутри JSON-строковых значений,
    сохраняя пробелы/переводы строк между токенами (они в JSON легальны)."""
    out = []
    in_string = False
    escape_next = False
    esc = {"\n": "\\n", "\r": "\\r", "\t": "\\t", "\b": "\\b", "\f": "\\f"}
    for ch in s:
        if escape_next:
            out.append(ch); escape_next = False; continue
        if in_string:
            if ch == "\\":
                out.append(ch); escape_next = True; continue
            if ch == '"':
                in_string = False; out.append(ch); continue
            if ch in esc:
                out.append(esc[ch]); continue
            if ord(ch) < 0x20:
                out.append(f"\\u{ord(ch):04x}"); continue
        else:
            if ch == '"':
                in_string = True
        out.append(ch)
    return "".join(out)


def _load_credentials(scopes):
    import base64
    # 1) base64-упакованный JSON — самый надёжный способ на Railway
    b64 = os.environ.get("GOOGLE_CREDENTIALS_JSON_B64")
    if b64:
        raw = base64.b64decode(b64).decode("utf-8")
        return Credentials.from_service_account_info(json.loads(raw), scopes=scopes)
    # 2) обычный JSON в env — пробуем как есть, при ошибке чиним control-символы
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if creds_json:
        try:
            data = json.loads(creds_json)
        except json.JSONDecodeError:
            data = json.loads(_escape_controls_inside_strings(creds_json))
        return Credentials.from_service_account_info(data, scopes=scopes)
    # 3) локальный файл (разработка)
    return Credentials.from_service_account_file(CREDS_PATH, scopes=scopes)

MONTHS_GEN = {
    1: "января", 2: "февраля", 3: "марта", 4: "апреля",
    5: "мая", 6: "июня", 7: "июля", 8: "августа",
    9: "сентября", 10: "октября", 11: "ноября", 12: "декабря",
}

# Каналы: правила по utm_source и/или utm_medium.
# Если совпало по source ИЛИ medium — засчитываем в канал.
# Все метки нормализуются в lower-case на загрузке модуля (см. _normalize_channels).
_CHANNELS_RAW = [
    ("Инст", {
        "source": {"a", "a_utm_medium", "anya", "lora", "n", "neelmo",
                   "neelmo_utm_medium", "nikita", "v", "v_utm_medium",
                   "vika", "i", "i_utm_medium", "inst"},
    }),
    ("Фб", {"source": {"fb_instbogds", "fb_philipp"}}),
    ("Яндекс", {"medium": {"redhumster", "redhumsterTG"}}),
    ("Яндекс тг", {"medium": {"yaazat"}}),
    ("Вк ниагара", {"source": {"vk_olga"}}),
    ("Ятг Влад", {"source": {"yandex_tg"}}),
    ("ТикТок", {"source": {"tiktok_phil"}}),
    ("Ютуб", {"source": {"youtube_phil"}}),
    ("ЯМакс", {"source": {"telegain"}}),
    ("Макс", {"source": {"max"}}),
]


def _normalize_channels(raw):
    return [
        (name, {key: {v.strip().lower() for v in vals} for key, vals in rules.items()})
        for name, rules in raw
    ]


CHANNELS = _normalize_channels(_CHANNELS_RAW)


def compute_dates(today: date) -> dict:
    dow = today.weekday()  # Пн=0 ... Вс=6
    if dow <= 2:
        mw = today - timedelta(days=dow)           # Пн этой недели (вкл. среду — добиваем вчерашний вторник)
    else:
        mw = today + timedelta(days=(7 - dow))      # Пн следующей недели
    wed = mw + timedelta(days=2)
    traffic_start = mw - timedelta(days=5)          # Ср прошлой недели
    traffic_end = mw + timedelta(days=1)            # Вт недели веба
    sat = mw - timedelta(days=2)
    sun = mw - timedelta(days=1)
    yesterday = today - timedelta(days=1)
    # Дата выгрузки: до min(вчера, конец трафика)
    date_to = min(yesterday, traffic_end)
    week_label = f"{sat.day} и {sun.day} {MONTHS_GEN[sun.month]}"
    group_name = (
        f"{MONTHS_RU[mw.month]} | Стажировка | Регистрация на веб. "
        f"{mw.strftime('%d')}-{wed.strftime('%d.%m.%Y')}"
    )
    return {
        "mw": mw, "wed": wed, "sat": sat, "sun": sun,
        "traffic_start": traffic_start,
        "date_to": date_to,
        "week_label": week_label,
        "group_name": group_name,
    }


def _channel_masks(df):
    """Возвращает dict channel_name -> boolean mask по CHANNELS."""
    src_col = next((c for c in ("utm_source", "user_utm_source") if c in df.columns), None)
    med_col = next((c for c in ("utm_medium", "user_utm_medium") if c in df.columns), None)
    src = df[src_col].fillna("").astype(str).str.strip().str.lower() if src_col else None
    med = df[med_col].fillna("").astype(str).str.strip().str.lower() if med_col else None
    masks = {}
    for name, rules in CHANNELS:
        mask = None
        if "source" in rules and src is not None:
            m = src.isin(rules["source"])
            mask = m if mask is None else (mask | m)
        if "medium" in rules and med is not None:
            m = med.isin(rules["medium"])
            mask = m if mask is None else (mask | m)
        masks[name] = mask
    return masks


NO_LABEL_CHANNEL = "Без меток"


def count_by_channel(df) -> dict:
    if df.empty:
        result = {name: 0 for name, _ in CHANNELS}
        result[NO_LABEL_CHANNEL] = 0
        return result
    masks = _channel_masks(df)
    result = {name: (int(m.sum()) if m is not None else 0) for name, m in masks.items()}
    # Остаток — всё что не попало ни в один канал
    result[NO_LABEL_CHANNEL] = int(len(df) - sum(result.values()))
    return result


def sum_by_channel(df, value_col: str) -> dict:
    """Суммирует value_col по каналам на основе CHANNELS. Остаток идёт в 'Без меток'."""
    if df.empty or value_col not in df.columns:
        result = {name: 0.0 for name, _ in CHANNELS}
        result[NO_LABEL_CHANNEL] = 0.0
        return result
    masks = _channel_masks(df)
    values = pd.to_numeric(df[value_col], errors="coerce").fillna(0)
    result = {
        name: (float(values[m].sum()) if m is not None else 0.0)
        for name, m in masks.items()
    }
    result[NO_LABEL_CHANNEL] = float(values.sum() - sum(result.values()))
    return result


def update_sheet(week_label: str, counts: dict, col_letter: str = REG_COLUMN_LETTER) -> dict:
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = _load_credentials(scopes)
    gc = gspread.authorize(creds)
    ws = gc.open_by_key(SHEET_ID).worksheet(TAB_NAME)
    all_values = ws.get_all_values()

    week_label = sanitize(week_label)
    counts = {sanitize(k): sanitize(v) for k, v in counts.items()}

    updates = []
    result = {}
    for name, value in counts.items():
        row_idx = None
        for i, row in enumerate(all_values, start=1):
            if i == 1:
                continue
            if len(row) >= 2 and row[0].strip() == week_label and row[1].strip() == name:
                row_idx = i
                break
        if row_idx is None:
            logger.warning("Строка не найдена: неделя=%r канал=%r", week_label, name)
            result[name] = ("не найдено", value)
            continue
        updates.append({
            "range": f"{col_letter}{row_idx}",
            "values": [[value]],
        })
        result[name] = (row_idx, value)

    if updates:
        ws.batch_update(updates, value_input_option="USER_ENTERED")
    return result


async def main():
    now_msk = datetime.now(ZoneInfo("Europe/Moscow"))
    today = now_msk.date()
    d = compute_dates(today)

    logger.info("МСК: %s", now_msk.isoformat())
    logger.info("Трёхдневник: Пн %s — Ср %s", d["mw"], d["wed"])
    logger.info("Трафик: %s — %s (выгружаем до %s)",
                d["traffic_start"], d["mw"] + timedelta(days=1), d["date_to"])
    logger.info("Неделя в таблице: %r", d["week_label"])
    logger.info("Группа: %r", d["group_name"])

    if d["date_to"] < d["traffic_start"]:
        logger.info("Нечего выгружать: date_to < traffic_start")
        return

    df = await fetch_users_by_group(
        d["group_name"],
        date_from=d["traffic_start"],
        date_to=d["date_to"],
    )
    logger.info("Пользователей в группе за период: %d", len(df))

    counts = count_by_channel(df)
    logger.info("Регистрации по каналам: %s", counts)

    result = update_sheet(d["week_label"], counts)
    logger.info("Результат обновления:")
    for name, (row, value) in result.items():
        logger.info("  %-15s строка=%s регистраций=%d", name, row, value)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.exception("Ошибка: %s", e)
        sys.exit(1)
