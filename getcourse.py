import asyncio
import io
import logging
import os
from datetime import date

import aiohttp
import pandas as pd

logger = logging.getLogger(__name__)

GC_DOMAIN = os.environ.get("GC_DOMAIN", "wildmanagerschoolru.getcourse.ru")
GC_API_KEY = os.environ["GC_API_KEY"]
BASE_URL = f"https://{GC_DOMAIN}/pl/api/account"

POLL_INTERVAL = 15
MAX_POLLS = 40
RETRY_INTERVAL = 300
MAX_RETRIES = 12

async def _create_export(session, date_from, date_to):
    url = f"{BASE_URL}/deals"
    params = {
        "key": GC_API_KEY,
        "payed_at[from]": date_from.strftime("%Y-%m-%d"),
        "payed_at[to]": date_to.strftime("%Y-%m-%d"),
        "status": "payed",
        "format": "xlsx",
    }
    for attempt in range(MAX_RETRIES):
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)
        logger.info(f"GetCourse create response: {data}")
        if data.get("success") is True:
            export_id = data["info"]["export_id"]
            logger.info(f"Экспорт создан, id={export_id}")
            return export_id
        if data.get("error_code") == 905:
            logger.info(f"Очередь занята, жду 5 минут (попытка {attempt+1}/{MAX_RETRIES})")
            await asyncio.sleep(RETRY_INTERVAL)
        else:
            raise RuntimeError(f"GetCourse API error: {data}")
    raise RuntimeError("GetCourse: очередь занята больше часа")

async def _wait_and_download(session, export_id):
    url = f"{BASE_URL}/exports/{export_id}"
    params = {"key": GC_API_KEY}
    for attempt in range(MAX_POLLS):
        await asyncio.sleep(POLL_INTERVAL)
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            content_type = resp.headers.get("Content-Type", "")
            logger.info(f"Попытка {attempt+1}: Content-Type={content_type}")
            if "application/json" in content_type or "text/html" in content_type:
                # Файл ещё не готов
                text = await resp.text()
                logger.info(f"Ответ: {text[:200]}")
                continue
            else:
                # Файл готов — читаем как бинарный
                content = await resp.read()
                logger.info(f"Файл получен, размер: {len(content)} байт")
                return content
    raise TimeoutError("Экспорт не готов после всех попыток")

def _normalize(df):
    rename_map = {}
    for col in df.columns:
        low = col.lower()
        if "utm_source" in low:
            rename_map[col] = "user_utm_source"
        elif "заработано" in low:
            rename_map[col] = "Заработано"
        elif "оплачено" in low:
            rename_map[col] = "Оплачено"
    df = df.rename(columns=rename_map)
    for num_col in ["Заработано", "Оплачено"]:
        if num_col in df.columns:
            df[num_col] = (df[num_col].astype(str).str.replace(r"[^\d.,]", "", regex=True).str.replace(",", ".").pipe(pd.to_numeric, errors="coerce").fillna(0))
    if "user_utm_source" in df.columns:
        df["user_utm_source"] = df["user_utm_source"].fillna("без источника").replace("", "без источника")
    else:
        df["user_utm_source"] = "без источника"
    return df

async def fetch_payments(for_date):
    async with aiohttp.ClientSession() as session:
        export_id = await _create_export(session, for_date, for_date)
        content = await _wait_and_download(session, export_id)
    df = pd.read_csv(io.BytesIO(content), encoding="utf-8-sig")
    logger.info(f"Скачано {len(df)} строк из GetCourse")
    df = _normalize(df)
    return df
