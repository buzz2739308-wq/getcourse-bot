import asyncio
import json
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
        if data.get("error_code") in (905, 903):
            logger.info(f"Ожидаю, попытка {attempt+1}/{MAX_RETRIES}")
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
            content = await resp.read()
        try:
            data = json.loads(content.decode("utf-8"))
            info = data.get("info", {})
            if isinstance(info, dict) and "items" in info:
                logger.info(f"Данные готовы, строк: {len(info['items'])}")
                return info["fields"], info["items"]
            logger.info(f"Попытка {attempt+1}: данные ещё не готовы")
        except Exception as e:
            logger.info(f"Попытка {attempt+1}: {e}")
    raise TimeoutError("Экспорт не готов после всех попыток")

def build_dataframe(fields, items):
    df = pd.DataFrame(items, columns=fields)
    # Чистим списки в ячейках
    for col in df.columns:
        df[col] = df[col].apply(lambda x: ", ".join(str(i) for i in x) if isinstance(x, list) else x)
    # Числовые колонки
    for num_col in ["Заработано", "Оплачено"]:
        if num_col in df.columns:
            df[num_col] = pd.to_numeric(
                df[num_col].astype(str).str.replace(r"[^\d.]", "", regex=True),
                errors="coerce"
            ).fillna(0)
    # utm_source
    if "user_utm_source" in df.columns:
        df["user_utm_source"] = df["user_utm_source"].fillna("без источника").replace("", "без источника")
    else:
        df["user_utm_source"] = "без источника"
    return df

async def fetch_payments(for_date):
    async with aiohttp.ClientSession() as session:
        export_id = await _create_export(session, for_date, for_date)
        fields, items = await _wait_and_download(session, export_id)
    df = build_dataframe(fields, items)
    logger.info(f"Итого строк: {len(df)}")
    return df
