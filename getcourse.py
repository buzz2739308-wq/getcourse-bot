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

EXPORT_FIELDS = ["id","created_at","user_email","user_name","user_utm_source","user_utm_medium","user_utm_campaign","deal_cost","deal_profit","deal_status","offer_title"]

POLL_INTERVAL = 5
MAX_POLLS = 40

async def _create_export(session, date_from, date_to):
    url = f"{BASE_URL}/exports"
    params = {"key": GC_API_KEY}
    payload = {
        "type": "deals",
        "params": {
            "status_date": "created",
            "date_from": date_from.strftime("%d.%m.%Y"),
            "date_to": date_to.strftime("%d.%m.%Y"),
            "deal_status": "payed",
        },
        "fields": EXPORT_FIELDS,
    }
    async with session.post(url, params=params, json=payload) as resp:
        resp.raise_for_status()
        data = await resp.json(content_type=None)
    if data.get("success") is not True:
        raise RuntimeError(f"GetCourse API error: {data}")
    export_id = data["info"]["id"]
    logger.info(f"Экспорт создан, id={export_id}")
    return export_id

async def _wait_for_export(session, export_id):
    url = f"{BASE_URL}/exports/{export_id}"
    params = {"key": GC_API_KEY}
    for attempt in range(MAX_POLLS):
        await asyncio.sleep(POLL_INTERVAL)
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)
        info = data.get("info", {})
        status = info.get("status")
        logger.info(f"Экспорт {export_id}: статус={status} (попытка {attempt + 1})")
        if status == "ready":
            return info["file_url"]
        if status == "error":
            raise RuntimeError(f"Экспорт завершился с ошибкой: {info}")
    raise TimeoutError(f"Экспорт {export_id} не готов после {MAX_POLLS} попыток")

async def _download_csv(session, file_url):
    async with session.get(file_url) as resp:
        resp.raise_for_status()
        content = await resp.read()
    df = pd.read_csv(io.BytesIO(content), sep=";", encoding="utf-8-sig")
    logger.info(f"Скачано {len(df)} строк из GetCourse")
    return df

def _normalize(df):
    rename_map = {}
    for col in df.columns:
        low = col.lower()
        if "utm_source" in low:
            rename_map[col] = "user_utm_source"
        elif "profit" in low or "заработано" in low:
            rename_map[col] = "Заработано"
        elif "cost" in low or "оплачено" in low:
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
        file_url = await _wait_for_export(session, export_id)
        df = await _download_csv(session, file_url)
    df = _normalize(df)
    return df
