import asyncio
import json
import logging
import os
from datetime import date, timedelta

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

MONTHS_RU = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
    5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
    9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь"
}

def get_wednesday_dates(today: date) -> dict:
    """Вычисляет все нужные даты для среды."""
    # Понедельник текущей недели
    mon = today - timedelta(days=today.weekday())
    wed = mon + timedelta(days=2)  # среда = пн + 2

    # Прошлый трёхдневник
    prev_mon = mon - timedelta(weeks=1)
    prev_wed = prev_mon + timedelta(days=2)

    # Позапрошлый трёхдневник
    prev2_mon = mon - timedelta(weeks=2)
    prev2_wed = prev2_mon + timedelta(days=2)

    return {
        # Текущий трёхдневник
        "mon": mon,
        "wed": wed,
        # Регистрации — группа с датами текущего трёхдневника
        "reg_group_name": (
            f"{MONTHS_RU[mon.month]} | Стажировка | "
            f"Регистрация на веб. {mon.strftime('%d')}-{wed.strftime('%d.%m.%Y')}"
        ),
        # Входы — группа с датой понедельника
        "entry_group_name": (
            f"{MONTHS_RU[mon.month]} | Курс 2-26 | "
            f"Вход на веб. {mon.strftime('%d.%m.%y')}"
        ),
        # Записи — постоянная группа, фильтр по дате добавления
        "views_group_name": "КУРС 1.1 | Просмотр записи вебинара. День 1. 2025",
        "views_date_from": mon,
        "views_date_to": mon + timedelta(days=6),  # пн + 6 = вс

        # Заказы прошлого трёхдневника: от среды прошлого до следующего вт
        "deals1_from": prev_wed,
        "deals1_to": prev_wed + timedelta(days=6),
        "deals1_label": f"{prev_wed.strftime('%d.%m')}-{(prev_wed + timedelta(days=6)).strftime('%d.%m')}",

        # Заказы позапрошлого трёхдневника
        "deals2_from": prev2_wed,
        "deals2_to": prev2_wed + timedelta(days=6),
        "deals2_label": f"{prev2_wed.strftime('%d.%m')}-{(prev2_wed + timedelta(days=6)).strftime('%d.%m')}",
    }

# --- Общие функции ---

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
                logger.info(f"Готово, строк: {len(info['items'])}")
                return info["fields"], info["items"]
            logger.info(f"Попытка {attempt+1}: ещё не готово")
        except Exception as e:
            logger.info(f"Попытка {attempt+1}: {e}")
    raise TimeoutError("Экспорт не готов")

async def _create_export_with_retry(session, url, params):
    for attempt in range(MAX_RETRIES):
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)
        logger.info(f"Create response: {data}")
        if data.get("success") is True:
            export_id = data["info"]["export_id"]
            logger.info(f"Экспорт создан id={export_id}")
            return export_id
        if data.get("error_code") in (905, 903):
            logger.info(f"Очередь занята, жду 5 мин (попытка {attempt+1}/{MAX_RETRIES})")
            await asyncio.sleep(RETRY_INTERVAL)
        else:
            raise RuntimeError(f"GetCourse error: {data}")
    raise RuntimeError("Очередь занята больше часа")

def _clean_df(fields, items):
    df = pd.DataFrame(items, columns=fields)
    for col in df.columns:
        df[col] = df[col].apply(lambda x: ", ".join(str(i) for i in x) if isinstance(x, list) else x)
    return df

# --- Поиск группы по названию ---

async def find_group_id(session, group_name: str) -> int:
    url = f"{BASE_URL}/groups"
    params = {"key": GC_API_KEY}
    async with session.get(url, params=params) as resp:
        resp.raise_for_status()
        data = await resp.json(content_type=None)
    groups = data.get("info", [])
    for g in groups:
        if g["name"] == group_name:
            logger.info(f"Найдена группа: {group_name} id={g['id']}")
            return g["id"]
    raise ValueError(f"Группа не найдена: {group_name}")

# --- Выгрузка пользователей по группе ---

async def fetch_users_by_group(group_name: str, date_from: date = None, date_to: date = None) -> pd.DataFrame:
    async with aiohttp.ClientSession() as session:
        group_id = await find_group_id(session, group_name)
        url = f"{BASE_URL}/groups/{group_id}/users"
        params = {"key": GC_API_KEY}
        if date_from:
            params["added_at[from]"] = date_from.strftime("%Y-%m-%d")
        if date_to:
            params["added_at[to]"] = date_to.strftime("%Y-%m-%d")
        export_id = await _create_export_with_retry(session, url, params)
        fields, items = await _wait_and_download(session, export_id)
    return _clean_df(fields, items)

# --- Выгрузка заказов ---

async def fetch_deals_wednesday(date_from: date, date_to: date) -> pd.DataFrame:
    async with aiohttp.ClientSession() as session:
        url = f"{BASE_URL}/deals"
        params = {
            "key": GC_API_KEY,
            "created_at[from]": date_from.strftime("%Y-%m-%d"),
            "created_at[to]": date_to.strftime("%Y-%m-%d"),
            "free": "0",
        }
        export_id = await _create_export_with_retry(session, url, params)
        fields, items = await _wait_and_download(session, export_id)
    df = _clean_df(fields, items)
    # Фильтруем — исключаем ненужные теги
    exclude_tags = ["*Инфографика", "Бизнес на Wildberries", "скидка_ноябрь2025"]
    if "Теги предложений" in df.columns:
        for tag in exclude_tags:
            df = df[~df["Теги предложений"].str.contains(tag, na=False)]
    return df

# --- Аналитика ---

def analytics_users(df: pd.DataFrame, label: str, kind: str) -> str:
    total = len(df)
    lines = [f"📊 <b>{kind} {label}:</b>", f"Всего: <b>{total}</b>", "", "🎯 <b>По источникам:</b>"]
    if "utm_source" in df.columns and "utm_medium" in df.columns:
        grouped = (
            df.assign(
                source=df["utm_source"].fillna("—").replace("", "—"),
                medium=df["utm_medium"].fillna("—").replace("", "—")
            )
            .groupby(["source", "medium"])
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
        )
        for _, row in grouped.iterrows():
            lines.append(f"— {row['source']} / {row['medium']}: {row['count']}")
    return "\n".join(lines)

def analytics_deals_wednesday(df: pd.DataFrame, label: str) -> str:
    total = len(df)
    cost_col = "Стоимость, RUB"
    total_sum = 0
    if cost_col in df.columns:
        df[cost_col] = pd.to_numeric(
            df[cost_col].astype(str).str.replace(r"[^\d.]", "", regex=True),
            errors="coerce"
        ).fillna(0)
        total_sum = df[cost_col].sum()

    def fmt(v):
        return f"{float(v):,.0f} ₽".replace(",", " ")

    lines = [
        f"📊 <b>Заказы {label}:</b>",
        f"Всего заказов: <b>{total}</b>",
        f"Сумма: <b>{fmt(total_sum)}</b>",
        "",
        "🎯 <b>По источникам:</b>"
    ]
    if "utm_source" in df.columns and "utm_medium" in df.columns and cost_col in df.columns:
        grouped = (
            df.assign(
                source=df["utm_source"].fillna("—").replace("", "—"),
                medium=df["utm_medium"].fillna("—").replace("", "—")
            )
            .groupby(["source", "medium"])
            .agg(count=("utm_source", "count"), total=(cost_col, "sum"))
            .sort_values("total", ascending=False)
            .reset_index()
        )
        for _, row in grouped.iterrows():
            lines.append(f"— {row['source']} / {row['medium']}: {int(row['count'])} заказов / {fmt(row['total'])}")
    return "\n".join(lines)
