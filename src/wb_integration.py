import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import httpx
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

load_dotenv()

BASE_HEADERS = {
    "Authorization": os.environ["WB_API_KEY"]
}

STATUSES = {
    "waiting": "В ожидании",
    "sorted": "Отсортировано",
    "sold": "Товар выкуплен",
    "canceled": "Отменено",
    "canceled_by_client": "Покупатель отказался",
    "declined_by_client": "Отменено покупателем",
    "defect": "Найдены дефекты",
    "ready_for_pickup": "Ждет покупателя в ПВЗ",
    "accepted_by_carrier": "Передан перевозчику",
    "sent_to_carrier": "Отправлен перевозчику"
}

def get_month_bounds(months_ago: int = 0):
    now = datetime.now(timezone(timedelta(hours=3)))
    start_current = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    y = start_current.year
    m = start_current.month - months_ago
    while m <= 0:
        m += 12
        y -= 1

    start = start_current.replace(year=y, month=m)

    if start.month == 12:
        next_start = start.replace(year=start.year + 1, month=1)
    else:
        next_start = start.replace(month=start.month + 1)

    return start, next_start  # [start, next_start)


# ---------- CARD NAMES ----------

def get_card_names(client: httpx.Client):
    url = "https://content-api.wildberries.ru/content/v2/get/cards/list"

    cursor_updated_at = None
    cursor_nm_id = None
    card_names = {}

    while True:
        body = {
            "settings": {
                "sort": {"ascending": True},
                "filter": {"withPhoto": -1},
                "cursor": {"limit": 100}
            }
        }

        if cursor_updated_at and cursor_nm_id:
            body["settings"]["cursor"]["updatedAt"] = cursor_updated_at
            body["settings"]["cursor"]["nmID"] = cursor_nm_id

        r = client.post(url, headers=BASE_HEADERS, json=body)
        r.raise_for_status()

        data = r.json()
        cards = data.get("cards", [])

        if not cards:
            break

        for card in cards:
            card_names[card["nmID"]] = card["title"]

        cursor = data.get("cursor", {})

        if cursor.get("total", 0) < 100:
            break

        cursor_updated_at = cursor.get("updatedAt")
        cursor_nm_id = cursor.get("nmID")

    return card_names


# ---------- STATUSES ----------

def get_statuses(client: httpx.Client, order_ids: list[int]):
    url = "https://marketplace-api.wildberries.ru/api/v3/orders/status"
    r = client.post(url, headers=BASE_HEADERS, json={"orders": order_ids})
    r.raise_for_status()

    data = r.json()
    result = {}

    for order in data.get("orders", []):
        wb_status = order.get("wbStatus")
        result[order["id"]] = STATUSES.get(wb_status, wb_status)

    return result


# ---------- PROCESS ----------

MAX_DAYS = 30

def process():
    result = {}

    with httpx.Client(timeout=30.0) as client:
        card_names = get_card_names(client)
        url = "https://marketplace-api.wildberries.ru/api/v3/orders"

        for iteration in range(3):
            month_start, month_next = get_month_bounds(iteration)
            chunk_start = month_start

            while chunk_start < month_next:
                chunk_end = min(chunk_start + timedelta(days=MAX_DAYS), month_next)

                next_value = 0

                while True:
                    params = {
                        "limit": 1000,
                        "next": next_value,
                        "dateFrom": int(chunk_start.timestamp()),
                        "dateTo": int(chunk_end.timestamp()) - 1,
                    }
                    r = client.get(url, headers=BASE_HEADERS, params=params)
                    r.raise_for_status()

                    data = r.json()
                    orders = data.get("orders", [])
                    if not orders:
                        break

                    order_ids = [order["id"] for order in orders]
                    statuses = get_statuses(client, order_ids)

                    for order in orders:
                        created = datetime.fromisoformat(
                            order["createdAt"].replace("Z", "+00:00")
                        ).astimezone(timezone(timedelta(hours=3)))

                        month_key = created.strftime("%m.%y")

                        result.setdefault(month_key, {})
                        result[month_key][order["id"]] = {
                            "id": str(order["id"]),
                            "title": card_names.get(order["nmId"]),
                            "article": str(order["article"]),
                            "price": str(order["price"] / 100),
                            "created_at": created.strftime("%d.%m.%Y"),
                            "status": statuses.get(order["id"]),
                        }

                    next_value = data.get("next", 0)

                chunk_start = chunk_end

    return result




