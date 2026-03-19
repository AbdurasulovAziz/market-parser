import os
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv

load_dotenv()

API_URL = "https://api-seller.ozon.ru/v3/posting/fbs/list"
HEADERS = {
    "Client-Id": os.environ['OZON_CLIENT_ID'],
    "Api-Key": os.environ['OZON_API_KEY'],
    "Content-Type": "application/json"
}

OZON_STATUSES = {
    ("awaiting_packaging", "posting_created"): "Ожидает сборки",
    ("delivering", "posting_on_way_to_city"): "Условно доставлен",
    ("delivering", "posting_in_pickup_point"): "Ждет покупателя в ПВЗ",
    ("delivering", "posting_conditionally_delivered"): "Доставляется",
    ("delivered", "posting_received"): "Доставлен",
}

# --- генератор диапазонов дат ---
def generate_month_ranges(months=3):
    now = datetime.now(timezone.utc)

    for i in range(months):
        first_day = (now.replace(day=1) - timedelta(days=0))  # начинаем с текущего месяца
        for _ in range(i):
            first_day = (first_day - timedelta(days=1)).replace(day=1)

        last_day = (first_day.replace(day=28) + timedelta(days=4)).replace(day=1)

        yield (
            first_day.strftime("%Y-%m-%dT%H:%M:%SZ"),
            last_day.strftime("%Y-%m-%dT%H:%M:%SZ")
        )


# --- загрузка заказов ---
def fetch_fbs_orders_by_month():
    all_orders = []

    for since, to in generate_month_ranges(3):
        offset = 0

        while True:
            payload = {
                "dir": "ASC",
                "filter": {
                    "since": since,
                    "to": to,
                },
                "limit": 100,
                "offset": offset
            }

            response = requests.post(API_URL, headers=HEADERS, json=payload)

            if response.status_code != 200:
                print("Ошибка API:", response.status_code, response.text)
                break

            data = response.json()
            result = data.get("result", {})

            postings = result.get("postings", [])
            all_orders.extend(postings)

            if not result.get("has_next", False):
                break

            offset += payload["limit"]

    return all_orders


# --- группировка по месяцам в формате dict ---
def group_by_month(orders):
    grouped = {}

    for order in orders:
        dt = order.get("in_process_at") or order.get("shipment_date")
        if not dt:
            continue

        dt_obj = datetime.fromisoformat(dt.replace("Z", "+00:00"))
        month_key = dt_obj.strftime("%m.%y")  # формат как в WB-скрипте

        if month_key not in grouped:
            grouped[month_key] = {}

        first_product = order.get("products", [{}])[0]

        grouped[month_key][order["order_number"]] = {
            "id": str(order["order_number"]),
            "title": first_product.get("name"),
            "article": str(first_product.get("offer_id")),
            "price": str(float(first_product["price"])),
            "platform": "Ozon",
            "created_at": dt_obj.strftime("%d.%m.%Y"),
            "status": get_order_status(order)
        }

    return grouped

def get_order_status(order):
    if order["status"] == "cancelled" and order['substatus'] == "posting_canceled" and order["cancellation"]["cancelled_after_ship"]:
        return "Отменено после отгрузки"
    elif order["status"] == "cancelled" and order['substatus'] == "posting_canceled" and not order["cancellation"]["cancelled_after_ship"]:
        return "Отменено до отгрузки"
    else:
        return OZON_STATUSES[order["status"], order["substatus"]]

def process():
    orders = fetch_fbs_orders_by_month()
    grouped_orders = group_by_month(orders)
    return grouped_orders
