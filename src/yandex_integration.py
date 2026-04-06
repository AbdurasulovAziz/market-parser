import os
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv

from utils import generate_month_ranges

load_dotenv()

HEADERS = {
    "Api-Key": os.environ.get("YANDEX_API_KEY"),
    "Content-Type": "application/json"
}

YANDEX_STATUSES = {
    "PROCESSING": "Обрабатывается",
    "DELIVERY": "Доставляется",
    "DELIVERED": "Доставлен",
    "CANCELLED": "Отменено",
}

CAMPAIGN_ID = os.environ.get("YANDEX_BUSINESS_ID")

API_URL = f"https://api.partner.market.yandex.ru/v1/businesses/{CAMPAIGN_ID}/orders"

def fetch_orders_by_month():
    all_orders = []
    for since, to in generate_month_ranges(3):
        params = {
            "limit": 50,
            "pageToken": None
        }
        body = {
            "dates": {
                "creationDateFrom": datetime.fromisoformat(since).strftime("%Y-%m-%d"),
                "creationDateTo": datetime.fromisoformat(to).strftime("%Y-%m-%d")
            }
        }
        while True:
            response = requests.post(API_URL.format(campaign_id=CAMPAIGN_ID), headers=HEADERS, params=params, json=body)
            if response.status_code != 200:
                print("Ошибка API:", response.status_code, response.text)
                break

            data = response.json()
            orders = data.get("orders", [])
            all_orders.extend(orders)

            next_token = data.get("paging", {}).get("nextPageToken")
            if not next_token:
                break

            params["pageToken"] = next_token

        params.pop("pageToken", None)

    return all_orders


def group_by_month(orders):
    grouped = {}
    for order in orders:
        dt = order.get("creationDate")
        if not dt:
            continue
        dt_obj = datetime.fromisoformat(dt.replace("Z", "+00:00"))
        month_key = dt_obj.strftime("%m.%y")

        if month_key not in grouped:
            grouped[month_key] = {}

        first_item = order.get("items", [{}])[0]

        grouped[month_key][str(order["orderId"])] = {
            "id": str(order["orderId"]),
            "title": first_item.get("offerName"),
            "article": str(first_item.get("offerId")),
            "price": str(first_item['prices']['payment']['value'] + first_item['prices'].get('subsidy', {}).get('value', 0)),
            "platform": "Yandex Market",
            "created_at": dt_obj.strftime("%d.%m.%Y"),
            "status": YANDEX_STATUSES.get(order["status"], order["status"]),
        }

    return grouped


def process():
    orders = fetch_orders_by_month()
    return group_by_month(orders)