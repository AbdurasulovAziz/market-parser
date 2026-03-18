import os
from datetime import datetime, timedelta, timezone

import httpx
from dotenv import load_dotenv

load_dotenv()


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

class WildberriesParser:
    CARD_NAMES_URL = "https://content-api.wildberries.ru/content/v2/get/cards/list"
    STATUSES_URL = "https://marketplace-api.wildberries.ru/api/v3/orders/status"
    ORDERS_URL = "https://marketplace-api.wildberries.ru/api/v3/orders"

    HEADERS = {
        "Authorization": os.environ["WB_API_KEY"]
    }

    def __init__(self, client: httpx.Client):
        self.client = client

    def get_card_names(self):
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

            response = self.client.post(
                self.CARD_NAMES_URL,
                headers=self.HEADERS,
                json=body
            )
            response.raise_for_status()

            data = response.json()
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

    def get_statuses(self, orders):
        order_ids = [order["id"] for order in orders]
        statuses = self._get_statuses(order_ids)

        return statuses

    def _get_statuses(self, order_ids: list[int]):
        response = self.client.post(
            self.STATUSES_URL,
            headers=self.HEADERS,
            json={"orders": order_ids}
        )
        response.raise_for_status()

        data = response.json()
        result = {}

        for order in data.get("orders", []):
            wb_status = order.get("wbStatus")
            result[order["id"]] = STATUSES.get(wb_status, wb_status)

        return result

    def get_orders(self):
        result = []
        for date_start, date_end in self._generate_exact_30day_chunks():
            next_value = 0

            while True:
                params = {
                    "limit": 1000,
                    "next": next_value,
                    "dateFrom": int(date_start.timestamp()),
                    "dateTo": int(date_end.timestamp()),
                }
                response = self.client.get(
                    self.ORDERS_URL,
                    headers=self.HEADERS,
                    params=params
                )
                response.raise_for_status()

                data = response.json()
                orders = data.get("orders", [])
                if not orders:
                    break

                result.extend(orders)

                next_value = data.get("next", 0)

        return result

    def format_data(self, orders, card_names, statuses):
        result = {}

        for order in orders:
            created = datetime.fromisoformat(
                order["createdAt"].replace("Z", "+00:00")
            ).astimezone(timezone(timedelta(hours=3)))

            month_key = created.strftime("%m.%y")

            result.setdefault(month_key, {})
            result[month_key][str(order["id"])] = {
                "id": str(order["id"]),
                "title": card_names.get(order["nmId"]),
                "article": str(order["article"]),
                "price": str(order["price"] / 100),
                "platform": "Wildberries",
                "created_at": created.strftime("%d.%m.%Y"),
                "status": statuses.get(order["id"]),
            }

        return result

    def process(self):
        card_names = self.get_card_names()
        orders = self.get_orders()
        statuses = self.get_statuses(orders)

        return self.format_data(orders, card_names, statuses)

    def _generate_exact_30day_chunks(self):
        """
        Генерирует интервалы ровно по 30 дней,
        начиная с конца текущего месяца и идя назад до начала месяца 2 месяца назад.
        """
        tz = timezone(timedelta(hours=3))
        today = datetime.now(tz)

        # начало месяца два месяца назад
        month_start_num = today.month - 2
        year = today.year
        while month_start_num <= 0:
            month_start_num += 12
            year -= 1
        start_limit = datetime(year, month_start_num, 1, 0, 0, 0, tzinfo=tz)

        # конец текущего месяца
        if today.month == 12:
            next_month_start = datetime(today.year + 1, 1, 1, tzinfo=tz)
        else:
            next_month_start = datetime(today.year, today.month + 1, 1, tzinfo=tz)
        end_date = next_month_start - timedelta(seconds=1)  # последний момент месяца

        chunk_end = end_date
        while chunk_end > start_limit:
            chunk_start = chunk_end - timedelta(days=30) + timedelta(seconds=1)  # ровно 30 дней включительно
            if chunk_start < start_limit:
                chunk_start = start_limit
            yield chunk_start, chunk_end
            chunk_end = chunk_start - timedelta(seconds=1)











# def get_month_bounds(months_ago: int = 0):
#     now = datetime.now(timezone(timedelta(hours=3)))
#     start_current = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
#
#     y = start_current.year
#     m = start_current.month - months_ago
#     while m <= 0:
#         m += 12
#         y -= 1
#
#     start = start_current.replace(year=y, month=m)
#
#     if start.month == 12:
#         next_start = start.replace(year=start.year + 1, month=1)
#     else:
#         next_start = start.replace(month=start.month + 1)
#
#     return start, next_start
#
#
# # ---------- CARD NAMES ----------
#
# def get_card_names(client: httpx.Client):
#     url = "https://content-api.wildberries.ru/content/v2/get/cards/list"
#
#     cursor_updated_at = None
#     cursor_nm_id = None
#     card_names = {}
#
#     while True:
#         body = {
#             "settings": {
#                 "sort": {"ascending": True},
#                 "filter": {"withPhoto": -1},
#                 "cursor": {"limit": 100}
#             }
#         }
#
#         if cursor_updated_at and cursor_nm_id:
#             body["settings"]["cursor"]["updatedAt"] = cursor_updated_at
#             body["settings"]["cursor"]["nmID"] = cursor_nm_id
#
#         r = client.post(url, headers=BASE_HEADERS, json=body)
#         r.raise_for_status()
#
#         data = r.json()
#         cards = data.get("cards", [])
#
#         if not cards:
#             break
#
#         for card in cards:
#             card_names[card["nmID"]] = card["title"]
#
#         cursor = data.get("cursor", {})
#
#         if cursor.get("total", 0) < 100:
#             break
#
#         cursor_updated_at = cursor.get("updatedAt")
#         cursor_nm_id = cursor.get("nmID")
#
#     return card_names
#
#
# # ---------- STATUSES ----------
#
# def get_statuses(client: httpx.Client, order_ids: list[int]):
#     url = "https://marketplace-api.wildberries.ru/api/v3/orders/status"
#     r = client.post(url, headers=BASE_HEADERS, json={"orders": order_ids})
#     r.raise_for_status()
#
#     data = r.json()
#     result = {}
#
#     for order in data.get("orders", []):
#         wb_status = order.get("wbStatus")
#         result[order["id"]] = STATUSES.get(wb_status, wb_status)
#
#     return result
#
#
# # ---------- PROCESS ----------
#
#
# def process():
#     result = {}
#
#     with httpx.Client(timeout=30.0) as client:
#         card_names = get_card_names(client)
#         url = "https://marketplace-api.wildberries.ru/api/v3/orders"
#
#         for iteration in range(3):
#             month_start, month_next = get_month_bounds(iteration)
#             chunk_start = month_start
#
#             while chunk_start < month_next:
#                 chunk_end = min(chunk_start + timedelta(days=30), month_next)
#
#                 next_value = 0
#
#                 while True:
#                     params = {
#                         "limit": 1000,
#                         "next": next_value,
#                         "dateFrom": int(chunk_start.timestamp()),
#                         "dateTo": int(chunk_end.timestamp()) - 1,
#                     }
#                     r = client.get(url, headers=BASE_HEADERS, params=params)
#                     r.raise_for_status()
#
#                     data = r.json()
#                     orders = data.get("orders", [])
#                     if not orders:
#                         break
#
#                     order_ids = [order["id"] for order in orders]
#                     statuses = get_statuses(client, order_ids)
#
#                     for order in orders:
#                         created = datetime.fromisoformat(
#                             order["createdAt"].replace("Z", "+00:00")
#                         ).astimezone(timezone(timedelta(hours=3)))
#
#                         month_key = created.strftime("%m.%y")
#
#                         result.setdefault(month_key, {})
#                         result[month_key][str(order["id"])] = {
#                             "id": str(order["id"]),
#                             "title": card_names.get(order["nmId"]),
#                             "article": str(order["article"]),
#                             "price": str(order["price"] / 100),
#                             "platform": "Wildberries",
#                             "created_at": created.strftime("%d.%m.%Y"),
#                             "status": statuses.get(order["id"]),
#                         }
#
#                     next_value = data.get("next", 0)
#
#                 chunk_start = chunk_end
#
#     return result
#
#


