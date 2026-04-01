import httpx

from ozon_integration import process as ozon_process
from yandex_integration import process as yandex_process
from sheets import run
from wb_integration import WildberriesParser


def merge_orders(*orders_dicts):
    """
    Объединяет несколько словарей заказов по месяцам.
    Каждый словарь должен иметь формат: { "MM.YY": {order_id: order_data} }
    """
    merged = {}
    for orders in orders_dicts:
        for month, month_orders in orders.items():
            if month not in merged:
                merged[month] = {}
            merged[month].update(month_orders)
    return dict(sorted(merged.items(), reverse=True))  # сортируем по убыванию месяца


if __name__ == "__main__":
    with httpx.Client(timeout=30.0) as client:
        wb_orders = WildberriesParser(client).process()

    ozon_orders = ozon_process()
    yandex_orders = yandex_process()

    result = merge_orders(wb_orders, ozon_orders, yandex_orders)

    run(result)