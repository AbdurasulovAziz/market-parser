import httpx

from ozon_integration import process as ozon_process
from sheets import run
from wb_integration import WildberriesParser

if __name__ == "__main__":
    with httpx.Client(timeout=30.0) as client:
        wb_orders = WildberriesParser(client).process()

    ozon_orders = ozon_process()

    for month, orders in ozon_orders.items():
        if month in wb_orders:
            wb_orders[month].update(orders)
        else:
            wb_orders[month] = orders

    wb_orders = dict(sorted(wb_orders.items(), reverse=True))
    run(wb_orders)