from sheets import run
from wb_integration import process

if __name__ == "__main__":
    orders = process()
    run(orders)