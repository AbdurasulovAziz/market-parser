import os
from typing import Dict, Any, List, Tuple, Optional

import gspread
from dotenv import load_dotenv
from gspread_formatting import format_cell_range, Color, TextFormat, CellFormat, set_frozen, \
    get_conditional_format_rules, ConditionalFormatRule, BooleanRule, BooleanCondition, GridRange

from wb_integration import STATUSES

load_dotenv()

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# --- Настройки колонок (под твою схему A:G, id заказа в колонке F) ---
DATE_COL = "A"
TITLE_COL = "B"
ARTICLE_COL = "C"
PRICE_COL = "D"
SOURCE_COL = "E"
ORDER_ID_COL = "F"
STATUS_COL = "G"

ID_COL_DATE = 1
ID_COL_INDEX_ZERO_BASED = 5  # F в диапазоне A:G -> индекс 5
ID_COL_STATUS = 6

HEADERS = [
    "Дата",
    "Наименование",
    "Артикул",
    "Цена продажи",
    "Площадка",
    "Номер заказа",
    "Статус заказа"
]

def setup_status_conditional_formatting(ws):
    """
    Добавляет условное форматирование для статусов в колонке G (начиная со 2 строки).
    Один раз вызываешь при создании листа (или перед синком) — и всё.
    """
    rules = get_conditional_format_rules(ws)

    # (опционально) очищаем старые правила, чтобы не плодить дубликаты
    rules.clear()

    status_colors = {
        "Отменено покупателем": Color(1.0, 0.5, 0.5),
        "Покупатель отказался": Color(1.0, 0.5, 0.5),
        "Отсортировано": Color(1, 0.9, 0.4),
        "В ожидании": Color(1, 0.9, 0.4),
        "Ждет покупателя в ПВЗ": Color(1, 0.9, 0.4),
        "Товар выкуплен": Color(0.8, 1.0, 0.8),
        "Отменено": Color(1.0, 0.5, 0.5),
        "Найдены дефекты": Color(1.0, 0.5, 0.5),
        "Передан перевозчику": Color(1, 0.9, 0.4),
        "Отправлен перевозчику": Color(1, 0.9, 0.4),


        "Ожидает сборки": Color(1, 0.9, 0.4),
        "Доставляется": Color(1, 0.9, 0.4),

        "Доставлен": Color(0.8, 1.0, 0.8),
        "Условно доставлен": Color(0.8, 1.0, 0.8),

        "Отменено после отгрузки": Color(1.0, 0.5, 0.5),
    }

    g_range = GridRange.from_a1_range("G2:G", ws)

    for status, color in status_colors.items():
        rules.append(
            ConditionalFormatRule(
                ranges=[g_range],
                booleanRule=BooleanRule(
                    condition=BooleanCondition("TEXT_EQ", [status]),
                    format=CellFormat(backgroundColor=color),
                ),
            )
        )

    rules.save()

def get_or_create_worksheet(spreadsheet, title: str, rows=1000, cols=20):
    try:
        return spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)

        # 1️⃣ Записываем заголовки
        ws.update("A1:G1", [HEADERS])

        # 2️⃣ Замораживаем первую строку
        set_frozen(ws, rows=1)

        # 3️⃣ Форматирование заголовков
        header_format = CellFormat(
            backgroundColor=Color(0.9, 0.9, 0.9),
            textFormat=TextFormat(bold=True),
            horizontalAlignment="CENTER"
        )

        format_cell_range(ws, "A1:G1", header_format)

        # 4️⃣ Границы
        ws.format("A1:G1", {
            "borders": {
                "top": {"style": "SOLID"},
                "bottom": {"style": "SOLID"},
                "left": {"style": "SOLID"},
                "right": {"style": "SOLID"},
            }
        })
        return ws
def open_worksheet(credentials_path: str, sheet_name: str):
    gc = gspread.service_account(credentials_path)
    sh = gc.open_by_url(os.environ["SHEET_URL"])

    return get_or_create_worksheet(sh, sheet_name)

def build_row(payload: Dict[str, Any]) -> List[Any]:
    """Строка под A:G."""
    return [
        payload.get("created_at", ""),
        payload.get("title", ""),
        payload.get("article", ""),
        str(payload.get("price", "")),
        payload.get("platform", ""),
        payload.get("id", ""),
        payload.get("status", ""),
    ]


def build_updates_from_sheet(
    ws,
    orders_by_id: Dict[int, Dict[str, Any]],
    *,
    data_range: str = "A2:G",
    id_index: int = ID_COL_INDEX_ZERO_BASED,
) -> Tuple[List[Dict[str, Any]], Dict[int, Dict[str, Any]]]:
    """
    Пробегаем по строкам листа (A2:G), ищем order_id в колонке E,
    если он есть в orders_by_id — готовим апдейт и удаляем из dict (pop).
    Возвращаем:
      - updates для batch_update
      - remaining dict (в нём останутся только новые заказы, которых нет в таблице)
    """
    rows = ws.get(data_range)
    updates: List[Dict[str, Any]] = []

    # gspread batch_update ждёт list[dict], поэтому собираем списком:
    updates_list: List[Dict[str, Any]] = []

    for row_num, row in enumerate(rows, start=2):
        # row может быть короче 6, если справа пустые ячейки
        if len(row) <= id_index:
            continue

        order_id = row[id_index]
        if order_id is None:
            continue

        payload = orders_by_id.pop(order_id, None)
        if not payload:
            continue

        updates_list.append({
            "range": f"A{row_num}:G{row_num}",
            "values": [build_row(payload)],
        })

    return updates_list, orders_by_id


def apply_updates(ws, updates: List[Dict[str, Any]]):
    if not updates:
        return
    # В gspread это прокидывается в values_batch_update
    ws.batch_update(updates, value_input_option="USER_ENTERED")


def append_new_rows(ws, orders_by_id: Dict[int, Dict[str, Any]], chunk_size: int = 500):
    """
    Добавляет оставшиеся заказы в конец листа.
    chunk_size — чтобы не упереться в лимиты на большие вставки.
    """
    new_rows = [build_row(payload) for payload in orders_by_id.values()]
    if not new_rows:
        return

    for start in range(0, len(new_rows), chunk_size):
        ws.append_rows(
            new_rows[start:start + chunk_size],
            value_input_option="USER_ENTERED",
        )

def remove_rows_with_status(ws, statuses: set):
    rows = ws.get_all_values()

    header = rows[0]

    filtered_rows = [header]

    for row in rows[1:]:
        if row[6] not in statuses:
            filtered_rows.append(row)

    ws.clear()
    ws.update(filtered_rows)


def sync_orders_to_sheet(
    *,
    ws,
    orders_by_id: Dict[int, Dict[str, Any]],
    data_range: str = "A2:G",
):
    """
    1) Обновить существующие строки по order_id (колонка F)
    2) Добавить новые строки (остаток dict)
    3) Удаляем ненужные строки по статусам
    """
    updates, remaining = build_updates_from_sheet(ws, orders_by_id, data_range=data_range)
    apply_updates(ws, updates)
    append_new_rows(ws, remaining)
    remove_rows_with_status(
        ws,
        {"Отменено", "Покупатель отказался", "Отменено до отгрузки"},
    )

    last_row = len(ws.get_all_values())

    ws.sort((ID_COL_DATE, 'asc'), range=f'A2:Z{last_row}')

def run(orders):
    for date in orders:
        ws = open_worksheet(f"{CURRENT_DIR}/credentials.json", date)
        setup_status_conditional_formatting(ws)
        sync_orders_to_sheet(ws=ws, orders_by_id=orders[date], data_range="A2:G")