import json
import ssl
from pathlib import Path
from typing import List, Optional

import pandas as pd
import websocket


# ======================================================
# НАСТРОЙКИ ДЛЯ QLIK SENSE ENTERPRISE
# ======================================================

QLIK_SERVER = "fdata1a003.halykbank.nb"
QLIK_USER = "UserDirectory=UNIVERSAL;UserId=00060961"
# Сертификаты
BASE_DIR    = Path(__file__).parent
CLIENT_CERT = str(BASE_DIR/"certificate"/"client.pem")
ROOT_CERT   = str(BASE_DIR/"certificate"/"root.pem")

# Excel
EXCEL_PATH = r"C:\Users\00060961\Desktop\master_measures_api\master_measures.xlsx"
SHEET_NAME = "Measures"

# APP_ID (тот, что в URL)
TARGET_APP_ID = "af1dae2a-4097-4fd5-a752-f37c7ce593f2"


# ======================================================
# КЛАСС "МЕРА"
# ======================================================

class Measure:
    def __init__(self, measure_id: str, title_ru: str, expression: str,
                 label_ru: Optional[str] = None, fmt: Optional[str] = None):
        self.measure_id = measure_id
        self.title_ru   = title_ru
        self.expression = expression
        self.label_ru   = label_ru or title_ru
        self.fmt        = fmt


# ======================================================
# ЧТЕНИЕ EXCEL
# ======================================================

def read_measures_from_excel(file_path: Path, sheet_name: str) -> List[Measure]:
    df = pd.read_excel(file_path, sheet_name=sheet_name).fillna("")

    required = ["measure_id", "title_ru", "expression"]
    for col in required:
        if col not in df.columns:
            raise RuntimeError(f"Нет обязательной колонки: {col}")

    measures: List[Measure] = []
    for _, row in df.iterrows():
        if not str(row["measure_id"]).strip():
            continue

        measures.append(
            Measure(
                measure_id=str(row["measure_id"]).strip(),
                title_ru=str(row["title_ru"]).strip(),
                expression=str(row["expression"]).strip(),
                label_ru=str(row.get("label_ru", "")).strip(),
                fmt=str(row.get("format", "")).strip(),
            )
        )

    return measures


# ======================================================
# КЛИЕНТ QLIK ENGINE API
# ======================================================

class QlikEngineClient:

    def __init__(self, app_id: str):
        """Подключаемся напрямую к приложению по APP_ID."""

        self.app_id = app_id
        self._id = 1

        # SSL
        ssl_ctx = ssl.create_default_context(cafile=ROOT_CERT)
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE
        ssl_ctx.load_cert_chain(certfile=CLIENT_CERT)

        url = f"wss://{QLIK_SERVER}:4747/app/{app_id}"
        print("\nПодключаемся к приложению:\n", url)

        self.ws = websocket.create_connection(
            url,
            sslopt={"context": ssl_ctx},
            header=[f"X-Qlik-User: {QLIK_USER}"]
        )

        print("✔ Соединение установлено.")

        resp = self._send("OpenDoc", -1, [app_id])
        self.app_handle = resp["result"]["qReturn"]["qHandle"]
        print(f"✔ Приложение открыто. HANDLE = {self.app_handle}\n")

    # ------------------------------------------
    # Универсальная отправка команды
    # ------------------------------------------
    def _send(self, method: str, handle: int, params=None):
        if params is None:
            params = []

        msg_id = self._id
        self._id += 1

        msg = {
            "jsonrpc": "2.0",
            "id": msg_id,
            "handle": handle,
            "method": method,
            "params": params
        }

        self.ws.send(json.dumps(msg))

        while True:
            resp = json.loads(self.ws.recv())
            if resp.get("id") == msg_id:
                if "error" in resp:
                    raise RuntimeError(resp["error"])
                return resp

    # ------------------------------------------
    # MeasureList: реальные мастер-меры
    # ------------------------------------------
    def get_real_master_measures(self):
        """Возвращает список мастер-мер, как их видит Qlik (MeasureList)."""

        session_def = {
            "qInfo": {"qType": "MeasureList", "qId": "MeasureList"},
            "qMeasureListDef": {
                "qType": "measure",
                "qData": {"title": "/qMetaDef/title"}
            }
        }

        resp = self._send("CreateSessionObject", self.app_handle, [session_def])
        list_handle = resp["result"]["qReturn"]["qHandle"]

        layout = self._send("GetLayout", list_handle, [])
        items = layout["result"]["qLayout"]["qMeasureList"]["qItems"]

        measures = []
        for item in items:
            measures.append({
                "title": item["qMeta"]["title"],
                "id": item["qInfo"]["qId"],
            })

        return measures

    def print_master_measures(self, msg: str):
        items = self.get_real_master_measures()
        print(f"\n{msg}")
        if not items:
            print("  (список пуст)")
            return

        for item in items:
            print(f"  - {item['title']} (id={item['id']})")

    # ------------------------------------------
    # УДАЛЕНИЕ ВСЕХ МАСТЕР-МЕР
    # ------------------------------------------
    def delete_all_measures(self):
        items = self.get_real_master_measures()
        print(f"Найдено существующих мер: {len(items)}")

        if not items:
            print("✔ Старых мер нет.\n")
            return

        print("\nУдаляем старые мастер-меры:")
        for item in items:
            mid = item["id"]
            title = item["title"]

            self._send("DestroyMeasure", self.app_handle, [mid])
            print(f" ✖ Удалена: {title} ({mid})")

        print("✔ Все старые меры удалены.\n")

    # ------------------------------------------
    # СОЗДАНИЕ МАСТЕР-МЕРЫ
    # ------------------------------------------
    def create_measure(self, m: Measure):
        """Создаёт мастер-меру в библиотеке Master Items."""

        params = [{
            "qInfo": {
                "qType": "measure",
                "qId": m.measure_id
            },
            "qMetaDef": {
                "title": m.title_ru,
                "description": m.label_ru,
                "tags": ["master"]
            },
            "qMeasure": {
                "qLabel": m.label_ru,
                "qDef": m.expression,
                "qLabelExpression": f"='{m.title_ru}'",
                "qExpressions": [m.expression],
                "qGrouping": "N",
                "qActiveExpression": 0
            }
        }]

        # ВАЖНО: здесь CreateMeasure, а не CreateGenericObject
        resp = self._send("CreateMeasure", self.app_handle, params)
        qid = resp["result"]["qInfo"]["qId"]

        print(f"✔ Создана MASTER-мера: {m.title_ru} (qId={qid})")

    # ------------------------------------------
    # Сохранение
    # ------------------------------------------
    def save(self):
        self._send("DoSave", self.app_handle, [])
        print("\n✔ Приложение сохранено.\n")

    def close(self):
        self.ws.close()


# ======================================================
# MAIN
# ======================================================

def main():

    print("\n========== ЗАГРУЗКА МАСТЕР-МЕР ==========\n")

    # 1. Читаем Excel
    measures = read_measures_from_excel(Path(EXCEL_PATH), SHEET_NAME)
    print(f"Найдено мер в Excel: {len(measures)}\n")

    # 2. Подключаемся к приложению
    engine = QlikEngineClient(TARGET_APP_ID)

    # 2.1. Печатаем, что есть до удаления
    engine.print_master_measures("Мастер-меры ДО удаления:")

    # 3. Удаляем все старые
    engine.delete_all_measures()

    # 3.1. Проверим, что стало пусто
    engine.print_master_measures("Мастер-меры ПОСЛЕ удаления:")

    # 4. Создаём новые
    print("\nСоздаём новые мастер-меры...\n")
    for m in measures:
        engine.create_measure(m)

    # 4.1. Ещё раз читаем список из MeasureList
    engine.print_master_measures("Мастер-меры ПОСЛЕ создания:")

    # 5. Сохраняем
    engine.save()

    # 6. Закрываем
    engine.close()

    print("========== ГОТОВО: ВСЕ МЕРЫ ОБНОВЛЕНЫ ==========\n")


if __name__ == "__main__":
    main()
