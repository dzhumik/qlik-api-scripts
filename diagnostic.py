# -*- coding: utf-8 -*-
"""
Диагностика - смотрим, как выглядит реальный ответ от Qlik QRS API.
Запусти этот скрипт, скинь вывод.
"""

import requests
from pathlib import Path
import urllib3
import json

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === НАСТРОЙКИ (те же, что в основном скрипте) ===
QLIK_SERVER = "fdata1a003.halykbank.nb"
QLIK_USER   = "UserDirectory=UNIVERSAL;UserId=00060961"

BASE_DIR    = Path(__file__).parent
CLIENT_CERT = str(BASE_DIR / "certificate" / "client.pem")
CLIENT_KEY  = str(BASE_DIR / "certificate" / "client_key.pem")
ROOT_CERT   = str(BASE_DIR / "certificate" / "root.pem")

XRFKEY = "0123456789abcdef"
HEADERS = {
    "X-Qlik-Xrfkey": XRFKEY,
    "X-Qlik-User": QLIK_USER,
    "Content-Type": "application/json"
}
PARAMS = {"xrfkey": XRFKEY}

# === ЗАПРОС ===
url = f"https://{QLIK_SERVER}:4242/qrs/license/analyzertimeaccessusage/full"

response = requests.get(
    url,
    headers=HEADERS,
    params=PARAMS,
    cert=(CLIENT_CERT, CLIENT_KEY),
    verify=ROOT_CERT,
    timeout=60
)

data = response.json()

print("=" * 80)
print(f"ВСЕГО ЗАПИСЕЙ: {len(data)}")
print("=" * 80)

if data:
    print("\n--- ПЕРВАЯ ЗАПИСЬ (полный JSON) ---")
    print(json.dumps(data[0], indent=2, ensure_ascii=False))
    
    print("\n--- ВТОРАЯ ЗАПИСЬ ---")
    if len(data) > 1:
        print(json.dumps(data[1], indent=2, ensure_ascii=False))
    
    print("\n--- ТРЕТЬЯ ЗАПИСЬ ---")
    if len(data) > 2:
        print(json.dumps(data[2], indent=2, ensure_ascii=False))
    
    print("\n--- КЛЮЧИ ВЕРХНЕГО УРОВНЯ ---")
    print(list(data[0].keys()))
    
    # Сохраним в файл для удобства
    out_file = BASE_DIR / "diagnostic_dump.json"
    with open(out_file, "w", encoding="utf-8") as f:
        # Первые 5 записей
        json.dump(data[:5], f, indent=2, ensure_ascii=False)
    print(f"\n[OK] Первые 5 записей сохранены в: {out_file}")

print("\nГотово.")
