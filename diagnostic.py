# -*- coding: utf-8 -*-
"""
Расширенная диагностика - ищем где лежат данные за март.
Проверяет несколько endpoint'ов и показывает диапазон дат.
"""

import requests
from datetime import datetime
from pathlib import Path
import urllib3
import json

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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


def parse_qlik_time(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
    except (ValueError, AttributeError):
        return None


def try_endpoint(endpoint, label):
    print("\n" + "=" * 80)
    print(f"ENDPOINT: /qrs/{endpoint}")
    print(f"Описание: {label}")
    print("=" * 80)
    
    url = f"https://{QLIK_SERVER}:4242/qrs/{endpoint}"
    try:
        response = requests.get(
            url, headers=HEADERS, params=PARAMS,
            cert=(CLIENT_CERT, CLIENT_KEY),
            verify=ROOT_CERT, timeout=120
        )
    except Exception as e:
        print(f"[ОШИБКА] {e}")
        return None
    
    if response.status_code != 200:
        print(f"[HTTP {response.status_code}]")
        print(f"   {response.text[:300]}")
        return None
    
    data = response.json()
    
    if isinstance(data, int):
        print(f"Ответ - число: {data}")
        return None
    
    if not isinstance(data, list):
        print(f"Ответ не массив, тип: {type(data).__name__}")
        if isinstance(data, dict):
            print(f"Ключи: {list(data.keys())}")
        return None
    
    print(f"Записей: {len(data)}")
    
    if not data:
        return data
    
    # Анализ дат - ищем все возможные поля с датой
    date_fields = ["useStartTime", "useStopTime", "createdDate", "modifiedDate", 
                   "latestActivity", "loggedDate", "used", "usedTime", "logTimeStamp"]
    
    found_field = None
    for field in date_fields:
        if field in data[0]:
            found_field = field
            break
    
    if found_field:
        print(f"Поле даты: {found_field}")
        dates = [parse_qlik_time(r.get(found_field)) for r in data]
        dates = [d for d in dates if d is not None]
        
        if dates:
            min_d = min(dates)
            max_d = max(dates)
            print(f"Диапазон: {min_d.strftime('%d.%m.%Y %H:%M')} -- {max_d.strftime('%d.%m.%Y %H:%M')}")
            
            # Группируем по месяцам
            by_month = {}
            for d in dates:
                key = (d.year, d.month)
                by_month[key] = by_month.get(key, 0) + 1
            
            print(f"По месяцам:")
            for key in sorted(by_month.keys()):
                y, m = key
                print(f"   {m:02d}.{y}: {by_month[key]} записей")
    else:
        print(f"Поля верхнего уровня: {list(data[0].keys())[:10]}")
    
    return data


def try_count(endpoint):
    """Получает количество записей."""
    url = f"https://{QLIK_SERVER}:4242/qrs/{endpoint}/count"
    try:
        response = requests.get(
            url, headers=HEADERS, params=PARAMS,
            cert=(CLIENT_CERT, CLIENT_KEY),
            verify=ROOT_CERT, timeout=60
        )
        if response.status_code == 200:
            return response.json()
    except Exception:
        pass
    return None


def main():
    print("=" * 80)
    print(f"  Расширенная диагностика  |  {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    print("=" * 80)
    
    # Список всех потенциальных endpoint'ов
    endpoints = [
        ("license/analyzertimeaccessusage/full", "Analyzer Capacity (текущий)"),
        ("license/loginaccessusage/full", "Login Access (Token)"),
        ("license/professionalaccesstype/full", "Professional license"),
        ("license/analyzeraccesstype/full", "Analyzer license"),
        ("license/useraccesstype/full", "User Access"),
        ("license/useraccessusage/full", "User Access Usage"),
        ("license/loginaccesstype/full", "Login Access Type"),
    ]
    
    all_results = {}
    
    for ep, label in endpoints:
        # Сначала count - чтобы быстро увидеть, есть ли данные
        count = try_count(ep.replace("/full", ""))
        if count is not None:
            print(f"\n[COUNT] /qrs/{ep.replace('/full', '')}/count = {count}")
        
        data = try_endpoint(ep, label)
        if data is not None:
            all_results[ep] = len(data) if isinstance(data, list) else 0
    
    # Также попробуем app objects - там может быть история по приложениям
    print("\n" + "=" * 80)
    print("ПРОВЕРКА: События в приложении 'License Monitor'")
    print("=" * 80)
    
    # Список приложений
    url = f"https://{QLIK_SERVER}:4242/qrs/app"
    try:
        response = requests.get(
            url, headers=HEADERS, params=PARAMS,
            cert=(CLIENT_CERT, CLIENT_KEY),
            verify=ROOT_CERT, timeout=60
        )
        if response.status_code == 200:
            apps = response.json()
            license_apps = [a for a in apps if 'license' in a.get('name', '').lower() or 
                            'monitor' in a.get('name', '').lower()]
            print(f"\nПриложения мониторинга:")
            for a in license_apps:
                print(f"  - {a.get('name')} (id: {a.get('id')})")
    except Exception as e:
        print(f"[ОШИБКА списка приложений] {e}")
    
    print("\n" + "=" * 80)
    print("ИТОГОВАЯ СВОДКА")
    print("=" * 80)
    for ep, count in all_results.items():
        print(f"  /qrs/{ep}: {count} записей")
    
    print("\nГотово.")


if __name__ == "__main__":
    main()
