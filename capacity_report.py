# -*- coding: utf-8 -*-
"""
Capacity License Report - Combined
Halyk Bank, DTPD - Self-Service BI

Два отчёта в одном файле, по всем пользователям, помесячно:

1. ИСТОРИЯ ПРИСВОЕНИЙ Analyzer-лицензий
   Источник: /qrs/license/analyzeraccesstype/full
   Поле даты: createdDate
   Что показывает: когда какому пользователю была присвоена лицензия

2. РЕАЛЬНОЕ ПОТРЕБЛЕНИЕ Capacity-минут
   Источник: /qrs/license/analyzertimeaccessusage/full
   Поле даты: useStartTime
   Что показывает: сколько минут реально использовал каждый пользователь
   (доступны только данные за последний расчётный период ~28 дней)
"""

import requests
from datetime import datetime
from pathlib import Path
import urllib3
import csv
import sys

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =====================================================================
# НАСТРОЙКИ
# =====================================================================

QLIK_SERVER = "fdata1a003.halykbank.nb"
QLIK_USER   = "UserDirectory=UNIVERSAL;UserId=00060961"

BASE_DIR    = Path(__file__).parent
CLIENT_CERT = str(BASE_DIR / "certificate" / "client.pem")
CLIENT_KEY  = str(BASE_DIR / "certificate" / "client_key.pem")
ROOT_CERT   = str(BASE_DIR / "certificate" / "root.pem")

XRFKEY = "0123456789abcdef"

# =====================================================================

HEADERS = {
    "X-Qlik-Xrfkey": XRFKEY,
    "X-Qlik-User": QLIK_USER,
    "Content-Type": "application/json"
}
PARAMS = {"xrfkey": XRFKEY}

QLIK_URL = f"https://{QLIK_SERVER}:4242/qrs"

MONTH_NAMES = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
    5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
    9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь"
}


def parse_qlik_time(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
    except (ValueError, AttributeError):
        return None


def qrs_get(endpoint):
    url = f"{QLIK_URL}/{endpoint}"
    try:
        response = requests.get(
            url,
            headers=HEADERS,
            params=PARAMS,
            cert=(CLIENT_CERT, CLIENT_KEY),
            verify=ROOT_CERT,
            timeout=120
        )
        return response
    except Exception as e:
        print(f"\n[ERROR] {e}")
        sys.exit(1)


# =====================================================================
# ОТЧЁТ 1: ИСТОРИЯ ПРИСВОЕНИЙ ЛИЦЕНЗИЙ
# =====================================================================

def fetch_assignments():
    print("[1/4] Получаем историю присвоений Analyzer-лицензий...")
    response = qrs_get("license/analyzeraccesstype/full")
    if response.status_code != 200:
        print(f"  Ошибка {response.status_code}: {response.text[:300]}")
        sys.exit(1)
    data = response.json()
    print(f"  Записей о присвоениях: {len(data)}")
    return data


def aggregate_assignments(records):
    """Группирует присвоения по месяцам."""
    print("[2/4] Группируем присвоения по месяцам...")
    
    monthly = {}  # {(year, month): [{"login":..., "name":..., "date":...}, ...]}
    
    for rec in records:
        user_obj = rec.get("user", {})
        if not isinstance(user_obj, dict):
            continue
        
        user_dir = user_obj.get("userDirectory", "")
        user_id = user_obj.get("userId", "")
        user_name = user_obj.get("name") or user_id
        login = f"{user_dir}\\{user_id}" if user_dir else user_id
        
        if not login or login == "\\":
            continue
        
        created = parse_qlik_time(rec.get("createdDate"))
        if not created:
            continue
        
        key = (created.year, created.month)
        if key not in monthly:
            monthly[key] = []
        
        monthly[key].append({
            "login": login,
            "name": user_name,
            "date": created
        })
    
    return monthly


# =====================================================================
# ОТЧЁТ 2: РЕАЛЬНОЕ ПОТРЕБЛЕНИЕ
# =====================================================================

def fetch_usage():
    print("[3/4] Получаем данные о реальном потреблении Capacity...")
    response = qrs_get("license/analyzertimeaccessusage/full")
    if response.status_code != 200:
        print(f"  Ошибка {response.status_code}: {response.text[:300]}")
        sys.exit(1)
    data = response.json()
    print(f"  Записей о сессиях: {len(data)}")
    return data


def aggregate_usage(records):
    """Группирует сессии по месяцам и пользователям."""
    print("[4/4] Группируем потребление по месяцам и пользователям...")
    
    monthly = {}  # {(year, month): {login: {name, minutes, sessions, last_login, first_login}}}
    
    for rec in records:
        user_obj = rec.get("user", {})
        if not isinstance(user_obj, dict):
            continue
        
        user_dir = user_obj.get("userDirectory", "")
        user_id = user_obj.get("userId", "")
        user_name = user_obj.get("name") or user_id
        login = f"{user_dir}\\{user_id}" if user_dir else user_id
        
        if not login or login == "\\":
            continue
        
        start = parse_qlik_time(rec.get("useStartTime"))
        stop = parse_qlik_time(rec.get("useStopTime"))
        
        if not start:
            continue
        
        # Минуты = разница, либо стандартные 6 если нет stop
        if stop and stop > start:
            minutes = (stop - start).total_seconds() / 60.0
        else:
            minutes = 6.0
        
        key = (start.year, start.month)
        if key not in monthly:
            monthly[key] = {}
        
        if login not in monthly[key]:
            monthly[key][login] = {
                "name": user_name,
                "minutes": 0.0,
                "sessions": 0,
                "first_login": None,
                "last_login": None
            }
        
        monthly[key][login]["minutes"] += minutes
        monthly[key][login]["sessions"] += 1
        
        if not monthly[key][login]["first_login"] or start < monthly[key][login]["first_login"]:
            monthly[key][login]["first_login"] = start
        if not monthly[key][login]["last_login"] or start > monthly[key][login]["last_login"]:
            monthly[key][login]["last_login"] = start
    
    return monthly


# =====================================================================
# ВЫВОД В КОНСОЛЬ
# =====================================================================

def print_assignments_summary(assignments):
    print("\n" + "=" * 110)
    print("  ОТЧЁТ 1: ИСТОРИЯ ПРИСВОЕНИЙ ANALYZER-ЛИЦЕНЗИЙ (помесячно)".center(110))
    print("=" * 110)
    print(f"  {'Месяц':<20} {'Новых присвоений':>20} {'Накопительно':>20}")
    print("-" * 110)
    
    sorted_keys = sorted(assignments.keys())
    cumulative = 0
    
    for key in sorted_keys:
        year, month = key
        count = len(assignments[key])
        cumulative += count
        month_label = f"{MONTH_NAMES[month]} {year}"
        print(f"  {month_label:<20} {count:>20} {cumulative:>20}")
    
    print("-" * 110)
    print(f"  {'ВСЕГО:':<20} {cumulative:>20}")
    print("=" * 110)


def print_usage_report(usage):
    print("\n" + "=" * 110)
    print("  ОТЧЁТ 2: РЕАЛЬНОЕ ПОТРЕБЛЕНИЕ CAPACITY (по каждому пользователю)".center(110))
    print("=" * 110)
    
    sorted_keys = sorted(usage.keys())
    
    for key in sorted_keys:
        year, month = key
        users = usage[key]
        month_name = MONTH_NAMES[month]
        
        print()
        print(f"  ----- {month_name} {year} -----")
        print(f"  {'Логин':<25} {'ФИО':<35} {'Минут':>10} {'Часов':>10} {'Сессий':>10} {'Посл.вход':>12}")
        print("  " + "-" * 105)
        
        sorted_users = sorted(users.items(), key=lambda x: -x[1]["minutes"])
        
        total_min = 0
        total_ses = 0
        
        for login, data in sorted_users:
            last = data["last_login"].strftime("%d.%m.%Y") if data["last_login"] else ""
            print(f"  {login:<25} {data['name'][:34]:<35} {data['minutes']:>10.1f} {data['minutes']/60:>10.2f} {data['sessions']:>10} {last:>12}")
            total_min += data["minutes"]
            total_ses += data["sessions"]
        
        print("  " + "-" * 105)
        print(f"  ИТОГО за {month_name}: {total_min:.1f} мин ({total_min/60:.2f} ч), {total_ses} сессий, {len(users)} активных пользователей")


def print_usage_summary(usage):
    print("\n" + "=" * 110)
    print("  СВОДКА ПО ПОТРЕБЛЕНИЮ".center(110))
    print("=" * 110)
    print(f"  {'Месяц':<20} {'Активных польз.':>17} {'Минут':>15} {'Часов':>15} {'Сессий':>15}")
    print("-" * 110)
    
    sorted_keys = sorted(usage.keys())
    grand_min = 0
    grand_ses = 0
    
    for key in sorted_keys:
        year, month = key
        users = usage[key]
        month_label = f"{MONTH_NAMES[month]} {year}"
        total_min = sum(u["minutes"] for u in users.values())
        total_ses = sum(u["sessions"] for u in users.values())
        grand_min += total_min
        grand_ses += total_ses
        print(f"  {month_label:<20} {len(users):>17} {total_min:>15.1f} {total_min/60:>15.2f} {total_ses:>15}")
    
    print("-" * 110)
    print(f"  {'ВСЕГО:':<37} {grand_min:>15.1f} {grand_min/60:>15.2f} {grand_ses:>15}")
    print("=" * 110)


# =====================================================================
# СОХРАНЕНИЕ В CSV (один файл, две секции)
# =====================================================================

def save_combined_csv(assignments, usage):
    filename = f"capacity_report_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    filepath = BASE_DIR / filename
    
    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f, delimiter=";")
        
        # ===== СЕКЦИЯ 1: Присвоения =====
        writer.writerow(["===== ОТЧЁТ 1: ИСТОРИЯ ПРИСВОЕНИЙ ANALYZER-ЛИЦЕНЗИЙ ====="])
        writer.writerow([])
        writer.writerow(["Месяц", "Год", "Логин", "ФИО", "Дата присвоения"])
        
        for key in sorted(assignments.keys()):
            year, month = key
            month_name = MONTH_NAMES[month]
            for entry in sorted(assignments[key], key=lambda x: x["date"]):
                writer.writerow([
                    month_name,
                    year,
                    entry["login"],
                    entry["name"],
                    entry["date"].strftime("%d.%m.%Y %H:%M")
                ])
        
        # Пустые строки между секциями
        writer.writerow([])
        writer.writerow([])
        writer.writerow([])
        
        # ===== СЕКЦИЯ 2: Потребление =====
        writer.writerow(["===== ОТЧЁТ 2: РЕАЛЬНОЕ ПОТРЕБЛЕНИЕ CAPACITY-МИНУТ ====="])
        writer.writerow([])
        writer.writerow([
            "Месяц", "Год", "Логин", "ФИО",
            "Минут", "Часов", "Сессий",
            "Первый вход", "Последний вход"
        ])
        
        for key in sorted(usage.keys()):
            year, month = key
            month_name = MONTH_NAMES[month]
            sorted_users = sorted(usage[key].items(), key=lambda x: -x[1]["minutes"])
            
            for login, data in sorted_users:
                first = data["first_login"].strftime("%d.%m.%Y %H:%M") if data["first_login"] else ""
                last = data["last_login"].strftime("%d.%m.%Y %H:%M") if data["last_login"] else ""
                writer.writerow([
                    month_name,
                    year,
                    login,
                    data["name"],
                    round(data["minutes"], 1),
                    round(data["minutes"] / 60, 2),
                    data["sessions"],
                    first,
                    last
                ])
    
    print(f"\nCSV сохранён: {filepath}")
    return filepath


# =====================================================================
# MAIN
# =====================================================================

def main():
    print("=" * 110)
    print(f"  Capacity License Report  |  {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    print(f"  Сервер: {QLIK_SERVER}")
    print(f"  Пользователь: {QLIK_USER}")
    print("=" * 110)
    print()
    
    # Отчёт 1: присвоения
    assignment_records = fetch_assignments()
    assignments = aggregate_assignments(assignment_records)
    
    # Отчёт 2: потребление
    usage_records = fetch_usage()
    usage = aggregate_usage(usage_records)
    
    # Вывод
    print_assignments_summary(assignments)
    print_usage_report(usage)
    print_usage_summary(usage)
    
    # Сохранение
    save_combined_csv(assignments, usage)
    
    print("\nГотово.")


if __name__ == "__main__":
    main()
