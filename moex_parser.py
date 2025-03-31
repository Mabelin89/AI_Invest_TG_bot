from datetime import datetime, timedelta
import pandas as pd
import requests
from io import StringIO
import os

def fetch_moex_candles(ticker, start_date, end_date, timeframe):
    timeframe_map = {
        '1m': 1,     # 1 минута
        '10m': 10,   # 10 минут
        '1h': 60,    # 1 час
        'daily': 24, # 1 день
        'weekly': 7, # 1 неделя
        'monthly': 31, # 1 месяц
        'quarterly': 4 # 1 квартал
    }
    interval = timeframe_map.get(timeframe.lower(), 24)
    url = f"https://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker}/candles.csv?interval={interval}&from={start_date}&till={end_date}"
    response = requests.get(url)
    if response.status_code == 200:
        csv_text = response.content.decode('utf-8')
        return pd.read_csv(StringIO(csv_text), sep=';', skiprows=2)
    else:
        print(f"Ошибка запроса для периода {start_date} - {end_date}: {response.status_code}")
        return None

def is_data_outdated(filename, timeframe, period_years):
    if not os.path.exists(filename):
        return True

    mod_time = datetime.fromtimestamp(os.path.getmtime(filename))
    now = datetime.now()

    timeframe_thresholds = {
        'daily': 1 * 24 * 3600,
        'weekly': 7 * 24 * 3600,
        'monthly': 31 * 24 * 3600,
        'quarterly': 90 * 24 * 3600,
        '1h': 1 * 3600,
        '10m': 10 * 60,
        '1m': 1 * 60
    }
    threshold = timeframe_thresholds.get(timeframe.lower(), 24 * 3600)
    return (now - mod_time).total_seconds() >= threshold

def get_historical_data(ticker, timeframe, period_years):
    now = datetime.now()
    if timeframe.lower() in ['1m', '10m', '1h']:
        date_str = now.strftime('%Y%m%d_%H%M%S')
    else:
        date_str = now.strftime('%Y%m%d')
    filename = f"{ticker.upper()}_{timeframe.upper()}_{period_years}Y_{date_str}.csv"

    if os.path.exists(filename) and not is_data_outdated(filename, timeframe, period_years):
        print(f"Данные в '{filename}' актуальны, загрузка из файла")
        return pd.read_csv(filename)

    if os.path.exists(filename):
        os.remove(filename)
        print(f"Удален устаревший файл '{filename}'")

    start = now - timedelta(days=period_years * 365)
    all_data = []

    if timeframe.lower() in ['1m', '10m']:
        step_days = 10
    elif timeframe.lower() == '1h':
        step_days = 30
    else:
        step_days = 365

    current_start = start
    while current_start < now:
        period_end = min(current_start + timedelta(days=step_days), now)
        data = fetch_moex_candles(ticker, current_start.strftime('%Y-%m-%d'), period_end.strftime('%Y-%m-%d'), timeframe)
        if data is not None:
            all_data.append(data)
        current_start = period_end + timedelta(days=1)

    if all_data:
        final_data = pd.concat(all_data, ignore_index=True)
        final_data = final_data.sort_values('begin').drop_duplicates(subset=['begin'])
        final_data.to_csv(filename, index=False)
        print(f"Данные успешно сохранены в '{filename}'")
        return final_data
    else:
        print("Не удалось получить данные")
        return pd.DataFrame()