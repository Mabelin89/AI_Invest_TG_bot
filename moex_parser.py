from datetime import datetime, timedelta
import pandas as pd
import requests
from io import StringIO
import os

# Константа для директории
HISTORICAL_DATA_DIR = "historical_data"


def fetch_moex_candles(ticker, start_date, end_date, timeframe):
    timeframe_map = {
        '1m': 1,  # 1 минута
        '10m': 10,  # 10 минут
        '1h': 60,  # 1 час
        'daily': 24,  # 1 день
        'weekly': 7,  # 1 неделя
        'monthly': 31,  # 1 месяц
        'quarterly': 4  # 1 квартал
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


def is_data_outdated(file_path, timeframe, period_years):
    if not os.path.exists(file_path):
        return True

    mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
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
    # Создание директории, если её нет
    if not os.path.exists(HISTORICAL_DATA_DIR):
        os.makedirs(HISTORICAL_DATA_DIR)

    now = datetime.now()
    if timeframe.lower() in ['1m', '10m', '1h']:
        date_str = now.strftime('%Y%m%d_%H%M%S')
    else:
        date_str = now.strftime('%Y%m%d')
    filename = f"{ticker.upper()}_{timeframe.upper()}_{period_years}Y_{date_str}.csv"
    file_path = os.path.join(HISTORICAL_DATA_DIR, filename)

    # Проверка актуальности данных
    if os.path.exists(file_path) and not is_data_outdated(file_path, timeframe, period_years):
        print(f"Данные в '{file_path}' актуальны, загрузка из файла")
        return pd.read_csv(file_path)

    # Если данных нет или они устарели, запрашиваем их
    end_date = now
    start_date = end_date - timedelta(days=period_years * 365)
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    data = fetch_moex_candles(ticker, start_date_str, end_date_str, timeframe)
    if data is None or data.empty:
        print(f"Не удалось получить данные для {ticker}")
        return pd.DataFrame()

    # Форматирование данных
    data = data[['begin', 'open', 'high', 'low', 'close', 'volume']]

    # Не сохраняем файл здесь, возвращаем DataFrame для дальнейшей обработки
    return data


if __name__ == "__main__":
    # Тестовый запуск
    df = get_historical_data("SBER", "daily", 2)
    print(df.head())