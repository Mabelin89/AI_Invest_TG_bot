from datetime import datetime, timedelta
import pandas as pd
import requests
from io import StringIO
import os

# Константа для директории
HISTORICAL_DATA_DIR = "historical_data"


def fetch_moex_candles(ticker, start_date, end_date, timeframe):
    timeframe_map = {
        '1m': 1,      # 1 минута
        '10m': 10,    # 10 минут
        '1h': 60,     # 1 час
        'daily': 24,  # 1 день
        'weekly': 7,  # 1 неделя
        'monthly': 31,# 1 месяц
        'quarterly': 4  # 1 квартал
    }
    interval = timeframe_map.get(timeframe.lower(), 24)
    url = f"https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/{ticker}/candles.csv"
    params = {
        "interval": interval,
        "from": start_date,
        "till": end_date,
        "iss.reverse": "false"
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    try:
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 200:
            csv_text = response.content.decode('utf-8')
            df = pd.read_csv(StringIO(csv_text), sep=';', skiprows=2)
            if df.empty:
                print(f"Данные для {ticker} ({timeframe}) пусты")
                return None
            print(f"Успешно получены данные для {ticker} ({timeframe}): {len(df)} строк")
            return df
        else:
            print(f"Ошибка запроса для {ticker} ({timeframe}) за {start_date} - {end_date}: статус {response.status_code}, текст: {response.text[:100]}")
            return None
    except Exception as e:
        print(f"Ошибка при запросе данных MOEX для {ticker} ({timeframe}): {str(e)}")
        return None


def aggregate_to_4h(data):
    try:
        data['date'] = pd.to_datetime(data['date'])
        data.set_index('date', inplace=True)
        agg_dict = {
            'high': 'max',
            'low': 'min',
            'open': 'first',
            'close': 'last',
            'volume': 'sum'
        }
        data_4h = data.resample('4h').agg(agg_dict).dropna()
        data_4h.reset_index(inplace=True)
        print(f"Агрегированы 4-часовые свечи: {len(data_4h)} строк")
        return data_4h
    except Exception as e:
        print(f"Ошибка при агрегации 4-часовых свечей: {str(e)}")
        return pd.DataFrame()


def is_data_outdated(file_path, timeframe, period_years):
    if not os.path.exists(file_path):
        return True

    mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
    now = datetime.now()

    timeframe_thresholds = {
        '1m': 1 * 60,
        '10m': 10 * 60,
        '1h': 1 * 3600,
        '4h': 4 * 3600,
        'daily': 1 * 24 * 3600,
        'weekly': 7 * 24 * 3600,
        'monthly': 31 * 24 * 3600,
        'quarterly': 90 * 24 * 3600
    }
    threshold = timeframe_thresholds.get(timeframe.lower(), 24 * 3600)
    return (now - mod_time).total_seconds() >= threshold


def get_historical_data(ticker, timeframe, period_years):
    if not os.path.exists(HISTORICAL_DATA_DIR):
        os.makedirs(HISTORICAL_DATA_DIR)

    now = datetime.now()
    if timeframe.lower() in ['1m', '10m', '1h', '4h']:
        date_str = now.strftime('%Y%m%d_%H%M%S')
    else:
        date_str = now.strftime('%Y%m%d')
    filename = f"{ticker.upper()}_{timeframe.upper()}_{period_years}Y_{date_str}.csv"
    file_path = os.path.join(HISTORICAL_DATA_DIR, filename)

    if os.path.exists(file_path) and not is_data_outdated(file_path, timeframe, period_years):
        print(f"Данные в '{file_path}' актуальны, загрузка из файла")
        return pd.read_csv(file_path)

    end_date = now
    start_date = end_date - timedelta(days=period_years * 365)
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    if timeframe.lower() == '4h':
        data = fetch_moex_candles(ticker, start_date_str, end_date_str, '1h')
        if data is None or data.empty:
            print(f"Не удалось получить 1-часовые данные для {ticker} для агрегации в 4h")
            return pd.DataFrame()
        data = data[['begin', 'high', 'low', 'open', 'close', 'volume']]
        data.rename(columns={'begin': 'date'}, inplace=True)
        data = aggregate_to_4h(data)
    else:
        data = fetch_moex_candles(ticker, start_date_str, end_date_str, timeframe)
        if data is None or data.empty:
            print(f"Не удалось получить данные для {ticker} ({timeframe}, {period_years} лет)")
            return pd.DataFrame()
        data = data[['begin', 'high', 'low', 'open', 'close', 'volume']]
        data.rename(columns={'begin': 'date'}, inplace=True)

    return data


if __name__ == "__main__":
    df = get_historical_data("SBER", "4h", 1)
    print(df.head())