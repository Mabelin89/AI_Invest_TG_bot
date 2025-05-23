import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import os
from moex_parser import get_historical_data, HISTORICAL_DATA_DIR

def fetch_yfinance_data(ticker, display_name, start_date, end_date, timeframe):
    """
    Получает исторические данные через yfinance, возвращая только дату, тикер и цену закрытия.

    Args:
        ticker (str): Тикер для Yahoo Finance.
        display_name (str): Отображаемое имя тикера.
        start_date (datetime): Начальная дата.
        end_date (datetime): Конечная дата.
        timeframe (str): Таймфрейм ('1m', '5m', '15m', '30m', '60m', '1d', '1wk', '1mo').

    Returns:
        pd.DataFrame: Данные с колонками ['date', 'ticker', 'close'] или пустой DataFrame при ошибке.
    """
    try:
        print(f"Получение данных через yfinance для {display_name} ({ticker})...")
        data = yf.download(ticker, start=start_date, end=end_date, interval=timeframe)
        if data.empty:
            print(f"Данные для {display_name} ({ticker}) не найдены.")
            return pd.DataFrame()

        # Форматирование данных
        data = data[['Close']]
        data.reset_index(inplace=True)
        data.rename(columns={'Date': 'date', 'Close': 'close'}, inplace=True)
        data['ticker'] = display_name
        data['date'] = pd.to_datetime(data['date']).dt.strftime('%Y-%m-%d')

        # Обработка NaN
        data['close'] = data['close'].astype(float)

        # Проверка дубликатов
        duplicates = data.duplicated(subset=['date', 'ticker']).sum()
        if duplicates > 0:
            print(f"Найдено {duplicates} дубликатов для {display_name}. Удаление дубликатов...")
            data = data.drop_duplicates(subset=['date', 'ticker'], keep='last')

        print(f"Успешно получены данные для {display_name}: {len(data)} строк")
        return data[['date', 'ticker', 'close']]

    except Exception as e:
        print(f"Ошибка при получении данных для {display_name} ({ticker}): {str(e)}")
        return pd.DataFrame()

def fetch_and_combine_all_data(years=5, timeframe="daily"):
    """
    Парсит данные для тикеров (MOEX и yfinance) и объединяет в CSV с колонками date и тикеры (цены закрытия).

    Args:
        years (int): Количество лет для данных.
        timeframe (str): Таймфрейм ('1m', '10m', '1h', 'daily', 'weekly', 'monthly', 'quarterly' для MOEX;
                        '1m', '5m', '15m', '30m', '60m', '1d', '1wk', '1mo' для yfinance).

    Returns:
        None: Сохраняет объединённый CSV в папке HISTORICAL_DATA_DIR.
    """
    # Тикеры для MOEX
    moex_tickers_config = [
        {"ticker": "IMOEX", "market": "index", "board": "MICEXINDEXCF", "display_name": "IMOEX"},
        {"ticker": "RTSI", "market": "index", "board": "RTSI", "display_name": "RTSI"},
        {"ticker": "MOEXOG", "market": "index", "board": "MICEXINDEXCF", "display_name": "MOEXOG"},
        {"ticker": "MOEXFN", "market": "index", "board": "MICEXINDEXCF", "display_name": "MOEXFN"},
        {"ticker": "MOEXMM", "market": "index", "board": "MICEXINDEXCF", "display_name": "MOEXMM"},
        {"ticker": "MOEXCN", "market": "index", "board": "MICEXINDEXCF", "display_name": "MOEXCN"},
        {"ticker": "USD000UTSTOM", "market": "currency", "board": "CETS", "display_name": "USD/RUB"},
        {"ticker": "EUR_RUB__TOM", "market": "currency", "board": "CETS", "display_name": "EUR/RUB"}
    ]

    # Тикеры для yfinance (из yahoo_finance_parser.py)
    yfinance_tickers_config = [
        {"ticker": "BZ=F", "display_name": "BRN"},
        {"ticker": "CL=F", "display_name": "CL"},
        {"ticker": "NG=F", "display_name": "NG"},
        {"ticker": "GC=F", "display_name": "XAU/USD"},
        {"ticker": "PA=F", "display_name": "XPD/USD"},
        {"ticker": "^GSPC", "display_name": "SPX"},
        {"ticker": "^FTSE", "display_name": "UKX"},
        {"ticker": "000001.SS", "display_name": "SSEC"}
    ]

    # Установка периода
    end_date = datetime.now()
    start_date = end_date - timedelta(days=years * 365)

    # Список для хранения данных
    all_data = []

    # Парсинг данных MOEX
    for config in moex_tickers_config:
        ticker = config["ticker"]
        market = config["market"]
        board = config["board"]
        display_name = config["display_name"]

        print(f"Получение данных MOEX для {display_name} ({ticker})...")
        try:
            df = get_historical_data(ticker, timeframe, years, market=market, board=board)
            if df is None or df.empty:
                print(f"Не удалось получить данные для {display_name} ({ticker})")
                continue

            df['ticker'] = display_name
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            df['close'] = df['close'].astype(float)

            # Проверка дубликатов
            duplicates = df.duplicated(subset=['date', 'ticker']).sum()
            if duplicates > 0:
                print(f"Найдено {duplicates} дубликатов для {display_name}. Удаление дубликатов...")
                df = df.drop_duplicates(subset=['date', 'ticker'], keep='last')

            all_data.append(df[['date', 'ticker', 'close']])
            print(f"Успешно получены данные MOEX для {display_name}: {len(df)} строк")

        except Exception as e:
            print(f"Ошибка при получении данных MOEX для {display_name} ({ticker}): {str(e)}")

    # Парсинг данных yfinance
    for config in yfinance_tickers_config:
        ticker = config["ticker"]
        display_name = config["display_name"]

        df = fetch_yfinance_data(ticker, display_name, start_date, end_date, timeframe.replace("daily", "1d"))
        if not df.empty:
            all_data.append(df)

    # Объединение данных
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True, sort=False)
        combined_df['date'] = pd.to_datetime(combined_df['date']).dt.strftime('%Y-%m-%d')
        combined_df['close'] = combined_df['close'].astype(float)

        # Проверка дубликатов в объединённом DataFrame
        duplicates = combined_df.duplicated(subset=['date', 'ticker']).sum()
        if duplicates > 0:
            print(f"Найдено {duplicates} дубликатов в объединённых данных:")
            print(combined_df[combined_df.duplicated(subset=['date', 'ticker'], keep=False)][['date', 'ticker', 'close']])
            print("Удаление дубликатов...")
            combined_df = combined_df.drop_duplicates(subset=['date', 'ticker'], keep='last')

        # Преобразование в широкий формат
        pivoted_df = combined_df.pivot(index='date', columns='ticker', values='close')
        pivoted_df.reset_index(inplace=True)

        # Убедимся, что все тикеры присутствуют, даже если данных нет
        expected_tickers = [config["display_name"] for config in moex_tickers_config + yfinance_tickers_config]
        for ticker in expected_tickers:
            if ticker not in pivoted_df.columns:
                pivoted_df[ticker] = float('nan')

        # Упорядочиваем столбцы
        pivoted_df = pivoted_df[['date'] + expected_tickers]

        # Сортировка по дате
        pivoted_df.sort_values(by='date', inplace=True)

        # Удаление дубликатов дат
        pivoted_df.drop_duplicates(subset=['date'], keep='last', inplace=True)

        # Сохранение в CSV
        output_dir = HISTORICAL_DATA_DIR
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        now = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = os.path.join(output_dir, f"combined_all_data_{timeframe}_{years}Y_{now}.csv")
        pivoted_df.to_csv(output_file, index=False, encoding='utf-8')

        print(f"Объединённые данные сохранены в {output_file}: {len(pivoted_df)} строк, {len(pivoted_df.columns)} столбцов")
    else:
        print("Не удалось получить данные ни для одного тикера.")

if __name__ == "__main__":
    # Пример: 5 лет, дневной таймфрейм
    fetch_and_combine_all_data(years=5, timeframe="daily")