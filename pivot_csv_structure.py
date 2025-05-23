import pandas as pd
import os

def pivot_csv_structure(input_file, output_file):
    """
    Преобразует CSV-файл в формат с датами и тикерами, где значения — цены закрытия.

    Args:
        input_file (str): Путь к исходному CSV-файлу.
        output_file (str): Путь к новому CSV-файлу.
    """
    # Чтение CSV
    df = pd.read_csv(input_file)

    # Основные столбцы для MOEX (берём только date, ticker, close)
    moex_df = df[['date', 'ticker', 'close']].dropna(subset=['ticker'])

    # Тикеры yfinance и их соответствующие столбцы
    yfinance_tickers = {
        'BZ=F': 'BRN',
        'CL=F': 'CL',
        'NG=F': 'NG',
        'GC=F': 'XAU/USD',
        'PA=F': 'XPD/USD',
        'NICK.L': 'NI',
        '^GSPC': 'SPX',
        '^FTSE': 'UKX',
        '000001.SS': 'SSEC'
    }

    # Список для хранения данных yfinance
    yfinance_data = []

    # Обработка данных для каждого тикера yfinance
    for ticker, display_name in yfinance_tickers.items():
        close_col = f"('close', '{ticker}')"
        if close_col in df.columns:
            temp_df = df[['date', close_col]].dropna(subset=[close_col])
            temp_df = temp_df.rename(columns={close_col: 'close'})
            temp_df['ticker'] = display_name
            temp_df['close'] = temp_df['close'].astype(float)
            yfinance_data.append(temp_df[['date', 'ticker', 'close']])

    # Объединение данных
    if yfinance_data:
        yfinance_df = pd.concat(yfinance_data, ignore_index=True)
        combined_df = pd.concat([moex_df, yfinance_df], ignore_index=True)
    else:
        combined_df = moex_df

    # Преобразование в сводную таблицу
    pivot_df = combined_df.pivot(index='date', columns='ticker', values='close')

    # Список всех ожидаемых тикеров в порядке, указанном в запросе
    expected_tickers = [
        'IMOEX', 'RTSI', 'MOEXOG', 'MOEXFN', 'MOEXMM', 'MOEXCN',
        'USD/RUB', 'EUR/RUB', 'BRN', 'CL', 'NG', 'XAU/USD', 'XPD/USD',
        'NI', 'SPX', 'UKX', 'SSEC'
    ]

    # Убедимся, что все тикеры присутствуют (даже если данных нет, будут NaN)
    for ticker in expected_tickers:
        if ticker not in pivot_df.columns:
            pivot_df[ticker] = pd.NA

    # Выбираем только ожидаемые тикеры в нужном порядке
    pivot_df = pivot_df[expected_tickers]

    # Сброс индекса, чтобы date стал столбцом
    pivot_df.reset_index(inplace=True)

    # Форматирование даты
    pivot_df['date'] = pd.to_datetime(pivot_df['date']).dt.strftime('%Y-%m-%d')

    # Сохранение результата
    output_dir = os.path.dirname(output_file)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    pivot_df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"Преобразованный CSV сохранён в {output_file}: {len(pivot_df)} строк")

if __name__ == "__main__":
    input_file = "historical_data/combined_all_data_daily_5Y_20250427_230011.csv"
    output_file = "historical_data/combined_all_data_daily_5Y_20250427_230011_pivoted.csv"
    pivot_csv_structure(input_file, output_file)