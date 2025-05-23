import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os

# Список инструментов и их тикеры на Yahoo Finance
tickers = {
    "BRN": "BZ=F",          # Brent Crude Oil
    "CL": "CL=F",           # WTI Crude Oil
    "NG": "NG=F",           # Natural Gas
    "XAUUSD": "GC=F",       # Gold
    "XPDUSD": "PA=F",       # Palladium
    "SPX": "^GSPC",         # S&P 500
    "UKX": "^FTSE",         # FTSE 100
    "SSEC": "000001.SS"     # Shanghai Composite
}

# Настройка временного диапазона (10 лет назад от текущей даты)
end_date = datetime.now()
start_date = end_date - timedelta(days=365 * 10)

# Создание директории для сохранения данных
if not os.path.exists("historical_data"):
    os.makedirs("historical_data")

# Функция для загрузки и сохранения данных
def download_data(ticker, name, start, end):
    try:
        # Загрузка данных с Yahoo Finance
        data = yf.download(ticker, start=start, end=end, interval="1d")
        
        # Проверка, что данные не пустые
        if not data.empty:
            # Переименование столбцов для соответствия желаемой структуре
            data = data[['Open', 'High', 'Low', 'Close', 'Volume']]
            data.reset_index(inplace=True)  # Перенос даты в столбец
            data = data[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]  # Порядок столбцов
            
            # Форматирование даты в строку (например, YYYY-MM-DD)
            data['Date'] = data['Date'].dt.strftime('%Y-%m-%d')
            
            # Сохранение в CSV
            output_file = f"historical_data/{name}_10y_daily.csv"
            data.to_csv(output_file, index=False)
            print(f"Данные для {name} ({ticker}) успешно сохранены в {output_file}")
        else:
            print(f"Не удалось загрузить данные для {name} ({ticker})")
    except Exception as e:
        print(f"Ошибка при загрузке данных для {name} ({ticker}): {e}")

# Загрузка данных для каждого инструмента
for name, ticker in tickers.items():
    download_data(ticker, name, start_date, end_date)

# Объединение данных в один Excel-файл
combined_data = {}
for name, ticker in tickers.items():
    file_path = f"historical_data/{name}_10y_daily.csv"
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        combined_data[name] = df

# Создание Excel-файла с отдельными листами
with pd.ExcelWriter("historical_data/combined_10y_daily.xlsx") as writer:
    for name, df in combined_data.items():
        df.to_excel(writer, sheet_name=name, index=False)
print("Все данные объединены в historical_data/combined_10y_daily.xlsx")