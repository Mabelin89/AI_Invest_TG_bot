import pandas as pd
import numpy as np
import os
import requests
import re
from openai import OpenAI
from moex_parser import get_historical_data
from utils import read_csv_file, read_monthly_macro_content, read_yearly_macro_content

# Папки для отчетов и исторических данных
REPORTS_DIR = "reports"
HISTORICAL_DATA_DIR = "historical_data"

# Инициализация клиента LLM
client = OpenAI(base_url="http://localhost:1234/v1", api_key="not-needed")

# Функция для расчёта EMA
def calculate_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

# Функция для сохранения исторических данных с индикаторами
def save_historical_data(ticker, timeframe, period_years):
    if not os.path.exists(HISTORICAL_DATA_DIR):
        os.makedirs(HISTORICAL_DATA_DIR)

    data = get_historical_data(ticker, timeframe, period_years)
    if not data.empty:
        data['begin'] = pd.to_datetime(data['begin'])
        data.set_index('begin', inplace=True)

        data['SMA_10'] = data['close'].rolling(window=10, min_periods=1).mean()
        data['SMA_20'] = data['close'].rolling(window=20, min_periods=1).mean()
        data['SMA_50'] = data['close'].rolling(window=50, min_periods=1).mean()
        data['SMA_200'] = data['close'].rolling(window=200, min_periods=1).mean()
        data['EMA_10'] = calculate_ema(data['close'], 10)
        data['EMA_20'] = calculate_ema(data['close'], 20)
        data['EMA_50'] = calculate_ema(data['close'], 50)

        delta = data['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14, min_periods=1).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14, min_periods=1).mean()
        rs = gain / loss
        data['RSI_14'] = 100 - (100 / (1 + rs))

        data['BB_middle'] = data['close'].rolling(window=20, min_periods=1).mean()
        data['BB_std'] = data['close'].rolling(window=20, min_periods=1).std()
        data['BB_upper'] = data['BB_middle'] + 2 * data['BB_std']
        data['BB_lower'] = data['BB_middle'] - 2 * data['BB_std']
        data = data.drop(columns=['BB_std'])

        ema_12 = calculate_ema(data['close'], 12)
        ema_26 = calculate_ema(data['close'], 26)
        data['MACD'] = ema_12 - ema_26
        data['MACD_signal'] = data['MACD'].rolling(window=9, min_periods=1).mean()
        data['MACD_histogram'] = data['MACD'] - data['MACD_signal']

        data.reset_index(inplace=True)
        data.rename(columns={'begin': 'date'}, inplace=True)

        filename = f"{ticker}_{timeframe.upper()}_{period_years}Y.csv"
        file_path = os.path.join(HISTORICAL_DATA_DIR, filename)
        data.to_csv(file_path, index=False)
        print(f"Исторические данные с индикаторами сохранены: {file_path}")
        return data
    return None

# Функция для скачивания отчетов
def download_reports(ticker, is_preferred=False, base_ticker=None):
    if not os.path.exists(REPORTS_DIR):
        os.makedirs(REPORTS_DIR)

    report_ticker = base_ticker if is_preferred and base_ticker else ticker

    report_urls = [
        f"https://smart-lab.ru/q/{report_ticker}/f/y/MSFO/download/",
        f"https://smart-lab.ru/q/{report_ticker}/f/y/RSBU/download/"
    ]
    report_names = [
        f"{report_ticker}-МСФО-годовые.csv",
        f"{report_ticker}-РСБУ-годовые.csv"
    ]

    for url, filename in zip(report_urls, report_names):
        file_path = os.path.join(REPORTS_DIR, filename)
        try:
            response = requests.get(url)
            if response.status_code == 200:
                with open(file_path, 'wb') as f:
                    f.write(response.content)
                print(f"Отчет сохранен: {file_path}")
            else:
                print(f"Не удалось скачать отчет {filename}: статус {response.status_code}")
        except Exception as e:
            print(f"Ошибка при скачивании {filename}: {str(e)}")

# Функция для анализа отчетов с учетом макроэкономики
def analyze_msfo_report(ticker, base_ticker, chat_id, bot, period_years):
    msfo_file = os.path.join(REPORTS_DIR, f"{base_ticker}-МСФО-годовые.csv")
    rsbu_file = os.path.join(REPORTS_DIR, f"{base_ticker}-РСБУ-годовые.csv")

    msfo_content = None
    rsbu_content = None
    monthly_macro_content = None
    yearly_macro_content = None

    if os.path.exists(msfo_file):
        msfo_df = read_csv_file(msfo_file)
        if msfo_df is not None:
            msfo_content = msfo_df.to_string(index=False)
        else:
            return f"Не удалось прочитать отчет МСФО для {base_ticker}. Проверьте файл '{msfo_file}'."
    else:
        return f"Отчет МСФО для {base_ticker} не найден в папке '{REPORTS_DIR}'."

    if os.path.exists(rsbu_file):
        rsbu_df = read_csv_file(rsbu_file)
        if rsbu_df is not None:
            rsbu_content = rsbu_df.to_string(index=False)
        else:
            print(f"Не удалось прочитать отчет РСБУ для {base_ticker}, продолжаем с МСФО.")

    monthly_macro_df = read_monthly_macro_content()
    if monthly_macro_df is not None:
        current_year = 2025
        start_year = current_year - period_years
        monthly_macro_df['Дата'] = pd.to_datetime(monthly_macro_df['Дата'], format='%Y-%m')
        monthly_macro_df = monthly_macro_df[monthly_macro_df['Дата'].dt.year >= start_year]
        monthly_macro_content = monthly_macro_df.to_string(index=False)
    else:
        print("Месячные макроэкономические данные недоступны, продолжаем без них.")

    yearly_macro_df = read_yearly_macro_content()
    if yearly_macro_df is not None:
        current_year = 2025
        start_year = current_year - period_years
        yearly_macro_df = yearly_macro_df[yearly_macro_df['Год'] >= start_year]
        yearly_macro_content = yearly_macro_df.to_string(index=False)
    else:
        print("Годовые макроэкономические данные недоступны, продолжаем без них.")

    bot.send_message(chat_id, "Пожалуйста подождите, работает LLM.")
    system_message = """
Ты финансовый аналитик, анализирующий отчеты компании по стандартам МСФО и РСБУ, а также макроэкономические данные России. 
Содержимое МСФО (данные за годы в столбцах, включая LTM): 
{msfo_content}
Содержимое РСБУ (если доступно, данные за годы в столбцах): 
{rsbu_content}
Помесячные макроэкономические данные России (за последние {period_years} лет): 
{monthly_macro_content}
Годовые макроэкономические данные России (за последние {period_years} лет): 
{yearly_macro_content}

Задача:
1. Извлеки список всех годов из заголовков столбцов МСФО (начиная с 2008 по LTM). Если РСБУ доступен, убедись, что годы совпадают или дополняют МСФО.
2. Определи ключевые финансовые показатели компании из МСФО и РСБУ:
   - Из МСФО (приоритет): Чистый операционный доход, Чистая прибыль, Активы, Капитал, Кредитный портфель, Депозиты, P/E, P/B, EV/EBITDA.
   - Из РСБУ (дополнение): Выручка, Себестоимость, Прибыль до налогообложения, EBITDA (если есть), Амортизация (если есть). Добавляй "(РСБУ)" к показателям из РСБУ.
3. Для EV/EBITDA:
   - Если есть в МСФО, используй напрямую.
   - Если нет, рассчитай как EV / EBITDA (из МСФО или РСБУ). Если EBITDA нет, используй "Операционная прибыль" (из МСФО) или "Прибыль до налогообложения + Амортизация" (из РСБУ, если есть), с пометкой "(приблизительно)".
4. Учти макроэкономические данные:
   - Сравни динамику Чистой прибыли и Активов компании с Инфляцией (CPI), Ключевой ставкой и Обменным курсом USD/RUB за те же годы.
   - Отметь влияние макроэкономических факторов (например, высокая инфляция, рост курса USD/RUB) на показатели компании.
5. Верни результат в формате:
   - "Данные за годы: 2008 | 2009 | ... | LTM"
   - "Показатель: Значение 2008 | Значение 2009 | ... | Значение LTM (единица измерения, если указана)"
   - "Комментарий: [влияние макроэкономики на показатели за период]"
   Раздели строки переносом.
6. Если данных за год нет, укажи "н/д". Если показатель доступен только в РСБУ, возьми его оттуда.

Правила:
- Работай только с данными из CSV, не придумывай сверх того.
- Если формат данных неясен, верни сообщение об ошибке.
- Ограничь ответ 500 токенов.
"""
    try:
        response = client.chat.completions.create(
            model="deepseek-r1-distill-qwen-14b",
            messages=[
                {"role": "system", "content": system_message.format(
                    msfo_content=msfo_content,
                    rsbu_content=rsbu_content if rsbu_content else "Отсутствует",
                    monthly_macro_content=monthly_macro_content if monthly_macro_content else "Отсутствует",
                    yearly_macro_content=yearly_macro_content if yearly_macro_content else "Отсутствует",
                    period_years=period_years
                )},
                {"role": "user", "content": f"Анализируй отчеты для тикера {base_ticker} с учетом макроэкономических данных."}
            ],
            max_tokens=10000,
            temperature=0.1
        )
        raw_response = response.choices[0].message.content.strip()
        result = re.sub(r'<think>.*?</think>\s*|<think>.*$|</think>\s*', '', raw_response, flags=re.DOTALL).strip()
        return result
    except Exception as e:
        return f"Ошибка при анализе отчетов для {base_ticker}: {str(e)}"