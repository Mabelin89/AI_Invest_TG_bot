import pandas as pd
import numpy as np
import os
import requests
import re
from openai import OpenAI
from moex_parser import get_historical_data
from utils import read_csv_file, read_monthly_macro_content, read_yearly_macro_content

REPORTS_DIR = "reports"
HISTORICAL_DATA_DIR = "historical_data"

client = OpenAI(base_url="http://localhost:1234/v1", api_key="not-needed")


def calculate_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()


def calculate_adx(high, low, close, period):
    # Расчёт ADX
    delta_high = high.diff()
    delta_low = low.diff()
    plus_dm = np.where((delta_high > delta_low) & (delta_high > 0), delta_high, 0)
    minus_dm = np.where((delta_low > delta_high) & (delta_low > 0), delta_low, 0)

    tr = pd.concat([
        high - low,
        abs(high - close.shift()),
        abs(low - close.shift())
    ], axis=1).max(axis=1)

    atr = tr.rolling(window=period, min_periods=1).mean()
    plus_di = 100 * pd.Series(plus_dm).rolling(window=period, min_periods=1).mean() / atr
    minus_di = 100 * pd.Series(minus_dm).rolling(window=period, min_periods=1).mean() / atr
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
    adx = dx.rolling(window=period, min_periods=1).mean()
    return adx


def save_historical_data(ticker, timeframe, period_years):
    if not os.path.exists(HISTORICAL_DATA_DIR):
        os.makedirs(HISTORICAL_DATA_DIR)

    data = get_historical_data(ticker, timeframe, period_years)
    if data is None or data.empty:
        print(f"Ошибка: данные для {ticker} ({timeframe}, {period_years} лет) не получены из moex_parser")
        return None

    print(f"Получены данные для {ticker} ({timeframe}): {len(data)} строк, столбцы: {list(data.columns)}")
    data['date'] = pd.to_datetime(data['date'])
    data.set_index('date', inplace=True)

    # Определение параметров в зависимости от таймфрейма и горизонта
    if timeframe.lower() in ['1h', '4h', 'daily']:  # Краткосрочная торговля
        sma_periods = [10, 20, 50]
        ema_periods = [10, 20, 50]
        macd_fast, macd_slow, macd_signal = 12, 26, 9
        adx_period = 14
        rsi_period = 14
        stoch_k, stoch_d, stoch_smooth = 14, 3, 3
    elif timeframe.lower() in ['weekly']:  # Среднесрочная торговля
        sma_periods = [50, 100, 200]
        ema_periods = [50, 100, 200]
        macd_fast, macd_slow, macd_signal = 24, 52, 9
        adx_period = 20
        rsi_period = 21
        stoch_k, stoch_d, stoch_smooth = 21, 5, 5
    elif timeframe.lower() in ['monthly', 'quarterly']:  # Долгосрочные инвестиции
        sma_periods = [200]
        ema_periods = [200]
        macd_fast, macd_slow, macd_signal = 50, 200, 9
        adx_period = 50
        rsi_period = 50
        stoch_k, stoch_d, stoch_smooth = 50, 10, 10
    else:
        # По умолчанию краткосрочные параметры
        sma_periods = [10, 20, 50]
        ema_periods = [10, 20, 50]
        macd_fast, macd_slow, macd_signal = 12, 26, 9
        adx_period = 14
        rsi_period = 14
        stoch_k, stoch_d, stoch_smooth = 14, 3, 3

    # Скользящие средние
    for period in sma_periods:
        data[f'SMA_{period}'] = data['close'].rolling(window=period, min_periods=1).mean()
    for period in ema_periods:
        data[f'EMA_{period}'] = calculate_ema(data['close'], period)

    # RSI
    delta = data['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=rsi_period, min_periods=1).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=rsi_period, min_periods=1).mean()
    rs = gain / loss
    data[f'RSI_{rsi_period}'] = 100 - (100 / (1 + rs))

    # Bollinger Bands
    data['BB_middle'] = data['close'].rolling(window=20, min_periods=1).mean()
    data['BB_std'] = data['close'].rolling(window=20, min_periods=1).std()
    data['BB_upper'] = data['BB_middle'] + 2 * data['BB_std']
    data['BB_lower'] = data['BB_middle'] - 2 * data['BB_std']
    data = data.drop(columns=['BB_std'])

    # MACD
    ema_fast = calculate_ema(data['close'], macd_fast)
    ema_slow = calculate_ema(data['close'], macd_slow)
    data['MACD'] = ema_fast - ema_slow
    data['MACD_signal'] = calculate_ema(data['MACD'], macd_signal)
    data['MACD_histogram'] = data['MACD'] - data['MACD_signal']

    # Stochastic Oscillator
    low_n = data['low'].rolling(window=stoch_k, min_periods=1).min()
    high_n = data['high'].rolling(window=stoch_k, min_periods=1).max()
    data['Stoch_K'] = 100 * (data['close'] - low_n) / (high_n - low_n)
    data['Stoch_D'] = data['Stoch_K'].rolling(window=stoch_d, min_periods=1).mean()
    data['Stoch_Slow'] = data['Stoch_D'].rolling(window=stoch_smooth, min_periods=1).mean()

    # OBV (On-Balance Volume)
    data['OBV'] = np.where(data['close'] > data['close'].shift(1), data['volume'],
                           np.where(data['close'] < data['close'].shift(1), -data['volume'], 0)).cumsum()

    # VWAP (Volume Weighted Average Price) - рассчитываем как кумулятивный VWAP
    data['Cum_Volume'] = data['volume'].cumsum()
    data['Cum_Vol_Price'] = (data['close'] * data['volume']).cumsum()
    data['VWAP'] = data['Cum_Vol_Price'] / data['Cum_Volume']
    data = data.drop(columns=['Cum_Volume', 'Cum_Vol_Price'])

    # ADX (Average Directional Index)
    data['ADX'] = calculate_adx(data['high'], data['low'], data['close'], adx_period)

    data.reset_index(inplace=True)

    filename = f"{ticker}_{timeframe.upper()}_{period_years}Y.csv"
    file_path = os.path.join(HISTORICAL_DATA_DIR, filename)
    data.to_csv(file_path, index=False)
    print(f"Исторические данные с индикаторами сохранены: {file_path}, столбцы: {list(data.columns)}")
    return data


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
    system_message = f"""
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
                {"role": "user",
                 "content": f"Анализируй отчеты для тикера {base_ticker} с учетом макроэкономических данных."}
            ],
            max_tokens=10000,
            temperature=0.1
        )
        raw_response = response.choices[0].message.content.strip()
        result = re.sub(r'<think>.*?</think>\s*|<think>.*$|</think>\s*', '', raw_response, flags=re.DOTALL).strip()
        return result
    except Exception as e:
        return f"Ошибка при анализе отчетов для {base_ticker}: {str(e)}"