import pandas as pd
import numpy as np
import os
import requests
import re
from gigachat import GigaChat
from bot_config import GIGACHAT_API_KEY, VERIFY_SSL_CERTS
from openai import OpenAI
from moex_parser import get_historical_data
from utils import read_csv_file, read_monthly_macro_content, read_yearly_macro_content
from ta.trend import ADXIndicator

REPORTS_DIR = "reports"
HISTORICAL_DATA_DIR = "historical_data"

def calculate_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

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

    # Проверка наличия необходимых столбцов
    required_columns = ['open', 'high', 'low', 'close', 'volume']
    missing_columns = [col for col in required_columns if col not in data.columns]
    if missing_columns:
        print(f"Ошибка: отсутствуют столбцы {missing_columns}, аппроксимация high и low")
        for col in missing_columns:
            if col in ['high', 'low']:
                volatility = data['close'].pct_change().std() * np.sqrt(20) or 0.05
                data['high'] = data['close'] * (1 + volatility)
                data['low'] = data['close'] * (1 - volatility)
            elif col == 'open':
                data['open'] = data['close']
            elif col == 'volume':
                data['volume'] = 0

    # Проверка на пропуски
    if data[['high', 'low', 'close']].isna().any().any():
        print("Предупреждение: найдены пропуски в high, low или close, заполняются средним")
        data[['high', 'low', 'close']] = data[['high', 'low', 'close']].fillna(data[['high', 'low', 'close']].mean())

    if timeframe.lower() in ['1h', '4h', 'daily']:
        sma_periods = [10, 20, 50]
        ema_periods = [10, 20, 50]
        macd_fast, macd_slow, macd_signal = 12, 26, 9
        adx_period = 14
        rsi_period = 14
        stoch_k, stoch_d, stoch_smooth = 14, 3, 3
    elif timeframe.lower() in ['weekly']:
        sma_periods = [50, 100, 200]
        ema_periods = [50, 100, 200]
        macd_fast, macd_slow, macd_signal = 24, 52, 9
        adx_period = 20
        rsi_period = 21
        stoch_k, stoch_d, stoch_smooth = 21, 5, 5
    else:
        sma_periods = [10, 20, 50]
        ema_periods = [10, 20, 50]
        macd_fast, macd_slow, macd_signal = 12, 26, 9
        adx_period = 14
        rsi_period = 14
        stoch_k, stoch_d, stoch_smooth = 14, 3, 3

    for period in sma_periods:
        data[f'SMA_{period}'] = data['close'].rolling(window=period, min_periods=1).mean()
    for period in ema_periods:
        data[f'EMA_{period}'] = calculate_ema(data['close'], period)

    delta = data['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=rsi_period, min_periods=1).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=rsi_period, min_periods=1).mean()
    rs = gain / loss
    data[f'RSI_{rsi_period}'] = 100 - (100 / (1 + rs))

    data['BB_middle'] = data['close'].rolling(window=20, min_periods=1).mean()
    data['BB_std'] = data['close'].rolling(window=20, min_periods=1).std()
    data['BB_upper'] = data['BB_middle'] + 2 * data['BB_std']
    data['BB_lower'] = data['BB_middle'] - 2 * data['BB_std']
    data = data.drop(columns=['BB_std'])

    ema_fast = calculate_ema(data['close'], macd_fast)
    ema_slow = calculate_ema(data['close'], macd_slow)
    data['MACD'] = ema_fast - ema_slow
    data['MACD_signal'] = calculate_ema(data['MACD'], macd_signal)
    data['MACD_histogram'] = data['MACD'] - data['MACD_signal']

    low_n = data['low'].rolling(window=stoch_k, min_periods=1).min()
    high_n = data['high'].rolling(window=stoch_k, min_periods=1).max()
    data['Stoch_K'] = 100 * (data['close'] - low_n) / (high_n - low_n)
    data['Stoch_D'] = data['Stoch_K'].rolling(window=stoch_d, min_periods=1).mean()
    data['Stoch_Slow'] = data['Stoch_D'].rolling(window=stoch_smooth, min_periods=1).mean()

    data['OBV'] = np.where(data['close'] > data['close'].shift(1), data['volume'],
                           np.where(data['close'] < data['close'].shift(1), -data['volume'], 0)).cumsum()

    data['Cum_Volume'] = data['volume'].cumsum()
    data['Cum_Vol_Price'] = (data['close'] * data['volume']).cumsum()
    data['VWAP'] = data['Cum_Vol_Price'] / data['Cum_Volume']
    data = data.drop(columns=['Cum_Volume', 'Cum_Vol_Price'])

    # Расчёт ADX с использованием библиотеки ta
    adx_indicator = ADXIndicator(high=data['high'], low=data['low'], close=data['close'], window=adx_period, fillna=True)
    data['ADX'] = adx_indicator.adx()

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
        f"https://smart-lab.ru/q/{report_ticker}/f/y/MSFO/download/"
    ]
    report_names = [
        f"{report_ticker}-МСФО-годовые.csv"
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

def analyze_msfo_report(ticker, base_ticker, chat_id, bot, period_years, model="local"):
    print(f"analyze_msfo_report called with ticker={ticker}, model={model}")
    msfo_file = os.path.join(REPORTS_DIR, f"{base_ticker}-МСФО-годовые.csv")

    msfo_content = None
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

    bot.send_message(chat_id, "Пожалуйста подождите, работает модель.")

    gigachat_prompt = f"""
Ты финансовый аналитик, анализирующий отчеты компании по стандартам МСФО и макроэкономические данные России. 
Содержимое МСФО (данные за годы в столбцах, включая LTM): 
{msfo_content}
Помесячные макроэкономические данные России (за последние {period_years} лет): 
{monthly_macro_content}
Годовые макроэкономические данные России (за последние {period_years} лет): 
{yearly_macro_content}

Задача:
1. Извлеки список всех годов из заголовков столбцов МСФО (начиная с 2008 по LTM).
2. Определи ключевые финансовые показатели из МСФО: Чистый операционный доход, Чистая прибыль, Активы, Капитал, Кредитный портфель, Депозиты, P/E, P/B, EV/EBITDA.
3. Для EV/EBITDA: если есть, используй напрямую; если нет, рассчитай как EV / EBITDA (используй "Операционная прибыль" с пометкой "(приблизительно)" при отсутствии EBITDA).
4. Сравни динамику Чистой прибыли и Активов с Инфляцией (CPI), Ключевой ставкой и Обменным курсом USD/RUB за те же годы.
5. Укажи влияние макроэкономики (инфляция, ставка, курс USD/RUB) на показатели компании.
6. Верни результат в формате:
   - Данные за годы: 2008 | 2009 | ... | LTM
   - Показатель: Значение 2008 | Значение 2009 | ... | Значение LTM (единица измерения, если есть)
   - Комментарий: [влияние макроэкономики на показатели]
   Раздели строки переносом.
7. Если данных за год нет, укажи "н/д".

Правила:
- Используй только предоставленные данные.
- Ответ не более 500 токенов.
"""

    local_llm_prompt = f"""
Ты финансовый аналитик, анализирующий отчеты компании по стандартам МСФО и РСБУ, а также макроэкономические данные России. 
Содержимое МСФО (данные за годы в столбцах, включая LTM): 
{msfo_content}
Содержимое РСБУ (если доступно, данные за годы в столбцах): 
{msfo_content}
Помесячные макроэкономические данные России (за последние {period_years} лет): 
{monthly_macro_content}
Годовые макроэкономические данные России (за последние {period_years} лет): 
{yearly_macro_content}

Задача:
1. Извлеки список всех годов из заголовков столбцов МСФО (начиная с 2008 по LTM).
2. Определи ключевые финансовые показатели компании из МСФО:
   - Чистый операционный доход, Чистая прибыль, Активы, Капитал, Кредитный портфель, Депозиты, P/E, P/B, EV/EBITDA.
3. Для EV/EBITDA:
   - Если есть в МСФО, используй напрямую.
   - Если нет, рассчитай как EV / EBITDA (из МСФО). Если EBITDA нет, используй "Операционная прибыль" с пометкой "(приблизительно)".
4. Учти макроэкономические данные:
   - Сравни динамику Чистой прибыли и Активов компании с Инфляцией (CPI), Ключевой ставкой и Обменным курсом USD/RUB за те же годы.
   - Отметь влияние макроэкономических факторов (например, высокая инфляция, рост курса USD/RUB) на показатели компании.
5. Верни результат в формате:
   - "Данные за годы: 2008 | 2009 | ... | LTM"
   - "Показатель: Значение 2008 | Значение 2009 | ... | Значение LTM (единица измерения, если указана)"
   - "Комментарий: [влияние макроэкономики на показатели за период]"
   Раздели строки переносом.
6. Если данных за год нет, укажи "н/д".

Правила:
- Работай только с данными из CSV, не придумывай сверх того.
- Если формат данных неясен, верни сообщение об ошибке.
- Ограничь ответ 500 токенов.
"""

    system_message = gigachat_prompt if model == "gigachat" else local_llm_prompt

    try:
        if model == "gigachat":
            print("Using GigaChat for MSFO analysis")
            with GigaChat(
                credentials=GIGACHAT_API_KEY,
                verify_ssl_certs=VERIFY_SSL_CERTS,
                model="GigaChat-2-Max"
            ) as gigachat_client:
                response = gigachat_client.chat(system_message)
                raw_response = response.choices[0].message.content.strip()
                result = re.sub(r'<think>.*?</think>\s*|<think>.*$|</think>\s*', '', raw_response, flags=re.DOTALL).strip()
                return result
        else:
            print("Using local LLM for MSFO analysis")
            openai_client = OpenAI(base_url="http://localhost:1234/v1", api_key="not-needed")
            response = openai_client.chat.completions.create(
                model="deepseek-r1-distill-qwen-14b",
                messages=[
                    {"role": "system", "content": system_message},
                    {"role": "user", "content": f"Анализируй отчеты для тикера {base_ticker} с учетом макроэкономических данных."}
                ],
                max_tokens=10000,
                temperature=0.1
            )
            raw_response = response.choices[0].message.content.strip()
            result = re.sub(r'<think>.*?</think>\s*|<think>.*$|</think>\s*', '', raw_response, flags=re.DOTALL).strip()
            return result
    except Exception as e:
        return f"Ошибка при анализе отчетов для {base_ticker}: {str(e)}\nТип ошибки: {type(e).__name__}"