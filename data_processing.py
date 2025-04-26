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
    data['date'] = pd.to_datetime(data['date'], errors='coerce')
    if data['date'].isna().any():
        print(f"Ошибка: некорректные даты для {ticker}")
        return None
    data.set_index('date', inplace=True)

    required_columns = ['open', 'high', 'low', 'close', 'volume']
    missing_columns = [col for col in required_columns if col not in data.columns]
    if missing_columns:
        print(f"Ошибка: отсутствуют столбцы {missing_columns}, аппроксимация high и low")
        for col in missing_columns:
            if col in ['high', 'low']:
                volatility = data['close'].pct_change().std() * np.sqrt(20) if not data['close'].empty else 0.05
                data['high'] = data['close'] * (1 + volatility)
                data['low'] = data['close'] * (1 - volatility)
            elif col == 'open':
                data['open'] = data['close']
            elif col == 'volume':
                data['volume'] = 0

    if data[['high', 'low', 'close']].isna().any().any():
        print("Предупреждение: найдены пропуски в high, low или close, заполняются последним значением")
        data[['high', 'low', 'close']] = data[['high', 'low', 'close']].fillna(method='ffill').fillna(method='bfill')

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
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                with open(file_path, 'wb') as f:
                    f.write(response.content)
                print(f"Отчет сохранен: {file_path}")
            else:
                print(f"Не удалось скачать отчет {filename}: статус {response.status_code}")
        except requests.RequestException as e:
            print(f"Ошибка при скачивании {filename}: {str(e)}")

def optimize_msfo_content(msfo_df):
    """
    Оптимизирует МСФО-данные для экономии токенов и упрощения понимания LLM.
    """
    if msfo_df is None or msfo_df.empty:
        return "Отсутствует"

    key_indicators = {
        'Чистая прибыль, млрд руб': 'NP',
        'Активы, млрд руб': 'Assets',
        'Чистые активы, млрд руб': 'Equity',
        'P/E': 'P/E',
        'P/B': 'P/B',
        'EV/EBITDA': 'EV/EBITDA',
        'Операционная прибыль, млрд руб': 'OP'
    }

    msfo_df = msfo_df[msfo_df['Unnamed: 0'].isin(key_indicators.keys())]
    msfo_df = msfo_df.dropna(how='all', subset=msfo_df.columns[1:])

    if msfo_df.empty:
        return "Отсутствует"

    msfo_df['Unnamed: 0'] = msfo_df['Unnamed: 0'].map(key_indicators)
    years = [col for col in msfo_df.columns[1:] if col.isdigit() and int(col) >= 2008 or col == 'LTM']
    if not years:
        return "Отсутствует"
    msfo_df = msfo_df[['Unnamed: 0'] + years]
    msfo_df = msfo_df.fillna('н/д')

    msfo_str = msfo_df.to_csv(index=False, header=True, sep='|', lineterminator='\n')
    msfo_str = re.sub(r'[ \t]+', '', msfo_str)
    msfo_str = msfo_str.replace('Unnamed:0', 'Показатель')
    return msfo_str

def optimize_monthly_macro(monthly_macro_df):
    """
    Оптимизирует месячные макроэкономические данные для экономии токенов.
    """
    if monthly_macro_df is None or monthly_macro_df.empty:
        return "Отсутствует"

    key_columns = {
        'Дата': 'Date',
        'Инфляция (CPI, %, год к году)': 'CPI',
        'Ключевая ставка (%)': 'Rate',
        'Обменный курс USD/RUB (ср. за месяц)': 'USD/RUB'
    }

    available_columns = [col for col in key_columns.keys() if col in monthly_macro_df.columns]
    if not available_columns:
        return "Отсутствует"

    monthly_macro_df = monthly_macro_df[available_columns]
    monthly_macro_df = monthly_macro_df.rename(columns=key_columns)

    try:
        monthly_macro_df['Date'] = pd.to_datetime(monthly_macro_df['Date'], errors='coerce').dt.strftime('%Y-%m')
        if monthly_macro_df['Date'].isna().any():
            print("Предупреждение: некорректные даты в месячных макро-данных")
            return "Отсутствует"
    except Exception as e:
        print(f"Ошибка обработки дат в месячных макро-данных: {str(e)}")
        return "Отсутствует"

    monthly_macro_df = monthly_macro_df.dropna(how='all', subset=['CPI', 'Rate', 'USD/RUB'])
    monthly_macro_df = monthly_macro_df.fillna('н/д')

    macro_str = monthly_macro_df.to_csv(index=False, header=True, sep='|', lineterminator='\n')
    macro_str = re.sub(r'[ \t]+', '', macro_str)
    return macro_str

def optimize_yearly_macro(yearly_macro_df):
    """
    Оптимизирует годовые макроэкономические данные для экономии токенов.
    """
    if yearly_macro_df is None or yearly_macro_df.empty:
        return "Отсутствует"

    key_columns = {
        'Год': 'Year',
        'Инфляция (CPI, %, дек к дек)': 'CPI',
        'Ключевая ставка (%)': 'Rate',
        'Обменный курс USD/RUB (ср. за год)': 'USD/RUB'
    }

    available_columns = [col for col in key_columns.keys() if col in yearly_macro_df.columns]
    if not available_columns:
        return "Отсутствует"

    yearly_macro_df = yearly_macro_df[available_columns]
    yearly_macro_df = yearly_macro_df.rename(columns=key_columns)

    yearly_macro_df = yearly_macro_df.dropna(how='all', subset=['CPI', 'Rate', 'USD/RUB'])
    yearly_macro_df = yearly_macro_df.fillna('н/д')

    macro_str = yearly_macro_df.to_csv(index=False, header=True, sep='|', lineterminator='\n')
    macro_str = re.sub(r'[ \t]+', '', macro_str)
    return macro_str

def analyze_msfo_report(ticker, base_ticker, chat_id, bot, period_years, model="local"):
    print(f"analyze_msfo_report called with ticker={ticker}, model={model}")
    msfo_file = os.path.join(REPORTS_DIR, f"{base_ticker}-МСФО-годовые.csv")

    msfo_content = None
    monthly_macro_content = None
    yearly_macro_content = None

    if os.path.exists(msfo_file):
        try:
            msfo_df = read_csv_file(msfo_file)
            if msfo_df is not None:
                msfo_content = optimize_msfo_content(msfo_df)
            else:
                return f"Не удалось прочитать отчет МСФО для {base_ticker}. Проверьте файл '{msfo_file}'."
        except Exception as e:
            return f"Ошибка чтения МСФО для {base_ticker}: {str(e)}"
    else:
        return f"Отчет МСФО для {base_ticker} не найден в папке '{REPORTS_DIR}'."

    try:
        monthly_macro_df = read_monthly_macro_content()
        if monthly_macro_df is not None and not monthly_macro_df.empty:
            current_year = 2025
            start_year = current_year - period_years
            monthly_macro_df['Дата'] = pd.to_datetime(monthly_macro_df['Дата'], format='%Y-%m', errors='coerce')
            monthly_macro_df = monthly_macro_df[monthly_macro_df['Дата'].dt.year >= start_year]
            monthly_macro_content = optimize_monthly_macro(monthly_macro_df)
        else:
            monthly_macro_content = "Отсутствует"
    except Exception as e:
        print(f"Ошибка обработки месячных макро-данных: {str(e)}")
        monthly_macro_content = "Отсутствует"

    try:
        yearly_macro_df = read_yearly_macro_content()
        if yearly_macro_df is not None and not yearly_macro_df.empty:
            current_year = 2025
            start_year = current_year - period_years
            yearly_macro_df = yearly_macro_df[yearly_macro_df['Год'] >= start_year]
            yearly_macro_content = optimize_yearly_macro(yearly_macro_df)
        else:
            yearly_macro_content = "Отсутствует"
    except Exception as e:
        print(f"Ошибка обработки годовых макро-данных: {str(e)}")
        yearly_macro_content = "Отсутствует"

    bot.send_message(chat_id, "Анализируется отчет МСФО, подождите.")

    gigachat_prompt = f"""
Ты финансовый аналитик, анализирующий МСФО и макроэкономику России.
МСФО (показатели|годы, разделитель |, строки \n):
{msfo_content}
Макро месячные ({period_years} лет, дата|показатели, |, \n):
{monthly_macro_content}
Макро годовые ({period_years} лет, год|показатели, |, \n):
{yearly_macro_content}

Задача:
1. Извлеки годы из МСФО (2008-LTM).
2. Используй показатели: NP (Чистая прибыль), Assets (Активы), Equity (Капитал), P/E, P/B, EV/EBITDA.
3. Для EV/EBITDA: используй напрямую или рассчитай как EV/OP (приблизительно) если нет.
4. Сравни NP и Assets с CPI, Rate, USD/RUB за те же годы.
5. Оцени влияние макро (CPI, Rate, USD/RUB) на показатели.
6. Формат ответа:
Годы:2008|2009|...|LTM
NP:[значение]|...|[значение]
Assets:[значение]|...|[значение]
Equity:[значение]|...|[значение]
P/E:[значение]|...|[значение]
P/B:[значение]|...|[значение]
EV/EBITDA:[значение]|...|[значение]
Комментарий:[влияние макро на показатели]
7. Если данных нет, укажи н/д.

Правила:
- Только предоставленные данные.
- Ответ ≤ 500 токенов.
- Без Markdown, лишних пробелов, переносов.
"""

    local_llm_prompt = f"""
Ты финансовый аналитик, анализирующий отчеты компании по стандартам МСФО и РСБУ, а также макроэкономические данные России. 
Содержимое МСФО (данные за годы в столбцах, включая LTM, |, \n): 
{msfo_content}
Содержимое РСБУ (если доступно, данные за годы в столбцах, |, \n): 
{msfo_content}
Помесячные макроэкономические данные России (за последние {period_years} лет, |, \n): 
{monthly_macro_content}
Годовые макроэкономические данные России (за последние {period_years} лет, |, \n): 
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
                bot.send_message(chat_id, result)
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
            result = re.sub(r'<think>.*?</think>\s*|<think>.*$?></think>\s*', '', raw_response, flags=re.DOTALL).strip()
            bot.send_message(chat_id, result)
            return result
    except Exception as e:
        error_msg = f"Ошибка при анализе отчетов для {base_ticker}: {str(e)}\nТип ошибки: {type(e).__name__}"
        bot.send_message(chat_id, error_msg)
        return error_msg