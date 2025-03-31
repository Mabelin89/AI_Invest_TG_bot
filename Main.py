import telebot
from openai import OpenAI
import pandas as pd
import numpy as np
import re
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import matplotlib.pyplot as plt
import os
import requests
from bot_config import BOT_TOKEN
from moex_parser import get_historical_data

# Инициализация бота и клиента LLM
bot = telebot.TeleBot(BOT_TOKEN)
client = OpenAI(base_url="http://localhost:1234/v1", api_key="not-needed")

# Путь к файлу и глобальная переменная для содержимого
FILE_PATH = "moex_companies_no_etf.csv"
FILE_CONTENT = None

# Папки для отчетов и исторических данных
REPORTS_DIR = "reports"
HISTORICAL_DATA_DIR = "historical_data"

# Словарь для отслеживания состояния пользователей
user_states = {}


# Функция для чтения CSV файла с разделителем ';' и обработкой чисел с пробелами
def read_csv_file(file_path):
    try:
        df = pd.read_csv(file_path, sep=';', decimal=',', thousands=' ')
        df.columns = df.columns.str.strip()
        df = df.applymap(lambda x: str(x).strip() if isinstance(x, str) else x)
        return df.to_string(index=False)
    except Exception as e:
        print(f"Ошибка чтения CSV: {e}")
        return None


# Чтение содержимого файла один раз при запуске
def read_file_content(file_path):
    global FILE_CONTENT
    if FILE_CONTENT is None:
        if file_path.endswith('.csv'):
            FILE_CONTENT = read_csv_file(file_path)
        else:
            print("Поддерживается только формат .csv")
            FILE_CONTENT = None
    return FILE_CONTENT


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


# Функция для расчёта EMA (экспоненциальной скользящей средней)
def calculate_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()


# Функция для сохранения исторических данных с техническими индикаторами
def save_historical_data(ticker, timeframe, period_years):
    if not os.path.exists(HISTORICAL_DATA_DIR):
        os.makedirs(HISTORICAL_DATA_DIR)

    data = get_historical_data(ticker, timeframe, period_years)
    if not data.empty:
        # Преобразуем 'begin' в дату и устанавливаем как индекс
        data['begin'] = pd.to_datetime(data['begin'])
        data.set_index('begin', inplace=True)

        # Добавляем скользящие средние
        data['SMA_10'] = data['close'].rolling(window=10, min_periods=1).mean()
        data['SMA_20'] = data['close'].rolling(window=20, min_periods=1).mean()
        data['SMA_50'] = data['close'].rolling(window=50, min_periods=1).mean()
        data['EMA_10'] = calculate_ema(data['close'], 10)
        data['EMA_20'] = calculate_ema(data['close'], 20)
        data['EMA_50'] = calculate_ema(data['close'], 50)

        # Добавляем RSI
        delta = data['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14, min_periods=1).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14, min_periods=1).mean()
        rs = gain / loss
        data['RSI_14'] = 100 - (100 / (1 + rs))

        # Добавляем Полосы Боллинджера
        data['BB_middle'] = data['close'].rolling(window=20, min_periods=1).mean()
        data['BB_std'] = data['close'].rolling(window=20, min_periods=1).std()
        data['BB_upper'] = data['BB_middle'] + 2 * data['BB_std']
        data['BB_lower'] = data['BB_middle'] - 2 * data['BB_std']
        data = data.drop(columns=['BB_std'])  # Удаляем временный столбец

        # Добавляем MACD
        ema_12 = calculate_ema(data['close'], 12)
        ema_26 = calculate_ema(data['close'], 26)
        data['MACD'] = ema_12 - ema_26
        data['MACD_signal'] = data['MACD'].rolling(window=9, min_periods=1).mean()
        data['MACD_histogram'] = data['MACD'] - data['MACD_signal']

        # Удаляем ненужные столбцы 'end' и возвращаем индекс как столбец 'date'
        data.reset_index(inplace=True)
        data = data.drop(columns=['end'])
        data.rename(columns={'begin': 'date'}, inplace=True)

        # Сохраняем в CSV
        filename = f"{ticker}_{timeframe.upper()}_{period_years}Y.csv"
        file_path = os.path.join(HISTORICAL_DATA_DIR, filename)
        data.to_csv(file_path, index=False)
        print(f"Исторические данные с индикаторами сохранены: {file_path}")
        return data
    return None


# Функция для анализа отчетов МСФО и РСБУ с помощью LLM
def analyze_msfo_report(ticker, base_ticker, chat_id):
    msfo_file = os.path.join(REPORTS_DIR, f"{base_ticker}-МСФО-годовые.csv")
    rsbu_file = os.path.join(REPORTS_DIR, f"{base_ticker}-РСБУ-годовые.csv")

    msfo_content = None
    rsbu_content = None

    if os.path.exists(msfo_file):
        msfo_content = read_csv_file(msfo_file)
        if not msfo_content:
            return f"Не удалось прочитать отчет МСФО для {base_ticker}."
    else:
        return f"Отчет МСФО для {base_ticker} не найден в папке '{REPORTS_DIR}'."

    if os.path.exists(rsbu_file):
        rsbu_content = read_csv_file(rsbu_file)
        if not rsbu_content:
            print(f"Не удалось прочитать отчет РСБУ для {base_ticker}, продолжаем с МСФО.")

    bot.send_message(chat_id, "Пожалуйста подождите, работает LLM.")
    system_message = """
Ты финансовый аналитик, анализирующий отчеты по стандартам МСФО и РСБУ из CSV-файлов. 
Содержимое МСФО (разделитель ';', данные за годы в столбцах, включая LTM): 
{msfo_content}
Содержимое РСБУ (если доступно, разделитель ';', данные за годы в столбцах): 
{rsbu_content}

Задача:
1. Извлеки список всех годов из заголовков столбцов МСФО (начиная с 2008 по LTM). Если РСБУ доступен, убедись, что годы совпадают или дополняют МСФО.
2. Определи ключевые финансовые показатели:
   - Из МСФО (приоритет): Чистый операционный доход, Чистая прибыль, Активы банка, Капитал, Кредитный портфель, Депозиты, P/E, P/B, EV/EBITDA.
   - Из РСБУ (дополнение): Выручка, Себестоимость, Прибыль до налогообложения, EBITDA (если есть), Амортизация (если есть). Добавляй "(РСБУ)" к показателям из РСБУ.
3. Для EV/EBITDA:
   - Если есть в МСФО, используй напрямую.
   - Если нет, рассчитай как EV / EBITDA (из МСФО или РСБУ). Если EBITDA нет, используй "Операционная прибыль" (из МСФО) или "Прибыль до налогообложения + Амортизация" (из РСБУ, если есть), с пометкой "(приблизительно)".
4. Верни результат в формате:
   - "Данные за годы: 2008 | 2009 | ... | LTM"
   - "Показатель: Значение 2008 | Значение 2009 | ... | Значение LTM (единица измерения, если указана)"
   Раздели строки переносом.
5. Если данных за год нет, укажи "н/д". Если показатель доступен только в РСБУ, возьми его оттуда.

Правила:
- Работай только с данными из CSV, не придумывай сверх того.
- Если формат данных неясен, верни сообщение об ошибке.
- Ограничь ответ 500 токенами.
"""
    try:
        response = client.chat.completions.create(
            model="deepseek-r1-distill-qwen-14b",
            messages=[
                {"role": "system", "content": system_message.format(msfo_content=msfo_content,
                                                                    rsbu_content=rsbu_content if rsbu_content else "Отсутствует")},
                {"role": "user", "content": f"Анализируй отчеты для тикера {base_ticker}."}
            ],
            max_tokens=10000,
            temperature=0.1
        )
        raw_response = response.choices[0].message.content.strip()
        result = re.sub(r'<think>.*?</think>\s*|<think>.*$|</think>\s*', '', raw_response, flags=re.DOTALL).strip()
        return result
    except Exception as e:
        return f"Ошибка при анализе отчетов для {base_ticker}: {str(e)}"


# Функция для получения тикеров с учетом опечаток и полного/частичного совпадения
def get_company_tickers(company_name, file_content, chat_id):
    bot.send_message(chat_id, "Пожалуйста подождите, работает LLM.")
    system_message = f"""
Ты помощник, анализирующий CSV-файл с данными о компаниях (столбцы: 'ticker', 'official_name'). Содержимое: {file_content}.
Запрос: '{company_name}'.

Задача:
1. Найди тикеры компаний из CSV:
   - Сначала ищи полное совпадение '{company_name}' с 'official_name' (регистр не важен).
   - Если нет полного совпадения, ищи частичное совпадение, где '{company_name}' — значимая подстрока названия.
   - Учитывай опечатки до 2 букв (например, 'Сбер' может быть 'Сбр' или 'Себр').
2. Для каждой найденной компании:
   - Если есть привилегированные акции ('P' в тикере или 'ап' в названии), верни оба тикера через запятую (например, 'SBER,SBERP').
   - Иначе верни только один тикер.
3. Если найдено несколько компаний, раздели их тикеры символом '|' (например, 'SBER,SBERP|VTBR').
4. Если нет совпадений, верни: 'Извините, компания не найдена. Попробуйте скорректировать запрос.'

Примеры:
- Запрос 'Сбер' → 'SBER,SBERP' (только Сбербанк, НЕ ВТБ, МТС Банк или Совкомбанк).
- Запрос 'Сбр' → 'SBER,SBERP' (опечатка, всё равно Сбербанк).
- Запрос 'банк' → 'SBER,SBERP|VTBR|MBNK|SVCB' (Сбербанк, ВТБ, МТС Банк, Совкомбанк).
- Запрос 'пром' → 'HIMCP|GAZP' (Химпром, Газпром).
- Запрос 'флот' → 'AFLT|FLOT,FLOTP' (Аэрофлот, Совкомфлот).
- Запрос 'нефть' → 'RNFT|ROSN|TRNFP' (РуссНефть, Роснефть, Транснефть).
- Запрос 'xyz' → 'Извините, компания не найдена. Попробуйте скорректировать запрос.'

Правила:
- Работай ТОЛЬКО с текстом из CSV, игнорируй знания вне данных.
- Сначала проверяй полное совпадение, затем частичное, затем с опечатками до 2 букв.
- '{company_name}' должно быть точной подстрокой названия или отличаться не более чем на 2 буквы (добавление, удаление, замена).
- НЕ ВКЛЮЧАЙ компании, где '{company_name}' не является частью названия или не подходит по опечаткам.
- Проверяй наличие обычных и привилегированных акций по 'P' или 'ап'.
- ВЫВОДИ ТОЛЬКО ТИКЕРЫ ИЛИ СООБЩЕНИЕ ОБ ОШИБКЕ — НИКАКОГО ТЕКСТА, ТЕГОВ ИЛИ РАССУЖДЕНИЙ.
- СТРОГО ЗАПРЕЩЕНО ДОБАВЛЯТЬ <think>, <reasoning> ИЛИ ЛЮБЫЕ ОБЪЯСНЕНИЯ.
- НЕ ВКЛЮЧАЙ ПОВТОРЯЮЩИЕСЯ ТИКЕРЫ.
- Ограничь ответ 50 токенами.
"""
    try:
        response = client.chat.completions.create(
            model="deepseek-r1-distill-qwen-14b",
            messages=[
                {"role": "system", "content": system_message},
                {"role": "user", "content": company_name}
            ],
            max_tokens=10000,
            temperature=0.1
        )
        raw_response = response.choices[0].message.content.strip()
        token_estimate = len(raw_response) // 4 + 1
        print(f"Сырой ответ LLM: {raw_response}")
        print(f"Длина ответа: {len(raw_response)} символов, ~{token_estimate} токенов")

        result = re.sub(r'<think>.*?</think>\s*|<think>.*$|</think>\s*', '', raw_response, flags=re.DOTALL).strip()
        if "Извините" in result:
            return "Извините, компания не найдена. Попробуйте скорректировать запрос."
        result = re.sub(r'[^A-Z,|]', '', result).strip()
        if not result:
            return "Извините, компания не найдена. Попробуйте скорректировать запрос."

        unique_tickers = []
        for company in result.split('|'):
            tickers = company.split(',')
            if len(tickers) == 2 and tickers[0] == tickers[1].replace('P', ''):
                unique_tickers.append(tickers[0] + ',' + tickers[1])
            else:
                unique_tickers.extend([t for t in tickers if t and t not in unique_tickers])
        return '|'.join(
            unique_tickers) if unique_tickers else "Извините, компания не найдена. Попробуйте скорректировать запрос."
    except Exception as e:
        print(f"Ошибка запроса к LLM: {str(e)}")
        return None


# Функция для создания клавиатуры с таймфреймами
def get_timeframe_keyboard():
    keyboard = InlineKeyboardMarkup()
    keyboard.row(
        InlineKeyboardButton("1m", callback_data="1m"),
        InlineKeyboardButton("10m", callback_data="10m"),
        InlineKeyboardButton("1h", callback_data="1h")
    )
    keyboard.row(
        InlineKeyboardButton("Daily", callback_data="daily"),
        InlineKeyboardButton("Weekly", callback_data="weekly")
    )
    keyboard.row(
        InlineKeyboardButton("Monthly", callback_data="monthly"),
        InlineKeyboardButton("Quarterly", callback_data="quarterly")
    )
    return keyboard


# Функция для создания клавиатуры "Да/Нет"
def get_plot_keyboard():
    keyboard = InlineKeyboardMarkup()
    keyboard.row(
        InlineKeyboardButton("Да", callback_data="yes_plot"),
        InlineKeyboardButton("Нет", callback_data="no_plot")
    )
    return keyboard


# Функция для построения и отправки графиков
def plot_and_send_chart(chat_id, ticker, timeframe, period_years, data, base_ticker):
    # Первый график: только цена закрытия
    plt.figure(figsize=(10, 5))
    plt.plot(data['date'], data['close'], label=f"{ticker} ({timeframe})")
    plt.title(f"Цена акции {ticker} за {period_years} лет")
    plt.xlabel("Дата")
    plt.ylabel("Цена закрытия")
    plt.legend()
    plt.grid()
    plt.xticks(rotation=45)
    chart_path = f"{ticker}_{timeframe}_{period_years}Y_chart.png"
    plt.savefig(chart_path, bbox_inches='tight')
    plt.close()

    with open(chart_path, 'rb') as photo:
        bot.send_photo(chat_id, photo)
    os.remove(chart_path)

    # Второй график: цена с индикаторами
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 12), gridspec_kw={'height_ratios': [3, 1, 1]}, sharex=True)

    # Основной график: цена, SMA, EMA, Bollinger Bands
    ax1.plot(data['date'], data['close'], label='Close', color='blue')
    ax1.plot(data['date'], data['SMA_20'], label='SMA 20', color='orange', linestyle='--')
    ax1.plot(data['date'], data['EMA_20'], label='EMA 20', color='green', linestyle='--')
    ax1.plot(data['date'], data['BB_upper'], label='BB Upper', color='red', linestyle='-.')
    ax1.plot(data['date'], data['BB_lower'], label='BB Lower', color='red', linestyle='-.')
    ax1.fill_between(data['date'], data['BB_upper'], data['BB_lower'], color='red', alpha=0.1)
    ax1.set_title(f"Технический анализ {ticker} ({timeframe}) за {period_years} лет")
    ax1.set_ylabel("Цена")
    ax1.legend(loc='upper left')
    ax1.grid()

    # RSI
    ax2.plot(data['date'], data['RSI_14'], label='RSI 14', color='purple')
    ax2.axhline(70, color='red', linestyle='--', alpha=0.5)
    ax2.axhline(30, color='green', linestyle='--', alpha=0.5)
    ax2.set_ylabel("RSI")
    ax2.legend(loc='upper left')
    ax2.grid()

    # MACD
    ax3.plot(data['date'], data['MACD'], label='MACD', color='blue')
    ax3.plot(data['date'], data['MACD_signal'], label='Signal', color='orange')
    ax3.bar(data['date'], data['MACD_histogram'], label='Histogram', color='gray', alpha=0.5)
    ax3.set_xlabel("Дата")
    ax3.set_ylabel("MACD")
    ax3.legend(loc='upper left')
    ax3.grid()

    plt.xticks(rotation=45)
    plt.tight_layout()
    indicators_chart_path = f"{ticker}_{timeframe}_{period_years}Y_indicators.png"
    plt.savefig(indicators_chart_path, bbox_inches='tight')
    plt.close()

    with open(indicators_chart_path, 'rb') as photo:
        bot.send_photo(chat_id, photo)
    os.remove(indicators_chart_path)

    # Анализ отчетов и отправка показателей
    msfo_analysis = analyze_msfo_report(ticker, base_ticker, chat_id)
    bot.send_message(chat_id, f"Ключевые показатели для {base_ticker}:\n{msfo_analysis}")


# Функция для повторного вопроса о компании
def ask_next_company(chat_id):
    bot.send_message(chat_id, "Тикер какой компании Вас интересует?")
    user_states[chat_id] = {"step": "ask_company"}


# Обработчик команды /start
@bot.message_handler(commands=['start'])
def handle_start(message):
    chat_id = message.chat.id
    ask_next_company(chat_id)


# Обработчик текстовых сообщений
@bot.message_handler(content_types=['text'])
def handle_message(message):
    chat_id = message.chat.id
    raw_message = message.text
    user_message = re.sub(r'<think>.*?</think>\s*|<think>.*$|</think>\s*', '', raw_message, flags=re.DOTALL).strip()
    user_message = '\n'.join(line for line in user_message.split('\n') if line.strip())
    print(f"Очищенный запрос пользователя: '{user_message}'")

    try:
        if not user_message:
            bot.send_message(chat_id, "Пожалуйста, введите название компании.")
            ask_next_company(chat_id)
            return

        if chat_id not in user_states:
            ask_next_company(chat_id)
            return

        file_content = read_file_content(FILE_PATH)
        if not file_content:
            bot.send_message(chat_id, "Ошибка: не удалось загрузить данные о компаниях.")
            ask_next_company(chat_id)
            return

        state = user_states[chat_id]["step"]

        if state == "ask_company":
            company_name = user_message
            tickers = get_company_tickers(company_name, file_content, chat_id)
            if not tickers:
                bot.send_message(chat_id, "Произошла ошибка при запросе к LLM. Проверьте сервер или модель.")
                ask_next_company(chat_id)
                return

            if tickers == "Извините, компания не найдена. Попробуйте скорректировать запрос.":
                bot.send_message(chat_id, tickers)
                ask_next_company(chat_id)
                return

            company_list = tickers.split("|")
            if len(company_list) == 1:
                ticker_list = company_list[0].split(",")
                if len(ticker_list) == 2:
                    user_states[chat_id] = {
                        "step": "choose_type",
                        "tickers": ticker_list,
                        "base_ticker": ticker_list[0],
                        "company": company_name
                    }
                    bot.send_message(chat_id, "Выберите тип акции:\n1 — обычная\n2 — привилегированная")
                else:
                    user_states[chat_id] = {
                        "step": "ask_timeframe",
                        "ticker": ticker_list[0],
                        "base_ticker": ticker_list[0],
                        "company": company_name
                    }
                    bot.send_message(chat_id, f"Тикер: {ticker_list[0]}")
                    bot.send_message(chat_id, "Выберите таймфрейм:", reply_markup=get_timeframe_keyboard())
            else:
                options = []
                ticker_options = []
                for i, company_tickers in enumerate(company_list, 1):
                    ticker_list = company_tickers.split(",")
                    company_name_in_file = \
                    [line.split()[1] for line in file_content.split("\n") if ticker_list[0] in line][0]
                    options.append(f"{i} — {company_name_in_file} ({company_tickers})")
                    ticker_options.append(ticker_list)
                user_states[chat_id] = {
                    "step": "choose_company",
                    "companies": ticker_options,
                    "original_query": company_name
                }
                bot.send_message(chat_id, "Какую компанию Вы имели в виду?\n" + "\n".join(options))

        elif state == "choose_type":
            choice = user_message.strip()
            ticker_list = user_states[chat_id]["tickers"]
            base_ticker = user_states[chat_id]["base_ticker"]
            if choice == "1":
                selected_ticker = ticker_list[0]
                is_preferred = False
            elif choice == "2":
                selected_ticker = ticker_list[1]
                is_preferred = True
            else:
                bot.send_message(chat_id, "Пожалуйста, выберите 1 или 2.")
                return

            user_states[chat_id] = {
                "step": "ask_timeframe",
                "ticker": selected_ticker,
                "base_ticker": base_ticker,
                "is_preferred": is_preferred,
                "company": user_states[chat_id]["company"]
            }
            bot.send_message(chat_id, f"Тикер: {selected_ticker}")
            bot.send_message(chat_id, "Выберите таймфрейм:", reply_markup=get_timeframe_keyboard())

        elif state == "choose_company":
            choice = user_message.strip()
            try:
                choice_idx = int(choice) - 1
                if 0 <= choice_idx < len(user_states[chat_id]["companies"]):
                    ticker_list = user_states[chat_id]["companies"][choice_idx]
                    if len(ticker_list) == 2:
                        user_states[chat_id] = {
                            "step": "choose_type",
                            "tickers": ticker_list,
                            "base_ticker": ticker_list[0],
                            "company": user_states[chat_id]["original_query"]
                        }
                        bot.send_message(chat_id, "Выберите тип акции:\n1 — обычная\n2 — привилегированная")
                    else:
                        user_states[chat_id] = {
                            "step": "ask_timeframe",
                            "ticker": ticker_list[0],
                            "base_ticker": ticker_list[0],
                            "company": user_states[chat_id]["original_query"]
                        }
                        bot.send_message(chat_id, f"Тикер: {ticker_list[0]}")
                        bot.send_message(chat_id, "Выберите таймфрейм:", reply_markup=get_timeframe_keyboard())
                else:
                    bot.send_message(chat_id, "Пожалуйста, выберите номер из списка.")
            except ValueError:
                bot.send_message(chat_id, "Пожалуйста, введите цифру для выбора компании.")

        elif state == "ask_period":
            try:
                period_years = int(user_message.strip())
                if period_years <= 0:
                    raise ValueError
                ticker = user_states[chat_id]["ticker"]
                base_ticker = user_states[chat_id]["base_ticker"]
                is_preferred = user_states[chat_id].get("is_preferred", False)
                timeframe = user_states[chat_id]["timeframe"]
                data = save_historical_data(ticker, timeframe, period_years)
                if data is not None:
                    bot.send_message(chat_id,
                                     f"Данные для {ticker} ({timeframe}, {period_years} лет) сохранены в папку '{HISTORICAL_DATA_DIR}'.")
                    download_reports(ticker, is_preferred, base_ticker)
                    bot.send_message(chat_id,
                                     f"Отчеты для {base_ticker} (МСФО и РСБУ) сохранены в папку '{REPORTS_DIR}'.")
                    user_states[chat_id]["data"] = data
                    user_states[chat_id]["period_years"] = period_years
                    bot.send_message(chat_id, "Вывести график цены акции за указанный период?",
                                     reply_markup=get_plot_keyboard())
                else:
                    bot.send_message(chat_id, f"Не удалось получить данные для {ticker}.")
                    ask_next_company(chat_id)
            except ValueError:
                bot.send_message(chat_id, "Пожалуйста, введите положительное целое число для периода в годах.")
            except Exception as e:
                bot.send_message(chat_id, f"Ошибка при обработке данных: {str(e)}")
                ask_next_company(chat_id)

    except Exception as e:
        bot.send_message(chat_id, f"Произошла ошибка: {str(e)}")
        ask_next_company(chat_id)


# Обработчик callback-запросов от inline-клавиатуры
@bot.callback_query_handler(func=lambda call: True)
def handle_callback(call):
    chat_id = call.message.chat.id
    if chat_id not in user_states:
        bot.answer_callback_query(call.id, "Сессия устарела. Начните заново с /start.")
        return

    state = user_states[chat_id]["step"]

    if state == "ask_timeframe":
        timeframe = call.data
        user_states[chat_id]["timeframe"] = timeframe
        user_states[chat_id]["step"] = "ask_period"
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text=f"Выбран таймфрейм: {timeframe}. Введите период в годах (например, 1, 2, 5):"
        )
        bot.answer_callback_query(call.id)

    elif state == "ask_period":
        if call.data == "yes_plot":
            ticker = user_states[chat_id]["ticker"]
            base_ticker = user_states[chat_id]["base_ticker"]
            timeframe = user_states[chat_id]["timeframe"]
            period_years = user_states[chat_id]["period_years"]
            data = user_states[chat_id]["data"]
            plot_and_send_chart(chat_id, ticker, timeframe, period_years, data, base_ticker)
            ask_next_company(chat_id)
        elif call.data == "no_plot":
            ask_next_company(chat_id)
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text="Обработка завершена."
        )
        bot.answer_callback_query(call.id)


# Запуск бота
if __name__ == "__main__":
    read_file_content(FILE_PATH)
    if FILE_CONTENT is None:
        print("Не удалось запустить бота: ошибка чтения файла.")
    else:
        print("Бот запущен...")
        bot.polling(none_stop=True, timeout=60)