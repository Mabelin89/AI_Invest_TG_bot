import pandas as pd
from gigachat import GigaChat
from bot_config import GIGACHAT_API_KEY, VERIFY_SSL_CERTS
from openai import OpenAI
import os
from datetime import datetime
from data_processing import save_historical_data, download_reports, read_csv_file, read_monthly_macro_content, read_yearly_macro_content, optimize_msfo_content, optimize_monthly_macro, optimize_yearly_macro, calculate_indicators
from moex_parser import get_historical_data
import re

def short_term_forecast(ticker, chat_id, bot, base_ticker=None, is_preferred=False, model="local"):
    """
    Генерирует краткосрочный прогноз (1-3 месяца) для акции с таймфреймом 'daily' за 1 год.
    Использует столбцы: date, close, SMA_20, SMA_50, MACD, ADX_14, RSI_14, VWAP.
    Включает текущую цену в ответ бота.
    Сохраняет промпт в папку 'prompts'.
    """
    print(f"short_term_forecast called with ticker={ticker}, model={model}")
    timeframe = "daily"
    period_years = 1

    if base_ticker is None:
        base_ticker = ticker

    prompts_dir = os.path.join(os.getcwd(), "prompts")
    if not os.path.exists(prompts_dir):
        os.makedirs(prompts_dir)

    data = save_historical_data(ticker, timeframe, period_years)
    if data is None or data.empty:
        bot.send_message(chat_id, f"Не удалось получить исторические данные для {ticker}.")
        print(f"Ошибка: исторические данные для {ticker} ({timeframe}, {period_years}Y) недоступны")
        return

    current_price = data['close'].iloc[-1] if 'close' in data.columns else None
    if current_price is None:
        bot.send_message(chat_id, f"Ошибка: не удалось определить текущую цену для {ticker}.")
        print(f"Ошибка: столбец close отсутствует в данных для {ticker}")
        return

    forecast_columns = ['date', 'close', 'SMA_20', 'SMA_50', 'MACD', 'ADX_14', 'RSI_14', 'VWAP']
    missing_columns = [col for col in forecast_columns if col not in data.columns]
    if missing_columns:
        bot.send_message(chat_id, f"Ошибка: отсутствуют столбцы {missing_columns} в данных для {ticker}.")
        print(f"Ошибка: отсутствуют столбцы {missing_columns}")
        return

    forecast_data = data[forecast_columns].copy()
    forecast_data['date'] = pd.to_datetime(forecast_data['date']).dt.strftime('%Y-%m-%d')
    indicators = forecast_data.to_csv(index=False, header=True, sep='|', lineterminator='\n')
    indicators = re.sub(r'[ \t]+', '', indicators)

    download_reports(ticker, is_preferred, base_ticker)
    msfo_file = os.path.join("reports", f"{base_ticker}-МСФО-годовые.csv")

    msfo_content = "Отсутствует"
    if os.path.exists(msfo_file):
        try:
            msfo_df = read_csv_file(msfo_file)
            if msfo_df is not None:
                msfo_content = optimize_msfo_content(msfo_df)
            else:
                print(f"Ошибка: не удалось прочитать МСФО для {base_ticker}")
        except Exception as e:
            print(f"Ошибка чтения МСФО для {base_ticker}: {str(e)}")

    monthly_macro_content = "Отсутствует"
    try:
        monthly_macro_df = read_monthly_macro_content()
        if monthly_macro_df is not None and not monthly_macro_df.empty:
            monthly_macro_df['Дата'] = pd.to_datetime(monthly_macro_df['Дата'], format='%Y-%m', errors='coerce')
            monthly_macro_df = monthly_macro_df[monthly_macro_df['Дата'].dt.year >= 2024]
            monthly_macro_content = optimize_monthly_macro(monthly_macro_df)
        else:
            print("Месячные макро-данные недоступны")
    except Exception as e:
        print(f"Ошибка обработки месячных макро-данных: {str(e)}")

    yearly_macro_content = "Отсутствует"
    try:
        yearly_macro_df = read_yearly_macro_content()
        if yearly_macro_df is not None and not yearly_macro_df.empty:
            yearly_macro_df = yearly_macro_df[yearly_macro_df['Год'] >= 2024]
            yearly_macro_content = optimize_yearly_macro(yearly_macro_df)
        else:
            print("Годовые макро-данные недоступны")
    except Exception as e:
        print(f"Ошибка обработки годовых макро-данных: {str(e)}")

    bot.send_message(chat_id, "Формируется краткосрочный прогноз, подождите.")

    gigachat_prompt = f"""
Ты финансовый аналитик, прогнозирующий цену акции (тикер: {ticker}) на 1-3 месяца по данным за год (daily), МСФО и макроэкономике России.
Текущая цена:{current_price}
Данные (год, daily, дата|close|SMA_20|SMA_50|MACD|ADX_14|RSI_14|VWAP, |, \n):
{indicators}
МСФО (показатели|годы, |, \n):
{msfo_content}
Макро (месячные, 2024-2025, дата|CPI|Rate|USD/RUB, |, \n):
{monthly_macro_content}
Макро (годовые, 2024-2025, год|GDP|CPI|Unemployment|Rate|TradeBalance|BudgetDeficit|MOEX|USD/RUB|CCI, |, \n):
{yearly_macro_content}
Индикаторы:
SMA_20:простая скользящая (20 дней)
SMA_50:простая скользящая (50 дней)
MACD:(12,26,9)
ADX_14:сила тренда (14 дней)
RSI_14:относительная сила (14 дней)
VWAP:объёмно-взвешенная цена (дневная)
Задача:
1.Анализируй краткосрочный тренд (SMA_20), среднесрочный тренд (SMA_50), импульс (MACD), силу тренда (ADX_14), перекупленность/перепроданность (RSI_14), сравни close с VWAP.
2.Учти МСФО (NP, Assets, EV/EBITDA) и макро (GDP, CPI, Unemployment, Rate, TradeBalance, BudgetDeficit, MOEX, USD/RUB, CCI).
3.Прогноз на 1-3 месяца:направление (рост,падение,боковик), вероятность (%), поддержка/сопротивление.
4.Рекомендация:Активно продавать/Продавать/Держать/Покупать/Активно покупать (по всем данным и по индикаторам с обоснованием).
Формат ответа:
Текущая цена:[число]
Прогноз:[направление] ([число])
Поддержка:[число],Сопротивление:[число]
Рекомендация (все данные):[действие]
Рекомендация (индикаторы):[действие]|[обоснование]
Комментарий:[обоснование: индикаторы, МСФО, макро]
Правила:
Без Markdown

Текст должен быть поделен на абзацы.
"""

    local_llm_prompt = f"""
Ты финансовый аналитик, прогнозирующий цену акции (тикер: {ticker}) на 1-3 месяца по данным за год (daily), МСФО и макроэкономике России.
Текущая цена:{current_price}
Данные (год, daily, дата|close|SMA_20|SMA_50|MACD|ADX_14|RSI_14|VWAP, |, \n):
{indicators}
МСФО (показатели|годы, |, \n):
{msfo_content}
Макро (месячные, 2024-2025, дата|CPI|Rate|USD/RUB, |, \n):
{monthly_macro_content}
Макро (годовые, 2024-2025, год|GDP|CPI|Unemployment|Rate|TradeBalance|BudgetDeficit|MOEX|USD/RUB|CCI, |, \n):
{yearly_macro_content}
Индикаторы:
SMA_20:простая скользящая (20 дней)
SMA_50:простая скользящая (50 дней)
MACD:(12,26,9)
ADX_14:сила тренда (14 дней)
RSI_14:относительная сила (14 дней)
VWAP:объёмно-взвешенная цена (дневная)
Задача:
1.Анализируй краткосрочный тренд (SMA_20), среднесрочный тренд (SMA_50), импульс (MACD), силу тренда (ADX_14), перекупленность/перепроданность (RSI_14), сравни close с VWAP.
2.Учти МСФО (NP, Assets, EV/EBITDA) и макро (GDP, CPI, Unemployment, Rate, TradeBalance, BudgetDeficit, MOEX, USD/RUB, CCI).
3.Прогноз на 1-3 месяца:направление (рост,падение,боковик), вероятность (%), поддержка/сопротивление.
4.Рекомендация:Активно продавать/Продавать/Держать/Покупать/Активно покупать (по всем данным и по индикаторам с обоснованием).
Формат ответа:
Текущая цена:[число]
Прогноз:[направление] ([число])
Поддержка:[число],Сопротивление:[число]
Рекомендация (все данные):[действие]
Рекомендация (индикаторы):[действие]|[обоснование]
Комментарий:[обоснование: индикаторы, МСФО, макро]
Правила:
Без Markdown

Текст должен быть поделен на абзацы.
"""

    system_message = gigachat_prompt if model == "gigachat" else local_llm_prompt

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    prompt_filename = f"prompt_{ticker}_{timestamp}.txt"
    prompt_filepath = os.path.join(prompts_dir, prompt_filename)
    with open(prompt_filepath, "w", encoding="utf-8") as f:
        f.write(system_message)
    print(f"Промпт сохранён в файл: {prompt_filepath}")

    try:
        if model == "gigachat":
            print("Using GigaChat for forecast")
            with GigaChat(
                credentials=GIGACHAT_API_KEY,
                verify_ssl_certs=VERIFY_SSL_CERTS,
                model="GigaChat-2-Max"
            ) as gigachat_client:
                response = gigachat_client.chat(system_message)
                raw_response = response.choices[0].message.content.strip()
                print(f"Raw GigaChat response: {raw_response}")
                result = re.sub(r'<think>.*?(</think>|\s*$)', '', raw_response, flags=re.DOTALL).strip()
                bot.send_message(chat_id, result)
                print(f"Краткосрочный прогноз для {ticker} отправлен: {result}")
        else:
            print("Using local LLM for forecast")
            openai_client = OpenAI(base_url="http://localhost:1234/v1", api_key="not-needed")
            response = openai_client.chat.completions.create(
                model="deepseek-r1-distill-qwen-14b",
                messages=[
                    {"role": "system", "content": system_message},
                    {"role": "user", "content": f"Сделай краткосрочный прогноз для акции {ticker}."}
                ],
                max_tokens=500,
                temperature=0.3
            )
            raw_response = response.choices[0].message.content.strip()
            print(f"Raw local LLM response: {raw_response}")
            result = re.sub(r'<think>.*?(</think>|\s*$)', '', raw_response, flags=re.DOTALL).strip()
            bot.send_message(chat_id, result)
            print(f"Краткосрочный прогноз для {ticker} отправлен: {result}")
    except Exception as e:
        error_msg = f"Ошибка при формировании прогноза для {ticker}: {str(e)}\nТип ошибки: {type(e).__name__}"
        bot.send_message(chat_id, error_msg)
        print(error_msg)

def medium_term_forecast(ticker, chat_id, bot, base_ticker=None, is_preferred=False, model="local"):
    """
    Генерирует среднесрочный прогноз (3-6 месяцев) для акции с таймфреймом 'weekly' за 5 лет.
    Использует столбцы: date, close, SMA_20, SMA_50, MACD, ADX_14, RSI_14, VWAP.
    Включает текущую цену в ответ бота.
    Сохраняет промпт в папку 'prompts'.
    """
    print(f"medium_term_forecast called with ticker={ticker}, model={model}")
    timeframe = "weekly"
    period_years = 5

    if base_ticker is None:
        base_ticker = ticker

    prompts_dir = os.path.join(os.getcwd(), "prompts")
    if not os.path.exists(prompts_dir):
        os.makedirs(prompts_dir)

    print(f"Загрузка сырых данных для {ticker} ({timeframe}, {period_years}Y)")
    raw_data = get_historical_data(ticker, timeframe, period_years)
    if raw_data is None or raw_data.empty:
        bot.send_message(chat_id, f"Не удалось получить исторические данные для {ticker}.")
        print(f"Ошибка: исторические данные для {ticker} ({timeframe}, {period_years}Y) недоступны")
        return

    print(f"Сырые данные для {ticker}: {len(raw_data)} строк, столбцы: {list(raw_data.columns)}")

    data = calculate_indicators(
        raw_data,
        sma_periods=[20, 50],
        macd_params=(12, 26, 9),
        adx_period=14,
        rsi_period=14
    )
    if data is None or data.empty:
        bot.send_message(chat_id, f"Не удалось рассчитать индикаторы для {ticker}.")
        print(f"Ошибка: не удалось рассчитать индикаторы для {ticker}")
        return

    print(f"Данные с индикаторами для {ticker}: {len(data)} строк, столбцы: {list(data.columns)}")

    current_price = data['close'].iloc[-1] if 'close' in data.columns else None
    if current_price is None:
        bot.send_message(chat_id, f"Ошибка: не удалось определить текущую цену для {ticker}.")
        print(f"Ошибка: столбец close отсутствует в данных для {ticker}")
        return

    forecast_columns = ['date', 'close', 'SMA_20', 'SMA_50', 'MACD', 'ADX_14', 'RSI_14', 'VWAP']
    missing_columns = [col for col in forecast_columns if col not in data.columns]
    if missing_columns:
        bot.send_message(chat_id, f"Ошибка: отсутствуют столбцы {missing_columns} в данных для {ticker}.")
        print(f"Ошибка: отсутствуют столбцы {missing_columns}")
        return

    forecast_data = data[forecast_columns].copy()
    forecast_data['date'] = pd.to_datetime(forecast_data['date']).dt.strftime('%Y-%m-%d')
    indicators = forecast_data.to_csv(index=False, header=True, sep='|', lineterminator='\n')
    indicators = re.sub(r'[ \t]+', '', indicators)

    download_reports(ticker, is_preferred, base_ticker)
    msfo_file = os.path.join("reports", f"{base_ticker}-МСФО-годовые.csv")

    msfo_content = "Отсутствует"
    if os.path.exists(msfo_file):
        try:
            msfo_df = read_csv_file(msfo_file)
            if msfo_df is not None:
                msfo_content = optimize_msfo_content(msfo_df)
            else:
                print(f"Ошибка: не удалось прочитать МСФО для {base_ticker}")
        except Exception as e:
            print(f"Ошибка чтения МСФО для {base_ticker}: {str(e)}")

    monthly_macro_content = "Отсутствует"
    try:
        monthly_macro_df = read_monthly_macro_content()
        if monthly_macro_df is not None and not monthly_macro_df.empty:
            monthly_macro_df['Дата'] = pd.to_datetime(monthly_macro_df['Дата'], format='%Y-%m', errors='coerce')
            monthly_macro_df = monthly_macro_df[monthly_macro_df['Дата'].dt.year >= 2020]
            monthly_macro_content = optimize_monthly_macro(monthly_macro_df)
        else:
            print("Месячные макро-данные недоступны")
    except Exception as e:
        print(f"Ошибка обработки месячных макро-данных: {str(e)}")

    yearly_macro_content = "Отсутствует"
    try:
        yearly_macro_df = read_yearly_macro_content()
        if yearly_macro_df is not None and not yearly_macro_df.empty:
            yearly_macro_df = yearly_macro_df[yearly_macro_df['Год'] >= 2020]
            yearly_macro_content = optimize_yearly_macro(yearly_macro_df)
        else:
            print("Годовые макро-данные недоступны")
    except Exception as e:
        print(f"Ошибка обработки годовых макро-данных: {str(e)}")

    bot.send_message(chat_id, "Формируется среднесрочный прогноз, подождите.")

    gigachat_prompt = f"""
Ты финансовый аналитик, прогнозирующий цену акции (тикер: {ticker}) на 3-9 месяцев по данным за 5 лет (weekly), МСФО и макроэкономике России.
Текущая цена:{current_price}
Данные (5 лет, weekly, дата|close|SMA_20|SMA_50|MACD|ADX_14|RSI_14|VWAP, |, \n):
{indicators}
МСФО (показатели|годы, |, \n):
{msfo_content}
Макро (месячные, 2020-2025, дата|CPI|Rate|USD/RUB, |, \n):
{monthly_macro_content}
Макро (годовые, 2020-2025, год|GDP|CPI|Unemployment|Rate|TradeBalance|BudgetDeficit|MOEX|USD/RUB|CCI, |, \n):
{yearly_macro_content}
Индикаторы:
SMA_20:простая скользящая (20 недель)
SMA_50:простая скользящая (50 недель)
MACD:(12,26,9)
ADX_14:сила тренда (14 недель)
RSI_14:относительная сила (14 недель)
VWAP:объёмно-взвешенная цена (недельная)
Задача:
1.Анализируй среднесрочный тренд (SMA_20, SMA_50), импульс (MACD), силу тренда (ADX_14), перекупленность/перепроданность (RSI_14), сравни close с VWAP.
2.Учти МСФО (NP, Assets, EV/EBITDA) и макро (GDP, CPI, Unemployment, Rate, TradeBalance, BudgetDeficit, MOEX, USD/RUB, CCI).
3.Прогноз на 3-6 месяцев:направление (рост,падение,боковик), вероятность (%), поддержка/сопротивление.
4.Рекомендация:Активно продавать/Продавать/Держать/Покупать/Активно покупать (по всем данным и по индикаторам с обоснованием).
Формат ответа:
Текущая цена:[число]
Прогноз:[направление] ([число])
Поддержка:[число],Сопротивление:[число]
Рекомендация (все данные):[действие]
Рекомендация (индикаторы):[действие]|[обоснование]
Комментарий:[обоснование: индикаторы, МСФО, макро]
Правила:
Без Markdown

Текст должен быть поделен на абзацы.
"""

    local_llm_prompt = f"""
Ты финансовый аналитик, прогнозирующий цену акции (тикер: {ticker}) на 3-9 месяцев по данным за 5 лет (weekly), МСФО и макроэкономике России.
Текущая цена:{current_price}
Данные (5 лет, weekly, дата|close|SMA_20|SMA_50|MACD|ADX_14|RSI_14|VWAP, |, \n):
{indicators}
МСФО (показатели|годы, |, \n):
{msfo_content}
Макро (месячные, 2020-2025, дата|CPI|Rate|USD/RUB, |, \n):
{monthly_macro_content}
Макро (годовые, 2020-2025, год|GDP|CPI|Unemployment|Rate|TradeBalance|BudgetDeficit|MOEX|USD/RUB|CCI, |, \n):
{yearly_macro_content}
Индикаторы:
SMA_20:простая скользящая (20 недель)
SMA_50:простая скользящая (50 недель)
MACD:(12,26,9)
ADX_14:сила тренда (14 недель)
RSI_14:относительная сила (14 недель)
VWAP:объёмно-взвешенная цена (недельная)
Задача:
1.Анализируй среднесрочный тренд (SMA_20, SMA_50), импульс (MACD), силу тренда (ADX_14), перекупленность/перепроданность (RSI_14), сравни close с VWAP.
2.Учти МСФО (NP, Assets, EV/EBITDA) и макро (GDP, CPI, Unemployment, Rate, TradeBalance, BudgetDeficit, MOEX, USD/RUB, CCI).
3.Прогноз на 3-6 месяцев:направление (рост,падение,боковик), вероятность (%), поддержка/сопротивление.
4.Рекомендация:Активно продавать/Продавать/Держать/Покупать/Активно покупать (по всем данным и по индикаторам с обоснованием).
Формат ответа:
Текущая цена:[число]
Прогноз:[направление] ([число])
Поддержка:[число],Сопротивление:[число]
Рекомендация (все данные):[действие]
Рекомендация (индикаторы):[действие]|[обоснование]
Комментарий:[обоснование: индикаторы, МСФО, макро]
Правила:
Без Markdown

Текст должен быть поделен на абзацы.
"""

    system_message = gigachat_prompt if model == "gigachat" else local_llm_prompt

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    prompt_filename = f"prompt_{ticker}_{timestamp}.txt"
    prompt_filepath = os.path.join(prompts_dir, prompt_filename)
    with open(prompt_filepath, "w", encoding="utf-8") as f:
        f.write(system_message)
    print(f"Промпт сохранён в файл: {prompt_filepath}")

    try:
        if model == "gigachat":
            print("Using GigaChat for forecast")
            with GigaChat(
                credentials=GIGACHAT_API_KEY,
                verify_ssl_certs=VERIFY_SSL_CERTS,
                model="GigaChat-2-Max"
            ) as gigachat_client:
                response = gigachat_client.chat(system_message)
                raw_response = response.choices[0].message.content.strip()
                print(f"Raw GigaChat response: {raw_response}")
                result = re.sub(r'<think>.*?(</think>|\s*$)', '', raw_response, flags=re.DOTALL).strip()
                bot.send_message(chat_id, result)
                print(f"Среднесрочный прогноз для {ticker} отправлен: {result}")
        else:
            print("Using local LLM for forecast")
            openai_client = OpenAI(base_url="http://localhost:1234/v1", api_key="not-needed")
            response = openai_client.chat.completions.create(
                model="deepseek-r1-distill-qwen-14b",
                messages=[
                    {"role": "system", "content": system_message},
                    {"role": "user", "content": f"Сделай среднесрочный прогноз для акции {ticker}."}
                ],
                max_tokens=500,
                temperature=0.3
            )
            raw_response = response.choices[0].message.content.strip()
            print(f"Raw local LLM response: {raw_response}")
            result = re.sub(r'<think>.*?(</think>|\s*$)', '', raw_response, flags=re.DOTALL).strip()
            bot.send_message(chat_id, result)
            print(f"Среднесрочный прогноз для {ticker} отправлен: {result}")
    except Exception as e:
        error_msg = f"Ошибка при формировании прогноза для {ticker}: {str(e)}\nТип ошибки: {type(e).__name__}"
        bot.send_message(chat_id, error_msg)
        print(error_msg)

def long_term_forecast(ticker, chat_id, bot, base_ticker=None, is_preferred=False, model="local"):
    """
    Генерирует долгосрочный прогноз (более 1 года) для акции с таймфреймом 'weekly' за 10 лет.
    Использует столбцы: date, close, SMA_50, SMA_200, MACD, ADX_14, RSI_21, VWAP.
    Включает текущую цену в ответ бота.
    Сохраняет промпт в папку 'prompts'.
    """
    print(f"long_term_forecast called with ticker={ticker}, model={model}")
    timeframe = "weekly"
    period_years = 10

    if base_ticker is None:
        base_ticker = ticker

    prompts_dir = os.path.join(os.getcwd(), "prompts")
    if not os.path.exists(prompts_dir):
        os.makedirs(prompts_dir)

    print(f"Загрузка сырых данных для {ticker} ({timeframe}, {period_years}Y)")
    raw_data = get_historical_data(ticker, timeframe, period_years)
    if raw_data is None or raw_data.empty:
        bot.send_message(chat_id, f"Не удалось получить исторические данные для {ticker}.")
        print(f"Ошибка: исторические данные для {ticker} ({timeframe}, {period_years}Y) недоступны")
        return

    print(f"Сырые данные для {ticker}: {len(raw_data)} строк, столбцы: {list(raw_data.columns)}")

    data = calculate_indicators(
        raw_data,
        sma_periods=[50, 200],
        macd_params=(24, 52, 9),
        adx_period=14,
        rsi_period=21
    )
    if data is None or data.empty:
        bot.send_message(chat_id, f"Не удалось рассчитать индикаторы для {ticker}.")
        print(f"Ошибка: не удалось рассчитать индикаторы для {ticker}")
        return

    print(f"Данные с индикаторами для {ticker}: {len(data)} строк, столбцы: {list(data.columns)}")

    current_price = data['close'].iloc[-1] if 'close' in data.columns else None
    if current_price is None:
        bot.send_message(chat_id, f"Ошибка: не удалось определить текущую цену для {ticker}.")
        print(f"Ошибка: столбец close отсутствует в данных для {ticker}")
        return

    forecast_columns = ['date', 'close', 'SMA_50', 'SMA_200', 'MACD', 'ADX_14', 'RSI_21', 'VWAP']
    missing_columns = [col for col in forecast_columns if col not in data.columns]
    if missing_columns:
        bot.send_message(chat_id, f"Ошибка: отсутствуют столбцы {missing_columns} в данных для {ticker}.")
        print(f"Ошибка: отсутствуют столбцы {missing_columns}")
        return

    forecast_data = data[forecast_columns].copy()
    forecast_data['date'] = pd.to_datetime(forecast_data['date']).dt.strftime('%Y-%m-%d')
    indicators = forecast_data.to_csv(index=False, header=True, sep='|', lineterminator='\n')
    indicators = re.sub(r'[ \t]+', '', indicators)

    download_reports(ticker, is_preferred, base_ticker)
    msfo_file = os.path.join("reports", f"{base_ticker}-МСФО-годовые.csv")

    msfo_content = "Отсутствует"
    if os.path.exists(msfo_file):
        try:
            msfo_df = read_csv_file(msfo_file)
            if msfo_df is not None:
                msfo_content = optimize_msfo_content(msfo_df)
            else:
                print(f"Ошибка: не удалось прочитать МСФО для {base_ticker}")
        except Exception as e:
            print(f"Ошибка чтения МСФО для {base_ticker}: {str(e)}")

    monthly_macro_content = "Отсутствует"
    try:
        monthly_macro_df = read_monthly_macro_content()
        if monthly_macro_df is not None and not monthly_macro_df.empty:
            monthly_macro_df['Дата'] = pd.to_datetime(monthly_macro_df['Дата'], format='%Y-%m', errors='coerce')
            monthly_macro_df = monthly_macro_df[monthly_macro_df['Дата'].dt.year >= 2015]
            monthly_macro_content = optimize_monthly_macro(monthly_macro_df)
        else:
            print("Месячные макро-данные недоступны")
    except Exception as e:
        print(f"Ошибка обработки месячных макро-данных: {str(e)}")

    yearly_macro_content = "Отсутствует"
    try:
        yearly_macro_df = read_yearly_macro_content()
        if yearly_macro_df is not None and not yearly_macro_df.empty:
            yearly_macro_df = yearly_macro_df[yearly_macro_df['Год'] >= 2015]
            yearly_macro_content = optimize_yearly_macro(yearly_macro_df)
        else:
            print("Годовые макро-данные недоступны")
    except Exception as e:
        print(f"Ошибка обработки годовых макро-данных: {str(e)}")

    bot.send_message(chat_id, "Формируется долгосрочный прогноз, подождите.")

    gigachat_prompt = f"""
Ты финансовый аналитик, прогнозирующий цену акции (тикер: {ticker}) на более чем 1 год по данным за 10 лет (weekly), МСФО и макроэкономике России.
Текущая цена:{current_price}
Данные (10 лет, weekly, дата|close|SMA_50|SMA_200|MACD|ADX_14|RSI_21|VWAP, |, \n):
{indicators}
МСФО (показатели|годы, |, \n):
{msfo_content}
Макро (месячные, 2015-2025, дата|CPI|Rate|USD/RUB, |, \n):
{monthly_macro_content}
Макро (годовые, 2015-2025, год|GDP|CPI|Unemployment|Rate|TradeBalance|BudgetDeficit|MOEX|USD/RUB|CCI, |, \n):
{yearly_macro_content}
Индикаторы:
SMA_50:простая скользящая (50 недель)
SMA_200:простая скользящая (200 недель)
MACD:(24,52,9)
ADX_14:сила тренда (14 недель)
RSI_21:относительная сила (21 неделя)
VWAP:объёмно-взвешенная цена (недельная)
Задача:
1.Анализируй долгосрочный тренд (SMA_50, SMA_200), импульс (MACD), силу тренда (ADX_14), перекупленность/перепроданность (RSI_21), сравни close с VWAP.
2.Учти МСФО (NP, Assets, EV/EBITDA) и макро (GDP, CPI, Unemployment, Rate, TradeBalance, BudgetDeficit, MOEX, USD/RUB, CCI).
3.Прогноз на 6-12 месяцев:направление (рост,падение,боковик), вероятность (%), поддержка/сопротивление.
4.Рекомендация:Активно продавать/Продавать/Держать/Покупать/Активно покупать (по всем данным и по индикаторам с обоснованием).
Формат ответа:
Текущая цена:[число]
Прогноз:[направление] ([число])
Поддержка:[число],Сопротивление:[число]
Рекомендация (все данные):[действие]
Рекомендация (индикаторы):[действие]|[обоснование]
Комментарий:[обоснование: индикаторы, МСФО, макро]
Правила:
Без Markdown

Текст должен быть поделен на абзацы.
"""

    local_llm_prompt = f"""
Ты финансовый аналитик, прогнозирующий цену акции (тикер: {ticker}) на более чем 1 год по данным за 10 лет (weekly), МСФО и макроэкономике России.
Текущая цена:{current_price}
Данные (10 лет, weekly, дата|close|SMA_50|SMA_200|MACD|ADX_14|RSI_21|VWAP, |, \n):
{indicators}
МСФО (показатели|годы, |, \n):
{msfo_content}
Макро (месячные, 2015-2025, дата|CPI|Rate|USD/RUB, |, \n):
{monthly_macro_content}
Макро (годовые, 2015-2025, год|GDP|CPI|Unemployment|Rate|TradeBalance|BudgetDeficit|MOEX|USD/RUB|CCI, |, \n):
{yearly_macro_content}
Индикаторы:
SMA_50:простая скользящая (50 недель)
SMA_200:простая скользящая (200 недель)
MACD:(24,52,9)
ADX_14:сила тренда (14 недель)
RSI_21:относительная сила (21 неделя)
VWAP:объёмно-взвешенная цена (недельная)
Задача:
1.Анализируй долгосрочный тренд (SMA_50, SMA_200), импульс (MACD), силу тренда (ADX_14), перекупленность/перепроданность (RSI_21), сравни close с VWAP.
2.Учти МСФО (NP, Assets, EV/EBITDA) и макро (GDP, CPI, Unemployment, Rate, TradeBalance, BudgetDeficit, MOEX, USD/RUB, CCI).
3.Прогноз на 6-12 месяцев:направление (рост,падение,боковик), вероятность (%), поддержка/сопротивление.
4.Рекомендация:Активно продавать/Продавать/Держать/Покупать/Активно покупать (по всем данным и по индикаторам с обоснованием).
Формат ответа:
Текущая цена:[число]
Прогноз:[направление] ([число])
Поддержка:[число],Сопротивление:[число]
Рекомендация (все данные):[действие]
Рекомендация (индикаторы):[действие]|[обоснование]
Комментарий:[обоснование: индикаторы, МСФО, макро]
Правила:
Без Markdown

Текст должен быть поделен на абзацы.
"""

    system_message = gigachat_prompt if model == "gigachat" else local_llm_prompt

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    prompt_filename = f"prompt_{ticker}_{timestamp}.txt"
    prompt_filepath = os.path.join(prompts_dir, prompt_filename)
    with open(prompt_filepath, "w", encoding="utf-8") as f:
        f.write(system_message)
    print(f"Промпт сохранён в файл: {prompt_filepath}")

    try:
        if model == "gigachat":
            print("Using GigaChat for forecast")
            with GigaChat(
                credentials=GIGACHAT_API_KEY,
                verify_ssl_certs=VERIFY_SSL_CERTS,
                model="GigaChat-2-Max"
            ) as gigachat_client:
                response = gigachat_client.chat(system_message)
                raw_response = response.choices[0].message.content.strip()
                print(f"Raw GigaChat response: {raw_response}")
                result = re.sub(r'<think>.*?(</think>|\s*$)', '', raw_response, flags=re.DOTALL).strip()
                bot.send_message(chat_id, result)
                print(f"Долгосрочный прогноз для {ticker} отправлен: {result}")
        else:
            print("Using local LLM for forecast")
            openai_client = OpenAI(base_url="http://localhost:1234/v1", api_key="not-needed")
            response = openai_client.chat.completions.create(
                model="deepseek-r1-distill-qwen-14b",
                messages=[
                    {"role": "system", "content": system_message},
                    {"role": "user", "content": f"Сделай долгосрочный прогноз для акции {ticker}."}
                ],
                max_tokens=50000,
                temperature=0.3
            )
            raw_response = response.choices[0].message.content.strip()
            print(f"Raw local LLM response: {raw_response}")
            result = re.sub(r'<think>.*?(</think>|\s*$)', '', raw_response, flags=re.DOTALL).strip()
            bot.send_message(chat_id, result)
            print(f"Долгосрочный прогноз для {ticker} отправлен: {result}")
    except Exception as e:
        error_msg = f"Ошибка при формировании прогноза для {ticker}: {str(e)}\nТип ошибки: {type(e).__name__}"
        bot.send_message(chat_id, error_msg)
        print(error_msg)