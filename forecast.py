import pandas as pd
from gigachat import GigaChat
from bot_config import GIGACHAT_API_KEY, VERIFY_SSL_CERTS
from openai import OpenAI
import os
from datetime import datetime
from data_processing import save_historical_data, download_reports, read_csv_file, read_monthly_macro_content, read_yearly_macro_content
import re

def short_term_forecast(ticker, chat_id, bot, base_ticker=None, is_preferred=False, model="local"):
    """
    Генерирует краткосрочный прогноз (1-3 месяца) для акции с таймфреймом 'weekly' за 1 год.
    Использует столбцы: date, close, SMA_50, MACD, ADX, RSI_21, VWAP.
    Включает текущую цену в ответ бота.
    Сохраняет промпт в папку 'prompts'.
    """
    print(f"short_term_forecast called with ticker={ticker}, model={model}")
    timeframe = "weekly"
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

    forecast_columns = ['date', 'close', 'SMA_50', 'MACD', 'ADX', 'RSI_21', 'VWAP']
    missing_columns = [col for col in forecast_columns if col not in data.columns]
    if missing_columns:
        bot.send_message(chat_id, f"Ошибка: отсутствуют столбцы {missing_columns} в данных для {ticker}.")
        print(f"Ошибка: отсутствуют столбцы {missing_columns}")
        return

    # Используем все данные за год вместо 15 точек
    forecast_data = data[forecast_columns].copy()
    forecast_data['date'] = pd.to_datetime(forecast_data['date']).dt.strftime('%Y-%m-%d')
    indicators = forecast_data.to_string(index=False)

    download_reports(ticker, is_preferred, base_ticker)
    msfo_file = os.path.join("reports", f"{base_ticker}-МСФО-годовые.csv")

    msfo_content = None
    if os.path.exists(msfo_file):
        msfo_df = read_csv_file(msfo_file)
        if msfo_df is not None:
            msfo_content = msfo_df.to_string(index=False)

    monthly_macro_df = read_monthly_macro_content()
    yearly_macro_df = read_yearly_macro_content()
    monthly_macro_content = None
    yearly_macro_content = None
    if monthly_macro_df is not None:
        monthly_macro_df['Дата'] = pd.to_datetime(monthly_macro_df['Дата'], format='%Y-%m')
        monthly_macro_df = monthly_macro_df[monthly_macro_df['Дата'].dt.year >= 2024]
        monthly_macro_content = monthly_macro_df.to_string(index=False)
    if yearly_macro_df is not None:
        yearly_macro_df = yearly_macro_df[yearly_macro_df['Год'] >= 2024]
        yearly_macro_content = yearly_macro_df.to_string(index=False)

    bot.send_message(chat_id, "Формируется краткосрочный прогноз, подождите.")

    # Сжатый промпт для GigaChat с данными за год
    gigachat_prompt = f"""
Ты финансовый аналитик, прогнозирующий цену акции (тикер: {ticker}) на 1-3 месяца по данным за год (weekly), МСФО и макроэкономике России.

Текущая цена: {current_price}

Данные (год, weekly):
{indicators}

МСФО:
{msfo_content if msfo_content else 'Отсутствует'}

Макро (месячные, 2024-2025):
{monthly_macro_content if monthly_macro_content else 'Отсутствует'}

Макро (годовые, 2024-2025):
{yearly_macro_content if yearly_macro_content else 'Отсутствует'}

Индикаторы:
- SMA_50: простая скользящая (50)
- MACD: (24, 52, 9)
- ADX: сила тренда (20)
- RSI_21: относительная сила (21)
- VWAP: объёмно-взвешенная цена

Задача:
1. Анализируй тренд (ADX), импульс (MACD), перекупленность/перепроданность (RSI_21), сравни close с SMA_50, VWAP.
2. Учти МСФО (прибыль, активы, EV/EBITDA) и макроэкономику (инфляция, ставка, USD/RUB).
3. Прогноз на 1-3 месяца: направление (рост, падение, боковик), вероятность (%), поддержка/сопротивление.
4. Рекомендация: Активно продавать/Продавать/Держать/Покупать/Активно покупать (по всем данным и по индикаторам с обоснованием).

Формат ответа:
Текущая цена: [число]
Прогноз: [направление] ([число])
Поддержка: [число], Сопротивление: [число]
Рекомендация (все данные): [действие]
Рекомендация (индикаторы): [действие] - [обоснование]
Комментарий: [обоснование: индикаторы, МСФО, макро]

Правила:
- Не используй Markdown (**, #, $, %, *, -), заголовки, списки или лишние пробелы/переносы.
- Числа без единиц измерения (например, 1273.5, 60, 1150).
- Каждая строка формата ответа начинается с указанного заголовка, без отступов.
"""

    # Промпт для локальной LLM (без изменений)
    local_llm_prompt = f"""
Ты финансовый аналитик, специализирующийся на краткосрочных прогнозах (1-3 месяца). Используй данные акции (тикер: {ticker}) за последние 15 недель (таймфрейм: weekly), финансовые показатели МСФО и макроэкономические данные России для прогнозирования цены акции.

**Текущая цена акции:** {current_price}

**Исторические данные (последние 15 точек, weekly):**
{indicators}

**Финансовые показатели МСФО:**
{msfo_content if msfo_content else 'Отсутствует'}

**Финансовые показатели РСБУ (если доступны):**
{msfo_content if msfo_content else 'Отсутствует'}  # Используем МСФО вместо РСБУ

**Месячные макроэкономические данные России (2024-2025):**
{monthly_macro_content if monthly_macro_content else 'Отсутствует'}

**Годовые макроэкономические данные России (2024-2025):**
{yearly_macro_content if yearly_macro_content else 'Отсутствует'}

**Индикаторы:**
- SMA_50: 50-периодная простая скользящая средняя
- MACD: индикатор (24, 52, 9)
- ADX: индекс силы тренда (20-периодный)
- RSI_21: индекс относительной силы (21-периодный)
- VWAP: объёмно-взвешенная средняя цена

**Задача:**
1. Проанализируй данные:
   - Оцени тренд (ADX), импульс (MACD), перекупленность/перепроданность (RSI_21).
   - Сравни close с SMA_50 и VWAP для определения направления.
2. Учти финансовые показатели МСФО (Чистая прибыль, Активы, EV/EBITDA).
3. Учти макроэкономику (Инфляция, Ключевая ставка, Курс USD/RUB).
4. Спрогнозируй движение цены на 1-3 месяца:
   - Укажи направление (рост, падение, боковик).
   - Дай вероятность основного сценария (%).
   - Определи уровни поддержки и сопротивления.
5. Дай рекомендацию по действиям с акцией (Активно продавать, Продавать, Держать, Покупать, Активно покупать) на основе всех данных.
6. Дай рекомендацию на основе индикаторов (ADX, RSI_21, MACD) с обоснованием.

**Формат ответа:**
- Текущая цена: [значение]
- Прогноз: [направление] (вероятность X%)
- Поддержка: [уровень], Сопротивление: [уровень]
- Рекомендация (все данные): [действие]
- Рекомендация (индикаторы): [действие] — [обоснование]
- Комментарий: [краткое обоснование с учётом индикаторов, МСФО и макроэкономики]

**Правила:**
- Опирайся только на предоставленные данные.
- Ответ не более 500 токенов.
- Входной промпт до 50,000 токенов.
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
                result = re.sub(r'<think>.*?</think>\s*|<think>.*$|</think>\s*', '', raw_response, flags=re.DOTALL).strip()
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
                max_tokens=50990,
                temperature=0.3
            )
            raw_response = response.choices[0].message.content.strip()
            result = re.sub(r'<think>.*?</think>\s*|<think>.*$|</think>\s*', '', raw_response, flags=re.DOTALL).strip()
            bot.send_message(chat_id, result)
            print(f"Краткосрочный прогноз для {ticker} отправлен: {result}")
    except Exception as e:
        error_msg = f"Ошибка при формировании прогноза для {ticker}: {str(e)}\nТип ошибки: {type(e).__name__}"
        bot.send_message(chat_id, error_msg)
        print(error_msg)