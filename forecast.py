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
    Использует исторические данные, индикаторы, финансовые и макроэкономические показатели.
    Сохраняет промпт в папку 'prompts' в текстовом формате.
    """
    print(f"short_term_forecast called with ticker={ticker}, model={model}")  # Отладка
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

    indicators = data[['date', 'close', 'volume', 'SMA_50', 'SMA_100', 'SMA_200',
                      'EMA_50', 'EMA_100', 'EMA_200', 'MACD', 'MACD_signal',
                      'MACD_histogram', 'ADX', 'RSI_21', 'Stoch_K', 'Stoch_D',
                      'Stoch_Slow', 'OBV', 'VWAP']].tail(50).to_string(index=False)

    bot.send_message(chat_id, "Пожалуйста подождите, формируется краткосрочный прогноз через модель.")

    # Промпт для локальной LLM (оригинальный, с упоминанием РСБУ)
    local_llm_prompt = f"""
Ты финансовый аналитик, специализирующийся на краткосрочных прогнозах (1-3 месяца). Тебе предоставлены данные акции за последний год с таймфреймом 'weekly', финансовые показатели и макроэкономические данные России. Используй эти данные для формирования прогноза движения цены акции.

Исторические данные с индикаторами (последние 50 точек, таймфрейм 'weekly'):
{indicators}

Финансовые показатели МСФО:
{msfo_content}

Финансовые показатели РСБУ (если доступны):
{msfo_content}  # Используем МСФО вместо РСБУ, так как РСБУ исключено

Помесячные макроэкономические данные России (за 2024-2025):
{monthly_macro_content}

Годовые макроэкономические данные России (за 2024-2025):
{yearly_macro_content}

Индикаторы:
- SMA: 50, 100, 200
- EMA: 50, 100, 200
- MACD: (24, 52, 9)
- ADX: 20
- RSI: 21
- Stochastic: (21, 5, 5)
- OBV, VWAP

Задача:
1. Проанализируй исторические данные и индикаторы:
   - Оцени тренд (ADX), перекупленность/перепроданность (RSI, Stochastic), импульс (MACD).
   - Учти потоки капитала (OBV) и справедливую цену (VWAP).
2. Сравни динамику цены и индикаторов с ключевыми финансовыми показателями (Чистая прибыль, Активы, EV/EBITDA).
3. Учти макроэкономические факторы (Инфляция, Ключевая ставка, Обменный курс USD/RUB).
4. Сформируй прогноз движения цены на 1-3 месяца:
   - Укажи направление (рост, падение, боковик).
   - Дай вероятность (в %) для основного сценария.
   - Предложи ключевые уровни поддержки и сопротивления.
5. Дай рекомендацию по действиям с акцией на основе всех данных (исторических, финансовых, макроэкономических): 
   - Выбери одно из: Активно продавать, Продавать, Держать, Покупать, Активно покупать.
6. Дай отдельную рекомендацию по действиям с акцией на основе наиболее важных индикаторов (ADX, RSI, MACD, Stochastic), обосновав выбор.
7. Верни результат в формате:
   - "Прогноз: [направление] (вероятность X%)"
   - "Поддержка: [уровень], Сопротивление: [уровень]"
   - "Рекомендация (все данные): [действие]"
   - "Рекомендация (индикаторы): [действие] — [обоснование]"
   - "Комментарий: [краткое обоснование с учётом индикаторов, финансов и макроэкономики]"
   Раздели строки переносом.

Правила:
- Опирайся только на предоставленные данные.
- Ограничь ответ 500 токенов, но входной промпт может быть до 50,000 токенов для поддержки размышлений.
"""

    # Оптимизированный промпт для GigaChat (без РСБУ)
    gigachat_prompt = f"""
Ты финансовый аналитик, эксперт в краткосрочных прогнозах (1-3 месяца). Используй предоставленные данные акции (тикер: {ticker}) за последний год (таймфрейм: weekly), финансовые показатели МСФО и макроэкономические данные России для прогнозирования цены акции.

**Данные:**
- **Исторические данные и индикаторы (последние 50 недель):**  
{indicators}
- **Финансовые показатели МСФО:**  
{msfo_content}
- **Месячные макроэкономические данные России (2024-2025):**  
{monthly_macro_content}
- **Годовые макроэкономические данные России (2024-2025):**  
{yearly_macro_content}

**Индикаторы:**
- SMA: 50, 100, 200
- EMA: 50, 100, 200
- MACD: (24, 52, 9)
- ADX: 20
- RSI: 21
- Stochastic: (21, 5, 5)
- OBV, VWAP

**Задача:**
1. Проанализируй тренд (ADX), импульс (MACD), перекупленность/перепроданность (RSI, Stochastic), потоки капитала (OBV) и справедливую цену (VWAP).
2. Сравни ценовую динамику с финансовыми показателями МСФО (Чистая прибыль, Активы, EV/EBITDA).
3. Учти макроэкономику: инфляция, ключевая ставка, курс USD/RUB.
4. Спрогнозируй движение цены на 1-3 месяца (рост, падение, боковик) с вероятностью основного сценария (%).
5. Определи уровни поддержки и сопротивления.
6. Дай рекомендацию по акции (Активно продавать, Продавать, Держать, Покупать, Активно покупать) на основе всех данных.
7. Дай рекомендацию на основе индикаторов (ADX, RSI, MACD, Stochastic) с обоснованием.

**Формат ответа:**
- Прогноз: [направление] (вероятность X%)
- Поддержка: [уровень], Сопротивление: [уровень]
- Рекомендация (все данные): [действие]
- Рекомендация (индикаторы): [действие] — [обоснование]
- Комментарий: [краткое обоснование с учётом индикаторов, МСФО и макроэкономики]

**Правила:**
- Используй только предоставленные данные.
- Ответ не более 500 токенов.
- Входной промпт до 50,000 токенов.
"""

    system_message = gigachat_prompt if model == "gigachat" else local_llm_prompt
    system_message = system_message.format(
        ticker=ticker,
        indicators=indicators,
        msfo_content=msfo_content if msfo_content else "Отсутствует",
        monthly_macro_content=monthly_macro_content if monthly_macro_content else "Отсутствует",
        yearly_macro_content=yearly_macro_content if yearly_macro_content else "Отсутствует"
    )

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
                model="GigaChat-Max"
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