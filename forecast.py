import pandas as pd
from openai import OpenAI
import os
from data_processing import save_historical_data, download_reports, read_csv_file, read_monthly_macro_content, read_yearly_macro_content
import re

client = OpenAI(base_url="http://localhost:1234/v1", api_key="not-needed")

def short_term_forecast(ticker, base_ticker, is_preferred, chat_id, bot):
    """
    Генерирует краткосрочный прогноз (1-3 месяца) для акции с таймфреймом 4 часа за 1 год.
    Использует исторические данные, индикаторы, финансовые и макроэкономические показатели.
    """
    timeframe = "4h"
    period_years = 1

    # Получение исторических данных с индикаторами
    data = save_historical_data(ticker, timeframe, period_years)
    if data is None or data.empty:
        bot.send_message(chat_id, f"Не удалось получить исторические данные для {ticker}.")
        print(f"Ошибка: исторические данные для {ticker} ({timeframe}, {period_years}Y) недоступны")
        return

    # Загрузка финансовых отчётов
    download_reports(ticker, is_preferred, base_ticker)
    msfo_file = os.path.join("reports", f"{base_ticker}-МСФО-годовые.csv")
    rsbu_file = os.path.join("reports", f"{base_ticker}-РСБУ-годовые.csv")

    msfo_content = None
    rsbu_content = None
    if os.path.exists(msfo_file):
        msfo_df = read_csv_file(msfo_file)
        if msfo_df is not None:
            msfo_content = msfo_df.to_string(index=False)
    if os.path.exists(rsbu_file):
        rsbu_df = read_csv_file(rsbu_file)
        if rsbu_df is not None:
            rsbu_content = rsbu_df.to_string(index=False)

    # Макроэкономические данные
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

    # Выборка индикаторов для краткосрочного прогноза
    indicators = data[['date', 'close', 'volume', 'SMA_10', 'SMA_20', 'SMA_50',
                      'EMA_10', 'EMA_20', 'EMA_50', 'MACD', 'MACD_signal',
                      'MACD_histogram', 'ADX', 'RSI_14', 'Stoch_K', 'Stoch_D',
                      'Stoch_Slow', 'OBV', 'VWAP']].tail(50).to_string(index=False)  # Последние 50 точек для анализа

    bot.send_message(chat_id, "Пожалуйста подождите, формируется краткосрочный прогноз через LLM.")

    # Формирование запроса к LLM
    system_message = """
Ты финансовый аналитик, специализирующийся на краткосрочных прогнозах (1-3 месяца). Тебе предоставлены данные акции за последний год с таймфреймом 4 часа, финансовые показатели и макроэкономические данные России. Используй эти данные для формирования прогноза движения цены акции.

Исторические данные с индикаторами (последние 50 точек, таймфрейм 4 часа):
{indicators}

Финансовые показатели МСФО:
{msfo_content}

Финансовые показатели РСБУ (если доступны):
{rsbu_content}

Помесячные макроэкономические данные России (за 2024-2025):
{monthly_macro_content}

Годовые макроэкономические данные России (за 2024-2025):
{yearly_macro_content}

Индикаторы:
- SMA: 10, 20, 50
- EMA: 10, 20, 50
- MACD: (12, 26, 9)
- ADX: 14
- RSI: 14
- Stochastic: (14, 3, 3)
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
5. Верни результат в формате:
   - "Прогноз: [направление] (вероятность X%)"
   - "Поддержка: [уровень], Сопротивление: [уровень]"
   - "Комментарий: [краткое обоснование с учётом индикаторов, финансов и макроэкономики]"
   Раздели строки переносом.

Правила:
- Опирайся только на предоставленные данные.
- Ограничь ответ 500 токенов.
"""
    try:
        response = client.chat.completions.create(
            model="deepseek-r1-distill-qwen-14b",
            messages=[
                {"role": "system", "content": system_message.format(
                    indicators=indicators,
                    msfo_content=msfo_content if msfo_content else "Отсутствует",
                    rsbu_content=rsbu_content if rsbu_content else "Отсутствует",
                    monthly_macro_content=monthly_macro_content if monthly_macro_content else "Отсутствует",
                    yearly_macro_content=yearly_macro_content if yearly_macro_content else "Отсутствует"
                )},
                {"role": "user", "content": f"Сделай краткосрочный прогноз для акции {ticker}."}
            ],
            max_tokens=500,
            temperature=0.3
        )
        raw_response = response.choices[0].message.content.strip()
        result = re.sub(r'<think>.*?</think>\s*|<think>.*$|</think>\s*', '', raw_response, flags=re.DOTALL).strip()
        bot.send_message(chat_id, result)
        print(f"Краткосрочный прогноз для {ticker} отправлен: {result}")
    except Exception as e:
        error_msg = f"Ошибка при формировании прогноза для {ticker}: {str(e)}"
        bot.send_message(chat_id, error_msg)
        print(error_msg)