import re
from openai import OpenAI

# Инициализация клиента LLM
client = OpenAI(base_url="http://localhost:1234/v1", api_key="not-needed")


# Функция для получения тикеров с учетом опечаток и полного/частичного совпадения
def get_company_tickers(company_name, companies_df, chat_id, bot):
    bot.send_message(chat_id, "Пожалуйста подождите, работает LLM.")

    # Проверяем структуру DataFrame
    if companies_df is not None:
        if len(companies_df.columns) == 1 and ',' in companies_df.columns[0]:
            # Если заголовок объединён, разделяем его
            companies_df.columns = ['ticker,official_name']  # Временное имя
            companies_df[['ticker', 'official_name']] = companies_df['ticker,official_name'].str.split(',', expand=True)
            companies_df = companies_df.drop(columns=['ticker,official_name'])
        ticker_col = 'ticker' if 'ticker' in companies_df.columns else companies_df.columns[0]
        name_col = 'official_name' if 'official_name' in companies_df.columns else companies_df.columns[1]
        print(f"Используемые столбцы: ticker='{ticker_col}', official_name='{name_col}'")
    else:
        return "Ошибка: данные о компаниях не загружены."

    system_message = f"""
Ты помощник, анализирующий CSV-файл с данными о компаниях (столбцы: '{ticker_col}', '{name_col}'). Содержимое: {companies_df.to_string(index=False)}.
Запрос: '{company_name}'.

Задача:
1. Найди тикеры компаний из CSV:
   - Сначала ищи полное совпадение '{company_name}' с '{name_col}' (регистр не важен).
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
- Ограничь ответ 50 токенов.
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

        # Проверяем тикеры и извлекаем имена из DataFrame
        ticker_str = '|'.join(unique_tickers)
        if ticker_str and companies_df is not None:
            validated_tickers = []
            for company_tickers in ticker_str.split('|'):
                ticker_list = company_tickers.split(',')
                first_ticker = ticker_list[0]
                # Ищем имя компании в DataFrame
                matching_rows = companies_df[companies_df[ticker_col] == first_ticker]
                if not matching_rows.empty:
                    company_name_in_file = matching_rows[name_col].iloc[0]
                else:
                    company_name_in_file = first_ticker  # Fallback на тикер
                validated_tickers.append((company_tickers, company_name_in_file))
            return validated_tickers if validated_tickers else "Извините, компания не найдена. Попробуйте скорректировать запрос."
        return "Извините, компания не найдена. Попробуйте скорректировать запрос."

    except Exception as e:
        print(f"Ошибка запроса к LLM: {str(e)}")
        return None