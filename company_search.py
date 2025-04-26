from gigachat import GigaChat
from bot_config import GIGACHAT_API_KEY, VERIFY_SSL_CERTS
from utils import read_csv_file
from openai import OpenAI
import requests
import re

def get_company_tickers(company_name: str, companies_df, chat_id, bot, model="local"):
    """
    Находит тикеры компании по её названию, используя GigaChat Max-2 или локальную LLM.
    Возвращает список кортежей [(тикеры, название_компании), ...].
    """
    print(f"get_company_tickers called with company_name={company_name}, model={model}")
    try:
        if companies_df is None or companies_df.empty:
            error_msg = "Ошибка: данные о компаниях не загружены."
            bot.send_message(chat_id, error_msg)
            return error_msg

        companies_list = companies_df[["ticker", "official_name"]].dropna()
        companies_str = companies_list.to_string(index=False)

        if model == "gigachat":
            print("Using GigaChat for ticker search")
            prompt = f"""
Выведи только существующий торговый тикер запрашиваемой компании "{company_name}". 
Если у запрашиваемой компании есть привилегированная акция, выведи два тикера через запятую: 
один для обыкновенной акции, другой для привилегированной. 
Не придумывай тикеры. 
Список компаний и их тикеров:
{companies_str}
"""
            with GigaChat(
                credentials=GIGACHAT_API_KEY,
                verify_ssl_certs=VERIFY_SSL_CERTS,
                model="GigaChat-2-Max"
            ) as gigachat_client:
                try:
                    response = gigachat_client.chat(prompt)
                    result = response.choices[0].message.content.strip()
                    if not result or "не найдена" in result.lower():
                        return "Извините, компания не найдена. Попробуйте скорректировать запрос."
                    ticker_list = result.split(",")
                    ticker_list = [t.strip().upper() for t in ticker_list]
                    matched_rows = companies_list[companies_list["ticker"].isin(ticker_list)]
                    if matched_rows.empty:
                        return "Извините, компания не найдена. Попробуйте скорректировать запрос."
                    return [(",".join(ticker_list), matched_rows.iloc[0]["official_name"])]
                except Exception as e:
                    error_msg = f"Ошибка GigaChat в get_company_tickers: {str(e)}\nТип ошибки: {type(e).__name__}"
                    print(error_msg)
                    bot.send_message(chat_id, error_msg)
                    return None
        else:
            print("Using local LLM for ticker search")
            openai_client = OpenAI(base_url="http://localhost:1234/v1", api_key="not-needed")
            prompt = f"""
Ты помощник, анализирующий CSV-файл с данными о компаниях (столбцы: 'ticker', 'official_name'). Содержимое: 
{companies_str}

Запрос: '{company_name}'

Задача:
1. Найди тикеры компаний из CSV:
   - Сначала ищи полное совпадение '{company_name}' с 'official_name' (регистр не важен).
   - Если нет полного совпадения, ищи частичное совпадение, где '{company_name}' — значимая подстрока 'official_name'.
   - Учитывай опечатки до 2 букв (например, 'Сбер' может быть 'Сбр' или 'Себр').
2. Для каждой найденной компании:
   - Если есть привилегированные акции ('P' в тикере или 'ап' в 'official_name'), верни оба тикера через запятую (например, 'SBER,SBERP').
   - Иначе верни только один тикер.
3. Если найдено несколько компаний, раздели их тикеры символом '|' (например, 'SBER,SBERP|VTBR').
4. Если нет совпадений, верни: 'Извините, компания не найдена. Попробуйте скорректировать запрос.'

Примеры:
- Запрос 'Сбер' → 'SBER,SBERP'
- Запрос 'Сбр' → 'SBER,SBERP'
- Запрос 'банк' → 'SBER,SBERP|VTBR|MBNK|SVCB'
- Запрос 'флот' → 'AFLT|FLOT,FLOTP'
- Запрос 'нефть' → 'RNFT|ROSN|TRNFP'
- Запрос 'xyz' → 'Извините, компания не найдена. Попробуйте скорректировать запрос.'

Правила:
- Работай ТОЛЬКО с текстом из CSV, игнорируй знания вне данных.
- Сначала проверяй полное совпадение, затем частичное, затем с опечатками до 2 букв.
- '{company_name}' должно быть точной подстрокой 'official_name' или отличаться не более чем на 2 буквы (добавление, удаление, замена).
- НЕ ВКЛЮЧАЙ компании, где '{company_name}' не является частью 'official_name' или не подходит по опечаткам.
- Проверяй наличие обычных и привилегированных акций по 'P' или 'ап'.
- ВЫВОДИ ТОЛЬКО ТИКЕРЫ ИЛИ СООБЩЕНИЕ ОБ ОШИБКЕ — НИКАКОГО ТЕКСТА, ТЕГОВ ИЛИ РАССУЖДЕНИЙ.
- СТРОГО ЗАПРЕЩЕНО ДОБАВЛЯТЬ <think>, <reasoning>, <output> ИЛИ ЛЮБЫЕ ОБЪЯСНЕНИЯ, ДАЖЕ ВНУТРИ ТЕГОВ.
- ЛЮБЫЕ ТЕГИ ИЛИ РАССУЖДЕНИЯ БУДУТ ОТФИЛЬТРОВАНЫ, ПОЭТОМУ НЕ ТРАТЬ ТОКЕНЫ НА НИХ.
- НЕ ВКЛЮЧАЙ ПОВТОРЯЮЩИЕСЯ ТИКЕРЫ.
- ОТВЕТ ДОЛЖЕН СОДЕРЖАТЬ ТОЛЬКО РЕЗУЛЬТАТ, НАПРИМЕР: 'SBER,SBERP' ИЛИ 'Извините, компания не найдена. Попробуйте скорректировать запрос.'
"""
            try:
                # Проверка доступности сервера LLM
                response = requests.get("http://localhost:1234/v1/models")
                if response.status_code != 200:
                    error_msg = "Ошибка: Локальный сервер LLM недоступен. Проверьте, запущен ли сервер на localhost:1234."
                    print(error_msg)
                    bot.send_message(chat_id, error_msg)
                    return None

                response = openai_client.chat.completions.create(
                    model="deepseek-r1-distill-qwen-14b",
                    messages=[
                        {"role": "system", "content": prompt},
                        {"role": "user", "content": f"Найди тикер для {company_name}"}
                    ],
                    max_tokens=20000,
                    temperature=0.1
                )
                raw_response = response.choices[0].message.content.strip()
                # Фильтрация тегов <think>, <reasoning> и любых других
                result = re.sub(r'<[^>]+>.*?</[^>]+>\s*|<[^>]+>.*$|</[^>]+>\s*', '', raw_response, flags=re.DOTALL).strip()
                print(f"Local LLM raw response: {raw_response}")
                print(f"Filtered LLM response: {result}")
                if result == "Извините, компания не найдена. Попробуйте скорректировать запрос.":
                    return result
                ticker_groups = result.split("|")
                result_list = []
                for group in ticker_groups:
                    ticker_list = group.split(",")
                    ticker_list = [t.strip().upper() for t in ticker_list if t.strip()]
                    if ticker_list:
                        matched_rows = companies_list[companies_list["ticker"].isin(ticker_list)]
                        if not matched_rows.empty:
                            result_list.append((",".join(ticker_list), matched_rows.iloc[0]["official_name"]))
                if not result_list:
                    return "Извините, компания не найдена. Попробуйте скорректировать запрос."
                return result_list
            except Exception as e:
                error_msg = f"Ошибка локальной LLM в get_company_tickers: {str(e)}\nТип ошибки: {type(e).__name__}"
                print(error_msg)
                bot.send_message(chat_id, error_msg)
                return None

    except Exception as e:
        error_msg = f"Общая ошибка в get_company_tickers: {str(e)}\nТип ошибки: {type(e).__name__}"
        print(error_msg)
        bot.send_message(chat_id, error_msg)
        return None