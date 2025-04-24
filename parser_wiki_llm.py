import pandas as pd
import wikipediaapi
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import OpenAI

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Инициализация клиента LLM
client = OpenAI(base_url="http://localhost:1234/v1", api_key="not-needed")

# Установка русской Википедии с user_agent
wiki = wikipediaapi.Wikipedia(
    language='ru',
    extract_format=wikipediaapi.ExtractFormat.WIKI,
    user_agent='MOEXParser/1.0 (https://example.com; mail@example.com)'  # Укажите свой email или оставьте пример
)


def generate_description_with_llm(ticker, company_name, wiki_text, existing_descriptions):
    """Генерирует описание с помощью LLM на основе текста Википедии и существующих описаний"""
    system_message = f"""
Ты помощник, создающий описания компаний в стиле существующих примеров из CSV-файла.

Существующие описания (примеры стиля):
{existing_descriptions}

Задача:
1. На основе текста из Википедии для компании '{company_name}' (тикер: {ticker}) создай описание в стиле примеров выше.
2. Текст из Википедии: '{wiki_text}'.
3. Описание должно:
   - Начинаться с названия компании (например, 'ПАО «{company_name}»').
   - Описывать основную деятельность компании (что делает, чем занимается).
   - Упоминать сегменты деятельности, если они есть в тексте.
   - Включать дату основания и местоположение штаб-квартиры, если информация доступна.
   - Быть длиной 100–300 слов, как в примерах.
4. Если текст Википедии пустой или недостаточный, используй только название и напиши базовое описание, основанное на здравом смысле и стиле примеров.
5. ВЫВОДИ ТОЛЬКО ОПИСАНИЕ — никаких тегов, рассуждений или лишнего текста.

Пример вывода:
ПАО «Аэрофлот — Российские авиалинии» осуществляет полёты и коммерческую деятельность на воздушных линиях. Компания предоставляет услуги пассажирских и грузовых авиаперевозок как внутри страны, так и на международном уровне, наряду с другими авиационными сервисами. Аэрофлот осуществляет свою деятельность посредством двух бизнес-сегментов: Авиаперевозки и Прочее. Компания была основана 17 марта 1932 года. Штаб-квартира расположена в Москве, Россия.
"""
    try:
        response = client.chat.completions.create(
            model="deepseek-r1-distill-qwen-14b",
            messages=[
                {"role": "system", "content": system_message},
                {"role": "user", "content": f"Сгенерируй описание для {company_name} (тикер: {ticker})"}
            ],
            max_tokens=500,
            temperature=0.3
        )
        description = response.choices[0].message.content.strip()
        logging.info(f"Сгенерировано описание для {ticker}: {description[:50]}...")
        return description
    except Exception as e:
        logging.error(f"Ошибка LLM для {ticker}: {str(e)}")
        return "Unknown"


def get_wikipedia_description(ticker, company_name):
    """Получает текст из Википедии и возвращает его"""
    page = wiki.page(company_name)
    if page.exists():
        summary = page.summary.split('. ')
        if len(summary) > 3:
            wiki_text = '. '.join(summary[:3]) + '.'
        else:
            wiki_text = page.summary
        logging.info(f"Найден текст Википедии для {ticker}: {wiki_text[:50]}...")
        return wiki_text
    logging.warning(f"Страница Википедии для {company_name} ({ticker}) не найдена")
    return ""


def process_ticker(ticker_data, existing_descriptions):
    """Обрабатывает один тикер"""
    ticker, official_name, name = ticker_data
    # Очистка названий для поиска
    official_name_clean = official_name.replace("i", "").replace(" ао", "").replace("ПАО ", "").strip()
    name_clean = name.replace("ПАО ", "").replace(" - обыкн.", "").strip()

    # Сначала пробуем official_name, затем name
    wiki_text = get_wikipedia_description(ticker, official_name_clean)
    company_name_used = official_name_clean
    if not wiki_text:
        wiki_text = get_wikipedia_description(ticker, name_clean)
        company_name_used = name_clean

    # Генерация описания с LLM
    description = generate_description_with_llm(ticker, company_name_used, wiki_text, existing_descriptions)
    return ticker, description


# Загрузка CSV
df = pd.read_csv("moex_companies.csv")

# Извлечение существующих описаний для шаблона (только первые 5 для экономии токенов)
existing_descriptions = "\n".join(
    df[df['description'] != "Unknown"]['description'].head(5).tolist()
)

# Фильтрация тикеров с "Unknown" в description
unknown_desc = df[df['description'] == "Unknown"][['ticker', 'official_name', 'name']]

# Параллельная обработка
MAX_WORKERS = 2  # Уменьшено для стабильности с LLM
results = []

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    future_to_ticker = {
        executor.submit(process_ticker, (row['ticker'], row['official_name'], row['name']), existing_descriptions): row[
            'ticker']
        for _, row in unknown_desc.iterrows()
    }

    for future in as_completed(future_to_ticker):
        ticker = future_to_ticker[future]
        try:
            ticker, description = future.result()
            df.loc[df['ticker'] == ticker, 'description'] = description
            logging.info(f"Тикер: {ticker}, Описание: {description[:50]}...")
        except Exception as e:
            logging.error(f"Ошибка обработки {ticker}: {str(e)}")

# Сохранение обновленного CSV
df.to_csv("moex_companies_wiki_llm.csv", index=False)
logging.info("Обновлённый CSV сохранён")