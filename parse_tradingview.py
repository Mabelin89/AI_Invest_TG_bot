import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Укажите путь к chromedriver.exe
CHROMEDRIVER_PATH = "chromedriver.exe"


def create_driver():
    """Создаёт новый экземпляр WebDriver для каждого потока"""
    options = Options()
    options.add_argument("--headless")  # Включён headless-режим (без окон)
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--blink-settings=imagesEnabled=false")
    options.add_argument("--disable-extensions")

    service = Service(CHROMEDRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=options)
    return driver


def get_company_data(ticker, driver, retries=3):
    """Получает название, описание, сектор, отрасль и сайт для одного тикера"""
    url = f"https://ru.tradingview.com/symbols/RUS-{ticker}/"
    logging.info(f"Запрос для {ticker}: {url}")
    for attempt in range(retries):
        try:
            driver.get(url)
            # Ждём появления контейнера профиля
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, "container-GRoarMHL"))
            )
            # Ждём заголовок
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "h1"))
            )
            soup = BeautifulSoup(driver.page_source, "html.parser")

            # Инициализация значений
            name = "Unknown"
            description = "Unknown"
            sector = "Unknown"
            industry = "Unknown"
            website = "Unknown"

            # Парсинг названия компании
            name_elem = soup.find("h1")
            if name_elem:
                name = name_elem.text.strip()

                # Парсинг описания
                desc_elem = soup.find("div", class_="container-OkHxJmnJ truncatedBlockText-Q8N4R3je")
                if desc_elem:
                    span = desc_elem.find("span")
                    if span:
                        description = span.text.strip()
                if description == "Unknown":
                    desc_elem = soup.find("div", class_="content-OkHxJmnJ")
                    if desc_elem:
                        span = desc_elem.find("span")
                        if span:
                            description = span.text.strip()
                if description == "Unknown":
                    desc_elem = soup.find("div", class_="js-symbol-profile-description")
                    if desc_elem:
                        description = desc_elem.text.strip()
                if description == "Unknown":
                    logging.debug(f"HTML для {ticker}: {soup.prettify()[:500]}")  # Вывод части HTML для диагностики

            # Парсинг из блоков block-GgmpMpKr
            blocks = soup.find_all("div", class_="block-GgmpMpKr")
            for block in blocks:
                label = block.find("div", class_="label-GgmpMpKr")
                value = block.find("div", class_="value-GgmpMpKr")
                if label and value:
                    label_text = label.text.strip()
                    value_text = value.text.strip()
                    if label_text == "Сектор":
                        sector = value_text
                    elif label_text == "Отрасль":
                        industry = value_text
                    elif label_text == "Сайт":
                        website = value_text
                        link = block.find("a", class_="link-GgmpMpKr")
                        if link and link.get("href"):
                            website = link["href"]

            # Парсинг сайта из data-cXDWtdxq, если не найден
            if website == "Unknown":
                website_block = soup.find("span", class_="data-cXDWtdxq value-SOkO5RD4")
                if website_block:
                    link = website_block.find("a", class_="link-SOkO5RD4")
                    if link and link.get("href"):
                        website = link["href"]
                    else:
                        website = website_block.text.strip()

            logging.info(
                f"Найден блок для {ticker}: Название: {name}, Описание: {description[:50]}..., Сектор: {sector}, Отрасль: {industry}, Сайт: {website}")
            return name, description, sector, industry, website

        except Exception as e:
            logging.error(f"Ошибка при запросе {ticker} (попытка {attempt + 1}/{retries}): {str(e)}")
            if attempt < retries - 1:
                time.sleep(2)
            else:
                return "Unknown", "Unknown", "Unknown", "Unknown", "Unknown"


def process_ticker(ticker):
    """Обрабатывает один тикер в отдельном потоке"""
    driver = create_driver()
    try:
        name, description, sector, industry, website = get_company_data(ticker, driver)
        return ticker, name, description, sector, industry, website
    finally:
        driver.quit()


# Загрузка CSV
df = pd.read_csv("moex_companies_no_etf.csv")
df["name"] = df.get("name", "")
df["description"] = df.get("description", "")
df["sector"] = df.get("sector", "")
df["industry"] = df.get("industry", "")
df["website"] = df.get("website", "")

# Параллельная обработка
MAX_WORKERS = 8  # Уменьшено до 2 для стабильности
results = []

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    future_to_ticker = {
        executor.submit(process_ticker, row["ticker"]): row["ticker"]
        for index, row in df.iterrows()
        if not row["name"] or not row["description"] or not row["sector"] or not row["industry"] or not row["website"]
    }

    for future in as_completed(future_to_ticker):
        ticker = future_to_ticker[future]
        try:
            ticker, name, description, sector, industry, website = future.result()
            df.loc[df["ticker"] == ticker, "name"] = name
            df.loc[df["ticker"] == ticker, "description"] = description
            df.loc[df["ticker"] == ticker, "sector"] = sector
            df.loc[df["ticker"] == ticker, "industry"] = industry
            df.loc[df["ticker"] == ticker, "website"] = website
            logging.info(
                f"Тикер: {ticker}, Название: {name}, Описание: {description[:50]}..., Сектор: {sector}, Отрасль: {industry}, Сайт: {website}")
        except Exception as e:
            logging.error(f"Ошибка обработки {ticker}: {str(e)}")

# Сохранение
df.to_csv("moex_companies.csv", index=False)
logging.info("Обновлённый CSV сохранён")