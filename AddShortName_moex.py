import pandas as pd
import requests
import json

# 1. Чтение исходного CSV-файла
# Предполагается, что файл называется 'moex_companies.csv' и находится в текущей директории
input_file = 'moex_companies.csv'
df = pd.read_csv(input_file)


# 2. Функция для получения данных из MOEX ISS API
def get_moex_shortnames():
    url = "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.json?securities.columns=SECID,SHORTNAME"
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"Ошибка при запросе к MOEX ISS: {response.status_code}")

    data = response.json()
    securities = data['securities']['data']
    # Создаем словарь {SECID: SHORTNAME}
    shortname_dict = {item[0]: item[1] for item in securities}
    return shortname_dict


# 3. Получение данных SHORTNAME из MOEX ISS
try:
    shortname_dict = get_moex_shortnames()
except Exception as e:
    print(f"Не удалось получить данные из MOEX ISS: {e}")
    shortname_dict = {}

# 4. Добавление столбца 'shortname' в DataFrame
# Используем тикеры из столбца 'ticker' для поиска соответствующих SHORTNAME
df['shortname'] = df['ticker'].apply(lambda x: shortname_dict.get(x, 'Unknown'))

# 5. Сохранение обновленного файла
output_file = 'moex_companies_updated.csv'
df.to_csv(output_file, index=False, encoding='utf-8')

print(f"Обновленный файл сохранен как '{output_file}'")
print(f"Количество строк в файле: {len(df)}")
print(f"Пример первых 5 строк:\n{df.head().to_string()}")