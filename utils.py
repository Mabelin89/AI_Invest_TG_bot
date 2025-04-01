import pandas as pd
import os
import csv

# Глобальные переменные для содержимого файлов
FILE_CONTENT = None
MONTHLY_MACRO_CONTENT = None
YEARLY_MACRO_CONTENT = None


# Функция для чтения CSV файла с автоматическим определением разделителя
def read_csv_file(file_path):
    try:
        if not os.path.exists(file_path):
            print(f"Файл '{file_path}' не найден в директории {os.getcwd()}")
            return None

        # Определяем разделитель с помощью csv.Sniffer
        with open(file_path, 'r', encoding='utf-8') as file:
            sample = file.read(1024)
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(sample)
            separator = dialect.delimiter

        # Читаем файл с определённым разделителем
        df = pd.read_csv(file_path, sep=separator, decimal=',', thousands=' ', encoding='utf-8')
        df.columns = df.columns.str.strip()
        df = df.map(lambda x: str(x).strip() if isinstance(x, str) else x)
        print(f"Файл '{file_path}' успешно прочитан (разделитель: '{separator}')")
        print(f"Столбцы DataFrame: {list(df.columns)}")
        print(f"Первые 5 строк:\n{df.head().to_string()}")
        return df
    except Exception as e:
        print(f"Ошибка чтения CSV '{file_path}': {e}")
        return None


# Чтение содержимого файла компаний один раз при запуске
def read_file_content(file_path):
    global FILE_CONTENT
    if FILE_CONTENT is None:
        if file_path.endswith('.csv'):
            FILE_CONTENT = read_csv_file(file_path)
        else:
            print(f"Поддерживается только формат .csv, передан: '{file_path}'")
            FILE_CONTENT = None
    return FILE_CONTENT


# Чтение помесячных макроэкономических данных
def read_monthly_macro_content(file_path="monthly_macro_indicators_russia.csv"):
    global MONTHLY_MACRO_CONTENT
    if MONTHLY_MACRO_CONTENT is None:
        if os.path.exists(file_path):
            MONTHLY_MACRO_CONTENT = read_csv_file(file_path)
        else:
            print(f"Файл '{file_path}' не найден")
            MONTHLY_MACRO_CONTENT = None
    return MONTHLY_MACRO_CONTENT


# Чтение годовых макроэкономических данных
def read_yearly_macro_content(file_path="yearly_macro_indicators_russia.csv"):
    global YEARLY_MACRO_CONTENT
    if YEARLY_MACRO_CONTENT is None:
        if os.path.exists(file_path):
            YEARLY_MACRO_CONTENT = read_csv_file(file_path)
        else:
            print(f"Файл '{file_path}' не найден")
            YEARLY_MACRO_CONTENT = None
    return YEARLY_MACRO_CONTENT