import pandas as pd

# Путь к файлу
FILE_PATH = "moex_companies_no_etf.csv"


def clean_csv_file(file_path):
    try:
        # Чтение CSV файла
        df = pd.read_csv(file_path)

        # Подсчет общего количества строк
        total_rows = len(df)
        print(f"Общее количество строк в файле: {total_rows}")

        # Подсчет дубликатов по столбцу 'ticker'
        duplicate_tickers = df['ticker'].duplicated(keep='first').sum()
        print(f"Количество дубликатов по тикерам: {duplicate_tickers}")

        # Удаление дубликатов по столбцу 'ticker', оставляем первое вхождение
        df_cleaned = df.drop_duplicates(subset=['ticker'], keep='first')

        # Подсчет строк после удаления дубликатов
        cleaned_rows = len(df_cleaned)
        print(f"Количество строк после удаления дубликатов: {cleaned_rows}")

        # Перезапись файла с очищенными данными
        df_cleaned.to_csv(file_path, index=False)
        print(f"Файл '{file_path}' перезаписан с очищенными данными.")

    except FileNotFoundError:
        print(f"Ошибка: Файл '{file_path}' не найден.")
    except Exception as e:
        print(f"Произошла ошибка: {str(e)}")


if __name__ == "__main__":
    clean_csv_file(FILE_PATH)