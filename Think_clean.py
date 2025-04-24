import pandas as pd
import re

# Чтение CSV файла
input_file = 'moex_companies_wiki_llm.csv'
output_file = 'moex_companies_wiki_llm_cleaned.csv'

# Загрузка данных
df = pd.read_csv(input_file)

# Функция для удаления тега <think> и текста внутри него
def remove_think_tags(text):
    # Используем регулярное выражение для удаления <think> и всего содержимого до </think>
    cleaned_text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
    # Удаляем лишние пробелы, которые могли остаться после удаления тегов
    cleaned_text = ' '.join(cleaned_text.split())
    return cleaned_text

# Применяем функцию к колонке 'description'
df['description'] = df['description'].apply(remove_think_tags)

# Сохраняем очищенный файл
df.to_csv(output_file, index=False)

print(f"Очищенный файл сохранён как {output_file}")