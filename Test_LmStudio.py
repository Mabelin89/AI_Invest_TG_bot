from openai import OpenAI
import pandas as pd

# Настраиваем клиент для локального API LM Studio
client = OpenAI(base_url="http://localhost:1234/v1", api_key="not-needed")


# Функция для чтения текстового файла
def read_text_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.read()
    except Exception as e:
        print(f"Ошибка чтения текстового файла: {e}")
        return None


# Функция для чтения CSV файла
def read_csv_file(file_path):
    try:
        df = pd.read_csv(file_path)
        return df.to_string()  # Преобразуем DataFrame в строку
    except Exception as e:
        print(f"Ошибка чтения CSV: {e}")
        return None


# Функция для отправки содержимого файла к LLM
def send_file_to_llm(file_path, prompt="Анализируй этот текст:", max_tokens=5000):
    # Определяем тип файла и читаем его
    if file_path.endswith('.txt'):
        file_content = read_text_file(file_path)
    elif file_path.endswith('.csv'):
        file_content = read_csv_file(file_path)
    else:
        print("Неподдерживаемый формат файла. Используйте .txt или .csv")
        return

    if not file_content:
        return

    # Отправляем запрос к LLM
    try:
        response = client.chat.completions.create(
            model="your_model_name",  # Укажите модель или оставьте пустым
            messages=[
                {"role": "system", "content": f"Напиши только тикер компании: {prompt} Тикеры находятся в первом столбце {file_content}"},
                {"role": "user", "content": f"{prompt}"}
            ],
            max_tokens=max_tokens,
            temperature=0.7
        )
        print("Ответ модели:")
        print(response.choices[0].message.content)
    except Exception as e:
        print(f"Ошибка запроса к LLM: {e}")


# Пример использования
if __name__ == "__main__":

    csv_file_path = "moex_companies_no_etf.csv"
    send_file_to_llm(csv_file_path, prompt="сбербанк:", max_tokens=1000)