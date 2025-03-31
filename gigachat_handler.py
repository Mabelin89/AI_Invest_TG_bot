from gigachat import GigaChat
from bot_config import GIGACHAT_API_KEY, GIGACHAT_MODEL, VERIFY_SSL_CERTS

# Синхронная функция для получения ответа от GigaChat
def get_gigachat_response(prompt: str) -> str:
    with GigaChat(
        credentials=GIGACHAT_API_KEY,  # Используем credentials вместо api_key
        verify_ssl_certs=VERIFY_SSL_CERTS,
        model=GIGACHAT_MODEL
    ) as client:
        try:
            # Синхронный запрос к GigaChat API с использованием позиционного аргумента
            response = client.chat(prompt)
            return response.choices[0].message.content
        except Exception as e:
            return f"Ошибка GigaChat: {str(e)}"