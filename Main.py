import telebot
from bot_config import BOT_TOKEN
from utils import read_file_content
from company_search import get_company_tickers
from keyboards import get_timeframe_keyboard, get_plot_keyboard
from data_processing import save_historical_data, download_reports, analyze_msfo_report
from plotting import plot_and_send_chart
import re

# Инициализация бота
bot = telebot.TeleBot(BOT_TOKEN)

# Путь к файлу и глобальная переменная для содержимого
FILE_PATH = "moex_companies_no_etf.csv"

# Словарь для отслеживания состояния пользователей
user_states = {}


# Функция для повторного вопроса о компании
def ask_next_company(chat_id):
    bot.send_message(chat_id, "Тикер какой компании Вас интересует?")
    user_states[chat_id] = {"step": "ask_company"}


# Обработчик команды /start
@bot.message_handler(commands=['start'])
def handle_start(message):
    chat_id = message.chat.id
    ask_next_company(chat_id)


# Обработчик текстовых сообщений
@bot.message_handler(content_types=['text'])
def handle_message(message):
    chat_id = message.chat.id
    raw_message = message.text
    user_message = re.sub(r'<think>.*?</think>\s*|<think>.*$|</think>\s*', '', raw_message, flags=re.DOTALL).strip()
    user_message = '\n'.join(line for line in user_message.split('\n') if line.strip())
    print(f"Очищенный запрос пользователя: '{user_message}'")

    try:
        if not user_message:
            bot.send_message(chat_id, "Пожалуйста, введите название компании.")
            ask_next_company(chat_id)
            return

        if chat_id not in user_states:
            ask_next_company(chat_id)
            return

        companies_df = read_file_content(FILE_PATH)
        if companies_df is None:
            bot.send_message(chat_id,
                             "Ошибка: данные о компаниях не загружены. Обратитесь к администратору или попробуйте позже.")
            ask_next_company(chat_id)
            return

        state = user_states[chat_id]["step"]

        if state == "ask_company":
            company_name = user_message
            tickers = get_company_tickers(company_name, companies_df, chat_id, bot)
            if not tickers:
                bot.send_message(chat_id, "Произошла ошибка при запросе к LLM. Проверьте сервер или модель.")
                ask_next_company(chat_id)
                return

            if tickers == "Извините, компания не найдена. Попробуйте скорректировать запрос.":
                bot.send_message(chat_id, tickers)
                ask_next_company(chat_id)
                return

            # tickers — это список кортежей [(tickers, company_name), ...]
            if len(tickers) == 1:
                ticker_list = tickers[0][0].split(",")
                if len(ticker_list) == 2:
                    user_states[chat_id] = {
                        "step": "choose_type",
                        "tickers": ticker_list,
                        "base_ticker": ticker_list[0],
                        "company": company_name
                    }
                    bot.send_message(chat_id, "Выберите тип акции:\n1 — обычная\n2 — привилегированная")
                else:
                    user_states[chat_id] = {
                        "step": "ask_timeframe",
                        "ticker": ticker_list[0],
                        "base_ticker": ticker_list[0],
                        "company": company_name
                    }
                    bot.send_message(chat_id, f"Тикер: {ticker_list[0]}")
                    bot.send_message(chat_id, "Выберите таймфрейм:", reply_markup=get_timeframe_keyboard())
            else:
                options = []
                ticker_options = []
                for i, (company_tickers, company_name_in_file) in enumerate(tickers, 1):
                    ticker_list = company_tickers.split(",")
                    options.append(f"{i} — {company_name_in_file} ({company_tickers})")
                    ticker_options.append(ticker_list)
                user_states[chat_id] = {
                    "step": "choose_company",
                    "companies": ticker_options,
                    "original_query": company_name
                }
                bot.send_message(chat_id, "Какую компанию Вы имели в виду?\n" + "\n".join(options))

        elif state == "choose_type":
            choice = user_message.strip()
            ticker_list = user_states[chat_id]["tickers"]
            base_ticker = user_states[chat_id]["base_ticker"]
            if choice == "1":
                selected_ticker = ticker_list[0]
                is_preferred = False
            elif choice == "2":
                selected_ticker = ticker_list[1]
                is_preferred = True
            else:
                bot.send_message(chat_id, "Пожалуйста, выберите 1 или 2.")
                return

            user_states[chat_id] = {
                "step": "ask_timeframe",
                "ticker": selected_ticker,
                "base_ticker": base_ticker,
                "is_preferred": is_preferred,
                "company": user_states[chat_id]["company"]
            }
            bot.send_message(chat_id, f"Тикер: {selected_ticker}")
            bot.send_message(chat_id, "Выберите таймфрейм:", reply_markup=get_timeframe_keyboard())

        elif state == "choose_company":
            choice = user_message.strip()
            try:
                choice_idx = int(choice) - 1
                if 0 <= choice_idx < len(user_states[chat_id]["companies"]):
                    ticker_list = user_states[chat_id]["companies"][choice_idx]
                    if len(ticker_list) == 2:
                        user_states[chat_id] = {
                            "step": "choose_type",
                            "tickers": ticker_list,
                            "base_ticker": ticker_list[0],
                            "company": user_states[chat_id]["original_query"]
                        }
                        bot.send_message(chat_id, "Выберите тип акции:\n1 — обычная\n2 — привилегированная")
                    else:
                        user_states[chat_id] = {
                            "step": "ask_timeframe",
                            "ticker": ticker_list[0],
                            "base_ticker": ticker_list[0],
                            "company": user_states[chat_id]["original_query"]
                        }
                        bot.send_message(chat_id, f"Тикер: {ticker_list[0]}")
                        bot.send_message(chat_id, "Выберите таймфрейм:", reply_markup=get_timeframe_keyboard())
                else:
                    bot.send_message(chat_id, "Пожалуйста, выберите номер из списка.")
            except ValueError:
                bot.send_message(chat_id, "Пожалуйста, введите цифру для выбора компании.")

        elif state == "ask_period":
            try:
                period_years = int(user_message.strip())
                if period_years <= 0:
                    raise ValueError
                ticker = user_states[chat_id]["ticker"]
                base_ticker = user_states[chat_id]["base_ticker"]
                is_preferred = user_states[chat_id].get("is_preferred", False)
                timeframe = user_states[chat_id]["timeframe"]
                data = save_historical_data(ticker, timeframe, period_years)
                if data is not None:
                    bot.send_message(chat_id,
                                     f"Данные для {ticker} ({timeframe}, {period_years} лет) сохранены в папку 'historical_data'.")
                    download_reports(ticker, is_preferred, base_ticker)
                    bot.send_message(chat_id, f"Отчеты для {base_ticker} (МСФО и РСБУ) сохранены в папку 'reports'.")
                    user_states[chat_id]["data"] = data
                    user_states[chat_id]["period_years"] = period_years
                    bot.send_message(chat_id, "Вывести график цены акции за указанный период?",
                                     reply_markup=get_plot_keyboard())
                else:
                    bot.send_message(chat_id, f"Не удалось получить данные для {ticker}.")
                    ask_next_company(chat_id)
            except ValueError:
                bot.send_message(chat_id, "Пожалуйста, введите положительное целое число для периода в годах.")
            except Exception as e:
                bot.send_message(chat_id, f"Ошибка при обработке данных: {str(e)}")
                ask_next_company(chat_id)

    except Exception as e:
        bot.send_message(chat_id, f"Произошла ошибка: {str(e)}")
        ask_next_company(chat_id)


# Обработчик callback-запросов от inline-клавиатуры
@bot.callback_query_handler(func=lambda call: True)
def handle_callback(call):
    chat_id = call.message.chat.id
    if chat_id not in user_states:
        bot.answer_callback_query(call.id, "Сессия устарела. Начните заново с /start.")
        return

    state = user_states[chat_id]["step"]

    if state == "ask_timeframe":
        timeframe = call.data
        user_states[chat_id]["timeframe"] = timeframe
        user_states[chat_id]["step"] = "ask_period"
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text=f"Выбран таймфрейм: {timeframe}. Введите период в годах (например, 1, 2, 5):"
        )
        bot.answer_callback_query(call.id)

    elif state == "ask_period":
        if call.data == "yes_plot":
            ticker = user_states[chat_id]["ticker"]
            base_ticker = user_states[chat_id]["base_ticker"]
            timeframe = user_states[chat_id]["timeframe"]
            period_years = user_states[chat_id]["period_years"]
            data = user_states[chat_id]["data"]
            plot_and_send_chart(chat_id, ticker, timeframe, period_years, data, base_ticker, bot)
            ask_next_company(chat_id)
        elif call.data == "no_plot":
            ask_next_company(chat_id)
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text="Обработка завершена."
        )
        bot.answer_callback_query(call.id)


# Запуск бота
if __name__ == "__main__":
    companies_df = read_file_content(FILE_PATH)
    if companies_df is None:
        print(
            "Предупреждение: Не удалось загрузить данные о компаниях. Функционал поиска тикеров может быть ограничен.")
        bot.polling(none_stop=True, timeout=60)
    else:
        print("Бот запущен с данными о компаниях...")
        bot.polling(none_stop=True, timeout=60)