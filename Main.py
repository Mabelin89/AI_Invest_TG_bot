import telebot
from telebot import types
from bot_config import BOT_TOKEN
from utils import read_file_content, read_monthly_macro_content, read_yearly_macro_content
from company_search import get_company_tickers
from keyboards import get_timeframe_keyboard, get_plot_keyboard, get_forecast_menu_keyboard
from data_processing import save_historical_data, download_reports, analyze_msfo_report
from plotting import plot_and_send_chart
from forecast import short_term_forecast, medium_term_forecast, long_term_forecast
import re
import time

# Инициализация бота
bot = telebot.TeleBot(BOT_TOKEN)

# Путь к файлу и глобальная переменная для содержимого
FILE_PATH = "moex_companies_no_etf.csv"

# Словарь для отслеживания состояния пользователей
user_states = {}

# Клавиатура для выбора модели
def get_model_keyboard():
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton("GigaChat", callback_data="model_gigachat"))
    keyboard.add(types.InlineKeyboardButton("Локальная LLM", callback_data="model_local"))
    return keyboard

# Функция для повторного вопроса о компании
def ask_next_company(chat_id):
    bot.send_message(chat_id, "Тикер какой компании Вас интересует?")
    user_states[chat_id]["step"] = "ask_company"

# Обработчик команды /start
@bot.message_handler(commands=['start'])
def handle_start(message):
    chat_id = message.chat.id
    user_states[chat_id] = {"step": "choose_model"}
    bot.send_message(chat_id, "Выберите модель для обработки запросов:", reply_markup=get_model_keyboard())

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
            user_states[chat_id] = {"step": "choose_model"}
            bot.send_message(chat_id, "Выберите модель для обработки запросов:", reply_markup=get_model_keyboard())
            return

        state = user_states[chat_id]["step"]
        model = user_states[chat_id].get("model", "local")
        print(f"Handling message with state={state}, model={model}")

        if state == "ask_company":
            company_name = user_message
            print(f"Calling get_company_tickers with model={model}")
            tickers = get_company_tickers(company_name, companies_df, chat_id, bot, model)
            if not tickers:
                bot.send_message(chat_id, "Произошла ошибка при запросе к модели. Проверьте сервер или модель.")
                ask_next_company(chat_id)
                return

            if tickers == "Извините, компания не найдена. Попробуйте скорректировать запрос.":
                bot.send_message(chat_id, tickers)
                ask_next_company(chat_id)
                return

            if len(tickers) == 1:
                ticker_list = tickers[0][0].split(",")
                if len(ticker_list) == 2:
                    user_states[chat_id].update({
                        "step": "choose_type",
                        "tickers": ticker_list,
                        "base_ticker": ticker_list[0],
                        "company": company_name,
                        "model": model
                    })
                    bot.send_message(chat_id, "Выберите тип акции:\n1 — обычная\n2 — привилегированная")
                else:
                    user_states[chat_id].update({
                        "step": "show_menu",
                        "ticker": ticker_list[0],
                        "base_ticker": ticker_list[0],
                        "company": company_name,
                        "model": model
                    })
                    bot.send_message(chat_id, f"Тикер: {ticker_list[0]}")
                    bot.send_message(chat_id, "Выберите действие:", reply_markup=get_forecast_menu_keyboard())
            else:
                options = []
                ticker_options = []
                for i, (company_tickers, company_name_in_file) in enumerate(tickers, 1):
                    ticker_list = company_tickers.split(",")
                    options.append(f"{i} — {company_name_in_file} ({company_tickers})")
                    ticker_options.append(ticker_list)
                user_states[chat_id].update({
                    "step": "choose_company",
                    "companies": ticker_options,
                    "original_query": company_name,
                    "model": model
                })
                bot.send_message(chat_id, "Какую компанию Вы имели в виду?\n" + "\n".join(options))

        elif state == "choose_type":
            choice = user_message.strip()
            if choice not in ["1", "2"]:
                bot.send_message(chat_id, "Пожалуйста, выберите 1 (обычная) или 2 (привилегированная).")
                return
            ticker = user_states[chat_id]["tickers"][int(choice) - 1]
            user_states[chat_id].update({
                "step": "show_menu",
                "ticker": ticker,
                "model": model
            })
            bot.send_message(chat_id, f"Выбрана акция: {ticker}")
            bot.send_message(chat_id, "Выберите действие:", reply_markup=get_forecast_menu_keyboard())

        elif state == "choose_company":
            try:
                choice = int(user_message.strip()) - 1
                if choice < 0 or choice >= len(user_states[chat_id]["companies"]):
                    bot.send_message(chat_id, "Пожалуйста, выберите номер из списка.")
                    return
                ticker_list = user_states[chat_id]["companies"][choice]
                if len(ticker_list) == 2:
                    user_states[chat_id].update({
                        "step": "choose_type",
                        "tickers": ticker_list,
                        "base_ticker": ticker_list[0],
                        "company": user_states[chat_id]["original_query"],
                        "model": model
                    })
                    bot.send_message(chat_id, "Выберите тип акции:\n1 — обычная\n2 — привилегированная")
                else:
                    user_states[chat_id].update({
                        "step": "show_menu",
                        "ticker": ticker_list[0],
                        "base_ticker": ticker_list[0],
                        "company": user_states[chat_id]["original_query"],
                        "model": model
                    })
                    bot.send_message(chat_id, f"Тикер: {ticker_list[0]}")
                    bot.send_message(chat_id, "Выберите действие:", reply_markup=get_forecast_menu_keyboard())
            except ValueError:
                bot.send_message(chat_id, "Пожалуйста, введите номер компании из списка.")
                return

        elif state == "ask_period":
            try:
                period_years = int(user_message.strip())
                if period_years < 1 or period_years > 10:
                    bot.send_message(chat_id, "Пожалуйста, введите период от 1 до 10 лет.")
                    return
                ticker = user_states[chat_id]["ticker"]
                base_ticker = user_states[chat_id]["base_ticker"]
                print(f"Calling analyze_msfo_report with model={model}")
                analysis_result = analyze_msfo_report(ticker, base_ticker, chat_id, bot, period_years, model)
                bot.send_message(chat_id, analysis_result)
                ask_next_company(chat_id)
            except ValueError:
                bot.send_message(chat_id, "Пожалуйста, введите число (например, 3).")
                return

    except Exception as e:
        bot.send_message(chat_id, f"Произошла ошибка: {str(e)}")
        ask_next_company(chat_id)

# Обработчик callback-запросов от inline-клавиатуры
@bot.callback_query_handler(func=lambda call: True)
def handle_callback(call):
    chat_id = call.message.chat.id

    if chat_id not in user_states:
        try:
            bot.answer_callback_query(call.id, "Сессия устарела. Начните заново с /start.")
        except Exception:
            print(f"Не удалось ответить на callback: {call.id}")
        return

    state = user_states[chat_id]["step"]
    model = user_states[chat_id].get("model", "local")
    print(f"Handling callback with state={state}, model={model}")

    if state == "choose_model":
        if call.data == "model_gigachat":
            user_states[chat_id] = {"step": "ask_company", "model": "gigachat"}
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text="Выбрана модель GigaChat. Укажите название компании."
            )
            ask_next_company(chat_id)
        elif call.data == "model_local":
            user_states[chat_id] = {"step": "ask_company", "model": "local"}
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text="Выбрана локальная LLM. Укажите название компании."
            )
            ask_next_company(chat_id)
        try:
            bot.answer_callback_query(call.id)
        except Exception:
            print(f"Не удалось ответить на callback: {call.id}")
        return

    if state == "show_menu":
        ticker = user_states[chat_id]["ticker"]
        base_ticker = user_states[chat_id]["base_ticker"]
        is_preferred = ticker != base_ticker

        if call.data == "short_term_forecast":
            print(f"Calling short_term_forecast with model={model}")
            short_term_forecast(ticker, chat_id, bot, base_ticker, is_preferred, model)
            ask_next_company(chat_id)
        elif call.data == "medium_term_forecast":
            print(f"Calling medium_term_forecast with model={model}")
            medium_term_forecast(ticker, chat_id, bot, base_ticker, is_preferred, model)
            ask_next_company(chat_id)
        elif call.data == "long_term_forecast":
            print(f"Calling long_term_forecast with model={model}")
            long_term_forecast(ticker, chat_id, bot, base_ticker, is_preferred, model)
            ask_next_company(chat_id)
        elif call.data == "plot":
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text="Выберите таймфрейм для графика:",
                reply_markup=get_timeframe_keyboard()
            )
            user_states[chat_id]["step"] = "choose_timeframe"
        elif call.data == "msfo_analysis":
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text="Введите период анализа МСФО (в годах, от 1 до 10):"
            )
            user_states[chat_id]["step"] = "ask_period"
        try:
            bot.answer_callback_query(call.id)
        except Exception:
            print(f"Не удалось ответить на callback: {call.id}")
        return

    if state == "choose_timeframe":
        timeframe = call.data
        user_states[chat_id]["timeframe"] = timeframe
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text="Выберите тип графика:",
            reply_markup=get_plot_keyboard()
        )
        user_states[chat_id]["step"] = "choose_plot"
        try:
            bot.answer_callback_query(call.id)
        except Exception:
            print(f"Не удалось ответить на callback: {call.id}")
        return

    if state == "choose_plot":
        plot_type = call.data
        timeframe = user_states[chat_id]["timeframe"]
        plot_and_send_chart(ticker, timeframe, plot_type, chat_id, bot)
        ask_next_company(chat_id)
        try:
            bot.answer_callback_query(call.id)
        except Exception:
            print(f"Не удалось ответить на callback: {call.id}")
        return

# Запуск бота
if __name__ == "__main__":
    companies_df = read_file_content(FILE_PATH)
    monthly_macro_df = read_monthly_macro_content()
    yearly_macro_df = read_yearly_macro_content()
    if companies_df is None:
        print("Предупреждение: Не удалось загрузить данные о компаниях. Функционал поиска тикеров может быть ограничен.")
        bot.polling(none_stop=True, timeout=60)
    else:
        print("Бот запущен с данными о компаниях и макроэкономическими данными...")
        while True:
            try:
                bot.polling(none_stop=True, timeout=60)
            except Exception as e:
                print(f"Ошибка в polling: {e}")
                time.sleep(5)