from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# Функция для создания клавиатуры с таймфреймами
def get_timeframe_keyboard():
    keyboard = InlineKeyboardMarkup()
    keyboard.row(
        InlineKeyboardButton("1m", callback_data="1m"),
        InlineKeyboardButton("10m", callback_data="10m"),
        InlineKeyboardButton("1h", callback_data="1h")
    )
    keyboard.row(
        InlineKeyboardButton("Daily", callback_data="daily"),
        InlineKeyboardButton("Weekly", callback_data="weekly")
    )
    keyboard.row(
        InlineKeyboardButton("Monthly", callback_data="monthly"),
        InlineKeyboardButton("Quarterly", callback_data="quarterly")
    )
    return keyboard

# Функция для создания клавиатуры "Да/Нет"
def get_plot_keyboard():
    keyboard = InlineKeyboardMarkup()
    keyboard.row(
        InlineKeyboardButton("Да", callback_data="yes_plot"),
        InlineKeyboardButton("Нет", callback_data="no_plot")
    )
    return keyboard