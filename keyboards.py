from telebot import types

def get_timeframe_keyboard():
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton("1 час", callback_data="1h"))
    keyboard.add(types.InlineKeyboardButton("4 часа", callback_data="4h"))
    keyboard.add(types.InlineKeyboardButton("День", callback_data="Daily"))
    keyboard.add(types.InlineKeyboardButton("Неделя", callback_data="Weekly"))
    keyboard.add(types.InlineKeyboardButton("Месяц", callback_data="Monthly"))
    keyboard.add(types.InlineKeyboardButton("Квартал", callback_data="Quarterly"))
    return keyboard

def get_plot_keyboard():
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton("Да", callback_data="yes_plot"))
    keyboard.add(types.InlineKeyboardButton("Нет", callback_data="no_plot"))
    return keyboard

def get_forecast_menu_keyboard():
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton("Краткосрочный прогноз (1-3 месяца)", callback_data="short_term_forecast"))
    keyboard.add(types.InlineKeyboardButton("Среднесрочный прогноз (3-9 месяцев) ", callback_data="medium_term_forecast"))
    keyboard.add(types.InlineKeyboardButton("Долгосрочный прогноз (1 год и более)", callback_data="long_term_forecast"))
#    keyboard.add(types.InlineKeyboardButton("Диагностика", callback_data="diagnostics"))
    return keyboard