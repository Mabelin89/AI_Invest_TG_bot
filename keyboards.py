from telebot import types

def get_timeframe_keyboard():
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton("Daily", callback_data="Daily"))
    keyboard.add(types.InlineKeyboardButton("Weekly", callback_data="Weekly"))
    keyboard.add(types.InlineKeyboardButton("Monthly", callback_data="Monthly"))
    return keyboard

def get_plot_keyboard():
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton("Да", callback_data="yes_plot"))
    keyboard.add(types.InlineKeyboardButton("Нет", callback_data="no_plot"))
    return keyboard