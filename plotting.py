import matplotlib.pyplot as plt
import os
from datetime import datetime


def plot_and_send_chart(chat_id, ticker, timeframe, period_years, data, base_ticker, bot):
    try:
        # Создаём график с двумя осями: цена и RSI
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), height_ratios=[3, 1], sharex=True)

        # График цены и скользящих средних
        ax1.plot(data['date'], data['close'], label='Close Price', color='blue')
        ax1.plot(data['date'], data['SMA_10'], label='SMA 10', color='orange', linestyle='--')
        ax1.plot(data['date'], data['SMA_20'], label='SMA 20', color='green', linestyle='--')
        ax1.plot(data['date'], data['SMA_50'], label='SMA 50', color='red', linestyle='--')
        ax1.plot(data['date'], data['EMA_10'], label='EMA 10', color='purple', linestyle='-.')
        ax1.plot(data['date'], data['EMA_20'], label='EMA 20', color='brown', linestyle='-.')

        # Полосы Боллинджера
        ax1.plot(data['date'], data['BB_upper'], label='BB Upper', color='gray', linestyle=':')
        ax1.plot(data['date'], data['BB_lower'], label='BB Lower', color='gray', linestyle=':')
        ax1.fill_between(data['date'], data['BB_lower'], data['BB_upper'], color='gray', alpha=0.1)

        ax1.set_title(f"{ticker} Price with Indicators ({timeframe}, {period_years} years)")
        ax1.set_ylabel("Price (RUB)")
        ax1.legend(loc='upper left')
        ax1.grid(True)

        # График RSI
        ax2.plot(data['date'], data['RSI_14'], label='RSI 14', color='purple')
        ax2.axhline(70, color='red', linestyle='--', alpha=0.5)
        ax2.axhline(30, color='green', linestyle='--', alpha=0.5)
        ax2.set_ylabel("RSI")
        ax2.set_xlabel("Date")
        ax2.legend(loc='upper left')
        ax2.grid(True)

        # Настройка отступов
        plt.tight_layout()

        # Сохраняем график
        plot_path = f"plot_{ticker}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        plt.savefig(plot_path)
        plt.close()

        # Отправляем график в Telegram
        with open(plot_path, 'rb') as photo:
            bot.send_photo(chat_id, photo)

        # Удаляем временный файл
        os.remove(plot_path)
        print(f"График для {ticker} отправлен и удалён: {plot_path}")
    except Exception as e:
        print(f"Ошибка в plot_and_send_chart для {ticker}: {str(e)}")
        bot.send_message(chat_id, f"Не удалось отправить график: {str(e)}")