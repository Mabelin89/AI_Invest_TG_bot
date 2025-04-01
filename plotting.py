import matplotlib.pyplot as plt
import os
from datetime import datetime


def plot_and_send_chart(chat_id, ticker, timeframe, period_years, data, base_ticker, bot):
    try:
        # Создаём график с четырьмя осями: цена, MACD, Stochastic, RSI
        fig, (ax1, ax2, ax3, ax4) = plt.subplots(4, 1, figsize=(12, 12), height_ratios=[3, 1, 1, 1], sharex=True)

        # График цены и EMA
        ax1.plot(data['date'], data['close'], label='Close Price', color='blue')
        ax1.plot(data['date'], data['EMA_10'], label='EMA 10', color='purple', linestyle='-.')
        ax1.plot(data['date'], data['EMA_20'], label='EMA 20', color='brown', linestyle='-.')

        # Полосы Боллинджера
        ax1.plot(data['date'], data['BB_upper'], label='BB Upper', color='gray', linestyle=':')
        ax1.plot(data['date'], data['BB_lower'], label='BB Lower', color='gray', linestyle=':')
        ax1.fill_between(data['date'], data['BB_lower'], data['BB_upper'], color='gray', alpha=0.1)

        ax1.set_title(f"{ticker} Diagnostics ({timeframe}, {period_years} years)")
        ax1.set_ylabel("Price (RUB)")
        ax1.legend(loc='upper left')
        ax1.grid(True)

        # График MACD
        ax2.plot(data['date'], data['MACD'], label='MACD', color='blue')
        ax2.plot(data['date'], data['MACD_signal'], label='Signal', color='orange')
        ax2.bar(data['date'], data['MACD_histogram'], label='Histogram', color='gray', alpha=0.5)
        ax2.axhline(0, color='black', linestyle='--', alpha=0.5)
        ax2.set_ylabel("MACD")
        ax2.legend(loc='upper left')
        ax2.grid(True)

        # График Stochastic Oscillator
        ax3.plot(data['date'], data['Stoch_K'], label='%K', color='blue')
        ax3.plot(data['date'], data['Stoch_D'], label='%D', color='orange')
        ax3.axhline(80, color='red', linestyle='--', alpha=0.5)
        ax3.axhline(20, color='green', linestyle='--', alpha=0.5)
        ax3.set_ylabel("Stochastic")
        ax3.legend(loc='upper left')
        ax3.grid(True)

        # График RSI 14
        ax4.plot(data['date'], data['RSI_14'], label='RSI 14', color='purple')
        ax4.axhline(70, color='red', linestyle='--', alpha=0.5)
        ax4.axhline(30, color='green', linestyle='--', alpha=0.5)
        ax4.set_ylabel("RSI")
        ax4.set_xlabel("Date")
        ax4.legend(loc='upper left')
        ax4.grid(True)
        ax4.set_ylim(0, 100)  # Ограничение диапазона RSI от 0 до 100

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