import matplotlib.pyplot as plt
import os

def plot_and_send_chart(chat_id, ticker, timeframe, period_years, data, base_ticker, bot):
    # Первый график: только цена закрытия
    plt.figure(figsize=(10, 5))
    plt.plot(data['date'], data['close'], label=f"{ticker} ({timeframe})")
    plt.title(f"Цена акции {ticker} за {period_years} лет")
    plt.xlabel("Дата")
    plt.ylabel("Цена закрытия")
    plt.legend()
    plt.grid()
    plt.xticks(rotation=45)
    chart_path = f"{ticker}_{timeframe}_{period_years}Y_chart.png"
    plt.savefig(chart_path, bbox_inches='tight')
    plt.close()

    with open(chart_path, 'rb') as photo:
        bot.send_photo(chat_id, photo)
    os.remove(chart_path)

    # Второй график: цена с индикаторами
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 12), gridspec_kw={'height_ratios': [3, 1, 1]}, sharex=True)

    # Основной график: цена, SMA, EMA, Bollinger Bands
    ax1.plot(data['date'], data['close'], label='Close', color='blue')
    ax1.plot(data['date'], data['SMA_20'], label='SMA 20', color='orange', linestyle='--')
    ax1.plot(data['date'], data['SMA_50'], label='SMA 50', color='purple', linestyle='--')
    ax1.plot(data['date'], data['SMA_200'], label='SMA 200', color='black', linestyle='--')
    ax1.plot(data['date'], data['EMA_20'], label='EMA 20', color='green', linestyle='--')
    ax1.plot(data['date'], data['BB_upper'], label='BB Upper', color='red', linestyle='-.')
    ax1.plot(data['date'], data['BB_lower'], label='BB Lower', color='red', linestyle='-.')
    ax1.fill_between(data['date'], data['BB_upper'], data['BB_lower'], color='red', alpha=0.1)
    ax1.set_title(f"Технический анализ {ticker} ({timeframe}) за {period_years} лет")
    ax1.set_ylabel("Цена")
    ax1.legend(loc='upper left')
    ax1.grid()

    # RSI
    ax2.plot(data['date'], data['RSI_14'], label='RSI 14', color='purple')
    ax2.axhline(70, color='red', linestyle='--', alpha=0.5)
    ax2.axhline(30, color='green', linestyle='--', alpha=0.5)
    ax2.set_ylabel("RSI")
    ax2.legend(loc='upper left')
    ax2.grid()

    # MACD
    ax3.plot(data['date'], data['MACD'], label='MACD', color='blue')
    ax3.plot(data['date'], data['MACD_signal'], label='Signal', color='orange')
    ax3.bar(data['date'], data['MACD_histogram'], label='Histogram', color='gray', alpha=0.5)
    ax3.set_xlabel("Дата")
    ax3.set_ylabel("MACD")
    ax3.legend(loc='upper left')
    ax3.grid()

    plt.xticks(rotation=45)
    plt.tight_layout()
    indicators_chart_path = f"{ticker}_{timeframe}_{period_years}Y_indicators.png"
    plt.savefig(indicators_chart_path, bbox_inches='tight')
    plt.close()

    with open(indicators_chart_path, 'rb') as photo:
        bot.send_photo(chat_id, photo)
    os.remove(indicators_chart_path)

    # Анализ отчетов и отправка показателей
    from data_processing import analyze_msfo_report
    msfo_analysis = analyze_msfo_report(ticker, base_ticker, chat_id, bot)
    bot.send_message(chat_id, f"Ключевые показатели для {base_ticker}:\n{msfo_analysis}")