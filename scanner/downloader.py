# scanner/downloader.py

import yfinance as yf
import pandas as pd
import os
from time import sleep

# Пример: можно начать с тикеров S&P 500
SP500_TICKERS = pd.read_csv("data/sp500_tickers.csv")["ticker"].tolist()


FIELDS_TO_EXTRACT = [
    "symbol", "longName", "sector", "trailingPE", "returnOnEquity",
    "debtToEquity", "freeCashflow", "revenueGrowth", "dividendYield"
]

def fetch_data(ticker):
    try:
        data = yf.Ticker(ticker)
        info = data.info
        return {field: info.get(field, None) for field in FIELDS_TO_EXTRACT}
    except Exception as e:
        print(f"[ERROR] {ticker}: {e}")
        return None

def save_results(results, filename="data/raw/market_snapshot.csv"):
    df = pd.DataFrame(results)
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    df.to_csv(filename, index=False)
    print(f"✅ Сохранено: {filename}")

def run():
    results = []
    for i, ticker in enumerate(SP500_TICKERS):
        print(f"🔄 [{i+1}/{len(SP500_TICKERS)}] Загружаем {ticker}...")
        data = fetch_data(ticker)
        if data:
            results.append(data)
        sleep(1)  # Чтобы не заблокировали
    save_results(results)

if __name__ == "__main__":
    run()
