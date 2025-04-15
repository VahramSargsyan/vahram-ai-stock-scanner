# scanner/downloader.py

import pandas as pd
import os
import sys
from time import sleep
import yfinance as yf


# Пример: можно начать с тикеров S&P 500
def load_tickers(file_path):
    return pd.read_csv(file_path)["ticker"].tolist()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("❌ Укажи путь к CSV-файлу со списком тикеров:")
        print("Пример: python downloader.py data/sp500_batch_1.csv")
        sys.exit(1)

    tickers_file = sys.argv[1]
    SP500_TICKERS = load_tickers(tickers_file)

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

def save_results(results, filename="data/raw/market_snapshot_full.csv"):
    df_new = pd.DataFrame(results)
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    if os.path.exists(filename) and os.path.getsize(filename) > 0:
        df_existing = pd.read_csv(filename)
        df_merged = pd.concat([df_existing, df_new]).drop_duplicates(subset="symbol")
    else:
        df_merged = df_new

    df_merged.to_csv(filename, index=False)
    print(f"✅ Добавлено: {len(df_new)} компаний. Всего сохранено: {len(df_merged)}")

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
