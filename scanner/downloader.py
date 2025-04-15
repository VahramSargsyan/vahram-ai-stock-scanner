import pandas as pd
import os
import glob
from time import sleep
import yfinance as yf
from datetime import datetime


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

def save_results(results, date_str):
    df_new = pd.DataFrame(results)
    os.makedirs("data/archive/", exist_ok=True)

    filename = f"data/archive/market_snapshot_{date_str}.csv"

    if os.path.exists(filename) and os.path.getsize(filename) > 0:
        try:
            df_existing = pd.read_csv(filename)
            df_merged = pd.concat([df_existing, df_new]).drop_duplicates(subset="symbol")
        except pd.errors.EmptyDataError:
            print("⚠️ Пустой файл, создаю с нуля.")
            df_merged = df_new
    else:
        df_merged = df_new

    df_merged.to_csv(filename, index=False)
    print(f"✅ Добавлено: {len(df_new)} компаний. Всего сохранено: {len(df_merged)} → 📄 {filename}")

def load_tickers_from_csv(file_path):
    return pd.read_csv(file_path)["ticker"].tolist()

def run_all_batches():
    date_str = datetime.now().strftime("%Y-%m-%d")

    batch_files = sorted(glob.glob("data/sp500_batch_*.csv"))
    if not batch_files:
        print("❌ Не найдено файлов batch'ей в папке data/")
        return

    for i, file_path in enumerate(batch_files):
        print(f"\n📦 Обработка батча {i+1}/{len(batch_files)}: {file_path}")
        tickers = load_tickers_from_csv(file_path)
        results = []
        for j, ticker in enumerate(tickers):
            print(f"🔄 [{j+1}/{len(tickers)}] Загружаем {ticker}...")
            data = fetch_data(ticker)
            if data:
                results.append(data)
            sleep(1)
        save_results(results, date_str)

if __name__ == "__main__":
    run_all_batches()
