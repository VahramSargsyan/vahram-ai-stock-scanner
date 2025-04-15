# scanner/analyzer.py

import pandas as pd
import yaml
import os


# Загрузка конфигурации фильтра
def load_filter_config(path="config/filter_criteria.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

# Проверка компании по фильтру
def company_passes_filters(company, config):
    try:
        if company["revenueGrowth"] * 100 < float(config["revenue_growth"][1:]):
            return False
        if company["returnOnEquity"] * 100 < float(config["roe"][1:]):
            return False
        if company["debtToEquity"] > float(config["debt_to_equity"][1:]):
            return False
        if company["freeCashflow"] is None or company["freeCashflow"] <= 0:
            return False
        if company.get("dividendYield", 0) not in [None, 0.0]:
            return False
        if company["sector"] not in config["sector_whitelist"]:
            return False
        if company.get("trailingPE", 0) > config["pe_limit"]:
            return False
        return True
    except Exception as e:
        print(f"[ERROR in filter]: {company.get('symbol')} — {e}")
        return False

# Основной запуск
def run():
    # Пути к файлам
    input_path = "data/raw/market_snapshot.csv"
    output_path = "data/filtered/filtered_companies.csv"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Загрузка данных
    df = pd.read_csv(input_path)
    config = load_filter_config()

    # Фильтрация
    filtered = df[df.apply(lambda row: company_passes_filters(row, config), axis=1)]

    # Сохранение
    filtered.to_csv(output_path, index=False)
    print(f"✅ Отобрано компаний: {len(filtered)}")
    print(f"📄 Сохранено в: {output_path}")

if __name__ == "__main__":
    run()
