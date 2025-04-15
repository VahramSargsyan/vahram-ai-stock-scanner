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
        revenue_growth = company.get("revenueGrowth")
        if revenue_growth is not None:
            if revenue_growth * 100 < float(config["revenue_growth"][1:]):
                return False

        roe = company.get("returnOnEquity")
        if roe is not None:
            if roe * 100 < float(config["roe"][1:]):
                return False

        debt_to_equity = company.get("debtToEquity")
        if debt_to_equity is not None:
            if debt_to_equity > float(config["debt_to_equity"][1:]):
                return False

        free_cash_flow = company.get("freeCashflow")
        if free_cash_flow is not None:
            if free_cash_flow <= 0:
                return False

        dividend_yield = company.get("dividendYield")
        if dividend_yield is not None and dividend_yield > 0:
            return False

        sector = company.get("sector")
        if sector is not None:
            if config["sector_whitelist"]:  # если список не пустой
                if sector not in config["sector_whitelist"]:
                    return False


        trailing_pe = company.get("trailingPE")
        if trailing_pe is not None:
            if trailing_pe > config["pe_limit"]:
                return False

        return True

    except Exception as e:
        print(f"[ERROR in filter]: {company.get('symbol')} — {e}")
        return False

# Основной запуск
def run():
    # Пути к файлам
    input_path = "data/archive/market_snapshot_2025-04-14.csv"
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
