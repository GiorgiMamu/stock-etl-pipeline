import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import List

from models import StockDailyData

def load_raw_json(filepath: Path) -> dict:
    print(f"Reading {filepath.name}...")

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"Loaded {filepath.name}")
        return data

    except FileNotFoundError:
        print(f"File not found: {filepath}")
        raise

    except json.JSONDecodeError as e:
        print(f"Invalid JSON in {filepath}: {e}")
        raise


def parse_alpha_vantage_data(data: dict, symbol: str) -> pd.DataFrame:
    print(f"Parsing data for {symbol}...")
    time_series = data.get("Time Series (Daily)", {})

    if not time_series:
        raise ValueError(f"No time series data found for {symbol}")

    rows = []

    for date_str, values in time_series.items():
        row = {
            "date": date_str,
            "open": float(values["1. open"]),
            "high": float(values["2. high"]),
            "low": float(values["3. low"]),
            "close": float(values["4. close"]),
            "volume": int(values["5. volume"])
        }
        rows.append(row)


    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])

    df = df.sort_values("date").reset_index(drop=True)

    print(f"Parsed {len(df)} rows for {symbol}")

    return df


def calculate_daily_change(df: pd.DataFrame) -> pd.DataFrame:

    print("Calculating daily change percentage...")
    df["daily_change_percentage"] = (
                                            (df["close"] - df["open"]) / df["open"]
                                    ) * 100

    df["daily_change_percentage"] = df["daily_change_percentage"].round(2)

    print("Daily change calculated")
    return df


def validate_with_pydantic(df: pd.DataFrame, symbol: str) -> List[StockDailyData]:

    print(f"Validating data with Pydantic...")
    validated_records = []
    errors = 0

    for idx, row in df.iterrows():
        try:
            stock_data = StockDailyData(
                symbol=symbol,
                date=row["date"].date(),
                open_price=row["open"],
                high_price=row["high"],
                low_price=row["low"],
                close_price=row["close"],
                volume=row["volume"],
                daily_change_percentage=row["daily_change_percentage"],
                extraction_timestamp=datetime.now()
            )

            validated_records.append(stock_data)

        except Exception as e:
            errors += 1
            print(f"Validation error on row {idx}: {e}")

    print(f"Validated {len(validated_records)} records ({errors} errors)")

    return validated_records


def transform_stock_data(filepath: Path, symbol: str) -> pd.DataFrame:
    print(f"TRANSFORMING: {symbol}")

    raw_data = load_raw_json(filepath)

    df = parse_alpha_vantage_data(raw_data, symbol)

    df["symbol"] = symbol

    df = calculate_daily_change(df)

    validated_records = validate_with_pydantic(df, symbol)

    if validated_records:
        validated_dicts = [record.model_dump() for record in validated_records]
        df_validated = pd.DataFrame(validated_dicts)

        column_order = [
            "symbol",
            "date",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "daily_change_percentage",
            "extraction_timestamp"
        ]
        df_validated = df_validated[column_order]

        print(f"Transformation complete: {len(df_validated)} rows")
        return df_validated
    else:
        print("No valid records after validation")
        return pd.DataFrame()


def transform_all_stocks(extracted_files: dict) -> dict:
    print("STARTING DATA TRANSFORMATION")

    transformed_data = {}

    for symbol, filepath in extracted_files.items():
        try:
            df = transform_stock_data(filepath, symbol)

            if not df.empty:
                transformed_data[symbol] = df
                print(f"\nSample data for {symbol}:")
                print(df.head(3).to_string())
                print(f"\n Stats:")
                print(f"   Rows: {len(df)}")
                print(f"   Date range: {df['date'].min()} to {df['date'].max()}")
                print(f"   Avg daily change: {df['daily_change_percentage'].mean():.2f}%")

        except Exception as e:
            print(f"Failed to transform {symbol}: {e}")

    print("TRANSFORMATION COMPLETE")
    print(f"Successfully transformed: {len(transformed_data)}/{len(extracted_files)} stocks")
    return transformed_data


