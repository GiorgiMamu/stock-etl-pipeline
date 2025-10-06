import requests
import json
from datetime import datetime
from pathlib import Path
import time
from typing import Optional
import config
from models import AlphaVantageResponse


def fetch_stock_data(symbol: str) -> Optional[dict]:
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": config.ALPHA_VANTAGE_API_KEY,
        "outputsize": "compact"
    }

    print(f"fetching data for {symbol}...")

    try:
        # make the HTTP GET request
        response = requests.get(
            config.API_BASE_URL,
            params=params,
            timeout=10
        )
        response.raise_for_status()

        data = response.json()

        if "Error Message" in data:
            print(f"API Error for {symbol}: {data['Error Message']}")
            return None

        if "Note" in data:
            print(f"rate limit warning: {data['Note']}")
            return None

        try:
            AlphaVantageResponse(**data)
            print(f"successfully fetched data for {symbol}")
            return data
        except Exception as e:
            print(f"invalid data structure for {symbol}: {e}")
            return None

    except requests.exceptions.Timeout:
        print(f"request timeout for {symbol}")
        return None

    except requests.exceptions.RequestException as e:
        print(f"request failed for {symbol}: {e}")
        return None


def save_raw_data(symbol: str, data: dict) -> Path:
    today = datetime.now().date().isoformat()
    filename = f"{symbol}_{today}.json"
    filepath = config.RAW_DATA_DIR / filename

    print(f"saving raw data to {filepath}...")

    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        print(f"saved {filepath}")
        return filepath

    except Exception as e:
        print(f"failed to save file: {e}")
        raise


def extract_all_stocks():
    print("STARTING DATA EXTRACTION")
    print(f"stocks to fetch: {config.STOCK_SYMBOLS}")
    print(f"API call delay: {config.API_CALL_DELAY} seconds")

    results = {}

    for i, symbol in enumerate(config.STOCK_SYMBOLS):
        print(f"\n[{i + 1}/{len(config.STOCK_SYMBOLS)}] Processing {symbol}...")
        data = fetch_stock_data(symbol)

        if data:
            # Save raw JSON to file
            try:
                filepath = save_raw_data(symbol, data)
                results[symbol] = filepath
            except Exception as e:
                print(f"failed to save {symbol}: {e}")
        else:
            print(f"skipping {symbol} - no data received")

        if i < len(config.STOCK_SYMBOLS) - 1:
            print(f"waiting {config.API_CALL_DELAY} seconds before next request...")
            time.sleep(config.API_CALL_DELAY)

    print("EXTRACTION COMPLETE")
    print(f"successfully saved: {len(results)}/{len(config.STOCK_SYMBOLS)} stocks")
    for symbol, filepath in results.items():
        print(f"{symbol}: {filepath.name}")
    return results


