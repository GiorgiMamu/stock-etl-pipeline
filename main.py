import sys
from datetime import datetime
from pathlib import Path

import config
from extract import extract_all_stocks
from transform import transform_all_stocks
from load import load_all_data


def setup_logging():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = config.LOGS_DIR / f"etl_pipeline_{timestamp}.log"
    return log_filename


def log_and_print(message: str, log_file: Path = None):
    print(message)

    if log_file:
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(message + '\n')

def run_etl_pipeline():
    log_file = setup_logging()

    print("ETL PIPELINE STARTED")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Log file: {log_file}")
    print(f"Stocks: {', '.join(config.STOCK_SYMBOLS)}")

    try:
        print("STEP 1: EXTRACT - Fetching data from Alpha Vantage API")
        extracted_files = extract_all_stocks()
        if not extracted_files:
            print("\nEXTRACTION FAILED: No data was extracted")
            return False
        print(f"\nExtraction complete: {len(extracted_files)} stocks")


        print("STEP 2: TRANSFORM - Cleaning and validating data")
        transformed_data = transform_all_stocks(extracted_files)
        if not transformed_data:
            print("\nTRANSFORMATION FAILED: No data was transformed")
            return False
        total_records = sum(len(df) for df in transformed_data.values())
        print(f"\nTransformation complete: {total_records} records")


        print("STEP 3: LOAD - Inserting data into database")
        load_all_data(transformed_data)
        print(f"\nLoad complete")

        print("ETL PIPELINE SUCCESS")
        print(f"Stocks processed: {len(transformed_data)}")
        print(f"Total records: {total_records}")
        print(f"Database: {config.DATABASE_PATH}")
        print(f"Log file: {log_file}")
        return True

    except KeyboardInterrupt:
        print("\nPipeline interrupted by user (Ctrl+C)")
        return False

    except Exception as e:
        print("ETL PIPELINE FAILED")
        print(f"Error: {e}")
        print(f"Error type: {type(e).__name__}")
        print("\nCheck the log file for details:")
        print(f"  {log_file}")

        import traceback
        print("\nFull error traceback:")
        traceback.print_exc()

        return False


def print_usage():
    print("ETL Pipeline - Stock Market Data")
    print("\nUsage:")
    print("  python main.py                 Run the complete ETL pipeline")
    print("  python main.py --help          Show this help message")
    print("\nConfiguration:")
    print(f"  Stocks: {', '.join(config.STOCK_SYMBOLS)}")
    print(f"  Database: {config.DATABASE_PATH}")
    print(f"  Raw data directory: {config.RAW_DATA_DIR}")
    print("\nEdit .env file to change configuration")


def main():
    if len(sys.argv) > 1:
        if sys.argv[1] in ['--help', '-h', 'help']:
            print_usage()
            sys.exit(0)
        else:
            print(f"unknown argument: {sys.argv[1]}")
            print_usage()
            sys.exit(1)

    success = run_etl_pipeline()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()