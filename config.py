import os
from pathlib import Path
from dotenv import load_dotenv


load_dotenv()

BASE_DIR = Path(__file__).parent.resolve()

# create paths for our data folders
RAW_DATA_DIR = BASE_DIR / "raw_data"
DATABASE_DIR = BASE_DIR / "database"
LOGS_DIR = BASE_DIR / "logs"

# exist_okay=True so we get no error if folder exists.
RAW_DATA_DIR.mkdir(exist_ok=True)
DATABASE_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# get key
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
API_BASE_URL = "https://www.alphavantage.co/query"

STOCK_SYMBOLS = os.getenv("STOCKS", "AAPL,GOOG,MSFT").split(",")

DATABASE_PATH = DATABASE_DIR / "stock_data.db"
TABLE_NAME = "stock_daily_data"

# seconds between API calls
API_CALL_DELAY = 12

# validation
if not ALPHA_VANTAGE_API_KEY:
    raise ValueError(
        "ALPHA_VANTAGE_API_KEY not found! "
        "add it to your .env file."
    )
