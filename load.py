import sqlite3
import pandas as pd
from datetime import datetime
from typing import List, Dict
from pathlib import Path

import config


def get_database_connection() -> sqlite3.Connection:
    print(f"connecting to database: {config.DATABASE_PATH}")

    try:
        conn = sqlite3.connect(config.DATABASE_PATH)
        conn.execute("PRAGMA foreign_keys = ON")
        print("database connection established")
        return conn

    except sqlite3.Error as e:
        print(f"database connection failed: {e}")
        raise


def create_table_if_not_exists(conn: sqlite3.Connection):
    print(f"creating table '{config.TABLE_NAME}' if not exists...")

    # SQL query
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {config.TABLE_NAME} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        date DATE NOT NULL,
        open_price REAL NOT NULL,
        high_price REAL NOT NULL,
        low_price REAL NOT NULL,
        close_price REAL NOT NULL,
        volume INTEGER NOT NULL,
        daily_change_percentage REAL,
        extraction_timestamp TIMESTAMP NOT NULL,
        UNIQUE(symbol, date)
    )
    """

    try:
        conn.execute(create_table_sql)
        conn.commit()
        print(f"table '{config.TABLE_NAME}' ready")

    except sqlite3.Error as e:
        print(f"failed to create table: {e}")
        raise


def create_index_if_not_exists(conn: sqlite3.Connection):
    print("creating indexes...")
    index_symbol_sql = f"""
    CREATE INDEX IF NOT EXISTS idx_symbol 
    ON {config.TABLE_NAME}(symbol)
    """

    index_date_sql = f"""
    CREATE INDEX IF NOT EXISTS idx_date 
    ON {config.TABLE_NAME}(date)
    """

    index_symbol_date_sql = f"""
    CREATE INDEX IF NOT EXISTS idx_symbol_date 
    ON {config.TABLE_NAME}(symbol, date)
    """

    try:
        conn.execute(index_symbol_sql)
        conn.execute(index_date_sql)
        conn.execute(index_symbol_date_sql)
        conn.commit()

        print("iindexes created")

    except sqlite3.Error as e:
        print(f"failed to create indexes: {e}")
        raise

def insert_data(conn: sqlite3.Connection, df: pd.DataFrame, symbol: str) -> int:
    print(f"inserting data for {symbol}...")
    insert_sql = f"""
    INSERT OR IGNORE INTO {config.TABLE_NAME} 
    (symbol, date, open_price, high_price, low_price, close_price, 
     volume, daily_change_percentage, extraction_timestamp)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    try:
        cursor = conn.cursor()
        rows_before = cursor.execute(f"SELECT COUNT(*) FROM {config.TABLE_NAME}").fetchone()[0]
        records = []
        for _, row in df.iterrows():
            record = (
                row["symbol"],
                row["date"].strftime("%Y-%m-%d"),
                row["open_price"],
                row["high_price"],
                row["low_price"],
                row["close_price"],
                row["volume"],
                row["daily_change_percentage"],
                row["extraction_timestamp"].strftime("%Y-%m-%d %H:%M:%S")
            )
            records.append(record)

        cursor.executemany(insert_sql, records)
        conn.commit()

        rows_after = cursor.execute(f"SELECT COUNT(*) FROM {config.TABLE_NAME}").fetchone()[0]
        rows_inserted = rows_after - rows_before

        print(f"   inserted {rows_inserted} new rows for {symbol}")
        print(f"   (skipped {len(records) - rows_inserted} duplicates)")

        return rows_inserted

    except sqlite3.Error as e:
        print(f"failed to insert data for {symbol}: {e}")
        conn.rollback()
        raise


def verify_data(conn: sqlite3.Connection, symbol: str):
    print(f"\nverifying data for {symbol}...")
    query = f"""
    SELECT 
        symbol,
        date,
        open_price,
        close_price,
        volume,
        daily_change_percentage
    FROM {config.TABLE_NAME}
    WHERE symbol = ?
    ORDER BY date DESC
    LIMIT 5
    """

    try:
        df = pd.read_sql_query(query, conn, params=(symbol,))

        if df.empty:
            print(f"   no data found for {symbol}")
        else:
            print(f"   latest 5 records for {symbol}:")
            print(df.to_string(index=False))

    except sqlite3.Error as e:
        print(f"query failed: {e}")


def get_database_stats(conn: sqlite3.Connection):
    print(f"\ndatabase Statistics:")

    try:
        cursor = conn.cursor()
        total_query = f"SELECT COUNT(*) FROM {config.TABLE_NAME}"
        total = cursor.execute(total_query).fetchone()[0]
        print(f"total records: {total}")
        symbol_query = f"""
        SELECT symbol, COUNT(*) as count 
        FROM {config.TABLE_NAME} 
        GROUP BY symbol
        ORDER BY symbol
        """
        symbol_counts = cursor.execute(symbol_query).fetchall()

        print("\nrecords per symbol:")
        for symbol, count in symbol_counts:
            print(f"  {symbol}: {count}")

        date_query = f"""
        SELECT 
            MIN(date) as earliest,
            MAX(date) as latest
        FROM {config.TABLE_NAME}
        """
        dates = cursor.execute(date_query).fetchone()
        print(f"\nDate range: {dates[0]} to {dates[1]}")

    except sqlite3.Error as e:
        print(f"failed to get stats: {e}")



def load_all_data(transformed_data: Dict[str, pd.DataFrame]):
    """
        1. Connect to database
        2. Create table and indexes
        3. Insert data for each stock
        4. Verify insertion
        5. Show statistics
    """


    print("STARTING DATA LOAD")

    conn = get_database_connection()

    try:
        create_table_if_not_exists(conn)
        create_index_if_not_exists(conn)

        total_inserted = 0

        for symbol, df in transformed_data.items():
            print(f"LOADING: {symbol}")

            rows_inserted = insert_data(conn, df, symbol)
            total_inserted += rows_inserted
            verify_data(conn, symbol)

        get_database_stats(conn)

        print("DATA LOAD COMPLETE")
        print(f"total new records inserted: {total_inserted}")
        print(f"database location: {config.DATABASE_PATH}")

    finally:
        conn.close()
        print("\ndatabase connection closed")



