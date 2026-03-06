from pathlib import Path
import pandas as pd
from database.db import get_connection

# Mapping of common column name variations to standardized column names used in the database.
COLUMN_MAP = {
    "symbol": "symbol",
    "instrument": "symbol",
    "pair": "symbol",

    "type": "trade_type",
    "side": "trade_type",
    "direction": "trade_type",

    "entry": "entry_price",
    "entry_price": "entry_price",
    "open_price": "entry_price",
    "price_open": "entry_price",

    "exit": "exit_price",
    "exit_price": "exit_price",
    "close_price": "exit_price",
    "price_close": "exit_price",

    "sl": "stop_loss",
    "stop_loss": "stop_loss",

    "tp": "take_profit",
    "take_profit": "take_profit",

    "lot": "lot_size",
    "lots": "lot_size",
    "volume": "lot_size",

    "open_time": "open_time",
    "entry_time": "open_time",
    "time_open": "open_time",

    "close_time": "close_time",
    "exit_time": "close_time",
    "time_close": "close_time",

    "profit": "pnl",
    "pnl": "pnl",
    "net_profit": "pnl",
}

  # Normalize column names by stripping, converting to lowercase, and mapping known variations.
def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
 
    df.columns = [str(col).strip().lower() for col in df.columns]
    df = df.rename(columns={col: COLUMN_MAP[col] for col in df.columns if col in COLUMN_MAP})
    return df

# Keep only columns used by the current database schema
def keep_supported_columns(df: pd.DataFrame) -> pd.DataFrame:
    allowed = [
        "symbol",
        "trade_type",
        "entry_price",
        "exit_price",
        "stop_loss",
        "take_profit",
        "lot_size",
        "open_time",
        "close_time",
        "pnl",
    ]
    existing = [col for col in allowed if col in df.columns]
    return df[existing].copy()


# Add any missing columns with None values so that all inserts have the same structure regardless of source file variations.
def add_missing_columns(df: pd.DataFrame) -> pd.DataFrame:
    required = [
        "symbol",
        "trade_type",
        "entry_price",
        "exit_price",
        "stop_loss",
        "take_profit",
        "lot_size",
        "open_time",
        "close_time",
        "pnl",
    ]

    for col in required:
        if col not in df.columns:
            df[col] = None

    return df[required]

 # Clean up common value types and values for consistency before inserting into the database.
def clean_basic_values(df: pd.DataFrame) -> pd.DataFrame:
    text_cols = ["symbol", "trade_type", "open_time", "close_time"]
    for col in text_cols:
        df[col] = df[col].astype(str).str.strip()

    df["symbol"] = df["symbol"].str.upper()
    df["trade_type"] = df["trade_type"].str.lower()

    numeric_cols = [
        "entry_price",
        "exit_price",
        "stop_loss",
        "take_profit",
        "lot_size",
        "pnl",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# Main function to import a CSV file, standardize it, and insert into the database.
def import_csv(file_path: str) -> int:
  
    file_path = Path(file_path)

    df = pd.read_csv(file_path)
    df = standardize_columns(df)
    df = keep_supported_columns(df)
    df = add_missing_columns(df)
    df = clean_basic_values(df)

    # Save source filename for traceability in the database
    df["source_file"] = file_path.name

    conn = get_connection()

    records = df.to_dict(orient="records")
    conn.executemany(
        """
        INSERT INTO trades (
            symbol, trade_type, entry_price, exit_price,
            stop_loss, take_profit, lot_size,
            open_time, close_time, pnl, source_file
        )
        VALUES (
            :symbol, :trade_type, :entry_price, :exit_price,
            :stop_loss, :take_profit, :lot_size,
            :open_time, :close_time, :pnl, :source_file
        )
        """,
        records,
    )

    conn.commit()
    conn.close()

    return len(records)