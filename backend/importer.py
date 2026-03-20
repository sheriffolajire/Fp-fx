from pathlib import Path
from io import StringIO

import pandas as pd
from database.db import get_connection

# Mapping of common column name variations to standardized column names used in the database.
COLUMN_MAP = {
    "order": "order_id",
    "ticket": "order_id",
    "deal": "order_id",
    "position id": "order_id",

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
    "price": "entry_price",

    "exit": "exit_price",
    "exit_price": "exit_price",
    "close_price": "exit_price",
    "price_close": "exit_price",

    "sl": "stop_loss",
    "stop_loss": "stop_loss",
    "s / l": "stop_loss",

    "tp": "take_profit",
    "take_profit": "take_profit",
    "t / p": "take_profit",

    "lot": "lot_size",
    "lots": "lot_size",
    "volume": "lot_size",

    "open_time": "open_time",
    "entry_time": "open_time",
    "time_open": "open_time",
    "open time": "open_time",

    "close_time": "close_time",
    "exit_time": "close_time",
    "time_close": "close_time",
    "time": "close_time",

    "profit": "pnl",
    "pnl": "pnl",
    "net_profit": "pnl",
}

# Standardizes column names by stripping whitespace.
def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(col).strip().lower() for col in df.columns]
    df = df.rename(columns={col: COLUMN_MAP[col] for col in df.columns if col in COLUMN_MAP})
    return df

# Keeps only the columns that are supported by the database schema, ignoring any extra columns.
def keep_supported_columns(df: pd.DataFrame) -> pd.DataFrame:
    allowed = [
        "order_id",
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

# Adds any missing columns that are required by the database schema.
def add_missing_columns(df: pd.DataFrame) -> pd.DataFrame:
    required = [
        "order_id",
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

# Cleaning and standardizing basic values in the DataFrame.
def clean_basic_values(df: pd.DataFrame) -> pd.DataFrame:
    text_cols = ["order_id", "symbol", "trade_type", "open_time", "close_time"]

    for col in text_cols:
        df[col] = df[col].fillna("").astype(str).str.strip()

    df["order_id"] = df["order_id"].replace("", pd.NA)
    df["symbol"] = df["symbol"].str.replace("+", "", regex=False).str.upper()
    df["trade_type"] = df["trade_type"].str.lower().str.strip()

    if "lot_size" in df.columns:
        df["lot_size"] = (
            df["lot_size"]
            .astype(str)
            .str.split("/")
            .str[0]
            .str.strip()
        )

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

# Filter out rows that are missing critical information or invalid trade types.
def filter_valid_trade_rows(df: pd.DataFrame) -> pd.DataFrame:
    valid_trade_types = {
        "buy",
        "sell",
        "buy limit",
        "sell limit",
        "buy stop",
        "sell stop",
        "buy stop limit",
        "sell stop limit",
    }

    df = df[df["symbol"].notna()]
    df = df[df["symbol"] != ""]
    df = df[df["symbol"] != "NAN"]
    df = df[df["symbol"] != "SYMBOL"]

    df = df[df["trade_type"].notna()]
    df = df[df["trade_type"].isin(valid_trade_types)]

    df = df[df["entry_price"].notna()]

    df = df[df["open_time"].notna()]
    df = df[df["open_time"] != ""]
    df = df[df["open_time"].str.lower() != "none"]

    return df.reset_index(drop=True)

# Remove duplicates within the current DataFrame before comparing against the database.
def remove_internal_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicates inside the current import file before touching the database.
    Prefer order_id when available.
    """
    if "order_id" in df.columns and df["order_id"].notna().any():
        df = df.drop_duplicates(subset=["order_id"], keep="first")
    else:
        df = df.drop_duplicates(
            subset=["symbol", "trade_type", "entry_price", "open_time", "source_file"],
            keep="first",
        )
    return df.reset_index(drop=True)

# Check the database for existing entries to avoid importing duplicates.
def filter_existing_database_duplicates(df: pd.DataFrame, conn) -> pd.DataFrame:
    """
    Remove rows already present in the database.
    1. Skip full file import if source_file already exists.
    2. Skip specific rows whose order_id already exists.
    """
    if df.empty:
        return df

    source_file = df["source_file"].iloc[0]

    existing_file = conn.execute(
        "SELECT 1 FROM trades WHERE source_file = ? LIMIT 1",
        (source_file,),
    ).fetchone()

    if existing_file is not None:
        print(f"Skipped import: source file '{source_file}' already exists in database.")
        return df.iloc[0:0]

    order_ids = [oid for oid in df["order_id"].dropna().astype(str).tolist() if oid.strip() != ""]
    if order_ids:
        placeholders = ",".join(["?"] * len(order_ids))
        query = f"SELECT order_id FROM trades WHERE order_id IN ({placeholders})"
        existing_orders = conn.execute(query, order_ids).fetchall()
        existing_orders = {row["order_id"] for row in existing_orders}

        if existing_orders:
            df = df[~df["order_id"].astype(str).isin(existing_orders)]

    return df.reset_index(drop=True)

# Main function to import a CSV file, clean and standardize the data, and insert it into the database.
def import_csv(file_path: str) -> int:
    file_path = Path(file_path)

    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()

    start_idx = None
    for i, line in enumerate(lines):
        if "Open Time" in line and "Symbol" in line:
            start_idx = i
            break

    if start_idx is None:
        raise ValueError("Trade table not found in file")

    data = "".join(lines[start_idx:])
    df = pd.read_csv(StringIO(data))

    df = standardize_columns(df)

    if "entry_price" in df.columns and "exit_price" not in df.columns:
        df["exit_price"] = df["entry_price"]

    df = keep_supported_columns(df)
    df = add_missing_columns(df)
    df = clean_basic_values(df)
    df = filter_valid_trade_rows(df)

    df["source_file"] = file_path.name
    df = remove_internal_duplicates(df)

    conn = get_connection()
    df = filter_existing_database_duplicates(df, conn)

    if df.empty:
        conn.close()
        return 0

    records = df.to_dict(orient="records")

    conn.executemany(
        """
        INSERT INTO trades (
            order_id, symbol, trade_type, entry_price, exit_price,
            stop_loss, take_profit, lot_size,
            open_time, close_time, pnl, source_file
        )
        VALUES (
            :order_id, :symbol, :trade_type, :entry_price, :exit_price,
            :stop_loss, :take_profit, :lot_size,
            :open_time, :close_time, :pnl, :source_file
        )
        """,
        records,
    )

    conn.commit()
    conn.close()

    return len(records)