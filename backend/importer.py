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
    "open price": "entry_price",
    "price open": "entry_price",
    "price": "entry_price",

    "exit": "exit_price",
    "exit_price": "exit_price",
    "close price": "exit_price",
    "price close": "exit_price",

    "sl": "stop_loss",
    "stop_loss": "stop_loss",

    "tp": "take_profit",
    "take_profit": "take_profit",

    "lot": "lot_size",
    "lots": "lot_size",
    "volume": "lot_size",

    "open time": "open_time",
    "entry time": "open_time",
    "time open": "open_time",

    "close time": "close_time",
    "exit time": "close_time",
    "time close": "close_time",

    "profit": "pnl",
    "pnl": "pnl",
    "net profit": "pnl",

    "commission": "commission",
    "swap": "swap",
}


# Standardizes column names by stripping whitespace.
def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    cleaned_cols = []

    for col in df.columns:
        col_clean = str(col).strip().lower()
        col_clean = col_clean.replace("(", "").replace(")", "")
        col_clean = col_clean.replace("/", " ")
        col_clean = col_clean.replace("-", " ")
        col_clean = col_clean.replace("_", " ")
        col_clean = " ".join(col_clean.split())
        cleaned_cols.append(col_clean)

    df.columns = cleaned_cols
    df = df.rename(columns={col: COLUMN_MAP[col] for col in df.columns if col in COLUMN_MAP})

    return df


# Ensures that the "pnl" column exists and is calculated correctly by summing profit, commission, and swap.
def ensure_pnl_column(df: pd.DataFrame) -> pd.DataFrame:
    if "pnl" not in df.columns:
        for col in df.columns:
            if "profit" in col:
                df["pnl"] = df[col]
                print(f"Detected profit column: {col}")
                break

    for col in ["pnl", "commission", "swap"]:
        if col not in df.columns:
            df[col] = 0

        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["pnl"] = df["pnl"] + df["commission"] + df["swap"]

    return df


# Keeps only the columns that are supported by the database schema, ignoring any extra columns.
def keep_supported_columns(df: pd.DataFrame) -> pd.DataFrame:
    allowed = [
        "order_id", "symbol", "trade_type",
        "entry_price", "exit_price",
        "stop_loss", "take_profit",
        "lot_size",
        "open_time", "close_time",
        "pnl", "commission", "swap"
    ]
    existing = [col for col in allowed if col in df.columns]
    return df[existing].copy()


# Adds any missing columns that are required by the database schema.
def add_missing_columns(df: pd.DataFrame) -> pd.DataFrame:
    required = [
        "order_id", "symbol", "trade_type",
        "entry_price", "exit_price",
        "stop_loss", "take_profit",
        "lot_size",
        "open_time", "close_time",
        "pnl", "commission", "swap"
    ]

    for col in required:
        if col not in df.columns:
            df[col] = 0 if col in ["pnl", "commission", "swap"] else None

    return df[required]


# Cleaning and standardizing basic values in the DataFrame.
def clean_basic_values(df: pd.DataFrame) -> pd.DataFrame:
    text_cols = ["order_id", "symbol", "trade_type", "open_time", "close_time"]

    for col in text_cols:
        df[col] = df[col].fillna("").astype(str).str.strip()

    df["symbol"] = df["symbol"].str.replace("+", "", regex=False).str.upper()
    df["trade_type"] = df["trade_type"].str.lower()

    numeric_cols = [
        "entry_price", "exit_price",
        "stop_loss", "take_profit",
        "lot_size", "pnl", "commission", "swap"
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df

# Filter out rows that are missing critical information or invalid trade types.
def filter_valid_trade_rows(df: pd.DataFrame) -> pd.DataFrame:
    valid_types = {
        "buy", "sell",
        "buy limit", "sell limit",
        "buy stop", "sell stop",
        "buy stop limit", "sell stop limit",
    }

    df = df[df["symbol"].notna() & (df["symbol"] != "")]
    df = df[df["trade_type"].isin(valid_types)]
    df = df[df["entry_price"].notna()]
    df = df[df["open_time"].notna() & (df["open_time"] != "")]

    return df.reset_index(drop=True)


# Remove duplicates within the current DataFrame before comparing against the database.
def remove_internal_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)

    df["close_time"] = pd.to_datetime(df["close_time"], errors="coerce")
    df = df.sort_values(by=["order_id", "close_time"])
    df = df.drop_duplicates(subset=["order_id"], keep="last")

    after = len(df)

    print(f"Removed duplicates (kept final trades): {before - after}")

    return df.reset_index(drop=True)


# Convert datetime strings to datetime objects for SQLite.
def convert_datetime_for_sqlite(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["open_time", "close_time"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

            df[col] = df[col].astype(str)
            df.loc[df[col] == "NaT", col] = None

    return df


# Check the database for existing entries to avoid importing duplicates.
def filter_existing_database_duplicates(df: pd.DataFrame, conn) -> pd.DataFrame:
    if df.empty:
        return df

    existing = pd.read_sql_query("SELECT order_id FROM trades", conn)

    if existing.empty:
        return df

    df = df[~df["order_id"].isin(existing["order_id"])]

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

    print("\nDETECTED RAW COLUMNS:")
    print(df.columns.tolist())

    df = standardize_columns(df)
    df = ensure_pnl_column(df)
    df = keep_supported_columns(df)
    df = add_missing_columns(df)
    df = clean_basic_values(df)
    df = filter_valid_trade_rows(df)

    df = remove_internal_duplicates(df)

    df = convert_datetime_for_sqlite(df)

    df["source_file"] = file_path.name

    conn = get_connection()

    df = filter_existing_database_duplicates(df, conn)

    if df.empty:
        conn.close()
        print("No new trades to insert.")
        return 0

    records = df.to_dict(orient="records")

    conn.executemany(
        """
        INSERT INTO trades (
            order_id, symbol, trade_type, entry_price, exit_price,
            stop_loss, take_profit, lot_size,
            open_time, close_time,
            pnl, commission, swap, source_file
        )
        VALUES (
            :order_id, :symbol, :trade_type, :entry_price, :exit_price,
            :stop_loss, :take_profit, :lot_size,
            :open_time, :close_time,
            :pnl, :commission, :swap, :source_file
        )
        """,
        records,
    )

    conn.commit()
    conn.close()

    print(f"Final rows inserted: {len(records)}")

    return len(records)