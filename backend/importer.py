from pathlib import Path
from io import StringIO

import pandas as pd
from database.db import get_connection


# Mapping of common column name variations to standardized column names used in the database.
COLUMN_MAP = {
    "order": "order_id",
    "ticket": "order_id",
    "deal": "order_id",
    "position": "order_id", 

    "symbol": "symbol",

    "type": "trade_type",

    "price": "entry_price",
    "price 1": "exit_price", 

    "sl": "stop_loss",
    "tp": "take_profit",

    "volume": "lot_size",

    "time": "open_time",
    "time 1": "close_time",

    "open time": "open_time",
    "close time": "close_time",

    "profit": "pnl",

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
        col_clean = col_clean.replace(".", " ") 
        col_clean = " ".join(col_clean.split())

        cleaned_cols.append(col_clean)

    df.columns = cleaned_cols
    df = df.rename(columns={col: COLUMN_MAP[col] for col in df.columns if col in COLUMN_MAP})

    return df


# Ensures that the "pnl" column exists and is calculated correctly by summing profit, commission, and swap.
def ensure_pnl_column(df: pd.DataFrame) -> pd.DataFrame:
    if "pnl" not in df.columns:
        df["pnl"] = 0

    df["pnl"] = pd.to_numeric(df["pnl"], errors="coerce").fillna(0)

    for col in ["commission", "swap"]:
        if col not in df.columns:
            df[col] = 0

        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["pnl"] = df["pnl"] + df["commission"] + df["swap"]

    return df


# Cleaning and standardizing basic values in the DataFrame.
def clean_basic_values(df: pd.DataFrame) -> pd.DataFrame:
    df["symbol"] = df["symbol"].astype(str).str.replace("+", "", regex=False).str.upper()
    df["trade_type"] = df["trade_type"].astype(str).str.lower()

    numeric_cols = ["entry_price", "exit_price", "lot_size", "pnl"]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


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
    is_deals = False

    for i, line in enumerate(lines):
        if "Open Time" in line and "Symbol" in line:
            start_idx = i
            print("✅ Detected ORDERS table")
            break

        if "Time" in line and "Symbol" in line and "Profit" in line:
            start_idx = i
            is_deals = True
            print("✅ Detected DEALS table")
            break

    if start_idx is None:
        raise ValueError("Trade table not found in file")

    data = "".join(lines[start_idx:])
    df = pd.read_csv(StringIO(data))

    print("\nDETECTED RAW COLUMNS:")
    print(df.columns.tolist())
    df = standardize_columns(df)
    df = ensure_pnl_column(df)
    df = clean_basic_values(df)

    if is_deals:
        df["open_time"] = pd.to_datetime(df["open_time"], errors="coerce")
        df["close_time"] = pd.to_datetime(df["close_time"], errors="coerce")

        df = df.sort_values(by=["order_id", "open_time"])

        grouped = df.groupby("order_id")

        df = pd.DataFrame({
            "order_id": grouped["order_id"].first(),
            "symbol": grouped["symbol"].first(),
            "trade_type": grouped["trade_type"].first(),
            "entry_price": grouped["entry_price"].first(),
            "exit_price": grouped["exit_price"].last(),

            "stop_loss": None,
            "take_profit": None,

            "open_time": grouped["open_time"].first(),
            "close_time": grouped["close_time"].last(),
            "lot_size": grouped["lot_size"].sum(),
            "pnl": grouped["pnl"].sum(),
            "commission": grouped["commission"].sum(),
            "swap": grouped["swap"].sum(),
        }).reset_index(drop=True)

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