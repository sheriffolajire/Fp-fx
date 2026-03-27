from pathlib import Path
import pandas as pd
from database.db import get_connection

    
# Import OHLC market data.
def import_market_data(file_path: str, symbol: str, timeframe: str) -> int:
    """
    Import OHLC market data from MT5 export (ACTUAL FORMAT FIXED)

    Format detected:
    DATE   TIME,OPEN,HIGH,LOW,CLOSE,VOLUME,SPREAD
    """

    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    df = pd.read_csv(
        file_path,
        encoding="utf-16",
        sep=r"\s+",
        engine="python",
        header=None
    )

    if df.empty:
        print("No data found in file.")
        return 0

    print("\nRAW DATA PREVIEW")
    print(df.head())

    if df.shape[1] != 2:
        raise ValueError(f"Unexpected format: expected 2 columns, got {df.shape[1]}")

    split_cols = df[1].str.split(",", expand=True)

    if split_cols.shape[1] != 7:
        raise ValueError(f"CSV split failed: expected 7 values, got {split_cols.shape[1]}")

    df_final = pd.DataFrame({
        "datetime": df[0].astype(str) + " " + split_cols[0],
        "open": split_cols[1],
        "high": split_cols[2],
        "low": split_cols[3],
        "close": split_cols[4],
        "volume": split_cols[5],
        "spread": split_cols[6],
    })

    df_final["datetime"] = pd.to_datetime(df_final["datetime"], errors="coerce")
    df_final["datetime"] = df_final["datetime"].astype(str)

    for col in ["open", "high", "low", "close", "volume", "spread"]:
        df_final[col] = pd.to_numeric(df_final[col], errors="coerce")

    before = len(df_final)

    df_final = df_final.dropna(subset=["datetime", "open", "high", "low", "close"])

    removed = before - len(df_final)

    if df_final.empty:
        print("No valid rows after cleaning.")
        return 0

    df_final["symbol"] = symbol.replace("+", "").upper()
    df_final["timeframe"] = timeframe.upper()

    print("\nCLEANED OHLC PREVIEW")
    print(df_final.head())

    conn = get_connection()
    cursor = conn.cursor()

    inserted = 0

    for record in df_final.to_dict(orient="records"):
        cursor.execute(
            """
            INSERT OR IGNORE INTO ohlc (
                symbol, timeframe, datetime,
                open, high, low, close,
                volume, spread
            )
            VALUES (
                :symbol, :timeframe, :datetime,
                :open, :high, :low, :close,
                :volume, :spread
            )
            """,
            record,
        )

        if cursor.rowcount > 0:
            inserted += 1

    conn.commit()
    conn.close()

    print("\nOHLC IMPORT SUMMARY")
    print(f"Inserted: {inserted}")
    print(f"Total rows: {len(df_final)}")
    print(f"Removed invalid rows: {removed}")

    return inserted