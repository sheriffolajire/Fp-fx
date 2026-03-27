import pandas as pd
from database.db import get_connection
from market_structure import apply_market_structure


#ohlc data loading function that retrieves historical price data for a given symbol from the database and converts it into a pandas DataFrame.
def load_ohlc_data(symbol: str) -> pd.DataFrame:
    conn = get_connection()

    query = """
    SELECT datetime, open, high, low, close
    FROM ohlc
    WHERE symbol = ?
    ORDER BY datetime ASC
    """

    df = pd.read_sql_query(query, conn, params=(symbol,))
    conn.close()

    if df.empty:
        return df

    df["datetime"] = pd.to_datetime(df["datetime"])
    return df


# Aligns each trade with the corresponding OHLC data based on the trade's open time, allowing us to analyze the market context at the time of the trade.
def align_trades_with_ohlc(trades_df: pd.DataFrame, ohlc_df: pd.DataFrame) -> pd.DataFrame:
    if trades_df.empty or ohlc_df.empty:
        return trades_df

    trades_df = trades_df.copy()

    trades_df["open_time"] = pd.to_datetime(trades_df["open_time"])

    trades_df = trades_df.sort_values("open_time")
    ohlc_df = ohlc_df.sort_values("datetime")

    merged = pd.merge_asof(
        trades_df,
        ohlc_df,
        left_on="open_time",
        right_on="datetime",
        direction="backward"
    )

    return merged


# Evaluates the trade context based on the market structure, trend, and trade type.
def evaluate_trade_context(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["trend_alignment"] = "unknown"
    df["trade_quality"] = "unknown"

    for i in range(len(df)):
        trade_type = str(df.loc[i, "trade_type"]).lower()
        trend = df.loc[i, "trend"]
        structure = df.loc[i, "structure_signal"]

        if trade_type == "buy" and trend == "bullish":
            alignment = "aligned"

        elif trade_type == "sell" and trend == "bearish":
            alignment = "aligned"

        elif trade_type in ["buy", "sell"]:
            alignment = "counter_trend"

        else:
            alignment = "unknown"

        df.loc[i, "trend_alignment"] = alignment

        if alignment == "aligned" and pd.notna(structure):
            df.loc[i, "trade_quality"] = "high"

        elif alignment == "aligned" and pd.isna(structure):
            df.loc[i, "trade_quality"] = "medium"

        elif alignment == "counter_trend" and pd.notna(structure):
            df.loc[i, "trade_quality"] = "low"

        else:
            df.loc[i, "trade_quality"] = "very_low"

    return df


# Main function that performs the trade context analysis process.
def analyze_trade_context(trades_df: pd.DataFrame) -> pd.DataFrame:
    if trades_df.empty:
        return trades_df

    symbol = trades_df["symbol"].dropna().iloc[0]

    ohlc_df = load_ohlc_data(symbol)

    if ohlc_df.empty:
        print("No OHLC data found. Skipping context analysis.")
        return trades_df

    ohlc_df = apply_market_structure(ohlc_df)

    merged_df = align_trades_with_ohlc(trades_df, ohlc_df)

    result_df = evaluate_trade_context(merged_df)

    return result_df