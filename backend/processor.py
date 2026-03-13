import pandas as pd
from database.db import get_connection

# Processor module to calculate additional fields for trades.
def calculate_rr(entry, stop_loss, take_profit, trade_type):
    """
    Calculate risk-to-reward ratio.
    """
    try:
        if pd.isna(entry) or pd.isna(stop_loss) or pd.isna(take_profit):
            return None

        if trade_type == "buy":
            risk = entry - stop_loss
            reward = take_profit - entry
        elif trade_type == "sell":
            risk = stop_loss - entry
            reward = entry - take_profit
        else:
            return None

        if risk <= 0:
            return None

        return round(reward / risk, 2)
    except Exception:
        return None

# Calculates trade duration in minutes by converting open and close times to datetime and finding the difference.
def calculate_duration(open_time, close_time):
    """
    Calculate trade duration in minutes.
    """
    try:
        open_dt = pd.to_datetime(open_time)
        close_dt = pd.to_datetime(close_time)
        duration = (close_dt - open_dt).total_seconds() / 60
        return round(duration, 2)
    except Exception:
        return None

# Labels the trade result as "win", "loss", "breakeven", or "unknown" based on the profit/loss value.
def get_trade_result(pnl):
    """
    Label trade result from profit/loss value.
    """
    if pd.isna(pnl):
        return "unknown"
    if pnl > 0:
        return "win"
    if pnl < 0:
        return "loss"
    return "breakeven"

#Reads database trades.
def process_trades():
    """
    Read trades from the database, analyse extra fields, and return processed data.
    """
    conn = get_connection()

    df = pd.read_sql_query("SELECT * FROM trades", conn)

    if df.empty:
        conn.close()
        return df

    df["trade_result"] = df["pnl"].apply(get_trade_result)

    df["duration_minutes"] = df.apply(
        lambda row: calculate_duration(row["open_time"], row["close_time"]),
        axis=1
    )

    df["rr_ratio"] = df.apply(
        lambda row: calculate_rr(
            row["entry_price"],
            row["stop_loss"],
            row["take_profit"],
            row["trade_type"]
        ),
        axis=1
    )

    conn.close()
    return df