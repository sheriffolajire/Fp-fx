import pandas as pd


VALID_TRADE_RESULTS = {"win", "loss", "breakeven", "unknown"}
VALID_TRADE_TYPES = {
    "buy",
    "sell",
    "buy limit",
    "sell limit",
    "buy stop",
    "sell stop",
    "buy stop limit",
    "sell stop limit",
    "close by",
}
INVALID_SYMBOL_VALUES = {"", "none", "nan", "null", "symbol"}

# Check whether the dataframe contains usable realized profit/loss values.
def has_real_pnl_data(df: pd.DataFrame) -> bool:
    """
    Check whether the dataframe contains usable realized pnl values.
    """
    return "pnl" in df.columns and not df["pnl"].dropna().empty

# Calculate maximum drawdown using cumulative profit/loss. Returns None if pnl data is unavailable.
def calculate_max_drawdown(df: pd.DataFrame):
    """
    Calculate maximum drawdown using cumulative profit/loss.
    Returns None if pnl data is unavailable.
    """
    if not has_real_pnl_data(df):
        return None

    equity_curve = df["pnl"].fillna(0).cumsum()
    running_max = equity_curve.cummax()
    drawdown = equity_curve - running_max

    return round(abs(drawdown.min()), 2)

# Calculate the breakdown of trades by symbol, removing invalid placeholder values and normalizing the text.
def calculate_symbol_breakdown(df: pd.DataFrame) -> dict:
    """
    Count number of trades per symbol after removing invalid placeholder values.
    """
    if df.empty or "symbol" not in df.columns:
        return {}

    cleaned = (
        df["symbol"]
        .dropna()
        .astype(str)
        .str.strip()
        .str.upper()
    )

    cleaned = cleaned[~cleaned.str.lower().isin(INVALID_SYMBOL_VALUES)]

    return cleaned.value_counts().to_dict()

# Calculate the breakdown of trades by trade type.
def calculate_trade_type_breakdown(df: pd.DataFrame) -> dict:
    """
    Count number of trades per trade type after keeping only valid trading actions.
    """
    if df.empty or "trade_type" not in df.columns:
        return {}

    cleaned = (
        df["trade_type"]
        .dropna()
        .astype(str)
        .str.strip()
        .str.lower()
    )

    cleaned = cleaned[cleaned.isin(VALID_TRADE_TYPES)]

    return cleaned.value_counts().to_dict()

# Return only the top N entries from a breakdown dictionary.
def get_top_items(data: dict, limit: int = 5) -> dict:
    """
    Return only the top N items from a dictionary.
    """
    if not data:
        return {}

    items = list(data.items())[:limit]
    return dict(items)

# Calculate performance metrics from processed trades.
def calculate_performance_metrics(df: pd.DataFrame) -> dict:
    """
    Calculate trading performance metrics from processed trades.
    Handles incomplete datasets safely by separating structural metrics
    from profit-based metrics.
    """
    if df.empty:
        return {
            "summary": {
                "total_trades": 0,
                "winning_trades": 0,
                "losing_trades": 0,
                "breakeven_trades": 0,
                "unknown_trades": 0,
                "win_rate": None,
            },
            "trade_characteristics": {
                "average_duration_minutes": 0.0,
                "average_lot_size": 0.0,
            },
            "profitability": {
                "pnl_metrics_available": False,
                "average_win": None,
                "average_loss": None,
                "profit_factor": None,
                "expectancy": None,
                "best_trade": None,
                "worst_trade": None,
                "max_drawdown": None,
            },
            "market_breakdown": {
                "top_symbols": {},
                "top_trade_types": {},
            },
        }

    total_trades = len(df)

    if "trade_result" in df.columns:
        cleaned_trade_result = (
            df["trade_result"]
            .fillna("unknown")
            .astype(str)
            .str.strip()
            .str.lower()
        )
        cleaned_trade_result = cleaned_trade_result.where(
            cleaned_trade_result.isin(VALID_TRADE_RESULTS),
            "unknown",
        )
    else:
        cleaned_trade_result = pd.Series(["unknown"] * total_trades, index=df.index)

    winning_trades = int((cleaned_trade_result == "win").sum())
    losing_trades = int((cleaned_trade_result == "loss").sum())
    breakeven_trades = int((cleaned_trade_result == "breakeven").sum())
    unknown_trades = int((cleaned_trade_result == "unknown").sum())

    known_result_trades = winning_trades + losing_trades + breakeven_trades
    win_rate = round((winning_trades / known_result_trades) * 100, 2) if known_result_trades > 0 else None

    average_duration = (
        round(df["duration_minutes"].dropna().mean(), 2)
        if "duration_minutes" in df.columns and not df["duration_minutes"].dropna().empty
        else 0.0
    )

    average_lot_size = (
        round(df["lot_size"].dropna().mean(), 4)
        if "lot_size" in df.columns and not df["lot_size"].dropna().empty
        else 0.0
    )

    pnl_available = has_real_pnl_data(df)

    if pnl_available:
        pnl_series = df["pnl"].dropna()
        wins = pnl_series[pnl_series > 0]
        losses = pnl_series[pnl_series < 0]

        average_win = round(wins.mean(), 2) if not wins.empty else None
        average_loss = round(losses.mean(), 2) if not losses.empty else None

        gross_profit = wins.sum() if not wins.empty else 0.0
        gross_loss = abs(losses.sum()) if not losses.empty else 0.0

        profit_factor = round(gross_profit / gross_loss, 2) if gross_loss > 0 else None
        expectancy = round(pnl_series.mean(), 2) if not pnl_series.empty else None
        best_trade = round(pnl_series.max(), 2) if not pnl_series.empty else None
        worst_trade = round(pnl_series.min(), 2) if not pnl_series.empty else None
        max_drawdown = calculate_max_drawdown(df)
    else:
        average_win = None
        average_loss = None
        profit_factor = None
        expectancy = None
        best_trade = None
        worst_trade = None
        max_drawdown = None

    symbol_breakdown = calculate_symbol_breakdown(df)
    trade_type_breakdown = calculate_trade_type_breakdown(df)

    return {
        "summary": {
            "total_trades": total_trades,
            "winning_trades": winning_trades,
            "losing_trades": losing_trades,
            "breakeven_trades": breakeven_trades,
            "unknown_trades": unknown_trades,
            "win_rate": win_rate,
        },
        "trade_characteristics": {
            "average_duration_minutes": average_duration,
            "average_lot_size": average_lot_size,
        },
        "profitability": {
            "pnl_metrics_available": pnl_available,
            "average_win": average_win,
            "average_loss": average_loss,
            "profit_factor": profit_factor,
            "expectancy": expectancy,
            "best_trade": best_trade,
            "worst_trade": worst_trade,
            "max_drawdown": max_drawdown,
        },
        "market_breakdown": {
            "top_symbols": get_top_items(symbol_breakdown, limit=10),
            "top_trade_types": get_top_items(trade_type_breakdown, limit=10),
        },
    }