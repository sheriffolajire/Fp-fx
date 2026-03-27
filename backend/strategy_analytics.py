import pandas as pd


#safe value counts.
def safe_value_counts(series):
    if series is None or series.empty:
        return {}
    return series.value_counts().to_dict()


#group profit metrics by category.
def group_profit_metrics(df: pd.DataFrame, column: str) -> dict:
    """
    Calculate performance per category:
    - number of trades
    - win rate
    - total pnl
    - average pnl
    """

    if column not in df.columns:
        return {}

    result = {}

    grouped = df.dropna(subset=[column]).groupby(column)

    for key, group in grouped:
        total = len(group)

        wins = (group["pnl"] > 0).sum()
        losses = (group["pnl"] < 0).sum()

        pnl_sum = group["pnl"].sum()
        avg_pnl = group["pnl"].mean()

        win_rate = (wins / total) * 100 if total > 0 else 0

        result[key] = {
            "trades": int(total),
            "win_rate": round(win_rate, 2),
            "total_pnl": round(pnl_sum, 2),
            "avg_pnl": round(avg_pnl, 2),
        }

    return result


#structure signal usage stats.
def count_structure_usage(df: pd.DataFrame) -> dict:
    if df.empty or "structure_signal" not in df.columns:
        return {}
    return safe_value_counts(df["structure_signal"].dropna())


def trend_alignment_stats(df: pd.DataFrame) -> dict:
    if df.empty or "trend_alignment" not in df.columns:
        return {}
    return safe_value_counts(df["trend_alignment"])


def trade_quality_distribution(df: pd.DataFrame) -> dict:
    if df.empty or "trade_quality" not in df.columns:
        return {}
    return safe_value_counts(df["trade_quality"])


#structure vs qualitycross analysis.
def structure_vs_quality(df: pd.DataFrame) -> dict:
    if df.empty or "structure_signal" not in df.columns:
        return {}

    grouped = (
        df.dropna(subset=["structure_signal"])
        .groupby(["structure_signal", "trade_quality"])
        .size()
        .unstack(fill_value=0)
    )

    return grouped.to_dict()


#full analytics report.
def generate_strategy_report(df: pd.DataFrame) -> dict:
    """
    Full analytics report:
    - Usage stats
    - Cross analysis
    - Profit-based edge detection
    """

    if df.empty:
        return {}

    return {
        "structure_usage": count_structure_usage(df),
        "trend_alignment": trend_alignment_stats(df),
        "trade_quality": trade_quality_distribution(df),

        "structure_vs_quality": structure_vs_quality(df),
        "structure_performance": group_profit_metrics(df, "structure_signal"),
        "trend_performance": group_profit_metrics(df, "trend_alignment"),
        "quality_performance": group_profit_metrics(df, "trade_quality"),
    }