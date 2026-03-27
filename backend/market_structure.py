import pandas as pd


#Trend Detection using simple price action logic.
def detect_trend(df: pd.DataFrame) -> pd.Series:
    """
    Detect market trend using simple price action logic.
    """

    df = df.copy()

    df["prev_high"] = df["high"].shift(1)
    df["prev_low"] = df["low"].shift(1)

    trend = []

    for i in range(len(df)):
        if i == 0:
            trend.append("neutral")
            continue

        if df.loc[i, "high"] > df.loc[i, "prev_high"] and df.loc[i, "low"] > df.loc[i, "prev_low"]:
            trend.append("bullish")

        elif df.loc[i, "high"] < df.loc[i, "prev_high"] and df.loc[i, "low"] < df.loc[i, "prev_low"]:
            trend.append("bearish")

        else:
            trend.append("neutral")

    return pd.Series(trend)


#break of structure (BOS) detection based on price action.
def detect_bos(df: pd.DataFrame) -> pd.Series:
    df = df.copy()

    df["prev_high"] = df["high"].shift(1)
    df["prev_low"] = df["low"].shift(1)

    bos = []

    for i in range(len(df)):
        if i == 0:
            bos.append(None)
            continue

        if df.loc[i, "high"] > df.loc[i, "prev_high"]:
            bos.append("bullish_bos")

        elif df.loc[i, "low"] < df.loc[i, "prev_low"]:
            bos.append("bearish_bos")

        else:
            bos.append(None)

    return pd.Series(bos)


# CHoCH detection based on price action and trend context.
def detect_choch(df: pd.DataFrame) -> pd.Series:
    df = df.copy()

    if "trend" not in df.columns:
        raise ValueError("Trend must be calculated before CHoCH")

    df["prev_high"] = df["high"].shift(1)
    df["prev_low"] = df["low"].shift(1)

    choch = []

    for i in range(len(df)):
        if i == 0:
            choch.append(None)
            continue

        trend = df.loc[i, "trend"]

        if trend == "bullish" and df.loc[i, "low"] < df.loc[i, "prev_low"]:
            choch.append("bearish_choch")

        elif trend == "bearish" and df.loc[i, "high"] > df.loc[i, "prev_high"]:
            choch.append("bullish_choch")

        else:
            choch.append(None)

    return pd.Series(choch)


# Fair Value Gap (FVG) detection based on price action.
def detect_fvg(df: pd.DataFrame) -> pd.Series:
    fvg = [None, None]

    for i in range(2, len(df)):
        c1 = df.iloc[i - 2]
        c3 = df.iloc[i]

        if c1["high"] < c3["low"]:
            fvg.append("bullish_fvg")

        elif c1["low"] > c3["high"]:
            fvg.append("bearish_fvg")

        else:
            fvg.append(None)

    return pd.Series(fvg)


# Main function to apply market structure logic.
def apply_market_structure(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply full market structure logic.
    """

    df = df.copy()
    df["trend"] = detect_trend(df)

    df["bos"] = detect_bos(df)
    df["choch"] = detect_choch(df)

    df["fvg"] = detect_fvg(df)

    df["structure_signal"] = None

    df.loc[df["choch"].notna(), "structure_signal"] = df["choch"]

    df.loc[
        df["structure_signal"].isna() & df["bos"].notna(),
        "structure_signal"
    ] = df["bos"]

    df.loc[
        df["structure_signal"].isna() & df["fvg"].notna(),
        "structure_signal"
    ] = df["fvg"]

    return df