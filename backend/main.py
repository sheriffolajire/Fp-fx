from pathlib import Path

from database.db import init_db
from importer import import_csv
from market_data_importer import import_market_data
from processor import process_trades
from analysis import calculate_performance_metrics
from trade_context_analysis import analyze_trade_context
from strategy_analytics import generate_strategy_report


#printing utilities for better console output formatting.
def print_section(title: str):
    print(f"\n{title}")
    print("-" * len(title))

#Print key-value pairs for metric sections.
def print_key_value_section(section_data: dict):
    for key, value in section_data.items():
        if value is None:
            print(f"  {key}: Not available")
        else:
            print(f"  {key}: {value}")

#Print ranked items (like top symbols or trade types).
def print_ranked_dict(title: str, data: dict):
    print(f"\n  {title}:")
    if not data:
        print("    Not available")
        return

    for index, (key, value) in enumerate(data.items(), start=1):
        print(f"    {index}. {key}: {value}")

#Print nested performance metrics for strategy analysis (like structure performance or trend performance).
def print_nested_metrics(title: str, data: dict):
    """
    Pretty print nested performance metrics (NEW)
    """
    print(f"\n  {title}:")
    if not data:
        print("    Not available")
        return

    for key, metrics in data.items():
        print(f"\n    {key}:")
        for m_key, m_val in metrics.items():
            print(f"      {m_key}: {m_val}")


# Main entry point for the application. Initializes the database and imports sample data.
def main():

    init_db()

    # Import sample CSV
    sample_file = Path("backend/data/samples/ReportHistory-deal1.csv")
    inserted = import_csv(sample_file)

   
    IMPORT_OHLC = True 

    if IMPORT_OHLC:
        try:
            ohlc_file = Path("backend/data/samples/XAUUSD+H1.csv")

            if ohlc_file.exists():
                ohlc_rows = import_market_data(ohlc_file, "XAUUSD+", "H1")
                print(f"Imported OHLC rows: {ohlc_rows}")
            else:
                print("OHLC file not found. Skipping OHLC import.")

        except Exception as e:
            print(f"OHLC import failed: {e}")

    # Process trades
    processed_df = process_trades()

    # Apply market context
    try:
        processed_df = analyze_trade_context(processed_df)
    except Exception as e:
        print(f"Context analysis failed: {e}")

     # Analyze performance
    metrics = calculate_performance_metrics(processed_df)
    strategy_metrics = generate_strategy_report(processed_df)

 

    print("\nDatabase initialized successfully.")
    print(f"Imported rows: {inserted}")

  #  print processed trades preview.

    print_section("Processed Trades Preview")

    base_columns = [
        "symbol",
        "trade_type",
        "pnl",
        "trade_result",
        "duration_minutes",
        "rr_ratio",
    ]

    optional_columns = [
        "trend_alignment",
        "structure_signal",
        "trade_quality",
    ]

    columns_to_show = [col for col in base_columns if col in processed_df.columns]

    for col in optional_columns:
        if col in processed_df.columns:
            columns_to_show.append(col)

    print(processed_df[columns_to_show].head())

   
# Print performance metrics
    print_section("Performance Report")

    summary = metrics.get("summary", {})
    trade_characteristics = metrics.get("trade_characteristics", {})
    profitability = metrics.get("profitability", {})
    market_breakdown = metrics.get("market_breakdown", {})

    print_section("Summary")
    print_key_value_section(summary)

    print_section("Trade Characteristics")
    print_key_value_section(trade_characteristics)

    print_section("Profitability")
    print_key_value_section(profitability)

    print_section("Market Breakdown")
    print_ranked_dict("Top Symbols", market_breakdown.get("top_symbols", {}))
    print_ranked_dict("Top Trade Types", market_breakdown.get("top_trade_types", {}))

 
# Print strategy analytics
    print_section("Strategy Analytics")

    # Basic stats
    print_ranked_dict("Structure Usage", strategy_metrics.get("structure_usage", {}))
    print_ranked_dict("Trend Alignment", strategy_metrics.get("trend_alignment", {}))
    print_ranked_dict("Trade Quality", strategy_metrics.get("trade_quality", {}))

    # Cross analysis
    print_nested_metrics("Structure Performance", strategy_metrics.get("structure_performance", {}))
    print_nested_metrics("Trend Performance", strategy_metrics.get("trend_performance", {}))
    print_nested_metrics("Quality Performance", strategy_metrics.get("quality_performance", {}))


# Run application
if __name__ == "__main__":
    main()