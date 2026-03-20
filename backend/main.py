from pathlib import Path

from database.db import init_db
from importer import import_csv
from processor import process_trades
from analysis import calculate_performance_metrics

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


# Main entry point for the application. Initializes the database and imports sample data.
def main():
    init_db()

    # Import sample CSV
    sample_file = Path("backend/data/samples/ReportHistory-14186080.csv")
    inserted = import_csv(sample_file)

    # Process trades
    processed_df = process_trades()

    # Analyze performance
    metrics = calculate_performance_metrics(processed_df)

    print("Database initialized successfully.")
    print(f"Imported rows: {inserted}")

    print_section("Processed Trades Preview")
    print(
        processed_df[
            ["symbol", "trade_type", "pnl", "trade_result", "duration_minutes", "rr_ratio"]
        ].head()
    )

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


if __name__ == "__main__":
    main()