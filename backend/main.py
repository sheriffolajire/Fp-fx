from pathlib import Path

from database.db import init_db
from importer import import_csv
from processor import process_trades


# Main entry point for the application. Initializes the database and imports sample data.
def main():
    init_db()

    # import sample CSV
    sample_file = Path("backend/data/samples/sample_trades.csv")
    inserted = import_csv(sample_file)

    processed_df = process_trades()

    print("Database initialized successfully.")
    print(f"Imported rows: {inserted}")

    print(
        processed_df[
            ["symbol", "trade_type", "pnl", "trade_result", "duration_minutes", "rr_ratio"]
        ].head()
    )


if __name__ == "__main__":
    main()