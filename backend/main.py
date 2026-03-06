from pathlib import Path

from database.db import init_db
from importer import import_csv

# Main entry point for the application. Initializes the database and imports sample data.
def main():
    init_db()

    # import sample CSV
    sample_file = Path("backend/data/samples/sample_trades.csv")
    inserted = import_csv(sample_file)

    print("Database initialized successfully.")
    print(f"Imported rows: {inserted}")


if __name__ == "__main__":
    main()