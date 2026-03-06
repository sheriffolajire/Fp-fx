import sqlite3
from pathlib import Path

# Database configuration
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "trades.db"
SCHEMA_PATH = BASE_DIR / "schema.sql"

# Get a connection to the SQLite database, ensuring rows are returned as dictionaries for easier access.
def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# Initialize the database schema
def init_db():
    conn = get_connection()
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        conn.executescript(f.read())
    conn.commit()
    conn.close()