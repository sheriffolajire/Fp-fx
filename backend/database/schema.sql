CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    trade_type TEXT NOT NULL,
    entry_price REAL,
    exit_price REAL,
    stop_loss REAL,
    take_profit REAL,
    lot_size REAL,
    open_time TEXT,
    close_time TEXT,
    pnl REAL,
    source_file TEXT
);