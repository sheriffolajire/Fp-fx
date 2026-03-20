CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT,
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

CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_order_id
ON trades(order_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_source_file_order_id
ON trades(source_file, order_id);