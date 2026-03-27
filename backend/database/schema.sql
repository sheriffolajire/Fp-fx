
-- TRADES TABLE SCHEMA
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
    commission REAL DEFAULT 0,   
    swap REAL DEFAULT 0,       

    source_file TEXT
);

-- Prevent duplicate trades
CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_order_id
ON trades(order_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_source_file_order_id
ON trades(source_file, order_id);



-- OHLC MARKET DATA TABLE

CREATE TABLE IF NOT EXISTS ohlc (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    datetime TEXT NOT NULL,

    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    spread REAL
);

-- Prevent duplicate candles for the same symbol, timeframe, and datetime
CREATE UNIQUE INDEX IF NOT EXISTS idx_ohlc_unique
ON ohlc(symbol, timeframe, datetime);