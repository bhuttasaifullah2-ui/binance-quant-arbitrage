import sqlite3
from pathlib import Path
from decimal import Decimal
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS market_observations (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 symbol TEXT NOT NULL,
 observed_at_ms INTEGER NOT NULL,
 spot_bid TEXT, spot_ask TEXT, spot_last TEXT, spot_quote_volume TEXT,
 futures_bid TEXT, futures_ask TEXT, futures_mark TEXT, futures_index TEXT,
 futures_quote_volume TEXT, funding_rate TEXT, next_funding_time_ms INTEGER,
 open_interest TEXT, spot_depth_usdt TEXT, futures_depth_usdt TEXT,
 spot_slippage_bps TEXT, futures_slippage_bps TEXT, basis_bps TEXT,
 source TEXT NOT NULL DEFAULT 'binance_rest',
 UNIQUE(symbol, observed_at_ms)
);
CREATE INDEX IF NOT EXISTS idx_market_symbol_time ON market_observations(symbol, observed_at_ms DESC);
CREATE INDEX IF NOT EXISTS idx_market_time ON market_observations(observed_at_ms DESC);
"""

class SQLiteMarketStore:
    def __init__(self, database_path: str, retention_days: int = 90):
        self.path = Path(database_path)
        self.retention_days = retention_days
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self):
        conn = sqlite3.connect(self.path, timeout=15)
        conn.row_factory = sqlite3.Row
        return conn

    def _initialize(self):
        with self._connect() as conn:
            conn.executescript(SCHEMA)

    def save_observation(self, row: dict[str, Any]) -> bool:
        keys = ["symbol","observed_at_ms","spot_bid","spot_ask","spot_last","spot_quote_volume",
                "futures_bid","futures_ask","futures_mark","futures_index","futures_quote_volume",
                "funding_rate","next_funding_time_ms","open_interest","spot_depth_usdt",
                "futures_depth_usdt","spot_slippage_bps","futures_slippage_bps","basis_bps","source"]
        values = [str(row.get(k)) if row.get(k) is not None and k not in {"observed_at_ms","next_funding_time_ms"} else row.get(k) for k in keys]
        placeholders = ",".join("?" for _ in keys)
        with self._connect() as conn:
            cur = conn.execute(f"INSERT OR IGNORE INTO market_observations ({','.join(keys)}) VALUES ({placeholders})", values)
            return cur.rowcount == 1

    def history(self, symbol: str, limit: int = 100) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM market_observations WHERE symbol=? ORDER BY observed_at_ms DESC LIMIT ?",
                                (symbol.upper(), limit)).fetchall()
        return [dict(row) for row in rows]

    def latest(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("""SELECT m.* FROM market_observations m
                JOIN (SELECT symbol, MAX(observed_at_ms) ts FROM market_observations GROUP BY symbol) x
                ON m.symbol=x.symbol AND m.observed_at_ms=x.ts ORDER BY m.symbol LIMIT ?""", (limit,)).fetchall()
        return [dict(row) for row in rows]

    def prune(self, now_ms: int) -> int:
        cutoff = now_ms - self.retention_days * 86400000
        with self._connect() as conn:
            cur = conn.execute("DELETE FROM market_observations WHERE observed_at_ms < ?", (cutoff,))
            return cur.rowcount

    def count(self) -> int:
        with self._connect() as conn:
            return int(conn.execute("SELECT COUNT(*) FROM market_observations").fetchone()[0])
