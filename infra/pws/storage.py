"""
PWS Storage Module
SQLite persistence for market contracts, orderbook history, and trade events.

Database: infra/pws/data/pws_data.db (WAL mode, separate from main project DB)

Tables:
    contract_registry    — Discovered contracts (token_id ↔ bucket mapping)
    orderbook_snapshots  — Historical orderbook snapshots (sampled)
    trade_events         — Observed trades from WebSocket

Timestamps: All stored as ISO 8601 UTC strings for consistency.
"""
import os
import sqlite3
import logging
import json
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict
from contextlib import contextmanager

from .models import PWSMarket, PWSOrderbook, PWSOrderbookLevel, PWSTrade

log = logging.getLogger("pws.storage")

# Default DB path
DEFAULT_DB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
DEFAULT_DB_PATH = os.path.join(DEFAULT_DB_DIR, "pws_data.db")


class PWSStorage:
    """
    SQLite persistence for PWS module.
    
    Handles:
    - Contract registry (which token_id maps to which temperature bucket)
    - Orderbook snapshots (sampled at configurable interval)
    - Trade events (all observed trades)
    
    Sampling: To avoid excessive disk writes, orderbook snapshots are 
    rate-limited per token_id. Default: 1 snapshot per 5 seconds per token.
    """
    
    def __init__(self, db_path: str = None, snapshot_interval_sec: int = 5):
        self.db_path = db_path or DEFAULT_DB_PATH
        self.snapshot_interval = snapshot_interval_sec
        
        # Sampling state: token_id → last snapshot timestamp
        self._last_snapshot: Dict[str, datetime] = {}
        
        # Ensure data directory exists
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        # Initialize schema
        self._init_schema()
        log.info(f"📁 PWS Database: {self.db_path}")
    
    @contextmanager
    def _connection(self):
        """Thread-safe connection context manager."""
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def _init_schema(self):
        """Create tables if they don't exist."""
        with self._connection() as conn:
            c = conn.cursor()
            
            # Enable WAL mode for concurrent read/write
            c.execute("PRAGMA journal_mode=WAL;")
            
            # ─── Table 1: Contract Registry ───
            c.execute("""
                CREATE TABLE IF NOT EXISTS contract_registry (
                    token_id        TEXT PRIMARY KEY,
                    city            TEXT NOT NULL,
                    market_date     TEXT NOT NULL,
                    event_slug      TEXT NOT NULL,
                    question        TEXT,
                    outcome         TEXT NOT NULL,
                    bucket_label    TEXT,
                    bucket_low      INTEGER,
                    bucket_high     INTEGER,
                    bucket_type     TEXT,
                    bucket_unit     TEXT DEFAULT 'C',
                    discovery_price REAL,
                    discovered_at   TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                    is_active       INTEGER DEFAULT 1
                )
            """)
            
            c.execute("""
                CREATE INDEX IF NOT EXISTS idx_registry_city_date 
                ON contract_registry(city, market_date)
            """)
            
            c.execute("""
                CREATE INDEX IF NOT EXISTS idx_registry_active 
                ON contract_registry(is_active, city)
            """)
            
            # ─── Table 2: Orderbook Snapshots ───
            c.execute("""
                CREATE TABLE IF NOT EXISTS orderbook_snapshots (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp   TEXT NOT NULL,
                    token_id    TEXT NOT NULL,
                    best_bid    REAL,
                    best_ask    REAL,
                    mid_price   REAL,
                    spread      REAL,
                    bid_depth   REAL,
                    ask_depth   REAL,
                    event_type  TEXT NOT NULL,
                    raw_levels  TEXT
                )
            """)
            
            c.execute("""
                CREATE INDEX IF NOT EXISTS idx_ob_token_time 
                ON orderbook_snapshots(token_id, timestamp)
            """)
            
            # ─── Table 3: Trade Events ───
            c.execute("""
                CREATE TABLE IF NOT EXISTS trade_events (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp   TEXT NOT NULL,
                    token_id    TEXT NOT NULL,
                    price       REAL NOT NULL,
                    size        REAL,
                    side        TEXT,
                    fee_rate    TEXT
                )
            """)
            
            c.execute("""
                CREATE INDEX IF NOT EXISTS idx_trades_token_time 
                ON trade_events(token_id, timestamp)
            """)
    
    # ══════════════════════════════════════════
    # Contract Registry
    # ══════════════════════════════════════════
    
    def save_contracts(self, markets: Dict[str, PWSMarket]):
        """
        Save discovered contracts to registry. Uses INSERT OR REPLACE (upsert).
        Call this after discover_city() to persist the token_id ↔ bucket mapping.
        """
        if not markets:
            return
        
        with self._connection() as conn:
            c = conn.cursor()
            for token_id, market in markets.items():
                c.execute("""
                    INSERT OR REPLACE INTO contract_registry 
                    (token_id, city, market_date, event_slug, question, outcome,
                     bucket_label, bucket_low, bucket_high, bucket_type, bucket_unit,
                     discovery_price, is_active)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                """, (
                    token_id,
                    market.city or "",
                    market.market_date or "",
                    market.slug or "",
                    market.question,
                    market.outcome,
                    market.bucket.label if market.bucket else None,
                    market.bucket.low if market.bucket else None,
                    market.bucket.high if market.bucket else None,
                    market.bucket.type if market.bucket else None,
                    market.bucket.unit if market.bucket else "C",
                    market.price,
                ))
        
        log.info(f"💾 Saved {len(markets)} contracts to registry")
    
    def get_contracts(self, city: str = None, market_date: str = None, 
                      active_only: bool = True) -> Dict[str, PWSMarket]:
        """
        Load contracts from registry, optionally filtered.
        Returns dict sorted by bucket temperature.
        """
        from .models import PWSTemperatureBucket
        from collections import OrderedDict
        
        with self._connection() as conn:
            c = conn.cursor()
            
            query = "SELECT * FROM contract_registry WHERE 1=1"
            params = []
            
            if city:
                query += " AND city = ?"
                params.append(city)
            if market_date:
                query += " AND market_date = ?"
                params.append(market_date)
            if active_only:
                query += " AND is_active = 1"
            
            query += " ORDER BY bucket_low ASC, outcome ASC"
            
            c.execute(query, params)
            rows = c.fetchall()
        
        results = {}
        for row in rows:
            bucket = None
            if row["bucket_label"]:
                bucket = PWSTemperatureBucket(
                    label=row["bucket_label"],
                    low=row["bucket_low"],
                    high=row["bucket_high"],
                    type=row["bucket_type"] or "range",
                    unit=row["bucket_unit"] or "C",
                )
            
            market = PWSMarket(
                token_id=row["token_id"],
                condition_id="",
                question=row["question"] or "",
                slug=row["event_slug"],
                outcome=row["outcome"],
                price=row["discovery_price"] or 0.0,
                bucket=bucket,
                city=row["city"],
                market_date=row["market_date"],
            )
            results[row["token_id"]] = market
        
        # Sort by bucket sort_key
        sorted_results = OrderedDict(
            sorted(results.items(), key=lambda item: item[1].sort_key)
        )
        
        return sorted_results
    
    def deactivate_market(self, token_id: str):
        """Mark a market as resolved/inactive."""
        with self._connection() as conn:
            conn.execute(
                "UPDATE contract_registry SET is_active = 0 WHERE token_id = ?",
                (token_id,)
            )
    
    def deactivate_date(self, city: str, market_date: str):
        """Mark all markets for a city+date as inactive (day ended)."""
        with self._connection() as conn:
            conn.execute(
                "UPDATE contract_registry SET is_active = 0 WHERE city = ? AND market_date = ?",
                (city, market_date)
            )
    
    # ══════════════════════════════════════════
    # Orderbook Snapshots
    # ══════════════════════════════════════════
    
    def save_snapshot(self, book: PWSOrderbook, event_type: str = "book",
                      force: bool = False) -> bool:
        """
        Save an orderbook snapshot, respecting the sampling interval.
        
        Returns True if saved, False if skipped (too soon since last save).
        Set force=True to bypass sampling.
        """
        now = datetime.now(timezone.utc)
        token_id = book.market_id
        
        # Sampling: skip if too recent
        if not force and token_id in self._last_snapshot:
            elapsed = (now - self._last_snapshot[token_id]).total_seconds()
            if elapsed < self.snapshot_interval:
                return False
        
        self._last_snapshot[token_id] = now
        
        # Build raw_levels JSON (top 5 bids + asks)
        raw = None
        if book.bids or book.asks:
            raw = json.dumps({
                "bids": [{"p": l.price, "s": l.size} for l in book.bids[:5]],
                "asks": [{"p": l.price, "s": l.size} for l in book.asks[:5]],
            })
        
        with self._connection() as conn:
            conn.execute("""
                INSERT INTO orderbook_snapshots 
                (timestamp, token_id, best_bid, best_ask, mid_price, spread,
                 bid_depth, ask_depth, event_type, raw_levels)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                now.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                token_id,
                book.best_bid,
                book.best_ask,
                book.mid_price,
                book.spread,
                book.bid_depth,
                book.ask_depth,
                event_type,
                raw,
            ))
        
        return True
    
    def get_price_history(self, token_id: str, hours: int = 24) -> List[Dict]:
        """Get orderbook price history for a token over the last N hours."""
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        
        with self._connection() as conn:
            c = conn.cursor()
            c.execute("""
                SELECT timestamp, best_bid, best_ask, mid_price, spread,
                       bid_depth, ask_depth, event_type
                FROM orderbook_snapshots
                WHERE token_id = ? AND timestamp >= ?
                ORDER BY timestamp ASC
            """, (token_id, cutoff))
            
            return [dict(row) for row in c.fetchall()]
    
    def get_latest_prices(self, city: str, market_date: str) -> List[Dict]:
        """
        Get the latest snapshot for each contract in a city+date market.
        Joins with contract_registry for bucket labels.
        Returns list sorted by bucket temperature.
        
        Each item:
        {
            "bucket_label": "12-13°C",
            "outcome": "YES",
            "best_bid": 0.23,
            "best_ask": 0.27,
            "mid_price": 0.25,
            "spread": 0.04,
            "last_update": "2026-02-17T11:45:02Z",
            "token_id": "abc...",
            "bucket_low": 12,
        }
        """
        with self._connection() as conn:
            c = conn.cursor()
            c.execute("""
                SELECT 
                    cr.token_id,
                    cr.bucket_label,
                    cr.bucket_low,
                    cr.bucket_high,
                    cr.bucket_type,
                    cr.outcome,
                    os.best_bid,
                    os.best_ask,
                    os.mid_price,
                    os.spread,
                    os.timestamp as last_update
                FROM contract_registry cr
                LEFT JOIN (
                    SELECT token_id, best_bid, best_ask, mid_price, spread, timestamp,
                           ROW_NUMBER() OVER (PARTITION BY token_id ORDER BY timestamp DESC) as rn
                    FROM orderbook_snapshots
                ) os ON cr.token_id = os.token_id AND os.rn = 1
                WHERE cr.city = ? AND cr.market_date = ? AND cr.is_active = 1
                ORDER BY 
                    CASE 
                        WHEN cr.bucket_type = 'below' THEN cr.bucket_high
                        WHEN cr.bucket_type = 'higher' THEN cr.bucket_low + 1000
                        ELSE cr.bucket_low
                    END ASC,
                    cr.outcome ASC
            """, (city, market_date))
            
            return [dict(row) for row in c.fetchall()]
    
    # ══════════════════════════════════════════
    # Trade Events
    # ══════════════════════════════════════════
    
    def save_trade(self, trade: PWSTrade):
        """Save a trade event from WebSocket."""
        with self._connection() as conn:
            conn.execute("""
                INSERT INTO trade_events 
                (timestamp, token_id, price, size, side, fee_rate)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                trade.timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                trade.market_id,
                trade.price,
                trade.size,
                trade.side,
                trade.fee_rate,
            ))
    
    def get_recent_trades(self, token_id: str = None, limit: int = 50) -> List[Dict]:
        """Get recent trades, optionally filtered by token."""
        with self._connection() as conn:
            c = conn.cursor()
            
            if token_id:
                c.execute("""
                    SELECT te.*, cr.bucket_label, cr.outcome
                    FROM trade_events te
                    LEFT JOIN contract_registry cr ON te.token_id = cr.token_id
                    WHERE te.token_id = ?
                    ORDER BY te.timestamp DESC LIMIT ?
                """, (token_id, limit))
            else:
                c.execute("""
                    SELECT te.*, cr.bucket_label, cr.outcome
                    FROM trade_events te
                    LEFT JOIN contract_registry cr ON te.token_id = cr.token_id
                    ORDER BY te.timestamp DESC LIMIT ?
                """, (limit,))
            
            return [dict(row) for row in c.fetchall()]
    
    # ══════════════════════════════════════════
    # Maintenance & Stats
    # ══════════════════════════════════════════
    
    def cleanup(self, retention_days: int = 30):
        """Delete data older than retention period."""
        cutoff = (datetime.now(timezone.utc) - timedelta(days=retention_days)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        
        with self._connection() as conn:
            c = conn.cursor()
            
            c.execute("DELETE FROM orderbook_snapshots WHERE timestamp < ?", (cutoff,))
            ob_deleted = c.rowcount
            
            c.execute("DELETE FROM trade_events WHERE timestamp < ?", (cutoff,))
            tr_deleted = c.rowcount
            
            # Deactivate old contracts
            c.execute(
                "UPDATE contract_registry SET is_active = 0 WHERE market_date < ?",
                (cutoff[:10],)  # Just the date part
            )
            
        log.info(f"🧹 Cleanup: deleted {ob_deleted} snapshots, {tr_deleted} trades (>{retention_days}d)")
    
    def get_stats(self) -> Dict:
        """Get database statistics."""
        with self._connection() as conn:
            c = conn.cursor()
            
            contracts = c.execute("SELECT COUNT(*) FROM contract_registry").fetchone()[0]
            active = c.execute("SELECT COUNT(*) FROM contract_registry WHERE is_active = 1").fetchone()[0]
            snapshots = c.execute("SELECT COUNT(*) FROM orderbook_snapshots").fetchone()[0]
            trades = c.execute("SELECT COUNT(*) FROM trade_events").fetchone()[0]
            
            # DB file size
            db_size_mb = os.path.getsize(self.db_path) / (1024 * 1024) if os.path.exists(self.db_path) else 0
            
            return {
                "contracts_total": contracts,
                "contracts_active": active,
                "orderbook_snapshots": snapshots,
                "trade_events": trades,
                "db_size_mb": round(db_size_mb, 2),
                "db_path": self.db_path,
            }
