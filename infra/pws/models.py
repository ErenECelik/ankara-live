"""
PWS Models
Independent data structures for the Polymarket Web Service module.
These models are decoupled from the main project's schema.

Includes unified temperature bucket parsing — single source of truth
for all regex patterns previously duplicated across the codebase.
"""
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timezone
import re


# ──────────────────────────────────────────────
# Temperature Bucket Parsing (Tek Kaynak)
# ──────────────────────────────────────────────

@dataclass
class PWSTemperatureBucket:
    """Parsed temperature range from a market question."""
    label: str              # "15-16°C", "≤14°C", "≥20°C"
    low: Optional[int]      # Alt sınır (None = "or below")
    high: Optional[int]     # Üst sınır (None = "or higher")
    type: str               # "range" | "below" | "higher" | "exact"
    unit: str = "C"         # "C" veya "F"

    @property
    def sort_key(self) -> int:
        """
        Sıralama anahtarı — kontratları sıcaklığa göre sıralar.
        below → low range → high range → higher
        """
        if self.type == "below":
            return self.high if self.high is not None else -999
        elif self.type == "higher":
            return self.low + 1000 if self.low is not None else 9999
        elif self.type == "exact":
            return self.low if self.low is not None else 0
        else:  # range
            return self.low if self.low is not None else 0


def parse_temperature_bucket(question: str) -> Optional[PWSTemperatureBucket]:
    """
    Parse a Polymarket market question to extract temperature bucket info.
    
    Supported formats (case-insensitive):
      - "between 15°C and 16°C"    → range(15, 16, "15-16°C")
      - "between 15-16°C"          → range(15, 16, "15-16°C")
      - "15°C or below"            → below(None, 15, "≤15°C")
      - "20°C or higher"           → higher(20, None, "≥20°C")
      - "be 15°C on"               → exact(15, 15, "15°C")
      - Same patterns with °F

    This function consolidates regex previously duplicated in:
      - run_ankara_bot.py (get_bucket_label)
      - brain.py (_parse_market_limits)
      - polymarket/client.py (_extract_bucket_info)
      - paper_trading/engine.py (execute_override_trade)
    
    Returns None if no temperature pattern is found.
    """
    q = question.strip()
    
    # Detect unit
    unit = "F" if "°F" in q or "°f" in q else "C"
    q_lower = q.lower()
    
    # Pattern 1: "X°C or higher" / "X or higher"
    m = re.search(r'(\d+)\s*°?[cf]?\s+or\s+higher', q_lower)
    if m:
        val = int(m.group(1))
        return PWSTemperatureBucket(
            label=f"≥{val}°{unit}",
            low=val, high=None,
            type="higher", unit=unit
        )
    
    # Pattern 2: "X°C or below" / "X or below"  
    m = re.search(r'(\d+)\s*°?[cf]?\s+or\s+below', q_lower)
    if m:
        val = int(m.group(1))
        return PWSTemperatureBucket(
            label=f"≤{val}°{unit}",
            low=None, high=val,
            type="below", unit=unit
        )
    
    # Pattern 3: "between X°C and Y°C" / "between X and Y"
    m = re.search(r'between\s+(\d+)\s*°?[cf]?\s+and\s+(\d+)', q_lower)
    if m:
        lo, hi = int(m.group(1)), int(m.group(2))
        return PWSTemperatureBucket(
            label=f"{lo}-{hi}°{unit}",
            low=lo, high=hi,
            type="range", unit=unit
        )
    
    # Pattern 4: "between X-Y°C" (hyphenated)
    m = re.search(r'between\s+(\d+)\s*-\s*(\d+)', q_lower)
    if m:
        lo, hi = int(m.group(1)), int(m.group(2))
        return PWSTemperatureBucket(
            label=f"{lo}-{hi}°{unit}",
            low=lo, high=hi,
            type="range", unit=unit
        )
    
    # Pattern 5: "be X°C on" (exact value)
    m = re.search(r'be\s+(\d+)\s*°?[cf]?\s+on', q_lower)
    if m:
        val = int(m.group(1))
        return PWSTemperatureBucket(
            label=f"{val}°{unit}",
            low=val, high=val,
            type="exact", unit=unit
        )
    
    # Pattern 6: Fallback — any "X-Y°C" in question    
    m = re.search(r'(\d+)\s*-\s*(\d+)\s*°[cf]', q_lower)
    if m:
        lo, hi = int(m.group(1)), int(m.group(2))
        return PWSTemperatureBucket(
            label=f"{lo}-{hi}°{unit}",
            low=lo, high=hi,
            type="range", unit=unit
        )
    
    return None


# ──────────────────────────────────────────────
# Market Model
# ──────────────────────────────────────────────

@dataclass
class PWSMarket:
    """Represents a Polymarket Market Contract (e.g. YES or NO)."""
    token_id: str
    condition_id: str
    question: str
    slug: str
    outcome: str  # "YES" or "NO"
    price: float = 0.0
    
    # Enhanced — bucket parsing + city context
    bucket: Optional[PWSTemperatureBucket] = None
    city: Optional[str] = None
    market_date: Optional[str] = None  # "2026-02-17"
    
    # Optional metadata
    end_date: Optional[str] = None
    rewards_daily: bool = False
    
    @property
    def sort_key(self) -> Tuple[int, int]:
        """
        Sıralama: (bucket sırası, outcome sırası).
        YES=0, NO=1 → aynı bucket'ta YES önce gelir.
        """
        bucket_key = self.bucket.sort_key if self.bucket else 0
        outcome_key = 0 if self.outcome == "YES" else 1
        return (bucket_key, outcome_key)
    
    @property    
    def display_label(self) -> str:
        """Kontrat gösterim etiketi."""
        if self.bucket:
            return f"{self.bucket.label} {self.outcome}"
        return f"{self.question[:30]}... {self.outcome}"
    
    @property
    def token_short(self) -> str:
        """Token ID'nin son 8 karakteri."""
        return f"...{self.token_id[-8:]}" if len(self.token_id) > 8 else self.token_id


# ──────────────────────────────────────────────
# Orderbook Models
# ──────────────────────────────────────────────

@dataclass
class PWSOrderbookLevel:
    """Single price level."""
    price: float
    size: float


@dataclass
class PWSOrderbook:
    """Snapshot of an orderbook for a single token."""
    market_id: str  # token_id
    bids: List[PWSOrderbookLevel] = field(default_factory=list)
    asks: List[PWSOrderbookLevel] = field(default_factory=list)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    hash: Optional[str] = None
    
    def __post_init__(self):
        """Auto-sort: bids DESC by price, asks ASC by price."""
        self.bids.sort(key=lambda x: x.price, reverse=True)
        self.asks.sort(key=lambda x: x.price)
    
    @property
    def best_bid(self) -> Optional[float]:
        return self.bids[0].price if self.bids else None
    
    @property
    def best_ask(self) -> Optional[float]:
        return self.asks[0].price if self.asks else None
    
    @property
    def spread(self) -> Optional[float]:
        if self.best_bid is not None and self.best_ask is not None:
            return round(self.best_ask - self.best_bid, 4)
        return None
    
    @property
    def mid_price(self) -> Optional[float]:
        if self.best_bid is not None and self.best_ask is not None:
            return round((self.best_bid + self.best_ask) / 2, 4)
        return None
    
    @property
    def bid_depth(self) -> float:
        """Total size across all bid levels."""
        return sum(level.size for level in self.bids)
    
    @property
    def ask_depth(self) -> float:
        """Total size across all ask levels."""
        return sum(level.size for level in self.asks)


# ──────────────────────────────────────────────
# Trade Model
# ──────────────────────────────────────────────

@dataclass
class PWSTrade:
    """Represents a trade execution."""
    market_id: str  # token_id
    price: float
    size: float = 0.0
    side: str = ""  # "BUY" or "SELL"
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    fee_rate: Optional[str] = None


# ──────────────────────────────────────────────
# Price Change Model (from WebSocket)
# ──────────────────────────────────────────────

@dataclass
class PWSPriceChange:
    """Detailed price change from WebSocket price_change event."""
    asset_id: str       # token_id
    price: float
    size: float
    side: str           # "BUY" or "SELL"
    best_bid: float
    best_ask: float
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    @property
    def spread(self) -> float:
        return round(self.best_ask - self.best_bid, 4)
    
    @property
    def mid_price(self) -> float:
        return round((self.best_bid + self.best_ask) / 2, 4)
