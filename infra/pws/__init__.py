"""
PWS — Polymarket Web Service Module
Modular data infrastructure for Polymarket temperature markets.

Exports:
    Models:     PWSMarket, PWSOrderbook, PWSOrderbookLevel, PWSTrade, 
                PWSPriceChange, PWSTemperatureBucket, parse_temperature_bucket
    Discovery:  PolymarketDiscovery, build_slug, CITY_SLUGS
    Stream:     PolymarketStream
    REST:       PolymarketREST
    Storage:    PWSStorage
    Manager:    MarketManager
    Scheduler:  MarketScheduler
"""

# Models
from .models import (
    PWSMarket,
    PWSOrderbook,
    PWSOrderbookLevel,
    PWSTrade,
    PWSPriceChange,
    PWSTemperatureBucket,
    parse_temperature_bucket,
)

# Discovery
from .discovery import (
    PolymarketDiscovery,
    build_slug,
    CITY_SLUGS,
)

# Stream
from .stream import PolymarketStream

# REST
from .rest import PolymarketREST

# Storage
from .storage import PWSStorage

# Manager
from .manager import MarketManager

# Scheduler
from .scheduler import MarketScheduler

__all__ = [
    # Models
    "PWSMarket", "PWSOrderbook", "PWSOrderbookLevel", "PWSTrade",
    "PWSPriceChange", "PWSTemperatureBucket", "parse_temperature_bucket",
    # Discovery
    "PolymarketDiscovery", "build_slug", "CITY_SLUGS",
    # Stream
    "PolymarketStream",
    # REST
    "PolymarketREST",
    # Storage
    "PWSStorage",
    # Manager
    "MarketManager",
    # Scheduler
    "MarketScheduler",
]
