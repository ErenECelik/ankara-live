"""
PWS Market Manager
Orchestrator that combines Discovery, Stream, REST, and Storage.

This is the main entry point for bots — they interact with MarketManager
instead of individual modules.

Features:
    - Automatic market discovery for configured cities
    - WebSocket streaming with orderbook caching
    - SQLite persistence for all data
    - Runtime city add/remove
    - Formatted terminal display
"""
import asyncio
import logging
from datetime import datetime, date, timezone
from typing import List, Dict, Optional, Callable
from collections import OrderedDict

from .models import PWSMarket, PWSOrderbook, PWSTrade, PWSPriceChange
from .discovery import PolymarketDiscovery, build_slug
from .stream import PolymarketStream
from .rest import PolymarketREST
from .storage import PWSStorage

log = logging.getLogger("pws.manager")


class MarketManager:
    """
    Central orchestrator for Polymarket data infrastructure.
    
    Usage:
        manager = MarketManager(cities=["ankara", "istanbul"])
        
        @manager.on_book
        async def on_book(book): ...
        
        await manager.start()        # discover + connect + subscribe + listen
        await manager.stop()         # graceful shutdown
    
    Or step-by-step:
        manager = MarketManager(cities=["ankara"])
        await manager.discover()     # find markets
        await manager.connect()      # WebSocket connect
        await manager.stream()       # start listening
    """
    
    def __init__(self, cities: List[str] = None, target_date: str = None,
                 auto_reconnect: bool = True, snapshot_interval: int = 5):
        # Services
        self.discovery = PolymarketDiscovery()
        self.stream = PolymarketStream(auto_reconnect=auto_reconnect)
        self.rest = PolymarketREST()
        self.storage = PWSStorage(snapshot_interval_sec=snapshot_interval)
        
        # Config
        self.cities: List[str] = cities or []
        self.target_date: str = target_date or date.today().strftime("%Y-%m-%d")
        
        # Registry: token_id → PWSMarket (sorted by bucket)
        self.registry: OrderedDict[str, PWSMarket] = OrderedDict()
        
        # Internal callbacks
        self._external_book_cbs: List[Callable] = []
        self._external_trade_cbs: List[Callable] = []
        self._external_price_cbs: List[Callable] = []
        self._external_discovery_cbs: List[Callable] = []
        self._external_resolved_cbs: List[Callable] = []
        
        # Wire up internal handlers
        self.stream.on_book(self._on_book_internal)
        self.stream.on_trade(self._on_trade_internal)
        self.stream.on_price_change(self._on_price_change_internal)
        self.stream.on_market_resolved(self._on_market_resolved_internal)
    
    # ══════════════════════════════════════════
    # External Callback Registration
    # ══════════════════════════════════════════
    
    def on_book(self, func: Callable):
        """Register callback for orderbook updates."""
        self._external_book_cbs.append(func)
        return func
    
    def on_trade(self, func: Callable):
        """Register callback for trade events."""
        self._external_trade_cbs.append(func)
        return func
    
    def on_price_change(self, func: Callable):
        """Register callback for price changes."""
        self._external_price_cbs.append(func)
        return func
    
    def on_discovery(self, func: Callable):
        """Register callback for new market discovery."""
        self._external_discovery_cbs.append(func)
        return func
    
    def on_resolved(self, func: Callable):
        """Register callback for market resolution."""
        self._external_resolved_cbs.append(func)
        return func
    
    # ══════════════════════════════════════════
    # Lifecycle: start / stop / refresh
    # ══════════════════════════════════════════
    
    async def start(self):
        """
        Full startup sequence:
        1. Discover markets for all cities
        2. Connect WebSocket
        3. Subscribe to all discovered tokens
        4. Start listening in background
        """
        log.info("🚀 MarketManager starting...")
        
        # Step 1: Discover
        await self.discover()
        
        if not self.registry:
            log.warning("⚠️ No markets found — cannot start streaming")
            return
        
        # Step 2: Connect
        await self.connect()
        
        # Step 3: Stream (runs in background)
        asyncio.create_task(self.stream.listen())
        
        log.info(f"✅ MarketManager running — {len(self.registry)} tokens, {len(self.cities)} cities")
    
    async def discover(self, target_date: str = None):
        """Discover markets for all configured cities."""
        if target_date:
            self.target_date = target_date
        
        log.info(f"🔍 Discovering markets for {len(self.cities)} cities on {self.target_date}...")
        
        new_markets = OrderedDict()
        
        for city in self.cities:
            city_markets = self.discovery.discover_city(city, self.target_date)
            new_markets.update(city_markets)
        
        if new_markets:
            self.registry.update(new_markets)
            self.storage.save_contracts(new_markets)
            
            log.info(f"📊 Registry: {len(self.registry)} tokens total")
            
            # Fire discovery callbacks
            for cb in self._external_discovery_cbs:
                try:
                    if asyncio.iscoroutinefunction(cb):
                        await cb(new_markets)
                    else:
                        cb(new_markets)
                except Exception as e:
                    log.error(f"Discovery callback error: {e}")
        
        return new_markets
    
    async def connect(self):
        """Connect WebSocket and subscribe to all registered tokens."""
        if not self.registry:
            log.warning("No tokens in registry — skipping connect")
            return
        
        await self.stream.connect()
        
        token_ids = list(self.registry.keys())
        await self.stream.subscribe(token_ids)
        
        log.info(f"📡 Subscribed to {len(token_ids)} tokens")
    
    async def refresh(self, target_date: str = None):
        """
        Switch to new day's markets.
        1. Deactivate old markets in DB
        2. Unsubscribe from old tokens
        3. Clear registry
        4. Discover new markets
        5. Subscribe to new tokens
        """
        log.info("🔄 Refreshing markets for new day...")
        
        old_date = self.target_date
        new_date = target_date or date.today().strftime("%Y-%m-%d")
        
        # Deactivate old
        for city in self.cities:
            self.storage.deactivate_date(city, old_date)
        
        # Unsubscribe old
        if self.registry and self.stream.is_connected:
            old_ids = list(self.registry.keys())
            await self.stream.unsubscribe(old_ids)
        
        # Clear
        self.registry.clear()
        self.target_date = new_date
        
        # Discover new
        new_markets = await self.discover(new_date)
        
        # Subscribe new
        if new_markets and self.stream.is_connected:
            await self.stream.subscribe(list(new_markets.keys()))
        
        log.info(f"✅ Refreshed: {len(new_markets)} new tokens for {new_date}")
        return new_markets
    
    async def stop(self):
        """Graceful shutdown."""
        log.info("🛑 MarketManager stopping...")
        await self.stream.stop()
        log.info("✅ MarketManager stopped.")
    
    # ══════════════════════════════════════════
    # Runtime: add/remove cities & tokens
    # ══════════════════════════════════════════
    
    async def add_city(self, city: str, target_date: str = None):
        """Add a city at runtime — discovers and subscribes."""
        if city not in self.cities:
            self.cities.append(city)
        
        dt = target_date or self.target_date
        city_markets = self.discovery.discover_city(city, dt)
        
        if city_markets:
            self.registry.update(city_markets)
            self.storage.save_contracts(city_markets)
            
            if self.stream.is_connected:
                await self.stream.subscribe(list(city_markets.keys()))
            
            log.info(f"➕ Added {city}: {len(city_markets)} tokens")
        
        return city_markets
    
    async def remove_city(self, city: str):
        """Remove a city at runtime — unsubscribes."""
        city_tokens = [
            tid for tid, m in self.registry.items() 
            if m.city == city
        ]
        
        if city_tokens and self.stream.is_connected:
            await self.stream.unsubscribe(city_tokens)
        
        for tid in city_tokens:
            del self.registry[tid]
        
        if city in self.cities:
            self.cities.remove(city)
        
        log.info(f"➖ Removed {city}: {len(city_tokens)} tokens unsubscribed")
    
    def add_manual_token(self, token_id: str, label: str = "manual"):
        """Manually add any token_id to the registry."""
        if token_id not in self.registry:
            self.registry[token_id] = PWSMarket(
                token_id=token_id,
                condition_id="",
                question=label,
                slug="manual",
                outcome="YES",
            )
            log.info(f"➕ Manual token added: ...{token_id[-8:]}")
    
    # ══════════════════════════════════════════
    # Data Access
    # ══════════════════════════════════════════
    
    def get_active_markets(self) -> Dict[str, PWSMarket]:
        """Get all active market registry."""
        return dict(self.registry)
    
    def get_markets_by_city(self, city: str) -> Dict[str, PWSMarket]:
        """Get markets filtered by city."""
        return {
            tid: m for tid, m in self.registry.items()
            if m.city == city
        }
    
    def get_orderbook(self, token_id: str) -> Optional[PWSOrderbook]:
        """Get cached orderbook for a token."""
        return self.stream.get_cached_book(token_id)
    
    def get_best_prices(self, city: str = None) -> List[Dict]:
        """
        Get current best bid/ask for all contracts, sorted by bucket.
        
        Returns list of dicts with YES + NO side by side for same contract.
        """
        markets = self.get_markets_by_city(city) if city else self.registry
        
        # Group by condition_id (same contract)
        groups = {}
        for tid, market in markets.items():
            key = market.condition_id or market.question
            if key not in groups:
                groups[key] = {"yes": None, "no": None, "bucket": market.bucket}
            
            book = self.stream.get_cached_book(tid)
            
            if market.outcome == "YES":
                groups[key]["yes"] = {
                    "token_id": tid,
                    "bid": book.best_bid if book else None,
                    "ask": book.best_ask if book else None,
                    "mid": book.mid_price if book else None,
                    "last_update": book.timestamp.strftime("%H:%M:%S") if book else "-",
                }
            else:
                groups[key]["no"] = {
                    "token_id": tid,
                    "bid": book.best_bid if book else None,
                    "ask": book.best_ask if book else None,
                }
        
        # Sort by bucket sort_key
        result = []
        for key, group in sorted(groups.items(), key=lambda x: x[1]["bucket"].sort_key if x[1]["bucket"] else 0):
            bucket = group["bucket"]
            yes = group.get("yes", {}) or {}
            no = group.get("no", {}) or {}
            
            result.append({
                "bucket": bucket.label if bucket else "???",
                "yes_bid": yes.get("bid"),
                "yes_ask": yes.get("ask"),
                "spread": round(yes["ask"] - yes["bid"], 4) if yes.get("ask") and yes.get("bid") else None,
                "mid": yes.get("mid"),
                "no_bid": no.get("bid"),
                "no_ask": no.get("ask"),
                "last_update": yes.get("last_update", "-"),
            })
        
        return result
    
    # ══════════════════════════════════════════
    # Internal Callbacks (wire stream → storage)
    # ══════════════════════════════════════════
    
    async def _on_book_internal(self, book: PWSOrderbook):
        """Book event → save snapshot + forward to external callbacks."""
        # Persist (sampling applied internally)
        self.storage.save_snapshot(book, event_type="book")
        
        # Forward
        for cb in self._external_book_cbs:
            try:
                if asyncio.iscoroutinefunction(cb):
                    await cb(book)
                else:
                    cb(book)
            except Exception as e:
                log.error(f"External book callback error: {e}")
    
    async def _on_trade_internal(self, trade: PWSTrade):
        """Trade event → save + forward."""
        self.storage.save_trade(trade)
        
        for cb in self._external_trade_cbs:
            try:
                if asyncio.iscoroutinefunction(cb):
                    await cb(trade)
                else:
                    cb(trade)
            except Exception as e:
                log.error(f"External trade callback error: {e}")
    
    async def _on_price_change_internal(self, change: PWSPriceChange):
        """Price change → forward to external callbacks."""
        for cb in self._external_price_cbs:
            try:
                if asyncio.iscoroutinefunction(cb):
                    await cb(change)
                else:
                    cb(change)
            except Exception as e:
                log.error(f"External price callback error: {e}")
    
    async def _on_market_resolved_internal(self, data: Dict):
        """Market resolved → deactivate in storage + forward."""
        # Try to deactivate resolved tokens
        asset_ids = data.get("assets_ids", [])
        for aid in asset_ids:
            self.storage.deactivate_market(aid)
        
        for cb in self._external_resolved_cbs:
            try:
                if asyncio.iscoroutinefunction(cb):
                    await cb(data)
                else:
                    cb(data)
            except Exception as e:
                log.error(f"External resolved callback error: {e}")
    
    # ══════════════════════════════════════════
    # Display
    # ══════════════════════════════════════════
    
    def print_discovery(self):
        """Print discovered contracts in formatted table."""
        self.discovery.print_contracts(self.registry, 
                                       city=", ".join(self.cities),
                                       target_date=self.target_date)
    
    def print_live_table(self):
        """Print current orderbook state as formatted table."""
        prices = self.get_best_prices()
        
        uptime = self.stream.uptime_seconds
        h, m, s = int(uptime // 3600), int((uptime % 3600) // 60), int(uptime % 60)
        
        header = f"📡 CANLI ORDERBOOK — {', '.join(c.title() for c in self.cities)} {self.target_date}"
        stats = f"[Uptime: {h:02d}:{m:02d}:{s:02d} | Events: {self.stream.event_count}]"
        
        print(f"\n{header}     {stats}")
        print("━" * 78)
        print(f" {'Kontrat':<10} │ {'YES Bid':>7} │ {'YES Ask':>7} │ {'Spread':>6} │ {'Mid':>6} │ {'NO Bid':>6} │ {'NO Ask':>6} │ {'Son':>5}")
        print("─" * 78)
        
        for row in prices:
            y_bid = f"{row['yes_bid']:.2f}" if row.get("yes_bid") is not None else "  -  "
            y_ask = f"{row['yes_ask']:.2f}" if row.get("yes_ask") is not None else "  -  "
            spread = f"{row['spread']:.2f}" if row.get("spread") is not None else " -  "
            mid = f"{row['mid']:.3f}" if row.get("mid") is not None else "  -  "
            n_bid = f"{row['no_bid']:.2f}" if row.get("no_bid") is not None else "  -  "
            n_ask = f"{row['no_ask']:.2f}" if row.get("no_ask") is not None else "  -  "
            
            print(f" {row['bucket']:<10} │ {y_bid:>7} │ {y_ask:>7} │ {spread:>6} │ {mid:>6} │ {n_bid:>6} │ {n_ask:>6} │ {row.get('last_update', '-'):>5}")
        
        print("━" * 78)
    
    def print_stats(self):
        """Print database and connection stats."""
        db_stats = self.storage.get_stats()
        print(f"\n📊 PWS Stats:")
        print(f"   Contracts: {db_stats['contracts_active']} active / {db_stats['contracts_total']} total")
        print(f"   Snapshots: {db_stats['orderbook_snapshots']:,}")
        print(f"   Trades:    {db_stats['trade_events']:,}")
        print(f"   DB Size:   {db_stats['db_size_mb']:.2f} MB")
        print(f"   Events:    {self.stream.event_count:,}")
