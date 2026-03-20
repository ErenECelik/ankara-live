"""
PWS Stream Module
WebSocket client for Polymarket CLOB real-time data.

Connects to: wss://ws-subscriptions-clob.polymarket.com/ws/market

Handles 7 event types:
    book              — Full orderbook snapshot (bids + asks)
    price_change      — Price changes with best_bid/best_ask per asset
    last_trade_price  — Last executed trade
    best_bid_ask      — Best bid/ask change notification
    tick_size_change   — Tick size change (edge case: price >0.96 or <0.04)
    new_market        — New market created on Polymarket
    market_resolved   — Market resolved with winning outcome

Features:
    - Auto-reconnect with exponential backoff
    - Subscription state tracking for re-subscribe after reconnect
    - Orderbook cache (latest snapshot per token_id)
    - Callback decorator pattern
"""
import json
import asyncio
import logging
from datetime import datetime, timezone
from typing import List, Dict, Callable, Optional, Any

try:
    import websockets
except ImportError:
    websockets = None

from .models import (
    PWSOrderbook, PWSOrderbookLevel, PWSTrade, PWSPriceChange,
)

# CLOB WebSocket URL
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

log = logging.getLogger("pws.stream")


class PolymarketStream:
    """
    WebSocket client for Polymarket CLOB real-time data.
    
    Usage:
        stream = PolymarketStream()
        
        @stream.on_book
        async def handle_book(book: PWSOrderbook):
            print(f"Bid: {book.best_bid} Ask: {book.best_ask}")
        
        await stream.connect()
        await stream.subscribe(["TOKEN_ID_1", "TOKEN_ID_2"])
        await stream.listen()
    """
    
    def __init__(self, uri: str = WS_URL, auto_reconnect: bool = True, 
                 max_retries: int = 10):
        self.uri = uri
        self.ws = None
        self.running = False
        
        # Reconnect settings
        self._auto_reconnect = auto_reconnect
        self._max_retries = max_retries
        self._retry_count = 0
        
        # Subscription state (for re-subscribe after reconnect)
        self._subscribed_ids: List[str] = []
        self._subscribed_channels: List[str] = []
        
        # Orderbook cache: token_id → PWSOrderbook
        self._orderbook_cache: Dict[str, PWSOrderbook] = {}
        
        # Connection stats
        self._connect_time: Optional[datetime] = None
        self._event_count: int = 0
        
        # ─── Callback registries ───
        self._callbacks_book: List[Callable] = []
        self._callbacks_trade: List[Callable] = []
        self._callbacks_price_change: List[Callable] = []
        self._callbacks_best_bid_ask: List[Callable] = []
        self._callbacks_tick_size: List[Callable] = []
        self._callbacks_new_market: List[Callable] = []
        self._callbacks_market_resolved: List[Callable] = []
    
    # ══════════════════════════════════════════
    # Callback Decorators
    # ══════════════════════════════════════════
    
    def on_book(self, func: Callable):
        """Register callback for orderbook updates. Receives PWSOrderbook."""
        self._callbacks_book.append(func)
        return func
    
    def on_trade(self, func: Callable):
        """Register callback for trade events. Receives PWSTrade."""
        self._callbacks_trade.append(func)
        return func
    
    def on_price_change(self, func: Callable):
        """Register callback for price changes. Receives PWSPriceChange."""
        self._callbacks_price_change.append(func)
        return func
    
    def on_best_bid_ask(self, func: Callable):
        """Register callback for best bid/ask changes. Receives dict."""
        self._callbacks_best_bid_ask.append(func)
        return func
    
    def on_tick_size_change(self, func: Callable):
        """Register callback for tick size changes. Receives dict."""
        self._callbacks_tick_size.append(func)
        return func
    
    def on_new_market(self, func: Callable):
        """Register callback for new market creation. Receives dict."""
        self._callbacks_new_market.append(func)
        return func
    
    def on_market_resolved(self, func: Callable):
        """Register callback for market resolution. Receives dict."""
        self._callbacks_market_resolved.append(func)
        return func

    async def _emit(self, callbacks: List[Callable], data: Any):
        """Fire all registered callbacks."""
        for cb in callbacks:
            try:
                if asyncio.iscoroutinefunction(cb):
                    await cb(data)
                else:
                    cb(data)
            except Exception as e:
                log.error(f"Callback error [{cb.__name__}]: {e}")

    # ══════════════════════════════════════════
    # Connection
    # ══════════════════════════════════════════

    async def connect(self):
        """Establish WebSocket connection."""
        if websockets is None:
            raise ImportError("websockets library required: pip install websockets")
            
        log.info(f"🔌 Connecting to {self.uri}...")
        try:
            self.ws = await websockets.connect(
                self.uri,
                ping_interval=20,      # Send ping every 20s
                ping_timeout=10,       # Wait 10s for pong
                close_timeout=5,
            )
            self.running = True
            self._connect_time = datetime.now(timezone.utc)
            self._retry_count = 0
            log.info("✅ Connected to Polymarket CLOB WebSocket.")
        except Exception as e:
            log.error(f"Connection failed: {e}")
            raise

    async def subscribe(self, token_ids: List[str], 
                       channels: List[str] = None):
        """
        Subscribe to market channels for given token IDs.
        Saves subscription state for automatic re-subscribe after reconnect.
        
        Default channels: ["book"] — gives book + price_change + last_trade_price + best_bid_ask
        """
        if not self.ws:
            raise RuntimeError("Not connected — call connect() first")
        
        if channels is None:
            channels = ["market"]
        
        ids = [str(x) for x in token_ids]
        
        # Save state for reconnect
        for tid in ids:
            if tid not in self._subscribed_ids:
                self._subscribed_ids.append(tid)
        self._subscribed_channels = channels
        
        for channel in channels:
            msg = {
                "type": "subscribe",
                "channel": channel,
                "assets_ids": ids,
            }
            await self.ws.send(json.dumps(msg))
            log.info(f"📤 Subscribed to '{channel}' for {len(ids)} assets")

    async def unsubscribe(self, token_ids: List[str]):
        """Unsubscribe from specific token IDs."""
        if not self.ws:
            return
        
        ids = [str(x) for x in token_ids]
        
        # Remove from subscription state
        self._subscribed_ids = [t for t in self._subscribed_ids if t not in ids]
        
        # Remove from cache
        for tid in ids:
            self._orderbook_cache.pop(tid, None)
        
        for channel in (self._subscribed_channels or ["market"]):
            msg = {
                "type": "unsubscribe",
                "channel": channel,
                "assets_ids": ids,
            }
            await self.ws.send(json.dumps(msg))
            
        log.info(f"📤 Unsubscribed from {len(ids)} assets")

    # ══════════════════════════════════════════
    # Listen Loop (with auto-reconnect)
    # ══════════════════════════════════════════

    async def listen(self):
        """
        Main message loop with auto-reconnect.
        
        On disconnect:
        1. Waits with exponential backoff (1s, 2s, 4s, ... max 60s)
        2. Reconnects to WebSocket
        3. Re-subscribes to all saved token_ids
        4. Continues listening
        """
        while self.running:
            try:
                if not self.ws:
                    await self.connect()
                
                log.info("👂 Listening for messages...")
                
                async for message in self.ws:
                    if not self.running:
                        break
                    
                    try:
                        data = json.loads(message)
                        
                        # Handle batch (list) or single (dict)
                        items = data if isinstance(data, list) else [data]
                        
                        for item in items:
                            await self._process_message(item)
                            self._event_count += 1
                            
                    except json.JSONDecodeError:
                        pass
                        
            except websockets.exceptions.ConnectionClosed as e:
                log.warning(f"⚡ Connection closed: {e}")
                self.ws = None
                
                if self._auto_reconnect and self.running:
                    await self._reconnect()
                else:
                    break
                    
            except Exception as e:
                log.error(f"Listen error: {e}")
                self.ws = None
                
                if self._auto_reconnect and self.running:
                    await self._reconnect()
                else:
                    break
        
        log.info("Stopped listening.")

    async def _reconnect(self):
        """Reconnect with exponential backoff."""
        if self._retry_count >= self._max_retries:
            log.error(f"❌ Max retries ({self._max_retries}) reached. Giving up.")
            self.running = False
            return
        
        # Exponential backoff: 1, 2, 4, 8, 16, 32, 60, 60, ...
        delay = min(2 ** self._retry_count, 60)
        self._retry_count += 1
        
        log.info(f"🔄 Reconnecting in {delay}s (attempt {self._retry_count}/{self._max_retries})...")
        await asyncio.sleep(delay)
        
        try:
            await self.connect()
            
            # Re-subscribe to saved state
            if self._subscribed_ids:
                await self.subscribe(
                    self._subscribed_ids,
                    self._subscribed_channels or ["market"],
                )
                log.info(f"🔄 Re-subscribed to {len(self._subscribed_ids)} assets")
                
        except Exception as e:
            log.error(f"Reconnect failed: {e}")
            # Will retry on next loop iteration

    # ══════════════════════════════════════════
    # Message Processing (7 Event Types)
    # ══════════════════════════════════════════

    async def _process_message(self, data: Dict):
        """Route incoming message to correct handler based on event_type."""
        event_type = data.get("event_type") or data.get("type")
        
        if event_type == "book":
            book = self._parse_book(data)
            if book:
                # Update cache
                self._orderbook_cache[book.market_id] = book
                await self._emit(self._callbacks_book, book)
        
        elif event_type == "price_change":
            changes = self._parse_price_changes(data)
            for change in changes:
                # Cached book'u güncelle: timestamp + top-of-book fiyatları.
                # Polymarket nadiren tam 'book' event'i gönderir; price_change
                # çok daha sık gelir. Timestamp'i güncellezsek book_age_ms
                # "son tam snapshot'tan bu yana" değil "son herhangi bir
                # market aktivitesinden bu yana" anlamına gelir — çok daha
                # anlamlı bir metrik.
                cached = self._orderbook_cache.get(change.asset_id)
                if cached is not None:
                    cached.timestamp = change.timestamp
                    # Top-of-book fiyatlarını price_change'deki best_bid/ask ile
                    # senkronize et (bids/asks listesi boşsa atlıyoruz).
                    if cached.bids and change.best_bid > 0:
                        cached.bids[0] = PWSOrderbookLevel(
                            price=change.best_bid,
                            size=cached.bids[0].size,
                        )
                    if cached.asks and change.best_ask > 0:
                        cached.asks[0] = PWSOrderbookLevel(
                            price=change.best_ask,
                            size=cached.asks[0].size,
                        )
                await self._emit(self._callbacks_price_change, change)
        
        elif event_type == "last_trade_price":
            trade = self._parse_trade(data)
            if trade:
                await self._emit(self._callbacks_trade, trade)
        
        elif event_type == "best_bid_ask":
            await self._emit(self._callbacks_best_bid_ask, data)
        
        elif event_type == "tick_size_change":
            await self._emit(self._callbacks_tick_size, data)
        
        elif event_type == "new_market":
            log.info(f"🆕 New market: {data.get('question', 'unknown')[:50]}")
            await self._emit(self._callbacks_new_market, data)
        
        elif event_type == "market_resolved":
            log.info(f"🏁 Market resolved: {data.get('question', 'unknown')[:50]}")
            await self._emit(self._callbacks_market_resolved, data)

    # ══════════════════════════════════════════
    # Parsers
    # ══════════════════════════════════════════

    def _parse_book(self, data: Dict) -> Optional[PWSOrderbook]:
        """Parse a 'book' event into PWSOrderbook."""
        asset_id = data.get("asset_id")
        if not asset_id:
            return None
        
        bids = []
        for b in data.get("bids", []):
            if isinstance(b, dict):
                bids.append(PWSOrderbookLevel(
                    float(b.get("price", 0)),
                    float(b.get("size", 0))
                ))
            elif isinstance(b, list) and len(b) >= 2:
                bids.append(PWSOrderbookLevel(float(b[0]), float(b[1])))
        
        asks = []
        for a in data.get("asks", []):
            if isinstance(a, dict):
                asks.append(PWSOrderbookLevel(
                    float(a.get("price", 0)),
                    float(a.get("size", 0))
                ))
            elif isinstance(a, list) and len(a) >= 2:
                asks.append(PWSOrderbookLevel(float(a[0]), float(a[1])))
        
        # Parse timestamp (Polymarket sends milliseconds)
        ts = data.get("timestamp")
        if ts:
            try:
                ts_dt = datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc)
            except (ValueError, OSError):
                ts_dt = datetime.now(timezone.utc)
        else:
            ts_dt = datetime.now(timezone.utc)
        
        return PWSOrderbook(
            market_id=asset_id,
            bids=bids,
            asks=asks,
            timestamp=ts_dt,
            hash=data.get("hash"),
        )

    def _parse_trade(self, data: Dict) -> Optional[PWSTrade]:
        """Parse a 'last_trade_price' event into PWSTrade."""
        asset_id = data.get("asset_id")
        if not asset_id:
            return None
        
        ts = data.get("timestamp")
        if ts:
            try:
                ts_dt = datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc)
            except (ValueError, OSError):
                ts_dt = datetime.now(timezone.utc)
        else:
            ts_dt = datetime.now(timezone.utc)
        
        return PWSTrade(
            market_id=asset_id,
            price=float(data.get("price", 0)),
            size=float(data.get("size", 0)),
            side=data.get("side", ""),
            timestamp=ts_dt,
            fee_rate=data.get("fee_rate_bps"),
        )

    def _parse_price_changes(self, data: Dict) -> List[PWSPriceChange]:
        """Parse a 'price_change' event into list of PWSPriceChange."""
        results = []
        
        ts = data.get("timestamp")
        if ts:
            try:
                ts_dt = datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc)
            except (ValueError, OSError):
                ts_dt = datetime.now(timezone.utc)
        else:
            ts_dt = datetime.now(timezone.utc)
        
        for change in data.get("price_changes", []):
            asset_id = change.get("asset_id")
            if not asset_id:
                continue
            
            results.append(PWSPriceChange(
                asset_id=asset_id,
                price=float(change.get("price", 0)),
                size=float(change.get("size", 0)),
                side=change.get("side", ""),
                best_bid=float(change.get("best_bid", 0)),
                best_ask=float(change.get("best_ask", 0)),
                timestamp=ts_dt,
            ))
        
        return results

    # ══════════════════════════════════════════
    # Data Access
    # ══════════════════════════════════════════

    def get_cached_book(self, token_id: str) -> Optional[PWSOrderbook]:
        """Get the latest cached orderbook for a token."""
        return self._orderbook_cache.get(token_id)
    
    def get_all_cached_books(self) -> Dict[str, PWSOrderbook]:
        """Get all cached orderbooks."""
        return dict(self._orderbook_cache)
    
    @property
    def uptime_seconds(self) -> float:
        """Seconds since connection was established."""
        if self._connect_time:
            return (datetime.now(timezone.utc) - self._connect_time).total_seconds()
        return 0.0
    
    @property
    def event_count(self) -> int:
        """Total events processed since connection."""
        return self._event_count

    # ══════════════════════════════════════════
    # Lifecycle
    # ══════════════════════════════════════════

    async def stop(self):
        """Graceful shutdown."""
        self.running = False
        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass
            self.ws = None
        log.info("🔌 Disconnected.")
    
    @property
    def is_connected(self) -> bool:
        """Check if WebSocket is open."""
        return self.ws is not None and self.running
