"""
PWS Demo Script
Full demonstration of the PWS module:
1. Discovery — find temperature contracts for a city
2. REST — fetch initial orderbook snapshots
3. Storage — persist contracts and snapshots to SQLite
4. Stream — connect WebSocket and receive live orderbook updates

Usage:
    python -m infra.pws.demo
    python -m infra.pws.demo --city ankara --date 2026-02-17 --duration 30
"""
import asyncio
import argparse
import logging
import sys
from datetime import date, datetime, timezone

# Setup logging before imports
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("pws.demo")

from .models import parse_temperature_bucket
from .discovery import PolymarketDiscovery, build_slug
from .rest import PolymarketREST
from .storage import PWSStorage
from .stream import PolymarketStream


async def run_demo(city: str, target_date: str, duration: int):
    """Run the full PWS demo."""
    
    print("=" * 60)
    print("🚀 PWS MODULE DEMO")
    print("=" * 60)
    
    # ─── Step 1: Parse Bucket Test ───
    print("\n=== 1. TEMPERATURE BUCKET PARSER ===")
    test_questions = [
        "Will the highest temperature be 15°C or higher?",
        "Will the highest temperature be 7°C or below?",
        "Will the highest temperature be between 10°C and 12°C?",
        "Will the highest temperature be between 8-9°C?",
    ]
    for q in test_questions:
        b = parse_temperature_bucket(q)
        print(f"   ✅ \"{q[:45]}...\" → {b.label if b else '???'}")
    
    # ─── Step 2: Slug Builder ───
    print("\n=== 2. SLUG BUILDER ===")
    slug = build_slug(city, target_date)
    print(f"   🔗 {city} / {target_date} → {slug}")
    
    # ─── Step 3: Discovery ───
    print("\n=== 3. MARKET DISCOVERY ===")
    disco = PolymarketDiscovery()
    markets = disco.discover_city(city, target_date)
    
    if not markets:
        print(f"   ❌ No markets found for {city} on {target_date}")
        print("   (Market may not exist yet — try a different date)")
        return
    
    # Print formatted table
    disco.print_contracts(markets, city, target_date)
    
    # ─── Step 4: Storage ───
    print("\n=== 4. STORAGE ===")
    storage = PWSStorage()
    storage.save_contracts(markets)
    
    stats = storage.get_stats()
    print(f"   💾 Saved {stats['contracts_active']} contracts to {stats['db_path']}")
    
    # ─── Step 5: REST Snapshot ───
    print("\n=== 5. REST ORDERBOOK SNAPSHOT ===")
    rest = PolymarketREST()
    
    # Get first YES token for test
    yes_tokens = [tid for tid, m in markets.items() if m.outcome == "YES"]
    
    if yes_tokens:
        sample_tid = yes_tokens[0]
        sample_market = markets[sample_tid]
        label = sample_market.bucket.label if sample_market.bucket else "???"
        
        print(f"   Fetching book for {label} YES (token ...{sample_tid[-8:]})...")
        book = rest.get_book(sample_tid)
        
        if book:
            print(f"   ✅ Bid: {book.best_bid} | Ask: {book.best_ask} | Spread: {book.spread} | Mid: {book.mid_price}")
            print(f"   📊 Depth: {len(book.bids)} bids, {len(book.asks)} asks")
            
            # Save to storage
            storage.save_snapshot(book, event_type="rest", force=True)
            print(f"   💾 Snapshot saved to DB")
        else:
            print(f"   ⚠️ REST book returned None (may need token_id format check)")
    
    # ─── Step 6: WebSocket Stream ───
    if duration > 0:
        print(f"\n=== 6. WEBSOCKET STREAM ({duration}s) ===")
        print("   (Gözlemlenen trade'ler başka kullanıcılara ait — bot trade yapmıyor)")
        
        stream = PolymarketStream(auto_reconnect=False)
        book_count = 0
        trade_log = []
        
        # In-memory cache for latest book per token
        latest_books = {}
        
        @stream.on_book
        async def on_book(book):
            nonlocal book_count
            book_count += 1
            latest_books[book.market_id] = book
            storage.save_snapshot(book, event_type="book")
        
        @stream.on_trade
        async def on_trade(trade):
            market = markets.get(trade.market_id)
            label = "???"
            if market:
                label = market.bucket.label if market.bucket else "???"
                label = f"{label} {market.outcome}"
            storage.save_trade(trade)
            trade_log.append(f"   💰 {trade.timestamp.strftime('%H:%M:%S')} | {label:<12} | {trade.side:<4} | ₵{trade.price:.2f} | {trade.size:.0f} adet")
        
        def print_summary():
            """Print current orderbook state as sorted table."""
            # Group by condition_id
            groups = {}
            for tid, m in markets.items():
                key = m.condition_id or m.question
                if key not in groups:
                    groups[key] = {"bucket": m.bucket, "yes": None, "no": None}
                book = latest_books.get(tid)
                if m.outcome == "YES":
                    groups[key]["yes"] = book
                else:
                    groups[key]["no"] = book
            
            sorted_g = sorted(groups.values(), key=lambda g: g["bucket"].sort_key if g["bucket"] else 0)
            
            print(f"\n   📊 {city.upper()} — Canlı Orderbook — {target_date}")
            print(f"   ┌{'─'*10}┬{'─'*20}┬{'─'*20}┐")
            print(f"   │ {'Bucket':<8} │ {'YES Bid/Ask':^18} │ {'NO Bid/Ask':^18} │")
            print(f"   ├{'─'*10}┼{'─'*20}┼{'─'*20}┤")
            
            for g in sorted_g:
                label = g["bucket"].label if g["bucket"] else "???"
                yb = g.get("yes")
                nb = g.get("no")
                
                if yb and yb.best_bid is not None:
                    y_str = f"{yb.best_bid:.2f} / {yb.best_ask:.2f}" if yb.best_ask is not None else f"{yb.best_bid:.2f} /  -  "
                elif yb and yb.best_ask is not None:
                    y_str = f" -   / {yb.best_ask:.2f}"
                else:
                    y_str = "   bekleniyor   "
                
                if nb and nb.best_bid is not None:
                    n_str = f"{nb.best_bid:.2f} / {nb.best_ask:.2f}" if nb.best_ask is not None else f"{nb.best_bid:.2f} /  -  "
                elif nb and nb.best_ask is not None:
                    n_str = f" -   / {nb.best_ask:.2f}"
                else:
                    n_str = "   bekleniyor   "
                
                print(f"   │ {label:<8} │ {y_str:^18} │ {n_str:^18} │")
            
            print(f"   └{'─'*10}┴{'─'*20}┴{'─'*20}┘")
            print(f"   📡 {book_count} book event alındı")
            
            # Show recent trades if any
            if trade_log:
                print(f"\n   Son Trade'ler (gözlemlenen):")
                for t in trade_log[-5:]:  # Last 5
                    print(t)
        
        try:
            await stream.connect()
            
            token_ids = list(markets.keys())
            await stream.subscribe(token_ids)
            print(f"   ✅ {len(token_ids)} token'a abone olundu")
            
            # Listen in background, print summary periodically
            listen_task = asyncio.create_task(stream.listen())
            
            elapsed = 0
            interval = 5  # Print summary every 5 seconds
            while elapsed < duration:
                wait = min(interval, duration - elapsed)
                await asyncio.sleep(wait)
                elapsed += wait
                print_summary()
            
            await stream.stop()
            listen_task.cancel()
            
            try:
                await listen_task
            except asyncio.CancelledError:
                pass
            
        except Exception as e:
            print(f"   ❌ WebSocket error: {e}")
        
        print(f"\n   ✅ {duration}s test — toplam {book_count} book event, {len(trade_log)} trade gözlemlendi")
    
    # ─── Final Stats ───
    print("\n=== FINAL STATS ===")
    final_stats = storage.get_stats()
    print(f"   📊 Contracts:  {final_stats['contracts_active']} active")
    print(f"   📊 Snapshots:  {final_stats['orderbook_snapshots']:,}")
    print(f"   📊 Trades:     {final_stats['trade_events']:,}")
    print(f"   📊 DB Size:    {final_stats['db_size_mb']:.2f} MB")
    print(f"   📊 DB Path:    {final_stats['db_path']}")
    print("\n" + "=" * 60)


def main():
    parser = argparse.ArgumentParser(description="PWS Module Demo")
    parser.add_argument("--city", default="ankara", help="City to discover (default: ankara)")
    parser.add_argument("--date", default=None, help="Target date YYYY-MM-DD (default: today)")
    parser.add_argument("--duration", type=int, default=15, help="WebSocket listen duration in seconds (default: 15, 0=skip)")
    
    args = parser.parse_args()
    target_date = args.date or date.today().strftime("%Y-%m-%d")
    
    asyncio.run(run_demo(args.city, target_date, args.duration))


if __name__ == "__main__":
    main()
