"""
PWS Discovery Module
Handles finding markets and resolving Token IDs via Polymarket Gamma API.
Includes slug builder, multi-city discovery, and temperature bucket parsing.

Decoupled from 'core.config' and 'schema.py'.
"""
import json
import requests
import logging
from datetime import datetime, date
from typing import List, Optional, Dict, OrderedDict
from collections import OrderedDict as ODict

from .models import PWSMarket, parse_temperature_bucket

# Default API URL
GAMMA_API_URL = "https://gamma-api.polymarket.com"

# Logger
log = logging.getLogger("pws.discovery")

# ──────────────────────────────────────────────
# City Slug Mapping
# ──────────────────────────────────────────────

CITY_SLUGS = {
    "ankara":       "ankara",
    "istanbul":     "istanbul",
    "nyc":          "nyc",
    "chicago":      "chicago",
    "dallas":       "dallas",
    "la":           "la",
    "london":       "london",
    "seattle":      "seattle",
    "seoul":        "seoul",
    "wellington":   "wellington",
    "buenos-aires": "buenos-aires",
    "toronto":      "toronto",
    "atlanta":      "atlanta",
    "paris":        "paris",
    "miami":        "miami",
}


def build_slug(city: str, target_date: str = None) -> str:
    """
    Build Polymarket event slug for a city and date.
    
    Args:
        city: City key, e.g. "ankara", "nyc"
        target_date: ISO date string "2026-02-17" or None for today
    
    Returns:
        "highest-temperature-in-ankara-on-february-17-2026"
    """
    # Resolve city slug
    city_slug = CITY_SLUGS.get(city.lower(), city.lower())
    
    # Parse date
    if target_date is None:
        dt = date.today()
    elif isinstance(target_date, str):
        dt = datetime.strptime(target_date, "%Y-%m-%d").date()
    else:
        dt = target_date
    
    # Format: month-day-year (lowercase, no zero-padding)
    month = dt.strftime("%B").lower()      # "february"
    day = str(dt.day)                       # "17" (no zero-pad)
    year = str(dt.year)                     # "2026"
    
    return f"highest-temperature-in-{city_slug}-on-{month}-{day}-{year}"


class PolymarketDiscovery:
    """Client for discovering markets and Token IDs via Gamma API."""
    
    def __init__(self, base_url: str = GAMMA_API_URL):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "PWS-Discovery/1.0",
            "Accept": "application/json",
        })
    
    # ──────────────────────────────────────────
    # Low-Level API Calls
    # ──────────────────────────────────────────
    
    def get_event_by_slug(self, slug: str) -> Optional[Dict]:
        """Fetch full event details by slug from Gamma API."""
        try:
            url = f"{self.base_url}/events"
            params = {"slug": slug}
            
            r = self.session.get(url, params=params, timeout=15)
            if r.status_code != 200:
                log.warning(f"API Error {r.status_code}: {r.text[:200]}")
                return None
                
            data = r.json()
            if isinstance(data, list) and len(data) > 0:
                return data[0]
            return None
            
        except requests.exceptions.Timeout:
            log.error(f"Gamma API timeout for slug: {slug}")
            return None
        except Exception as e:
            log.error(f"Discovery Error: {e}")
            return None

    def get_token_ids(self, slug: str) -> Dict[str, PWSMarket]:
        """
        Get all markets (YES/NO tokens) for a given Event Slug.
        Returns a dict mapping: token_id -> PWSMarket object
        """
        event = self.get_event_by_slug(slug)
        if not event:
            return {}
            
        markets_data = event.get("markets", [])
        results = {}
        
        for m in markets_data:
            question = m.get("question", "")
            condition_id = m.get("conditionId", "")
            slug_m = m.get("slug", "")
            
            # Parse clobTokenIds (JSON string or list)
            clob_ids = m.get("clobTokenIds", "[]")
            if isinstance(clob_ids, str):
                try:
                    clob_ids = json.loads(clob_ids)
                except (json.JSONDecodeError, ValueError):
                    clob_ids = []
            
            # Parse prices
            prices = m.get("outcomePrices", ["0", "0"])
            if isinstance(prices, str):
                try:
                    prices = json.loads(prices)
                except (json.JSONDecodeError, ValueError):
                    prices = ["0", "0"]
            
            if not clob_ids:
                continue
            
            # YES token (index 0)
            yes_id = clob_ids[0]
            yes_price = float(prices[0]) if len(prices) > 0 else 0.0
            
            results[yes_id] = PWSMarket(
                token_id=yes_id,
                condition_id=condition_id,
                question=question,
                slug=slug_m,
                outcome="YES",
                price=yes_price,
            )
            
            # NO token (index 1, if exists)
            if len(clob_ids) > 1:
                no_id = clob_ids[1]
                no_price = float(prices[1]) if len(prices) > 1 else 0.0
                
                results[no_id] = PWSMarket(
                    token_id=no_id,
                    condition_id=condition_id,
                    question=question,
                    slug=slug_m,
                    outcome="NO",
                    price=no_price,
                )
                    
        return results

    # ──────────────────────────────────────────
    # High-Level Discovery
    # ──────────────────────────────────────────

    def discover_city(self, city: str, target_date: str = None) -> Dict[str, PWSMarket]:
        """
        Discover all temperature contracts for a city and date.
        
        1. Builds the slug automatically
        2. Fetches token IDs from Gamma API
        3. Parses temperature buckets from questions
        4. Attaches city + date metadata
        5. Returns dict SORTED by temperature bucket
        
        Args:
            city: City key, e.g. "ankara"
            target_date: ISO date "2026-02-17" or None for today
        
        Returns:
            OrderedDict[token_id → PWSMarket] sorted by bucket temperature
        """
        if target_date is None:
            target_date = date.today().strftime("%Y-%m-%d")
            
        slug = build_slug(city, target_date)
        log.info(f"🔍 Discovering markets: {city} / {target_date} → slug: {slug}")
        
        markets = self.get_token_ids(slug)
        
        if not markets:
            log.warning(f"❌ No markets found for {city} on {target_date}")
            return {}
        
        # Enrich each market with bucket, city, date
        for token_id, market in markets.items():
            market.bucket = parse_temperature_bucket(market.question)
            market.city = city.lower()
            market.market_date = target_date
            
            if market.bucket:
                log.debug(f"   [{market.outcome}] {market.bucket.label:<10} → ...{token_id[-8:]}")
            else:
                log.warning(f"   ⚠️ Could not parse bucket: {market.question[:50]}")
        
        # Sort by bucket temperature (low→high), then outcome (YES first)
        sorted_markets = ODict(
            sorted(markets.items(), key=lambda item: item[1].sort_key)
        )
        
        log.info(f"✅ Found {len(sorted_markets)} tokens for {city} ({len(sorted_markets)//2} contracts)")
        return sorted_markets

    def discover_cities(self, cities: List[str], target_date: str = None) -> Dict[str, PWSMarket]:
        """
        Discover markets for multiple cities.
        
        Returns:
            Combined dict of all markets, grouped by city, sorted by bucket.
        """
        if target_date is None:
            target_date = date.today().strftime("%Y-%m-%d")
        
        all_markets = ODict()
        
        for city in cities:
            city_markets = self.discover_city(city, target_date)
            all_markets.update(city_markets)
            
        log.info(f"📊 Total: {len(all_markets)} tokens across {len(cities)} cities")
        return all_markets

    def search_active_events(self, keyword: str = "highest temperature") -> List[Dict]:
        """
        Search for active temperature events on Gamma API.
        Useful for discovering new cities or markets.
        """
        try:
            url = f"{self.base_url}/events"
            params = {
                "active": True,
                "limit": 50,
            }
            r = self.session.get(url, params=params, timeout=15)
            if r.status_code != 200:
                return []
            
            events = r.json()
            # Filter by keyword
            results = []
            for event in events:
                title = event.get("title", "").lower()
                if keyword.lower() in title:
                    results.append({
                        "id": event.get("id"),
                        "title": event.get("title"),
                        "slug": event.get("slug"),
                        "markets_count": len(event.get("markets", [])),
                    })
            
            return results
            
        except Exception as e:
            log.error(f"Search error: {e}")
            return []

    # ──────────────────────────────────────────
    # Display Helpers
    # ──────────────────────────────────────────

    @staticmethod
    def print_contracts(markets: Dict[str, PWSMarket], city: str = "", target_date: str = ""):
        """
        Print discovered contracts in formatted table.
        YES and NO are displayed side-by-side per bucket row.
        """
        if not markets:
            print("   ❌ No contracts found.")
            return
        
        # Detect city/date from first market if not provided
        first = next(iter(markets.values()))
        city = city or first.city or "?"
        target_date = target_date or first.market_date or "?"
        
        # Group by condition_id → pair YES + NO
        groups = {}
        for tid, m in markets.items():
            key = m.condition_id or m.question
            if key not in groups:
                groups[key] = {"bucket": m.bucket, "yes_price": None, "no_price": None}
            if m.outcome == "YES":
                groups[key]["yes_price"] = m.price
            else:
                groups[key]["no_price"] = m.price
        
        # Sort by bucket
        sorted_groups = sorted(
            groups.values(), 
            key=lambda g: g["bucket"].sort_key if g["bucket"] else 0
        )
        
        contract_count = len(sorted_groups)
        
        # Header with market identification
        print(f"\n📊 {city.upper()} — Max Temperature — {target_date}")
        print(f"   {contract_count} kontrat bulundu")
        print(f"┌{'─'*12}┬{'─'*12}┬{'─'*12}┐")
        print(f"│ {'Bucket':<10} │ {'YES Fiyat':>10} │ {'NO Fiyat':>10} │")
        print(f"├{'─'*12}┼{'─'*12}┼{'─'*12}┤")
        
        for g in sorted_groups:
            label = g["bucket"].label if g["bucket"] else "???"
            y = f"{g['yes_price']:.2f}" if g["yes_price"] is not None else "  -  "
            n = f"{g['no_price']:.2f}" if g["no_price"] is not None else "  -  "
            print(f"│ {label:<10} │ {y:>10} │ {n:>10} │")
        
        print(f"└{'─'*12}┴{'─'*12}┴{'─'*12}┘")


# ──────────────────────────────────────────────
# Quick Test
# ──────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    disco = PolymarketDiscovery()
    
    # Test slug builder
    slug = build_slug("ankara", "2026-02-17")
    print(f"🔗 Slug: {slug}")
    
    # Test city discovery
    markets = disco.discover_city("ankara")
    disco.print_contracts(markets, "ankara")
