"""
PWS REST Module
CLOB REST API client for orderbook snapshots and price queries.
Used as fallback when WebSocket is unavailable, or for initial snapshots.

Endpoints:
    GET /book?token_id=X     → Full orderbook
    GET /price?token_id=X    → Current price
    GET /midpoint?token_id=X → Midpoint price
"""
import requests
import logging
from typing import Optional, Dict
from datetime import datetime, timezone

from .models import PWSOrderbook, PWSOrderbookLevel

CLOB_API_URL = "https://clob.polymarket.com"

log = logging.getLogger("pws.rest")


class PolymarketREST:
    """CLOB REST API client — synchronous, for snapshots and fallback."""
    
    def __init__(self, base_url: str = CLOB_API_URL):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "PWS-REST/1.0",
            "Accept": "application/json",
        })
    
    def get_book(self, token_id: str) -> Optional[PWSOrderbook]:
        """
        Fetch full orderbook for a token via REST.
        
        GET /book?token_id=<TOKEN_ID>
        
        Returns PWSOrderbook with all bid/ask levels.
        """
        try:
            url = f"{self.base_url}/book"
            params = {"token_id": token_id}
            
            r = self.session.get(url, params=params, timeout=10)
            if r.status_code != 200:
                log.warning(f"REST book error {r.status_code} for ...{token_id[-8:]}")
                return None
            
            data = r.json()
            
            bids = [
                PWSOrderbookLevel(price=float(b["price"]), size=float(b["size"]))
                for b in data.get("bids", [])
            ]
            asks = [
                PWSOrderbookLevel(price=float(a["price"]), size=float(a["size"]))
                for a in data.get("asks", [])
            ]
            
            return PWSOrderbook(
                market_id=token_id,
                bids=bids,
                asks=asks,
                timestamp=datetime.now(timezone.utc),
                hash=data.get("hash"),
            )
            
        except Exception as e:
            log.error(f"REST book error: {e}")
            return None
    
    def get_price(self, token_id: str) -> Optional[float]:
        """
        Get current price for a token.
        
        GET /price?token_id=<TOKEN_ID>
        """
        try:
            url = f"{self.base_url}/price"
            params = {"token_id": token_id}
            
            r = self.session.get(url, params=params, timeout=10)
            if r.status_code != 200:
                return None
            
            data = r.json()
            return float(data.get("price", 0))
            
        except Exception as e:
            log.error(f"REST price error: {e}")
            return None
    
    def get_midpoint(self, token_id: str) -> Optional[float]:
        """
        Get midpoint price for a token.
        
        GET /midpoint?token_id=<TOKEN_ID>
        """
        try:
            url = f"{self.base_url}/midpoint"
            params = {"token_id": token_id}
            
            r = self.session.get(url, params=params, timeout=10)
            if r.status_code != 200:
                return None
            
            data = r.json()
            return float(data.get("mid", 0))
            
        except Exception as e:
            log.error(f"REST midpoint error: {e}")
            return None
    
    def get_books_bulk(self, token_ids: list) -> Dict[str, PWSOrderbook]:
        """
        Fetch orderbooks for multiple tokens.
        Returns dict: token_id → PWSOrderbook
        """
        results = {}
        for tid in token_ids:
            book = self.get_book(tid)
            if book:
                results[tid] = book
        return results
