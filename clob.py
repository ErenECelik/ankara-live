"""
Polymarket CLOB Order Client — Ankara Bot
==========================================

Görev: Bot'tan gelen sinyal → CLOB REST API → emir

Kullanım:
    from clob import ClobOrderClient

    client = ClobOrderClient.from_env()   # credentials.env'den yükle

    # Limit emir
    resp = await client.buy_yes(token_id="0x...", price=0.27, size=50)
    resp = await client.buy_no(token_id="0x...",  price=0.03, size=50)

DRY_RUN=true ise emirler sadece loglanır, gönderilmez.

Kimlik doğrulama akışı:
    1. Ethereum private key (L1) → eth_account
    2. CLOB API key (opsiyonel, türetilir veya direkt verilir)
    3. Her emir EIP-712 ile imzalanır → Polygon mainnet (chain_id=137)

credentials.env dosyası gerekli (bkz. credentials.env.example).
"""

import os
import logging
from typing import Optional

log = logging.getLogger("clob")

CLOB_HOST   = "https://clob.polymarket.com"
CHAIN_ID    = 137   # Polygon mainnet

# Minimum token sayısı — çok küçük emirleri engelle
MIN_ORDER_TOKENS = 1.0


class ClobOrderClient:
    """
    Polymarket CLOB emir istemcisi.

    Attributes:
        dry_run: True → emirleri gönderme, sadece logla.
        client: py_clob_client.ClobClient instance.
    """

    def __init__(self, private_key: str, api_key: str, api_secret: str,
                 api_passphrase: str, dry_run: bool = True, funder: str = None):
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds

        self.dry_run = dry_run
        sig_type = 2 if funder else 0  # 2 = browser proxy wallet (MetaMask→Polymarket), 0 = EOA
        self._client = ClobClient(
            host      = CLOB_HOST,
            chain_id  = CHAIN_ID,
            key       = private_key,
            creds     = ApiCreds(
                api_key        = api_key,
                api_secret     = api_secret,
                api_passphrase = api_passphrase,
            ),
            signature_type = sig_type,
            funder         = funder,
        )

        mode = "DRY-RUN" if dry_run else "LIVE"
        funder_info = f"  funder={funder[:10]}..." if funder else ""
        log.info(f"[CLOB] Başlatıldı — {mode}  signer={self._client.get_address()[:10]}...{funder_info}")

    # ── Factory ──────────────────────────────────────────────────────

    @classmethod
    def from_env(cls, env_file: str = "credentials.env") -> "ClobOrderClient":
        """
        credentials.env'den yükle.
        CLOB_FUNDER varsa proxy wallet modu (signature_type=1).
        DRY_RUN=true → emirler gönderilmez (varsayılan: true).
        """
        _load_env(env_file)

        private_key    = _require("CLOB_PRIVATE_KEY")
        api_key        = _require("CLOB_API_KEY")
        api_secret     = _require("CLOB_API_SECRET")
        api_passphrase = _require("CLOB_API_PASSPHRASE")
        funder         = os.getenv("CLOB_FUNDER") or None
        dry_run        = os.getenv("DRY_RUN", "true").lower() != "false"

        return cls(private_key, api_key, api_secret, api_passphrase,
                   dry_run=dry_run, funder=funder)

    # ── Emir verme ────────────────────────────────────────────────────

    async def buy_yes(self, token_id: str, usdc_amount: float = 50.0,
                      price: Optional[float] = None) -> Optional[dict]:
        """YES token satın al — market FAK emir, usdc_amount USDC harcar.
        price: pre-fetch fill_price geçilirse takerAmount buna göre hesaplanır;
               None ise builder kendi hesaplar (lowest_ask fallback riski var)."""
        return await self._place_market("BUY", token_id, usdc_amount, price)

    async def buy_no(self, token_id: str, usdc_amount: float = 50.0,
                     price: Optional[float] = None) -> Optional[dict]:
        """NO token satın al — market FAK emir, usdc_amount USDC harcar."""
        return await self._place_market("BUY", token_id, usdc_amount, price)

    async def buy_no_limit(self, token_id: str, usdc_amount: float,
                           price: float) -> Optional[dict]:
        """NO token limit BUY — GTC maker, kalan miktar için fallback."""
        return await self._place_limit(token_id, usdc_amount, price)

    async def buy_yes_limit(self, token_id: str, usdc_amount: float,
                            price: float) -> Optional[dict]:
        """YES token limit BUY — GTC maker, kalan miktar için fallback."""
        return await self._place_limit(token_id, usdc_amount, price)

    async def cancel(self, order_id: str) -> dict:
        """Emir iptal."""
        if self.dry_run:
            log.info(f"[CLOB][DRY] cancel  order_id={order_id}")
            return {"dry_run": True}

        resp = self._client.cancel({"orderID": order_id})
        log.info(f"[CLOB] cancel  order_id={order_id}  resp={resp}")
        return resp

    # ── Internal ──────────────────────────────────────────────────────

    async def _place_limit(self, token_id: str, usdc_amount: float,
                           price: float) -> Optional[dict]:
        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY

        size_tokens = round(usdc_amount / price, 2)
        log.info(
            f"[CLOB]{'[DRY]' if self.dry_run else ''} "
            f"LIMIT BUY token={token_id[-8:]}  size={size_tokens:.2f} tokens  price={price:.4f}"
        )

        if self.dry_run:
            return {"dry_run": True, "token_id": token_id,
                    "size": size_tokens, "price": price}

        oa = OrderArgs(token_id=token_id, price=price, size=size_tokens, side=BUY)
        try:
            signed = self._client.create_order(oa)
            resp   = self._client.post_order(signed, OrderType.GTC)
            log.info(f"[CLOB] Limit emir kabul edildi: {resp}")
            return resp
        except Exception as e:
            log.error(f"[CLOB] Limit emir hatası: {e}")
            return {"error": str(e)}

    async def _place_market(self, side: str, token_id: str,
                            usdc_amount: float,
                            price: Optional[float] = None) -> Optional[dict]:
        from py_clob_client.clob_types import MarketOrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY

        price_str = f"  price={price:.4f}" if price else ""
        log.info(
            f"[CLOB]{'[DRY]' if self.dry_run else ''} "
            f"MARKET {side} token={token_id[-8:]}  amount={usdc_amount:.2f} USDC{price_str}"
        )

        if self.dry_run:
            return {
                "dry_run":    True,
                "side":       side,
                "token_id":   token_id,
                "usdc_amount": usdc_amount,
            }

        mo = MarketOrderArgs(
            token_id   = token_id,
            amount     = usdc_amount,
            price      = price,   # None ise builder kendi hesaplar
            side       = BUY,
            order_type = OrderType.FAK,
        )

        try:
            signed = self._client.create_market_order(mo)
            resp   = self._client.post_order(signed, OrderType.FAK)
            log.info(f"[CLOB] Emir kabul edildi: {resp}")
            return resp
        except Exception as e:
            log.error(f"[CLOB] Emir hatası: {e}")
            return {"error": str(e)}

    # ── Yardımcı ─────────────────────────────────────────────────────

    def get_balance(self) -> dict:
        """USDC bakiye + allowance."""
        from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
        return self._client.get_balance_allowance(
            params=BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        )

    def get_open_orders(self) -> list:
        """Açık emirleri listele."""
        return self._client.get_orders()

    def warm_cache(self, token_id: str):
        """
        tick-size / neg-risk / fee-rate cache'ini ısıt.
        create_market_order() bu üçünü API'den çeker; cache soğuksa emir anında ~450ms gecikme.
        (book her seferinde çekilir — dinamik, önbelleğe alınamaz)
        """
        if self.dry_run:
            return
        try:
            self._client.get_tick_size(token_id)
            self._client.get_neg_risk(token_id)
            self._client.get_fee_rate_bps(token_id)
        except Exception as e:
            log.debug(f"[CLOB] warm_cache {token_id[-8:]}: {e}")


# ── Kimlik bilgisi türetme (ilk kurulum) ──────────────────────────────

def derive_api_key(private_key: str) -> dict:
    """
    L1 private key'den CLOB API key türet.
    İLK KURULUMDA BİR KEZ çalıştır, çıktıyı credentials.env'e yaz.

    Kullanım:
        python -c "from clob import derive_api_key; derive_api_key('0xYOUR_KEY')"
    """
    from py_clob_client.client import ClobClient

    client = ClobClient(host=CLOB_HOST, chain_id=CHAIN_ID, key=private_key)
    creds  = client.derive_api_key()

    print("\n=== CLOB API Credentials ===")
    print(f"CLOB_PRIVATE_KEY    = {private_key}")
    print(f"CLOB_API_KEY        = {creds.api_key}")
    print(f"CLOB_API_SECRET     = {creds.api_secret}")
    print(f"CLOB_API_PASSPHRASE = {creds.api_passphrase}")
    print("\nBunları credentials.env dosyasına kopyala.")
    return {"api_key": creds.api_key, "api_secret": creds.api_secret,
            "api_passphrase": creds.api_passphrase}


# ── Helpers ───────────────────────────────────────────────────────────

def _load_env(path: str):
    """Basit .env yükleyici (python-dotenv gerektirmez)."""
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"credentials.env bulunamadı: {path}\n"
            "credentials.env.example'dan kopyala ve doldur."
        )
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                os.environ.setdefault(key.strip(), val.strip())


def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise ValueError(f"credentials.env içinde {key} eksik veya boş.")
    return val
