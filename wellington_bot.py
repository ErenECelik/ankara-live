"""
Wellington Dead Bucket Bot — Gözlem + Emir Modu

Veri kaynağı : MetService AWS 93439 (Wellington Aero)
Polymarket   : WebSocket + discovery (infra/pws)
Emir         : sadece METSERVICE tetikleyicisinde, HIGH confidence

Kayıtlar (logs/wellington_signals/):
  {date}_obs.jsonl          — Her MetService güncellemesinde tam snapshot
  {date}_price_changes.jsonl— Ham WebSocket price_change akışı
  {date}_trades.jsonl       — Ham WebSocket trade akışı
"""

import asyncio
import logging
import argparse
import json
import sys
import time
import fcntl
import aiohttp
from datetime import datetime, timezone, date, timedelta
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple
from pathlib import Path

from infra.pws import MarketManager
from infra.pws.models import PWSMarket, PWSOrderbook, PWSPriceChange, PWSTrade

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(message)s",
)
log = logging.getLogger("wellington_bot")

# ── Sabitler ───────────────────────────────────────────────────────
CITY               = "wellington"
METSERVICE_STATION = 93439
WELLINGTON_TZ_OFF  = 13   # UTC+13 (NZDT)

METSERVICE_URL = (
    "https://www.metservice.com/publicData/webdata/module/"
    "weatherStationCurrentConditions/93439"
)
METSERVICE_HEADERS = {
    "User-Agent":    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":        "application/json, text/plain, */*",
    "Cache-Control": "no-cache, no-store, must-revalidate",
    "Pragma":        "no-cache",
    "Referer":       "https://www.metservice.com/towns-cities/locations/wellington",
}

MAX_ORDER_USDC       = 10.0   # emir başına max harcama (test limiti)
MAX_ORDERS           = 2      # günlük max emir sayısı
MAX_NO_ASK_THRESHOLD = 0.95   # NO ask bu eşiğin üstündeyse getiri < %5 — atla
ENABLE_YES_ORDERS    = False  # YES emri: sadece type=higher LIVE (şimdilik kapalı)

# CDN burst polling (Varnish ~21s TTL — wellington_watch.py'den ölçülmüş)
CDN_TTL   = 21    # saniye — adaptif güncellenir
BURST_INT = 0.2   # saniye — burst zone poll aralığı
BURST_PRE = 3     # saniye — expire'dan kaç saniye önce burst başlasın

LOG_DIR = Path(__file__).resolve().parent / "logs" / "wellington_signals"


# ════════════════════════════════════════════════════════════════════
#   STATE
# ════════════════════════════════════════════════════════════════════

@dataclass
class BotState:
    # MetService gözlem verisi
    obs_temp: Optional[float]          = None   # son okunan sıcaklık
    prev_obs_temp: Optional[float]     = None   # bir önceki temp (değişim tespiti)
    daily_max: float                   = -999.0 # günün max (gözlenen)
    prev_daily_max: float              = -999.0
    last_obs_raw: str                  = ""     # son JSON response (string)
    last_obs_time: Optional[datetime]  = None
    obs_count: int                     = 0      # kaç kez değişim gözlemlendi

    # Ek gözlem alanları
    wind_dir: Optional[str]            = None
    wind_avg: Optional[int]            = None
    wind_gust: Optional[int]           = None
    humidity: Optional[int]            = None
    rainfall: Optional[float]          = None
    pressure: Optional[int]            = None

    # Zamanlama (ms precision — latency analizi için)
    last_fetch_start_ms: int           = 0
    last_fetch_end_ms: int             = 0
    last_detect_ms: int                = 0
    last_fetch_duration_ms: int        = 0

    # Last-Modified header — CDN cache bypass için gerçek değişim tespiti
    last_modified: str                 = ""

    # Bot sayaçları
    snapshot_count: int                = 0

    # Emir takibi — aynı token için iki kez emir atma
    ordered_token_ids: set             = field(default_factory=set)

    # Sinyal kaydı dedup — signal_books'a her token bir kez yazılır
    signaled_token_ids: set            = field(default_factory=set)

    # Açık pozisyon takibi (sell high Phase 2 için)
    # {token_id, side, size, entry_price, entry_time_utc}
    open_positions: List[dict]         = field(default_factory=list)

    # Bakiye önbelleği — emir anında REST çağrısı yapma
    cached_balance_usdc: float         = 0.0

    # REST pre-fetch: METSERVICE gelmeden fill_price hazır olsun — emir anında sıfır gecikme
    prefetched_order_params: dict      = field(default_factory=dict)

    # METAR pencere takibi — "xx:00 / xx:30 sonraki ilk değişim = resmi METAR"
    # metar_window: hangi pencerede olduğumuz ("h00" | "h30" | "")
    # metar_confirmed: bu pencerede ilk Last-Modified değişimi görüldü mü
    metar_window: str                  = ""
    metar_confirmed: bool              = False


# ════════════════════════════════════════════════════════════════════
#   METSERVICE FETCH
# ════════════════════════════════════════════════════════════════════

def _in_window(now_utc: datetime) -> bool:
    m = now_utc.minute
    return m >= 58 or m <= 5 or (28 <= m <= 35)


def _is_post_metar_zone(now_utc: datetime) -> bool:
    """
    True ise METAR yayınlanmış olabilir — Last-Modified değişimi emir tetikler.
    False ise pre-METAR bölgesindeyiz (:58/:59 veya :28/:29) — veri önceki periyottan.
    """
    m = now_utc.minute
    return m <= 5 or 30 <= m <= 35


def _secs_to_next_window(now_utc: datetime) -> float:
    elapsed = now_utc.minute * 60 + now_utc.second
    for start in (28 * 60, 58 * 60):
        if elapsed < start:
            return start - elapsed
    return (3600 - elapsed) + 28 * 60


async def _metservice_fetch(session: aiohttp.ClientSession) -> Tuple[dict, int, int]:
    """
    (parsed_data, fetch_start_ms, fetch_end_ms) döndürür.
    Persistent aiohttp session kullanır — TCP handshake her request'te tekrarlanmaz.
    parsed_data: {..., _last_modified, _age}
      _age: CDN Varnish yaşı (saniye int) veya None (taze / header yok)
    """
    fetch_start_ms = int(time.time() * 1000)
    async with session.get(METSERVICE_URL, timeout=aiohttp.ClientTimeout(total=5)) as r:
        last_modified = r.headers.get("Last-Modified", "")
        age_raw       = r.headers.get("Age", "")
        age           = int(age_raw) if age_raw and age_raw.isdigit() else None
        raw           = await r.read()
    fetch_end_ms = int(time.time() * 1000)

    d   = json.loads(raw)
    obs = d.get("observations", {})
    return {
        "temp_c":          obs.get("temperature", [{}])[0].get("current"),
        "feels_c":         obs.get("temperature", [{}])[0].get("feelsLike"),
        "wind_dir":        obs.get("wind", [{}])[0].get("direction"),
        "wind_avg":        obs.get("wind", [{}])[0].get("averageSpeed"),
        "wind_gust":       obs.get("wind", [{}])[0].get("gustSpeed"),
        "humidity":        obs.get("rain", [{}])[0].get("relativeHumidity"),
        "rainfall":        obs.get("rain", [{}])[0].get("rainfall"),
        "pressure":        obs.get("pressure", [{}])[0].get("atSeaLevel"),
        "_raw":            raw.decode(),
        "_last_modified":  last_modified,
        "_age":            age,
    }, fetch_start_ms, fetch_end_ms


# ════════════════════════════════════════════════════════════════════
#   BUCKET HELPERS  (Ankara bot.py'den aynı — generic)
# ════════════════════════════════════════════════════════════════════

def _bucket_max(b) -> Optional[float]:
    """
    Bucket'ın DEAD eşiği: bu değerin altında kalan daily_max bucket'ın hâlâ LIVE olduğunu gösterir.
    MetService 0.1°C hassasiyetinde raporlar; Polymarket tam sayıya yuvarlar.

    Kural: b.high = N → market rounds-to-N durumunu kapsar → raw max < N+0.5 iken bucket yaşar.
    Örn:
      exact  14°C  → [13.5, 14.5) → DEAD eşik = 14.5
      range  13-14 → [12.5, 14.5) → DEAD eşik = 14.5  (b.high+0.5)
      below  ≤14°C → (-∞,  14.5) → DEAD eşik = 14.5  (b.high+0.5)
      higher ≥15°C → [14.5, +∞)  → DEAD eşik yok (None)
    """
    if b.type == "exact":
        return float(b.low) + 0.5 if b.low is not None else None
    elif b.type == "range":
        return float(b.high) + 0.5 if b.high is not None else None
    elif b.type == "below":
        return float(b.high) + 0.5 if b.high is not None else None
    return None  # "higher" → no upper bound


def _bucket_min(b) -> Optional[float]:
    if b.type in ("range", "exact"):
        return float(b.low) if b.low is not None else None
    elif b.type == "higher":
        return float(b.low) if b.low is not None else None
    elif b.type == "below":
        return None
    return None


def _bucket_contains(b, temp: float) -> bool:
    """
    LIVE tespiti: bu temp değeri, bu bucket'ın kazanabileceği aralığa giriyor mu?
    Tüm sınırlar +0.5 yuvarlama mantığıyla (bkz. _bucket_max).
      exact  14°C  → [13.5, 14.5)
      range  13-14 → [12.5, 14.5)  (lo - 0.5, hi + 0.5)
      below  ≤14°C → (-∞,  14.5)
      higher ≥15°C → [14.5, +∞)
    """
    if b.type == "higher":
        lo = float(b.low) - 0.5 if b.low is not None else float("-inf")
        return temp >= lo
    elif b.type == "below":
        hi = float(b.high) + 0.5 if b.high is not None else float("inf")
        return temp < hi
    elif b.type == "exact":
        lo = float(b.low) - 0.5 if b.low is not None else float("-inf")
        hi = float(b.low) + 0.5 if b.low is not None else float("inf")
        return lo <= temp < hi
    elif b.type == "range":
        lo = float(b.low) - 0.5 if b.low is not None else float("-inf")
        hi = float(b.high) + 0.5 if b.high is not None else float("inf")
        return lo <= temp < hi
    return False


def _calc_avg_fill(asks: list, size_usdc: float) -> Tuple[Optional[float], str, Optional[float], float]:
    """
    Ask ladderını yürüterek sweep dolum fiyatını hesapla.

    asks      : [PWSOrderbookLevel] — fiyata göre ASC sıralı
    size_usdc : harcamak istediğimiz USDC miktarı

    Döndürür: (avg_price_per_token, depth_summary_str, fill_price, total_tokens)
      fill_price   : en yüksek fiyat seviyesi → bu fiyattan limit gir, tüm seviyeleri sweep et
      total_tokens : bu fiyata kadar alınacak toplam token sayısı
    """
    remaining    = size_usdc
    total_tokens = 0.0
    levels_used  = []
    fill_price   = None

    for lvl in asks:
        fill_price      = lvl.price
        available_usdc  = lvl.price * lvl.size
        if available_usdc >= remaining:
            tokens = remaining / lvl.price
            total_tokens += tokens
            levels_used.append(f"{lvl.price:.3f}×{tokens:.1f}")
            remaining = 0.0
            break
        total_tokens += lvl.size
        levels_used.append(f"{lvl.price:.3f}×{lvl.size:.1f}")
        remaining -= available_usdc

    if total_tokens == 0:
        return None, "liquidity yok", None, 0.0

    filled_usdc = size_usdc - remaining
    avg    = filled_usdc / total_tokens
    suffix = "  [LİKİDİTE YETERSİZ]" if remaining > 0.01 else ""
    summary = " | ".join(levels_used[:6]) + suffix
    return avg, summary, fill_price, total_tokens


def _categorize_bucket(b, daily_max: float) -> str:
    if daily_max <= -999:
        return "ABOVE"
    bmax = _bucket_max(b)
    if bmax is not None and daily_max >= bmax:
        return "DEAD"
    if _bucket_contains(b, daily_max):
        return "LIVE"
    return "ABOVE"


def _serialize_book(book: Optional[PWSOrderbook], snapshot_ms: int = 0, full: bool = False) -> dict:
    if book is None:
        return {"bid": None, "ask": None, "mid": None, "levels": [], "book_age_ms": None}
    bid = book.best_bid
    ask = book.best_ask
    mid = round((bid + ask) / 2, 4) if bid is not None and ask is not None else None
    age = (snapshot_ms - int(book.timestamp.timestamp() * 1000)) if snapshot_ms and book.timestamp else None
    bids_list = book.bids if full else book.bids[:5]
    asks_list = book.asks if full else book.asks[:5]
    levels = (
        [{"price": lv.price, "size": lv.size, "side": "BUY"}  for lv in bids_list] +
        [{"price": lv.price, "size": lv.size, "side": "SELL"} for lv in asks_list]
    )
    result = {"bid": bid, "ask": ask, "mid": mid, "levels": levels, "book_age_ms": age}
    if full:
        result["bid_depth"]    = round(book.bid_depth, 2)
        result["ask_depth"]    = round(book.ask_depth, 2)
        result["n_bid_levels"] = len(book.bids)
        result["n_ask_levels"] = len(book.asks)
    return result


def build_bucket_snapshot(
    all_markets: Dict[str, PWSMarket],
    manager: MarketManager,
    daily_max: float,
    snapshot_ms: int = 0,
) -> List[dict]:
    groups: Dict[str, dict] = {}
    for tid, m in all_markets.items():
        if not m.bucket:
            continue
        cid = m.condition_id
        if cid not in groups:
            groups[cid] = {"bucket": m.bucket, "yes_market": None, "no_market": None}
        if m.outcome == "YES":
            groups[cid]["yes_market"] = m
        else:
            groups[cid]["no_market"] = m

    buckets = []
    for cid, g in sorted(groups.items(), key=lambda x: x[1]["bucket"].sort_key):
        b     = g["bucket"]
        yes_m = g["yes_market"]
        no_m  = g["no_market"]

        yes_book = manager.get_orderbook(yes_m.token_id) if yes_m else None
        no_book  = manager.get_orderbook(no_m.token_id)  if no_m  else None

        yes_data = _serialize_book(yes_book, snapshot_ms)
        no_data  = _serialize_book(no_book,  snapshot_ms)
        category = _categorize_bucket(b, daily_max)

        buckets.append({
            "label":        b.label,
            "type":         b.type,
            "low":          b.low,
            "high":         b.high,
            "sort_key":     b.sort_key,
            "category":     category,
            "condition_id": cid,
            "yes": {"token_id": yes_m.token_id if yes_m else None, **yes_data},
            "no":  {"token_id": no_m.token_id  if no_m  else None, **no_data},
            "yes_mid": yes_data["mid"],
            "no_mid":  no_data["mid"],
        })
    return buckets


def detect_adjacent_tensions(bucket_snapshots: List[dict], threshold: float = 0.75) -> List[dict]:
    tensions = []
    live_above = [b for b in bucket_snapshots if b["category"] in ("LIVE", "ABOVE")]
    for i in range(len(live_above) - 1):
        lo = live_above[i]
        hi = live_above[i + 1]
        lo_mid = lo["yes_mid"]
        hi_mid = hi["yes_mid"]
        if lo_mid is None or hi_mid is None:
            continue
        yes_sum = lo_mid + hi_mid
        if yes_sum >= threshold:
            tensions.append({
                "lower_label": lo["label"],
                "upper_label": hi["label"],
                "yes_sum":     round(yes_sum, 4),
                "trade_hint":  f"Market bu ikisi arasında — sonraki veri birini öne cikarir",
            })
    return tensions


# ════════════════════════════════════════════════════════════════════
#   SIGNAL
# ════════════════════════════════════════════════════════════════════

@dataclass
class Signal:
    action: str         # "BUY_YES" | "BUY_NO"
    bucket_label: str
    token_id: str
    side: str           # "YES" | "NO"
    current_ask: Optional[float]
    current_bid: Optional[float]
    confidence: str     # "HIGH" | "MEDIUM"
    reason: str


def evaluate_signals(bucket_snapshots: List[dict], state: BotState) -> List[Signal]:
    """
    Garanti kazanç sinyalleri — sadece METSERVICE verisi.

    HIGH (emir tetiklenir):
      A) DEAD bucket      → BUY_NO  (daily_max bu bucket'ın üst sınırını geçti, kesin NO)
      B) LIVE type=higher → BUY_YES (daily_max üst sınırsız bucketa ulaştı, kesin YES)

    MEDIUM (gözlem, emir yok):
      - Diğer tüm LIVE bucket'lar (soğuma dahil — zirve henüz kesin değil)
    """
    signals = []
    max_t = state.daily_max

    for b in bucket_snapshots:
        if b["category"] == "DEAD":
            # A) Kesin ölü → BUY_NO HIGH
            no_ask = b["no"]["ask"]
            no_bid = b["no"]["bid"]
            if no_ask is not None and no_ask > 0.01:
                # "below" için b["low"] yok, b["high"] kullan; diğerleri için b["high"] veya b["low"]
                _thresh_base = b["high"] if b["type"] == "below" else b["high"] if b["type"] == "range" else b["low"]
                dead_threshold = float(_thresh_base) + 0.5 if _thresh_base is not None else "?"
                signals.append(Signal(
                    action       = "BUY_NO",
                    bucket_label = b["label"],
                    token_id     = b["no"]["token_id"] or "",
                    side         = "NO",
                    current_ask  = no_ask,
                    current_bid  = no_bid,
                    confidence   = "HIGH",
                    reason       = f"Kesin olu: daily_max={max_t}°C >= {dead_threshold}°C (dead threshold)",
                ))

        elif b["category"] == "LIVE":
            yes_ask = b["yes"]["ask"]
            yes_bid = b["yes"]["bid"]

            if b["type"] == "higher":
                # B) Üst sınırsız bucket LIVE → kesin YES
                confidence = "HIGH"
                reason = (
                    f"METSERVICE garantisi: daily_max={max_t}°C >= {float(b['low']) - 0.5:.1f}°C"
                    f" | ust sinırsız bucket — kesin YES"
                )
            else:
                # Exact/range/below LIVE — zirve henüz kesin değil, izle
                confidence = "MEDIUM"
                reason = (
                    f"LIVE (izleniyor): daily_max={max_t}°C  bucket={b['label']}  "
                    f"metar_window={state.metar_window or 'yok'}"
                )

            if yes_ask is not None:
                signals.append(Signal(
                    action       = "BUY_YES",
                    bucket_label = b["label"],
                    token_id     = b["yes"]["token_id"] or "",
                    side         = "YES",
                    current_ask  = yes_ask,
                    current_bid  = yes_bid,
                    confidence   = confidence,
                    reason       = reason,
                ))

    return signals


# ════════════════════════════════════════════════════════════════════
#   DISPLAY
# ════════════════════════════════════════════════════════════════════

CONF_ICON = {"HIGH": ">>>", "MEDIUM": "---"}


def print_obs_snapshot(
    state: BotState,
    bucket_snapshots: List[dict],
    tensions: List[dict],
    signals: List[Signal],
    source: str,
    is_new_max: bool,
):
    now_l = datetime.now().strftime("%H:%M:%S")
    now_u = datetime.now(timezone.utc).strftime("%H:%M:%S")
    new_max_flag = "  <<< YENI MAX >>>" if is_new_max else ""

    src_label = "MetService AWS" if source == "METSERVICE" else source

    print("\n" + "=" * 72)
    print(f"  [{src_label}]  {now_l} local / {now_u} UTC{new_max_flag}")
    print(
        f"  Sicaklik : {state.obs_temp}°C  |  Gunun maks: {state.daily_max}°C  (#{state.obs_count})"
    )
    if state.wind_avg is not None:
        print(
            f"  Ruzgar   : {state.wind_avg} km/h {state.wind_dir}  "
            f"(gust {state.wind_gust} km/h)  nem=%{state.humidity}"
        )

    print("-" * 72)
    print(
        f"  {'Bucket':<12} {'Durum':<8}  {'YES ask':>7} {'YES bid':>7}  "
        f"{'NO ask':>6} {'NO bid':>6}  {'Getiri(YES)':>11}"
    )
    print("  " + "-" * 68)

    for b in bucket_snapshots:
        cat       = b["category"]
        cat_label = {"DEAD": "OLDU", "LIVE": "CANLI", "ABOVE": "bekle"}.get(cat, cat)
        yes       = b["yes"]
        no        = b["no"]

        def fmt(v): return f"{v:.3f}" if v is not None else "  -- "

        ret_str = ""
        if yes["ask"] and yes["ask"] > 0:
            ret_str = f"{(1.0/yes['ask']-1)*100:+.0f}%"

        marker = " <-- BURADA" if cat == "LIVE" else (" (bitti)" if cat == "DEAD" else "")

        print(
            f"  {b['label']:<12} {cat_label:<8}  "
            f"{fmt(yes['ask']):>7} {fmt(yes['bid']):>7}  "
            f"{fmt(no['ask']):>6} {fmt(no['bid']):>6}  "
            f"{ret_str:>11}{marker}"
        )

    print("-" * 72)
    high_sigs  = [s for s in signals if s.confidence == "HIGH"]
    other_sigs = [s for s in signals if s.confidence != "HIGH"]

    if source == "METSERVICE" and high_sigs:
        print("  EMIRLER (HIGH — tetiklenecek):")
        for s in high_sigs:
            ask_str = f"{s.current_ask:.3f}" if s.current_ask else "?"
            ret = (1.0/s.current_ask - 1)*100 if s.current_ask and s.current_ask > 0 else 0
            print(f"    >>> {s.action:<8} {s.bucket_label:<14}  ask={ask_str}  getiri={ret:+.0f}%")
            print(f"        {s.reason}")
    else:
        print("  EMIRLER: yok (sinyal bekleniyor)")

    if other_sigs:
        print("  Bilgi (emir yok):")
        for s in other_sigs:
            ask_str = f"{s.current_ask:.3f}" if s.current_ask else "?"
            print(f"    --- {s.confidence:<6} {s.action:<8} {s.bucket_label:<14}  ask={ask_str}  |  {s.reason}")

    if tensions:
        print("-" * 72)
        print("  GERILIM:")
        for t in tensions:
            print(f"    [{t['lower_label']} + {t['upper_label']}]  YES={t['yes_sum']:.3f}  {t['trade_hint']}")

    print("=" * 72)
    sys.stdout.flush()


# ════════════════════════════════════════════════════════════════════
#   KAYIT
# ════════════════════════════════════════════════════════════════════

def save_obs_snapshot(state: BotState, date_str: str, is_new_max: bool):
    """Sadece temp değiştiğinde çağrılır — minimal format."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    path = LOG_DIR / f"{date_str}_obs.jsonl"
    now_utc = datetime.now(timezone.utc)
    record = {
        "ts_utc":          now_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "temp_c":          state.obs_temp,
        "prev_temp":       state.prev_obs_temp,
        "daily_max":       state.daily_max,
        "is_new_max":      is_new_max,
        "metar_window":    state.metar_window,
        "metar_confirmed": state.metar_confirmed,
        "fetch_ms":        state.last_fetch_duration_ms,
        "obs_count":       state.obs_count,
    }
    with open(path, "a") as f:
        f.write(json.dumps(record) + "\n")


def save_signal_orderbook(
    signals: List[Signal],
    all_markets: Dict[str, PWSMarket],
    manager,
    date_str: str,
    snapshot_ms: int,
    state: "BotState",
):
    """
    HIGH sinyal oluştuğunda ilgili YES + NO token'larının tam orderbook'unu kaydet.
    {date}_signal_books.jsonl → sabah analizi için kim nerede duruyordu
    """
    # Sadece ilk kez ateşlenen sinyalleri kaydet (signaled_token_ids ile dedup)
    high_signals = [
        s for s in signals
        if s.confidence == "HIGH" and s.token_id not in state.signaled_token_ids
    ]
    if not high_signals:
        return

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    path    = LOG_DIR / f"{date_str}_signal_books.jsonl"
    ts_utc  = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    for s in high_signals:
        # s.token_id = satın alınan tarafın token'ı
        # Her iki tarafın tam orderbook'unu kaydetmek için condition_id üzerinden ara
        signal_market = all_markets.get(s.token_id)
        if not signal_market:
            continue

        cond_id      = signal_market.condition_id
        yes_token_id = None
        no_token_id  = None
        for tid, m in all_markets.items():
            if m.condition_id == cond_id:
                if m.outcome == "YES":
                    yes_token_id = tid
                elif m.outcome == "NO":
                    no_token_id = tid

        yes_data = _serialize_book(manager.get_orderbook(yes_token_id) if yes_token_id else None, snapshot_ms, full=True)
        no_data  = _serialize_book(manager.get_orderbook(no_token_id)  if no_token_id  else None, snapshot_ms, full=True)

        record = {
            "ts_utc":      ts_utc,
            "snapshot_ms": snapshot_ms,
            "signal":      s.action,
            "bucket":      s.bucket_label,
            "buy_side":    s.side,           # "NO" veya "YES" — satın alınan taraf
            "confidence":  s.confidence,
            "reason":      s.reason,
            "daily_max":   state.daily_max,
            "obs_temp":    state.obs_temp,
            "metar_window":    state.metar_window,
            "metar_confirmed": state.metar_confirmed,
            "yes": {"token_id": yes_token_id, **yes_data},
            "no":  {"token_id": no_token_id,  **no_data},
        }
        state.signaled_token_ids.add(s.token_id)

        with open(path, "a") as f:
            f.write(json.dumps(record) + "\n")

        # Terminal: sinyal anında orderbook özeti
        def _depth_str(d: dict) -> str:
            bid = d.get("bid") or 0
            ask = d.get("ask") or 0
            bd  = d.get("bid_depth") or 0
            ad  = d.get("ask_depth") or 0
            nb  = d.get("n_bid_levels") or 0
            na  = d.get("n_ask_levels") or 0
            return (
                f"bid={bid:.3f}({bd:.0f}$ {nb}lv)  "
                f"ask={ask:.3f}({ad:.0f}$ {na}lv)"
            )
        log.info(
            f"[SIGBOOK] {s.bucket_label}  {s.action}  "
            f"YES: {_depth_str(yes_data)}  ||  NO: {_depth_str(no_data)}"
        )


def save_price_change(change: PWSPriceChange, date_str: str, market: "PWSMarket | None" = None):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    path = LOG_DIR / f"{date_str}_price_changes.jsonl"
    record = {
        "ts_utc":   change.timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ts_ms":    int(change.timestamp.timestamp() * 1000),
        "token_id": change.asset_id,
        "price":    change.price,
        "size":     change.size,
        "side":     change.side,
        "best_bid": change.best_bid,
        "best_ask": change.best_ask,
        "mid":      change.mid_price,
        # Bucket bağlamı — hangi market/outcome olduğunu analiz için
        "bucket":   market.bucket.label if market and market.bucket else None,
        "outcome":  market.outcome if market else None,
        "city":     market.city if market else None,
    }
    with open(path, "a") as f:
        f.write(json.dumps(record) + "\n")


def save_trade(trade: PWSTrade, date_str: str, market: "PWSMarket | None" = None):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    path = LOG_DIR / f"{date_str}_trades.jsonl"
    bucket  = market.bucket.label if market and market.bucket else "?"
    outcome = market.outcome       if market else "?"
    record = {
        "ts_utc":    trade.timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ts_ms":     int(trade.timestamp.timestamp() * 1000),
        "token_id":  trade.market_id,
        "price":     trade.price,
        "size":      trade.size,
        "side":      trade.side,
        "fee_rate":  trade.fee_rate,
        "bucket":    bucket,
        "outcome":   outcome,
        "city":      market.city if market else None,
    }
    with open(path, "a") as f:
        f.write(json.dumps(record) + "\n")
    # Terminal: her trade görünür
    log.info(
        f"[TRADE] {bucket} {outcome:<3}  {trade.side:<4}  "
        f"price={trade.price:.4f}  size={trade.size:.1f} USDC  "
        f"token=...{trade.market_id[-8:]}"
    )


# ════════════════════════════════════════════════════════════════════
#   CORE EVALUATION
# ════════════════════════════════════════════════════════════════════

async def run_evaluation(
    state: BotState,
    manager: MarketManager,
    source: str,
    date_str: str,
    is_new_max: bool = False,
    clob_client=None,
    temp_changed: bool = False,
):
    all_markets = manager.get_markets_by_city(CITY)
    if not all_markets:
        log.warning("[EVAL] Market yok — atlanıyor")
        return

    snapshot_ms = int(time.time() * 1000)

    bucket_snaps = build_bucket_snapshot(
        all_markets, manager,
        daily_max   = state.daily_max,
        snapshot_ms = snapshot_ms,
    )
    tensions = detect_adjacent_tensions(bucket_snaps)
    signals  = evaluate_signals(bucket_snaps, state)

    state.snapshot_count += 1

    # Emir — SADECE METSERVICE tetikleyicisinde, ÖNCE atılır — print/save gecikme yaratmaz
    if clob_client is not None and source == "METSERVICE":
        if len(state.ordered_token_ids) >= MAX_ORDERS:
            log.info(f"[ORDER] Max emir sayısına ulaşıldı ({MAX_ORDERS}) — atlanıyor")
        else:
            eligible = [
                s for s in signals
                if s.confidence == "HIGH"
                and s.token_id
                and s.token_id not in state.ordered_token_ids
                and s.current_ask is not None
                and not (s.action == "BUY_NO" and s.current_ask > MAX_NO_ASK_THRESHOLD)
                and (ENABLE_YES_ORDERS or s.action == "BUY_NO")
            ]
            # En iyi getiri: en düşük ask → en yüksek %
            if len(eligible) > 1:
                eligible = sorted(eligible, key=lambda s: s.current_ask)[:1]
                log.info(f"[ORDER] En iyi getiri: {eligible[0].action} ask={eligible[0].current_ask:.3f}")

            if eligible:
                balance_usdc = state.cached_balance_usdc
                size_usdc    = max(5.0, min(MAX_ORDER_USDC, round(balance_usdc, 2)))
                log.info(f"[ORDER] {size_usdc:.2f} USDC  (önbellek bakiye={balance_usdc:.2f})")

                for s in eligible:
                    # Pre-fetch verisi varsa kullan — sıfır ek gecikme
                    prefetched = state.prefetched_order_params.get(s.token_id)
                    if prefetched:
                        order_price = prefetched["fill_price"]
                        size_tokens = prefetched["total_tokens"]
                        age_ms      = int(time.time() * 1000) - prefetched["fetch_ms"]
                        log.info(
                            f"[ORDER] Pre-fetch {age_ms}ms önce | "
                            f"depth: {prefetched['depth_summary']} | "
                            f"sweep={order_price:.4f}  {size_tokens:.1f} token  ({size_usdc:.1f} USDC)"
                        )
                    else:
                        # Fallback: WebSocket ask ile hesapla
                        order_price = s.current_ask
                        size_tokens = round(size_usdc / s.current_ask, 1)
                        log.warning(
                            f"[ORDER] Pre-fetch yok — WebSocket ask={order_price:.4f} kullanılıyor"
                        )

                    log.info(
                        f"[ORDER] {s.action}  bucket={s.bucket_label}  "
                        f"sweep={order_price:.4f}  {size_tokens:.0f} token  ({size_usdc:.1f} USDC)  "
                        f"token=...{s.token_id[-8:]}"
                    )
                    if s.action == "BUY_NO":
                        resp = await clob_client.buy_no(
                            token_id=s.token_id, price=order_price, size=size_tokens
                        )
                    else:
                        resp = await clob_client.buy_yes(
                            token_id=s.token_id, price=order_price, size=size_tokens
                        )

                    log.info(f"[ORDER] Yanit: {resp}")
                    if resp and "error" not in resp:
                        state.ordered_token_ids.add(s.token_id)
                        ordered_file = LOG_DIR / f"{date_str}_ordered.txt"
                        with open(ordered_file, "a") as _f:
                            _f.write(s.token_id + "\n")
                        state.open_positions.append({
                            "token_id":       s.token_id,
                            "side":           s.side,
                            "bucket":         s.bucket_label,
                            "size_usdc":      size_usdc,
                            "entry_price":    order_price,
                            "entry_time_utc": datetime.now(timezone.utc).isoformat(),
                        })
                        log.info(f"[ORDER] TAMAM — token kaydedildi, bir daha girilmeyecek")
                    else:
                        log.error(f"[ORDER] HATA — token kaydedilmedi, bir sonraki güncellemede tekrar denenecek")

    # Çıktı + kayıt — emrden SONRA (gecikme yaratmaz)
    if temp_changed:
        print_obs_snapshot(state, bucket_snaps, tensions, signals, source, is_new_max)
        save_obs_snapshot(state, date_str, is_new_max)
    save_signal_orderbook(signals, all_markets, manager, date_str, snapshot_ms, state)


# ════════════════════════════════════════════════════════════════════
#   ASYNC TASKS
# ════════════════════════════════════════════════════════════════════

async def metservice_poller(
    state: BotState, manager: MarketManager, date_str: str, clob_client=None
):
    log.info(f"[MET] Poller başladı — station={METSERVICE_STATION}")

    connector = aiohttp.TCPConnector(limit=1, ttl_dns_cache=300)
    session   = aiohttp.ClientSession(headers=METSERVICE_HEADERS, connector=connector)

    # İlk fetch
    while True:
        try:
            data, _, _ = await _metservice_fetch(session)
            temp = data["temp_c"]
            state.obs_temp      = temp
            state.wind_dir      = data["wind_dir"]
            state.wind_avg      = data["wind_avg"]
            state.wind_gust     = data["wind_gust"]
            state.humidity      = data["humidity"]
            state.rainfall      = data["rainfall"]
            state.pressure      = data["pressure"]
            state.last_obs_raw  = data["_raw"]
            state.last_modified = data["_last_modified"]
            if temp is not None:
                state.daily_max      = temp
                state.prev_daily_max = temp
            log.info(
                f"[MET] Başlangıç: temp={temp}°C  "
                f"wind={data['wind_avg']}km/h {data['wind_dir']}  "
                f"Last-Modified={data['_last_modified'][17:25] if data['_last_modified'] else 'yok'}"
            )
            break
        except Exception as e:
            log.warning(f"[MET] İlk fetch hatası: {e} — 3s")
            await asyncio.sleep(3)

    # Ana döngü — CDN Age-aware burst polling
    cdn_ttl           = CDN_TTL       # adaptif güncellenir
    cdn_refresh_times: list = []

    while True:
        now_utc = datetime.now(timezone.utc)

        # ── Pencere dışı: sonraki pencereye kadar uyu ─────────────────
        if not _in_window(now_utc):
            wait    = _secs_to_next_window(now_utc)
            next_dt = now_utc + timedelta(seconds=wait)
            log.info(
                f"[MET] Pencere dışı — sonraki: {next_dt.strftime('%H:%M:%S')} UTC ({int(wait)}s)"
            )
            # 5s erken uyan (geçiş anını kaçırma)
            await asyncio.sleep(max(0.0, wait - 5))
            continue

        # ── Pencere içi: METAR pencere takibi ─────────────────────────
        now_m = now_utc.minute
        cur_window = "h00" if (now_m >= 58 or now_m <= 5) else "h30"
        if cur_window != state.metar_window:
            state.metar_window    = cur_window
            state.metar_confirmed = False
            log.info(f"[MET] METAR penceresi: {cur_window} — onay bekleniyor")

        t0 = time.monotonic()
        age = None
        try:
            data, fetch_start_ms, fetch_end_ms = await _metservice_fetch(session)
            detect_ms = fetch_end_ms
            temp = data["temp_c"]
            lm   = data["_last_modified"]
            age  = data["_age"]

            # CDN TTL adaptif ölçüm (Age=None → CDN yeni refresh yaptı)
            if age is None:
                cdn_refresh_times.append(time.time())
                if len(cdn_refresh_times) >= 2:
                    observed = cdn_refresh_times[-1] - cdn_refresh_times[-2]
                    if 10 < observed < 60:
                        cdn_ttl = round(observed)

            age_str = f"{age}s" if age is not None else "FRESH"

            if lm and lm != state.last_modified:
                # Sunucu verisi gerçekten güncellendi (Last-Modified değişti)
                old_max    = state.daily_max
                old_temp   = state.obs_temp
                is_new_max = False

                state.last_fetch_start_ms    = fetch_start_ms
                state.last_fetch_end_ms      = fetch_end_ms
                state.last_detect_ms         = detect_ms
                state.last_fetch_duration_ms = fetch_end_ms - fetch_start_ms
                state.last_obs_time          = datetime.fromtimestamp(detect_ms / 1000, tz=timezone.utc)

                state.prev_daily_max = old_max
                state.prev_obs_temp  = old_temp
                state.obs_temp       = temp
                state.wind_dir       = data["wind_dir"]
                state.wind_avg       = data["wind_avg"]
                state.wind_gust      = data["wind_gust"]
                state.humidity       = data["humidity"]
                state.rainfall       = data["rainfall"]
                state.pressure       = data["pressure"]
                state.last_obs_raw   = data["_raw"]
                state.last_modified  = lm
                state.obs_count     += 1

                # METAR onayı — pencere içindeki ilk Last-Modified değişimi
                if state.metar_window and not state.metar_confirmed:
                    state.metar_confirmed = True
                    log.info(
                        f"[MET] *** METAR ONAYLANDI ({state.metar_window}) — "
                        f"temp={temp}°C  age={age_str}  data_time={lm[17:25]}"
                    )

                if temp is not None and temp > state.daily_max:
                    if state.metar_window:
                        state.daily_max = temp
                        is_new_max = True
                    else:
                        log.info(
                            f"[MET] Pencere dışı yüksek temp={temp}°C > daily_max={state.daily_max}°C "
                            f"— daily_max güncellenmedi (METAR değil)"
                        )

                detect_local = datetime.fromtimestamp(detect_ms / 1000).strftime("%H:%M:%S.%f")[:-3]
                log.info(
                    f"[MET] YENI OBS #{state.obs_count}  {detect_local} local  "
                    f"fetch={state.last_fetch_duration_ms}ms  age={age_str}  "
                    f"temp={temp}°C  daily_max={state.daily_max}°C  "
                    f"data_time={lm[17:25]}"
                    + ("  <<< YENI MAX >>>" if is_new_max else "")
                )

                temp_changed = (temp != old_temp)
                eval_source = "METSERVICE" if _is_post_metar_zone(now_utc) else "METSERVICE_EARLY"
                if eval_source == "METSERVICE_EARLY":
                    log.info(f"[MET] Pre-METAR zone (:{now_utc.minute:02d}) — veri güncellendi ama emir yok, METAR bekleniyor")
                await run_evaluation(
                    state, manager, eval_source, date_str, is_new_max, clob_client,
                    temp_changed=temp_changed,
                )
            else:
                burst_in = max(0, cdn_ttl - (age or 0) - BURST_PRE)
                burst_flag = "[BURST]" if (age is not None and age >= cdn_ttl - BURST_PRE) else f"burst_in={burst_in:.0f}s"
                log.debug(
                    f"[MET] cache  age={age_str}/TTL={cdn_ttl}s  temp={temp}°C  {burst_flag}  "
                    f"fetch={fetch_end_ms - fetch_start_ms}ms"
                )

        except Exception as e:
            log.debug(f"[MET] Fetch hatası: {e}")

        # ── Age-aware uyku ─────────────────────────────────────────────
        elapsed = time.monotonic() - t0
        if age is None:
            # Taze CDN → bir sonraki burst başlangıcına kadar uyu
            sleep_t = cdn_ttl - BURST_PRE - elapsed
        elif age >= cdn_ttl - BURST_PRE:
            # Burst zone: BURST_INT aralıkla poll
            sleep_t = BURST_INT - elapsed
        else:
            # Burst başlamadan önce bekle
            sleep_t = (cdn_ttl - age - BURST_PRE) - elapsed

        await asyncio.sleep(max(0.05, sleep_t))


async def price_change_recorder(manager: MarketManager, date_str: str):
    """WebSocket price_change event'lerini bucket bağlamıyla kaydet."""
    @manager.on_price_change
    async def _on_price_change(change: PWSPriceChange):
        market = manager.registry.get(change.asset_id)
        save_price_change(change, date_str, market)

    log.info("[RECORDER] Price change recorder aktif")
    await asyncio.Future()


async def trade_recorder(manager: MarketManager, date_str: str):
    """WebSocket trade event'lerini bucket bağlamıyla kaydet — strateji analizi için."""
    @manager.on_trade
    async def _on_trade(trade: PWSTrade):
        market = manager.registry.get(trade.market_id)
        save_trade(trade, date_str, market)

    log.info("[RECORDER] Trade recorder aktif")
    await asyncio.Future()


async def balance_refresher(state: BotState, clob_client):
    """
    Bakiyeyi 60 saniyede bir önbellekle.
    METSERVICE geldiğinde emir anında REST beklemeden hazır olsun.
    """
    loop = asyncio.get_event_loop()
    while True:
        try:
            bal_raw = await loop.run_in_executor(None, clob_client.get_balance)
            new_bal = int(bal_raw.get("balance", "0")) / 1e6
            if abs(new_bal - state.cached_balance_usdc) > 0.01:
                log.info(f"[BALANCE] {state.cached_balance_usdc:.2f} → {new_bal:.2f} USDC")
            state.cached_balance_usdc = new_bal
        except Exception as e:
            log.warning(f"[BALANCE] Fetch hatası: {e}")
        await asyncio.sleep(60)


async def rest_prefetcher(state: BotState, manager: MarketManager):
    """
    Arkaplan: Olası dead/higher bucket tokenlarının REST depth'ini önceden çek.
    METSERVICE gelince fill_price zaten hesaplı olsun — emir anında REST bekleme yok.

    Kapsam:
      - NO tokenlar: bucket_max <= daily_max + 1 (DEAD adayı)
      - YES tokenlar: type=higher ve bucket_min <= daily_max + 2
    """
    loop = asyncio.get_event_loop()
    from infra.pws.rest import PolymarketREST
    _rest = PolymarketREST()

    while True:
        try:
            all_markets = manager.get_markets_by_city(CITY)
            if all_markets and state.daily_max > -999:
                size_usdc = max(5.0, min(MAX_ORDER_USDC, max(state.cached_balance_usdc, 5.0)))
                no_candidates = [
                    (tid, m) for tid, m in all_markets.items()
                    if m.outcome == "NO"
                    and m.bucket is not None
                    and _bucket_max(m.bucket) is not None
                    and _bucket_max(m.bucket) <= state.daily_max + 1
                    and tid not in state.ordered_token_ids
                ]
                yes_candidates = [
                    (tid, m) for tid, m in all_markets.items()
                    if m.outcome == "YES"
                    and m.bucket is not None
                    and m.bucket.type == "higher"
                    and _bucket_min(m.bucket) is not None
                    and _bucket_min(m.bucket) <= state.daily_max + 2
                    and tid not in state.ordered_token_ids
                ]
                for tid, m in no_candidates + yes_candidates:
                    try:
                        fresh = await loop.run_in_executor(None, _rest.get_book, tid)
                        if fresh and fresh.asks:
                            avg, depth_str, fill_price, total_tokens = _calc_avg_fill(fresh.asks, size_usdc)
                            if fill_price is not None:
                                state.prefetched_order_params[tid] = {
                                    "fill_price":    fill_price,
                                    "total_tokens":  round(total_tokens, 1),
                                    "fetch_ms":      int(time.time() * 1000),
                                    "best_ask":      fresh.asks[0].price,
                                    "depth_summary": depth_str,
                                }
                    except Exception as e:
                        log.debug(f"[PREFETCH] {tid[-8:]}: {e}")
        except Exception as e:
            log.debug(f"[PREFETCH] Hata: {e}")
        await asyncio.sleep(2)


async def status_printer(state: BotState, manager: MarketManager):
    while True:
        await asyncio.sleep(60)
        markets = manager.get_markets_by_city(CITY)
        n_books = sum(1 for tid in markets if manager.get_orderbook(tid) is not None)
        print(
            f"\n[STATUS] obs={state.obs_temp}°C  daily_max={state.daily_max}°C  "
            f"obs#{state.obs_count}  snapshots={state.snapshot_count}  "
            f"books={n_books}/{len(markets)}  "
            f"open_pos={len(state.open_positions)}"
        )
        sys.stdout.flush()


# ════════════════════════════════════════════════════════════════════
#   MAIN
# ════════════════════════════════════════════════════════════════════

async def run(target_date: str):
    state   = BotState()
    manager = MarketManager(cities=[CITY], target_date=target_date)

    # Daha önce atılan emirleri dosyadan restore et (crash/restart koruması)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ordered_file = LOG_DIR / f"{target_date}_ordered.txt"
    if ordered_file.exists():
        for line in ordered_file.read_text().splitlines():
            t = line.strip()
            if t:
                state.ordered_token_ids.add(t)
        if state.ordered_token_ids:
            log.info(
                f"[STATE] {len(state.ordered_token_ids)} daha önce atılan emir restore edildi "
                f"→ bu token'lar tekrar işlenmeyecek"
            )

    clob_client = None
    try:
        from clob import ClobOrderClient
        clob_client = ClobOrderClient.from_env()
        mode = "DRY-RUN" if clob_client.dry_run else "CANLI"
        log.info(f"[MAIN] CLOB istemcisi yüklendi — {mode}")
    except FileNotFoundError:
        log.info("[MAIN] credentials.env bulunamadı — sadece gözlem modu")
    except Exception as e:
        log.warning(f"[MAIN] CLOB yüklenemedi: {e} — sadece gözlem modu")

    log.info(f"[MAIN] Wellington Bot başlatılıyor — {target_date}")
    await manager.discover()

    if not manager.registry:
        log.error("[MAIN] Polymarket'te Wellington marketi bulunamadı")
        return

    manager.print_discovery()
    await manager.connect()

    asyncio.create_task(manager.stream.listen())

    log.info("[MAIN] İlk orderbook'ların gelmesi için 5s bekleniyor...")
    await asyncio.sleep(5)

    # Bakiyeyi hemen çek — METSERVICE gelmeden hazır olsun
    if clob_client is not None:
        try:
            loop = asyncio.get_event_loop()
            bal_raw = await loop.run_in_executor(None, clob_client.get_balance)
            state.cached_balance_usdc = int(bal_raw.get("balance", "0")) / 1e6
            log.info(f"[BALANCE] Başlangıç bakiyesi: {state.cached_balance_usdc:.2f} USDC")
        except Exception as e:
            log.warning(f"[BALANCE] Başlangıç fetch hatası: {e}")

    tasks = [
        metservice_poller(state, manager, target_date, clob_client),
        price_change_recorder(manager, target_date),
        trade_recorder(manager, target_date),
        status_printer(state, manager),
    ]
    if clob_client is not None:
        tasks.append(balance_refresher(state, clob_client))
        tasks.append(rest_prefetcher(state, manager))

    await asyncio.gather(*tasks)


def main():
    parser = argparse.ArgumentParser(description="Wellington Dead Bucket Bot")
    parser.add_argument("--date", default=None, help="YYYY-MM-DD (default: bugun NZDT)")
    args = parser.parse_args()

    # Wellington tarihi: UTC+13 → yerel gün
    if args.date:
        target_date = args.date
    else:
        from datetime import timedelta
        target_date = (datetime.now(timezone.utc) + timedelta(hours=WELLINGTON_TZ_OFF)).strftime("%Y-%m-%d")

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    lock_path = LOG_DIR / "wellington_bot.lock"
    lock_fh   = open(lock_path, "w")
    try:
        fcntl.flock(lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        print(f"\nHATA: Başka bir wellington_bot zaten çalışıyor! ({lock_path})")
        sys.exit(1)

    print(f"""
╔══════════════════════════════════════════════════════════════════╗
║       WELLINGTON DEAD BUCKET BOT — Gozlem + Emir                ║
║  MetService AWS 93439  |  1s poll  |  PM WebSocket              ║
╚══════════════════════════════════════════════════════════════════╝
  Tarih  : {target_date}  (Wellington NZDT)
  Log    : {LOG_DIR}/
    → {target_date}_obs.jsonl
    → {target_date}_price_changes.jsonl
    → {target_date}_trades.jsonl
""")

    try:
        asyncio.run(run(target_date))
    except KeyboardInterrupt:
        print("\n[MAIN] Bot durduruldu.")
    finally:
        fcntl.flock(lock_fh, fcntl.LOCK_UN)
        lock_fh.close()


if __name__ == "__main__":
    main()
