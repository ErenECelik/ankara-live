"""
Ankara Dead Bucket Bot v2 — Observation Mode

Görev:
  Her METAR raporu geldiğinde TÜM BUCKET'LARIN tam orderbook'unu kaydet.
  Sinyali aldığımız anda market nasıl görünüyordu → sonradan analiz.

Tespit mekanizmaları:
  A) Dead bucket  : daily_max_metar > bucket_max → YES kesin 0'a gider
  B) Adjacent tension: iki komşu bucket'ın YES mid toplamı > threshold
     → Market "bu ikisinden biri" diyor; METAR birisini öne çıkarınca fırsat
  C) Live bucket  : mevcut max'ı kapsayan bucket (forecast'e göre güven değişir)

Veri kaydı (logs/ankara_signals/{date}_metar.jsonl):
  Her METAR'da:
    - METAR string + sıcaklık
    - daily_max, forecast_max
    - Tüm bucket'lar → her birinde: YES/NO bid/ask/mid + tüm depth levels
    - Tespit edilen tension'lar + sinyaller

  Ayrı dosya (logs/ankara_signals/{date}_price_changes.jsonl):
    - WebSocket'ten gelen her fiyat değişikliği (raw akış)

Kullanım:
    python -m src.bot.ankara_bot
    python -m src.bot.ankara_bot --date 2026-02-28
"""

import asyncio
import logging
import argparse
import requests
import re
import sys
import time
import json
import fcntl
from datetime import datetime, timezone, timedelta, date
from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, List, Tuple
from pathlib import Path

from infra.pws import MarketManager
from infra.pws.models import PWSMarket, PWSOrderbook, PWSPriceChange

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(message)s",
)
log = logging.getLogger("ankara_bot")
logging.getLogger("pws.rest").setLevel(logging.ERROR)   # 404 spam'ını bastır

# ── Constants ──────────────────────────────────────────────────────
CITY            = "ankara"
MGM_MERKEZID    = 90626           # Ankara Esenboğa / Merkez İstasyonu
ANKARA_TZ_OFFSET = 3              # UTC+3 (TRT)
ANKARA_LAT      = 39.9334
ANKARA_LON      = 32.8597

MGM_IDLE_INTERVAL    = 30.0       # saniye — pencere dışı (sunucu yükü minimize)
MGM_BURST_INTERVAL   = 0.3        # saniye — METAR penceresi içinde maksimum hız
MGM_BURST_PRE        = 60         # saniye — pencere açılmadan kaç saniye önce burst başlasın
WUNDER_POLL_INTERVAL = 65         # CDN max-age=53s

# Adjacent tension: iki YES mid'i bu eşiği geçerse "gerilim var" sinyali
ADJACENT_TENSION_THRESHOLD = 0.75

# BUY_NO filtresi: NO ask bu eşikten pahalıysa sinyal üretme
# no_ask=0.95 → getiri=(1/0.95−1)=5.3% — altı anlamsız
MAX_NO_ASK_THRESHOLD = 0.95

# Emir başına maksimum harcama (USDC)
MAX_ORDER_USDC = 30.0

LOG_DIR = Path(__file__).resolve().parent / "logs" / "ankara_signals"

# ── MGM ──
MGM_URL = "https://servis.mgm.gov.tr/web/sondurumlar"
MGM_HEADERS = {
    "Origin":        "https://www.mgm.gov.tr",
    "Referer":       "https://www.mgm.gov.tr/",
    "User-Agent":    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":        "application/json, */*",
    "Cache-Control": "no-cache, no-store, must-revalidate",
    "Pragma":        "no-cache",
}

# ── Wunderground ──
WUNDER_API_KEY = "e1f10a1e78da46f5b10a1e78da96f525"
WUNDER_URL     = "https://api.weather.com/v3/wx/forecast/hourly/2day"


# ════════════════════════════════════════════════════════════════════
#   STATE
# ════════════════════════════════════════════════════════════════════

@dataclass
class BotState:
    # METAR
    metar_temp: Optional[float]         = None
    daily_max_metar: float              = -999.0
    prev_daily_max: float               = -999.0   # bir önceki max (yeni max geldi mi?)
    last_metar_str: str                 = ""
    last_metar_time: Optional[datetime] = None
    last_veri_zamani: Optional[str]     = None
    metar_count: int                    = 0

    # METAR tespit zamanlaması (ms precision)
    last_fetch_start_ms: int            = 0   # HTTP GET gönderildiği an (epoch ms)
    last_fetch_end_ms: int              = 0   # HTTP yanıt alındığı an (epoch ms)
    last_detect_ms: int                 = 0   # yeni METAR tespit edildiği an (epoch ms)
    last_fetch_duration_ms: int         = 0   # HTTP süre

    # Wunderground
    forecast_temps: List[int]           = field(default_factory=list)
    forecast_valid_utcs: List[int]      = field(default_factory=list)
    forecast_valid_locals: List[str]    = field(default_factory=list)
    forecast_exp_sentinel: int          = 0
    forecast_max_remaining: Optional[float] = None
    last_forecast_time: Optional[datetime]  = None

    # Sayaçlar
    snapshot_count: int = 0

    # Bakiye önbelleği — emir anında REST çağrısı yapma
    cached_balance_usdc: float = 0.0

    # Emir takibi — aynı token için iki kez emir atma
    ordered_token_ids: set = field(default_factory=set)

    # REST pre-fetch: METAR gelmeden fill_price hazır olsun — emir anında sıfır gecikme
    prefetched_order_params: dict = field(default_factory=dict)


# ════════════════════════════════════════════════════════════════════
#   MGM FETCH
# ════════════════════════════════════════════════════════════════════

def _mgm_fetch_sync(merkezid: int) -> Tuple[str, str, int, int]:
    """
    (veriZamani, rasatMetar, fetch_start_ms, fetch_end_ms) döndürür.
    Zamanlar: Unix epoch milisaniye, wall clock.
    """
    fetch_start_ms = int(time.time() * 1000)
    params = {"merkezid": merkezid, "_": fetch_start_ms}
    r = requests.get(MGM_URL, params=params, headers=MGM_HEADERS, timeout=(3, 8))
    fetch_end_ms = int(time.time() * 1000)
    r.raise_for_status()
    d = r.json()[0]
    return d["veriZamani"], d.get("rasatMetar", ""), fetch_start_ms, fetch_end_ms


def parse_metar_temp(metar: str) -> Optional[float]:
    """METAR TT/TdTd formatından sıcaklık (M prefix = negatif)."""
    m = re.search(r'\b(M?\d{2})/(M?\d{2})\b', metar)
    if not m:
        return None
    raw = m.group(1)
    return float(-int(raw[1:]) if raw.startswith('M') else int(raw))


# ════════════════════════════════════════════════════════════════════
#   WUNDERGROUND FETCH
# ════════════════════════════════════════════════════════════════════

def _wunder_fetch_sync(lat: float, lon: float) -> dict:
    params = {
        "geocode": f"{lat},{lon}",
        "format": "json",
        "units": "m",
        "language": "en-US",
        "apiKey": WUNDER_API_KEY,
    }
    r = requests.get(WUNDER_URL, params=params, timeout=15)
    r.raise_for_status()
    return r.json()


def calc_forecast_max_remaining(
    temps: List[int],
    valid_utcs: List[int],
    reference_utc: datetime,
) -> Optional[float]:
    """Günün kalan saatlerindeki (UTC+3) max forecast sıcaklığı."""
    if not temps or not valid_utcs:
        return None
    now_ts = reference_utc.timestamp()
    tz_ankara = timezone(timedelta(hours=ANKARA_TZ_OFFSET))
    end_of_day = reference_utc.astimezone(tz_ankara).replace(hour=23, minute=59, second=59)
    end_ts = end_of_day.timestamp()
    remaining = [t for t, ts in zip(temps, valid_utcs) if now_ts <= ts <= end_ts]
    return float(max(remaining)) if remaining else None


def get_hourly_forecast_table(state: BotState, n_hours: int = 8) -> List[dict]:
    """Sonraki n saat için saatlik forecast tablosu."""
    result = []
    now_ts = datetime.now(timezone.utc).timestamp()
    for temp, ts, local in zip(
        state.forecast_temps,
        state.forecast_valid_utcs,
        state.forecast_valid_locals,
    ):
        if ts >= now_ts:
            result.append({"local": local, "temp": temp, "ts_utc": ts})
            if len(result) >= n_hours:
                break
    return result


# ════════════════════════════════════════════════════════════════════
#   BUCKET HELPERS
# ════════════════════════════════════════════════════════════════════

def _bucket_max(b) -> Optional[float]:
    if b.type in ("range", "exact"):
        return float(b.high if b.type == "range" else b.low) if b.high is not None or b.low is not None else None
    elif b.type == "below":
        return float(b.high) if b.high is not None else None
    elif b.type == "higher":
        return 999.0
    return None


def _bucket_min(b) -> Optional[float]:
    if b.type in ("range", "exact"):
        return float(b.low) if b.low is not None else None
    elif b.type == "below":
        return -999.0
    elif b.type == "higher":
        return float(b.low) if b.low is not None else None
    return None


def _bucket_contains(b, temp: float) -> bool:
    if b.type == "range":
        return b.low is not None and b.high is not None and b.low <= temp <= b.high
    elif b.type == "exact":
        return b.low is not None and int(temp) == b.low
    elif b.type == "below":
        return b.high is not None and temp <= b.high
    elif b.type == "higher":
        return b.low is not None and temp >= b.low
    return False


def _categorize_bucket(b, daily_max: float) -> str:
    """DEAD | LIVE | ABOVE"""
    bmax = _bucket_max(b)
    if bmax is not None and bmax < daily_max:
        return "DEAD"
    if _bucket_contains(b, daily_max):
        return "LIVE"
    return "ABOVE"


# ════════════════════════════════════════════════════════════════════
#   ORDERBOOK SNAPSHOT — Tek bucket için full veri
# ════════════════════════════════════════════════════════════════════

def _calc_avg_fill(asks: list, size_usdc: float) -> Tuple[Optional[float], str, Optional[float], float]:
    """
    Ask ladderını yürüterek ortalama dolum fiyatını hesapla.

    asks      : [PWSOrderbookLevel] — fiyata göre ASC sıralı
    size_usdc : harcamak istediğimiz USDC miktarı

    Döndürür: (avg_price_per_token, depth_summary_str, fill_price, total_tokens)
      avg_price_per_token : ağırlıklı ortalama ödeme / toplam token
      depth_summary_str   : "0.050×30.0 | 0.080×12.5  [LİKİDİTE YETERSİZ]"
      fill_price          : siparişi doldurmak için gereken en yüksek fiyat seviyesi
                            → limit emrini bu fiyattan gir, tüm seviyeleri sweep et
      total_tokens        : bu fiyata kadar alınacak toplam token sayısı
    """
    remaining = size_usdc
    total_tokens = 0.0
    levels_used = []
    fill_price = None

    for lvl in asks:
        fill_price = lvl.price
        available_usdc = lvl.price * lvl.size
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
    avg = filled_usdc / total_tokens
    suffix = "  [LİKİDİTE YETERSİZ]" if remaining > 0.01 else ""
    summary = " | ".join(levels_used[:6]) + suffix
    return avg, summary, fill_price, total_tokens


def _serialize_book(book: Optional[PWSOrderbook], snapshot_ms: int = 0) -> dict:
    """
    PWSOrderbook → JSON-serializable dict.

    snapshot_ms: bu fonksiyonun çağrıldığı an (epoch ms).
    book.timestamp: Polymarket WebSocket'in bu kitabı son güncellediği an.
    book_age_ms: snapshot anında kitap kaç ms önce güncellenmişti.
    """
    if book is None:
        return {
            "bid": None, "ask": None, "mid": None,
            "spread": None,
            "bid_depth": None, "ask_depth": None,
            "bids": [], "asks": [],
            "book_ts_ms": None,
            "book_ts_utc": None,
            "book_age_ms": None,
        }
    book_ts_ms  = int(book.timestamp.timestamp() * 1000)
    book_age_ms = (snapshot_ms - book_ts_ms) if snapshot_ms > 0 else None
    return {
        "bid":       book.best_bid,
        "ask":       book.best_ask,
        "mid":       book.mid_price,
        "spread":    book.spread,
        "bid_depth": round(book.bid_depth, 2),
        "ask_depth": round(book.ask_depth, 2),
        # Polymarket'in bu kitabı ne zaman gönderdiği
        "book_ts_ms":  book_ts_ms,
        "book_ts_utc": book.timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        # Snapshot anında kitap kaç ms eskiydi
        "book_age_ms": book_age_ms,
        # Tüm seviyeleri sakla — sonradan spread analizi için
        "bids": [{"p": round(l.price, 4), "s": round(l.size, 2)} for l in book.bids[:10]],
        "asks": [{"p": round(l.price, 4), "s": round(l.size, 2)} for l in book.asks[:10]],
    }


def build_bucket_snapshot(
    all_markets: Dict[str, PWSMarket],
    manager: MarketManager,
    daily_max: float,
    forecast_max: Optional[float],
    snapshot_ms: int = 0,
) -> List[dict]:
    """
    Tüm bucket'lar için anlık tam fotoğraf.
    Döndürür: bucket listesi (sıcaklığa göre sıralı), her birinde YES+NO tam orderbook.
    """
    # condition_id → {yes_market, no_market} grupla
    groups: Dict[str, dict] = {}
    for tid, m in all_markets.items():
        if not m.bucket:
            continue
        cid = m.condition_id
        if cid not in groups:
            groups[cid] = {
                "bucket":     m.bucket,
                "yes_market": None,
                "no_market":  None,
            }
        if m.outcome == "YES":
            groups[cid]["yes_market"] = m
        else:
            groups[cid]["no_market"] = m

    buckets = []
    for cid, g in sorted(
        groups.items(),
        key=lambda x: x[1]["bucket"].sort_key,
    ):
        b    = g["bucket"]
        yes_m = g["yes_market"]
        no_m  = g["no_market"]

        yes_book = manager.get_orderbook(yes_m.token_id) if yes_m else None
        no_book  = manager.get_orderbook(no_m.token_id)  if no_m  else None

        yes_data = _serialize_book(yes_book, snapshot_ms)
        no_data  = _serialize_book(no_book,  snapshot_ms)

        category = _categorize_bucket(b, daily_max)

        # Forecast ile üst sınır kontrolü
        fc_dead = False
        if forecast_max is not None:
            bmin = _bucket_min(b)
            if bmin is not None and forecast_max < bmin:
                fc_dead = True  # forecast bile bu bucket'a ulaşamıyor

        buckets.append({
            "label":       b.label,
            "type":        b.type,
            "low":         b.low,
            "high":        b.high,
            "sort_key":    b.sort_key,
            "category":    category,          # DEAD | LIVE | ABOVE
            "fc_dead":     fc_dead,           # forecast'a göre de ölü mü?
            "condition_id": cid,
            "yes": {
                "token_id": yes_m.token_id if yes_m else None,
                **yes_data,
            },
            "no": {
                "token_id": no_m.token_id if no_m else None,
                **no_data,
            },
            # Kolay erişim için
            "yes_mid": yes_data["mid"],
            "no_mid":  no_data["mid"],
        })

    return buckets


# ════════════════════════════════════════════════════════════════════
#   ADJACENT TENSION DETECTOR
# ════════════════════════════════════════════════════════════════════

def detect_adjacent_tensions(
    bucket_snapshots: List[dict],
    threshold: float = ADJACENT_TENSION_THRESHOLD,
) -> List[dict]:
    """
    Komşu iki bucket'ın YES mid toplamı threshold'u geçiyor mu?

    Mantık:
      Market "bu ikisinden biri" diyorsa, YES_A + YES_B → 0.80+
      METAR gelince birisini öldürür → fırsat penceresi açılır.

    Döndürür: gerilimli çiftlerin listesi.
    """
    tensions = []
    # Sadece price verisi olan bucket'lar
    valid = [b for b in bucket_snapshots if b["yes_mid"] is not None]

    for i in range(len(valid) - 1):
        a = valid[i]
        b_next = valid[i + 1]

        # Sadece komşu bucket'lar (sort_key bitişik değerler)
        # Yeterince yakın mı kontrol et (≤5 birlik fark)
        if abs(a["sort_key"] - b_next["sort_key"]) > 5:
            continue

        yes_sum = a["yes_mid"] + b_next["yes_mid"]

        if yes_sum >= threshold:
            tensions.append({
                "lower_label":    a["label"],
                "upper_label":    b_next["label"],
                "lower_yes_mid":  round(a["yes_mid"], 4),
                "upper_yes_mid":  round(b_next["yes_mid"], 4),
                "yes_sum":        round(yes_sum, 4),
                "lower_category": a["category"],
                "upper_category": b_next["category"],
                # Hangi tarafta aksiyon?
                # METAR lower'da → upper satılabilir (BUY NO upper)
                # METAR upper'da → lower zaten dead (BUY NO lower)
                "trade_hint": _tension_trade_hint(a, b_next),
            })

    return tensions


def _tension_trade_hint(lower: dict, upper: dict) -> str:
    """İki gerilimli bucket için hangi senaryoda ne yapılabilir."""
    hints = []
    if lower["category"] == "LIVE":
        hints.append(
            f"Mevcut max {lower['label']}'da → "
            f"{upper['label']} NO (YES={upper['yes_mid']:.2f}) düşebilir"
        )
    if upper["category"] == "LIVE":
        hints.append(
            f"Mevcut max {upper['label']}'da → "
            f"{lower['label']} NO (YES={lower['yes_mid']:.2f}) → 0 gidecek"
        )
    if lower["category"] == "ABOVE" and upper["category"] == "ABOVE":
        hints.append(
            f"Her ikisi de bekleniyor — sonraki METAR belirleyici"
        )
    return " | ".join(hints) if hints else "Gözle"


# ════════════════════════════════════════════════════════════════════
#   SIGNAL DETECTION
# ════════════════════════════════════════════════════════════════════

@dataclass
class Signal:
    action:        str            # "BUY_YES" | "BUY_NO" | "WATCH"
    bucket_label:  str
    token_id:      str
    side:          str            # "YES" | "NO"
    current_ask:   Optional[float]
    current_bid:   Optional[float]
    confidence:    str            # "HIGH" | "MEDIUM" | "LOW"
    reason:        str
    bucket_type:   str = ""       # "exact" | "range" | "below" | "higher"
    skipped:       bool = False   # MAX_NO_ASK_THRESHOLD aşıldı → emir yok
    skip_reason:   str = ""       # "NO zaten 0.92 — ATLANDI"


def evaluate_signals(
    bucket_snapshots: List[dict],
    state: BotState,
) -> List[Signal]:
    """
    Bucket snapshot'larından sinyal üret.

    Kategoriler:
      A) DEAD bucket → BUY_NO (HIGH) — kesin ölü
      B) fc_dead bucket → BUY_NO (MEDIUM) — forecast'a göre ölü
      C) LIVE bucket → BUY_YES (güven forecast'e göre)
    """
    signals = []
    max_t  = state.daily_max_metar
    fc_max = state.forecast_max_remaining

    for b in bucket_snapshots:
        if b["category"] == "DEAD":
            # A) Kesin ölü → BUY NO
            no_ask = b["no"]["ask"]
            no_bid = b["no"]["bid"]
            if no_ask is not None and no_ask > 0.01:
                if no_ask > MAX_NO_ASK_THRESHOLD:
                    # NO çok pahalı — kapital kilitlemenin anlamı yok
                    signals.append(Signal(
                        action        = "BUY_NO",
                        bucket_label  = b["label"],
                        token_id      = b["no"]["token_id"] or "",
                        side          = "NO",
                        current_ask   = no_ask,
                        current_bid   = no_bid,
                        confidence    = "HIGH",
                        reason        = (
                            f"Kesin ölü: daily_max={max_t}°C > {b['label']} (max={b['high'] or b['low']}°C)"
                        ),
                        bucket_type   = b["type"],
                        skipped       = True,
                        skip_reason   = (
                            f"NO zaten {no_ask:.2f} — ATLANDI "
                            f"(eşik={MAX_NO_ASK_THRESHOLD}, getiri<%"
                            f"{(1/MAX_NO_ASK_THRESHOLD - 1)*100:.0f})"
                        ),
                    ))
                else:
                    signals.append(Signal(
                        action        = "BUY_NO",
                        bucket_label  = b["label"],
                        token_id      = b["no"]["token_id"] or "",
                        side          = "NO",
                        current_ask   = no_ask,
                        current_bid   = no_bid,
                        confidence    = "HIGH",
                        reason        = (
                            f"Kesin ölü: daily_max={max_t}°C > {b['label']} (max={b['high'] or b['low']}°C)"
                        ),
                        bucket_type   = b["type"],
                    ))

        elif b["category"] == "LIVE":
            # B) Canlı bucket → BUY YES
            #
            # HIGH koşulları (SADECE METAR verisi, tahmin yok):
            #   1) type=higher: sıcaklık üst sınırsız bucketa ulaştı → kesin YES
            #   2) metar_temp < daily_max: sıcaklık zirvenin altına düştü
            #      → zirve sabitlendi, bu bucket kazanıyor
            #
            # MEDIUM: sıcaklık hâlâ zirvede, yönü belli değil
            yes_ask = b["yes"]["ask"]
            yes_bid = b["yes"]["bid"]

            cur_t = state.metar_temp  # en son METAR sıcaklığı

            if b["type"] == "higher":
                # Koşul 1: üst sınırsız — METAR bu eşiği geçti, kesin YES
                confidence = "HIGH"
                reason     = (f"METAR garantisi: daily_max={max_t}°C ∈ {b['label']}"
                              f" | ust sinırsız bucket — kesin YES")

            elif cur_t is not None and cur_t < max_t:
                # Koşul 2: sıcaklık düştü → zirve sabitlendi
                # Örnek: daily_max=12, metar=11 → 12°C bucket kazanıyor
                confidence = "HIGH"
                reason     = (f"Soguma sinyali: metar={cur_t}°C < daily_max={max_t}°C"
                              f" | {b['label']} zirvesi {max_t}°C sabitlendi — kesin YES")

            else:
                # Sıcaklık hâlâ zirvede, yukarı mı aşağı mı belirsiz
                fc_note = f" | fc_max={fc_max}°C" if fc_max is not None else ""
                confidence = "MEDIUM"
                reason     = f"Zirve henüz sabitlenmedi: metar={cur_t}°C = daily_max={max_t}°C{fc_note}"

            if yes_ask is not None:
                signals.append(Signal(
                    action        = "BUY_YES",
                    bucket_label  = b["label"],
                    token_id      = b["yes"]["token_id"] or "",
                    side          = "YES",
                    current_ask   = yes_ask,
                    current_bid   = yes_bid,
                    confidence    = confidence,
                    reason        = reason,
                    bucket_type   = b["type"],
                ))

    return signals


# ════════════════════════════════════════════════════════════════════
#   DISPLAY
# ════════════════════════════════════════════════════════════════════

CONF_ICON = {"HIGH": "🟢", "MEDIUM": "🟡", "LOW": "🔴"}


def print_metar_snapshot(
    state: BotState,
    bucket_snapshots: List[dict],
    tensions: List[dict],
    signals: List[Signal],
    source: str,
    is_new_max: bool,
):
    now_l = datetime.now().strftime("%H:%M:%S")
    now_u = datetime.now(timezone.utc).strftime("%H:%M:%S")
    new_max_flag = "  <<< YENİ MAX >>>" if is_new_max else ""

    src_label = {
        "METAR":    "METAR (MGM)",
        "FORECAST": "FORECAST (Wunderground) — emir yok",
    }.get(source, source)

    print("\n" + "=" * 80)
    print(f"  [{src_label}]  {now_l} yerel / {now_u} UTC{new_max_flag}")
    print(f"  METAR     : {state.last_metar_str}")
    print(f"  Sicaklik  : {state.metar_temp}°C  |  Gunun maks: {state.daily_max_metar}°C  (#{state.metar_count})")
    fc_str = f"{state.forecast_max_remaining}°C" if state.forecast_max_remaining is not None else "?"
    print(f"  Tahmin max: {fc_str}  (gunun kalan saatleri, SADECE bilgi amacli)")

    # ── Bucket tablosu ──
    print("-" * 80)
    print(f"  {'Bucket':<12} {'Tip':<8} {'Durum':<9}  {'YES ask':>7} {'YES bid':>7}  {'NO ask':>6} {'NO bid':>6}  {'Getiri(YES)':>11}")
    print("  " + "-" * 76)

    _type_label = {"exact": "tek", "range": "aralık", "below": "altı", "higher": "üstü"}

    for b in bucket_snapshots:
        cat = b["category"]
        cat_label  = {"DEAD": "OLDU", "LIVE": "CANLI", "ABOVE": "bekliyor"}.get(cat, cat)
        type_label = _type_label.get(b["type"], b["type"])
        yes = b["yes"]
        no  = b["no"]

        def fmt(v): return f"{v:.3f}" if v is not None else "  -- "

        # Beklenen getiri: 1/ask - 1 (kazanilirsa)
        ret_str = ""
        if yes["ask"] and yes["ask"] > 0:
            ret = (1.0 / yes["ask"] - 1) * 100
            ret_str = f"{ret:+.0f}%"

        marker = ""
        if cat == "LIVE":
            marker = " <-- BURADA"
        elif cat == "DEAD":
            marker = " (bitti)"

        print(
            f"  {b['label']:<12} {type_label:<8} {cat_label:<9}  "
            f"{fmt(yes['ask']):>7} {fmt(yes['bid']):>7}  "
            f"{fmt(no['ask']):>6} {fmt(no['bid']):>6}  "
            f"{ret_str:>11}{marker}"
        )

    # ── Sinyaller ──
    print("-" * 80)
    high_signals   = [s for s in signals if s.confidence == "HIGH" and not s.skipped]
    other_signals  = [s for s in signals if s.confidence != "HIGH" and not s.skipped]
    skipped_signals = [s for s in signals if s.skipped]

    if high_signals:
        print("  EMIRLER (HIGH confidence — METAR gelirse tetiklenir):")
        for s in high_signals:
            ask_str = f"{s.current_ask:.3f}" if s.current_ask is not None else "?"
            ret = (1.0 / s.current_ask - 1) * 100 if s.current_ask and s.current_ask > 0 else 0
            type_tag = f"[{_type_label.get(s.bucket_type, s.bucket_type)}]" if s.bucket_type else ""
            print(f"    >>> {s.action:<8} {s.bucket_label:<12} {type_tag:<8}  ask={ask_str}  getiri={ret:+.0f}%  |  {s.reason}")
    else:
        print("  EMIRLER: yok — yeni METAR bekleniyor")

    if skipped_signals:
        print("  ATLANDI (NO pahalı — eşik üstü):")
        for s in skipped_signals:
            ask_str = f"{s.current_ask:.3f}" if s.current_ask is not None else "?"
            print(f"    ⛔ DEAD  {s.bucket_label:<12}  ask={ask_str}  |  {s.skip_reason}")

    if other_signals:
        print("  Diger sinyaller (emir yok):")
        for s in other_signals:
            icon = CONF_ICON.get(s.confidence, "?")
            ask_str = f"{s.current_ask:.3f}" if s.current_ask is not None else "?"
            print(f"    {icon} {s.confidence:<6} {s.action:<8} {s.bucket_label:<10}  ask={ask_str}  |  {s.reason}")

    # ── Adjacent tensions ──
    if tensions:
        print("-" * 80)
        print("  GERILIM:")
        for t in tensions:
            print(f"    [{t['lower_label']} + {t['upper_label']}]  YES toplam={t['yes_sum']:.3f}  {t['trade_hint']}")

    print("=" * 80)


# ════════════════════════════════════════════════════════════════════
#   DATA RECORDING
# ════════════════════════════════════════════════════════════════════

def save_metar_snapshot(
    state: BotState,
    bucket_snapshots: List[dict],
    tensions: List[dict],
    signals: List[Signal],
    source: str,
    date_str: str,
    is_new_max: bool,
):
    """
    Her METAR/FORECAST tetikleyicisinde tam snapshot'ı JSONL'e yazar.
    Her satır kendi başına tam bir analiz birimi.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    path = LOG_DIR / f"{date_str}_metar.jsonl"

    now_utc = datetime.now(timezone.utc)

    # Hourly forecast özeti (sonraki 8 saat)
    fc_table = get_hourly_forecast_table(state, n_hours=8)

    record = {
        # ── Zaman damgaları (ms precision) ──────────────────────────
        # detect_ms: HTTP yanıt geldiği an = METAR'ın fark edildiği an
        # fetch_start_ms → fetch_end_ms arası: HTTP latency
        # snapshot_delay_ms: tespit ile orderbook okuma arasındaki süre
        "detect_ts_utc":     now_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "detect_ms":         state.last_detect_ms,
        "fetch_start_ms":    state.last_fetch_start_ms,
        "fetch_end_ms":      state.last_fetch_end_ms,
        "fetch_duration_ms": state.last_fetch_duration_ms,
        "snapshot_ms":       int(now_utc.timestamp() * 1000),
        "snapshot_delay_ms": (int(now_utc.timestamp() * 1000) - state.last_detect_ms
                              if state.last_detect_ms else None),

        # ── METAR ───────────────────────────────────────────────────
        "source":            source,
        "metar_temp":        state.metar_temp,
        "metar_str":         state.last_metar_str,
        "veri_zamani":       state.last_veri_zamani,
        "daily_max_metar":   state.daily_max_metar,
        "prev_daily_max":    state.prev_daily_max,
        "is_new_max":        is_new_max,
        "metar_count":       state.metar_count,

        # ── Forecast ────────────────────────────────────────────────
        "forecast_max_remaining": state.forecast_max_remaining,
        "forecast_table":    fc_table,

        # ── Orderbook snapshot ──────────────────────────────────────
        "buckets":           bucket_snapshots,

        # ── Analiz ──────────────────────────────────────────────────
        "tensions":          tensions,
        "signals": [
            {
                "action":      s.action,
                "bucket":      s.bucket_label,
                "side":        s.side,
                "confidence":  s.confidence,
                "ask":         s.current_ask,
                "bid":         s.current_bid,
                "token_id":    s.token_id,
                "reason":      s.reason,
            }
            for s in signals
        ],
    }

    with open(path, "a") as f:
        f.write(json.dumps(record, default=str) + "\n")


def save_price_change(change: PWSPriceChange, date_str: str,
                      label: str = "", outcome: str = ""):
    """
    WebSocket price_change event'lerini ham olarak kaydet.
    Sonradan: METAR öncesi/sonrası fiyat hareketi analizi.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    path = LOG_DIR / f"{date_str}_price_changes.jsonl"

    record = {
        "ts_utc":   change.timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ts_ms":    int(change.timestamp.timestamp() * 1000),
        "label":    label,    # "11°C", "≤10°C" gibi — hangi bucket
        "outcome":  outcome,  # "YES" | "NO"
        "token_id": change.asset_id,
        "price":    change.price,
        "size":     change.size,
        "side":     change.side,
        "best_bid": change.best_bid,
        "best_ask": change.best_ask,
        "mid":      change.mid_price,
    }

    with open(path, "a") as f:
        f.write(json.dumps(record) + "\n")


# ════════════════════════════════════════════════════════════════════
#   CORE EVALUATION — Her METAR/FORECAST'ta çağrılır
# ════════════════════════════════════════════════════════════════════

async def run_evaluation(
    state: BotState,
    manager: MarketManager,
    source: str,
    date_str: str,
    is_new_max: bool = False,
    clob_client=None,
):
    """
    1. Tüm bucket'ların anlık tam fotoğrafını al
    2. Adjacent tension tespiti
    3. Sinyal değerlendirmesi
    4. Ekrana bas + diske kaydet
    """
    all_markets = manager.get_markets_by_city(CITY)
    if not all_markets:
        log.warning("[EVAL] Market yok — atlanıyor")
        return

    # Orderbook snapshot'ının çekildiği an — her bucket için book_age_ms hesabında kullanılır
    snapshot_ms = int(time.time() * 1000)

    # 1) Tam bucket fotoğrafı
    bucket_snaps = build_bucket_snapshot(
        all_markets, manager,
        daily_max    = state.daily_max_metar,
        forecast_max = state.forecast_max_remaining,
        snapshot_ms  = snapshot_ms,
    )

    # 2) Adjacent tension
    tensions = detect_adjacent_tensions(bucket_snaps)

    # 3) Sinyaller
    signals = evaluate_signals(bucket_snaps, state)

    state.snapshot_count += 1

    # 5) Emir — SADECE METAR tetikleyicisinde, SADECE BUY_NO (DEAD bucket)
    #    Emir ÖNCE atılır — print/save sonraya bırakılır, gecikme olmaz
    if clob_client is not None and source == "METAR":
        eligible = [
            s for s in signals
            if s.confidence == "HIGH"
            and (
                s.action == "BUY_NO"                                    # DEAD bucket NO — kesin kazanır
                or (s.action == "BUY_YES" and s.bucket_type == "higher") # ≥N°C YES — üst sınırsız, kesin kazanır
            )
            and not s.skipped
            and s.token_id
            and s.token_id not in state.ordered_token_ids
            and s.current_ask is not None
        ]
        # Tüm eligible sinyalleri işle — tekli seçim yok, her birine MAX_ORDER_USDC kadar FAK at
        if eligible:
            log.info(f"[ORDER] {len(eligible)} eligible sinyal — her birine {MAX_ORDER_USDC:.0f} USDC FAK")

            for s in eligible:
                balance_usdc = state.cached_balance_usdc
                order_usdc = max(5.0, min(MAX_ORDER_USDC, round(balance_usdc, 2)))

                prefetched = state.prefetched_order_params.get(s.token_id)
                if prefetched:
                    age_ms = int(time.time() * 1000) - prefetched["fetch_ms"]
                    avail = prefetched.get("available_usdc", order_usdc)
                    if avail < order_usdc:
                        order_usdc = max(5.0, round(avail * 0.98, 2))
                        log.info(
                            f"[ORDER] Pre-fetch {age_ms}ms önce | depth: {prefetched['depth_summary']} | "
                            f"likidite kısıtı: {avail:.2f} USDC → {order_usdc:.2f} USDC"
                        )
                    else:
                        log.info(
                            f"[ORDER] Pre-fetch {age_ms}ms önce | depth: {prefetched['depth_summary']}"
                        )
                else:
                    log.warning(f"[ORDER] Pre-fetch yok — market order yine de gönderilecek")

                log.info(
                    f"[ORDER] {s.action}  bucket={s.bucket_label}  "
                    f"amount={order_usdc:.2f} USDC  token=...{s.token_id[-8:]}"
                )
                fill_price = prefetched.get("fill_price") if prefetched else None
                if s.action == "BUY_NO":
                    resp = await clob_client.buy_no(
                        token_id=s.token_id,
                        usdc_amount=order_usdc,
                        price=fill_price,
                    )
                else:
                    resp = await clob_client.buy_yes(
                        token_id=s.token_id,
                        usdc_amount=order_usdc,
                        price=fill_price,
                    )
                log.info(f"[ORDER] Yanit: {resp}")
                filled = isinstance(resp, dict) and resp.get("status") == "matched"
                if filled:
                    state.ordered_token_ids.add(s.token_id)
                    ordered_file = LOG_DIR / f"{date_str}_ordered.txt"
                    with open(ordered_file, "a") as _f:
                        _f.write(s.token_id + "\n")
                    log.info(f"[ORDER] TAMAM — FAK doldu (matched), token kaydedildi")
                else:
                    log.warning(f"[ORDER] FAK dolmadı (status={resp.get('status') if isinstance(resp, dict) else 'error'}) — sonraki METAR'da tekrar denenecek")

    # 4) Çıktı + kayıt — emrden SONRA (gecikme yaratmaz)
    print_metar_snapshot(state, bucket_snaps, tensions, signals, source, is_new_max)
    save_metar_snapshot(state, bucket_snaps, tensions, signals, source, date_str, is_new_max)


# ════════════════════════════════════════════════════════════════════
#   ASYNC TASKS
# ════════════════════════════════════════════════════════════════════

def _in_metar_window(now_utc: datetime) -> bool:
    """
    Ankara METAR raporları xx:20 ve xx:50 UTC'de yayınlanır.
    MGM birkaç dakika geç yayınlayabilir.
    Burst penceresi: :19→:27 (xx:20 için) ve :49→:57 (xx:50 için).
    """
    m = now_utc.minute
    return (19 <= m <= 27) or (49 <= m <= 57)


def _secs_to_next_window(now_utc: datetime) -> float:
    """Bir sonraki burst penceresine kaç saniye kaldığı."""
    m = now_utc.minute
    s = now_utc.second
    elapsed = m * 60 + s
    # Pencere başlangıçları (saniye cinsinden, saat içinde)
    starts = [19 * 60, 49 * 60]
    for start in sorted(starts):
        if elapsed < start:
            return start - elapsed - MGM_BURST_PRE
    # Bir sonraki saatin :19'u
    return (60 * 60 - elapsed) + 19 * 60 - MGM_BURST_PRE


async def mgm_poller(state: BotState, manager: MarketManager, date_str: str, clob_client=None):
    loop = asyncio.get_event_loop()
    log.info(f"[MGM] Poller başladı — merkezid={MGM_MERKEZID}")

    # İlk fetch
    while True:
        try:
            vz, metar, _, _ = await loop.run_in_executor(None, _mgm_fetch_sync, MGM_MERKEZID)
            temp = parse_metar_temp(metar)
            state.last_veri_zamani = vz
            state.last_metar_str   = metar
            state.metar_temp       = temp
            if temp is not None:
                state.daily_max_metar = temp
                state.prev_daily_max  = temp
            log.info(f"[MGM] Başlangıç: {metar}  temp={temp}°C")
            break
        except Exception as e:
            log.warning(f"[MGM] İlk fetch hatası: {e} — 3s")
            await asyncio.sleep(3)

    # Ana döngü — pencere-aware
    while True:
        now_utc   = datetime.now(timezone.utc)
        in_window = _in_metar_window(now_utc)

        # Pencere dışıysa: bir sonraki pencereye kadar uyu
        if not in_window:
            wait = _secs_to_next_window(now_utc)
            if wait > MGM_IDLE_INTERVAL:
                # Uzun süre pencere yok → 30s'de bir kontrol at (METAR erken gelebilir)
                log.debug(f"[MGM] Pencere dışı — {int(wait)}s sonra burst, şimdi idle poll")
                t0 = loop.time()
                try:
                    vz, metar, fetch_start_ms, fetch_end_ms = await loop.run_in_executor(
                        None, _mgm_fetch_sync, MGM_MERKEZID
                    )
                    if metar != state.last_metar_str:
                        # Beklenmedik zamanda yeni METAR — işle
                        temp       = parse_metar_temp(metar)
                        old_max    = state.daily_max_metar
                        is_new_max = False
                        state.last_fetch_start_ms    = fetch_start_ms
                        state.last_fetch_end_ms      = fetch_end_ms
                        state.last_detect_ms         = fetch_end_ms
                        state.last_fetch_duration_ms = fetch_end_ms - fetch_start_ms
                        state.prev_daily_max   = old_max
                        state.last_metar_str   = metar
                        state.last_veri_zamani = vz
                        state.last_metar_time  = datetime.fromtimestamp(fetch_end_ms / 1000, tz=timezone.utc)
                        state.metar_count     += 1
                        if temp is not None:
                            state.metar_temp = temp
                            if temp > state.daily_max_metar:
                                state.daily_max_metar = temp
                                is_new_max = True
                        detect_local = datetime.fromtimestamp(fetch_end_ms / 1000).strftime("%H:%M:%S.%f")[:-3]
                        log.info(
                            f"[MGM] YENİ METAR #{state.metar_count} (idle)  "
                            f"{detect_local} local  fetch={state.last_fetch_duration_ms}ms  "
                            f"temp={temp}°C  daily_max={state.daily_max_metar}°C"
                            + ("  *** YENİ MAX ***" if is_new_max else "")
                        )
                        await run_evaluation(state, manager, "METAR", date_str, is_new_max, clob_client)
                except Exception as e:
                    log.debug(f"[MGM] Idle fetch hatası: {e}")
                elapsed = loop.time() - t0
                await asyncio.sleep(max(0.0, MGM_IDLE_INTERVAL - elapsed))
                continue
            else:
                # Pencereye yakın — bekle
                log.debug(f"[MGM] Pencereye {int(wait)}s kaldı — bekleniyor")
                await asyncio.sleep(max(1.0, wait))
                continue

        # ── BURST penceresi: maksimum hız ──────────────────────────────
        t0 = loop.time()
        try:
            vz, metar, fetch_start_ms, fetch_end_ms = await loop.run_in_executor(
                None, _mgm_fetch_sync, MGM_MERKEZID
            )
            # ── Tespit anı: executor dönüşü = HTTP yanıtının geldiği an ──
            detect_ms = fetch_end_ms

            if metar != state.last_metar_str:
                temp       = parse_metar_temp(metar)
                old_max    = state.daily_max_metar
                is_new_max = False

                # Timing state'e yaz — daha önce hiçbir şey yapma
                state.last_fetch_start_ms    = fetch_start_ms
                state.last_fetch_end_ms      = fetch_end_ms
                state.last_detect_ms         = detect_ms
                state.last_fetch_duration_ms = fetch_end_ms - fetch_start_ms

                state.prev_daily_max   = old_max
                state.last_metar_str   = metar
                state.last_veri_zamani = vz
                state.last_metar_time  = datetime.fromtimestamp(detect_ms / 1000, tz=timezone.utc)
                state.metar_count     += 1

                if temp is not None:
                    state.metar_temp = temp
                    if temp > state.daily_max_metar:
                        state.daily_max_metar = temp
                        is_new_max = True

                detect_local = datetime.fromtimestamp(detect_ms / 1000).strftime("%H:%M:%S.%f")[:-3]
                log.info(
                    f"[MGM] YENİ METAR #{state.metar_count} [BURST]  "
                    f"{detect_local} local  fetch={state.last_fetch_duration_ms}ms  "
                    f"temp={temp}°C  daily_max={state.daily_max_metar}°C"
                    + ("  *** YENİ MAX ***" if is_new_max else "")
                )

                await run_evaluation(state, manager, "METAR", date_str, is_new_max, clob_client)
            else:
                log.debug(f"[MGM] Aynı [BURST]  fetch={fetch_end_ms - fetch_start_ms}ms")

        except Exception as e:
            log.debug(f"[MGM] Burst fetch hatası: {e}")
            # Pencerede timeout → hızlı retry
            await asyncio.sleep(0.5)
            continue

        elapsed = loop.time() - t0
        await asyncio.sleep(max(0.0, MGM_BURST_INTERVAL - elapsed))


async def wunder_poller(state: BotState, manager: MarketManager, date_str: str, clob_client=None):
    loop = asyncio.get_event_loop()
    log.info(f"[WUNDER] Poller başladı — {ANKARA_LAT},{ANKARA_LON}")

    # İlk fetch
    while True:
        try:
            data = await loop.run_in_executor(None, _wunder_fetch_sync, ANKARA_LAT, ANKARA_LON)
            exp   = data.get("expirationTimeUtc", [])
            state.forecast_temps        = data.get("temperature", [])
            state.forecast_valid_utcs   = data.get("validTimeUtc", [])
            state.forecast_valid_locals = data.get("validTimeLocal", [])
            state.forecast_exp_sentinel = min(exp) if exp else 0
            state.forecast_max_remaining = calc_forecast_max_remaining(
                state.forecast_temps, state.forecast_valid_utcs,
                datetime.now(timezone.utc),
            )
            state.last_forecast_time = datetime.now(timezone.utc)
            log.info(f"[WUNDER] Başlangıç: fc_max_remaining={state.forecast_max_remaining}°C")
            break
        except Exception as e:
            log.warning(f"[WUNDER] İlk fetch hatası: {e} — 5s")
            await asyncio.sleep(5)

    # Ana döngü
    while True:
        await asyncio.sleep(WUNDER_POLL_INTERVAL)
        try:
            data = await loop.run_in_executor(None, _wunder_fetch_sync, ANKARA_LAT, ANKARA_LON)
            exp  = data.get("expirationTimeUtc", [])
            new_sentinel = min(exp) if exp else 0

            state.forecast_temps        = data.get("temperature", [])
            state.forecast_valid_utcs   = data.get("validTimeUtc", [])
            state.forecast_valid_locals = data.get("validTimeLocal", [])
            state.last_forecast_time    = datetime.now(timezone.utc)

            old_fc = state.forecast_max_remaining
            state.forecast_max_remaining = calc_forecast_max_remaining(
                state.forecast_temps, state.forecast_valid_utcs,
                datetime.now(timezone.utc),
            )

            if new_sentinel != state.forecast_exp_sentinel:
                state.forecast_exp_sentinel = new_sentinel
                log.info(
                    f"[WUNDER] YENİ MODEL RUN  "
                    f"fc_max: {old_fc}°C → {state.forecast_max_remaining}°C"
                )
                # Forecast değişti → sinyalleri yeniden değerlendir
                await run_evaluation(state, manager, "FORECAST", date_str, is_new_max=False, clob_client=clob_client)
            else:
                state.forecast_exp_sentinel = new_sentinel
                log.debug(f"[WUNDER] Aynı model  fc_max={state.forecast_max_remaining}°C")

        except Exception as e:
            log.warning(f"[WUNDER] Fetch hatası: {e}")


async def price_change_recorder(
    manager: MarketManager,
    date_str: str,
):
    """
    WebSocket price_change event'lerini ham olarak kaydet.
    METAR zamanından önce/sonra fiyat hareketi analizi için.
    """
    # token_id → (label, outcome) önbelleği — her event'te registry aramasını önle
    _label_cache: dict = {}

    def _lookup(token_id: str):
        if token_id not in _label_cache:
            markets = manager.get_markets_by_city(CITY)
            m = markets.get(token_id)
            if m and m.bucket:
                _label_cache[token_id] = (m.bucket.label, m.outcome or "")
            else:
                _label_cache[token_id] = ("", "")
        return _label_cache[token_id]

    @manager.on_price_change
    async def _on_price_change(change: PWSPriceChange):
        label, outcome = _lookup(change.asset_id)
        save_price_change(change, date_str, label=label, outcome=outcome)

    log.info("[RECORDER] Price change recorder aktif")
    # Bu fonksiyon sadece callback kaydeder, döngüye girmez
    await asyncio.Future()  # sonsuza kadar bekle


async def balance_refresher(state: BotState, clob_client):
    """
    Bakiyeyi 60 saniyede bir önbellekle.
    METAR geldiğinde emir anında REST beklemeden hazır olsun.
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


async def rest_prefetcher(state: BotState, manager: MarketManager, clob_client=None):
    """
    Arkaplan: Olası dead bucket NO tokenlarının REST depth'ini önceden çek.
    METAR gelince fill_price zaten hesaplı olsun — emir anında REST bekleme yok.

    Kapsam:
      - NO tokenlar: bucket_max <= daily_max_metar + 1 (DEAD adayı)
      - YES tokenlar: type=higher ve bucket_min <= daily_max_metar + 1 (METAR eşiğe yaklaşınca)
    """
    loop = asyncio.get_event_loop()
    from infra.pws.rest import PolymarketREST
    _rest = PolymarketREST()

    while True:
        try:
            all_markets = manager.get_markets_by_city(CITY)
            if all_markets and state.daily_max_metar > -999:
                size_usdc = max(5.0, min(MAX_ORDER_USDC, max(state.cached_balance_usdc, 5.0)))
                no_candidates = [
                    (tid, m) for tid, m in all_markets.items()
                    if m.outcome == "NO"
                    and m.bucket is not None
                    and _bucket_max(m.bucket) is not None
                    and _bucket_max(m.bucket) <= state.daily_max_metar + 3
                ]
                # type=higher YES: daily_max bu eşiğe ulaşınca kesin YES
                yes_candidates = [
                    (tid, m) for tid, m in all_markets.items()
                    if m.outcome == "YES"
                    and m.bucket is not None
                    and m.bucket.type == "higher"
                    and _bucket_min(m.bucket) is not None
                    and _bucket_min(m.bucket) <= state.daily_max_metar + 2
                ]
                candidates = no_candidates + yes_candidates
                for tid, m in candidates:
                    if tid in state.ordered_token_ids:
                        continue  # zaten emir atıldı, boşuna çekme
                    try:
                        fresh = await loop.run_in_executor(None, _rest.get_book, tid)
                        if fresh and fresh.asks:
                            avg, depth_str, fill_price, total_tokens = _calc_avg_fill(fresh.asks, size_usdc)
                            if fill_price is not None:
                                state.prefetched_order_params[tid] = {
                                    "fill_price":      fill_price,
                                    "total_tokens":    round(total_tokens, 1),
                                    "fetch_ms":        int(time.time() * 1000),
                                    "best_ask":        fresh.asks[0].price,
                                    "depth_summary":   depth_str,
                                    "available_usdc":  round(sum(lvl.price * lvl.size for lvl in fresh.asks), 2),
                                }
                        # CLOB cache'ini ısıt — emir anında tick-size/neg-risk/fee-rate
                        # için ekstra HTTP isteği olmasın (~600ms tasarruf)
                        if clob_client is not None:
                            await loop.run_in_executor(None, clob_client.warm_cache, tid)
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
            f"\n[STATUS] METAR={state.metar_temp}°C  "
            f"daily_max={state.daily_max_metar}°C  "
            f"fc_max={state.forecast_max_remaining}°C  "
            f"METAR#{state.metar_count}  "
            f"snapshots={state.snapshot_count}  "
            f"books={n_books}/{len(markets)}"
        )


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

    # CLOB istemcisini yükle — credentials.env yoksa gözlem modunda çalış
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

    log.info(f"[MAIN] Ankara Bot başlatılıyor — {target_date}")
    await manager.discover()

    if not manager.registry:
        log.error("[MAIN] Polymarket'te Ankara marketi bulunamadı")
        return

    manager.print_discovery()
    await manager.connect()

    # WebSocket dinleyiciyi başlat (arka planda)
    asyncio.create_task(manager.stream.listen())

    log.info("[MAIN] İlk orderbook'ların gelmesi için 5s bekleniyor...")
    await asyncio.sleep(5)

    # Bakiyeyi hemen çek — METAR gelmeden hazır olsun
    if clob_client is not None:
        try:
            loop = asyncio.get_event_loop()
            bal_raw = await loop.run_in_executor(None, clob_client.get_balance)
            state.cached_balance_usdc = int(bal_raw.get("balance", "0")) / 1e6
            log.info(f"[BALANCE] Başlangıç bakiyesi: {state.cached_balance_usdc:.2f} USDC")
        except Exception as e:
            log.warning(f"[BALANCE] Başlangıç fetch hatası: {e}")

    tasks = [
        mgm_poller(state, manager, target_date, clob_client),
        wunder_poller(state, manager, target_date, clob_client),
        price_change_recorder(manager, target_date),
        status_printer(state, manager),
    ]
    if clob_client is not None:
        tasks.append(balance_refresher(state, clob_client))
        tasks.append(rest_prefetcher(state, manager, clob_client))

    await asyncio.gather(*tasks)


def main():
    parser = argparse.ArgumentParser(description="Ankara Dead Bucket Bot v2")
    parser.add_argument("--date", default=None, help="YYYY-MM-DD (default: today)")
    args = parser.parse_args()

    target_date = args.date or date.today().strftime("%Y-%m-%d")

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    # ── Tek instance kontrolü ──
    # Aynı anda birden fazla bot çalışırsa her METAR 3× kaydedilir.
    # fcntl.flock ile exclusive lock alınır; ikinci instance anında çıkar.
    lock_path = LOG_DIR / "ankara_bot.lock"
    lock_fh = open(lock_path, "w")
    try:
        fcntl.flock(lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        print(f"\nHATA: Başka bir ankara_bot örneği zaten çalışıyor!")
        print(f"      Lock: {lock_path}")
        print("      Önce çalışan botu durdurun (Ctrl+C veya kill).")
        sys.exit(1)

    print(f"""
╔══════════════════════════════════════════════════════════════════╗
║       ANKARA DEAD BUCKET BOT v2 — Observation Mode              ║
║  METAR: MGM ~1s  |  Forecast: Wunder 65s  |  PM: WebSocket      ║
╚══════════════════════════════════════════════════════════════════╝
  Tarih : {target_date}
  MGM   : merkezid={MGM_MERKEZID} (Ankara)
  Log   : {LOG_DIR}/
    → {target_date}_metar.jsonl         (METAR tetikli tam snapshot)
    → {target_date}_price_changes.jsonl (ham WebSocket akışı)
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
