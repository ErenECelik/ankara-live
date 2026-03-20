"""
Wellington METAR vs Fiyat Analizi

Kullanım:
  python3 analyze_metar.py [tarih]          # tarih: 2026-03-03 (default: bugün)
  python3 analyze_metar.py --window 10      # ±10 dk fiyat penceresi (default: 5)
  python3 analyze_metar.py --bucket "14°C"  # sadece bu bucket
"""
import json
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ── Ayarlar ───────────────────────────────────────────────────────────
LOG_DIR    = Path(__file__).parent / "logs" / "wellington_signals"
WINDOW_MIN = 5       # her temp değişimi etrafında ±N dakika price snapshot


def load_temp_changes(obs_path: Path) -> list[dict]:
    """obs.jsonl → sadece temp değişimi olan satırlar."""
    changes = []
    prev_temp = None
    with open(obs_path) as f:
        for line in f:
            r = json.loads(line)
            t = r.get("obs_temp")
            if t != prev_temp:
                changes.append(r)
                prev_temp = t
    return changes


def load_price_index(pc_path: Path) -> dict[str, list[dict]]:
    """price_changes.jsonl → bucket bazlı liste {bucket_outcome: [records]}"""
    idx: dict[str, list] = {}
    with open(pc_path) as f:
        for line in f:
            r = json.loads(line)
            key = f"{r.get('bucket')} {r.get('outcome')}"
            idx.setdefault(key, []).append(r)
    return idx


def nearest_price(records: list[dict], ts: datetime, direction: str) -> tuple[float | None, float | None, str]:
    """
    ts'e en yakın best_bid/best_ask'ı bul.
    direction: "before" veya "after"
    """
    window = timedelta(minutes=WINDOW_MIN)
    candidates = []
    for r in records:
        try:
            rts = datetime.strptime(r["ts_utc"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except Exception:
            continue
        diff = (rts - ts).total_seconds()
        if direction == "before" and -window.total_seconds() <= diff <= 0:
            candidates.append((abs(diff), r))
        elif direction == "after" and 0 < diff <= window.total_seconds():
            candidates.append((diff, r))

    if not candidates:
        return None, None, ""
    _, best = min(candidates, key=lambda x: x[0])
    ts_str = best["ts_utc"][11:19]
    return best.get("best_bid"), best.get("best_ask"), ts_str


def bucket_for_temp(temp: float, daily_max: float) -> str:
    """Hangi bucket izlenmeli? daily_max'ı kapsayan bucket."""
    # Basit yuvarlama: 14.2 → "14°C", 14.6 → "15°C" (0.5 boundary)
    import math
    rounded = math.floor(daily_max + 0.5)
    if daily_max >= 18:
        return "≥18°C"
    return f"{rounded}°C"


def parse_args():
    import argparse
    p = argparse.ArgumentParser(description="METAR vs fiyat analizi")
    p.add_argument("date", nargs="?", default=datetime.now().strftime("%Y-%m-%d"))
    p.add_argument("--window", type=int, default=5, help="±dakika pencere (default 5)")
    p.add_argument("--bucket", default=None, help='sadece bu bucket: "14°C"')
    p.add_argument("--all-buckets", action="store_true", help="tüm bucket'ları göster")
    return p.parse_args()


def main():
    global WINDOW_MIN
    args = parse_args()
    WINDOW_MIN = args.window

    date_str = args.date
    obs_path = LOG_DIR / f"{date_str}_obs.jsonl"
    pc_path  = LOG_DIR / f"{date_str}_price_changes.jsonl"

    if not obs_path.exists():
        print(f"Dosya yok: {obs_path}")
        sys.exit(1)
    if not pc_path.exists():
        print(f"Dosya yok: {pc_path}")
        sys.exit(1)

    print(f"\n=== Wellington METAR Analizi — {date_str} (±{WINDOW_MIN}dk pencere) ===\n")

    temp_changes = load_temp_changes(obs_path)
    price_idx    = load_price_index(pc_path)

    print(f"Toplam temp değişimi: {len(temp_changes)}")

    # ── Tablo başlığı ────────────────────────────────────────────────
    print()
    header = f"{'Saat':8}  {'Temp':6}  {'DailyMax':8}  {'Window':5}  {'Bucket':8}  "
    header += f"{'YES_ÖNCESİ':18}  {'YES_SONRASI':18}  {'NO_ÖNCESİ':18}  {'NO_SONRASI':18}"
    print(header)
    print("-" * len(header))

    for r in temp_changes:
        ts_str  = r["ts_utc"]
        temp    = r.get("obs_temp")
        dmax    = r.get("daily_max")
        window  = r.get("metar_window", "") or r.get("window", "")
        is_max  = r.get("is_new_max", False)

        try:
            ts = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
        except Exception:
            try:
                ts = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            except Exception:
                continue

        # Hangi bucket'ı izle?
        focus_bucket = args.bucket or bucket_for_temp(temp, dmax)

        yes_key = f"{focus_bucket} YES"
        no_key  = f"{focus_bucket} NO"

        yes_records = price_idx.get(yes_key, [])
        no_records  = price_idx.get(no_key, [])

        yb_bid, yb_ask, yb_t = nearest_price(yes_records, ts, "before")
        ya_bid, ya_ask, ya_t = nearest_price(yes_records, ts, "after")
        nb_bid, nb_ask, nb_t = nearest_price(no_records,  ts, "before")
        na_bid, na_ask, na_t = nearest_price(no_records,  ts, "after")

        def fmt(bid, ask, t):
            if bid is None:
                return f"{'—':18}"
            mid = (bid + ask) / 2 if ask else bid
            return f"{t} {bid:.3f}/{ask:.3f}={mid:.3f}"

        saat     = ts.strftime("%H:%M:%S")
        max_flag = "MAX!" if is_max else ""
        win_disp = window or "—"

        line = (
            f"{saat:8}  {temp:5.1f}°  {dmax:7.1f}°  {win_disp:5}  "
            f"{focus_bucket:8}  "
            f"{fmt(yb_bid, yb_ask, yb_t):18}  "
            f"{fmt(ya_bid, ya_ask, ya_t):18}  "
            f"{fmt(nb_bid, nb_ask, nb_t):18}  "
            f"{fmt(na_bid, na_ask, na_t):18}  "
            f"{max_flag}"
        )
        print(line)

        # Eğer --all-buckets istenmişse tüm bucket'ları da göster
        if args.all_buckets and (yes_records or no_records):
            for bk in sorted(price_idx.keys()):
                if bk == yes_key or bk == no_key:
                    continue
                bname, bside = bk.rsplit(" ", 1)
                recs = price_idx[bk]
                bb, ba, bt = nearest_price(recs, ts, "before")
                if bb is not None:
                    print(f"  {'':8}  {'':6}  {'':8}  {'':5}  {bname:8}  {bside:3}  {bt} {bb:.3f}/{ba:.3f}")

    # ── Özet ─────────────────────────────────────────────────────────
    print()
    print("=== Bucket fiyat özeti (gün sonu) ===")
    for key in sorted(price_idx.keys()):
        recs = price_idx[key]
        if not recs:
            continue
        last  = recs[-1]
        first = recs[0]
        print(
            f"  {key:20}  ilk: {first['best_bid']:.3f}/{first['best_ask']:.3f}"
            f"  → son: {last['best_bid']:.3f}/{last['best_ask']:.3f}"
            f"  ({len(recs):4} kayıt)"
        )


if __name__ == "__main__":
    main()
