"""
Price change logunu okunabilir formatta göster.

Kullanım:
    python show_prices.py                   # bugün
    python show_prices.py --date 2026-03-05 # belirli gün
    python show_prices.py --label "11°C"    # sadece belirli bucket
    python show_prices.py --tail 50         # son N satır
"""
import json, sys, argparse
from datetime import date
from pathlib import Path

LOG_DIR = Path(__file__).parent / "logs" / "ankara_signals"

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--date",  default=date.today().strftime("%Y-%m-%d"))
    p.add_argument("--label", default=None, help="Bucket filtresi: '11°C', '≤10°C' vb.")
    p.add_argument("--tail",  type=int, default=0, help="Son N satır")
    p.add_argument("--no",    action="store_true", help="Sadece NO tokenlar")
    p.add_argument("--yes",   action="store_true", help="Sadece YES tokenlar")
    args = p.parse_args()

    path = LOG_DIR / f"{args.date}_price_changes.jsonl"
    if not path.exists():
        print(f"Dosya yok: {path}")
        sys.exit(1)

    lines = path.read_text().splitlines()
    if args.tail:
        lines = lines[-args.tail:]

    records = []
    for line in lines:
        try:
            records.append(json.loads(line))
        except Exception:
            continue

    # Filtrele
    if args.label:
        records = [r for r in records if args.label in r.get("label", "")]
    if args.no:
        records = [r for r in records if r.get("outcome") == "NO"]
    if args.yes:
        records = [r for r in records if r.get("outcome") == "YES"]

    if not records:
        print("Kayıt yok.")
        return

    # Başlık
    print(f"\n{'Saat':<10} {'Bucket':<12} {'Taraf':<5} {'İşlem':<5} {'Fiyat':>6} {'Miktar':>8}  {'Bid':>6} {'Ask':>6} {'Mid':>6}")
    print("─" * 75)

    prev_label = None
    for r in records:
        label   = r.get("label", "?") or "?"
        outcome = r.get("outcome", "?") or "?"
        ts      = r.get("ts_utc", "")[-9:-1]  # HH:MM:SS
        price   = r.get("price", 0)
        size    = r.get("size", 0)
        side    = r.get("side", "?")
        bid     = r.get("best_bid") or 0
        ask     = r.get("best_ask") or 0
        mid     = r.get("mid") or 0

        # Bucket değişince boş satır
        cur_key = f"{label}{outcome}"
        if prev_label is not None and cur_key != prev_label:
            print()
        prev_label = cur_key

        side_str = "AL" if side == "BUY" else "SAT"
        print(
            f"{ts:<10} {label:<12} {outcome:<5} {side_str:<5} "
            f"{price:>6.3f} {size:>8.1f}  "
            f"{bid:>6.3f} {ask:>6.3f} {mid:>6.3f}"
        )

    print(f"\nToplam: {len(records)} işlem")

if __name__ == "__main__":
    main()
