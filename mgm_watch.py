"""
MGM METAR İzleyici — Pencereli Burst Polling

METAR raporları standart olarak xx:00 ve xx:30 UTC'de yayınlanır.
MGM genellikle birkaç dakika geç yayınlar.

Strateji:
  Pencere dışı : 30s'de bir idle poll (sunucu yükü minimal, block riski düşük)
  Pencere içi  : 0.3s aralıkla burst (yeni METAR anında tespit)

Pencereler (UTC):
  Pencere 1: :59 → :06  (xx:00 METAR için)
  Pencere 2: :29 → :36  (xx:30 METAR için)
"""
import requests, time, re, sys
from datetime import datetime, timezone

HEADERS = {
    "Origin":        "https://www.mgm.gov.tr",
    "Referer":       "https://www.mgm.gov.tr/",
    "User-Agent":    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":        "application/json, */*",
    "Cache-Control": "no-cache, no-store, must-revalidate",
    "Pragma":        "no-cache",
}

IDLE_INTERVAL  = 30.0  # saniye — pencere dışı poll aralığı
BURST_INTERVAL = 0.3   # saniye — burst poll aralığı
BURST_PRE      = 60    # saniye — pencereden kaç saniye önce burst başlasın


def parse_temp(metar):
    m = re.search(r'\b(M?\d{2})/(M?\d{2})\b', metar)
    if not m:
        return None
    raw = m.group(1)
    return -float(raw[1:]) if raw.startswith('M') else float(raw)


def in_window(now_utc: datetime) -> bool:
    m = now_utc.minute
    return (m >= 59) or (m <= 6) or (29 <= m <= 36)


def secs_to_next_window(now_utc: datetime) -> float:
    m = now_utc.minute
    s = now_utc.second
    elapsed = m * 60 + s
    starts = [29 * 60, 59 * 60]
    for start in sorted(starts):
        if elapsed < start:
            return start - elapsed - BURST_PRE
    return (60 * 60 - elapsed) + 29 * 60 - BURST_PRE


def fetch():
    ts = int(time.time() * 1000)
    r  = requests.get(
        "https://servis.mgm.gov.tr/web/sondurumlar",
        params={"merkezid": 90626, "_": ts},
        headers=HEADERS,
        timeout=(3, 8),
    )
    r.raise_for_status()
    d = r.json()[0]
    return d.get("rasatMetar", ""), d.get("veriZamani", "")


def main():
    daily_max  = -999
    last_metar = ""
    fetch_count = 0
    change_count = 0

    print("MGM METAR izleniyor — pencereli burst polling (Ctrl+C ile dur)\n")
    print(f"  Pencere 1: :19→:27 UTC  (xx:20 METAR)")
    print(f"  Pencere 2: :49→:57 UTC  (xx:50 METAR)")
    print(f"  Burst: {BURST_INTERVAL}s  |  Idle: {IDLE_INTERVAL}s")
    print("-" * 60)

    while True:
        now_utc   = datetime.now(timezone.utc)
        burst_mode = in_window(now_utc)

        # Pencere dışı ve uzun süre pencere yok → idle poll sonra uyu
        if not burst_mode:
            wait = secs_to_next_window(now_utc)
            if wait > IDLE_INTERVAL:
                t0 = time.time()
                try:
                    metar, vz = fetch()
                    temp = parse_temp(metar)
                    fetch_count += 1
                    if temp is not None and temp > daily_max:
                        daily_max = temp
                    if metar != last_metar:
                        change_count += 1
                        last_metar = metar
                        print(f"\n[{time.strftime('%H:%M:%S')}] {vz[11:16]} UTC  "
                              f"T={temp}°C  max={daily_max}°C  ★ YENİ METAR (idle)")
                        print(f"  {metar}\n")
                    else:
                        print(
                            f"  [{now_utc.strftime('%H:%M:%S')} UTC]  idle  "
                            f"T={temp}°C  max={daily_max}°C  "
                            f"{fetch_count}req/{change_count}new  "
                            f"sonraki pencere: {int(wait)}s   ",
                            end="\r", flush=True,
                        )
                except Exception as e:
                    print(f"\n  [{now_utc.strftime('%H:%M:%S')} UTC]  idle HATA: {e}   ")
                elapsed = time.time() - t0
                time.sleep(max(0.0, IDLE_INTERVAL - elapsed))
                continue
            else:
                # Pencereye yakın — bekle
                print(
                    f"  [{now_utc.strftime('%H:%M:%S')} UTC]  "
                    f"pencereye {int(wait)}s kaldı — bekleniyor   ",
                    end="\r", flush=True,
                )
                time.sleep(max(1.0, min(wait, 5.0)))
                continue

        # ── BURST penceresi ────────────────────────────────────────────
        t0 = time.time()
        try:
            metar, vz = fetch()
            temp = parse_temp(metar)
            fetch_count += 1

            if temp is not None and temp > daily_max:
                daily_max = temp

            if metar != last_metar:
                change_count += 1
                last_metar = metar
                print(f"\n[{time.strftime('%H:%M:%S')}] {vz[11:16]} UTC  "
                      f"T={temp}°C  max={daily_max}°C  ★ YENİ METAR [BURST]")
                print(f"  {metar}\n")
            else:
                print(
                    f"  [{now_utc.strftime('%H:%M:%S')} UTC]  [BURST]  "
                    f"T={temp}°C  max={daily_max}°C  "
                    f"{fetch_count}req/{change_count}new   ",
                    end="\r", flush=True,
                )

        except Exception as e:
            print(f"\n  [{now_utc.strftime('%H:%M:%S')} UTC]  burst HATA: {e}")
            time.sleep(0.5)
            continue

        elapsed = time.time() - t0
        time.sleep(max(0.0, BURST_INTERVAL - elapsed))


if __name__ == "__main__":
    main()
