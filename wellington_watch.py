"""
Wellington Hava İstasyonu Gözlemci
Kaynak: MetService 93439 (Wellington Aero AWS)

METAR raporları xx:00 ve xx:30 UTC'de yayınlanır.
Sadece bu pencereler etrafında poll ederiz:
  Pencere 1: :58 → :05  (xx:00 METAR için)
  Pencere 2: :28 → :35  (xx:30 METAR için)

CDN TTL ≈ 21 saniye (Varnish).  Age header takip edilerek CDN expire
olmadan BURST_PRE saniye önce burst poll başlar (BURST_INT aralıkla).
Bekleme süresi = CDN_TTL - age - BURST_PRE  →  yalnızca expire yakınında yoğun istek.
"""
import time
import json
import sys
import urllib.request
from datetime import datetime, timezone, timedelta

URL      = "https://www.metservice.com/publicData/webdata/module/weatherStationCurrentConditions/93439"
OUT_FILE = "wellington_obs.jsonl"
HEADERS  = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":          "application/json, text/plain, */*",
    "Cache-Control":   "no-cache, no-store, must-revalidate",
    "Pragma":          "no-cache",
    "Referer":         "https://www.metservice.com/towns-cities/locations/wellington",
}

CDN_TTL   = 21   # saniye — Varnish TTL (ölçülen, adaptif güncellenir)
BURST_INT = 0.2  # saniye — CDN expire yakınında poll aralığı
BURST_PRE = 3    # saniye — CDN expire'dan kaç saniye önce burst başlasın


def in_window(now_utc: datetime) -> bool:
    m = now_utc.minute
    if m >= 58:       return True
    if m <= 5:        return True
    if 28 <= m <= 35: return True
    return False


def secs_to_next_window(now_utc: datetime) -> float:
    m = now_utc.minute
    s = now_utc.second
    elapsed = m * 60 + s
    starts  = [28 * 60, 58 * 60]
    for start in sorted(starts):
        if elapsed < start:
            return start - elapsed
    return (60 * 60 - elapsed) + 28 * 60


def fetch():
    """
    (data_dict, last_modified, age_seconds) döndürür.
    age_seconds: CDN cache'in kaç saniye önce çekildiği (None = CDN yeni refresh yaptı).
    """
    req = urllib.request.Request(URL, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=8) as r:
        last_modified = r.headers.get("Last-Modified", "")
        age_raw       = r.headers.get("Age", "")
        age           = int(age_raw) if age_raw and age_raw.isdigit() else None
        data          = json.loads(r.read())
    return data, last_modified, age


def extract(data):
    obs  = data.get("observations", {})
    temp = obs.get("temperature", [{}])[0].get("current")
    wind = obs.get("wind", [{}])[0]
    rain = obs.get("rain", [{}])[0]
    pres = obs.get("pressure", [{}])[0]
    return {
        "temp_c":    temp,
        "feels_c":   obs.get("temperature", [{}])[0].get("feelsLike"),
        "wind_dir":  wind.get("direction"),
        "wind_avg":  wind.get("averageSpeed"),
        "wind_gust": wind.get("gustSpeed"),
        "humidity":  rain.get("relativeHumidity"),
        "rainfall":  rain.get("rainfall"),
        "pressure":  pres.get("atSeaLevel"),
    }


def main():
    global CDN_TTL

    last_modified     = None
    last_temp         = None
    fetch_count       = 0
    change_count      = 0
    cdn_refresh_times = []   # CDN TTL'i adaptif ölçmek için

    print("Wellington izleme başladı — Age-aware burst polling")
    print(f"  Pencere 1: :58→:05 UTC  (xx:00 METAR)")
    print(f"  Pencere 2: :28→:35 UTC  (xx:30 METAR)")
    print(f"  CDN TTL başlangıç: {CDN_TTL}s  |  Burst: {BURST_PRE}s önce, {BURST_INT}s aralık")
    print(f"  Çıktı: {OUT_FILE}")
    print("-" * 60)

    with open(OUT_FILE, "a") as f:
        while True:
            now_utc = datetime.now(timezone.utc)

            if not in_window(now_utc):
                wait     = secs_to_next_window(now_utc)
                next_dt  = now_utc + timedelta(seconds=wait)
                deadline = time.time() + wait
                while True:
                    remaining = deadline - time.time()
                    if remaining <= 0:
                        break
                    now_live = datetime.now(timezone.utc)
                    print(
                        f"  [{now_live.strftime('%H:%M:%S')} UTC]  pencere dışı — "
                        f"sonraki: {next_dt.strftime('%H:%M:%S')} UTC  ({int(remaining):4d}s)   ",
                        end="\r", flush=True,
                    )
                    time.sleep(1)
                print()
                continue

            # ── Pencere içindeyiz: poll et ─────────────────────────────────
            t0 = time.time()
            age = None
            try:
                data, lm, age = fetch()
                fields   = extract(data)
                temp     = fields["temp_c"]
                fetch_ms = round((time.time() - t0) * 1000)
                fetch_count += 1

                # CDN TTL adaptif ölçüm (Age=None = CDN yeni refresh yaptı)
                if age is None:
                    cdn_refresh_times.append(time.time())
                    if len(cdn_refresh_times) >= 2:
                        observed = cdn_refresh_times[-1] - cdn_refresh_times[-2]
                        if 10 < observed < 60:
                            CDN_TTL = round(observed)

                # Yeni veri mi?
                if lm and lm != last_modified:
                    change_count += 1
                    window = "h00" if now_utc.minute <= 5 or now_utc.minute >= 58 else "h30"
                    record = {
                        "ts_detect_utc": now_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
                        "ts_data_utc":   lm,
                        "fetch_ms":      fetch_ms,
                        "cdn_age_s":     age,
                        "new_temp_c":    temp,
                        "prev_temp_c":   last_temp,
                        "window":        window,
                        **fields,
                    }
                    f.write(json.dumps(record) + "\n")
                    f.flush()

                    age_str = f"{age}s" if age is not None else "FRESH"
                    print(
                        f"\n  [{now_utc.strftime('%H:%M:%S')} UTC]  "
                        f"*** YENİ VERİ  {last_temp}°C → {temp}°C  "
                        f"data={lm[17:25]}  age={age_str}  "
                        f"wind={fields['wind_avg']}km/h {fields['wind_dir']}  "
                        f"fetch={fetch_ms}ms"
                    )
                    last_modified = lm
                    last_temp     = temp
                else:
                    age_str  = f"{age}s" if age is not None else "FRESH"
                    burst_in = max(0, CDN_TTL - (age or 0) - BURST_PRE)
                    burst_flag = " [BURST]" if (age is not None and age >= CDN_TTL - BURST_PRE) else f" (burst in {burst_in:.0f}s)"
                    print(
                        f"  [{now_utc.strftime('%H:%M:%S')} UTC]  "
                        f"cache  temp={temp}°C  age={age_str}/TTL={CDN_TTL}s  "
                        f"{fetch_count}req/{change_count}new  {fetch_ms}ms{burst_flag}",
                        end="\r", flush=True,
                    )

            except Exception as e:
                print(f"\n  [{now_utc.strftime('%H:%M:%S')} UTC]  HATA: {e}")

            sys.stdout.flush()

            # ── Age-aware uyku ─────────────────────────────────────────────
            # age=None  → CDN taze → bir sonraki expire'a kadar uyu
            # age büyük → burst zone → çok kısa uyu
            # age küçük → sıradaki burst öncesi uyu
            elapsed = time.time() - t0
            if age is None:
                # Yeni taze veri çekildi, bir sonraki CDN expire'ına kadar uyu
                sleep_t = CDN_TTL - BURST_PRE - elapsed
            elif age >= CDN_TTL - BURST_PRE:
                # Burst zone: BURST_INT aralıkla poll
                sleep_t = BURST_INT - elapsed
            else:
                # Burst başlamadan önceki bekleme
                sleep_t = (CDN_TTL - age - BURST_PRE) - elapsed

            time.sleep(max(0.05, sleep_t))


if __name__ == "__main__":
    main()
