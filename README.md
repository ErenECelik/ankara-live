# Ankara Dead Bucket Bot

Polymarket Ankara günlük maksimum sıcaklık marketleri için otomatik emir botu.
Sadece **garanti kazanan** pozisyonlara emir atar.

## Yapı

```
ankara_live/
├── bot.py                     ← giriş noktası (tüm mantık burada)
├── clob.py                    ← emir gönderme (buy_yes / buy_no / cancel)
├── credentials.env.example    ← kimlik bilgisi şablonu
├── requirements.txt
├── infra/pws → (symlink)      ← WebSocket + discovery altyapısı
└── logs/ankara_signals/
    ├── YYYY-MM-DD_metar.jsonl         ← METAR tetikli tam orderbook snapshot
    ├── YYYY-MM-DD_price_changes.jsonl ← ham WebSocket fiyat akışı
    └── YYYY-MM-DD_ordered.txt         ← başarılı emirlerin token_id listesi
```

## Hızlı başlangıç

```bash
cd /home/eren/ankara_live
/home/eren/pws/venv/bin/python bot.py
```

Belirli tarih için:
```bash
/home/eren/pws/venv/bin/python bot.py --date 2026-02-28
```

**Dikkat**: Aynı anda tek instance çalıştır. İkinci instance `ankara_bot.lock` hatası verir.

---

## Strateji — Sadece Garanti Pozisyonlar

Bot **iki** senaryoda emir atar, her ikisi de kesin kazanan:

### A) DEAD Bucket — BUY_NO (ana strateji)

```
Koşul: bucket_max < daily_max_metar (METAR kaynaklı)
       ör: bucket "≤11°C", METAR günlük max=12°C → bu bucket asla YES olmaz
Emir : BUY_NO, sweep fiyatı (fill_price), MAX_ORDER_USDC limiti
Neden garanti: Günlük max bu bucket'ın üst sınırını aştı → YES = 0 kesin
```

### B) Higher Bucket — BUY_YES (bonus)

```
Koşul: bucket type="higher" (ör: ≥13°C) AND METAR daily_max >= bucket alt sınırı
       ör: bucket "≥13°C", METAR günlük max=14°C → bu bucket kesin YES
Emir : BUY_YES, sweep fiyatı, MAX_ORDER_USDC limiti
Neden garanti: Üst sınırsız bucket, METAR eşiği geçti → YES = 1 kesin
```

### Emir ATILMAYAN sinyaller (sadece ekranda görünür)

| Sinyal | Neden emir yok |
|--------|----------------|
| BUY_YES — LIVE bucket (soğuma) | Spekülatif, garanti değil |
| BUY_YES — LIVE bucket, MEDIUM | Zirve sabitlenmedi, belirsiz |
| BUY_NO — NO ask > 0.95 | Getiri < %5.3, anlamsız |

---

## Emir Mekanizması

**Tetikleyici**: Sadece yeni METAR (MGM kaynaklı). Forecast tetiklemez.

**Sweep emri**: REST'ten tam depth çekilir, fill_price hesaplanır.
- `price = fill_price` (en derin seviyeye kadar kapsar)
- `size = total_tokens` (o price'a kadar alınacak tüm token)
- → Tüm ask seviyelerini süpürür, resting bid kalmaz

**Pre-fetch** (`rest_prefetcher`): Arka planda 2s'de bir, `daily_max + 1` eşiğindeki
NO tokenlarının depth'ini günceller. METAR gelince fill_price zaten hazır → sıfır ek gecikme.

**Bakiye önbelleği**: 60s'de bir güncellenir. Emir anında REST çağrısı yok.

**Boyut**: `max(5.0, min(MAX_ORDER_USDC, cached_balance_usdc))`

**Çift emir koruması**: `{tarih}_ordered.txt` → restart sonrası aynı token tekrar işlenmez.

---

## Veri Kaynakları

| Kaynak | Sıklık | Amaç |
|--------|--------|------|
| MGM (`servis.mgm.gov.tr`) | Burst: ~0.3s (xx:19-27 ve xx:49-57 UTC) / Idle: 30s | METAR → günlük max → dead bucket tespiti |
| Wunderground | 65s | Saatlik tahmin → bilgi amaçlı, emir kararına etki etmez |
| Polymarket WebSocket | Sürekli | Orderbook canlı akış |
| Polymarket REST | Pre-fetch 2s | Sweep fiyatı hesabı |

---

## Bucket Kategorisi ve DEAD Mantığı

```
_bucket_max() hesabı:
  exact (11°C)    → bucket_max = low  = 11  → daily_max=12 → DEAD ✓
  range (11-12°C) → bucket_max = high = 12  → daily_max=12 → DEAD DEĞİL! (LIVE)
  below (≤11°C)   → bucket_max = high = 11  → daily_max=12 → DEAD ✓
  higher (≥13°C)  → bucket_max = 999        → asla DEAD olmaz

_categorize_bucket():
  bucket_max < daily_max  → DEAD
  _bucket_contains(temp)  → LIVE
  diğer                   → ABOVE
```

**Terminalde** "Tip" kolonu: tek=exact, aralık=range, altı=below, üstü=higher.

---

## Sabitler (bot.py)

| Sabit | Değer | Açıklama |
|-------|-------|----------|
| `MAX_ORDER_USDC` | 50.0 | Emir başına max USDC |
| `MAX_NO_ASK_THRESHOLD` | 0.95 | NO ask üstü → emir atılmaz |
| `MGM_BURST_INTERVAL` | 0.3s | Burst penceresi poll hızı |
| `MGM_IDLE_INTERVAL` | 30s | Pencere dışı poll hızı |
| `MGM_MERKEZID` | 90626 | Ankara Esenboğa / Merkez |

---

## Kısıtlamalar

- **Satış yok**: `clob.py`'de sell() yok. Manuel satış: Polymarket UI'dan 0.999 limit sat.
  Kilidi kaldırmak için: `{tarih}_ordered.txt`'den token_id satırını sil, botu restart et.
- **Bot sadece alım yapıyor**: Pozisyon açıldıktan sonra bot ilgilenmez.
- **DRY_RUN**: `credentials.env` içinde `DRY_RUN=true/false` → false yapmadan önce dry-run ile test et.

---

## Log Dosyaları

`{tarih}_metar.jsonl`: ~10-20 satır/gün. Her METAR'da tam snapshot:
timing (ms), METAR string, tüm bucket'lar (YES+NO full depth), tension'lar, sinyaller.

`{tarih}_price_changes.jsonl`: ~50-200MB/gün. Ham WebSocket akışı.

`{tarih}_ordered.txt`: Başarılı emirlerin token_id listesi (crash koruması için).

---

## CLOB API Kurulumu

### 1. credentials.env oluştur

```bash
cp credentials.env.example credentials.env
```

### 2. API key türet

```bash
/home/eren/pws/venv/bin/python -c "
from clob import derive_api_key
derive_api_key('0xYOUR_PRIVATE_KEY')
"
```

Çıktıdaki `CLOB_API_KEY`, `CLOB_API_SECRET`, `CLOB_API_PASSPHRASE` değerlerini
`credentials.env` dosyasına yapıştır.

### 3. Bakiye testi (DRY_RUN=true)

```bash
/home/eren/pws/venv/bin/python -c "
from clob import ClobOrderClient
c = ClobOrderClient.from_env()
print('Bakiye:', c.get_balance())
"
```

### 4. Canlı emir

`credentials.env` içinde `DRY_RUN=false` yap. Bot otomatik algılar.

---

## Önemli Notlar

- **MGM avantajı**: Türk istasyonlar için MGM muhtemelen NOAA'dan 1-3 dakika daha hızlı.
- **lock dosyası**: `logs/ankara_signals/ankara_bot.lock` — process ölürse otomatik serbest kalır.
- **book_age_ms**: "Son herhangi bir market aktivitesinden bu yana" — fiyat hareketsizse book eski değil demektir.
- Wellington ve Ankara botları aynı anda çalışabilir (farklı lock dosyaları).
