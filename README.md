# traffic-node-proxy

Edge device upload proxy — receives pedestrian attribute JSON from edge AI devices, queues via Redis, writes to MySQL, and keeps the Spring Boot realtime cache in sync.

---

## Architecture

```
Edge Device (.NET)
    │ POST /upload
    ▼
Node Proxy (Express :3000)     ← validate + LPUSH, return 200 immediately
    │
    ▼ Redis List: traffic:upload:queue
    │
Worker (same process, BRPOP)
    ├─ bindId → merchantId  (cached in Redis 10 min)
    ├─ Parse 26-dim attributes
    ├─ INSERT ... ON DUPLICATE KEY UPDATE traffic_fact
    └─ Recompute today summary → SET traffic:realtime:{merchantId}
                                         ▲
                       Spring Boot reads ┘  miniprogram polls Spring Boot
```

## Stack

- Node.js 18+
- Express — HTTP server
- ioredis — Redis queue + cache
- mysql2 — MySQL pool (same DB as Spring Boot backend)

---

## Setup

```bash
cp .env.example .env
# Edit .env with your DB/Redis credentials

npm install
npm start
```

## API

### `POST /upload`

Accepts the vendor JSON format (see vendor doc). Returns immediately after queuing.

**Request body:**
```json
{
  "systemId": "EA-01234567",
  "systemName": "边缘AI平台",
  "bindId": "BIND-001",
  "personDetections": [
    {
      "id": 12345,
      "personId": "a1b2c3d4e5f6",
      "deviceId": "cam-0",
      "entryTime": "2026-04-10T08:30:00Z",
      "exitTime": "2026-04-10T09:15:00Z",
      "entryCount": 1,
      "exitCount": 1,
      "isPassThrough": false,
      "lastSeenAt": "2026-04-10T09:15:00Z",
      "createdAt": "2026-04-10T08:30:00Z",
      "attributes": [26 floats, 0.0~1.0]
    }
  ]
}
```

**Response:**
```json
{ "code": 0, "message": "success" }
```

### `GET /health`
```json
{ "status": "ok" }
```

---

## 26-Dim Attribute Mapping

| Index | Name | → traffic_fact column |
|-------|------|-----------------------|
| 0 | Hat | accessory_hat |
| 1 | Glasses | accessory_glasses |
| 2/3/10 | ShortSleeve/LongSleeve/LongCoat (exclusive) | upper_short / upper_long / upper_coat |
| 4–7 | Upper styles (multi) | upper_style_stripe/logo/plaid/splice |
| 8–9 | Lower styles (multi) | lower_style_stripe/pattern |
| 11/12/13 | Trousers/Shorts/Skirt (exclusive) | lower_trousers / lower_shorts / lower_skirt |
| 14 | Boots | accessory_boots |
| 15/16/17 | HandBag/ShoulderBag/Backpack | bag_handbag / bag_shoulder / bag_backpack |
| 18 | HoldObjectsInFront | hold_item |
| 19/20/21 | Age (exclusive) | age_under18 / age_18_60 / age_over60 |
| 22 | Female (exclusive) | gender_male / gender_female |

Threshold: 0.5 for all attributes. Exclusive groups use argmax.

---

## bindId Setup

The hardware sends `bindId` in the payload. This must match the `bind_id` column in the `merchant` table:

```sql
UPDATE merchant SET bind_id = 'BIND-001' WHERE id = 1;
```

The proxy caches `bindId → merchantId` in Redis for 10 minutes.

---

## Notes

- `traffic_fact` has a UNIQUE KEY on `(merchant_id, device_id, time_bucket)`. All writes use `ON DUPLICATE KEY UPDATE` to accumulate counts — no lock contention.
- `time_bucket` is minute-level (seconds = 0), stored in Asia/Shanghai time.
- After each write the worker refreshes `traffic:realtime:{merchantId}` in the same format as the Spring Boot backend, so the miniprogram dashboard stays up to date without any Spring Boot changes.
- The Spring Boot `POST /api/device/upload` endpoint remains available for legacy/direct uploads.
