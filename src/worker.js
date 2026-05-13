const db    = require('./db');
const redis = require('./redis');
const { parseAttributes, toTimeBucket } = require('./parser');

const QUEUE_KEY    = process.env.QUEUE_KEY    || 'traffic:upload:queue';
const IDLE_MS      = parseInt(process.env.WORKER_IDLE_MS || '500');
const CACHE_PREFIX = 'traffic:realtime:';
const BIND_CACHE   = 'traffic:bindid:';  // bindId → merchantId, TTL 10 min

let running = false;

// ── bindId → merchantId lookup (Redis-cached) ─────────────────────────────
async function getMerchantId(bindId) {
  if (!bindId) return null;
  const cacheKey = BIND_CACHE + bindId;
  const cached = await redis.get(cacheKey);
  if (cached) return parseInt(cached);

  const [rows] = await db.query(
    'SELECT id FROM merchant WHERE bind_id = ? AND status != 2 LIMIT 1',
    [bindId]
  );
  if (!rows.length) return null;

  const merchantId = rows[0].id;
  await redis.setex(cacheKey, 600, merchantId);  // cache 10 min
  return merchantId;
}

// ── Upsert one detection into traffic_fact ────────────────────────────────
async function upsertDetection(merchantId, detection) {
  const { deviceId, entryTime, exitTime, entryCount, isPassThrough, attributes } = detection;

  const timeBucket = toTimeBucket(entryTime || new Date());
  const parsed = parseAttributes(attributes);
  if (!parsed) return;

  // Stay time — only for non-passthrough with valid entry+exit
  let staySeconds  = 0;
  let stayCount    = 0;
  let under5       = 0;
  let mid5to15     = 0;
  let over15       = 0;

  if (!isPassThrough && entryTime && exitTime) {
    staySeconds = Math.round((new Date(exitTime) - new Date(entryTime)) / 1000);
    if (staySeconds > 0) {
      stayCount = 1;
      const stayMin = staySeconds / 60;
      if      (stayMin < 5)  under5  = 1;
      else if (stayMin < 15) mid5to15 = 1;
      else                   over15  = 1;
    }
  }

  const enter = isPassThrough ? 0 : (entryCount || 1);
  const pass  = isPassThrough ? (entryCount || 1) : 0;

  await db.query(`
    INSERT INTO traffic_fact
      (merchant_id, device_id, time_bucket,
       enter_count, pass_count,
       gender_male, gender_female,
       age_under18, age_18_60, age_over60,
       accessory_hat, accessory_glasses, accessory_boots,
       bag_handbag, bag_shoulder, bag_backpack, hold_item,
       upper_short, upper_long, upper_coat,
       upper_style_stripe, upper_style_logo, upper_style_plaid, upper_style_splice,
       lower_trousers, lower_shorts, lower_skirt,
       lower_style_stripe, lower_style_pattern,
       total_stay_seconds, stay_count,
       stay_under5min, stay_5to15min, stay_over15min)
    VALUES
      (?, ?, ?,
       ?, ?,
       ?, ?,
       ?, ?, ?,
       ?, ?, ?,
       ?, ?, ?, ?,
       ?, ?, ?,
       ?, ?, ?, ?,
       ?, ?, ?,
       ?, ?,
       ?, ?,
       ?, ?, ?)
    ON DUPLICATE KEY UPDATE
       enter_count          = enter_count          + VALUES(enter_count),
       pass_count           = pass_count           + VALUES(pass_count),
       gender_male          = gender_male          + VALUES(gender_male),
       gender_female        = gender_female        + VALUES(gender_female),
       age_under18          = age_under18          + VALUES(age_under18),
       age_18_60            = age_18_60            + VALUES(age_18_60),
       age_over60           = age_over60           + VALUES(age_over60),
       accessory_hat        = accessory_hat        + VALUES(accessory_hat),
       accessory_glasses    = accessory_glasses    + VALUES(accessory_glasses),
       accessory_boots      = accessory_boots      + VALUES(accessory_boots),
       bag_handbag          = bag_handbag          + VALUES(bag_handbag),
       bag_shoulder         = bag_shoulder         + VALUES(bag_shoulder),
       bag_backpack         = bag_backpack         + VALUES(bag_backpack),
       hold_item            = hold_item            + VALUES(hold_item),
       upper_short          = upper_short          + VALUES(upper_short),
       upper_long           = upper_long           + VALUES(upper_long),
       upper_coat           = upper_coat           + VALUES(upper_coat),
       upper_style_stripe   = upper_style_stripe   + VALUES(upper_style_stripe),
       upper_style_logo     = upper_style_logo     + VALUES(upper_style_logo),
       upper_style_plaid    = upper_style_plaid    + VALUES(upper_style_plaid),
       upper_style_splice   = upper_style_splice   + VALUES(upper_style_splice),
       lower_trousers       = lower_trousers       + VALUES(lower_trousers),
       lower_shorts         = lower_shorts         + VALUES(lower_shorts),
       lower_skirt          = lower_skirt          + VALUES(lower_skirt),
       lower_style_stripe   = lower_style_stripe   + VALUES(lower_style_stripe),
       lower_style_pattern  = lower_style_pattern  + VALUES(lower_style_pattern),
       total_stay_seconds   = total_stay_seconds   + VALUES(total_stay_seconds),
       stay_count           = stay_count           + VALUES(stay_count),
       stay_under5min       = stay_under5min       + VALUES(stay_under5min),
       stay_5to15min        = stay_5to15min        + VALUES(stay_5to15min),
       stay_over15min       = stay_over15min       + VALUES(stay_over15min)
  `, [
    merchantId, deviceId, timeBucket,
    enter, pass,
    parsed.gender_male, parsed.gender_female,
    parsed.age_under18, parsed.age_18_60, parsed.age_over60,
    parsed.accessory_hat, parsed.accessory_glasses, parsed.accessory_boots,
    parsed.bag_handbag, parsed.bag_shoulder, parsed.bag_backpack, parsed.hold_item,
    parsed.upper_short, parsed.upper_long, parsed.upper_coat,
    parsed.upper_style_stripe, parsed.upper_style_logo, parsed.upper_style_plaid, parsed.upper_style_splice,
    parsed.lower_trousers, parsed.lower_shorts, parsed.lower_skirt,
    parsed.lower_style_stripe, parsed.lower_style_pattern,
    staySeconds, stayCount,
    under5, mid5to15, over15,
  ]);
}

// ── Refresh Redis realtime cache for one merchant ─────────────────────────
// Format must match DeviceServiceImpl.aggregateTodaySummary() output keys
// so the Spring Boot backend can read it without changes.
async function refreshRealtimeCache(merchantId) {
  const now = new Date();
  // Today's range in Asia/Shanghai (+08:00)
  const offset = 8 * 60 * 60000;
  const localNow = new Date(now.getTime() + offset);
  const todayStr = localNow.toISOString().substring(0, 10);
  const todayStart = `${todayStr} 00:00:00`;
  const todayEnd   = `${todayStr} 23:59:59`;

  const [rows] = await db.query(`
    SELECT
      COALESCE(SUM(enter_count),        0) AS totalEnter,
      COALESCE(SUM(pass_count),         0) AS totalPass,
      COALESCE(SUM(gender_male),        0) AS genderMale,
      COALESCE(SUM(gender_female),      0) AS genderFemale,
      COALESCE(SUM(age_under18),        0) AS ageUnder18,
      COALESCE(SUM(age_18_60),          0) AS age1860,
      COALESCE(SUM(age_over60),         0) AS ageOver60,
      COALESCE(SUM(total_stay_seconds), 0) AS totalStaySeconds,
      COALESCE(SUM(stay_count),         0) AS stayCount,
      COALESCE(SUM(new_customer_count),       0) AS newCustomerCount,
      COALESCE(SUM(returning_customer_count), 0) AS returningCustomerCount
    FROM traffic_fact
    WHERE merchant_id = ? AND time_bucket BETWEEN ? AND ?
  `, [merchantId, todayStart, todayEnd]);

  const r = rows[0];
  const sc = parseInt(r.stayCount);
  const summary = {
    totalEnter:              parseInt(r.totalEnter),
    totalPass:               parseInt(r.totalPass),
    genderMale:              parseInt(r.genderMale),
    genderFemale:            parseInt(r.genderFemale),
    ageUnder18:              parseInt(r.ageUnder18),
    age1860:                 parseInt(r.age1860),
    ageOver60:               parseInt(r.ageOver60),
    avgStaySeconds:          sc > 0 ? Math.round(parseInt(r.totalStaySeconds) / sc) : 0,
    stayCount:               sc,
    newCustomerCount:        parseInt(r.newCustomerCount),
    returningCustomerCount:  parseInt(r.returningCustomerCount),
  };

  // TTL = seconds until next midnight (Asia/Shanghai)
  const midnightLocal = new Date(`${todayStr}T16:00:00Z`);  // next UTC midnight = +08:00 midnight today
  if (midnightLocal <= now) midnightLocal.setDate(midnightLocal.getDate() + 1);
  const ttl = Math.max(60, Math.round((midnightLocal - now) / 1000));

  await redis.set(
    `${CACHE_PREFIX}${merchantId}`,
    JSON.stringify(summary),
    'EX', ttl
  );
}

// ── Main worker loop ──────────────────────────────────────────────────────
async function processOne(payload) {
  const { bindId, detection } = payload;
  const merchantId = await getMerchantId(bindId);
  if (!merchantId) {
    console.warn(`[Worker] unknown bindId="${bindId}", skipping`);
    return;
  }
  await upsertDetection(merchantId, detection);
  await refreshRealtimeCache(merchantId);
}

async function start() {
  if (running) return;
  running = true;
  console.log('[Worker] started, listening on queue:', QUEUE_KEY);

  // Use a separate blocking Redis connection for BRPOP
  const blockingRedis = redis.duplicate();

  while (running) {
    try {
      // BRPOP blocks up to 2 seconds, returns [key, value] or null
      const result = await blockingRedis.brpop(QUEUE_KEY, 2);
      if (!result) continue;

      const payload = JSON.parse(result[1]);
      await processOne(payload);
    } catch (err) {
      console.error('[Worker] error processing item:', err.message);
      await new Promise(r => setTimeout(r, IDLE_MS));
    }
  }
}

function stop() { running = false; }

module.exports = { start, stop };
