const express = require('express');
const redis   = require('./redis');

const app      = express();
const QUEUE_KEY = process.env.QUEUE_KEY || 'traffic:upload:queue';

app.use(express.json({ limit: '2mb' }));

// ── Validation ─────────────────────────────────────────────────────────────
function validate(body) {
  if (!body.systemId)          return 'missing systemId';
  if (!Array.isArray(body.personDetections)) return 'personDetections must be array';
  for (let i = 0; i < body.personDetections.length; i++) {
    const d = body.personDetections[i];
    if (d.id == null)          return `item[${i}] missing id`;
    if (!d.personId)           return `item[${i}] missing personId`;
    if (!d.deviceId)           return `item[${i}] missing deviceId`;
    if (!Array.isArray(d.attributes) || d.attributes.length !== 26)
      return `item[${i}] attributes must be array of 26 numbers`;
  }
  return null;
}

// ── POST /upload ───────────────────────────────────────────────────────────
app.post('/upload', async (req, res) => {
  const err = validate(req.body);
  if (err) return res.status(400).json({ code: 400, message: err });

  const { systemId, bindId, personDetections } = req.body;

  console.log(`[Server] recv systemId=${systemId} bindId=${bindId || 'none'} detections=${personDetections.length}`);

  // Push each detection as a separate queue item so the worker
  // can process them one at a time and avoid any thundering herd.
  const pipeline = redis.pipeline();
  for (const detection of personDetections) {
    pipeline.lpush(QUEUE_KEY, JSON.stringify({ bindId, detection }));
  }
  await pipeline.exec();

  res.json({ code: 0, message: 'success' });
});

// ── GET /health ────────────────────────────────────────────────────────────
app.get('/health', (_req, res) => res.json({ status: 'ok' }));

module.exports = app;
