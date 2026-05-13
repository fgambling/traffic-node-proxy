require('dotenv').config();
const app    = require('./server');
const worker = require('./worker');
const redis  = require('./redis');
const db     = require('./db');

const PORT = parseInt(process.env.PORT || '3000');

async function main() {
  await redis.connect();
  console.log('[Redis] connected');

  // Verify DB connection
  await db.query('SELECT 1');
  console.log('[MySQL] connected');

  // Start HTTP server
  app.listen(PORT, () => {
    console.log(`[Server] listening on port ${PORT}`);
  });

  // Start queue worker in same process
  worker.start();
}

main().catch(err => {
  console.error('[Startup] fatal:', err);
  process.exit(1);
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('[Shutdown] SIGTERM received');
  worker.stop();
  await redis.quit();
  process.exit(0);
});
