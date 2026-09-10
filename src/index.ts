// index.ts
import express, { type Express } from 'express';
import crypto from 'node:crypto';
import 'dotenv/config';
import { redis, STREAM_KEY } from './redis.js';

const app: Express = express();

app.use(
  express.json({
    verify: (req, _res, buf) => {
      (req as any).rawBody = buf;
    },
  })
);

app.get('/', (_req, res) => {
  res.send('Hello, World!');
});

app.post('/webhook', async (req, res) => {
  const signature = req.headers['x-hub-signature-256'];
  const secret = process.env.WEBHOOK_SECRET;
  const deliveryId = req.headers['x-github-delivery'];
  const eventType = req.headers['x-github-event'];

  if (!secret) {
    console.error('WEBHOOK_SECRET not set');
    return res.status(500).send('server misconfigured');
  }

  if (typeof signature !== 'string') {
    return res.status(401).send('missing signature');
  }

  const expected =
    'sha256=' +
    crypto
      .createHmac('sha256', secret)
      .update((req as any).rawBody)
      .digest('hex');

  const expectedBuf = Buffer.from(expected);
  const signatureBuf = Buffer.from(signature);

  if (
    expectedBuf.length !== signatureBuf.length ||
    !crypto.timingSafeEqual(expectedBuf, signatureBuf)
  ) {
    return res.status(401).send('invalid signature');
  }

  if (typeof deliveryId !== 'string') {
    return res.status(400).send('missing delivery id');
  }

  if (typeof eventType !== 'string') {
    return res.status(400).send('missing event type');
  }

  // 1. Claim delivery ID atomically with 3-day TTL
  const dedupKey = `webhook:delivery:${deliveryId}`;
  const claimed = await redis.set(dedupKey, '1', {
    NX: true,
    EX: 60 * 60 * 24 * 3,
  });

  if (claimed === null) {
    console.log(`Duplicate delivery ${deliveryId}, skipping`);
    return res.status(200).send('ok');
  }

  // 2. Queue into Redis Stream, releasing claim if XADD fails
  try {
    await redis.xAdd(STREAM_KEY, '*', {
      deliveryId,
      eventType,
      payload: JSON.stringify(req.body),
    });
  } catch (err) {
    await redis.del(dedupKey);
    console.error('XADD failed, released claim', err);
    return res.status(500).send('failed to queue event');
  }

  console.log(`Queued ${eventType} event ${deliveryId}`);
  return res.status(200).send('ok');
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Listening on port ${PORT}`));