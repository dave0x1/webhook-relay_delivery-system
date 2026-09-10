// index.ts
import express, { type Express } from 'express';
import crypto from 'node:crypto';
import 'dotenv/config';
import { redis, STREAM_KEY, GROUP_NAME } from './redis.js';
import { ensureConsumerGroup } from './initGroup.js';

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

app.get('/events', async (req, res) => {
  await ensureConsumerGroup();

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  // 1. Use a stable consumer name so the client recovers its own pending history
  // Accepts a name from the client via query ?consumer=..., or falls back to 'laptop-client'
  const consumerId =
    (req.query.consumer as string) ||
    (req.headers['x-consumer-id'] as string) ||
    'laptop-client';

  // Read the Last-Event-ID sent by the SSE client on reconnect (if available)
  const lastEventIdHeader = req.headers['last-event-id'] as string | undefined;

  console.log(
    `SSE client connected: ${consumerId} (Last-Event-ID: ${lastEventIdHeader ?? 'none'})`
  );

  let isConnected = true;

  req.on('close', () => {
    isConnected = false;
    console.log(`SSE client disconnected: ${consumerId}`);
  });

  const heartbeatTimer = setInterval(() => {
    if (!isConnected) return;
    res.write(': ping\n\n');
  }, 15000);

  // Helper to serialize and write an SSE event frame
  const sendEvent = async (id: string, fields: Record<string, string>) => {
    res.write(`id: ${id}\n`);
    res.write(`event: ${fields.eventType}\n`);
    res.write(
      `data: ${JSON.stringify({
        deliveryId: fields.deliveryId,
        eventType: fields.eventType,
        payload: JSON.parse(fields.payload!),
      })}\n\n`
    );

    // Only acknowledge from the PEL once flushed
    await redis.xAck(STREAM_KEY, GROUP_NAME, id);
    console.log(`Delivered and ACKed stream event ${id} to ${consumerId}`);
  };

  try {
    // 2. PHASE A: Drain unacknowledged / pending messages from the PEL
    // Passing '0' returns entries in this consumer's Pending Entries List
    let checkingPending = true;

    while (isConnected && checkingPending) {
      const pendingResponse = await redis.xReadGroup(
        GROUP_NAME,
        consumerId,
        [{ key: STREAM_KEY, id: '0' }],
        { COUNT: 10 }
      );

      const messages = pendingResponse?.[0]?.messages || [];

      if (messages.length === 0) {
        // All pending items have been replayed and ACKed
        checkingPending = false;
        break;
      }

      for (const msg of messages) {
        if (!isConnected) break;

        // If the client already acknowledged receiving up to Last-Event-ID,
        // we can ACK and skip sending it again
        if (lastEventIdHeader && msg.id <= lastEventIdHeader) {
          await redis.xAck(STREAM_KEY, GROUP_NAME, msg.id);
          continue;
        }

        await sendEvent(msg.id, msg.message);
      }
    }

    // 3. PHASE B: Listen for new incoming events using '>'
    while (isConnected) {
      const liveResponse = await redis.xReadGroup(
        GROUP_NAME,
        consumerId,
        [{ key: STREAM_KEY, id: '>' }],
        { COUNT: 5, BLOCK: 2000 }
      );

      if (!isConnected) break;

      if (!liveResponse || liveResponse.length === 0) {
        continue;
      }

      for (const stream of liveResponse) {
        for (const msg of stream.messages) {
          if (!isConnected) break;
          await sendEvent(msg.id, msg.message);
        }
      }
    }
  } catch (err) {
    console.error(`Streaming error for ${consumerId}:`, err);
  } finally {
    clearInterval(heartbeatTimer);
    if (!res.writableEnded) {
      res.end();
    }
  }
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