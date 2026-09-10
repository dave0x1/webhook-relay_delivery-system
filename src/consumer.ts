// consumer.ts
import 'dotenv/config';
import { redis, STREAM_KEY, GROUP_NAME } from './redis.js';
import { ensureConsumerGroup } from './initGroup.js';

const CONSUMER_NAME = process.env.CONSUMER_NAME || `consumer-${process.pid}`;

async function runConsumer() {
  await ensureConsumerGroup();
  console.log(`Starting consumer ${CONSUMER_NAME}...`);

  while (true) {
    try {
      // 1. Read new unread messages assigned to this consumer group
      // ID '>' means "give me messages never delivered to any other consumer"
      // BLOCK: 5000 waits up to 5 seconds for new events before looping
      const response = await redis.xReadGroup(
        GROUP_NAME,
        CONSUMER_NAME,
        [{ key: STREAM_KEY, id: '>' }],
        { COUNT: 1, BLOCK: 5000 }
      );

      if (!response || response.length === 0) {
        continue;
      }

      for (const stream of response) {
        for (const message of stream.messages) {
          const { id, message: fields } = message;

          console.log(`\n--- Processing Event ---`);
          console.log(`Stream ID: ${id}`);
          console.log(`Delivery ID: ${fields.deliveryId}`);
          console.log(`Event Type: ${fields.eventType}`);

          try {
            const payload = JSON.parse(fields.payload);
            console.log(`Repository: ${payload.repository?.full_name || 'N/A'}`);

            // 2. Acknowledge the message once processing succeeds
            await redis.xAck(STREAM_KEY, GROUP_NAME, id);
            console.log(`ACKed message ${id}`);
          } catch (err) {
            console.error(`Failed to process message ${id}:`, err);
            // Don't XACK here — message stays in PEL for retry or inspection
          }
        }
      }
    } catch (err) {
      console.error('Consumer error in loop:', err);
      // Wait briefly before retrying to prevent tight error loops
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  }
}

runConsumer().catch((err) => {
  console.error('Fatal consumer error:', err);
  process.exit(1);
});