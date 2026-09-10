// initGroup.ts
import { redis, STREAM_KEY, GROUP_NAME } from './redis.js';

export async function ensureConsumerGroup() {
  try {
    // MKSTREAM creates the stream if it doesn't exist yet
    // '$' means start listening only for new events arriving after group creation
    // (use '0' if you want it to process all historical messages already in the stream)
    await redis.xGroupCreate(STREAM_KEY, GROUP_NAME, '$', { MKSTREAM: true });
    console.log(`Consumer group '${GROUP_NAME}' created.`);
  } catch (err: any) {
    if (err?.message?.includes('BUSYGROUP')) {
      // Group already exists, safe to ignore
      return;
    }
    throw err;
  }
}