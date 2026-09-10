import { createClient } from 'redis';

const redisUrl = process.env.REDIS_URL;
if (!redisUrl) {
    throw new Error('REDIS_URL not set');
}

export const redis = createClient({ url: redisUrl });
await redis.connect();

export const STREAM_KEY = 'webhook:events';
export const GROUP_NAME = 'laptop-consumers';