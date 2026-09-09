import express, {type Express, type Response, type Request} from "express";
import crypto from 'node:crypto';
import 'dotenv/config';
import { createClient } from 'redis';

const app: Express = express();
const redisUrl = process.env.REDIS_URL;
if (!redisUrl) {
    throw new Error('REDIS_URL not set');
}
const redis = createClient({ url: redisUrl });
await redis.connect();

app.use(express.json({ verify: (req, res, buf) => {
    (req as any).rawBody = buf;
},}));

async function isDuplicate(deliveryId: string): Promise<boolean> {
    const result = await redis.set(
        `webhook:delivery:${deliveryId}`,
        '1',
        {
            NX: true,
            EX: 60 * 60 * 24 * 3 // 3 days, matching GitHub's redelivery window
        }
    );

    // result is 'OK' if the key was newly set (not a duplicate)
    // result is null if the key already existed (it IS a duplicate)
    return result === null;
}

app.get('/', (req, res) => {
    const secret = process.env.WEBHOOK_SECRET;
    console.log(JSON.stringify(secret));
    res.send("Hello, World!");
})

app.post('/webhook', async (req, res) => {
    const signature = req.headers["x-hub-signature-256"];
    const secret = process.env.WEBHOOK_SECRET;
    const deliveryId = req.headers["x-github-delivery"];

    if (!secret) {
        console.error("WEBHOOK_SECRET not set");
        return res.status(500).send('server misconfigured');
    }

    if (typeof signature !== 'string') {
        return res.status(401).send('missing signature');
    }

    const expected = 'sha256=' + crypto
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

    if (await isDuplicate(deliveryId)) {
        console.log(`Duplicate delivery ${deliveryId}, skipping`);
        return res.status(200).send('ok'); // still 200 — don't make GitHub think this failed
    }

    const eventType = req.headers["x-github-event"];

    if (typeof eventType !== 'string') {
        return res.status(400).send('missing event type');
    }

    const claimed = await redis.set(`webhook:delivery:${deliveryId}`, '1', { NX: true, EX: 60 * 60 * 24 * 3 });

    if (claimed === null) {
        // duplicate — already claimed
        return res.status(200).send('ok');
    }

    try {
    await redis.xAdd('webhook:events', '*', {
        deliveryId,
        eventType,
        payload: JSON.stringify(req.body)
    });
    } catch (err) {
        await redis.del(`webhook:delivery:${deliveryId}`); // release the claim
        console.error('XADD failed, released claim', err);
        return res.status(500).send('failed to queue event');
    }

    res.status(200).send('ok');
    console.log(`Queued ${eventType} event ${deliveryId}`);
})

const PORT = 3000;
app.listen(PORT, () => console.log(`Listening on port ${PORT}`));
