import express, {type Express, type Response, type Request} from "express";
import crypto from 'node:crypto';
import 'dotenv/config';

const app: Express = express();

app.use(express.json({ verify: (req, res, buf) => {
    (req as any).rawBody = buf;
},}));

app.get('/', (req, res) => {
    const secret = process.env.WEBHOOK_SECRET;
    console.log(JSON.stringify(secret));
    res.send("Hello, World!");
})

app.post('/webhook', (req, res) => {
    const signature = req.headers["x-hub-signature-256"];
    const secret = process.env.WEBHOOK_SECRET;

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

    console.log(req.body);
    res.status(200).send('ok');
})

const PORT = 3000;
app.listen(PORT, () => console.log(`Listening on port ${PORT}`));
