import express, {type Express, type Response, type Request} from "express";

const app: Express = express();

app.use(express.json());

app.get('/', (req, res) => {
    res.send("Hello, World!");
})

const PORT = 3000;
app.listen(PORT, () => console.log(`Listening on port ${PORT}`));
