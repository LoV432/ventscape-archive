import pg from "pg";
import { createClient } from "redis";
import puppeteer from "puppeteer";
import { config } from "dotenv";
import { refetch } from "./refetch";
import { errorToFile, getUserId, addToDb, getColorId } from "./utils";
config();

type Message = {
  userId: string;
  id: string;
  messageText: string;
  createdAt: number;
  color: string;
};

async function init() {
  const errorFile = "pg-error.log";
  const redisClient = createClient({
    password: process.env.REDIS_PASSWORD,
    url: process.env.REDIS_URL,
    username: process.env.REDIS_USERNAME,
  });
  const { Client } = pg;
  const dbClient = new Client({
    user: process.env.PG_USER,
    host: process.env.PG_HOST,
    database: process.env.PG_DATABASE,
    password: process.env.PG_PASSWORD,
    port: parseInt(process.env.PG_PORT || "5432"),
  });
  await dbClient.connect();
  await redisClient.connect();

  await dbClient.query(`CREATE TABLE IF NOT EXISTS colors (
    id SERIAL PRIMARY KEY,
    color_name VARCHAR(7) NOT NULL UNIQUE
  )`);

  await dbClient.query(`CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    user_name VARCHAR(50) NOT NULL UNIQUE
  )`);

  await dbClient.query(`CREATE TABLE IF NOT EXISTS messages (
    id SERIAL PRIMARY KEY,
    uuid UUID NOT NULL UNIQUE,
    message_text TEXT,
    created_at TIMESTAMP,
    user_id INTEGER,
    color_id INTEGER,
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (color_id) REFERENCES colors(id)
  )`);

  // Launch the browser and open a new blank page
  const browser = await puppeteer.launch();
  const page = await browser.newPage();

  // Navigate the page to a URL
  await page.goto("https://ventscape.life/");
  console.log(await page.title());

  const client = await page.createCDPSession();
  await client.send("Network.enable");
  let purgeInProgress = false;
  let refetchTimeout: NodeJS.Timeout;
  const dbPurge = setInterval(purgeToDb, 5 * 60 * 1000);

  client.on("Network.webSocketFrameReceived", async ({ response }) => {
    if (response.payloadData.startsWith("42")) {
      const data = response.payloadData;
      let jsonData: Message;
      try {
        jsonData = JSON.parse(data.slice(data.indexOf("{"), -1)) as Message;
        await redisClient.set(jsonData.id, JSON.stringify(jsonData));
      } catch (err) {
        errorToFile(errorFile, `Failed to store data in redis: ${data} ${err}`);
        console.error("Failed to store data in redis", data, err);
        return;
      }
    }
  });

  client.on(
    "Network.webSocketFrameError",
    async ({ errorMessage, requestId, timestamp }) => {
      console.error(
        "Network.webSocketFrameError",
        errorMessage,
        requestId,
        timestamp
      );
      errorToFile(
        errorFile,
        `Network.webSocketFrameError - ${timestamp} - ${requestId} - ${errorMessage}`
      );
    }
  );

  client.on("Network.webSocketClosed", async ({ requestId, timestamp }) => {
    console.error("Network.webSocketClosed", requestId, timestamp);
    errorToFile(
      errorFile,
      `Network.webSocketClosed - ${timestamp} - ${requestId}`
    );
  });

  client.on(
    "Network.webSocketCreated",
    async ({ requestId, url, initiator }) => {
      console.error("Network.webSocketCreated", requestId, url, initiator);
      errorToFile(
        errorFile,
        `Network.webSocketCreated - ${url} - ${initiator} - ${requestId}`
      );
      refetchTimeout && clearTimeout(refetchTimeout);
      refetchTimeout = setTimeout(async () => {
        try {
          console.info(`${new Date()} Starting refetch...`);
          errorToFile(errorFile, `Starting refetch...`);
          const { messages } = await refetch(browser);
          console.info(
            `${new Date()} Finished refetch, ${messages.length} messages`
          );
          errorToFile(
            errorFile,
            `Finished refetch - ${JSON.stringify(messages)}`
          );
          for (const message of messages) {
            await redisClient.set(message.id, JSON.stringify(message));
          }
        } catch (err) {
          errorToFile(errorFile, `Failed to refetch: ${err}`);
          console.error("Failed to refetch", err);
        }
      }, 10000);
    }
  );

  async function purgeToDb() {
    if (purgeInProgress) {
      errorToFile(errorFile, "Purge already in progress, skipping");
      console.info("Purge already in progress, skipping");
      return;
    }
    purgeInProgress = true;
    console.info(`${new Date()} Starting purge to DB...`);
    try {
      const rows = await redisClient.keys("*");
      for (const row of rows) {
        const message = JSON.parse(
          (await redisClient.get(row)) as string
        ) as Message;
        const userId = await getUserId(message.userId, dbClient, errorFile);
        if (!userId) {
          errorToFile(
            errorFile,
            `Failed to get userId for ${JSON.stringify(message)}`
          );
          console.error("Failed to get userId for", message);
          continue;
        }
        const color = await getColorId(message.color, dbClient, errorFile);
        const addToDbResult = await addToDb(
          message.messageText,
          message.createdAt,
          userId,
          color,
          message.id,
          dbClient,
          errorFile
        );
        if (addToDbResult) {
          await redisClient.del(row);
        }
      }
      console.info(`${new Date()} Purged to DB... (${rows.length} messages)`);
    } catch (err) {
      errorToFile(errorFile, `Failed to purge to db: ${err}`);
      console.error(err);
    } finally {
      purgeInProgress = false;
    }
  }
}

init();
