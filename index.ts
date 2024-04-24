import pg from "pg";
import { createClient } from "redis";
import puppeteer from "puppeteer";
import fs from "fs";
import { config } from "dotenv";
config();

type Message = {
  userId: string;
  id: string;
  messageText: string;
  createdAt: number;
  color: string;
};

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

async function init() {
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
  const dbPurge = setInterval(purgeToDb, 5 * 60 * 1000);

  client.on("Network.webSocketFrameReceived", async ({ response }) => {
    if (response.payloadData.startsWith("42")) {
      const data = response.payloadData;
      let jsonData: Message;
      try {
        jsonData = JSON.parse(data.slice(data.indexOf("{"), -1)) as Message;
        await redisClient.set(
          `${jsonData.createdAt}-${jsonData.id}`,
          JSON.stringify(jsonData)
        );
      } catch (err) {
        errorToFile(
          "pg-error.log",
          `Failed to store data in redis: ${data} ${err}`
        );
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
        "pg-error.log",
        `Network.webSocketFrameError - ${timestamp} - ${requestId} - ${errorMessage}`
      );
    }
  );

  client.on("Network.webSocketClosed", async ({ requestId, timestamp }) => {
    console.error("Network.webSocketClosed", requestId, timestamp);
    errorToFile(
      "pg-error.log",
      `Network.webSocketClosed - ${timestamp} - ${requestId}`
    );
  });

  client.on(
    "Network.webSocketCreated",
    async ({ requestId, url, initiator }) => {
      console.error("Network.webSocketCreated", requestId, url, initiator);
      errorToFile(
        "pg-error.log",
        `Network.webSocketCreated - ${url} - ${initiator} - ${requestId}`
      );
    }
  );

  async function purgeToDb() {
    if (purgeInProgress) {
      errorToFile("pg-error.log", "Purge already in progress, skipping");
      console.info("Purge already in progress, skipping");
      return;
    }
    purgeInProgress = true;
    console.info(`${new Date()} Starting purge to DB...`);
    try {
      const rows = await redisClient.keys("*");
      rows.sort((a, b) => Number(a.split("-")[0]) - Number(b.split("-")[0]));
      for (const row of rows) {
        const message = JSON.parse(
          (await redisClient.get(row)) as string
        ) as Message;
        const userId = await getUserId(message.userId);
        if (!userId) {
          errorToFile(
            "pg-error.log",
            `Failed to get userId for ${JSON.stringify(message)}`
          );
          console.error("Failed to get userId for", message);
          continue;
        }
        const color = await getColorId(message.color);
        const addToDbResult = await addToDb(
          message.messageText,
          message.createdAt,
          userId,
          color,
          message.id
        );
        if (addToDbResult) {
          await redisClient.del(row);
        }
      }
      console.info(`${new Date()} Purged to DB... (${rows.length} messages)`);
    } catch (err) {
      errorToFile("pg-error.log", `Failed to purge to db: ${err}`);
      console.error(err);
    } finally {
      purgeInProgress = false;
    }
  }

  async function getUserId(messageUserId: string) {
    let userId: number | undefined;
    try {
      userId = (
        await dbClient.query(`SELECT id FROM users WHERE user_name = $1`, [
          messageUserId,
        ])
      ).rows[0]?.id as number | undefined;
      if (!userId) {
        userId = (
          await dbClient.query(
            `INSERT INTO users(user_name) VALUES ($1) RETURNING id`,
            [messageUserId]
          )
        ).rows[0].id as number;
      }
      return userId;
    } catch (err) {
      errorToFile("pg-error.log", `Failed to get userId: ${err}`);
      console.error("Failed to get userId", err);
      return null;
    }
  }

  async function getColorId(messageColor: string | null) {
    if (!messageColor) return null;
    let colorId: number | undefined;
    try {
      colorId = (
        await dbClient.query(`SELECT id FROM colors WHERE color_name = $1`, [
          messageColor,
        ])
      ).rows[0]?.id as number | undefined;
      if (!colorId) {
        colorId = (
          await dbClient.query(
            `INSERT INTO colors(color_name) VALUES ($1) RETURNING id`,
            [messageColor]
          )
        ).rows[0].id as number;
      }
      return colorId;
    } catch (err) {
      errorToFile("pg-error.log", `Failed to get colorId: ${err}`);
      console.error("Failed to get colorId", err);
      return null;
    }
  }

  async function addToDb(
    messageText: string,
    createdAt: number,
    userId: number,
    colorId: number | null,
    uuid: string
  ) {
    try {
      await dbClient.query(
        `INSERT INTO messages(message_text, created_at, user_id, color_id, uuid) VALUES ($1, $2, $3, $4, $5)`,
        [
          messageText,
          `${new Date(createdAt).toISOString()}`,
          userId,
          colorId,
          uuid,
        ]
      );
      return true;
    } catch (err) {
      errorToFile(
        "pg-error.log",
        `Failed to add to db: ${err} - ${messageText} - ${createdAt} - ${userId} - ${colorId}`
      );
      console.error("Failed to add to db", err);
      return false;
    }
  }
}

init();

function errorToFile(file: string, message: string) {
  try {
    fs.appendFileSync(file, `${new Date()} ${message}\n`, "utf8");
  } catch (err) {
    console.error("Failed to append to error.log", err);
  }
}
