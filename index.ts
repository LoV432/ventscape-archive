import puppeteer from "puppeteer";
import sqlite3 from "sqlite3";
import { open } from "sqlite";

import fs from "fs";

type Message = {
  userId: string;
  id: string;
  messageText: string;
  createdAt: number;
  color: string;
};

async function init() {
  const db = await open({
    filename: "database.db",
    driver: sqlite3.Database,
  });
  const memDb = await open({
    filename: ":memory:",
    driver: sqlite3.Database,
  });
  let totalMessages = 0;
  let debouncedPruge: NodeJS.Timeout;
  let purgeInProgress = false;

  await memDb.run(`CREATE TABLE IF NOT EXISTS messages (
      userId TEXT,
      id TEXT,
      messageText TEXT,
      createdAt INTEGER,
      color TEXT
    )`);

  await db.run("BEGIN;");
  await db.run(`CREATE TABLE IF NOT EXISTS colors (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    colorName CHAR(7) NOT NULL
    )`);

  await db.run(`CREATE TABLE IF NOT EXISTS users (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    userName VARCHAR(50) NOT NULL
    )`);

  await db.run(`CREATE TABLE IF NOT EXISTS messages (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    messageText TEXT,
    createdAt INTEGER,
    userId INTEGER,
    color INTEGER,
    CONSTRAINT messages_new_users_FK FOREIGN KEY (userId) REFERENCES users(id),
    CONSTRAINT messages_new_colors_FK FOREIGN KEY (color) REFERENCES colors(id)
    )`);

  await db.run(`COMMIT;`);

  // Launch the browser and open a new blank page
  const browser = await puppeteer.launch();
  const page = await browser.newPage();

  // Navigate the page to a URL
  await page.goto("https://ventscape.life/");
  console.log(await page.title());

  const client = await page.createCDPSession();
  await client.send("Network.enable");

  client.on("Network.webSocketFrameReceived", async ({ response }) => {
    if (response.payloadData.startsWith("42")) {
      const data = response.payloadData;
      let jsonData: Message;
      try {
        jsonData = JSON.parse(data.slice(data.indexOf("{"), -1)) as Message;
      } catch (err) {
        try {
          fs.appendFileSync(
            "error.log",
            `${new Date()} Failed to parse JSON: ${data} - ${err}\n`,
            "utf8"
          );
        } catch (err) {
          console.error("Failed to append to error.log", err);
        }
        console.error("Failed to parse JSON", data, err);
        return;
      }
      totalMessages += 1;
      addToMemDb(jsonData);
      if (totalMessages % 50 === 0) {
        if (debouncedPruge) {
          clearTimeout(debouncedPruge);
        }
        await purgeToDb();
      } else {
        if (debouncedPruge) {
          clearTimeout(debouncedPruge);
        }
        debouncedPruge = setTimeout(purgeToDb, 1000 * 20);
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
      try {
        fs.appendFileSync(
          "error.log",
          `${new Date()} Network.webSocketFrameError: ${errorMessage} - ${requestId} - ${timestamp}\n`,
          "utf8"
        );
      } catch (err) {
        console.error("Failed to append to error.log", err);
      }
    }
  );

  client.on("Network.webSocketClosed", async ({ requestId, timestamp }) => {
    console.error("Network.webSocketClosed", requestId, timestamp);
    try {
      fs.appendFileSync(
        "error.log",
        `${new Date()} Network.webSocketClosed: ${requestId} - ${timestamp}\n`,
        "utf8"
      );
    } catch (err) {
      console.error("Failed to append to error.log", err);
    }
  });

  client.on(
    "Network.webSocketCreated",
    async ({ requestId, url, initiator }) => {
      console.error("Network.webSocketCreated", requestId, url, initiator);
      try {
        fs.appendFileSync(
          "error.log",
          `${new Date()} Network.webSocketCreated: ${requestId} - ${url} - ${initiator}\n`,
          "utf8"
        );
      } catch (err) {
        console.error("Failed to append to error.log", err);
      }
    }
  );

  async function purgeToDb() {
    if (purgeInProgress) {
      console.info("Purge already in progress, skipping");
      return;
    }
    purgeInProgress = true;
    console.info(`Saving to DB... (${totalMessages} messages)`);
    try {
      const rows = await memDb.all(`SELECT * FROM messages ORDER BY createdAt`);
      for (const message of rows as Message[]) {
        const userId = await getUserId(message.userId);
        if (!userId) {
          console.error("Failed to get userId for", message);
          try {
            fs.appendFileSync(
              "error.log",
              `${new Date()} Failed to get userId for ${JSON.stringify(
                message
              )}\n`,
              "utf8"
            );
          } catch (err) {
            console.error("Failed to append to error.log", err);
          }
          deleteFromMemDb(message);
          continue;
        }
        const color = await getColorId(message.color);
        await addToDb(message.messageText, message.createdAt, userId, color);
        deleteFromMemDb(message);
      }
      totalMessages = 0;
    } catch (err) {
      try {
        fs.appendFileSync(
          "error.log",
          `${new Date()} Purge failed: ${err}\n`,
          "utf8"
        );
      } catch (err) {
        console.error("Failed to append to error.log", err);
      }
      console.error("Purge failed", err);
    } finally {
      purgeInProgress = false;
    }
  }

  async function getUserId(messageUserId: string) {
    let userId: number | undefined;
    try {
      userId = (
        await db.get(`SELECT id FROM users WHERE userName = ?`, [messageUserId])
      )?.id as number | undefined;
      if (!userId) {
        db.run(`BEGIN;`);
        userId = (
          await db.run(`INSERT INTO users(userName) VALUES (?)`, [
            messageUserId,
          ])
        ).lastID as number;
        db.run(`COMMIT;`);
      }
      return userId;
    } catch (err) {
      try {
        fs.appendFileSync(
          "error.log",
          `${new Date()} Failed to get userId: ${err}\n`,
          "utf8"
        );
      } catch (err) {
        console.error("Failed to append to error.log", err);
      }
      console.error("Failed to get userId", err);
      return null;
    }
  }

  async function getColorId(messageColor: string | null) {
    if (!messageColor) return Promise.resolve(null);
    let colorId: number | undefined;
    try {
      colorId = (
        await db.get(`SELECT id FROM colors WHERE colorName = ?`, [
          messageColor,
        ])
      )?.id as number | undefined;
      if (!colorId) {
        db.run(`BEGIN;`);
        colorId = (
          await db.run(`INSERT INTO colors(colorName) VALUES (?)`, [
            messageColor,
          ])
        ).lastID as number;
        db.run(`COMMIT;`);
      }
      return colorId;
    } catch (err) {
      try {
        fs.appendFileSync(
          "error.log",
          `${new Date()} Failed to get colorId: ${err}\n`,
          "utf8"
        );
      } catch (err) {
        console.error("Failed to append to error.log", err);
      }
      console.error("Failed to get colorId", err);
      return null;
    }
  }

  async function addToDb(
    messageText: string,
    createdAt: number,
    userId: number,
    color: number | null
  ) {
    await db.run(`BEGIN;`);
    await db.run(
      `INSERT INTO messages(messageText, createdAt, userId, color) VALUES (?, ?, ?, ?)`,
      [messageText, createdAt, userId, color]
    );
    await db.run(`COMMIT;`);
  }

  function addToMemDb(message: Message) {
    memDb.run(`INSERT INTO messages VALUES (?, ?, ?, ?, ?)`, [
      message.userId,
      message.id,
      message.messageText,
      message.createdAt,
      message.color,
    ]);
  }

  function deleteFromMemDb(message: Message) {
    memDb.run(`DELETE FROM messages WHERE id = ?`, [message.id]);
  }
}

init();
