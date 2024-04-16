import puppeteer from "puppeteer";
import sqlite3 from "sqlite3";
import fs from "fs";

type Message = {
  userId: string;
  id: string;
  messageText: string;
  createdAt: number;
  color: string;
};

const db = new sqlite3.Database("database.db");
const memDb = new sqlite3.Database(":memory:");
let totalMessages = 0;
let debouncedPruge: NodeJS.Timeout;
let purgeInProgress = false;

memDb.serialize(() => {
  memDb.run(`CREATE TABLE IF NOT EXISTS messages (
    userId TEXT,
    id TEXT,
    messageText TEXT,
    createdAt INTEGER,
    color TEXT
  )`);
});
db.serialize(() => {
  db.run(`CREATE TABLE IF NOT EXISTS messages (
        userId TEXT,
        id TEXT,
        messageText TEXT,
        createdAt INTEGER,
        color TEXT
    )`);
});

(async () => {
  // Launch the browser and open a new blank page
  const browser = await puppeteer.launch();
  const page = await browser.newPage();

  // Navigate the page to a URL
  await page.goto("https://ventscape.life/");
  console.log(await page.title());

  // Wait for the page to load
  const textSelector = await page.waitForSelector(
    '[data-tip="You are connected to VentScape!"]'
  );
  console.log("Website Loaded");

  const client = await page.createCDPSession();
  await client.send("Network.enable");

  client.on("Network.webSocketFrameReceived", async ({ response }) => {
    if (response.payloadData.length > 10) {
      const data = response.payloadData;
      let jsonData: Message;
      try {
        jsonData = JSON.parse(data.slice(data.indexOf("{"), -1)) as Message;
      } catch (err) {
        try {
          fs.appendFileSync(
            "error.log",
            `Failed to parse JSON: ${data} - ${err}\n`,
            "utf8"
          );
        } catch (err) {
          console.error("Failed to append to error.log", err);
        }
        console.error("Failed to parse JSON", data, err);
      }
      totalMessages += 1;
      memDb.serialize(() => {
        memDb.run(`INSERT INTO messages VALUES (?, ?, ?, ?, ?)`, [
          jsonData.userId,
          jsonData.id,
          jsonData.messageText,
          jsonData.createdAt,
          jsonData.color,
        ]);
      });
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
})();

async function purgeToDb() {
  if (purgeInProgress) {
    console.info("Purge already in progress, skipping");
    return;
  }
  purgeInProgress = true;
  console.info(`Saving to DB... (${totalMessages} messages)`);
  try {
    memDb.all(`SELECT * FROM messages`, (err, rows: Message[]) => {
      db.serialize(() => {
        rows.forEach((message) => {
          db.run(`INSERT INTO messages VALUES (?, ?, ?, ?, ?)`, [
            message.userId,
            message.id,
            message.messageText,
            message.createdAt,
            message.color,
          ]);
          memDb.serialize(() => {
            memDb.run(`DELETE FROM messages WHERE id = ?`, [message.id]);
          });
        });
      });
      totalMessages = 0;
    });
  } catch (err) {
    try {
      fs.appendFileSync("error.log", `Purge failed: ${err}\n`, "utf8");
    } catch (err) {
      console.error("Failed to append to error.log", err);
    }
    console.error("Purge failed", err);
  } finally {
    purgeInProgress = false;
  }
}

