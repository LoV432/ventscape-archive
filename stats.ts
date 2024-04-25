import pg from "pg";
import { stopWords } from "./stop-words.ts";
import { config } from "dotenv";
config();

type Message = {
  message_text: string;
};

(async () => {
  const { Client } = pg;
  const dbClient = new Client({
    user: process.env.PG_USER,
    host: process.env.PG_HOST,
    database: process.env.PG_DATABASE,
    password: process.env.PG_PASSWORD,
    port: parseInt(process.env.PG_PORT || "5432"),
  });
  await dbClient.connect();
  let allWordsList: string[] = [];
  let allWordsCount: Record<string, number> = {};

  const allMessages = await dbClient.query(`SELECT message_text FROM messages`);
  allMessages.rows.forEach((message: Message) => {
    try {
      allWordsList.push(...message.message_text.split(" "));
    } catch {}
  });
  allWordsList.forEach((word: string) => {
    word = word.replace(/[.,!?:'";]+/g, "").toLowerCase();
    if (word.length < 2 || [...stopWords].includes(word.toLowerCase())) return;
    allWordsCount[word] = (allWordsCount[word] || 0) + 1;
  });
  const allWordsCountArray = Object.entries(allWordsCount).sort(
    (a, b) => b[1] - a[1]
  );
  console.log(allWordsCountArray);
  dbClient.end();
})();
