import sqlite3 from "sqlite3";
import { stopWords } from "./stop-words.ts";

type Message = {
  messageText: string;
};

const db = new sqlite3.Database("database.db");

let allWordsList: string[] = [];
let allWordsCount: Record<string, number> = {};

db.all(`SELECT messageText FROM messages`, (err, rows: Message[]) => {
  rows.forEach((message) => {
    try {
      allWordsList.push(...message.messageText.split(" "));
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
});
