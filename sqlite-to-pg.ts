import pg from "pg";
import sqlite3 from "sqlite3";
import { open } from "sqlite";
import { config } from "dotenv";
config();

(async () => {
  const sqlitedb = await open({
    filename: "database.db",
    driver: sqlite3.Database,
    mode: sqlite3.OPEN_READONLY,
  });

  const { Client } = pg;
  const pgdb = new Client({
    user: process.env.PG_USER,
    host: process.env.PG_HOST,
    database: process.env.PG_DATABASE,
    password: process.env.PG_PASSWORD,
    port: parseInt(process.env.PG_PORT || "5432"),
  });
  await pgdb.connect();

  await pgdb.query(`CREATE TABLE IF NOT EXISTS colors (
    id SERIAL PRIMARY KEY,
    color_name VARCHAR(7) NOT NULL UNIQUE
  )`);

  await pgdb.query(`CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    user_name VARCHAR(50) NOT NULL UNIQUE
  )`);

  await pgdb.query(`CREATE TABLE IF NOT EXISTS messages (
    id SERIAL PRIMARY KEY,
    uuid UUID NOT NULL UNIQUE,
    message_text TEXT,
    created_at TIMESTAMP,
    user_id INTEGER,
    color_id INTEGER,
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (color_id) REFERENCES colors(id)
  )`);

  const users = await sqlitedb.all("SELECT * FROM users");
  for (const user of users) {
    await pgdb.query(`INSERT INTO users(user_name) VALUES ($1)`, [
      user.userName,
    ]);
  }

  const colors = await sqlitedb.all("SELECT * FROM colors");
  for (const color of colors) {
    await pgdb.query(`INSERT INTO colors(color_name) VALUES ($1)`, [
      color.colorName,
    ]);
  }

  const messages = await sqlitedb.all("SELECT * FROM messages");
  for (const message of messages) {
    await pgdb.query(
      `INSERT INTO messages(message_text, created_at, user_id, color_id) VALUES ($1, $2, $3, $4)`,
      [
        message.messageText,
        `${new Date(message.createdAt).toISOString()}`,
        message.userId,
        message.color,
      ]
    );
  }

  await pgdb.end();
  await sqlitedb.close();
  return true;
})();
