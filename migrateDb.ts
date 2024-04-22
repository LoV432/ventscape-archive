import sqlite3 from "sqlite3";
const db = new sqlite3.Database("database.db");

db.serialize(() => {
  db.run(`DELETE FROM messages
  WHERE rowid NOT IN (
      SELECT MIN(rowid)
      FROM messages
      GROUP BY id
  );`);

  db.run(`CREATE TABLE IF NOT EXISTS colors (
      id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
      colorName CHAR(7) NOT NULL
    )`);
  db.run(`CREATE TABLE IF NOT EXISTS users (
      id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
      userName VARCHAR(50) NOT NULL
    )`);
  db.run(`CREATE TABLE IF NOT EXISTS messages_new (
      id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
      messageText TEXT,
      createdAt INTEGER,
      userId INTEGER,
      color INTEGER,
      CONSTRAINT messages_new_users_FK FOREIGN KEY (userId) REFERENCES users(id),
      CONSTRAINT messages_new_colors_FK FOREIGN KEY (color) REFERENCES colors(id)
  )`);
  db.run(`INSERT
	INTO
	  colors (colorName)
  SELECT
    DISTINCT color
  FROM
    messages
  WHERE
    color IS NOT NULL`);

  db.run(`INSERT
	INTO
	  users (userName)
  SELECT
    DISTINCT userId
  FROM
    messages
  WHERE
    userId IS NOT NULL`);

  db.run(`INSERT
	INTO
    messages_new (messageText,
    createdAt,
    userId,
    color)
  SELECT
    m.messageText,
    m.createdAt,
    u.id,
    c.id
  FROM
    messages m
  JOIN users u ON
    m.userId = u.userName
  LEFT JOIN colors c ON
    m.color = c.colorName
  WHERE
    m.messageText IS NOT NULL
    AND m.createdAt IS NOT NULL`);

  db.run(`DROP TABLE messages`);
  db.run(`ALTER TABLE messages_new RENAME TO messages`);
  db.run(`VACUUM`);
});
