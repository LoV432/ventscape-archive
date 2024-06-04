import pg from "pg";
import { config } from "dotenv";
import { errorToFile, getUserId, addToDb, getColorId } from "./utils";
const [syncDb, syncReplica] = process.argv.slice(2);
config();

type Message = {
  user_name: string;
  id: string;
  message_text: string;
  created_at: Date;
  color_name: string;
};

const { Client } = pg;
const dbClient = new Client({
  user: process.env.PG_USER,
  host: process.env.PG_HOST,
  database: process.env.PG_DATABASE,
  password: process.env.PG_PASSWORD,
  port: parseInt(process.env.PG_PORT || "5432"),
});
const dbReplicaClient = new Client({
  user: process.env.PG_USER,
  host: process.env.PG_HOST_REPLICA,
  database: process.env.PG_DATABASE,
  password: process.env.PG_PASSWORD,
  port: parseInt(process.env.PG_PORT || "5432"),
});

async function init() {
  const errorFile = "logs/db-sync-replica-error.log";
  await dbClient.connect();
  await dbReplicaClient.connect();

  const now = new Date();

  const oneDayAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000);
  const oneHourAgo = new Date(now.getTime() - 60 * 60 * 1000);
  console.log(oneDayAgo, oneHourAgo);

  const messagesInReplica = (
    await dbReplicaClient.query(
      `SELECT uuid as id FROM messages WHERE created_at BETWEEN $1 AND $2 ORDER BY created_at ASC`,
      [oneDayAgo.toUTCString(), oneHourAgo.toUTCString()]
    )
  ).rows.map(({ id }) => id as string);
  console.log(messagesInReplica[messagesInReplica.length - 1]);

  const messageInMain = (
    await dbClient.query(
      `SELECT uuid as id FROM messages WHERE created_at BETWEEN $1 AND $2 ORDER BY created_at ASC`,
      [oneDayAgo.toUTCString(), oneHourAgo.toUTCString()]
    )
  ).rows.map(({ id }) => id as string);
  console.log(messageInMain[messageInMain.length - 1]);

  const messagesInReplicaSet = new Set(messagesInReplica);
  const messagesInMainSet = new Set(messageInMain);

  const missingInReplica = messageInMain.filter(
    (id) => !messagesInReplicaSet.has(id)
  );
  const missingMessagesInMain = messagesInReplica.filter(
    (id) => !messagesInMainSet.has(id)
  );

  console.log(`Found ${missingMessagesInMain.length} missing messages in main`);
  console.log(missingMessagesInMain);
  console.log(`Found ${missingInReplica.length} missing messages in replica`);
  console.log(missingInReplica);

  if (missingMessagesInMain.length > 0 && syncDb === "true") {
    console.log("Adding missing messages in main");
    const messagesToAddInMain = await dbReplicaClient.query(
      `SELECT uuid as id, message_text, u.user_name, c.color_name, created_at FROM messages JOIN users u ON messages.user_id = u.id LEFT JOIN colors c ON messages.color_id = c.id WHERE uuid = ANY($1)`,
      [`{${missingMessagesInMain.join(",")}}`]
    );

    for (const message of messagesToAddInMain.rows as Message[]) {
      console.log(message);
      const userId = await getUserId(message.user_name, dbClient, errorFile);
      if (!userId) {
        errorToFile(errorFile, `Failed to get userId for ${message}`);
        console.error("Failed to get userId for", message);
        continue;
      }
      const color = await getColorId(message.color_name, dbClient, errorFile);
      const addToDbResult = await addToDb(
        message.message_text,
        message.created_at,
        userId,
        color,
        message.id,
        dbClient,
        errorFile
      );
      if (!addToDbResult) {
        errorToFile(errorFile, `Failed to add ${message}`);
        console.error("Failed to add", message);
      }
    }
  }

  if (missingInReplica.length > 0 && syncReplica === "true") {
    console.log("Adding missing messages in replica");
    const messagesToAddInReplica = await dbClient.query(
      `SELECT uuid as id, message_text, u.user_name, c.color_name, created_at FROM messages JOIN users u ON messages.user_id = u.id LEFT JOIN colors c ON messages.color_id = c.id WHERE uuid = ANY($1)`,
      [`{${missingInReplica.join(",")}}`]
    );
    for (const message of messagesToAddInReplica.rows as Message[]) {
      console.log(message);
      const userId = await getUserId(
        message.user_name,
        dbReplicaClient,
        errorFile
      );
      if (!userId) {
        errorToFile(errorFile, `Failed to get userId for ${message}`);
        console.error("Failed to get userId for", message);
        continue;
      }
      const color = await getColorId(
        message.color_name,
        dbReplicaClient,
        errorFile
      );
      const addToDbResult = await addToDb(
        message.message_text,
        message.created_at,
        userId,
        color,
        message.id,
        dbReplicaClient,
        errorFile
      );
      if (!addToDbResult) {
        errorToFile(errorFile, `Failed to add ${message}`);
        console.error("Failed to add", message);
      }
    }
  }

  await dbClient.end();
  await dbReplicaClient.end();
}

init();
