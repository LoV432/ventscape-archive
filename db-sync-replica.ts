import pg from "pg";
import { config } from "dotenv";
import { errorToFile, getUserId, addToDb, getColorId } from "./utils";
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
  const errorFile = "db-sync-replica-error.log";
  await dbClient.connect();
  await dbReplicaClient.connect();

  const lastMessageInMainDb = await dbClient.query(
    `SELECT created_at FROM messages ORDER BY created_at DESC LIMIT 1`
  );
  let fiveMinutesBforeLastMessage: Date | undefined;
  if (lastMessageInMainDb.rows.length > 0) {
    const lastMessage = lastMessageInMainDb.rows[0];
    if (lastMessage && lastMessage.created_at) {
      fiveMinutesBforeLastMessage = lastMessageInMainDb.rows[0]
        .created_at as Date;
      fiveMinutesBforeLastMessage.setMinutes(
        fiveMinutesBforeLastMessage.getMinutes() -
          fiveMinutesBforeLastMessage.getTimezoneOffset()
      );

      fiveMinutesBforeLastMessage = new Date(
        fiveMinutesBforeLastMessage.getTime() - 5 * 60000
      );
    }
  }
  if (!fiveMinutesBforeLastMessage) {
    console.error("Failed to retrieve last message from database");
    process.exit(1);
  }
  console.log(fiveMinutesBforeLastMessage);
  console.log(`Last message: ${fiveMinutesBforeLastMessage.toUTCString()}`);

  const messagesAfterTime = await dbReplicaClient.query(
    `SELECT uuid as id, message_text, u.user_name, c.color_name, created_at FROM messages JOIN users u ON messages.user_id = u.id LEFT JOIN colors c ON messages.color_id = c.id WHERE created_at > $1 ORDER BY created_at ASC`,
    [fiveMinutesBforeLastMessage.toISOString()]
  );
  const lastMessageInReplica = await dbReplicaClient.query(
    `SELECT * FROM messages ORDER BY created_at DESC LIMIT 1`
  );
  console.log(lastMessageInReplica.rows);

  console.log(
    `Found ${
      messagesAfterTime.rows.length
    } messages after ${fiveMinutesBforeLastMessage.toUTCString()}`
  );
  for (const message of messagesAfterTime.rows as Message[]) {
    console.log(message);
    const userId = await getUserId(message.user_name, dbClient, errorFile);
    if (!userId) {
      errorToFile(errorFile, `Failed to get userId for ${message}`);
      console.error("Failed to get userId for", message);
      continue;
    }
    const color = await getColorId(message.color_name, dbClient, errorFile);
    message.created_at.setMinutes(
      message.created_at.getMinutes() - message.created_at.getTimezoneOffset()
    );
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

  await dbClient.end();
  await dbReplicaClient.end();
}

init();
