import { Client } from "pg";
import fs from "fs";

export function errorToFile(file: string, message: string) {
  try {
    fs.appendFileSync(file, `${new Date()} ${message}\n`, "utf8");
  } catch (err) {
    console.error("Failed to append to error.log", err);
  }
}

export async function getUserId(
  messageUserId: string,
  dbClient: Client,
  errorFile: string
) {
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
    errorToFile(errorFile, `Failed to get userId: ${err}`);
    console.error("Failed to get userId", err);
    return null;
  }
}

export async function getColorId(
  messageColor: string | null,
  dbClient: Client,
  errorFile: string
) {
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
    errorToFile(errorFile, `Failed to get colorId: ${err}`);
    console.error("Failed to get colorId", err);
    return null;
  }
}

export async function addToDb(
  messageText: string,
  createdAt: number | Date,
  userId: number,
  colorId: number | null,
  uuid: string,
  dbClient: Client,
  errorFile: string
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
      errorFile,
      `Failed to add to db: ${err} - ${messageText} - ${createdAt} - ${userId} - ${colorId}`
    );
    if (err.code === "23505") {
      // Duplicate entry
      return true;
    }
    console.error(`Failed to add ${uuid} to db. Error in logs`);
    return false;
  }
}
