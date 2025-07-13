import pg from 'pg';
import { createClient } from 'redis';
import puppeteer from 'puppeteer';
import { config } from 'dotenv';
import { refetch } from './refetch';
import { errorToFile, getUserId, addToDb, getColorId, getFontId } from './utils';
config();

type Message = {
    userId: string;
    id: string;
    messageText: string;
    createdAt: number;
    color: string;
    nickname: string | null;
    font: string | null;
};

async function init() {
    const errorFile = 'logs/pg-error.log';
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
        port: parseInt(process.env.PG_PORT || '5432'),
    });
    await dbClient.connect();
    await redisClient.connect();

    try {
        await dbClient.query('BEGIN');
        await dbClient.query(`CREATE TABLE IF NOT EXISTS colors (
      id SERIAL PRIMARY KEY,
      color_name VARCHAR(7) NOT NULL UNIQUE
    )`);
        await dbClient.query(`CREATE TABLE IF NOT EXISTS fonts (
      id SERIAL PRIMARY KEY,
      font_name VARCHAR(20) NOT NULL UNIQUE
    )`);

        await dbClient.query(`CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      user_name VARCHAR(50) NOT NULL UNIQUE
    )`);

        await dbClient.query(`CREATE TABLE IF NOT EXISTS messages (
      id SERIAL PRIMARY KEY,
      uuid UUID NOT NULL UNIQUE,
      message_text TEXT,
      created_at TIMESTAMPTZ,
      user_id INTEGER,
      color_id INTEGER,
      font_id INTEGER,
      is_deleted BOOLEAN DEFAULT FALSE,
      nickname VARCHAR(50),
      FOREIGN KEY (user_id) REFERENCES users(id),
      FOREIGN KEY (color_id) REFERENCES colors(id)
      FOREIGN KEY (font_id) REFERENCES fonts(id)
    )`);

        await dbClient.query(`CREATE EXTENSION IF NOT EXISTS pg_trgm`);
        await dbClient.query(
            `CREATE INDEX IF NOT EXISTS idx_messages_message_text ON messages USING gin (message_text gin_trgm_ops)`
        );
        await dbClient.query(
            `CREATE INDEX IF NOT EXISTS idx_messages_created_at ON messages(created_at)`
        );
        await dbClient.query(
            `CREATE INDEX IF NOT EXISTS idx_messages_color_id ON messages(color_id)`
        );
        await dbClient.query(
            `CREATE INDEX IF NOT EXISTS idx_messages_user_id on messages(user_id)`
        );
        await dbClient.query(
            `CREATE INDEX IF NOT EXISTS idx_colors_id ON colors(id)`
        );
        await dbClient.query(
            `CREATE INDEX IF NOT EXISTS idx_users_id on users(id)`
        );
        await dbClient.query(`COMMIT`);
    } catch (err) {
        await dbClient.query('ROLLBACK');
        await dbClient.end();
        errorToFile(errorFile, `Failed to create tables: ${err}`);
        console.error(err);
        process.exit(1);
    }

    // Launch the browser and open a new blank page
    const browser = await puppeteer.launch();
    const page = await browser.newPage();

    // Navigate the page to a URL
    await page.goto('https://www.ventscape.life/');
    errorToFile(errorFile, 'Script started');
    console.log(await page.title());

    const client = await page.createCDPSession();
    await client.send('Network.enable');
    let purgeInProgress = false;
    let refetchTimeout: NodeJS.Timeout;
    const dbPurge = setInterval(purgeToDb, 5 * 60 * 1000);
    let previousRows: number = 0;

    client.on('Network.webSocketFrameReceived', async ({ response }) => {
        if (response.payloadData.startsWith('42')) {
            const data = response.payloadData;
            let jsonData: Message;
            try {
                jsonData = JSON.parse(
                    data.slice(data.indexOf('{'), -1)
                ) as Message;
            } catch (err) {
                errorToFile(
                    errorFile,
                    `Could not parse data sent by the server: ${data} ${err}`
                );
                console.error(
                    'Could not parse data sent by the server. Error in logs'
                );
                return;
            }
            try {
                await redisClient.set(jsonData.id, JSON.stringify(jsonData));
            } catch (err) {
                errorToFile(
                    errorFile,
                    `Failed to store data in redis: ${data} ${err}`
                );
                console.error('Failed to store data in redis. Error in logs');
                return;
            }
        }
    });

    client.on(
        'Network.webSocketFrameError',
        async ({ errorMessage, requestId, timestamp }) => {
            console.error('Network.webSocketFrameError. Error in logs');
            errorToFile(
                errorFile,
                `Network.webSocketFrameError - ${timestamp} - ${requestId} - ${errorMessage}`
            );
        }
    );

    client.on('Network.webSocketClosed', async ({ requestId, timestamp }) => {
        console.error('Network.webSocketClosed', requestId, timestamp);
        errorToFile(
            errorFile,
            `Network.webSocketClosed - ${timestamp} - ${requestId}`
        );
        // refetchTimeout && clearTimeout(refetchTimeout);
        // refetchTimeout = setTimeout(handleRefetch, 20000);
    });

    client.on(
        'Network.webSocketCreated',
        async ({ requestId, url, initiator }) => {
            console.error('Network.webSocketCreated. More info in logs');
            errorToFile(
                errorFile,
                `Network.webSocketCreated - ${url} - ${initiator} - ${requestId}`
            );
            refetchTimeout && clearTimeout(refetchTimeout);
            refetchTimeout = setTimeout(() => handleRefetch(true), 20000);
        }
    );

    async function purgeToDb() {
        if (purgeInProgress) {
            errorToFile(errorFile, 'Purge already in progress, skipping');
            console.info('Purge already in progress, skipping');
            return;
        }
        purgeInProgress = true;
        console.info(`${new Date()} Starting purge to DB...`);
        try {
            const rows = await redisClient.keys('*');
            for (const row of rows) {
                const message = JSON.parse(
                    (await redisClient.get(row)) as string
                ) as Message;
                const userId = await getUserId(
                    message.userId,
                    dbClient,
                    errorFile
                );
                if (!userId) {
                    errorToFile(
                        errorFile,
                        `Failed to get userId for ${JSON.stringify(message)}`
                    );
                    console.error('Failed to get userId for. Error in logs');
                    continue;
                }
                const color = await getColorId(
                    message.color,
                    dbClient,
                    errorFile
                );
                const font = await getFontId(
                    message.font,
                    dbClient,
                    errorFile
                );
                const addToDbResult = await addToDb(
                    message.messageText,
                    message.createdAt,
                    userId,
                    color,
                    message.id,
                    message.nickname,
                    font,
                    dbClient,
                    errorFile
                );
                if (addToDbResult) {
                    await redisClient.del(row);
                }
            }
            console.info(
                `${new Date()} Purged to DB... (${rows.length} messages)`
            );
            if (previousRows === 0 && rows.length === 0) {
                try {
                    console.info(
                        `Something might be wrong, no messages in 10 minutes. Reloading page...`
                    );
                    errorToFile(
                        errorFile,
                        'Something might be wrong, no messages in 10 minutes. Reloading page...'
                    );
                    // page.reload(); did not work.
                    // This might have better chances of working... Will see i guess
                    await page.goto(
                        `https://www.ventscape.life/?reload=${Math.random()}`
                    );
                } catch (err) {
                    errorToFile(errorFile, `Failed to reload page: ${err}`);
                    console.error('Failed to reload page. Error in logs');
                }
            }
            previousRows = rows.length;
        } catch (err) {
            errorToFile(errorFile, `Failed to purge to db: ${err}`);
            console.error('Failed to purge to db. Error in logs');
        } finally {
            purgeInProgress = false;
        }
    }

    async function handleRefetch(retryOnError = false) {
        let success = false;
        let tries = 0;

        while (!success && tries < 10 && retryOnError) {
            if (tries > 0) {
                console.info(`${new Date()} Retrying refetch, tries: ${tries}`);
                errorToFile(errorFile, `Retrying refetch, tries: ${tries}`);
                // TODO: I might need to fine tune this delay for better results.
                await new Promise((resolve) => setTimeout(resolve, 10000));
            }

            const refetchPage = await browser.newPage();
            try {
                console.info(`${new Date()} Starting refetch...`);
                errorToFile(errorFile, `Starting refetch...`);
                const { messages } = await refetch(refetchPage);
                console.info(
                    `${new Date()} Finished refetch, ${
                        messages.length
                    } messages`
                );
                errorToFile(
                    errorFile,
                    `Finished refetch - ${JSON.stringify(messages)}`
                );
                for (const message of messages) {
                    await redisClient.set(message.id, JSON.stringify(message));
                }
                success = true;
            } catch (err) {
                errorToFile(
                    errorFile,
                    `Failed to refetch: ${JSON.stringify(err)}`
                );
                console.error('Failed to refetch. Error in logs');
            } finally {
                tries++;
                await refetchPage.close();
            }
        }
        if (!success) {
            console.error('All attempts to refetch failed');
            errorToFile(errorFile, 'All attempts to refetch failed');
        }
    }
}

init();
