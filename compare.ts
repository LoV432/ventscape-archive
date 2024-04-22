import sqlite3 from "sqlite3";

// Function to connect to the database
function connectDatabase(dbPath) {
  return new sqlite3.Database(dbPath, sqlite3.OPEN_READONLY, (err) => {
    if (err) {
      console.error(
        `Error connecting to the database ${dbPath}: ${err.message}`
      );
    } else {
      console.log(`Connected to the database ${dbPath}`);
    }
  });
}

// Function to compare databases
function compareDatabases(dbPath1, dbPath2) {
  const db1 = connectDatabase(dbPath1);
  const db2 = connectDatabase(dbPath2);

  // Attach the second database
  db1.serialize(() => {
    db1.run(`ATTACH DATABASE '${dbPath2}' AS db2`);
  });

  // Query to find missing createdAt values in db2
  const query = `
      SELECT createdAt
      FROM messages
      WHERE createdAt NOT IN (
          SELECT createdAt FROM db2.messages
      )
  `;

  // Execute the query on db1
  db1.all(query, (err, rows: { createdAt: number }[]) => {
    if (err) {
      console.error(`Error executing query on ${dbPath1}: ${err.message}`);
      return;
    }

    // Log missing createdAt values
    console.log(`Missing createdAt values in ${dbPath2}:`);
    rows.forEach((row) => {
      console.log(row.createdAt);
    });

    // Close database connections
    db1.close((err) => {
      if (err) {
        console.error(`Error closing ${dbPath1}: ${err.message}`);
      } else {
        console.log(`Disconnected from ${dbPath1}`);
      }
    });

    db2.close((err) => {
      if (err) {
        console.error(`Error closing ${dbPath2}: ${err.message}`);
      } else {
        console.log(`Disconnected from ${dbPath2}`);
      }
    });
  });
}

// Usage: node compare-databases.js db1.db db2.db
const [dbPath1, dbPath2] = process.argv.slice(2);
if (!dbPath1 || !dbPath2) {
  console.error("Usage: node compare-databases.js <dbPath1> <dbPath2>");
  process.exit(1);
}

// Compare databases
compareDatabases(dbPath1, dbPath2);
