// export_all_collections.js
const { MongoClient } = require("mongodb");
const fs = require("fs");
const path = require("path");

require("dotenv").config();

const uri = process.env.MONGO_DB_URL; // Change to your MongoDB URI
const dbName = process.env.DB_NAME; // Change to your database name

async function exportCollections() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log("✅ Connected to MongoDB");

    const db = client.db(dbName);

    // Get all collection names
    const collections = await db.listCollections().toArray();

    if (!fs.existsSync("./exports")) {
      fs.mkdirSync("./exports");
    }

    for (const coll of collections) {
      const name = coll.name;
      const collection = db.collection(name);

      console.log(`📦 Exporting collection: ${name}`);

      // Fetch all documents
      const documents = await collection.find({}).toArray();

      // Save as JSON
      const filePath = path.join(__dirname, "exports", `${name}.json`);
      fs.writeFileSync(filePath, JSON.stringify(documents, null, 2));

      console.log(`✅ Exported ${name} to ${filePath}`);
    }

    console.log("🎉 All collections exported!");
  } catch (err) {
    console.error("❌ Error:", err);
  } finally {
    await client.close();
  }
}

exportCollections();
