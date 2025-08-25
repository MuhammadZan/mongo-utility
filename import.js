// import_all_collections.js
const { MongoClient } = require("mongodb");
const fs = require("fs");
const path = require("path");

const uri = "mongodb://localhost:27017"; // Change to your MongoDB URI
const dbName = "your_database"; // Change to your database name

async function importCollections() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log("✅ Connected to MongoDB");

    const db = client.db(dbName);

    const exportsDir = path.join(__dirname, "exports");
    const files = fs.readdirSync(exportsDir).filter(f => f.endsWith(".json"));

    for (const file of files) {
      const collectionName = path.basename(file, ".json");
      const filePath = path.join(exportsDir, file);

      console.log(`📥 Importing collection: ${collectionName}`);

      // Read JSON data
      const rawData = fs.readFileSync(filePath, "utf-8");
      const documents = JSON.parse(rawData);

      if (documents.length > 0) {
        const collection = db.collection(collectionName);

        // Optionally clear the existing collection before inserting
        await collection.deleteMany({});
        
        await collection.insertMany(documents);
        console.log(`✅ Imported ${documents.length} documents into ${collectionName}`);
      } else {
        console.log(`⚠️ Skipped ${collectionName} (no documents)`);
      }
    }

    console.log("🎉 All collections imported!");
  } catch (err) {
    console.error("❌ Error:", err);
  } finally {
    await client.close();
  }
}

importCollections();
