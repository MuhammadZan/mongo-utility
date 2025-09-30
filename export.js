// Enhanced MongoDB Export Utility with Schema and Migration Support
const { MongoClient } = require("mongodb");
const fs = require("fs");
const path = require("path");

require("dotenv").config();

const uri = process.env.MONGODB_URL; 
const dbName = process.env.DB_NAME; 

// Helper function to analyze field types from sample documents
function analyzeFieldTypes(documents) {
  const schema = {};
  
  documents.forEach(doc => {
    analyzeObject(doc, schema, '');
  });
  
  // Convert counts to most common types
  Object.keys(schema).forEach(field => {
    const types = schema[field];
    const mostCommon = Object.keys(types).reduce((a, b) => 
      types[a] > types[b] ? a : b
    );
    schema[field] = {
      type: mostCommon,
      nullable: types['null'] > 0,
      samples: Math.max(...Object.values(types))
    };
  });
  
  return schema;
}

function analyzeObject(obj, schema, prefix) {
  Object.keys(obj).forEach(key => {
    const fullKey = prefix ? `${prefix}.${key}` : key;
    const value = obj[key];
    const type = getValueType(value);
    
    if (!schema[fullKey]) {
      schema[fullKey] = {};
    }
    
    schema[fullKey][type] = (schema[fullKey][type] || 0) + 1;
    
    // Recursively analyze nested objects
    if (type === 'object' && value !== null) {
      analyzeObject(value, schema, fullKey);
    } else if (type === 'array' && value.length > 0) {
      // Analyze array elements
      value.forEach(item => {
        if (typeof item === 'object' && item !== null) {
          analyzeObject(item, schema, `${fullKey}[]`);
        }
      });
    }
  });
}

function getValueType(value) {
  if (value === null) return 'null';
  if (value === undefined) return 'undefined';
  if (Array.isArray(value)) return 'array';
  if (value instanceof Date) return 'date';
  if (typeof value === 'object' && value.constructor.name === 'ObjectId') return 'objectId';
  return typeof value;
}

// Generate SQL-like CREATE TABLE statements
function generateCreateTableSQL(collectionName, schema) {
  let sql = `-- Schema for collection: ${collectionName}\n`;
  sql += `CREATE TABLE IF NOT EXISTS \`${collectionName}\` (\n`;
  
  const fields = [];
  Object.keys(schema).forEach(field => {
    const fieldInfo = schema[field];
    let sqlType = mongoTypeToSQL(fieldInfo.type);
    let nullable = fieldInfo.nullable ? '' : ' NOT NULL';
    
    // Handle nested fields by flattening them
    const cleanField = field.replace(/\[\]/g, '_array').replace(/\./g, '_');
    fields.push(`  \`${cleanField}\` ${sqlType}${nullable}`);
  });
  
  // Add MongoDB _id as primary key
  if (!schema._id) {
    fields.unshift('  `_id` VARCHAR(24) PRIMARY KEY');
  }
  
  sql += fields.join(',\n');
  sql += '\n);\n\n';
  
  return sql;
}

function mongoTypeToSQL(mongoType) {
  const typeMap = {
    'string': 'TEXT',
    'number': 'DECIMAL(10,2)',
    'boolean': 'BOOLEAN',
    'date': 'DATETIME',
    'objectId': 'VARCHAR(24)',
    'array': 'JSON',
    'object': 'JSON',
    'null': 'TEXT'
  };
  
  return typeMap[mongoType] || 'TEXT';
}

// Generate SQL INSERT statements
function generateInsertSQL(collectionName, documents, schema) {
  let sql = `-- Data for collection: ${collectionName}\n`;
  
  documents.forEach(doc => {
    const fields = [];
    const values = [];
    
    // Flatten the document for SQL insertion
    const flatDoc = flattenObject(doc);
    
    Object.keys(flatDoc).forEach(key => {
      const cleanKey = key.replace(/\[\]/g, '_array').replace(/\./g, '_');
      fields.push(`\`${cleanKey}\``);
      values.push(formatValueForSQL(flatDoc[key]));
    });
    
    sql += `INSERT INTO \`${collectionName}\` (${fields.join(', ')}) VALUES (${values.join(', ')});\n`;
  });
  
  return sql + '\n';
}

function flattenObject(obj, prefix = '') {
  const flattened = {};
  
  Object.keys(obj).forEach(key => {
    const value = obj[key];
    const newKey = prefix ? `${prefix}.${key}` : key;
    
    if (value === null || value === undefined) {
      flattened[newKey] = null;
    } else if (Array.isArray(value) || (typeof value === 'object' && value.constructor.name !== 'ObjectId' && !(value instanceof Date))) {
      flattened[newKey] = JSON.stringify(value);
    } else {
      flattened[newKey] = value;
    }
  });
  
  return flattened;
}

function formatValueForSQL(value) {
  if (value === null || value === undefined) {
    return 'NULL';
  }
  
  if (typeof value === 'string') {
    return `'${value.replace(/'/g, "''")}'`;
  }
  
  if (typeof value === 'boolean') {
    return value ? '1' : '0';
  }
  
  if (value instanceof Date) {
    return `'${value.toISOString().slice(0, 19).replace('T', ' ')}'`;
  }
  
  if (typeof value === 'object' && value.constructor.name === 'ObjectId') {
    return `'${value.toString()}'`;
  }
  
  return value;
}

async function exportCollections() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log("Connected to MongoDB");

    const db = client.db(dbName);

    // Get all collection names
    const collections = await db.listCollections().toArray();

    // Create export directories
    const exportDir = "./db";
    const dataDir = path.join(exportDir, "data");
    const schemaDir = path.join(exportDir, "schema");
    const migrationDir = path.join(exportDir, "migration");

    [exportDir, dataDir, schemaDir, migrationDir].forEach(dir => {
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
      }
    });

    let fullMigrationSQL = `-- Complete Database Migration Script for: ${dbName}\n`;
    fullMigrationSQL += `-- Generated on: ${new Date().toISOString()}\n\n`;
    fullMigrationSQL += `-- Database: ${dbName}\n`;
    fullMigrationSQL += `CREATE DATABASE IF NOT EXISTS \`${dbName}\`;\n`;
    fullMigrationSQL += `USE \`${dbName}\`;\n\n`;

    const databaseSchema = {
      database: dbName,
      collections: {},
      exportDate: new Date().toISOString(),
      totalCollections: collections.length
    };

    for (const coll of collections) {
      const name = coll.name;
      const collection = db.collection(name);

      console.log(`Analyzing and exporting collection: ${name}`);

      // Get collection stats and indexes
      const stats = await db.command({ collStats: name }).catch(() => ({}));
      const indexes = await collection.indexes();
      
      // Get sample documents for schema analysis
      const sampleSize = Math.min(100, stats.count || 100);
      const documents = await collection.find({}).limit(sampleSize).toArray();
      
      if (documents.length === 0) {
        console.log(`Collection ${name} is empty, skipping...`);
        continue;
      }

      // Analyze schema
      const schema = analyzeFieldTypes(documents);
      
      // Get all documents for export
      const allDocuments = await collection.find({}).toArray();

      // Save raw JSON data
      const dataFilePath = path.join(dataDir, `${name}.json`);
      fs.writeFileSync(dataFilePath, JSON.stringify(allDocuments, null, 2));

      // Save schema information
      const collectionSchema = {
        name: name,
        fields: schema,
        indexes: indexes,
        stats: {
          documentCount: allDocuments.length,
          avgDocSize: stats.avgObjSize,
          totalSize: stats.size
        }
      };

      const schemaFilePath = path.join(schemaDir, `${name}_schema.json`);
      fs.writeFileSync(schemaFilePath, JSON.stringify(collectionSchema, null, 2));

      // Generate SQL migration
      const createTableSQL = generateCreateTableSQL(name, schema);
      const insertSQL = generateInsertSQL(name, allDocuments, schema);
      
      const sqlFilePath = path.join(migrationDir, `${name}.sql`);
      fs.writeFileSync(sqlFilePath, createTableSQL + insertSQL);

      // Add to full migration script
      fullMigrationSQL += createTableSQL + insertSQL;

      // Add to database schema
      databaseSchema.collections[name] = collectionSchema;

      console.log(`✓ Exported ${name}: ${allDocuments.length} documents`);
    }

    // Save complete database schema
    const dbSchemaPath = path.join(schemaDir, "database_schema.json");
    fs.writeFileSync(dbSchemaPath, JSON.stringify(databaseSchema, null, 2));

    // Save complete migration script
    const fullMigrationPath = path.join(migrationDir, "complete_migration.sql");
    fs.writeFileSync(fullMigrationPath, fullMigrationSQL);

    // Generate MongoDB shell script for recreation
    let mongoShellScript = `// MongoDB Shell Script to recreate database: ${dbName}\n`;
    mongoShellScript += `// Generated on: ${new Date().toISOString()}\n\n`;
    mongoShellScript += `use ${dbName};\n\n`;

    Object.keys(databaseSchema.collections).forEach(collName => {
      const collSchema = databaseSchema.collections[collName];
      
      // Add indexes
      collSchema.indexes.forEach(index => {
        if (index.name !== '_id_') {
          mongoShellScript += `db.${collName}.createIndex(${JSON.stringify(index.key)});\n`;
        }
      });
    });

    const mongoScriptPath = path.join(migrationDir, "mongodb_recreation.js");
    fs.writeFileSync(mongoScriptPath, mongoShellScript);

    console.log("\n🎉 Export completed successfully!");
    console.log(`📁 Exports saved to: ${exportDir}`);
    console.log(`📊 Data files: ${dataDir}`);
    console.log(`📋 Schema files: ${schemaDir}`);
    console.log(`🔄 Migration files: ${migrationDir}`);
    console.log(`📄 Complete SQL migration: ${fullMigrationPath}`);
    console.log(`🍃 MongoDB recreation script: ${mongoScriptPath}`);

  } catch (err) {
    console.error("Error:", err);
  } finally {
    await client.close();
  }
}

exportCollections();
