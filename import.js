// Enhanced MongoDB Import Utility with Schema-based Database Recreation
const { MongoClient, ObjectId } = require("mongodb");
const fs = require("fs");
const path = require("path");

require("dotenv").config();

const uri = process.env.MONGODB_URL;
const dbName = process.env.DB_NAME;

// Configuration options
const IMPORT_OPTIONS = {
  recreateDatabase: true,        // Drop and recreate database
  recreateIndexes: true,         // Recreate indexes from schema
  validateData: true,            // Validate data against schema
  typeCasting: true,             // Automatically cast data to required types
  batchSize: 1000,              // Batch size for bulk operations
  clearCollections: true,        // Clear existing collections before import
  selectiveImport: null          // Array of collection names to import (null = all)
};

// Type casting functions
function castToType(value, targetType, fieldPath) {
  if (value === null || value === undefined) {
    return value;
  }

  const currentType = getValueType(value);
  
  // If already correct type, return as is
  if (currentType === targetType) {
    return value;
  }

  try {
    switch (targetType) {
      case 'string':
        return castToString(value);
      
      case 'number':
        return castToNumber(value);
      
      case 'boolean':
        return castToBoolean(value);
      
      case 'date':
        return castToDate(value);
      
      case 'objectId':
        return castToObjectId(value);
      
      case 'array':
        return castToArray(value);
      
      case 'object':
        return castToObject(value);
      
      default:
        return value;
    }
  } catch (error) {
    console.warn(`    ⚠️  Type casting failed for field '${fieldPath}': ${error.message}`);
    return value; // Return original value if casting fails
  }
}

function castToString(value) {
  if (typeof value === 'string') return value;
  if (typeof value === 'number' || typeof value === 'boolean') return String(value);
  if (value instanceof Date) return value.toISOString();
  if (typeof value === 'object' && value.constructor && value.constructor.name === 'ObjectId') {
    return value.toString();
  }
  if (typeof value === 'object') return JSON.stringify(value);
  return String(value);
}

function castToNumber(value) {
  if (typeof value === 'number') return value;
  if (typeof value === 'string') {
    const num = Number(value);
    if (isNaN(num)) throw new Error(`Cannot convert '${value}' to number`);
    return num;
  }
  if (typeof value === 'boolean') return value ? 1 : 0;
  if (value instanceof Date) return value.getTime();
  throw new Error(`Cannot convert ${typeof value} to number`);
}

function castToBoolean(value) {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'string') {
    const lower = value.toLowerCase();
    if (lower === 'true' || lower === '1' || lower === 'yes' || lower === 'on') return true;
    if (lower === 'false' || lower === '0' || lower === 'no' || lower === 'off') return false;
    throw new Error(`Cannot convert string '${value}' to boolean`);
  }
  if (typeof value === 'number') return value !== 0;
  throw new Error(`Cannot convert ${typeof value} to boolean`);
}

function castToDate(value) {
  if (value instanceof Date) return value;
  if (typeof value === 'string') {
    const date = new Date(value);
    if (isNaN(date.getTime())) throw new Error(`Invalid date string: '${value}'`);
    return date;
  }
  if (typeof value === 'number') {
    // Assume timestamp (milliseconds or seconds)
    const date = new Date(value > 1e10 ? value : value * 1000);
    if (isNaN(date.getTime())) throw new Error(`Invalid timestamp: ${value}`);
    return date;
  }
  throw new Error(`Cannot convert ${typeof value} to date`);
}

function castToObjectId(value) {
  if (typeof value === 'object' && value.constructor && value.constructor.name === 'ObjectId') {
    return value;
  }
  if (typeof value === 'string') {
    if (ObjectId.isValid(value)) {
      return new ObjectId(value);
    }
    throw new Error(`Invalid ObjectId string: '${value}'`);
  }
  throw new Error(`Cannot convert ${typeof value} to ObjectId`);
}

function castToArray(value) {
  if (Array.isArray(value)) return value;
  if (typeof value === 'string') {
    try {
      const parsed = JSON.parse(value);
      if (Array.isArray(parsed)) return parsed;
      return [value]; // Wrap single string in array
    } catch {
      return [value]; // Wrap single string in array
    }
  }
  return [value]; // Wrap any other type in array
}

function castToObject(value) {
  if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
    return value;
  }
  if (typeof value === 'string') {
    try {
      const parsed = JSON.parse(value);
      if (typeof parsed === 'object' && parsed !== null && !Array.isArray(parsed)) {
        return parsed;
      }
    } catch {
      // If JSON parsing fails, create object with the string value
    }
  }
  // For non-object types, create a wrapper object
  return { value: value };
}

// Helper function to validate document against schema
function validateDocument(doc, schema) {
  const errors = [];
  const warnings = [];
  let typeCastingCount = 0;

  function validateField(value, fieldSchema, fieldPath) {
    // Handle null/undefined values
    if (value === null || value === undefined) {
      if (fieldSchema.required && fieldSchema.nullCount === 0) {
        errors.push(`Required field '${fieldPath}' is missing`);
      }
      return { value, castingPerformed: false };
    }

    const currentType = getValueType(value);
    const expectedType = fieldSchema.type;
    let finalValue = value;
    let castingPerformed = false;

    // Type casting if enabled and types don't match
    if (IMPORT_OPTIONS.typeCasting && currentType !== expectedType) {
      try {
        finalValue = castToType(value, expectedType, fieldPath);
        if (finalValue !== value) {
          castingPerformed = true;
          typeCastingCount++;
          console.log(`    🔄 Type cast: ${fieldPath} (${currentType} → ${expectedType})`);
        }
      } catch (error) {
        warnings.push(`Type casting failed for '${fieldPath}': ${error.message}`);
      }
    }

    // Validate type after potential casting
    const finalType = getValueType(finalValue);
    if (finalType !== expectedType) {
      if (IMPORT_OPTIONS.typeCasting) {
        warnings.push(`Field '${fieldPath}' has type '${finalType}', expected '${expectedType}' (casting failed)`);
      } else {
        warnings.push(`Field '${fieldPath}' has type '${finalType}', expected '${expectedType}'`);
      }
    }

    // Validate nested objects
    if (finalType === 'object' && fieldSchema.nestedFields) {
      for (const [nestedField, nestedSchema] of Object.entries(fieldSchema.nestedFields)) {
        if (finalValue.hasOwnProperty(nestedField)) {
          const nestedResult = validateField(
            finalValue[nestedField], 
            nestedSchema, 
            `${fieldPath}.${nestedField}`
          );
          finalValue[nestedField] = nestedResult.value;
          if (nestedResult.castingPerformed) {
            castingPerformed = true;
          }
        }
      }
    }

    // Validate array elements
    if (finalType === 'array' && Array.isArray(finalValue) && fieldSchema.arrayElementType) {
      finalValue = finalValue.map((item, index) => {
        const itemResult = validateField(
          item,
          { type: fieldSchema.arrayElementType, required: false },
          `${fieldPath}[${index}]`
        );
        if (itemResult.castingPerformed) {
          castingPerformed = true;
        }
        return itemResult.value;
      });
    }

    return { value: finalValue, castingPerformed };
  }

  // Create a copy of the document for modification
  let validatedDoc = JSON.parse(JSON.stringify(doc));

  // Validate each field in the schema
  for (const [fieldName, fieldSchema] of Object.entries(schema.fields)) {
    if (validatedDoc.hasOwnProperty(fieldName)) {
      const result = validateField(validatedDoc[fieldName], fieldSchema, fieldName);
      validatedDoc[fieldName] = result.value;
    } else if (fieldSchema.required && fieldSchema.nullCount === 0) {
      errors.push(`Required field '${fieldName}' is missing`);
    }
  }

  return {
    isValid: errors.length === 0,
    document: validatedDoc,
    errors,
    warnings,
    typeCastingCount
  };
}

function getNestedValue(obj, path) {
  return path.split('.').reduce((current, key) => {
    if (key.endsWith('[]')) {
      // Handle array notation
      const arrayKey = key.slice(0, -2);
      return current && current[arrayKey] ? current[arrayKey] : undefined;
    }
    return current && current[key] !== undefined ? current[key] : undefined;
  }, obj);
}

function getValueType(value) {
  if (value === null) return 'null';
  if (value === undefined) return 'undefined';
  if (Array.isArray(value)) return 'array';
  if (value instanceof Date) return 'date';
  if (typeof value === 'object' && value.constructor && value.constructor.name === 'ObjectId') return 'objectId';
  return typeof value;
}

// Create indexes for a collection based on schema
async function createIndexes(collection, indexes, collectionName) {
  console.log(`  📋 Creating indexes for ${collectionName}...`);
  
  for (const index of indexes) {
    try {
      if (index.name === '_id_') {
        continue; // Skip default _id index
      }
      
      const indexOptions = {
        name: index.name,
        background: true
      };
      
      // Add additional index options if they exist
      if (index.unique) indexOptions.unique = true;
      if (index.sparse) indexOptions.sparse = true;
      if (index.partialFilterExpression) indexOptions.partialFilterExpression = index.partialFilterExpression;
      
      await collection.createIndex(index.key, indexOptions);
      console.log(`    ✓ Created index: ${index.name}`);
    } catch (error) {
      console.warn(`    ⚠️  Failed to create index ${index.name}: ${error.message}`);
    }
  }
}

// Import data in batches with validation
async function importCollectionData(collection, documents, schema, collectionName) {
  console.log(`  📊 Importing ${documents.length} documents into ${collectionName}...`);
  
  let validDocuments = [];
  let invalidCount = 0;
  let totalTypeCasts = 0;
  let totalValidationErrors = 0;
  let totalValidationWarnings = 0;
  
  // Validate documents if validation is enabled
  if (IMPORT_OPTIONS.validateData) {
    console.log(`  🔍 Validating documents against schema...`);
    
    for (let i = 0; i < documents.length; i++) {
      const doc = documents[i];
      const validation = validateDocument(doc, schema);
      
      if (validation.isValid) {
        validDocuments.push(validation.document);
        totalTypeCasts += validation.typeCastingCount;
        totalValidationWarnings += validation.warnings.length;
        
        // Log warnings if any
        if (validation.warnings.length > 0) {
          validation.warnings.forEach(warning => {
            console.warn(`    ⚠️  ${warning}`);
          });
        }
      } else {
        totalValidationErrors += validation.errors.length;
        console.error(`    ❌ Document ${i + 1} validation failed:`);
        validation.errors.forEach(error => {
          console.error(`      - ${error}`);
        });
        invalidCount++;
        
        // Optionally skip invalid documents or include them anyway
        if (!IMPORT_OPTIONS.skipInvalidDocuments) {
          validDocuments.push(validation.document); // Include with potential casting
        }
      }
    }
    
    console.log(`  ✓ Validation complete: ${validDocuments.length} valid, ${invalidCount} invalid`);
    if (totalTypeCasts > 0) {
      console.log(`  🔄 Total type casts performed: ${totalTypeCasts}`);
    }
    if (totalValidationWarnings > 0) {
      console.log(`  ⚠️  Total validation warnings: ${totalValidationWarnings}`);
    }
    if (totalValidationErrors > 0) {
      console.log(`  ❌ Total validation errors: ${totalValidationErrors}`);
    }
  } else {
    validDocuments = documents;
  }
  
  if (validDocuments.length === 0) {
    console.log(`  ⚠️  No valid documents to import for ${collectionName}`);
    return 0;
  }
  
  // Clear existing collection if option is enabled
  if (IMPORT_OPTIONS.clearCollections) {
    await collection.deleteMany({});
    console.log(`  🗑️  Cleared existing documents from ${collectionName}`);
  }
  
  // Import in batches
  let importedCount = 0;
  const batchSize = IMPORT_OPTIONS.batchSize;
  
  for (let i = 0; i < validDocuments.length; i += batchSize) {
    const batch = validDocuments.slice(i, i + batchSize);
    
    try {
      await collection.insertMany(batch, { ordered: false });
      importedCount += batch.length;
      console.log(`    ✓ Imported batch: ${importedCount}/${validDocuments.length} documents`);
    } catch (error) {
      console.error(`    ❌ Failed to import batch starting at ${i}: ${error.message}`);
      
      // Try inserting documents one by one in case of partial failures
      for (const doc of batch) {
        try {
          await collection.insertOne(doc);
          importedCount++;
        } catch (docError) {
          console.warn(`    ⚠️  Failed to import document: ${docError.message}`);
        }
      }
    }
  }
  
  return {
    success: true,
    imported: importedCount,
    total: documents.length,
    typeCasts: totalTypeCasts,
    warnings: totalValidationWarnings,
    errors: totalValidationErrors
  };
}

async function importCollections() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log("Connected to MongoDB");

    // Recreate database if option is enabled
    if (IMPORT_OPTIONS.recreateDatabase) {
      console.log(`Dropping existing database: ${dbName}`);
      await client.db(dbName).dropDatabase();
      console.log(`Database ${dbName} dropped`);
    }

    const db = client.db(dbName);

    // Read database schema
    const schemaDir = path.join(__dirname, "db", "schema");
    const dataDir = path.join(__dirname, "db", "data");
    
    if (!fs.existsSync(schemaDir) || !fs.existsSync(dataDir)) {
      throw new Error("Export directories not found. Please run export first.");
    }

    const databaseSchemaPath = path.join(schemaDir, "database_schema.json");
    if (!fs.existsSync(databaseSchemaPath)) {
      throw new Error("Database schema file not found. Please run export first.");
    }

    console.log("Reading database schema...");
    const databaseSchema = JSON.parse(fs.readFileSync(databaseSchemaPath, "utf-8"));
    
    console.log(`Database: ${databaseSchema.database}`);
    console.log(`Export Date: ${databaseSchema.exportDate}`);
    console.log(`Collections: ${databaseSchema.totalCollections}`);

    // Get collections to import
    const collectionsToImport = IMPORT_OPTIONS.selectiveImport || 
                               Object.keys(databaseSchema.collections);

    console.log(`\n Starting import of ${collectionsToImport.length} collections...\n`);

    let totalImported = 0;
    let totalCollections = 0;
    let totalTypeCasts = 0;
    let totalWarnings = 0;
    let totalErrors = 0;

    for (const collectionName of collectionsToImport) {
      if (!databaseSchema.collections[collectionName]) {
        console.warn(` Collection '${collectionName}' not found in schema, skipping...`);
        continue;
      }

      const collectionSchema = databaseSchema.collections[collectionName];
      const dataFilePath = path.join(dataDir, `${collectionName}.json`);

      if (!fs.existsSync(dataFilePath)) {
        console.warn(`Data file for '${collectionName}' not found, skipping...`);
        continue;
      }

      console.log(`\n Processing collection: ${collectionName}`);
      console.log(`   Expected documents: ${collectionSchema.stats.documentCount}`);
      console.log(`   Fields: ${Object.keys(collectionSchema.fields).length}`);
      console.log(`   Indexes: ${collectionSchema.indexes.length}`);

      // Read collection data
      const documents = JSON.parse(fs.readFileSync(dataFilePath, "utf-8"));
      
      if (documents.length === 0) {
        console.log(` No documents found in ${collectionName}, skipping...`);
        continue;
      }

      // Create collection
      const collection = db.collection(collectionName);

      // Import data
      const result = await importCollectionData(
        collection, 
        documents, 
        collectionSchema, 
        collectionName
      );

      // Create indexes if option is enabled
      if (IMPORT_OPTIONS.recreateIndexes && collectionSchema.indexes.length > 0) {
        await createIndexes(collection, collectionSchema.indexes, collectionName);
      }

      if (result.success) {
        totalImported += result.imported;
        totalTypeCasts += result.typeCasts || 0;
        totalWarnings += result.warnings || 0;
        totalErrors += result.errors || 0;
      }
      totalCollections++;

      console.log(`  ✅ Collection ${collectionName} completed: ${result.imported} documents imported`);
    }

    console.log(`\n🎉 Import completed successfully!`);
    console.log(` Summary:`);
    console.log(`   Collections processed: ${totalCollections}`);
    console.log(`   Total documents imported: ${totalImported}`);
    if (totalTypeCasts > 0) {
      console.log(`   Type casts performed: ${totalTypeCasts}`);
    }
    if (totalWarnings > 0) {
      console.log(`     Validation warnings: ${totalWarnings}`);
    }
    if (totalErrors > 0) {
      console.log(`   Validation errors: ${totalErrors}`);
    }
    console.log(`   Type casting: ${IMPORT_OPTIONS.typeCasting ? 'Enabled' : 'Disabled'}`);
    console.log(`   Data validation: ${IMPORT_OPTIONS.validateData ? 'Enabled' : 'Disabled'}`);
    console.log(`   Database: ${dbName}`);
    
    // Verify import by getting collection stats
    console.log(`\n Verification - Collection Statistics:`);
    const collections = await db.listCollections().toArray();
    
    for (const coll of collections) {
      const collection = db.collection(coll.name);
      const count = await collection.countDocuments();
      const indexes = await collection.indexes();
      console.log(`  📦 ${coll.name}: ${count} documents, ${indexes.length} indexes`);
    }

  } catch (err) {
    console.error("❌ Import Error:", err.message);
    console.error("Stack trace:", err.stack);
  } finally {
    await client.close();
    console.log("🔌 Disconnected from MongoDB");
  }
}

// Allow configuration via command line arguments
if (process.argv.length > 2) {
  const args = process.argv.slice(2);
  
  args.forEach(arg => {
    if (arg === '--no-recreate-db') IMPORT_OPTIONS.recreateDatabase = false;
    if (arg === '--no-indexes') IMPORT_OPTIONS.recreateIndexes = false;
    if (arg === '--no-validation') IMPORT_OPTIONS.validateData = false;
    if (arg === '--no-clear') IMPORT_OPTIONS.clearCollections = false;
    if (arg.startsWith('--collections=')) {
      IMPORT_OPTIONS.selectiveImport = arg.split('=')[1].split(',');
    }
    if (arg.startsWith('--batch-size=')) {
      IMPORT_OPTIONS.batchSize = parseInt(arg.split('=')[1]);
    }
  });
}

console.log("🔧 Import Configuration:");
console.log(`  🗑️  Recreate Database: ${IMPORT_OPTIONS.recreateDatabase}`);
console.log(`  📋 Recreate Indexes: ${IMPORT_OPTIONS.recreateIndexes}`);
console.log(`  🔍 Validate Data: ${IMPORT_OPTIONS.validateData}`);
console.log(`  🔄 Type Casting: ${IMPORT_OPTIONS.typeCasting}`);
console.log(`  🗑️  Clear Collections: ${IMPORT_OPTIONS.clearCollections}`);
console.log(`  📦 Batch Size: ${IMPORT_OPTIONS.batchSize}`);
console.log(`  🎯 Selective Import: ${IMPORT_OPTIONS.selectiveImport || 'All collections'}\n`);

importCollections();
