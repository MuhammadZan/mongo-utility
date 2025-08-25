# MongoDB Export and Import Tool

A simple Node.js tool for exporting and importing MongoDB collections to/from JSON files.

## Features

- Export all collections from a MongoDB database to JSON files
- Import collections from JSON files back to MongoDB
- Automatic collection discovery and processing
- Environment-based configuration
- Clean JSON formatting with proper indentation

## Prerequisites

- Node.js (v14 or higher)
- MongoDB instance (local or remote)
- npm or yarn package manager

## Installation

1. Clone or download this project
2. Install dependencies:
   ```bash
   npm install
   ```

## Configuration

1. Copy the environment file:
   ```bash
   cp .env.local .env
   ```

2. Edit the `.env` file with your MongoDB connection details:
   ```env
   MONGODB_URL=mongodb://localhost:27017
   DB_NAME=your_database_name
   ```

   **Environment Variables:**
   - `MONGODB_URL`: Your MongoDB connection string
   - `DB_NAME`: The name of the database to export/import

## Usage

### Exporting Collections

To export all collections from your MongoDB database:

```bash
npm run export
```

Or directly:
```bash
node export.js
```

This will:
- Connect to your MongoDB database
- Create an `exports` directory (if it doesn't exist)
- Export each collection to a separate JSON file in the `exports` folder
- Each file will be named `{collection_name}.json`

### Importing Collections

To import collections from JSON files:

```bash
npm run import
```

Or directly:
```bash
node import.js
```

This will:
- Connect to your MongoDB database
- Read all JSON files from the `mongodb_exports` directory
- Clear existing collections (⚠️ **Warning: This will delete existing data**)
- Import the data from JSON files into corresponding collections

## Directory Structure

```
mongo_export_and_import/
├── .env.local              # Environment configuration template
├── .gitignore             # Git ignore rules
├── README.md              # This file
├── export.js              # Export script
├── import.js              # Import script
├── package.json           # Node.js dependencies and scripts
├── exports/               # Created during export (contains exported JSON files)
└── mongodb_exports/       # Place JSON files here for import
    └── README.md          # Instructions for import directory
```

## Important Notes

⚠️ **Data Safety Warning**: The import process will **delete all existing data** in the target collections before importing new data. Make sure to backup your data before running imports.

- Export files are saved to the `exports/` directory
- Import files should be placed in the `mongodb_exports/` directory
- Each JSON file should contain an array of MongoDB documents
- File names determine the collection names (without the .json extension)

## Example Workflow

1. **Setup environment**:
   ```bash
   cp .env.local .env
   # Edit .env with your MongoDB details
   ```

2. **Export from source database**:
   ```bash
   npm run export
   ```

3. **Move exported files for import** (if needed):
   ```bash
   cp exports/*.json mongodb_exports/
   ```

4. **Import to target database**:
   ```bash
   # Update .env with target database details
   npm run import
   ```

## Troubleshooting

- **Connection Error**: Verify your `MONGODB_URL` and ensure MongoDB is running
- **Permission Error**: Ensure the application has write permissions for the export directory
- **Import Error**: Check that JSON files are valid and contain proper MongoDB document arrays
- **Missing Collections**: Ensure JSON files are placed in the `mongodb_exports/` directory

## Dependencies

- `mongodb`: Official MongoDB driver for Node.js
- `dotenv`: Environment variable loader

## License

ISC License