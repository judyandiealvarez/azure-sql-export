# Azure SQL Database Export & Import Tools

Python scripts to export and import Azure SQL Database schema objects and table data for migration between servers.

## Features

### Export Tool (`azure_sql_export.py`)
- **Schema Export**: Exports tables, views, stored procedures, functions, and triggers
- **Data Export**: Exports table data as SQL INSERT statements
- **Organized Output**: Creates structured directories for easy migration
- **Authentication Support**: Supports both SQL Server and Azure AD authentication
- **Batch Processing**: Handles large datasets efficiently
- **Migration Script**: Generates a master migration script with proper order

### Import Tool (`azure_sql_import.py`)
- **Interactive Import**: Compare and confirm imports with detailed differences
- **Schema Comparison**: Shows differences between existing and new objects
- **Data Import Options**: Truncate and import or append to existing tables
- **Safe Import**: Rollback capabilities and detailed logging
- **Object Management**: ALTER existing objects or skip them
- **Progress Tracking**: Real-time progress updates and batch processing

## Prerequisites

- Python 3.7+
- ODBC Driver 17 for SQL Server (or compatible version)
- Access to Azure SQL Database

## Installation

1. Clone or download this repository
2. Install required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

### For Export (Source Database)

1. Copy and edit the export configuration file:
   ```bash
   cp config.example.yaml config.yaml
   ```

2. Update `config.yaml` with your **source** Azure SQL Database details:

   **For SQL Server Authentication:**
   ```yaml
   # Database connection settings
   server: "your-server.database.windows.net"
   database: "your-database-name"
   
   # Authentication type: "sql" or "azure_ad"
   authentication_type: "sql"
   
   # SQL Server authentication
   username: "your-username"
   password: "your-password"
   
   # ODBC driver name
   driver: "ODBC Driver 17 for SQL Server"
   
   # Output settings
   output_directory: "export_output"
   export_data: true
   batch_size: 1000
   
   # Schema filtering
   include_schemas: []
   exclude_schemas:
     - "sys"
     - "INFORMATION_SCHEMA"
   ```

   **For Azure AD Authentication:**
   ```yaml
   # Database connection settings
   server: "your-server.database.windows.net"
   database: "your-database-name"
   
   # Authentication type: "sql" or "azure_ad"
   authentication_type: "azure_ad"
   
   # ODBC driver name
   driver: "ODBC Driver 17 for SQL Server"
   
   # Output settings
   output_directory: "export_output"
   export_data: true
   batch_size: 1000
   
   # Schema filtering
   include_schemas: []
   exclude_schemas:
     - "sys"
     - "INFORMATION_SCHEMA"
   ```

### For Import (Target Database)

1. Copy and edit the import configuration file:
   ```bash
   cp config.import.example.yaml config.yaml
   ```

2. Update `config.yaml` with your **target** Azure SQL Database details:
   ```yaml
   # Target database connection settings
   server: "target-server.database.windows.net"
   database: "target-database-name"
   
   # Authentication type: "sql" or "azure_ad"
   authentication_type: "sql"
   
   # SQL Server authentication
   username: "target-username"
   password: "target-password"
   
   # Import settings
   import_directory: "export_output"  # Directory containing exported files
   import_data: true                  # Whether to import table data
   batch_size: 1000                  # Batch size for data import
   
   # Interactive options
   auto_confirm: false               # Skip interactive confirmations
   truncate_tables: false           # Truncate tables before importing data
   alter_existing: true             # Allow altering existing objects
   ```

## Usage

### Export (Source Database)

#### Basic Export
```bash
python azure_sql_export.py --config config.yaml
```

#### Custom Output Directory
```bash
python azure_sql_export.py --config config.yaml --output /path/to/output
```

### Import (Target Database)

#### Interactive Import
```bash
python azure_sql_import.py --config config.yaml
```

#### Non-Interactive Import
```bash
python azure_sql_import.py --config config.yaml --auto-confirm
```

#### Import with Custom Options
```bash
python azure_sql_import.py --config config.yaml \
  --import-dir /path/to/exported/files \
  --truncate-tables \
  --schema-only
```

### Export Options

#### Schema Only (No Data)
Edit your config file and set `export_data: false`

#### Export Specific Schemas Only
To export only specific schemas (e.g., only 'dbo' and 'custom_schema'):
```yaml
include_schemas:
  - "dbo"
  - "custom_schema"
exclude_schemas: []
```

### Exclude System Schemas
To exclude system schemas (default behavior):
```yaml
include_schemas: []
exclude_schemas:
  - "sys"
  - "INFORMATION_SCHEMA"
     - "guest"
   ```

### Import Options

#### Interactive Mode Features
- **Schema Comparison**: Shows differences between existing and new objects
- **Data Import Choices**: Choose to truncate and import or append data
- **Object-by-Object Confirmation**: Confirm each object before importing
- **Detailed Differences**: See exactly what will change

#### Command Line Options
- `--auto-confirm`: Skip all interactive prompts
- `--truncate-tables`: Truncate tables before importing data
- `--no-alter`: Skip altering existing objects
- `--schema-only`: Import schema only, skip data
- `--import-dir`: Specify custom import directory

## Output Structure

The script creates the following directory structure:

```
export_output/
├── schema/
│   ├── dbo/
│   │   ├── table1_schema.sql
│   │   ├── table2_schema.sql
│   │   ├── view1_view.sql
│   │   ├── procedure1_procedure.sql
│   │   └── function1_function.sql
│   └── schema2/
│       └── ...
├── data/
│   ├── dbo/
│   │   ├── table1_data.sql
│   │   └── table2_data.sql
│   └── schema2/
│       └── ...
└── migration_script.sql
```

## Migration Process

### Complete Migration Workflow

1. **Export from Source Database**:
   ```bash
   # Configure source database connection
   cp config.example.yaml config.yaml
   # Edit config.yaml with source database details
   
   # Export schema and data
   python azure_sql_export.py --config config.yaml
   ```

2. **Import to Target Database**:
   ```bash
   # Configure target database connection
   cp config.import.example.yaml config.yaml
   # Edit config.yaml with target database details
   
   # Interactive import with comparison
   python azure_sql_import.py --config config.yaml
   ```

### Manual Migration (Alternative)

1. **Run the export script** on your source Azure SQL Database
2. **Review the generated files** in the output directory
3. **On your target server**, run the files in this order:
   - Schema files (tables first, then views, functions, procedures, triggers)
   - Data files (INSERT statements)
4. **Use the migration_script.sql** as a reference for the proper order

## Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `server` | Azure SQL Server name | Required |
| `database` | Database name | Required |
| `authentication_type` | "sql" or "azure_ad" | "sql" |
| `username` | SQL Server username (for SQL auth) | Required for SQL auth |
| `password` | SQL Server password (for SQL auth) | Required for SQL auth |
| `driver` | ODBC driver name | "ODBC Driver 17 for SQL Server" |
| `output_directory` | Output directory path | "export_output" |
| `export_data` | Whether to export table data | true |
| `batch_size` | Number of rows per batch | 1000 |
| `include_schemas` | List of schemas to include (empty = all) | [] |
| `exclude_schemas` | List of schemas to exclude | ["sys", "INFORMATION_SCHEMA"] |

### Import Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `import_directory` | Directory containing exported files | "export_output" |
| `import_data` | Whether to import table data | true |
| `auto_confirm` | Skip interactive confirmations | false |
| `truncate_tables` | Truncate tables before importing data | false |
| `alter_existing` | Allow altering existing objects | true |

## Troubleshooting

### Connection Issues
- Ensure your Azure SQL Database allows connections from your IP
- Check firewall rules in Azure portal
- Verify authentication credentials
- For Azure AD auth, ensure you're logged in with `az login`

### ODBC Driver Issues
- Install the latest ODBC Driver for SQL Server
- On Linux/Mac, you may need to install additional dependencies

### Large Database Issues
- Increase `batch_size` for better performance
- Consider exporting data separately if memory is limited
- Monitor disk space for large exports

## Logging

The script creates detailed logs in `azure_sql_export.log` and displays progress in the console.

## Configuration File Support

The tool supports both YAML and JSON configuration files:
- **YAML** (recommended): More readable with comments and better structure
- **JSON**: Traditional format, still supported for compatibility

## Security Notes

- Never commit configuration files with real credentials
- Use environment variables for sensitive data in production
- Consider using Azure Key Vault for credential management
- Ensure proper network security for database connections

## License

This tool is provided as-is for educational and migration purposes.
