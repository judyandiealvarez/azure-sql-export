# Azure SQL Database Export, Import & Comparison Tools

Python scripts to export, import, and compare Azure SQL Database schema objects and table data for migration between servers.

## Features

### Export Tool (`azure_sql_export.py`)
- **Schema Export**: Exports tables, views, stored procedures, functions, and triggers
- **Data Export**: Exports table data as SQL INSERT statements or binary format
- **Binary Data Export**: High-performance compressed binary export for large datasets
- **Organized Output**: Creates structured directories for easy migration
- **Authentication Support**: Supports both SQL Server and Azure AD authentication
- **Batch Processing**: Handles large datasets efficiently
- **Migration Script**: Generates a master migration script with proper order

### Import Tool (`azure_sql_import.py`)
- **Interactive Import**: Compare and confirm imports with detailed differences
- **Schema Comparison**: Shows differences between existing and new objects
- **Data Import Options**: Truncate and import or append to existing tables
- **Binary Data Import**: High-performance binary data import with compression
- **Safe Import**: Rollback capabilities and detailed logging
- **Object Management**: ALTER existing objects or skip them
- **Progress Tracking**: Real-time progress updates and batch processing
- **Dependency Analysis**: Automatically analyzes and resolves object dependencies
- **Smart Import Order**: Uses topological sorting to import objects in correct order

### Comparison Tool (`azure_sql_compare.py`)
- **Schema Comparison**: Compare exported schema with target database
- **Data Comparison**: Compare row counts and sample data (SQL or binary)
- **Binary Data Analysis**: Analyze compressed binary data files
- **Detailed Differences**: Show exact differences between objects
- **Change Summary**: Categorize changes (new, modified, deleted, unchanged)
- **Export Reports**: Generate detailed comparison reports
- **Safe Analysis**: Read-only comparison without making changes

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
   data_format: "sql"  # "sql" or "binary"
   
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
   data_format: "sql"  # "sql" or "binary"
   
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

### For Comparison (Target Database)

1. Copy and edit the comparison configuration file:
   ```bash
   cp config.compare.example.yaml config.yaml
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

   ```yaml
   # Target database connection settings
   server: "target-server.database.windows.net"
   database: "target-database-name"
   
   # Authentication type: "sql" or "azure_ad"
   authentication_type: "sql"
   
   # SQL Server authentication
   username: "target-username"
   password: "target-password"
   
   # Comparison settings
   import_directory: "export_output"  # Directory containing exported files
   show_data_samples: true           # Show sample data in comparison
   sample_size: 5                   # Number of sample rows to show
   export_report: true              # Export detailed report to file
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

#### Analyze Dependencies
```bash
python azure_sql_import.py --config config.yaml --show-dependencies
```

### Comparison (Target Database)

#### Basic Comparison
```bash
python azure_sql_compare.py --config config.yaml
```

#### Comparison with Custom Options
```bash
python azure_sql_compare.py --config config.yaml \
  --import-dir /path/to/exported/files \
  --no-samples \
  --sample-size 10
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

#### Binary Data Export
For large datasets, use binary format for better performance and compression:
```yaml
data_format: "binary"      # Much faster and smaller files
batch_size: 10000          # Larger batches for binary export
reporting_interval: 1000   # Report progress every N batches (default: 1000)
```

**Binary Format Benefits:**
- **Faster Export/Import**: 5-10x faster than SQL format
- **Better Compression**: 60-80% smaller file sizes
- **Preserves Data Types**: No string conversion issues
- **Large Dataset Support**: Handles millions of rows efficiently
- **Optimized Logging**: Configurable reporting to avoid performance impact

#### Performance Optimization
For maximum performance with large datasets:
```yaml
# High-performance settings
data_format: "binary"        # Use binary format
batch_size: 10000           # Larger batches
reporting_interval: 10000   # Report every 10,000 batches (less frequent logging)
```

**Performance Tips:**
- **Binary Format**: Always use binary for large datasets (>100K rows)
- **Larger Batches**: Increase `batch_size` for better throughput
- **Less Frequent Logging**: Increase `reporting_interval` to reduce I/O overhead
- **System Resources**: More RAM allows larger batch sizes

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
- `--show-dependencies`: Analyze and display object dependencies

#### Dependency Analysis
The import tool automatically analyzes dependencies between database objects:
- **Views** that depend on tables
- **Functions** that reference other objects
- **Stored Procedures** that call functions or reference tables
- **Triggers** that depend on tables
- **Cross-schema dependencies**

Objects are imported in the correct order to avoid dependency errors.

#### Comparison Options
The comparison tool provides detailed analysis without making changes:
- **Schema Comparison**: Shows differences between exported and database objects
- **Data Comparison**: Compares row counts and shows sample data
- **Change Categories**: New, modified, deleted, and unchanged objects
- **Detailed Reports**: Export comprehensive comparison reports
- **Safe Analysis**: Read-only operations that don't modify the database

## Output Structure

The script creates the following directory structure organized by object type:

```
export_output/
├── schema/
│   ├── tables/
│   │   ├── dbo.table1.sql
│   │   ├── dbo.table2.sql
│   │   └── schema2.table3.sql
│   ├── views/
│   │   ├── dbo.view1.sql
│   │   └── schema2.view2.sql
│   ├── procedures/
│   │   ├── dbo.procedure1.sql
│   │   └── schema2.procedure2.sql
│   ├── functions/
│   │   ├── dbo.function1.sql
│   │   └── schema2.function2.sql
│   └── triggers/
│       ├── dbo.trigger1.sql
│       └── schema2.trigger2.sql
├── data/
│   ├── dbo.table1.sql
│   ├── dbo.table2.sql
│   └── schema2.table3.sql
├── binary_data/          # When using binary format
│   ├── dbo.table1.pkl.gz
│   ├── dbo.table2.pkl.gz
│   └── schema2.table3.pkl.gz
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

2. **Compare with Target Database** (Optional but recommended):
   ```bash
   # Configure target database connection
   cp config.compare.example.yaml config.yaml
   # Edit config.yaml with target database details
   
   # Compare exported files with target database
   python azure_sql_compare.py --config config.yaml
   ```

3. **Import to Target Database**:
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
   - Schema files from `schema/` directory (tables first, then views, functions, procedures, triggers)
   - Data files from `data/` directory (INSERT statements)
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
| `data_format` | Data export format: "sql" or "binary" | "sql" |
| `reporting_interval` | Report progress every N batches | 1000 |
| `include_schemas` | List of schemas to include (empty = all) | [] |
| `exclude_schemas` | List of schemas to exclude | ["sys", "INFORMATION_SCHEMA"] |

### Import Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `import_directory` | Directory containing exported files | "export_output" |
| `import_data` | Whether to import table data | true |
| `data_format` | Data import format: "sql" or "binary" | "sql" |
| `reporting_interval` | Report progress every N batches | 1000 |
| `auto_confirm` | Skip interactive confirmations | false |
| `truncate_tables` | Truncate tables before importing data | false |
| `alter_existing` | Allow altering existing objects | true |

### Comparison Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `import_directory` | Directory containing exported files | "export_output" |
| `show_data_samples` | Show sample data in comparison | true |
| `sample_size` | Number of sample rows to show | 5 |
| `export_report` | Export detailed report to file | true |
| `data_format` | Data format to compare: "sql" or "binary" | "sql" |

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
