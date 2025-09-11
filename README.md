# Azure SQL Database Export Tool

A Python script to export Azure SQL Database schema objects and table data for migration to another server.

## Features

- **Schema Export**: Exports tables, views, stored procedures, functions, and triggers
- **Data Export**: Exports table data as SQL INSERT statements
- **Organized Output**: Creates structured directories for easy migration
- **Authentication Support**: Supports both SQL Server and Azure AD authentication
- **Batch Processing**: Handles large datasets efficiently
- **Migration Script**: Generates a master migration script with proper order

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

1. Copy and edit the configuration file:
   ```bash
   cp config.json my_config.json
   ```

2. Update `my_config.json` with your Azure SQL Database details:

   **For SQL Server Authentication:**
   ```json
   {
     "server": "your-server.database.windows.net",
     "database": "your-database-name",
     "authentication_type": "sql",
     "username": "your-username",
     "password": "your-password",
     "driver": "ODBC Driver 17 for SQL Server",
     "output_directory": "export_output",
     "export_data": true,
     "batch_size": 1000
   }
   ```

   **For Azure AD Authentication:**
   ```json
   {
     "server": "your-server.database.windows.net",
     "database": "your-database-name",
     "authentication_type": "azure_ad",
     "driver": "ODBC Driver 17 for SQL Server",
     "output_directory": "export_output",
     "export_data": true,
     "batch_size": 1000
   }
   ```

## Usage

### Basic Usage
```bash
python azure_sql_export.py --config my_config.json
```

### Custom Output Directory
```bash
python azure_sql_export.py --config my_config.json --output /path/to/output
```

### Schema Only (No Data)
Edit your config file and set `"export_data": false`

### Export Specific Schemas Only
To export only specific schemas (e.g., only 'dbo' and 'custom_schema'):
```json
{
  "include_schemas": ["dbo", "custom_schema"],
  "exclude_schemas": []
}
```

### Exclude System Schemas
To exclude system schemas (default behavior):
```json
{
  "include_schemas": [],
  "exclude_schemas": ["sys", "INFORMATION_SCHEMA", "guest"]
}
```

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

## Security Notes

- Never commit configuration files with real credentials
- Use environment variables for sensitive data in production
- Consider using Azure Key Vault for credential management
- Ensure proper network security for database connections

## License

This tool is provided as-is for educational and migration purposes.
