# azs (pyazs) – Azure SQL unified CLI

Unified command-line for exporting, importing, comparing, copying data, web UI, schema sync, and migration script generation for Azure SQL.

## Install

- Local dev (recommended):
```bash
pip install -r requirements.txt
pip install -e .
```
This installs the `azs` command on PATH.

- Run without install (repo root):
```bash
./azs <command> [args]
```

## Quick start

```bash
cp config.example.yaml config.yaml
# edit config.yaml (server, database, auth, etc.)

# export schema/data
azs export -c config.yaml

# compare against DB
azs compare -c config.yaml --import-dir export_output

# import into DB
azs import -c config.yaml --import-dir export_output --auto-confirm

# copy data between two DBs (uses source/target in config)
azs copy -c config.yaml --tables dbo.Table1,dbo.Table2

# run web UI
azs web

# sync DB schema objects -> local sql files
azs sync -c config.yaml --schema-name dbo

# generate migration script from DB vs local files
azs migrate -c config.yaml --schema-name dbo
```

## Commands

- export: Export schema and optionally data to `output_directory`.
- import: Import schema/data from `import_directory` with safe ALTER logic.
- compare: Compare exported files with a live DB; produce reports.
- copy: Copy table data using `source` → `target` profiles.
- web: Flask web UI for export/import/compare workflows.
- sync: Materialize DB objects to local `.sql` files per object type.
- migrate: Create a migration script to reconcile local files to DB state.

Run `azs <command> -h` for command-specific flags.

## Unified config (config.yaml)

Copy and edit:
```bash
cp config.example.yaml config.yaml
```
Key sections (YAML or JSON supported):

- Connection (shared by most commands)
  - server, database, authentication_type (sql|azure_ad), driver
  - username/password (only for authentication_type: sql)

- export
  - output_directory, export_data, batch_size, data_format, reporting_interval
  - include_schemas, exclude_schemas

- import
  - import_directory, confirm_each_object, truncate_before_load

- compare
  - show_data_samples, sample_size, export_report

- copy
  - source: connection profile
  - target: connection profile
  - tables, identity_insert, dry_run

- web
  - upload_folder, export_folder, max_content_length_mb

- sync
  - schema_name, sql_schema_dir

- migrate
  - schema_name, sql_schema_dir, migrations_dir

See `config.example.yaml` for a complete, commented example.

## Requirements

- Python 3.8+
- ODBC Driver 17 for SQL Server (or compatible)

Install driver (macOS):
```bash
brew install --cask msodbcsql17 mssql-tools
```

## Notes

- Auth:
  - sql: uses username/password from config
  - azure_ad: uses `ActiveDirectoryDefault` (interactive/managed identity as supported by the environment)
- Large datasets: Prefer `data_format: binary` and increase `batch_size` for performance.
- Logs: individual commands write `azure_sql_*.log` in repo root.

## License

MIT

## Detailed features

### Export (azs export)
- Schema export: tables, views, procedures, functions, triggers
- Data export: INSERT statements or binary format (fast, compressed)
- Structured output directories per object type
- Auth: SQL auth and Azure AD
- Progress logging with configurable intervals

### Import (azs import)
- Interactive or non-interactive imports
- Smart diff against target, auto-skip identical objects
- Safe ALTER-first logic for existing objects; preserves data
- Dependency analysis and topological ordering
- Supports importing data with truncate/append options

### Compare (azs compare)
- Compare exported files vs live DB
- Change categories: new, modified, deleted, unchanged
- Optional sample data comparison
- Exportable reports for auditing

### Copy (azs copy)
- Copy table data source → target using profiles in config
- Batch copy with retries and identity insert handling
- Dry-run mode

### Web (azs web)
- Bootstrap UI for export/import/compare
- Background operations with progress and logs
- Upload/download support

### Sync (azs sync)
- Materialize DB objects to local `.sql` files per type
- Creates/updates/cleans files to match live DB for a schema

### Migrate (azs migrate)
- Generate migration script that reconciles local files to live DB state
- Uses schema-specific comparisons and produces numbered `updateXXXX.sql`

## Command usage examples

### export
```bash
azs export -c config.yaml \
  --output-directory ./export_output \
  --data-format sql \
  --batch-size 1000
```

### import
```bash
azs import -c config.yaml \
  --import-dir ./export_output \
  --auto-confirm \
  --truncate-tables
```

### compare
```bash
azs compare -c config.yaml \
  --import-dir ./export_output \
  --sample-size 5
```

### copy
```bash
azs copy -c config.yaml \
  --tables dbo.Table1,dbo.Table2 \
  --identity-insert auto \
  --dry-run
```

### web
```bash
azs web
# open http://localhost:5000
```

### sync
```bash
azs sync -c config.yaml \
  --schema-name dbo \
  --sql-schema-dir sql/schema
```

### migrate
```bash
azs migrate -c config.yaml \
  --schema-name dbo \
  --sql-schema-dir sql/schema \
  --migrations-dir sql/migrations
```

## Options reference

### Shared connection keys (config)
- server: Azure SQL server hostname
- database: database name
- authentication_type: sql | azure_ad
- driver: ODBC driver name (e.g., ODBC Driver 17 for SQL Server)
- username/password: required for sql auth

### export options
- output_directory: path for results
- export_data: true|false
- batch_size: int
- data_format: sql|binary
- reporting_interval: int
- include_schemas: [schema,...]
- exclude_schemas: [schema,...]

### import options
- import_directory: path from export
- confirm_each_object: true|false
- truncate_before_load: true|false

### compare options
- import_directory: path from export
- show_data_samples: true|false
- sample_size: int
- export_report: true|false

### copy options
- source: connection profile
- target: connection profile
- tables: [qualified names] or comma-separated via CLI
- identity_insert: auto|on|off
- dry_run: true|false

### web options
- upload_folder: path
- export_folder: path
- max_content_length_mb: int

### sync options
- schema_name: schema to fetch (e.g., dbo)
- sql_schema_dir: local folder for object files

### migrate options
- schema_name: schema to compare
- sql_schema_dir: local folder for existing object files
- migrations_dir: output folder for generated scripts

## Performance tips
- Prefer data_format: binary for large datasets
- Increase batch_size and reduce reporting frequency for throughput
- Ensure ODBC driver is up-to-date
- Run from a host near the database to lower latency

## Troubleshooting
- Authentication
  - sql: verify username/password and firewall rules
  - azure_ad: ensure environment supports `ActiveDirectoryDefault` (e.g., `az login`)
- ODBC driver
  - macOS: install msodbcsql17 and mssql-tools via Homebrew
  - Linux: follow Microsoft docs for your distro
- Permissions
  - Ensure account has rights to read schema, create/alter objects when importing
- File paths
  - Use absolute paths for export/import directories if running outside repo root
