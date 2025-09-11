#!/usr/bin/env python3
"""
Azure SQL Database Import Tool

This script imports schema objects and table data from exported files
into an Azure SQL Database with interactive confirmation and comparison.

Features:
- Interactive schema comparison and confirmation
- Data import with truncate/append options
- Object existence checking and ALTER statements
- Safe import with rollback capabilities
- Progress tracking and detailed logging
"""

import os
import sys
import yaml
import json
import logging
import argparse
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Set
import pyodbc
import pandas as pd
from pathlib import Path
from difflib import unified_diff

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('azure_sql_import.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class AzureSQLImporter:
    """Main class for importing Azure SQL Database schema and data."""
    
    def __init__(self, config_file: str = "config.yaml"):
        """Initialize the importer with configuration."""
        self.config = self._load_config(config_file)
        self.connection = None
        self.import_dir = Path(self.config.get('import_directory', 'export_output'))
        self.schema_dir = self.import_dir / 'schema'
        self.data_dir = self.import_dir / 'data'
        
        # Import options
        self.auto_confirm = self.config.get('auto_confirm', False)
        self.truncate_tables = self.config.get('truncate_tables', False)
        self.alter_existing = self.config.get('alter_existing', True)
        
    def _load_config(self, config_file: str) -> Dict:
        """Load configuration from YAML or JSON file."""
        try:
            with open(config_file, 'r') as f:
                if config_file.endswith(('.yaml', '.yml')):
                    return yaml.safe_load(f)
                else:
                    return json.load(f)
        except FileNotFoundError:
            logger.error(f"Configuration file {config_file} not found!")
            sys.exit(1)
        except (yaml.YAMLError, json.JSONDecodeError) as e:
            logger.error(f"Invalid configuration file format: {e}")
            sys.exit(1)
    
    def connect(self) -> bool:
        """Establish connection to Azure SQL Database."""
        try:
            server = self.config['server']
            database = self.config['database']
            
            if self.config.get('authentication_type', 'sql') == 'azure_ad':
                # Azure AD authentication
                connection_string = (
                    f"DRIVER={{{self.config['driver']}}};"
                    f"SERVER={server};"
                    f"DATABASE={database};"
                    f"Authentication=ActiveDirectoryDefault;"
                    f"TrustServerCertificate=yes;"
                )
            else:
                # SQL Server authentication
                username = self.config['username']
                password = self.config['password']
                connection_string = (
                    f"DRIVER={{{self.config['driver']}}};"
                    f"SERVER={server};"
                    f"DATABASE={database};"
                    f"UID={username};"
                    f"PWD={password};"
                    f"TrustServerCertificate=yes;"
                )
            
            self.connection = pyodbc.connect(connection_string)
            logger.info("Successfully connected to Azure SQL Database")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def disconnect(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
    
    def get_existing_objects(self) -> Dict[str, Dict]:
        """Get existing objects in the target database."""
        existing = {
            'tables': {},
            'views': {},
            'procedures': {},
            'functions': {},
            'triggers': {}
        }
        
        try:
            cursor = self.connection.cursor()
            
            # Get existing tables
            cursor.execute("""
                SELECT 
                    TABLE_SCHEMA,
                    TABLE_NAME,
                    TABLE_TYPE
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_SCHEMA, TABLE_NAME
            """)
            for row in cursor.fetchall():
                key = f"{row[0]}.{row[1]}"
                existing['tables'][key] = {'schema': row[0], 'name': row[1], 'type': row[2]}
            
            # Get existing views
            cursor.execute("""
                SELECT 
                    TABLE_SCHEMA,
                    TABLE_NAME
                FROM INFORMATION_SCHEMA.VIEWS
                ORDER BY TABLE_SCHEMA, TABLE_NAME
            """)
            for row in cursor.fetchall():
                key = f"{row[0]}.{row[1]}"
                existing['views'][key] = {'schema': row[0], 'name': row[1]}
            
            # Get existing procedures
            cursor.execute("""
                SELECT 
                    ROUTINE_SCHEMA,
                    ROUTINE_NAME
                FROM INFORMATION_SCHEMA.ROUTINES
                WHERE ROUTINE_TYPE = 'PROCEDURE'
                ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME
            """)
            for row in cursor.fetchall():
                key = f"{row[0]}.{row[1]}"
                existing['procedures'][key] = {'schema': row[0], 'name': row[1]}
            
            # Get existing functions
            cursor.execute("""
                SELECT 
                    ROUTINE_SCHEMA,
                    ROUTINE_NAME
                FROM INFORMATION_SCHEMA.ROUTINES
                WHERE ROUTINE_TYPE = 'FUNCTION'
                ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME
            """)
            for row in cursor.fetchall():
                key = f"{row[0]}.{row[1]}"
                existing['functions'][key] = {'schema': row[0], 'name': row[1]}
            
            # Get existing triggers
            cursor.execute("""
                SELECT 
                    TRIGGER_SCHEMA,
                    TRIGGER_NAME
                FROM INFORMATION_SCHEMA.TRIGGERS
                ORDER BY TRIGGER_SCHEMA, TRIGGER_NAME
            """)
            for row in cursor.fetchall():
                key = f"{row[0]}.{row[1]}"
                existing['triggers'][key] = {'schema': row[0], 'name': row[1]}
            
            cursor.close()
            logger.info(f"Found existing objects: {sum(len(v) for v in existing.values())} total")
            
        except Exception as e:
            logger.error(f"Error retrieving existing objects: {e}")
        
        return existing
    
    def get_table_schema(self, schema_name: str, table_name: str) -> str:
        """Get current table schema definition."""
        try:
            cursor = self.connection.cursor()
            
            # Get column information
            cursor.execute("""
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    CHARACTER_MAXIMUM_LENGTH,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE,
                    IS_NULLABLE,
                    COLUMN_DEFAULT,
                    ORDINAL_POSITION
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
            """, schema_name, table_name)
            
            columns = cursor.fetchall()
            
            # Get primary key information
            cursor.execute("""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? 
                AND CONSTRAINT_NAME IN (
                    SELECT CONSTRAINT_NAME
                    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
                    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? 
                    AND CONSTRAINT_TYPE = 'PRIMARY KEY'
                )
                ORDER BY ORDINAL_POSITION
            """, schema_name, table_name, schema_name, table_name)
            
            pk_columns = [row[0] for row in cursor.fetchall()]
            
            # Build CREATE TABLE statement
            create_sql = f"CREATE TABLE [{schema_name}].[{table_name}] (\n"
            
            column_definitions = []
            for col in columns:
                col_name, data_type, max_length, precision, scale, nullable, default, _ = col
                
                col_def = f"    [{col_name}] {data_type}"
                
                if max_length and data_type in ['varchar', 'nvarchar', 'char', 'nchar']:
                    col_def += f"({max_length})"
                elif precision and data_type in ['decimal', 'numeric']:
                    if scale:
                        col_def += f"({precision},{scale})"
                    else:
                        col_def += f"({precision})"
                
                if nullable == 'NO':
                    col_def += " NOT NULL"
                
                if default:
                    col_def += f" DEFAULT {default}"
                
                column_definitions.append(col_def)
            
            create_sql += ",\n".join(column_definitions)
            
            if pk_columns:
                pk_def = f"    CONSTRAINT [PK_{table_name}] PRIMARY KEY ([{'], ['.join(pk_columns)}])"
                create_sql += f",\n{pk_def}"
            
            create_sql += "\n);\n"
            
            cursor.close()
            return create_sql
            
        except Exception as e:
            logger.error(f"Error getting table schema for {schema_name}.{table_name}: {e}")
            return ""
    
    def get_table_row_count(self, schema_name: str, table_name: str) -> int:
        """Get current row count in table."""
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM [{schema_name}].[{table_name}]")
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Exception as e:
            logger.error(f"Error getting row count for {schema_name}.{table_name}: {e}")
            return 0
    
    def compare_schemas(self, new_schema: str, existing_schema: str, object_name: str) -> List[str]:
        """Compare two schema definitions and return differences."""
        if not existing_schema:
            return [f"New object: {object_name}"]
        
        new_lines = new_schema.strip().split('\n')
        existing_lines = existing_schema.strip().split('\n')
        
        diff = list(unified_diff(
            existing_lines, new_lines,
            fromfile=f"Existing {object_name}",
            tofile=f"New {object_name}",
            lineterm=""
        ))
        
        return diff if diff else ["No differences found"]
    
    def ask_confirmation(self, message: str, default: bool = False) -> bool:
        """Ask for user confirmation."""
        if self.auto_confirm:
            return default
        
        while True:
            response = input(f"{message} [{'Y/n' if default else 'y/N'}]: ").strip().lower()
            if not response:
                return default
            if response in ['y', 'yes']:
                return True
            elif response in ['n', 'no']:
                return False
            else:
                print("Please enter 'y' or 'n'")
    
    def load_exported_files(self) -> Dict[str, List[Dict]]:
        """Load all exported files from the import directory."""
        files = {
            'tables': [],
            'views': [],
            'procedures': [],
            'functions': [],
            'triggers': []
        }
        
        if not self.schema_dir.exists():
            logger.error(f"Schema directory not found: {self.schema_dir}")
            return files
        
        # Define type directories mapping
        type_dirs = {
            'tables': self.schema_dir / 'tables',
            'views': self.schema_dir / 'views',
            'procedures': self.schema_dir / 'procedures',
            'functions': self.schema_dir / 'functions',
            'triggers': self.schema_dir / 'triggers'
        }
        
        # Load files from each type directory
        for obj_type, type_dir in type_dirs.items():
            if type_dir.exists():
                for file_path in type_dir.glob("*.sql"):
                    # Parse filename: schema.object.sql
                    filename = file_path.stem
                    if '.' in filename:
                        schema_name, object_name = filename.split('.', 1)
                        files[obj_type].append({
                            'schema': schema_name,
                            'name': object_name,
                            'file': file_path
                        })
                    else:
                        logger.warning(f"Unexpected filename format: {file_path}")
        
        logger.info(f"Loaded exported files: {sum(len(v) for v in files.values())} total")
        return files
    
    def execute_sql_file(self, file_path: Path, description: str) -> bool:
        """Execute SQL statements from a file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            # Split by semicolon and execute each statement
            statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
            
            cursor = self.connection.cursor()
            for i, statement in enumerate(statements):
                if statement and not statement.startswith('--'):
                    try:
                        cursor.execute(statement)
                        logger.info(f"Executed statement {i+1}/{len(statements)} for {description}")
                    except Exception as e:
                        logger.error(f"Error executing statement {i+1} for {description}: {e}")
                        logger.error(f"Statement: {statement[:100]}...")
                        cursor.rollback()
                        return False
            
            cursor.commit()
            cursor.close()
            logger.info(f"Successfully executed {description}")
            return True
            
        except Exception as e:
            logger.error(f"Error executing {description}: {e}")
            return False
    
    def import_table_data(self, schema_name: str, table_name: str, data_file: Path) -> bool:
        """Import table data with options for truncate/append."""
        try:
            # Check if table has existing data
            existing_count = self.get_table_row_count(schema_name, table_name)
            
            if existing_count > 0:
                if not self.auto_confirm:
                    print(f"\nTable {schema_name}.{table_name} has {existing_count} existing rows.")
                    print("Options:")
                    print("1. Truncate table and import new data")
                    print("2. Append new data to existing data")
                    print("3. Skip this table")
                    
                    while True:
                        choice = input("Choose option [1/2/3]: ").strip()
                        if choice == '1':
                            truncate = True
                            break
                        elif choice == '2':
                            truncate = False
                            break
                        elif choice == '3':
                            logger.info(f"Skipping table {schema_name}.{table_name}")
                            return True
                        else:
                            print("Please enter 1, 2, or 3")
                else:
                    truncate = self.truncate_tables
            else:
                truncate = False
            
            cursor = self.connection.cursor()
            
            # Truncate table if requested
            if truncate:
                cursor.execute(f"TRUNCATE TABLE [{schema_name}].[{table_name}]")
                logger.info(f"Truncated table {schema_name}.{table_name}")
            
            # Read and execute INSERT statements
            with open(data_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Extract INSERT statements
            insert_statements = []
            for line in content.split('\n'):
                line = line.strip()
                if line.startswith('INSERT INTO') and not line.startswith('--'):
                    insert_statements.append(line)
            
            if not insert_statements:
                logger.info(f"No data to import for {schema_name}.{table_name}")
                cursor.close()
                return True
            
            # Execute INSERT statements in batches
            batch_size = self.config.get('batch_size', 1000)
            for i in range(0, len(insert_statements), batch_size):
                batch = insert_statements[i:i + batch_size]
                for statement in batch:
                    try:
                        cursor.execute(statement)
                    except Exception as e:
                        logger.error(f"Error executing INSERT for {schema_name}.{table_name}: {e}")
                        logger.error(f"Statement: {statement[:100]}...")
                        cursor.rollback()
                        return False
                
                cursor.commit()
                logger.info(f"Imported batch {i//batch_size + 1} for {schema_name}.{table_name}")
            
            cursor.close()
            logger.info(f"Successfully imported data for {schema_name}.{table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error importing data for {schema_name}.{table_name}: {e}")
            return False
    
    def import_schema_objects(self, exported_files: Dict[str, List[Dict]], existing_objects: Dict[str, Dict]):
        """Import schema objects with comparison and confirmation."""
        logger.info("Starting schema object import...")
        
        # Import order: tables, views, functions, procedures, triggers
        import_order = ['tables', 'views', 'functions', 'procedures', 'triggers']
        
        for object_type in import_order:
            if not exported_files[object_type]:
                continue
            
            logger.info(f"\n=== Importing {object_type.upper()} ===")
            
            for obj in exported_files[object_type]:
                schema_name = obj['schema']
                object_name = obj['name']
                file_path = obj['file']
                full_name = f"{schema_name}.{object_name}"
                
                # Check if object exists
                object_key = f"{schema_name}.{object_name}"
                exists = object_key in existing_objects[object_type]
                
                if exists and self.alter_existing:
                    # Compare schemas
                    with open(file_path, 'r', encoding='utf-8') as f:
                        new_schema = f.read()
                    
                    if object_type == 'tables':
                        existing_schema = self.get_table_schema(schema_name, object_name)
                    else:
                        existing_schema = ""  # For other objects, we'll just show the file content
                    
                    differences = self.compare_schemas(new_schema, existing_schema, full_name)
                    
                    print(f"\n--- {object_type.upper()}: {full_name} ---")
                    print("Differences found:")
                    for diff_line in differences[:10]:  # Show first 10 lines
                        print(diff_line)
                    if len(differences) > 10:
                        print(f"... and {len(differences) - 10} more lines")
                    
                    if not self.ask_confirmation(f"Import {object_type[:-1]} {full_name}?"):
                        logger.info(f"Skipped {object_type[:-1]} {full_name}")
                        continue
                
                elif exists and not self.alter_existing:
                    logger.info(f"Skipping existing {object_type[:-1]} {full_name}")
                    continue
                
                # Execute the import
                description = f"{object_type[:-1]} {full_name}"
                if self.execute_sql_file(file_path, description):
                    logger.info(f"Successfully imported {description}")
                else:
                    logger.error(f"Failed to import {description}")
    
    def import_table_data_all(self, exported_files: Dict[str, List[Dict]]):
        """Import data for all tables."""
        if not self.data_dir.exists():
            logger.info("No data directory found, skipping data import")
            return
        
        logger.info("\n=== Importing Table Data ===")
        
        for table_obj in exported_files['tables']:
            schema_name = table_obj['schema']
            table_name = table_obj['name']
            data_file = self.data_dir / f"{schema_name}.{table_name}.sql"
            
            if data_file.exists():
                if not self.ask_confirmation(f"Import data for table {schema_name}.{table_name}?"):
                    logger.info(f"Skipped data import for {schema_name}.{table_name}")
                    continue
                
                self.import_table_data(schema_name, table_name, data_file)
            else:
                logger.info(f"No data file found for {schema_name}.{table_name}")
    
    def run_import(self):
        """Run the complete import process."""
        logger.info("Starting Azure SQL Database import...")
        
        if not self.connect():
            return False
        
        try:
            # Load exported files
            exported_files = self.load_exported_files()
            if not any(exported_files.values()):
                logger.error("No exported files found to import")
                return False
            
            # Get existing objects
            existing_objects = self.get_existing_objects()
            
            # Show import summary
            print("\n=== Import Summary ===")
            for obj_type, objects in exported_files.items():
                if objects:
                    print(f"{obj_type.capitalize()}: {len(objects)} objects")
            
            if not self.ask_confirmation("Proceed with import?"):
                logger.info("Import cancelled by user")
                return False
            
            # Import schema objects
            self.import_schema_objects(exported_files, existing_objects)
            
            # Import table data
            if self.config.get('import_data', True):
                self.import_table_data_all(exported_files)
            
            logger.info("Import completed successfully!")
            return True
            
        except Exception as e:
            logger.error(f"Import failed: {e}")
            return False
        finally:
            self.disconnect()


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Import Azure SQL Database schema and data')
    parser.add_argument('--config', default='config.yaml', help='Configuration file path (YAML or JSON)')
    parser.add_argument('--import-dir', help='Import directory path (overrides config)')
    parser.add_argument('--auto-confirm', action='store_true', help='Skip interactive confirmations')
    parser.add_argument('--truncate-tables', action='store_true', help='Truncate tables before importing data')
    parser.add_argument('--no-alter', action='store_true', help='Skip altering existing objects')
    parser.add_argument('--schema-only', action='store_true', help='Import schema only, skip data')
    
    args = parser.parse_args()
    
    try:
        importer = AzureSQLImporter(args.config)
        
        if args.import_dir:
            importer.import_dir = Path(args.import_dir)
            importer.schema_dir = importer.import_dir / 'schema'
            importer.data_dir = importer.import_dir / 'data'
        
        # Override config with command line arguments
        importer.auto_confirm = args.auto_confirm
        importer.truncate_tables = args.truncate_tables
        importer.alter_existing = not args.no_alter
        
        if args.schema_only:
            importer.config['import_data'] = False
        
        success = importer.run_import()
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        logger.info("Import cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
