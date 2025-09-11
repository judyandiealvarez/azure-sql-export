#!/usr/bin/env python3
"""
Azure SQL Database Schema and Data Export Tool

This script exports Azure SQL Database schema objects and table data
for migration to another server.

Features:
- Exports schema objects (tables, views, stored procedures, functions, triggers)
- Exports table data as SQL INSERT statements
- Organizes output in structured directories
- Supports both SQL authentication and Azure AD authentication
- Handles large datasets with batch processing
"""

import os
import sys
import json
import logging
import argparse
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import pyodbc
import pandas as pd
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('azure_sql_export.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class AzureSQLExporter:
    """Main class for exporting Azure SQL Database schema and data."""
    
    def __init__(self, config_file: str = "config.json"):
        """Initialize the exporter with configuration."""
        self.config = self._load_config(config_file)
        self.connection = None
        self.output_dir = Path(self.config.get('output_directory', 'export_output'))
        self.output_dir.mkdir(exist_ok=True)
        
        # Create subdirectories
        self.schema_dir = self.output_dir / 'schema'
        self.data_dir = self.output_dir / 'data'
        self.schema_dir.mkdir(exist_ok=True)
        self.data_dir.mkdir(exist_ok=True)
        
    def _load_config(self, config_file: str) -> Dict:
        """Load configuration from JSON file."""
        try:
            with open(config_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error(f"Configuration file {config_file} not found!")
            sys.exit(1)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in configuration file: {e}")
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
    
    def get_schema_objects(self) -> Dict[str, List[Dict]]:
        """Retrieve all schema objects from the database."""
        objects = {
            'tables': [],
            'views': [],
            'stored_procedures': [],
            'functions': [],
            'triggers': [],
            'indexes': []
        }
        
        # Get schema filter configuration
        include_schemas = self.config.get('include_schemas', [])
        exclude_schemas = self.config.get('exclude_schemas', ['sys', 'INFORMATION_SCHEMA'])
        
        # Build schema filter condition
        schema_filter = ""
        if include_schemas:
            placeholders = ",".join(["?" for _ in include_schemas])
            schema_filter = f"AND TABLE_SCHEMA IN ({placeholders})"
        elif exclude_schemas:
            placeholders = ",".join(["?" for _ in exclude_schemas])
            schema_filter = f"AND TABLE_SCHEMA NOT IN ({placeholders})"
        
        try:
            cursor = self.connection.cursor()
            
            # Get tables
            query = f"""
                SELECT 
                    TABLE_SCHEMA,
                    TABLE_NAME,
                    TABLE_TYPE
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
                {schema_filter}
                ORDER BY TABLE_SCHEMA, TABLE_NAME
            """
            
            if include_schemas:
                cursor.execute(query, include_schemas)
            elif exclude_schemas:
                cursor.execute(query, exclude_schemas)
            else:
                cursor.execute(query)
                
            objects['tables'] = [{'schema': row[0], 'name': row[1], 'type': row[2]} 
                               for row in cursor.fetchall()]
            
            # Get views
            query = f"""
                SELECT 
                    TABLE_SCHEMA,
                    TABLE_NAME
                FROM INFORMATION_SCHEMA.VIEWS
                WHERE 1=1 {schema_filter.replace('TABLE_SCHEMA', 'TABLE_SCHEMA')}
                ORDER BY TABLE_SCHEMA, TABLE_NAME
            """
            
            if include_schemas:
                cursor.execute(query, include_schemas)
            elif exclude_schemas:
                cursor.execute(query, exclude_schemas)
            else:
                cursor.execute(query)
            objects['views'] = [{'schema': row[0], 'name': row[1]} 
                              for row in cursor.fetchall()]
            
            # Get stored procedures
            query = f"""
                SELECT 
                    ROUTINE_SCHEMA,
                    ROUTINE_NAME,
                    ROUTINE_DEFINITION
                FROM INFORMATION_SCHEMA.ROUTINES
                WHERE ROUTINE_TYPE = 'PROCEDURE'
                {schema_filter.replace('TABLE_SCHEMA', 'ROUTINE_SCHEMA')}
                ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME
            """
            
            if include_schemas:
                cursor.execute(query, include_schemas)
            elif exclude_schemas:
                cursor.execute(query, exclude_schemas)
            else:
                cursor.execute(query)
            objects['stored_procedures'] = [{'schema': row[0], 'name': row[1], 'definition': row[2]} 
                                          for row in cursor.fetchall()]
            
            # Get functions
            query = f"""
                SELECT 
                    ROUTINE_SCHEMA,
                    ROUTINE_NAME,
                    ROUTINE_DEFINITION
                FROM INFORMATION_SCHEMA.ROUTINES
                WHERE ROUTINE_TYPE = 'FUNCTION'
                {schema_filter.replace('TABLE_SCHEMA', 'ROUTINE_SCHEMA')}
                ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME
            """
            
            if include_schemas:
                cursor.execute(query, include_schemas)
            elif exclude_schemas:
                cursor.execute(query, exclude_schemas)
            else:
                cursor.execute(query)
            objects['functions'] = [{'schema': row[0], 'name': row[1], 'definition': row[2]} 
                                  for row in cursor.fetchall()]
            
            # Get triggers
            query = f"""
                SELECT 
                    TRIGGER_SCHEMA,
                    TRIGGER_NAME,
                    EVENT_MANIPULATION,
                    EVENT_OBJECT_TABLE,
                    ACTION_STATEMENT
                FROM INFORMATION_SCHEMA.TRIGGERS
                WHERE 1=1 {schema_filter.replace('TABLE_SCHEMA', 'TRIGGER_SCHEMA')}
                ORDER BY TRIGGER_SCHEMA, TRIGGER_NAME
            """
            
            if include_schemas:
                cursor.execute(query, include_schemas)
            elif exclude_schemas:
                cursor.execute(query, exclude_schemas)
            else:
                cursor.execute(query)
            objects['triggers'] = [{'schema': row[0], 'name': row[1], 'event': row[2], 
                                  'table': row[3], 'statement': row[4]} 
                                 for row in cursor.fetchall()]
            
            cursor.close()
            logger.info(f"Retrieved schema objects: {sum(len(v) for v in objects.values())} total")
            
        except Exception as e:
            logger.error(f"Error retrieving schema objects: {e}")
        
        return objects
    
    def export_table_schema(self, table_info: Dict) -> str:
        """Export table schema (CREATE TABLE statement)."""
        try:
            cursor = self.connection.cursor()
            schema_name = table_info['schema']
            table_name = table_info['name']
            
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
                
                # Build column definition
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
            
            # Add primary key constraint
            if pk_columns:
                pk_def = f"    CONSTRAINT [PK_{table_name}] PRIMARY KEY ([{'], ['.join(pk_columns)}])"
                create_sql += f",\n{pk_def}"
            
            create_sql += "\n);\n"
            
            cursor.close()
            return create_sql
            
        except Exception as e:
            logger.error(f"Error exporting table schema for {table_info['schema']}.{table_info['name']}: {e}")
            return f"-- Error exporting table schema: {e}\n"
    
    def export_table_data(self, table_info: Dict) -> str:
        """Export table data as INSERT statements."""
        try:
            schema_name = table_info['schema']
            table_name = table_info['name']
            full_table_name = f"[{schema_name}].[{table_name}]"
            
            # Get column information
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
            """, schema_name, table_name)
            
            columns = cursor.fetchall()
            column_names = [col[0] for col in columns]
            
            # Check if table has data
            cursor.execute(f"SELECT COUNT(*) FROM {full_table_name}")
            row_count = cursor.fetchone()[0]
            
            if row_count == 0:
                cursor.close()
                return f"-- No data in table {full_table_name}\n"
            
            logger.info(f"Exporting {row_count} rows from {full_table_name}")
            
            # Export data in batches
            batch_size = self.config.get('batch_size', 1000)
            insert_statements = []
            
            for offset in range(0, row_count, batch_size):
                cursor.execute(f"""
                    SELECT * FROM {full_table_name}
                    ORDER BY (SELECT NULL)
                    OFFSET {offset} ROWS
                    FETCH NEXT {batch_size} ROWS ONLY
                """)
                
                rows = cursor.fetchall()
                
                for row in rows:
                    values = []
                    for i, value in enumerate(row):
                        if value is None:
                            values.append('NULL')
                        elif columns[i][1] in ['varchar', 'nvarchar', 'char', 'nchar', 'text', 'ntext']:
                            # Escape single quotes in string values
                            escaped_value = str(value).replace("'", "''")
                            values.append(f"'{escaped_value}'")
                        elif columns[i][1] in ['datetime', 'datetime2', 'date', 'time']:
                            values.append(f"'{value}'")
                        else:
                            values.append(str(value))
                    
                    insert_sql = f"INSERT INTO {full_table_name} ([{'], ['.join(column_names)}]) VALUES ({', '.join(values)});"
                    insert_statements.append(insert_sql)
            
            cursor.close()
            return "\n".join(insert_statements) + "\n"
            
        except Exception as e:
            logger.error(f"Error exporting table data for {table_info['schema']}.{table_info['name']}: {e}")
            return f"-- Error exporting table data: {e}\n"
    
    def export_schema_objects(self, objects: Dict[str, List[Dict]]):
        """Export all schema objects to SQL files."""
        logger.info("Exporting schema objects...")
        
        # Export tables
        for table in objects['tables']:
            schema_name = table['schema']
            table_name = table['name']
            
            # Create schema directory if it doesn't exist
            schema_path = self.schema_dir / schema_name
            schema_path.mkdir(exist_ok=True)
            
            # Export table schema
            table_schema = self.export_table_schema(table)
            schema_file = schema_path / f"{table_name}_schema.sql"
            
            with open(schema_file, 'w', encoding='utf-8') as f:
                f.write(f"-- Table schema for {schema_name}.{table_name}\n")
                f.write(f"-- Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                f.write(table_schema)
            
            logger.info(f"Exported table schema: {schema_file}")
        
        # Export views
        for view in objects['views']:
            schema_name = view['schema']
            view_name = view['name']
            
            schema_path = self.schema_dir / schema_name
            schema_path.mkdir(exist_ok=True)
            
            try:
                cursor = self.connection.cursor()
                cursor.execute(f"""
                    SELECT VIEW_DEFINITION
                    FROM INFORMATION_SCHEMA.VIEWS
                    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                """, schema_name, view_name)
                
                view_definition = cursor.fetchone()[0]
                cursor.close()
                
                view_file = schema_path / f"{view_name}_view.sql"
                with open(view_file, 'w', encoding='utf-8') as f:
                    f.write(f"-- View definition for {schema_name}.{view_name}\n")
                    f.write(f"-- Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                    f.write(f"CREATE VIEW [{schema_name}].[{view_name}] AS\n{view_definition}\n")
                
                logger.info(f"Exported view: {view_file}")
                
            except Exception as e:
                logger.error(f"Error exporting view {schema_name}.{view_name}: {e}")
        
        # Export stored procedures
        for proc in objects['stored_procedures']:
            schema_name = proc['schema']
            proc_name = proc['name']
            
            schema_path = self.schema_dir / schema_name
            schema_path.mkdir(exist_ok=True)
            
            proc_file = schema_path / f"{proc_name}_procedure.sql"
            with open(proc_file, 'w', encoding='utf-8') as f:
                f.write(f"-- Stored procedure: {schema_name}.{proc_name}\n")
                f.write(f"-- Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                f.write(proc['definition'] + "\n")
            
            logger.info(f"Exported stored procedure: {proc_file}")
        
        # Export functions
        for func in objects['functions']:
            schema_name = func['schema']
            func_name = func['name']
            
            schema_path = self.schema_dir / schema_name
            schema_path.mkdir(exist_ok=True)
            
            func_file = schema_path / f"{func_name}_function.sql"
            with open(func_file, 'w', encoding='utf-8') as f:
                f.write(f"-- Function: {schema_name}.{func_name}\n")
                f.write(f"-- Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                f.write(func['definition'] + "\n")
            
            logger.info(f"Exported function: {func_file}")
        
        # Export triggers
        for trigger in objects['triggers']:
            schema_name = trigger['schema']
            trigger_name = trigger['name']
            
            schema_path = self.schema_dir / schema_name
            schema_path.mkdir(exist_ok=True)
            
            trigger_file = schema_path / f"{trigger_name}_trigger.sql"
            with open(trigger_file, 'w', encoding='utf-8') as f:
                f.write(f"-- Trigger: {schema_name}.{trigger_name}\n")
                f.write(f"-- Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                f.write(f"CREATE TRIGGER [{schema_name}].[{trigger_name}]\n")
                f.write(f"ON [{schema_name}].[{trigger['table']}]\n")
                f.write(f"FOR {trigger['event']}\n")
                f.write(f"AS\n{trigger['statement']}\n")
            
            logger.info(f"Exported trigger: {trigger_file}")
    
    def export_table_data_all(self, objects: Dict[str, List[Dict]]):
        """Export data for all tables."""
        logger.info("Exporting table data...")
        
        for table in objects['tables']:
            schema_name = table['schema']
            table_name = table['name']
            
            # Create schema directory if it doesn't exist
            schema_path = self.data_dir / schema_name
            schema_path.mkdir(exist_ok=True)
            
            # Export table data
            table_data = self.export_table_data(table)
            data_file = schema_path / f"{table_name}_data.sql"
            
            with open(data_file, 'w', encoding='utf-8') as f:
                f.write(f"-- Table data for {schema_name}.{table_name}\n")
                f.write(f"-- Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                f.write(table_data)
            
            logger.info(f"Exported table data: {data_file}")
    
    def create_migration_script(self, objects: Dict[str, List[Dict]]):
        """Create a master migration script."""
        migration_file = self.output_dir / 'migration_script.sql'
        
        with open(migration_file, 'w', encoding='utf-8') as f:
            f.write(f"-- Azure SQL Database Migration Script\n")
            f.write(f"-- Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"-- Source: {self.config['server']}/{self.config['database']}\n\n")
            
            f.write("-- =============================================\n")
            f.write("-- SCHEMA CREATION ORDER\n")
            f.write("-- =============================================\n\n")
            
            # Tables first
            f.write("-- 1. Create Tables\n")
            for table in objects['tables']:
                f.write(f"-- {table['schema']}.{table['name']}\n")
            
            f.write("\n-- 2. Create Views\n")
            for view in objects['views']:
                f.write(f"-- {view['schema']}.{view['name']}\n")
            
            f.write("\n-- 3. Create Functions\n")
            for func in objects['functions']:
                f.write(f"-- {func['schema']}.{func['name']}\n")
            
            f.write("\n-- 4. Create Stored Procedures\n")
            for proc in objects['stored_procedures']:
                f.write(f"-- {proc['schema']}.{proc['name']}\n")
            
            f.write("\n-- 5. Create Triggers\n")
            for trigger in objects['triggers']:
                f.write(f"-- {trigger['schema']}.{trigger['name']}\n")
            
            f.write("\n-- =============================================\n")
            f.write("-- DATA LOADING ORDER\n")
            f.write("-- =============================================\n\n")
            
            for table in objects['tables']:
                f.write(f"-- Load data into {table['schema']}.{table['name']}\n")
        
        logger.info(f"Created migration script: {migration_file}")
    
    def run_export(self):
        """Run the complete export process."""
        logger.info("Starting Azure SQL Database export...")
        
        if not self.connect():
            return False
        
        try:
            # Get all schema objects
            objects = self.get_schema_objects()
            
            # Export schema objects
            self.export_schema_objects(objects)
            
            # Export table data
            if self.config.get('export_data', True):
                self.export_table_data_all(objects)
            
            # Create migration script
            self.create_migration_script(objects)
            
            logger.info("Export completed successfully!")
            logger.info(f"Output directory: {self.output_dir.absolute()}")
            
            return True
            
        except Exception as e:
            logger.error(f"Export failed: {e}")
            return False
        finally:
            self.disconnect()


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Export Azure SQL Database schema and data')
    parser.add_argument('--config', default='config.json', help='Configuration file path')
    parser.add_argument('--output', help='Output directory (overrides config)')
    
    args = parser.parse_args()
    
    try:
        exporter = AzureSQLExporter(args.config)
        
        if args.output:
            exporter.output_dir = Path(args.output)
            exporter.output_dir.mkdir(exist_ok=True)
            exporter.schema_dir = exporter.output_dir / 'schema'
            exporter.data_dir = exporter.output_dir / 'data'
            exporter.schema_dir.mkdir(exist_ok=True)
            exporter.data_dir.mkdir(exist_ok=True)
        
        success = exporter.run_export()
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        logger.info("Export cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
