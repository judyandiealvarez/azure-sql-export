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
import yaml
import logging
import argparse
import pickle
import gzip
import time
from datetime import datetime, timedelta
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
    
    def __init__(self, config_file: str = "config.yaml", config_dict: dict = None, output_dir: str = None):
        """Initialize the exporter with configuration."""
        if config_dict is not None:
            # Use provided config dictionary
            self.config = config_dict
        else:
            # Load config from file
            self.config = self._load_config(config_file)
        
        self.connection = None
        
        # Use provided output directory or from config
        if output_dir is not None:
            self.output_dir = Path(output_dir)
        else:
            self.output_dir = Path(self.config.get('output_directory', 'export_output'))
        
        self.output_dir.mkdir(exist_ok=True)
        
        # Create subdirectories organized by object type
        self.schema_dir = self.output_dir / 'schema'
        self.data_dir = self.output_dir / 'data'
        self.binary_data_dir = self.output_dir / 'binary_data'
        self.schema_dir.mkdir(exist_ok=True)
        self.data_dir.mkdir(exist_ok=True)
        self.binary_data_dir.mkdir(exist_ok=True)
        
        # Create type-specific directories
        self.tables_dir = self.schema_dir / 'tables'
        self.views_dir = self.schema_dir / 'views'
        self.procedures_dir = self.schema_dir / 'procedures'
        self.functions_dir = self.schema_dir / 'functions'
        self.triggers_dir = self.schema_dir / 'triggers'
        
        for dir_path in [self.tables_dir, self.views_dir, self.procedures_dir, self.functions_dir, self.triggers_dir]:
            dir_path.mkdir(exist_ok=True)
        
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
            
            # Get stored procedures - use OBJECT_DEFINITION for exact formatting
            query = f"""
                SELECT 
                    ROUTINE_SCHEMA,
                    ROUTINE_NAME
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
            
            # Get procedure definitions using sys.sql_modules for exact original formatting
            procedures = []
            for row in cursor.fetchall():
                schema_name, proc_name = row
                
                # Get procedure definition using sp_helptext for complete original script
                cursor.execute("EXEC sp_helptext ?", f"{schema_name}.{proc_name}")
                definition_rows = cursor.fetchall()
                definition = "\n".join([row[0] for row in definition_rows]) if definition_rows else ""
                
                # Convert ALTER to CREATE for export purposes
                definition = definition.replace("ALTER PROCEDURE", "CREATE PROCEDURE", 1)
                
                # Generate SSMS-style script with headers and SET statements
                ssms_script = f"""/****** Object: StoredProcedure [{schema_name}].[{proc_name}] Script Date: {datetime.now().strftime('%m/%d/%Y %I:%M:%S %p')} ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
{definition}
GO"""
                definition = ssms_script
                
                procedures.append({
                    'schema': schema_name,
                    'name': proc_name,
                    'definition': definition
                })
            
            objects['stored_procedures'] = procedures
            
            # Get functions - use OBJECT_DEFINITION for exact formatting
            query = f"""
                SELECT 
                    ROUTINE_SCHEMA,
                    ROUTINE_NAME
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
            
            # Get function definitions using sys.sql_modules for exact original formatting
            functions = []
            for row in cursor.fetchall():
                schema_name, func_name = row
                
                # Get function definition using sp_helptext for complete original script
                cursor.execute("EXEC sp_helptext ?", f"{schema_name}.{func_name}")
                definition_rows = cursor.fetchall()
                definition = "\n".join([row[0] for row in definition_rows]) if definition_rows else ""
                
                # Convert ALTER to CREATE for export purposes
                definition = definition.replace("ALTER FUNCTION", "CREATE FUNCTION", 1)
                
                # Generate SSMS-style script with headers and SET statements
                ssms_script = f"""/****** Object: UserDefinedFunction [{schema_name}].[{func_name}] Script Date: {datetime.now().strftime('%m/%d/%Y %I:%M:%S %p')} ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
{definition}
GO"""
                definition = ssms_script
                
                functions.append({
                    'schema': schema_name,
                    'name': func_name,
                    'definition': definition
                })
            
            objects['functions'] = functions
            
            # Get triggers - use sys.triggers for better definition access
            cursor.execute("""
                SELECT 
                    s.name as schema_name,
                    t.name as trigger_name,
                    o.name as table_name,
                    t.is_disabled,
                    t.is_not_for_replication,
                    t.is_instead_of_trigger
                FROM sys.triggers t
                INNER JOIN sys.objects o ON t.parent_id = o.object_id
                INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
                WHERE t.parent_class = 1  -- Only table triggers
                ORDER BY s.name, t.name
            """)
            
            trigger_info = cursor.fetchall()
            
            # Get trigger definitions using OBJECT_DEFINITION
            triggers = []
            for row in trigger_info:
                schema_name, trigger_name, table_name, is_disabled, is_not_for_replication, is_instead_of_trigger = row
                
                # Check schema filter
                if include_schemas and schema_name not in include_schemas:
                    continue
                if exclude_schemas and schema_name in exclude_schemas:
                    continue
                
                # Get trigger definition using sp_helptext for complete original script
                cursor.execute("EXEC sp_helptext ?", f"{schema_name}.{trigger_name}")
                definition_rows = cursor.fetchall()
                definition = "\n".join([row[0] for row in definition_rows]) if definition_rows else ""
                
                # Convert ALTER to CREATE for export purposes
                definition = definition.replace("ALTER TRIGGER", "CREATE TRIGGER", 1)
                
                # Generate SSMS-style script with headers and SET statements
                ssms_script = f"""/****** Object: DdlTrigger [{schema_name}].[{trigger_name}] Script Date: {datetime.now().strftime('%m/%d/%Y %I:%M:%S %p')} ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
{definition}
GO"""
                definition = ssms_script
                
                triggers.append({
                    'schema': schema_name,
                    'name': trigger_name,
                    'table': table_name,
                    'definition': definition,
                    'is_disabled': is_disabled,
                    'is_not_for_replication': is_not_for_replication,
                    'is_instead_of_trigger': is_instead_of_trigger
                })
            
            objects['triggers'] = triggers
            
            cursor.close()
            logger.info(f"Retrieved schema objects: {sum(len(v) for v in objects.values())} total")
            logger.info(f"Triggers found: {len(triggers)}")
            for trigger in triggers:
                logger.info(f"  - {trigger['schema']}.{trigger['name']} on {trigger['table']}")
            
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
            reporting_interval = self.config.get('reporting_interval', 1000)  # Report every N batches
            insert_statements = []
            start_time = time.time()
            
            for offset in range(0, row_count, batch_size):
                cursor.execute(f"""
                    SELECT * FROM {full_table_name}
                    ORDER BY (SELECT NULL)
                    OFFSET {offset} ROWS
                    FETCH NEXT {batch_size} ROWS ONLY
                """)
                
                rows = cursor.fetchall()
                
                # Only log every N batches to avoid slowing down export
                batch_num = offset//batch_size + 1
                total_batches = (row_count-1)//batch_size + 1
                if batch_num % reporting_interval == 0 or batch_num == total_batches:
                    # Calculate ETA
                    elapsed_time = time.time() - start_time
                    processed_rows = offset + len(rows)
                    if processed_rows > 0 and elapsed_time > 0:
                        rows_per_second = processed_rows / elapsed_time
                        remaining_rows = row_count - processed_rows
                        eta_seconds = remaining_rows / rows_per_second if rows_per_second > 0 else 0
                        eta = datetime.now() + timedelta(seconds=eta_seconds)
                        eta_str = eta.strftime("%H:%M:%S")
                        logger.info(f"Processed batch {batch_num}/{total_batches} for {full_table_name} ({processed_rows}/{row_count} rows) - ETA: {eta_str}")
                    else:
                        logger.info(f"Processed batch {batch_num}/{total_batches} for {full_table_name} ({processed_rows}/{row_count} rows)")
                
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
    
    def export_table_data_binary(self, table_info: Dict) -> bool:
        """Export table data as binary format (pickle + gzip)."""
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
                logger.info(f"No data in table {full_table_name}")
                return True
            
            logger.info(f"Exporting {row_count} rows from {full_table_name} as binary")
            
            # Export data in batches
            batch_size = self.config.get('batch_size', 10000)  # Larger batches for binary
            reporting_interval = self.config.get('reporting_interval', 1000)  # Report every N batches
            all_data = []
            start_time = time.time()
            
            for offset in range(0, row_count, batch_size):
                cursor.execute(f"""
                    SELECT * FROM {full_table_name}
                    ORDER BY (SELECT NULL)
                    OFFSET {offset} ROWS
                    FETCH NEXT {batch_size} ROWS ONLY
                """)
                
                rows = cursor.fetchall()
                all_data.extend(rows)
                
                # Only log every N batches to avoid slowing down export
                batch_num = offset//batch_size + 1
                total_batches = (row_count-1)//batch_size + 1
                if batch_num % reporting_interval == 0 or batch_num == total_batches:
                    # Calculate ETA
                    elapsed_time = time.time() - start_time
                    processed_rows = len(all_data)
                    if processed_rows > 0 and elapsed_time > 0:
                        rows_per_second = processed_rows / elapsed_time
                        remaining_rows = row_count - processed_rows
                        eta_seconds = remaining_rows / rows_per_second if rows_per_second > 0 else 0
                        eta = datetime.now() + timedelta(seconds=eta_seconds)
                        eta_str = eta.strftime("%H:%M:%S")
                        logger.info(f"Processed batch {batch_num}/{total_batches} for {full_table_name} ({processed_rows}/{row_count} rows) - ETA: {eta_str}")
                    else:
                        logger.info(f"Processed batch {batch_num}/{total_batches} for {full_table_name} ({processed_rows}/{row_count} rows)")
            
            cursor.close()
            
            # Create binary file
            binary_file = self.binary_data_dir / f"{schema_name}.{table_name}.pkl.gz"
            
            # Prepare data for serialization
            data_info = {
                'schema': schema_name,
                'table': table_name,
                'columns': column_names,
                'data': all_data,
                'row_count': len(all_data),
                'exported_at': datetime.now().isoformat()
            }
            
            # Save as compressed pickle
            with gzip.open(binary_file, 'wb') as f:
                pickle.dump(data_info, f, protocol=pickle.HIGHEST_PROTOCOL)
            
            # Calculate compression ratio
            original_size = len(str(all_data))
            compressed_size = binary_file.stat().st_size
            compression_ratio = (1 - compressed_size / original_size) * 100 if original_size > 0 else 0
            
            logger.info(f"Exported binary data: {binary_file}")
            logger.info(f"Compression ratio: {compression_ratio:.1f}% (original: {original_size:,} bytes, compressed: {compressed_size:,} bytes)")
            
            return True
            
        except Exception as e:
            logger.error(f"Error exporting binary data for {table_info['schema']}.{table_info['name']}: {e}")
            return False
    
    def export_schema_objects(self, objects: Dict[str, List[Dict]]):
        """Export all schema objects to SQL files."""
        logger.info("Exporting schema objects...")
        
        # Export tables
        for table in objects['tables']:
            schema_name = table['schema']
            table_name = table['name']
            
            # Export table schema
            table_schema = self.export_table_schema(table)
            schema_file = self.tables_dir / f"{schema_name}.{table_name}.sql"
            
            with open(schema_file, 'w', encoding='utf-8') as f:
                f.write(f"-- Table schema for {schema_name}.{table_name}\n")
                f.write(f"-- Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(table_schema)
            
            logger.info(f"Exported table schema: {schema_file}")
        
        # Export views
        for view in objects['views']:
            schema_name = view['schema']
            view_name = view['name']
            
            try:
                cursor = self.connection.cursor()
                # Use sp_helptext for complete original script
                cursor.execute("EXEC sp_helptext ?", f"{schema_name}.{view_name}")
                definition_rows = cursor.fetchall()
                view_definition = "\n".join([row[0] for row in definition_rows]) if definition_rows else ""
                
                # Convert ALTER to CREATE for export purposes
                view_definition = view_definition.replace("ALTER VIEW", "CREATE VIEW", 1)
                
                # Generate SSMS-style script with headers and SET statements
                ssms_script = f"""/****** Object: View [{schema_name}].[{view_name}] Script Date: {datetime.now().strftime('%m/%d/%Y %I:%M:%S %p')} ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
{view_definition}
GO"""
                view_definition = ssms_script
                cursor.close()
                
                view_file = self.views_dir / f"{schema_name}.{view_name}.sql"
                with open(view_file, 'w', encoding='utf-8') as f:
                    f.write(view_definition)
                
                logger.info(f"Exported view: {view_file}")
                
            except Exception as e:
                logger.error(f"Error exporting view {schema_name}.{view_name}: {e}")
        
        # Export stored procedures
        for proc in objects['stored_procedures']:
            schema_name = proc['schema']
            proc_name = proc['name']
            
            proc_file = self.procedures_dir / f"{schema_name}.{proc_name}.sql"
            with open(proc_file, 'w', encoding='utf-8') as f:
                f.write(proc['definition'])
            
            logger.info(f"Exported stored procedure: {proc_file}")
        
        # Export functions
        for func in objects['functions']:
            schema_name = func['schema']
            func_name = func['name']
            
            func_file = self.functions_dir / f"{schema_name}.{func_name}.sql"
            with open(func_file, 'w', encoding='utf-8') as f:
                f.write(func['definition'])
            
            logger.info(f"Exported function: {func_file}")
        
        # Export triggers
        for trigger in objects['triggers']:
            schema_name = trigger['schema']
            trigger_name = trigger['name']
            
            trigger_file = self.triggers_dir / f"{schema_name}.{trigger_name}.sql"
            with open(trigger_file, 'w', encoding='utf-8') as f:
                # Write the full trigger definition
                if trigger['definition']:
                    f.write(trigger['definition'])
                else:
                    # Fallback if definition is not available
                    f.write(f"-- Warning: Trigger definition not available\n")
                    f.write(f"-- This trigger exists on table {schema_name}.{trigger['table']}\n")
                    f.write(f"-- Please recreate manually or check permissions")
            
            logger.info(f"Exported trigger: {trigger_file}")
    
    def export_table_data_all(self, objects: Dict[str, List[Dict]]):
        """Export data for all tables."""
        data_format = self.config.get('data_format', 'sql')  # 'sql' or 'binary'
        
        if data_format == 'binary':
            logger.info("Exporting table data as binary format...")
            for table in objects['tables']:
                self.export_table_data_binary(table)
        else:
            logger.info("Exporting table data as SQL format...")
            for table in objects['tables']:
                schema_name = table['schema']
                table_name = table['name']
                
                # Export table data
                table_data = self.export_table_data(table)
                data_file = self.data_dir / f"{schema_name}.{table_name}.sql"
                
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
    parser.add_argument('--config', default='config.yaml', help='Configuration file path (YAML or JSON)')
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
