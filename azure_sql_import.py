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
import pickle
import gzip
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Set
import pyodbc
import pandas as pd
from pathlib import Path
from difflib import unified_diff
from collections import defaultdict, deque

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


class DependencyAnalyzer:
    """Analyzes dependencies between database objects."""
    
    def __init__(self):
        self.dependencies = defaultdict(set)  # object -> set of dependencies
        self.dependents = defaultdict(set)    # object -> set of dependents
        self.object_types = {}                # object -> type
    
    def add_object(self, obj_name: str, obj_type: str):
        """Add an object to the dependency graph."""
        self.object_types[obj_name] = obj_type
    
    def add_dependency(self, obj_name: str, dependency: str):
        """Add a dependency relationship."""
        if dependency != obj_name:  # Avoid self-dependencies
            self.dependencies[obj_name].add(dependency)
            self.dependents[dependency].add(obj_name)
    
    def analyze_sql_dependencies(self, sql_content: str, obj_name: str) -> Set[str]:
        """Analyze SQL content to find dependencies."""
        dependencies = set()
        
        # Common patterns for object references
        patterns = [
            r'FROM\s+\[?(\w+)\]?\.\[?(\w+)\]?',  # FROM schema.table
            r'JOIN\s+\[?(\w+)\]?\.\[?(\w+)\]?',   # JOIN schema.table
            r'EXEC\s+\[?(\w+)\]?\.\[?(\w+)\]?',   # EXEC schema.procedure
            r'EXECUTE\s+\[?(\w+)\]?\.\[?(\w+)\]?', # EXECUTE schema.procedure
            r'\[?(\w+)\]?\.\[?(\w+)\]?\(',         # Function calls
            r'CREATE\s+VIEW\s+\[?(\w+)\]?\.\[?(\w+)\]?\s+AS\s+.*?FROM\s+\[?(\w+)\]?\.\[?(\w+)\]?',  # View dependencies
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, sql_content, re.IGNORECASE | re.MULTILINE)
            for match in matches:
                if len(match) == 2:
                    schema, obj = match
                    if schema and obj:
                        dep_name = f"{schema}.{obj}"
                        dependencies.add(dep_name)
                elif len(match) == 4:  # For view patterns
                    schema1, obj1, schema2, obj2 = match
                    if schema2 and obj2:
                        dep_name = f"{schema2}.{obj2}"
                        dependencies.add(dep_name)
        
        return dependencies
    
    def topological_sort(self) -> List[str]:
        """Perform topological sort to determine import order."""
        # Calculate in-degrees
        in_degree = defaultdict(int)
        for obj in self.object_types:
            in_degree[obj] = len(self.dependencies[obj])
        
        # Find objects with no dependencies
        queue = deque([obj for obj in self.object_types if in_degree[obj] == 0])
        result = []
        
        while queue:
            obj = queue.popleft()
            result.append(obj)
            
            # Reduce in-degree for dependents
            for dependent in self.dependents[obj]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)
        
        # Check for circular dependencies
        if len(result) != len(self.object_types):
            remaining = set(self.object_types.keys()) - set(result)
            logger.warning(f"Circular dependencies detected for objects: {remaining}")
            # Add remaining objects at the end
            result.extend(remaining)
        
        return result
    
    def get_import_order(self, objects: Dict[str, List[Dict]]) -> List[Tuple[str, Dict]]:
        """Get the proper import order for all objects."""
        # Add all objects to the dependency graph
        for obj_type, obj_list in objects.items():
            for obj in obj_list:
                obj_name = f"{obj['schema']}.{obj['name']}"
                self.add_object(obj_name, obj_type)
        
        # Analyze dependencies from SQL files
        for obj_type, obj_list in objects.items():
            for obj in obj_list:
                obj_name = f"{obj['schema']}.{obj['name']}"
                try:
                    with open(obj['file'], 'r', encoding='utf-8') as f:
                        sql_content = f.read()
                    
                    dependencies = self.analyze_sql_dependencies(sql_content, obj_name)
                    for dep in dependencies:
                        self.add_dependency(obj_name, dep)
                        
                except Exception as e:
                    logger.warning(f"Could not analyze dependencies for {obj_name}: {e}")
        
        # Get topological order
        ordered_objects = self.topological_sort()
        
        # Convert back to (type, obj) tuples
        result = []
        for obj_name in ordered_objects:
            obj_type = self.object_types[obj_name]
            # Find the original object
            for obj in objects[obj_type]:
                if f"{obj['schema']}.{obj['name']}" == obj_name:
                    result.append((obj_type, obj))
                    break
        
        return result
    
    def show_dependency_info(self, objects: Dict[str, List[Dict]]):
        """Show dependency information for debugging."""
        logger.info("Analyzing dependencies...")
        
        # Add all objects to the dependency graph
        for obj_type, obj_list in objects.items():
            for obj in obj_list:
                obj_name = f"{obj['schema']}.{obj['name']}"
                self.add_object(obj_name, obj_type)
        
        # Analyze dependencies from SQL files
        for obj_type, obj_list in objects.items():
            for obj in obj_list:
                obj_name = f"{obj['schema']}.{obj['name']}"
                try:
                    with open(obj['file'], 'r', encoding='utf-8') as f:
                        sql_content = f.read()
                    
                    dependencies = self.analyze_sql_dependencies(sql_content, obj_name)
                    for dep in dependencies:
                        self.add_dependency(obj_name, dep)
                        
                except Exception as e:
                    logger.warning(f"Could not analyze dependencies for {obj_name}: {e}")
        
        # Show dependency information
        logger.info("Dependency Analysis:")
        for obj_name in sorted(self.object_types.keys()):
            deps = self.dependencies[obj_name]
            if deps:
                logger.info(f"  {obj_name} depends on: {', '.join(sorted(deps))}")
            else:
                logger.info(f"  {obj_name} has no dependencies")


class AzureSQLImporter:
    """Main class for importing Azure SQL Database schema and data."""
    
    def __init__(self, config_file: str = "config.yaml", config_dict: dict = None, import_dir: str = None):
        """Initialize the importer with configuration."""
        if config_dict is not None:
            # Use provided config dictionary
            self.config = config_dict
        else:
            # Load config from file
            self.config = self._load_config(config_file)
        
        self.connection = None
        
        # Use provided import directory or from config
        if import_dir is not None:
            self.import_dir = Path(import_dir)
        else:
            self.import_dir = Path(self.config.get('import_directory', 'export_output'))
        self.schema_dir = self.import_dir / 'schema'
        self.data_dir = self.import_dir / 'data'
        self.binary_data_dir = self.import_dir / 'binary_data'
        
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
    
    def _normalize_sql(self, sql_text: str) -> str:
        """Normalize SQL text for comparison by removing cosmetic differences."""
        if not sql_text:
            return ""
        
        import re
        
        # Remove SSMS headers and SET statements from exported files
        # This handles the difference between exported (with headers) and existing (without headers)
        sql_text = re.sub(r'/\*.*?Script Date:.*?\*/', '', sql_text, flags=re.DOTALL)
        sql_text = re.sub(r'SET ANSI_NULLS ON\s*', '', sql_text, flags=re.IGNORECASE)
        sql_text = re.sub(r'SET QUOTED_IDENTIFIER ON\s*', '', sql_text, flags=re.IGNORECASE)
        sql_text = re.sub(r'GO\s*', '', sql_text, flags=re.IGNORECASE)
        
        # Remove multi-line comments (/* ... */)
        sql_text = re.sub(r'/\*.*?\*/', '', sql_text, flags=re.DOTALL)
        
        lines = sql_text.split('\n')
        normalized_lines = []
        
        for line in lines:
            # Remove leading/trailing whitespace
            line = line.strip()
            
            # Skip empty lines
            if not line:
                continue
                
            # Skip comment lines that are just metadata
            if line.startswith('--'):
                # Skip generation comments and metadata comments
                if any(meta in line.lower() for meta in [
                    'generated on', 'script date', 'object:', 'table schema for'
                ]):
                    continue
                # Keep other comments that might be important
                normalized_lines.append(line)
                continue
            
            # Normalize whitespace in SQL statements
            line = re.sub(r'\s+', ' ', line)
            normalized_lines.append(line)
        
        return '\n'.join(normalized_lines)
    
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
                    s.name as schema_name,
                    t.name as trigger_name
                FROM sys.triggers t
                INNER JOIN sys.objects o ON t.parent_id = o.object_id
                INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
                WHERE t.parent_class = 1  -- Only table triggers
                ORDER BY s.name, t.name
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
    
    def get_procedure_schema(self, schema_name: str, procedure_name: str) -> str:
        """Get current procedure schema definition using sp_helptext (same as export)."""
        try:
            cursor = self.connection.cursor()
            cursor.execute("EXEC sp_helptext ?", f"{schema_name}.{procedure_name}")
            definition_rows = cursor.fetchall()
            definition = "\n".join([row[0].rstrip('\r\n') for row in definition_rows]) if definition_rows else ""
            cursor.close()
            return definition
        except Exception as e:
            logger.error(f"Error getting procedure schema for {schema_name}.{procedure_name}: {e}")
            return ""
    
    def get_function_schema(self, schema_name: str, function_name: str) -> str:
        """Get current function schema definition using sp_helptext (same as export)."""
        try:
            cursor = self.connection.cursor()
            cursor.execute("EXEC sp_helptext ?", f"{schema_name}.{function_name}")
            definition_rows = cursor.fetchall()
            definition = "\n".join([row[0].rstrip('\r\n') for row in definition_rows]) if definition_rows else ""
            cursor.close()
            return definition
        except Exception as e:
            logger.error(f"Error getting function schema for {schema_name}.{function_name}: {e}")
            return ""
    
    def get_view_schema(self, schema_name: str, view_name: str) -> str:
        """Get current view schema definition using sp_helptext (same as export)."""
        try:
            cursor = self.connection.cursor()
            cursor.execute("EXEC sp_helptext ?", f"{schema_name}.{view_name}")
            definition_rows = cursor.fetchall()
            definition = "\n".join([row[0].rstrip('\r\n') for row in definition_rows]) if definition_rows else ""
            cursor.close()
            return definition
        except Exception as e:
            logger.error(f"Error getting view schema for {schema_name}.{view_name}: {e}")
            return ""
    
    def get_trigger_schema(self, schema_name: str, trigger_name: str) -> str:
        """Get current trigger schema definition using sp_helptext (same as export)."""
        try:
            cursor = self.connection.cursor()
            cursor.execute("EXEC sp_helptext ?", f"{schema_name}.{trigger_name}")
            definition_rows = cursor.fetchall()
            definition = "\n".join([row[0].rstrip('\r\n') for row in definition_rows]) if definition_rows else ""
            cursor.close()
            return definition
        except Exception as e:
            logger.error(f"Error getting trigger schema for {schema_name}.{trigger_name}: {e}")
            return ""
    
    def compare_schemas(self, new_schema: str, existing_schema: str, object_name: str) -> List[str]:
        """Compare two schema definitions and return differences."""
        if not existing_schema:
            return [f"New object: {object_name}"]
        
        # Normalize both schemas for comparison
        new_normalized = self._normalize_sql(new_schema)
        existing_normalized = self._normalize_sql(existing_schema)
        
        # If normalized versions are identical, no real differences
        if new_normalized == existing_normalized:
            return ["No differences found"]
        
        # Show differences in original format for user review
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
            
            # Split by GO statements and execute each batch separately
            batches = []
            current_batch = []
            
            for line in sql_content.split('\n'):
                line = line.strip()
                if line.upper() == 'GO':
                    if current_batch:
                        batches.append('\n'.join(current_batch))
                        current_batch = []
                elif line and not line.startswith('--') and not line.startswith('/*'):
                    current_batch.append(line)
            
            # Add the last batch if it exists
            if current_batch:
                batches.append('\n'.join(current_batch))
            
            print(f"\n🔍 DEBUG: Found {len(batches)} batches to execute for {description}")
            for i, batch in enumerate(batches):
                print(f"Batch {i+1}: {batch[:100]}...")
            
            cursor = self.connection.cursor()
            for i, batch in enumerate(batches):
                if batch.strip():
                    try:
                        print(f"\n🔍 DEBUG: Executing batch {i+1}/{len(batches)}:")
                        print(f"SQL: {batch}")
                        cursor.execute(batch)
                        logger.info(f"Executed batch {i+1}/{len(batches)} for {description}")
                        print(f"✅ Batch {i+1} executed successfully")
                    except Exception as e:
                        logger.error(f"Error executing batch {i+1} for {description}: {e}")
                        logger.error(f"Batch: {batch[:100]}...")
                        print(f"❌ ERROR executing batch {i+1}: {e}")
                        print(f"Failed SQL: {batch}")
                        cursor.rollback()
                        return False
            
            cursor.commit()
            cursor.close()
            logger.info(f"Successfully executed {description}")
            return True
            
        except Exception as e:
            logger.error(f"Error executing {description}: {e}")
            return False
    
    def generate_alter_statements(self, new_schema: str, existing_schema: str, schema_name: str, table_name: str) -> List[str]:
        """Generate ALTER TABLE statements for table changes."""
        alter_statements = []
        
        try:
            # Parse new table definition
            new_lines = [line.strip() for line in new_schema.split('\n') if line.strip() and not line.strip().startswith('--')]
            existing_lines = [line.strip() for line in existing_schema.split('\n') if line.strip() and not line.strip().startswith('--')]
            
            # Extract column definitions from CREATE TABLE statements
            new_columns = self._extract_columns(new_schema)
            existing_columns = self._extract_columns(existing_schema)
            
            # Find new columns
            for col_name, col_def in new_columns.items():
                if col_name not in existing_columns:
                    alter_statements.append(f"ALTER TABLE [{schema_name}].[{table_name}] ADD {col_def}")
            
            # Find modified columns
            for col_name, col_def in new_columns.items():
                if col_name in existing_columns and existing_columns[col_name] != col_def:
                    # Parse column definition to separate data type from constraints
                    new_col_parts = self._parse_column_definition(col_def)
                    existing_col_parts = self._parse_column_definition(existing_columns[col_name])
                    
                    # Check if data type or nullability changed
                    if (new_col_parts['data_type'] != existing_col_parts['data_type'] or 
                        new_col_parts['nullable'] != existing_col_parts['nullable']):
                        # Generate ALTER COLUMN for data type/nullability changes
                        alter_col_def = f"[{col_name}] {new_col_parts['data_type']}"
                        if not new_col_parts['nullable']:
                            alter_col_def += " NOT NULL"
                        alter_statements.append(f"ALTER TABLE [{schema_name}].[{table_name}] ALTER COLUMN {alter_col_def}")
                    
                    # Check if DEFAULT constraint changed
                    if new_col_parts['default'] != existing_col_parts['default']:
                        # Drop existing default constraint if it exists
                        if existing_col_parts['default']:
                            # Try to find the actual constraint name first
                            constraint_name = self._get_default_constraint_name(schema_name, table_name, col_name)
                            if constraint_name:
                                alter_statements.append(f"ALTER TABLE [{schema_name}].[{table_name}] DROP CONSTRAINT [{constraint_name}]")
                            else:
                                # Fallback to standard naming convention
                                alter_statements.append(f"ALTER TABLE [{schema_name}].[{table_name}] DROP CONSTRAINT [DF_{table_name}_{col_name}]")
                        
                        # Add new default constraint if specified
                        if new_col_parts['default']:
                            # Extract just the value part from DEFAULT (value)
                            default_value = new_col_parts['default'].replace('DEFAULT', '').strip()
                            alter_statements.append(f"ALTER TABLE [{schema_name}].[{table_name}] ADD CONSTRAINT [DF_{table_name}_{col_name}] DEFAULT {default_value} FOR [{col_name}]")
            
            # Find dropped columns (be careful with this)
            for col_name in existing_columns:
                if col_name not in new_columns:
                    logger.warning(f"Column {col_name} exists in database but not in new schema - manual review needed")
            
            return alter_statements
            
        except Exception as e:
            logger.error(f"Error generating ALTER statements: {e}")
            return []
    
    def _extract_columns(self, schema_sql: str) -> Dict[str, str]:
        """Extract column definitions from CREATE TABLE statement."""
        columns = {}
        
        # Find the CREATE TABLE statement
        lines = schema_sql.split('\n')
        in_table_def = False
        
        for line in lines:
            line = line.strip()
            if not line or line.startswith('--'):
                continue
                
            if line.upper().startswith('CREATE TABLE'):
                in_table_def = True
                continue
                
            if in_table_def:
                if line.startswith(')') or line.upper().startswith('CONSTRAINT'):
                    break
                    
                # Extract column definition
                if '[' in line and ']' in line:
                    # Find column name and definition
                    parts = line.split(',')
                    for part in parts:
                        part = part.strip()
                        if '[' in part and ']' in part:
                            # Extract column name
                            start = part.find('[')
                            end = part.find(']')
                            if start != -1 and end != -1:
                                col_name = part[start+1:end]
                                col_def = part.strip()
                                if col_def.endswith(','):
                                    col_def = col_def[:-1]
                                columns[col_name] = col_def
        
        return columns
    
    def _parse_column_definition(self, col_def: str) -> Dict[str, str]:
        """Parse a column definition into its components."""
        parts = {
            'data_type': '',
            'nullable': True,
            'default': None
        }
        
        # Remove column name and brackets
        col_def = col_def.strip()
        if col_def.startswith('[') and ']' in col_def:
            col_def = col_def[col_def.find(']') + 1:].strip()
        
        # Remove trailing comma if present
        if col_def.endswith(','):
            col_def = col_def[:-1].strip()
        
        # Check for NOT NULL
        if 'NOT NULL' in col_def.upper():
            parts['nullable'] = False
            col_def = col_def.replace('NOT NULL', '').strip()
        
        # Check for DEFAULT constraint
        if 'DEFAULT' in col_def.upper():
            default_start = col_def.upper().find('DEFAULT')
            default_part = col_def[default_start:].strip()
            parts['default'] = default_part
            col_def = col_def[:default_start].strip()
        
        # Clean up data type
        parts['data_type'] = col_def.strip()
        
        return parts
    
    def _get_default_constraint_name(self, schema_name: str, table_name: str, column_name: str) -> str:
        """Get the actual name of a default constraint for a column."""
        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT dc.name
                FROM sys.default_constraints dc
                INNER JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
                INNER JOIN sys.tables t ON c.object_id = t.object_id
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = ? AND t.name = ? AND c.name = ?
            """, schema_name, table_name, column_name)
            
            result = cursor.fetchone()
            cursor.close()
            return result[0] if result else None
            
        except Exception as e:
            logger.warning(f"Could not get default constraint name for {schema_name}.{table_name}.{column_name}: {e}")
            return None
    
    def execute_alter_statements(self, alter_statements: List[str], description: str) -> bool:
        """Execute ALTER statements."""
        if not alter_statements:
            logger.info(f"No ALTER statements needed for {description}")
            return True
            
        try:
            cursor = self.connection.cursor()
            
            for i, statement in enumerate(alter_statements):
                try:
                    cursor.execute(statement)
                    logger.info(f"Executed ALTER statement {i+1}/{len(alter_statements)} for {description}")
                except Exception as e:
                    # For constraint operations, try to continue if it's a "doesn't exist" error
                    if "does not exist" in str(e).lower() or "not found" in str(e).lower():
                        logger.warning(f"Constraint operation skipped (constraint may not exist): {statement}")
                        continue
                    else:
                        logger.error(f"Error executing ALTER statement {i+1} for {description}: {e}")
                        logger.error(f"Statement: {statement}")
                        cursor.rollback()
                        return False
            
            cursor.commit()
            cursor.close()
            return True
            
        except Exception as e:
            logger.error(f"Error executing ALTER statements for {description}: {e}")
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
            reporting_interval = self.config.get('reporting_interval', 1000)  # Report every N batches
            start_time = time.time()
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
                
                # Only log every N batches to avoid slowing down import
                batch_num = i//batch_size + 1
                total_batches = (len(insert_statements)-1)//batch_size + 1
                if batch_num % reporting_interval == 0 or batch_num == total_batches:
                    # Calculate ETA
                    elapsed_time = time.time() - start_time
                    processed_statements = i + len(batch)
                    if processed_statements > 0 and elapsed_time > 0:
                        statements_per_second = processed_statements / elapsed_time
                        remaining_statements = len(insert_statements) - processed_statements
                        eta_seconds = remaining_statements / statements_per_second if statements_per_second > 0 else 0
                        eta = datetime.now() + timedelta(seconds=eta_seconds)
                        eta_str = eta.strftime("%H:%M:%S")
                        logger.info(f"Imported batch {batch_num}/{total_batches} for {schema_name}.{table_name} ({processed_statements}/{len(insert_statements)} statements) - ETA: {eta_str}")
                    else:
                        logger.info(f"Imported batch {batch_num}/{total_batches} for {schema_name}.{table_name} ({processed_statements}/{len(insert_statements)} statements)")
            
            cursor.close()
            logger.info(f"Successfully imported data for {schema_name}.{table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error importing data for {schema_name}.{table_name}: {e}")
            return False
    
    def import_table_data_binary(self, schema_name: str, table_name: str, binary_file: Path) -> bool:
        """Import table data from binary format."""
        try:
            # Load binary data
            with gzip.open(binary_file, 'rb') as f:
                data_info = pickle.load(f)
            
            logger.info(f"Loading binary data for {schema_name}.{table_name}: {data_info['row_count']} rows")
            
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
            
            # Prepare data for bulk insert
            columns = data_info['columns']
            data = data_info['data']
            
            if not data:
                logger.info(f"No data to import for {schema_name}.{table_name}")
                cursor.close()
                return True
            
            # Create parameterized insert statement
            placeholders = ",".join(["?" for _ in columns])
            insert_sql = f"INSERT INTO [{schema_name}].[{table_name}] ([{'], ['.join(columns)}]) VALUES ({placeholders})"
            
            # Insert data in batches
            batch_size = self.config.get('batch_size', 1000)
            reporting_interval = self.config.get('reporting_interval', 1000)  # Report every N batches
            total_rows = len(data)
            start_time = time.time()
            
            for i in range(0, total_rows, batch_size):
                batch = data[i:i + batch_size]
                
                for row in batch:
                    try:
                        cursor.execute(insert_sql, row)
                    except Exception as e:
                        logger.error(f"Error inserting row for {schema_name}.{table_name}: {e}")
                        logger.error(f"Row data: {row}")
                        cursor.rollback()
                        return False
                
                cursor.commit()
                
                # Only log every N batches to avoid slowing down import
                batch_num = i//batch_size + 1
                total_batches = (total_rows-1)//batch_size + 1
                if batch_num % reporting_interval == 0 or batch_num == total_batches:
                    # Calculate ETA
                    elapsed_time = time.time() - start_time
                    processed_rows = i + len(batch)
                    if processed_rows > 0 and elapsed_time > 0:
                        rows_per_second = processed_rows / elapsed_time
                        remaining_rows = total_rows - processed_rows
                        eta_seconds = remaining_rows / rows_per_second if rows_per_second > 0 else 0
                        eta = datetime.now() + timedelta(seconds=eta_seconds)
                        eta_str = eta.strftime("%H:%M:%S")
                        logger.info(f"Imported batch {batch_num}/{total_batches} for {schema_name}.{table_name} ({processed_rows}/{total_rows} rows) - ETA: {eta_str}")
                    else:
                        logger.info(f"Imported batch {batch_num}/{total_batches} for {schema_name}.{table_name} ({processed_rows}/{total_rows} rows)")
            
            cursor.close()
            logger.info(f"Successfully imported {total_rows} rows for {schema_name}.{table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error importing binary data for {schema_name}.{table_name}: {e}")
            return False
    
    def import_schema_objects(self, exported_files: Dict[str, List[Dict]], existing_objects: Dict[str, Dict]):
        """Import schema objects with dependency analysis and proper ordering."""
        logger.info("Starting schema object import with dependency analysis...")
        
        # Track statistics
        skipped_identical = 0
        total_processed = 0
        
        # Analyze dependencies and get proper import order
        analyzer = DependencyAnalyzer()
        import_order = analyzer.get_import_order(exported_files)
        
        logger.info(f"Import order determined: {len(import_order)} objects")
        
        # Group by type for logging
        type_counts = defaultdict(int)
        for obj_type, obj in import_order:
            type_counts[obj_type] += 1
        
        logger.info("Import plan:")
        for obj_type, count in type_counts.items():
            logger.info(f"  {obj_type.capitalize()}: {count} objects")
        
        # Import objects in dependency order
        for obj_type, obj in import_order:
            schema_name = obj['schema']
            object_name = obj['name']
            file_path = obj['file']
            full_name = f"{schema_name}.{object_name}"
            
            # Check if object exists
            object_key = f"{schema_name}.{object_name}"
            exists = object_key in existing_objects[obj_type]
            
            if exists and self.alter_existing:
                # Compare schemas
                with open(file_path, 'r', encoding='utf-8') as f:
                    new_schema = f.read()
                
                if obj_type == 'tables':
                    existing_schema = self.get_table_schema(schema_name, object_name)
                elif obj_type == 'procedures':
                    existing_schema = self.get_procedure_schema(schema_name, object_name)
                    print(f"\n🔍 DEBUG: Existing procedure schema for {full_name}:")
                    print("=" * 80)
                    print(existing_schema)
                    print("=" * 80)
                elif obj_type == 'functions':
                    existing_schema = self.get_function_schema(schema_name, object_name)
                elif obj_type == 'views':
                    existing_schema = self.get_view_schema(schema_name, object_name)
                elif obj_type == 'triggers':
                    existing_schema = self.get_trigger_schema(schema_name, object_name)
                else:
                    existing_schema = ""  # Fallback
                
                differences = self.compare_schemas(new_schema, existing_schema, full_name)
                
                # Debug: Show normalized versions for comparison
                if obj_type == 'procedures':
                    new_normalized = self._normalize_sql(new_schema)
                    existing_normalized = self._normalize_sql(existing_schema)
                    print(f"\n🔍 DEBUG: Normalized comparison for {full_name}:")
                    print("New normalized:")
                    print(new_normalized)
                    print("Existing normalized:")
                    print(existing_normalized)
                    print(f"Are they identical? {new_normalized == existing_normalized}")
                
                # Check if there are no real differences
                if len(differences) == 1 and "No differences found" in differences[0]:
                    skipped_identical += 1
                    logger.info(f"Skipping {obj_type[:-1]} {full_name} - no differences found (identical)")
                    continue
                
                print(f"\n--- {obj_type.upper()}: {full_name} ---")
                print("Differences found:")
                print("=" * 60)
                
                # Show actual differences with proper formatting
                in_diff_section = False
                for diff_line in differences:
                    if diff_line.startswith('---'):
                        print(f"\n📄 EXISTING: {diff_line[4:]}")  # Remove "--- "
                        in_diff_section = True
                    elif diff_line.startswith('+++'):
                        print(f"📄 NEW:      {diff_line[4:]}")  # Remove "+++ "
                    elif diff_line.startswith('@@'):
                        # Skip the hunk header for cleaner display
                        continue
                    elif diff_line.startswith('-') and in_diff_section:
                        print(f"❌ REMOVE:   {diff_line[1:].strip()}")  # Remove the - and trim
                    elif diff_line.startswith('+') and in_diff_section:
                        print(f"✅ ADD:      {diff_line[1:].strip()}")  # Remove the + and trim
                    elif diff_line.startswith(' ') and in_diff_section:
                        print(f"   CONTEXT:  {diff_line[1:].strip()}")  # Context line
                    elif not diff_line.startswith('@@'):
                        print(diff_line)
                
                print("=" * 60)
                
                # Show what will happen
                if exists and obj_type == 'tables':
                    print("📋 ACTION: Will generate ALTER TABLE statements to update the existing table")
                elif exists and obj_type in ['views', 'procedures', 'functions']:
                    print("📋 ACTION: Will use CREATE OR ALTER to update the existing object")
                elif exists and obj_type == 'triggers':
                    print("📋 ACTION: Will DROP and recreate the existing trigger")
                else:
                    print("📋 ACTION: Will create the new object")
                
                print("=" * 60)
                
                if not self.ask_confirmation(f"Import {obj_type[:-1]} {full_name}?"):
                    logger.info(f"Skipped {obj_type[:-1]} {full_name}")
                    continue
            
            elif exists and not self.alter_existing:
                logger.info(f"Skipping existing {obj_type[:-1]} {full_name}")
                continue
            
            # Execute the import
            description = f"{obj_type[:-1]} {full_name}"
            
            if exists and obj_type == 'tables':
                # For existing tables, generate and execute ALTER statements
                with open(file_path, 'r', encoding='utf-8') as f:
                    new_schema = f.read()
                
                existing_schema = self.get_table_schema(schema_name, object_name)
                alter_statements = self.generate_alter_statements(new_schema, existing_schema, schema_name, object_name)
                
                if self.execute_alter_statements(alter_statements, description):
                    total_processed += 1
                    logger.info(f"Successfully altered {description}")
                else:
                    logger.error(f"Failed to alter {description}")
            else:
                # For new objects or non-table objects, use CREATE OR ALTER
                if exists and obj_type in ['views', 'procedures', 'functions']:
                    # Use CREATE OR ALTER for existing objects
                    with open(file_path, 'r', encoding='utf-8') as f:
                        sql_content = f.read()
                    
                    # Debug: Show original SQL content
                    print(f"\n🔍 DEBUG: Original SQL content for {description}:")
                    print("=" * 80)
                    print(sql_content)
                    print("=" * 80)
                    
                    # Replace CREATE or ALTER with CREATE OR ALTER
                    # Handle SSMS headers by finding the actual SQL statement
                    lines = sql_content.split('\n')
                    modified_lines = []
                    replaced = False
                    
                    for line in lines:
                        line_upper = line.strip().upper()
                        if not replaced and (line_upper.startswith('CREATE ') or line_upper.startswith('ALTER ')):
                            # Replace the first CREATE or ALTER we find
                            if line_upper.startswith('CREATE '):
                                modified_line = line.replace('CREATE ', 'CREATE OR ALTER ', 1)
                                print(f"🔍 DEBUG: Replaced 'CREATE ' with 'CREATE OR ALTER ' in line: {line.strip()}")
                            else:
                                modified_line = line.replace('ALTER ', 'CREATE OR ALTER ', 1)
                                print(f"🔍 DEBUG: Replaced 'ALTER ' with 'CREATE OR ALTER ' in line: {line.strip()}")
                            modified_lines.append(modified_line)
                            replaced = True
                        else:
                            modified_lines.append(line)
                    
                    modified_sql = '\n'.join(modified_lines)
                    
                    # Write to temporary file
                    temp_file = file_path.parent / f"temp_{file_path.name}"
                    with open(temp_file, 'w', encoding='utf-8') as f:
                        f.write(modified_sql)
                    
                    # Log the SQL being executed for debugging
                    logger.info(f"Executing SQL for {description}:")
                    logger.info(f"Full SQL:\n{modified_sql}")
                    print(f"\n🔍 DEBUG: Full SQL being executed for {description}:")
                    print("=" * 80)
                    print(modified_sql)
                    print("=" * 80)
                    
                    success = self.execute_sql_file(temp_file, description)
                    temp_file.unlink()  # Clean up temp file
                    
                    # If CREATE OR ALTER failed, try DROP + CREATE
                    if not success:
                        print(f"\n🔍 DEBUG: CREATE OR ALTER failed, trying DROP + CREATE...")
                        with open(file_path, 'r', encoding='utf-8') as f:
                            original_sql = f.read()
                        
                        # Generate DROP + CREATE statements
                        drop_sql = f"DROP PROCEDURE IF EXISTS [{schema_name}].[{object_name}];\n"
                        combined_sql = drop_sql + original_sql
                        
                        # Write to temporary file
                        temp_file2 = file_path.parent / f"temp2_{file_path.name}"
                        with open(temp_file2, 'w', encoding='utf-8') as f:
                            f.write(combined_sql)
                        
                        print(f"🔍 DEBUG: Trying DROP + CREATE approach:")
                        print("=" * 80)
                        print(combined_sql)
                        print("=" * 80)
                        
                        success = self.execute_sql_file(temp_file2, description)
                        temp_file2.unlink()  # Clean up temp file
                    
                    if success:
                        total_processed += 1
                        logger.info(f"Successfully created or altered {description}")
                        
                        # Wait a moment for database to commit changes
                        import time
                        time.sleep(0.5)
                        
                        # Debug: Check what the procedure looks like after update
                        print(f"\n🔍 DEBUG: Checking procedure after update...")
                        updated_schema = self.get_procedure_schema(schema_name, object_name)
                        print(f"Updated procedure schema:")
                        print("=" * 80)
                        print(updated_schema)
                        print("=" * 80)
                        
                        # Check if the procedure was actually updated
                        if updated_schema == existing_schema:
                            print(f"⚠️  WARNING: Procedure schema is identical to before update!")
                            print(f"This suggests the CREATE OR ALTER didn't actually change anything.")
                            
                            # Check if the new schema is also identical to existing
                            if new_schema == existing_schema:
                                print(f"🔍 DEBUG: New schema is identical to existing schema!")
                                print(f"This means there was no actual difference to update.")
                            else:
                                print(f"🔍 DEBUG: New schema is different from existing, but update didn't change it.")
                                print(f"This suggests CREATE OR ALTER might not be working as expected.")
                        else:
                            print(f"✅ Procedure schema was updated successfully")
                        
                        # Compare again to see if they're now identical
                        print(f"\n🔍 DEBUG: Re-comparing after update...")
                        new_normalized = self._normalize_sql(new_schema)
                        updated_normalized = self._normalize_sql(updated_schema)
                        print(f"New normalized: {new_normalized}")
                        print(f"Updated normalized: {updated_normalized}")
                        print(f"Are they identical? {new_normalized == updated_normalized}")
                        
                        # If still not identical, show the differences
                        if new_normalized != updated_normalized:
                            print(f"\n🔍 DEBUG: Still different after update. Differences:")
                            diff = list(unified_diff(
                                updated_normalized.split('\n'), new_normalized.split('\n'),
                                fromfile="Updated", tofile="New", lineterm=""
                            ))
                            for line in diff:
                                print(line)
                        
                    else:
                        logger.error(f"Failed to create or alter {description}")
                elif exists and obj_type == 'triggers':
                    # For existing triggers, drop first then create
                    with open(file_path, 'r', encoding='utf-8') as f:
                        sql_content = f.read()
                    
                    # Generate DROP TRIGGER statement
                    drop_sql = f"DROP TRIGGER IF EXISTS [{schema_name}].[{object_name}];\n"
                    modified_sql = drop_sql + sql_content
                    
                    # Write to temporary file
                    temp_file = file_path.parent / f"temp_{file_path.name}"
                    with open(temp_file, 'w', encoding='utf-8') as f:
                        f.write(modified_sql)
                    
                    success = self.execute_sql_file(temp_file, description)
                    temp_file.unlink()  # Clean up temp file
                    
                    if success:
                        total_processed += 1
                        logger.info(f"Successfully recreated trigger {description}")
                    else:
                        logger.error(f"Failed to recreate trigger {description}")
                else:
                    # For new objects, use original CREATE
                    if self.execute_sql_file(file_path, description):
                        total_processed += 1
                        logger.info(f"Successfully imported {description}")
                    else:
                        logger.error(f"Failed to import {description}")
                        # Continue with other objects even if one fails
        
        # Print summary
        if skipped_identical > 0:
            logger.info(f"Import summary: {total_processed} objects imported, {skipped_identical} identical objects skipped")
        else:
            logger.info(f"Import summary: {total_processed} objects imported")
    
    def import_table_data_all(self, exported_files: Dict[str, List[Dict]]):
        """Import data for all tables."""
        data_format = self.config.get('data_format', 'sql')  # 'sql' or 'binary'
        
        if data_format == 'binary':
            data_dir = self.binary_data_dir
            file_extension = '.pkl.gz'
            logger.info("\n=== Importing Table Data (Binary Format) ===")
        else:
            data_dir = self.data_dir
            file_extension = '.sql'
            logger.info("\n=== Importing Table Data (SQL Format) ===")
        
        if not data_dir.exists():
            logger.info(f"No {data_format} data directory found, skipping data import")
            return
        
        for table_obj in exported_files['tables']:
            schema_name = table_obj['schema']
            table_name = table_obj['name']
            data_file = data_dir / f"{schema_name}.{table_name}{file_extension}"
            
            if data_file.exists():
                if not self.ask_confirmation(f"Import data for table {schema_name}.{table_name}?"):
                    logger.info(f"Skipped data import for {schema_name}.{table_name}")
                    continue
                
                if data_format == 'binary':
                    self.import_table_data_binary(schema_name, table_name, data_file)
                else:
                    self.import_table_data(schema_name, table_name, data_file)
            else:
                logger.info(f"No {data_format} data file found for {schema_name}.{table_name}")
    
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
    parser.add_argument('--show-dependencies', action='store_true', help='Show dependency analysis and exit')
    
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
        
        # Show dependencies and exit if requested
        if args.show_dependencies:
            if not importer.connect():
                sys.exit(1)
            try:
                exported_files = importer.load_exported_files()
                if not any(exported_files.values()):
                    logger.error("No exported files found to analyze")
                    sys.exit(1)
                
                analyzer = DependencyAnalyzer()
                analyzer.show_dependency_info(exported_files)
                sys.exit(0)
            finally:
                importer.disconnect()
        
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
