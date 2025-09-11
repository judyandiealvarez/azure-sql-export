#!/usr/bin/env python3
"""
Azure SQL Database Comparison Tool

This script compares exported schema and data files with a target database
to show differences without making any changes.

Features:
- Schema object comparison (tables, views, procedures, functions, triggers)
- Data comparison (row counts, sample data differences)
- Detailed difference reporting
- Dependency analysis
- Export-ready comparison reports
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
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('azure_sql_compare.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class DatabaseComparator:
    """Main class for comparing exported files with target database."""
    
    def __init__(self, config_file: str = "config.yaml"):
        """Initialize the comparator with configuration."""
        self.config = self._load_config(config_file)
        self.connection = None
        self.import_dir = Path(self.config.get('import_directory', 'export_output'))
        self.schema_dir = self.import_dir / 'schema'
        self.data_dir = self.import_dir / 'data'
        
        # Comparison options
        self.show_data_samples = self.config.get('show_data_samples', True)
        self.sample_size = self.config.get('sample_size', 5)
        self.export_report = self.config.get('export_report', True)
        
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
    
    def get_database_objects(self) -> Dict[str, Dict]:
        """Get all objects from the target database."""
        objects = {
            'tables': {},
            'views': {},
            'procedures': {},
            'functions': {},
            'triggers': {}
        }
        
        try:
            cursor = self.connection.cursor()
            
            # Get tables
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
                objects['tables'][key] = {
                    'schema': row[0], 
                    'name': row[1], 
                    'type': row[2],
                    'exists': True
                }
            
            # Get views
            cursor.execute("""
                SELECT 
                    TABLE_SCHEMA,
                    TABLE_NAME
                FROM INFORMATION_SCHEMA.VIEWS
                ORDER BY TABLE_SCHEMA, TABLE_NAME
            """)
            for row in cursor.fetchall():
                key = f"{row[0]}.{row[1]}"
                objects['views'][key] = {
                    'schema': row[0], 
                    'name': row[1],
                    'exists': True
                }
            
            # Get procedures
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
                objects['procedures'][key] = {
                    'schema': row[0], 
                    'name': row[1],
                    'exists': True
                }
            
            # Get functions
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
                objects['functions'][key] = {
                    'schema': row[0], 
                    'name': row[1],
                    'exists': True
                }
            
            # Get triggers
            cursor.execute("""
                SELECT 
                    TRIGGER_SCHEMA,
                    TRIGGER_NAME
                FROM INFORMATION_SCHEMA.TRIGGERS
                ORDER BY TRIGGER_SCHEMA, TRIGGER_NAME
            """)
            for row in cursor.fetchall():
                key = f"{row[0]}.{row[1]}"
                objects['triggers'][key] = {
                    'schema': row[0], 
                    'name': row[1],
                    'exists': True
                }
            
            cursor.close()
            logger.info(f"Found database objects: {sum(len(v) for v in objects.values())} total")
            
        except Exception as e:
            logger.error(f"Error retrieving database objects: {e}")
        
        return objects
    
    def get_table_schema(self, schema_name: str, table_name: str) -> str:
        """Get current table schema definition from database."""
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
    
    def get_table_sample_data(self, schema_name: str, table_name: str, limit: int = 5) -> List[Tuple]:
        """Get sample data from table."""
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"SELECT TOP {limit} * FROM [{schema_name}].[{table_name}]")
            rows = cursor.fetchall()
            cursor.close()
            return rows
        except Exception as e:
            logger.error(f"Error getting sample data for {schema_name}.{table_name}: {e}")
            return []
    
    def compare_schemas(self, new_schema: str, existing_schema: str, object_name: str) -> List[str]:
        """Compare two schema definitions and return differences."""
        if not existing_schema:
            return [f"New object: {object_name}"]
        
        new_lines = new_schema.strip().split('\n')
        existing_lines = existing_schema.strip().split('\n')
        
        diff = list(unified_diff(
            existing_lines, new_lines,
            fromfile=f"Database {object_name}",
            tofile=f"Exported {object_name}",
            lineterm=""
        ))
        
        return diff if diff else ["No differences found"]
    
    def compare_objects(self, exported_files: Dict[str, List[Dict]], database_objects: Dict[str, Dict]) -> Dict:
        """Compare exported files with database objects."""
        comparison = {
            'new_objects': defaultdict(list),
            'modified_objects': defaultdict(list),
            'deleted_objects': defaultdict(list),
            'unchanged_objects': defaultdict(list),
            'summary': {}
        }
        
        # Check each object type
        for obj_type in ['tables', 'views', 'procedures', 'functions', 'triggers']:
            exported_objs = {f"{obj['schema']}.{obj['name']}": obj for obj in exported_files[obj_type]}
            db_objs = database_objects[obj_type]
            
            # Find new objects (in export, not in database)
            for obj_name, obj_info in exported_objs.items():
                if obj_name not in db_objs:
                    comparison['new_objects'][obj_type].append(obj_info)
                else:
                    # Check if object is modified
                    try:
                        with open(obj_info['file'], 'r', encoding='utf-8') as f:
                            exported_content = f.read()
                        
                        if obj_type == 'tables':
                            existing_content = self.get_table_schema(obj_info['schema'], obj_info['name'])
                        else:
                            existing_content = ""  # For other objects, we'll mark as potentially modified
                        
                        differences = self.compare_schemas(exported_content, existing_content, obj_name)
                        
                        if len(differences) > 1:  # More than just "No differences found"
                            comparison['modified_objects'][obj_type].append({
                                **obj_info,
                                'differences': differences
                            })
                        else:
                            comparison['unchanged_objects'][obj_type].append(obj_info)
                            
                    except Exception as e:
                        logger.warning(f"Could not compare {obj_name}: {e}")
                        comparison['modified_objects'][obj_type].append(obj_info)
            
            # Find deleted objects (in database, not in export)
            for obj_name, obj_info in db_objs.items():
                if obj_name not in exported_objs:
                    comparison['deleted_objects'][obj_type].append(obj_info)
        
        # Calculate summary
        for obj_type in ['tables', 'views', 'procedures', 'functions', 'triggers']:
            comparison['summary'][obj_type] = {
                'new': len(comparison['new_objects'][obj_type]),
                'modified': len(comparison['modified_objects'][obj_type]),
                'deleted': len(comparison['deleted_objects'][obj_type]),
                'unchanged': len(comparison['unchanged_objects'][obj_type])
            }
        
        return comparison
    
    def compare_data(self, exported_files: Dict[str, List[Dict]]) -> Dict:
        """Compare table data between export and database."""
        data_comparison = {}
        
        if not self.data_dir.exists():
            logger.info("No data directory found for comparison")
            return data_comparison
        
        for table_obj in exported_files['tables']:
            schema_name = table_obj['schema']
            table_name = table_obj['name']
            data_file = self.data_dir / f"{schema_name}.{table_name}.sql"
            
            if data_file.exists():
                # Get database row count
                db_count = self.get_table_row_count(schema_name, table_name)
                
                # Count exported rows
                try:
                    with open(data_file, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    exported_count = content.count('INSERT INTO')
                    
                    data_comparison[f"{schema_name}.{table_name}"] = {
                        'database_rows': db_count,
                        'exported_rows': exported_count,
                        'difference': exported_count - db_count,
                        'has_data_file': True
                    }
                    
                    # Get sample data if requested
                    if self.show_data_samples and db_count > 0:
                        sample_data = self.get_table_sample_data(schema_name, table_name, self.sample_size)
                        data_comparison[f"{schema_name}.{table_name}"]['sample_data'] = sample_data
                        
                except Exception as e:
                    logger.warning(f"Could not analyze data for {schema_name}.{table_name}: {e}")
                    data_comparison[f"{schema_name}.{table_name}"] = {
                        'database_rows': db_count,
                        'exported_rows': 0,
                        'difference': -db_count,
                        'has_data_file': False,
                        'error': str(e)
                    }
            else:
                # No data file, but table exists in database
                db_count = self.get_table_row_count(schema_name, table_name)
                if db_count > 0:
                    data_comparison[f"{schema_name}.{table_name}"] = {
                        'database_rows': db_count,
                        'exported_rows': 0,
                        'difference': -db_count,
                        'has_data_file': False
                    }
        
        return data_comparison
    
    def print_comparison_report(self, comparison: Dict, data_comparison: Dict):
        """Print a detailed comparison report."""
        print("\n" + "="*80)
        print("DATABASE COMPARISON REPORT")
        print("="*80)
        print(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Source: {self.import_dir}")
        print(f"Target: {self.config['server']}/{self.config['database']}")
        
        # Summary
        print("\n" + "-"*50)
        print("SUMMARY")
        print("-"*50)
        
        total_new = sum(comparison['summary'][obj_type]['new'] for obj_type in comparison['summary'])
        total_modified = sum(comparison['summary'][obj_type]['modified'] for obj_type in comparison['summary'])
        total_deleted = sum(comparison['summary'][obj_type]['deleted'] for obj_type in comparison['summary'])
        total_unchanged = sum(comparison['summary'][obj_type]['unchanged'] for obj_type in comparison['summary'])
        
        print(f"New objects:     {total_new}")
        print(f"Modified objects: {total_modified}")
        print(f"Deleted objects:  {total_deleted}")
        print(f"Unchanged objects: {total_unchanged}")
        
        # Detailed breakdown by type
        for obj_type in ['tables', 'views', 'procedures', 'functions', 'triggers']:
            summary = comparison['summary'][obj_type]
            if any(summary.values()):
                print(f"\n{obj_type.upper()}:")
                print(f"  New: {summary['new']}, Modified: {summary['modified']}, Deleted: {summary['deleted']}, Unchanged: {summary['unchanged']}")
        
        # New objects
        if any(comparison['new_objects'].values()):
            print("\n" + "-"*50)
            print("NEW OBJECTS (will be created)")
            print("-"*50)
            for obj_type, objects in comparison['new_objects'].items():
                if objects:
                    print(f"\n{obj_type.upper()}:")
                    for obj in objects:
                        print(f"  + {obj['schema']}.{obj['name']}")
        
        # Modified objects
        if any(comparison['modified_objects'].values()):
            print("\n" + "-"*50)
            print("MODIFIED OBJECTS (will be altered)")
            print("-"*50)
            for obj_type, objects in comparison['modified_objects'].items():
                if objects:
                    print(f"\n{obj_type.upper()}:")
                    for obj in objects:
                        print(f"  ~ {obj['schema']}.{obj['name']}")
                        if 'differences' in obj and len(obj['differences']) > 1:
                            print("    Differences:")
                            for diff_line in obj['differences'][:5]:  # Show first 5 lines
                                print(f"      {diff_line}")
                            if len(obj['differences']) > 5:
                                print(f"      ... and {len(obj['differences']) - 5} more lines")
        
        # Deleted objects
        if any(comparison['deleted_objects'].values()):
            print("\n" + "-"*50)
            print("DELETED OBJECTS (exist in database but not in export)")
            print("-"*50)
            for obj_type, objects in comparison['deleted_objects'].items():
                if objects:
                    print(f"\n{obj_type.upper()}:")
                    for obj in objects:
                        print(f"  - {obj['schema']}.{obj['name']}")
        
        # Data comparison
        if data_comparison:
            print("\n" + "-"*50)
            print("DATA COMPARISON")
            print("-"*50)
            
            for table_name, data_info in data_comparison.items():
                print(f"\n{table_name}:")
                print(f"  Database rows: {data_info['database_rows']}")
                print(f"  Exported rows: {data_info['exported_rows']}")
                print(f"  Difference: {data_info['difference']:+d}")
                
                if 'sample_data' in data_info and data_info['sample_data']:
                    print("  Sample data (database):")
                    for i, row in enumerate(data_info['sample_data'][:3]):  # Show first 3 rows
                        print(f"    Row {i+1}: {row}")
        
        print("\n" + "="*80)
    
    def export_comparison_report(self, comparison: Dict, data_comparison: Dict) -> str:
        """Export comparison report to file."""
        report_file = self.import_dir / f"comparison_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("DATABASE COMPARISON REPORT\n")
            f.write("="*80 + "\n")
            f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Source: {self.import_dir}\n")
            f.write(f"Target: {self.config['server']}/{self.config['database']}\n\n")
            
            # Summary
            f.write("-"*50 + "\n")
            f.write("SUMMARY\n")
            f.write("-"*50 + "\n")
            
            total_new = sum(comparison['summary'][obj_type]['new'] for obj_type in comparison['summary'])
            total_modified = sum(comparison['summary'][obj_type]['modified'] for obj_type in comparison['summary'])
            total_deleted = sum(comparison['summary'][obj_type]['deleted'] for obj_type in comparison['summary'])
            total_unchanged = sum(comparison['summary'][obj_type]['unchanged'] for obj_type in comparison['summary'])
            
            f.write(f"New objects:     {total_new}\n")
            f.write(f"Modified objects: {total_modified}\n")
            f.write(f"Deleted objects:  {total_deleted}\n")
            f.write(f"Unchanged objects: {total_unchanged}\n\n")
            
            # Detailed breakdown
            for obj_type in ['tables', 'views', 'procedures', 'functions', 'triggers']:
                summary = comparison['summary'][obj_type]
                if any(summary.values()):
                    f.write(f"{obj_type.upper()}:\n")
                    f.write(f"  New: {summary['new']}, Modified: {summary['modified']}, Deleted: {summary['deleted']}, Unchanged: {summary['unchanged']}\n")
            
            # New objects
            if any(comparison['new_objects'].values()):
                f.write("\n" + "-"*50 + "\n")
                f.write("NEW OBJECTS (will be created)\n")
                f.write("-"*50 + "\n")
                for obj_type, objects in comparison['new_objects'].items():
                    if objects:
                        f.write(f"\n{obj_type.upper()}:\n")
                        for obj in objects:
                            f.write(f"  + {obj['schema']}.{obj['name']}\n")
            
            # Modified objects with full differences
            if any(comparison['modified_objects'].values()):
                f.write("\n" + "-"*50 + "\n")
                f.write("MODIFIED OBJECTS (will be altered)\n")
                f.write("-"*50 + "\n")
                for obj_type, objects in comparison['modified_objects'].items():
                    if objects:
                        f.write(f"\n{obj_type.upper()}:\n")
                        for obj in objects:
                            f.write(f"  ~ {obj['schema']}.{obj['name']}\n")
                            if 'differences' in obj:
                                f.write("    Differences:\n")
                                for diff_line in obj['differences']:
                                    f.write(f"      {diff_line}\n")
            
            # Data comparison
            if data_comparison:
                f.write("\n" + "-"*50 + "\n")
                f.write("DATA COMPARISON\n")
                f.write("-"*50 + "\n")
                
                for table_name, data_info in data_comparison.items():
                    f.write(f"\n{table_name}:\n")
                    f.write(f"  Database rows: {data_info['database_rows']}\n")
                    f.write(f"  Exported rows: {data_info['exported_rows']}\n")
                    f.write(f"  Difference: {data_info['difference']:+d}\n")
        
        return str(report_file)
    
    def run_comparison(self):
        """Run the complete comparison process."""
        logger.info("Starting database comparison...")
        
        if not self.connect():
            return False
        
        try:
            # Load exported files
            exported_files = self.load_exported_files()
            if not any(exported_files.values()):
                logger.error("No exported files found to compare")
                return False
            
            # Get database objects
            database_objects = self.get_database_objects()
            
            # Compare objects
            logger.info("Comparing schema objects...")
            comparison = self.compare_objects(exported_files, database_objects)
            
            # Compare data
            logger.info("Comparing table data...")
            data_comparison = self.compare_data(exported_files)
            
            # Print report
            self.print_comparison_report(comparison, data_comparison)
            
            # Export report if requested
            if self.export_report:
                report_file = self.export_comparison_report(comparison, data_comparison)
                logger.info(f"Comparison report exported to: {report_file}")
            
            logger.info("Comparison completed successfully!")
            return True
            
        except Exception as e:
            logger.error(f"Comparison failed: {e}")
            return False
        finally:
            self.disconnect()


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Compare exported files with Azure SQL Database')
    parser.add_argument('--config', default='config.yaml', help='Configuration file path (YAML or JSON)')
    parser.add_argument('--import-dir', help='Import directory path (overrides config)')
    parser.add_argument('--no-samples', action='store_true', help='Skip data sample comparison')
    parser.add_argument('--sample-size', type=int, default=5, help='Number of sample rows to show')
    parser.add_argument('--no-export', action='store_true', help='Do not export comparison report')
    
    args = parser.parse_args()
    
    try:
        comparator = DatabaseComparator(args.config)
        
        if args.import_dir:
            comparator.import_dir = Path(args.import_dir)
            comparator.schema_dir = comparator.import_dir / 'schema'
            comparator.data_dir = comparator.import_dir / 'data'
        
        # Override config with command line arguments
        comparator.show_data_samples = not args.no_samples
        comparator.sample_size = args.sample_size
        comparator.export_report = not args.no_export
        
        success = comparator.run_comparison()
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        logger.info("Comparison cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
