import os
import sys
import json
import yaml
import argparse
import pyodbc
from datetime import datetime

SQL_SCHEMA_DIR = os.path.join('sql', 'dna', 'BPG_FinOps_Invoice_Reimbursement')
MIGRATIONS_DIR = os.path.join('sql', 'migrations')

# Require config to provide these; no hardcoded defaults
SERVER = None
DATABASE = None
USERNAME = None
PASSWORD = None
DRIVER = None
SCHEMA_NAME = None

def _build_queries(schema_name: str):
    return {
        'Tables': f"""
            SELECT t.name, OBJECT_DEFINITION(t.object_id) AS definition
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = '{schema_name}'
        """,
        'Views': f"""
            SELECT v.name, OBJECT_DEFINITION(v.object_id) AS definition
            FROM sys.views v
            JOIN sys.schemas s ON v.schema_id = s.schema_id
            WHERE s.name = '{schema_name}'
        """,
        'StoredProcedures': f"""
            SELECT p.name, OBJECT_DEFINITION(p.object_id) AS definition
            FROM sys.procedures p
            JOIN sys.schemas s ON p.schema_id = s.schema_id
            WHERE s.name = '{schema_name}'
        """,
        'Functions': f"""
            SELECT f.name, OBJECT_DEFINITION(f.object_id) AS definition
            FROM sys.objects f
            JOIN sys.schemas s ON f.schema_id = s.schema_id
            WHERE s.name = '{schema_name}' AND f.type IN ('FN','TF','IF')
        """,
        'Triggers': f"""
            SELECT tr.name, OBJECT_DEFINITION(tr.object_id) AS definition
            FROM sys.triggers tr
            JOIN sys.objects o ON tr.parent_id = o.object_id
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE s.name = '{schema_name}'
        """
    }

OBJECT_QUERIES = None

def get_db_objects(cursor, obj_type):
    cursor.execute(OBJECT_QUERIES[obj_type])
    return {row.name: row.definition for row in cursor.fetchall() if row.definition}

def get_file_objects(folder):
    if not os.path.exists(folder):
        return {}
    result = {}
    for f in os.listdir(folder):
        if f.endswith('.sql'):
            with open(os.path.join(folder, f), encoding='utf-8') as file:
                result[os.path.splitext(f)[0]] = file.read()
    return result

def generate_migration():
    conn_str = f'DRIVER={{{DRIVER}}};SERVER={SERVER};PORT=1433;DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};TDS_Version=8.0'
    migration_sql = []



    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        for obj_type in OBJECT_QUERIES:
            db_objs = get_db_objects(cursor, obj_type)
            folder_name = obj_type if obj_type != 'StoredProcedures' else 'Stored Procedures'
            folder = os.path.join(SQL_SCHEMA_DIR, folder_name)
            file_objs = get_file_objects(folder)

            # Find objects to create or alter (bring files up to DB state)
            for name, db_def in db_objs.items():
                file_def = file_objs.get(name)
                if file_def is None:
                    print(f"[MIGRATION] {obj_type} '{name}' exists in DB but not in files. Will CREATE.")
                    migration_sql.append(f"-- Create {obj_type[:-1]}: {name}\n{db_def}\nGO\n")
                else:
                    def norm(s):
                        return '\n'.join(line.rstrip() for line in s.replace('\r\n', '\n').replace('\r', '\n').split('\n')).strip()
                    if norm(file_def) != norm(db_def):
                        print(f"[MIGRATION] {obj_type} '{name}' differs between DB and file. Will UPDATE.")
                        print('--- FILE DEF ---')
                        print(file_def)
                        print('--- DB DEF ---')
                        print(db_def)
                        import difflib
                        diff = difflib.unified_diff(
                            file_def.splitlines(), db_def.splitlines(),
                            fromfile='file', tofile='db', lineterm='')
                        print('--- UNIFIED DIFF ---')
                        print('\n'.join(list(diff)))
                        migration_sql.append(f"-- Update {obj_type[:-1]}: {name}\n{db_def}\nGO\n")

            # Find objects to drop (in files but not in DB)
            for name, file_def in file_objs.items():
                if name not in db_objs:
                    print(f"[MIGRATION] {obj_type} '{name}' exists in files but not in DB. Will DROP.")
                    if obj_type == 'Tables':
                        migration_sql.append(f"DROP TABLE [BPG_FinOps_Invoice_Reimbursement].[{name}];\n")
                    elif obj_type == 'Views':
                        migration_sql.append(f"DROP VIEW [BPG_FinOps_Invoice_Reimbursement].[{name}];\n")
                    elif obj_type == 'StoredProcedures':
                        migration_sql.append(f"DROP PROCEDURE [BPG_FinOps_Invoice_Reimbursement].[{name}];\n")
                    elif obj_type == 'Functions':
                        migration_sql.append(f"DROP FUNCTION [BPG_FinOps_Invoice_Reimbursement].[{name}];\n")
                    elif obj_type == 'Triggers':
                        migration_sql.append(f"DROP TRIGGER [BPG_FinOps_Invoice_Reimbursement].[{name}];\n")

    if migration_sql:
        os.makedirs(MIGRATIONS_DIR, exist_ok=True)
        # Find next migration number
        existing = [f for f in os.listdir(MIGRATIONS_DIR) if f.startswith('update') and f.endswith('.sql')]
        nums = [int(f[6:10]) for f in existing if f[6:10].isdigit()]
        next_num = max(nums, default=0) + 1
        filename = f"update{next_num:04d}.sql"
        path = os.path.join(MIGRATIONS_DIR, filename)
        with open(path, 'w', encoding='utf-8') as f:
            f.writelines(migration_sql)
        print(f"Migration generated: {path}")
    else:
        print("No migration needed. DB and files are in sync.")

if __name__ == '__main__':
    # Minimal CLI to load config; no hardcoded connection defaults
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-c', '--config', default='config.yaml')
    parser.add_argument('--schema-name')
    parser.add_argument('--sql-schema-dir')
    parser.add_argument('--migrations-dir')
    args, _ = parser.parse_known_args()

    if not os.path.exists(args.config):
        raise SystemExit(f"Config file not found: {args.config}")

    with open(args.config, 'r', encoding='utf-8') as f:
        cfg = yaml.safe_load(f) if args.config.endswith(('.yaml', '.yml')) else json.load(f)

    SERVER = cfg.get('server')
    DATABASE = cfg.get('database')
    USERNAME = cfg.get('username') or cfg.get('user') or cfg.get('uid')
    PASSWORD = cfg.get('password') or cfg.get('pwd')
    DRIVER = cfg.get('driver')
    SCHEMA_NAME = args.schema_name or (cfg.get('migrate') or {}).get('schema_name') or cfg.get('schema_name') or cfg.get('schema')

    if not all([SERVER, DATABASE, USERNAME, PASSWORD, DRIVER, SCHEMA_NAME]):
        raise SystemExit('Missing required config values (server, database, username, password, driver, schema_name/schema)')

    SQL_SCHEMA_DIR = args.sql_schema_dir or (cfg.get('migrate') or {}).get('sql_schema_dir') or cfg.get('sql_schema_dir', SQL_SCHEMA_DIR)
    MIGRATIONS_DIR = args.migrations_dir or (cfg.get('migrate') or {}).get('migrations_dir') or cfg.get('migrations_dir', MIGRATIONS_DIR)
    os.makedirs(SQL_SCHEMA_DIR, exist_ok=True)
    os.makedirs(MIGRATIONS_DIR, exist_ok=True)
    # Rebuild queries with provided schema
    OBJECT_QUERIES = _build_queries(SCHEMA_NAME)

    generate_migration()
