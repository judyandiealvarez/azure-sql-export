# Config-driven migration generator consistent with other commands
import os
import sys
import json
import yaml
import argparse
import pyodbc
from datetime import datetime
from typing import Dict, List, DefaultDict
from collections import defaultdict
import difflib

OBJECT_QUERIES = {
    'Tables': """
        SELECT t.name, m.definition
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        CROSS APPLY (SELECT OBJECT_DEFINITION(t.object_id) AS definition) m
        WHERE s.name = ?
    """,
    'Views': """
        SELECT v.name, m.definition
        FROM sys.views v
        JOIN sys.schemas s ON v.schema_id = s.schema_id
        CROSS APPLY (SELECT OBJECT_DEFINITION(v.object_id) AS definition) m
        WHERE s.name = ?
    """,
    'StoredProcedures': """
        SELECT p.name, m.definition
        FROM sys.procedures p
        JOIN sys.schemas s ON p.schema_id = s.schema_id
        CROSS APPLY (SELECT OBJECT_DEFINITION(p.object_id) AS definition) m
        WHERE s.name = ?
    """,
    'Functions': """
        SELECT f.name, m.definition
        FROM sys.objects f
        JOIN sys.schemas s ON f.schema_id = s.schema_id
        CROSS APPLY (SELECT OBJECT_DEFINITION(f.object_id) AS definition) m
        WHERE s.name = ? AND f.type IN ('FN','TF','IF')
    """,
    'Triggers': """
        SELECT tr.name, m.definition
        FROM sys.triggers tr
        JOIN sys.objects o ON tr.parent_id = o.object_id
        JOIN sys.schemas s ON o.schema_id = s.schema_id
        CROSS APPLY (SELECT OBJECT_DEFINITION(tr.object_id) AS definition) m
        WHERE s.name = ?
    """
}


def _load_config(config_path: str) -> Dict:
    with open(config_path, 'r', encoding='utf-8') as f:
        if config_path.endswith(('.yaml', '.yml')):
            return yaml.safe_load(f)
        return json.load(f)


def _build_conn_str(config: Dict) -> str:
    driver = config.get('driver', 'ODBC Driver 17 for SQL Server')
    server = config['server']
    database = config['database']
    auth_type = config.get('authentication_type', 'sql')

    if auth_type == 'azure_ad':
        return (
            "DRIVER={" + driver + "};" +
            "SERVER=" + server + ";" +
            "DATABASE=" + database + ";" +
            "Authentication=ActiveDirectoryDefault;"
        )

    username = config.get('username') or config.get('user') or config.get('uid')
    password = config.get('password') or config.get('pwd')
    if not username or not password:
        raise SystemExit('Missing username/password for SQL authentication in config')

    return (
        "DRIVER={" + driver + "};" +
        "SERVER=" + server + ";" +
        "PORT=1433;" +
        "DATABASE=" + database + ";" +
        "UID=" + str(username) + ";" +
        "PWD=" + str(password)
    )


def get_db_objects(cursor, obj_type: str, schema_name: str):
    cursor.execute(OBJECT_QUERIES[obj_type], schema_name)
    return {row.name: row.definition for row in cursor.fetchall() if row.definition}


def get_file_objects(folder: str):
    if not os.path.exists(folder):
        return {}
    result = {}
    for f in os.listdir(folder):
        if f.endswith('.sql'):
            with open(os.path.join(folder, f), encoding='utf-8') as file:
                result[os.path.splitext(f)[0]] = file.read()
    return result


def _normalize_newlines(sql: str) -> str:
    if sql is None:
        return ""
    return sql.replace('\r\n', '\n').replace('\r', '\n')


def _same_definition(file_def: str, db_def: str) -> bool:
    return _normalize_newlines(file_def) == _normalize_newlines(db_def)


def generate_migration(config: Dict, sql_schema_dir: str, migrations_dir: str, schema_name: str, debug_diff: int = 0):
    conn_str = _build_conn_str(config)
    migration_sql: List[str] = []
    # Summary buckets
    created: DefaultDict[str, List[str]] = defaultdict(list)
    updated: DefaultDict[str, List[str]] = defaultdict(list)
    dropped: DefaultDict[str, List[str]] = defaultdict(list)

    debug_shown = 0

    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        for obj_type in OBJECT_QUERIES:
            db_objs = get_db_objects(cursor, obj_type, schema_name)
            folder_name = obj_type if obj_type != 'StoredProcedures' else 'Stored Procedures'
            folder = os.path.join(sql_schema_dir, folder_name)
            file_objs = get_file_objects(folder)

            # Find objects to create or alter
            for name, db_def in db_objs.items():
                file_def = file_objs.get(name)
                if file_def is None:
                    created[obj_type].append(name)
                    migration_sql.append(f"-- Create {obj_type[:-1]}: {name}\n{db_def}\nGO\n")
                else:
                    if not _same_definition(file_def, db_def):
                        updated[obj_type].append(name)
                        migration_sql.append(f"-- Update {obj_type[:-1]}: {name}\n{db_def}\nGO\n")
                        if debug_shown < debug_diff:
                            debug_shown += 1
                            print(f"[DEBUG] Diff for {obj_type[:-1]}: {name}")
                            file_lines = _normalize_newlines(file_def).splitlines()
                            db_lines = _normalize_newlines(db_def).splitlines()
                            for line in difflib.unified_diff(file_lines, db_lines, fromfile='file', tofile='db', lineterm=''):
                                print(line)

            # Find objects to drop (in files but not in DB)
            for name, _ in file_objs.items():
                if name not in db_objs:
                    dropped[obj_type].append(name)
                    if obj_type == 'Tables':
                        migration_sql.append(f"DROP TABLE [{schema_name}].[{name}];\n")
                    elif obj_type == 'Views':
                        migration_sql.append(f"DROP VIEW [{schema_name}].[{name}];\n")
                    elif obj_type == 'StoredProcedures':
                        migration_sql.append(f"DROP PROCEDURE [{schema_name}].[{name}];\n")
                    elif obj_type == 'Functions':
                        migration_sql.append(f"DROP FUNCTION [{schema_name}].[{name}];\n")
                    elif obj_type == 'Triggers':
                        migration_sql.append(f"DROP TRIGGER [{schema_name}].[{name}];\n")

    if migration_sql:
        os.makedirs(migrations_dir, exist_ok=True)
        existing = [f for f in os.listdir(migrations_dir) if f.startswith('update') and f.endswith('.sql')]
        nums = [int(f[6:10]) for f in existing if f[6:10].isdigit()]
        next_num = max(nums, default=0) + 1
        filename = f"update{next_num:04d}.sql"
        outfile = os.path.join(migrations_dir, filename)
        with open(outfile, 'w', encoding='utf-8') as f:
            # Counts table
            f.write('-- Summary\n')
            f.write(f"-- Schema: {schema_name}\n")
            types_order = ['Tables', 'Views', 'StoredProcedures', 'Functions', 'Triggers']
            f.write('-- | Type | Created | Updated | Dropped |\n')
            f.write('-- |------|---------:|---------:|---------:|\n')
            for typ in types_order:
                c = len(created.get(typ, []))
                u = len(updated.get(typ, []))
                d = len(dropped.get(typ, []))
                f.write(f"-- | {typ} | {c} | {u} | {d} |\n")
            f.write('--\n')
            # Flat detailed table: one row per object
            f.write('-- Details\n')
            f.write('-- | Change | Type | Object |\n')
            f.write('-- |--------|------|--------|\n')
            for typ in types_order:
                for name in sorted(created.get(typ, [])):
                    f.write(f"-- | Created | {typ} | {name} |\n")
                for name in sorted(updated.get(typ, [])):
                    f.write(f"-- | Updated | {typ} | {name} |\n")
                for name in sorted(dropped.get(typ, [])):
                    f.write(f"-- | Dropped | {typ} | {name} |\n")
            f.write(f"-- Generated at {datetime.utcnow().isoformat()}Z\n\n")
            # SQL body
            f.write('\n'.join(migration_sql))
        print(f"Migration written to {outfile}")
    else:
        print("No changes detected. Migration not created.")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description='Generate SQL migrations by comparing DB to local files')
    parser.add_argument('-c', '--config', default='config.yaml', help='Path to YAML/JSON config (default: config.yaml)')
    parser.add_argument('--schema-name', help='Schema name (e.g., dbo). Overrides config.migrate.schema_name or top-level schema/schema_name')
    parser.add_argument('--sql-schema-dir', help='Local schema root directory. Overrides config.migrate.sql_schema_dir or top-level sql_schema_dir')
    parser.add_argument('--migrations-dir', help='Output migrations directory. Overrides config.migrate.migrations_dir or top-level migrations_dir')
    parser.add_argument('--debug-diff', type=int, default=0, help='Print unified diffs for first N mismatches')
    args = parser.parse_args(argv)

    if not os.path.exists(args.config):
        raise SystemExit(f"Config file not found: {args.config}")
    config = _load_config(args.config)

    migrate_cfg = (config.get('migrate') or {}) if isinstance(config, dict) else {}

    # Resolve with precedence: CLI > migrate.* > top-level keys > defaults
    schema_name = (
        args.schema_name or
        migrate_cfg.get('schema_name') or
        config.get('schema_name') or
        config.get('schema')
    )
    if not schema_name:
        raise SystemExit('schema_name must be provided via --schema-name or config (migrate.schema_name or top-level schema/schema_name)')

    sql_schema_dir = (
        args.sql_schema_dir or
        migrate_cfg.get('sql_schema_dir') or
        config.get('sql_schema_dir') or
        os.path.join('sql', 'schema')
    )
    migrations_dir = (
        args.migrations_dir or
        migrate_cfg.get('migrations_dir') or
        config.get('migrations_dir') or
        os.path.join('sql', 'migrations')
    )

    generate_migration(config=config,
                       sql_schema_dir=sql_schema_dir,
                       migrations_dir=migrations_dir,
                       schema_name=schema_name,
                       debug_diff=args.debug_diff)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
