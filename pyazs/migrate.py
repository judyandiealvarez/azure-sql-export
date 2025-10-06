# Config-driven migration generator consistent with other commands
import os
import sys
import json
import yaml
import argparse
import pytds
import certifi
from datetime import datetime
from typing import Dict, List, DefaultDict
import re
from collections import defaultdict
try:
    from .common import get_db_objects, OBJECT_QUERIES, write_definition_to_file
except ImportError:
    from common import get_db_objects, OBJECT_QUERIES, write_definition_to_file





def _load_config(config_path: str) -> Dict:
    with open(config_path, 'r', encoding='utf-8', newline='') as f:
        if config_path.endswith(('.yaml', '.yml')):
            return yaml.safe_load(f)
        return json.load(f)
## IMPORTANT: exact comparison only; no normalization

def _first_diff(a: str, b: str) -> int:
    limit = min(len(a), len(b))
    for i in range(limit):
        if a[i] != b[i]:
            return i
    return limit if len(a) != len(b) else -1

def _definition_for_update(obj_type: str, definition: str) -> str:
    """Return DDL suitable for updating an existing object.
    For views/procedures/functions/triggers emit CREATE OR ALTER to avoid DROP/CREATE.
    Tables are left as-is (table diffs not auto-altered here).
    """
    if obj_type in ("Views", "StoredProcedures", "Functions", "Triggers"):
        # Convert the first line that declares the object from CREATE[*] <type> to ALTER <type>
        # Preserve leading whitespace and original <type> casing/spaces. Allow comments/blank lines before header.
        # 1) Try at file start (after optional BOM/whitespace)
        start_pattern = re.compile(r"^\ufeff?(\s*)create(?:\s+or\s+alter)?(\s+)(view|procedure|function|trigger)\b",
                                   re.IGNORECASE)
        def repl(m: re.Match) -> str:
            return f"{m.group(1)}ALTER{m.group(2)}{m.group(3)}"
        new_def, n = start_pattern.subn(repl, definition, count=1)
        if n:
            return new_def
        # 2) Fallback: convert the first line beginning with CREATE <type> anywhere (skip comments not guaranteed)
        line_pattern = re.compile(r"(?m)^(\s*)create(?:\s+or\s+alter)?(\s+)(view|procedure|function|trigger)\b",
                                  re.IGNORECASE)
        new_def, n = line_pattern.subn(repl, definition, count=1)
        return new_def if n else definition
    return definition


def _build_conn_params(config: Dict) -> Dict:
    server = config['server']
    database = config['database']
    username = config.get('username') or config.get('user') or config.get('uid')
    password = config.get('password') or config.get('pwd')
    if not username or not password:
        raise SystemExit('Missing username/password for SQL authentication in config')
    return {
        'server': server,
        'database': database,
        'user': str(username),
        'password': str(password),
        'port': 1433,
        'cafile': certifi.where(),
        'validate_host': False,
    }




def get_file_objects(folder: str):
    if not os.path.exists(folder):
        return {}
    result = {}
    for f in os.listdir(folder):
        if f.endswith('.sql'):
            with open(os.path.join(folder, f), encoding='utf-8', newline='') as file:
                result[os.path.splitext(f)[0]] = file.read()
    return result




def generate_migration(config: Dict, sql_schema_dir: str, migrations_dir: str, schema_name: str, debug_diff: int = 0, only_object: str | None = None):
    params = _build_conn_params(config)
    migration_sql: List[str] = []
    # Summary buckets
    created: DefaultDict[str, List[str]] = defaultdict(list)
    updated: DefaultDict[str, List[str]] = defaultdict(list)
    dropped: DefaultDict[str, List[str]] = defaultdict(list)

    debug_shown = 0

    with pytds.connect(**params) as conn:
        cursor = conn.cursor()
        for obj_type in OBJECT_QUERIES:
            db_objs = get_db_objects(cursor, obj_type, schema_name, include_null=True)
            folder_name = obj_type if obj_type != 'StoredProcedures' else 'Stored Procedures'
            folder = os.path.join(sql_schema_dir, folder_name)
            file_objs = get_file_objects(folder)
            file_objs_ci = {k.lower(): v for k, v in file_objs.items()}

            # Find objects to create or alter (bring files up to DB state)
            print(f"[DEBUG] Processing {obj_type}: {list(db_objs.keys())}")
            db_names_lower = {k.lower() for k in db_objs.keys()}
            for name, db_def in db_objs.items():
                file_def = file_objs.get(name)
                if file_def is None:
                    file_def = file_objs_ci.get(name.lower())
                if file_def is None:
                    if db_def is None:
                        print(f"[MIGRATION] {obj_type} '{name}' exists in DB but definition is unavailable (possibly WITH ENCRYPTION). Skipping SQL; report only.")
                        created[obj_type].append(name + " (definition unavailable)")
                        # Do not append SQL body when definition is NULL
                    else:
                        print(f"[MIGRATION] {obj_type} '{name}' exists in DB but not in files. Will CREATE.")
                        created[obj_type].append(name)
                        migration_sql.append(f"-- Create {obj_type[:-1]}: {name}\n{db_def}\nGO\n")
                else:
                    if db_def is None:
                        # Cannot compare; report and skip SQL body
                        print(f"[MIGRATION] {obj_type} '{name}' definition unavailable in DB (possibly WITH ENCRYPTION). Skipping UPDATE.")
                    elif file_def != db_def:
                        print(f"[MIGRATION] {obj_type} '{name}' differs between DB and file. Will UPDATE.")
                        if debug_shown < debug_diff and (only_object is None or only_object == name):
                            debug_shown += 1
                            idx = _first_diff(file_def, db_def)
                            print(f"--- First diff index: {idx} (file len={len(file_def)}, db len={len(db_def)}) ---")
                            start = max(0, idx - 40) if idx >= 0 else 0
                            end_f = min(len(file_def), (idx + 40) if idx >= 0 else len(file_def))
                            end_d = min(len(db_def), (idx + 40) if idx >= 0 else len(db_def))
                            print('--- FILE slice ---')
                            print(repr(file_def[start:end_f]))
                            print('--- DB slice ---')
                            print(repr(db_def[start:end_d]))
                        updated[obj_type].append(name)
                        # Update DB to match local files: use file definition (header converted to ALTER)
                        migration_sql.append(
                            f"-- Update {obj_type[:-1]}: {name}\n{_definition_for_update(obj_type, file_def)}\nGO\n"
                        )

            # Find objects to drop (in files but not in DB)
            for name, file_def in file_objs.items():
                if name.lower() not in db_names_lower:
                    print(f"[MIGRATION] {obj_type} '{name}' exists in files but not in DB. Will DROP.")
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
    parser.add_argument('--debug-diff', type=int, default=0, help='Print file and DB content for first N mismatches')
    parser.add_argument('--only-object', help='Object name to focus debug output on (exact name)')
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
                       debug_diff=args.debug_diff,
                       only_object=args.only_object)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
