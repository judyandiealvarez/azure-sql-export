# Config-driven schema sync tool consistent with other commands
import os
import sys
import json
import yaml
import argparse
import pyodbc
from typing import Dict

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
    'Stored Procedures': """
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
        "DATABASE=" + database + ";" +
        "UID=" + str(username) + ";" +
        "PWD=" + str(password)
    )


def get_db_objects(cursor, obj_type: str, schema_name: str):
    cursor.execute(OBJECT_QUERIES[obj_type], schema_name)
    return {row.name: row.definition for row in cursor.fetchall() if row.definition}


def get_local_objects(folder: str):
    if not os.path.exists(folder):
        os.makedirs(folder, exist_ok=True)
    return {os.path.splitext(f)[0]: os.path.join(folder, f)
            for f in os.listdir(folder) if f.endswith('.sql')}


def sync_schema_objects(config: Dict, sql_schema_dir: str, schema_name: str):
    conn_str = _build_conn_str(config)
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        for obj_type in OBJECT_QUERIES:
            print(f'Processing {obj_type}...')
            db_objs = get_db_objects(cursor, obj_type, schema_name)
            folder = os.path.join(sql_schema_dir, obj_type.replace(' ', ''))
            local_objs = get_local_objects(folder)

            # Create or update
            for name, definition in db_objs.items():
                path = os.path.join(folder, f'{name}.sql')
                if name not in local_objs or open(path, encoding='utf-8').read() != definition:
                    with open(path, 'w', encoding='utf-8') as f:
                        f.write(definition)
                    print(f'Created/Updated: {path}')

            # Remove non-existing
            for name, path in local_objs.items():
                if name not in db_objs:
                    os.remove(path)
                    print(f'Removed: {path}')


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description='Sync DB schema objects to local .sql files')
    parser.add_argument('-c', '--config', default='config.yaml', help='Path to YAML/JSON config (default: config.yaml)')
    parser.add_argument('--schema-name', help='Schema name (e.g., dbo). Overrides config.sync.schema_name')
    parser.add_argument('--sql-schema-dir', help='Local schema root. Overrides config.sync.sql_schema_dir')
    args = parser.parse_args(argv)

    if not os.path.exists(args.config):
        raise SystemExit(f"Config file not found: {args.config}")
    config = _load_config(args.config)

    sync_cfg = (config.get('sync') or {}) if isinstance(config, dict) else {}
    schema_name = args.schema_name or sync_cfg.get('schema_name')
    if not schema_name:
        raise SystemExit('schema_name must be provided via --schema-name or config.sync.schema_name')
    sql_schema_dir = args.sql_schema_dir or sync_cfg.get('sql_schema_dir') or os.path.join('sql', 'schema')

    sync_schema_objects(config=config,
                        sql_schema_dir=sql_schema_dir,
                        schema_name=schema_name)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
