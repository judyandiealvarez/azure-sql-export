# Config-driven schema sync tool consistent with other commands
import os
import sys
import json
import yaml
import argparse
import pytds
from typing import Dict
try:
    from .common import get_db_objects, OBJECT_QUERIES, write_definition_to_file
except ImportError:
    from common import get_db_objects, OBJECT_QUERIES, write_definition_to_file



def _load_config(config_path: str) -> Dict:
    with open(config_path, 'r', encoding='utf-8') as f:
        if config_path.endswith(('.yaml', '.yml')):
            return yaml.safe_load(f)
        return json.load(f)


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
        'tds_version': 7.4,
    }




def get_local_objects(folder: str):
    if not os.path.exists(folder):
        os.makedirs(folder, exist_ok=True)
    return {os.path.splitext(f)[0]: os.path.join(folder, f)
            for f in os.listdir(folder) if f.endswith('.sql')}


def sync_schema_objects(config: Dict, sql_schema_dir: str, schema_name: str):
    params = _build_conn_params(config)
    with pytds.connect(**{k: v for k, v in params.items() if v is not None}) as conn:
        cursor = conn.cursor()
        for obj_type in OBJECT_QUERIES:
            print(f'Processing {obj_type}...')
            db_objs = get_db_objects(cursor, obj_type, schema_name)
            folder_name = obj_type if obj_type != 'StoredProcedures' else 'Stored Procedures'
            folder = os.path.join(sql_schema_dir, folder_name)
            local_objs = get_local_objects(folder)

            # Create or update
            for name, definition in db_objs.items():
                path = os.path.join(folder, f'{name}.sql')
                if name not in local_objs:
                    write_definition_to_file(definition, path)
                    print(f'Created/Updated: {path}')
                else:
                    with open(path, encoding='utf-8', newline='') as f:
                        existing = f.read()
                    if existing != definition:
                        write_definition_to_file(definition, path)
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
