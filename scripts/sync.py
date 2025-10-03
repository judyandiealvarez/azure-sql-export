import os
import sys
import json
import yaml
import argparse
import pyodbc

SQL_SCHEMA_DIR = os.path.join('sql', 'dna', 'BPG_FinOps_Invoice_Reimbursement')
os.makedirs(SQL_SCHEMA_DIR, exist_ok=True)

# Require config to populate these; avoid hardcoded secrets
SERVER = None
DATABASE = None
USERNAME = None
PASSWORD = None
DRIVER = None
SCHEMA_NAME = None

OBJECT_QUERIES = {
    'Tables': """
        SELECT t.name, m.definition
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        CROSS APPLY (SELECT OBJECT_DEFINITION(t.object_id) AS definition) m
        WHERE s.name = 'BPG_FinOps_Invoice_Reimbursement'
    """,
    'Views': """
        SELECT v.name, m.definition
        FROM sys.views v
        JOIN sys.schemas s ON v.schema_id = s.schema_id
        CROSS APPLY (SELECT OBJECT_DEFINITION(v.object_id) AS definition) m
        WHERE s.name = 'BPG_FinOps_Invoice_Reimbursement'
    """,
    'Stored Procedures': """
        SELECT p.name, m.definition
        FROM sys.procedures p
        JOIN sys.schemas s ON p.schema_id = s.schema_id
        CROSS APPLY (SELECT OBJECT_DEFINITION(p.object_id) AS definition) m
        WHERE s.name = 'BPG_FinOps_Invoice_Reimbursement'
    """,
    'Functions': """
        SELECT f.name, m.definition
        FROM sys.objects f
        JOIN sys.schemas s ON f.schema_id = s.schema_id
        CROSS APPLY (SELECT OBJECT_DEFINITION(f.object_id) AS definition) m
        WHERE s.name = 'BPG_FinOps_Invoice_Reimbursement' AND f.type IN ('FN','TF','IF')
    """,
    'Triggers': """
        SELECT tr.name, m.definition
        FROM sys.triggers tr
        JOIN sys.schemas s ON tr.schema_id = s.schema_id
        CROSS APPLY (SELECT OBJECT_DEFINITION(tr.object_id) AS definition) m
        WHERE s.name = 'BPG_FinOps_Invoice_Reimbursement'
    """
}

def get_db_objects(cursor, obj_type):
    cursor.execute(OBJECT_QUERIES[obj_type])
    return {row.name: row.definition for row in cursor.fetchall() if row.definition}

def get_local_objects(folder):
    if not os.path.exists(folder):
        os.makedirs(folder, exist_ok=True)
    return {os.path.splitext(f)[0]: os.path.join(folder, f)
            for f in os.listdir(folder) if f.endswith('.sql')}

def sync_schema_objects():
    conn_str = f'DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        for obj_type in OBJECT_QUERIES:
            print(f'Processing {obj_type}...')
            db_objs = get_db_objects(cursor, obj_type)
            folder = os.path.join(SQL_SCHEMA_DIR, obj_type)
            local_objs = get_local_objects(folder)

            # Create or update
            for name, definition in db_objs.items():
                path = os.path.join(folder, f'{name}.sql')
                if name not in local_objs:
                    with open(path, 'w', encoding="utf-8", newline='') as f:
                        f.write(definition)
                    print(f'Created/Updated: {path}')
                else:
                    with open(path, encoding='utf-8', newline='') as f:
                        existing = f.read()
                    if existing != definition:
                        with open(path, 'w', encoding="utf-8", newline='') as f:
                            f.write(definition)
                        print(f'Created/Updated: {path}')

            # Remove non-existing
            for name, path in local_objs.items():
                if name not in db_objs:
                    os.remove(path)
                    print(f'Removed: {path}')

if __name__ == '__main__':
    # Minimal CLI to load config; keep original logic
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-c', '--config', default='config.yaml')
    parser.add_argument('--schema-name')
    parser.add_argument('--sql-schema-dir')
    args, _ = parser.parse_known_args()

    if not os.path.exists(args.config):
        raise SystemExit(f"Config file not found: {args.config}")

    with open(args.config, 'r', encoding='utf-8') as f:
        cfg = yaml.safe_load(f) if args.config.endswith(('.yaml', '.yml')) else json.load(f)

    SERVER = cfg.get('server')
    DATABASE = cfg.get('database')
    USERNAME = cfg.get('username') or cfg.get('user') or cfg.get('uid')
    PASSWORD = cfg.get('password') or cfg.get('pwd')
    driver_val = cfg.get('driver')
    DRIVER = '{' + driver_val.strip('{}') + '}' if driver_val else None
    # Schema name precedence: CLI > config.sync.schema_name > top-level
    SCHEMA_NAME = args.schema_name or (cfg.get('sync') or {}).get('schema_name') or cfg.get('schema_name') or cfg.get('schema')

    if not all([SERVER, DATABASE, USERNAME, PASSWORD, DRIVER, SCHEMA_NAME]):
        raise SystemExit('Missing required config values (server, database, username, password, driver, schema_name/schema)')

    # SQL schema dir precedence: CLI > config.sync.sql_schema_dir > top-level
    SQL_SCHEMA_DIR = args.sql_schema_dir or (cfg.get('sync') or {}).get('sql_schema_dir') or cfg.get('sql_schema_dir', SQL_SCHEMA_DIR)
    os.makedirs(SQL_SCHEMA_DIR, exist_ok=True)

    # Inject schema name into existing queries without changing their structure
    for k in list(OBJECT_QUERIES.keys()):
        OBJECT_QUERIES[k] = OBJECT_QUERIES[k].replace('BPG_FinOps_Invoice_Reimbursement', SCHEMA_NAME)

    sync_schema_objects()
