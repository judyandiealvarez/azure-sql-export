import os
import sys
import json
import yaml
import argparse
import pyodbc
try:
    from .common import get_db_objects, OBJECT_QUERIES
except ImportError:
    from common import get_db_objects, OBJECT_QUERIES


def _load_config(config_path: str):
    with open(config_path, 'r', encoding='utf-8') as f:
        if config_path.endswith(('.yaml', '.yml')):
            return yaml.safe_load(f)
        return json.load(f)


def _build_conn_str(config):
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


def test_object(config, object_name: str, schema_name: str, hex_output: bool = False):
    conn_str = _build_conn_str(config)
    
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        
        for obj_type in OBJECT_QUERIES:
            db_objs = get_db_objects(cursor, obj_type, schema_name)
            
            if object_name in db_objs:
                definition = db_objs[object_name]
                print(f"Found {obj_type[:-1]}: {object_name}")
                
                if hex_output:
                    print("Definition (hex):")
                    print(definition.encode('utf-8').hex())
                else:
                    print("Definition:")
                    print(definition)
                return
        
        print(f"Object '{object_name}' not found in schema '{schema_name}'")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description='Test object extraction by name')
    parser.add_argument('-c', '--config', default='config.yaml', help='Path to YAML/JSON config (default: config.yaml)')
    parser.add_argument('--schema-name', help='Schema name (e.g., dbo). Overrides config.schema_name or top-level schema')
    parser.add_argument('--hex', action='store_true', help='Output definition in hex format')
    parser.add_argument('object_name', help='Name of the object to find')
    args = parser.parse_args(argv)

    if not os.path.exists(args.config):
        raise SystemExit(f"Config file not found: {args.config}")
    config = _load_config(args.config)

    schema_name = (
        args.schema_name or
        config.get('schema_name') or
        config.get('schema')
    )

    test_object(config=config,
                object_name=args.object_name,
                schema_name=schema_name,
                hex_output=args.hex)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
