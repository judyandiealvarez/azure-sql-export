import os
import sys
import json
import yaml
import argparse
import pytds
import certifi
try:
    from .common import get_db_objects, OBJECT_QUERIES, write_definition_to_file
except ImportError:
    from common import get_db_objects, OBJECT_QUERIES, write_definition_to_file


def _load_config(config_path: str):
    with open(config_path, 'r', encoding='utf-8') as f:
        if config_path.endswith(('.yaml', '.yml')):
            return yaml.safe_load(f)
        return json.load(f)


def _build_conn_params(config):
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


def test_object(config, object_name: str, schema_name: str, hex_output: bool = False, output_file: str = None):
    params = _build_conn_params(config)
    
    with pytds.connect(**params) as conn:
        cursor = conn.cursor()
        
        for obj_type in OBJECT_QUERIES:
            db_objs = get_db_objects(cursor, obj_type, schema_name, include_null=True)
            # Case-insensitive lookup
            lookup = {name.lower(): name for name in db_objs.keys()}
            key = object_name.lower()
            if key in lookup:
                real_name = lookup[key]
                definition = db_objs[real_name]
                print(f"Found {obj_type[:-1]}: {object_name}")
                
                if definition is None:
                    print("Definition unavailable (permissions or WITH ENCRYPTION).")
                    return

                if output_file:
                    write_definition_to_file(definition, output_file)
                    print(f"Definition written to: {output_file}")
                else:
                    if hex_output:
                        print("Definition (hex):")
                        print(definition)
                        print("Hex breakdown:")
                        hex_chars = []
                        for char in definition:
                            if char == '\n':
                                hex_chars.append(f"\\n (0x{ord(char):02x})")
                            elif char == '\r':
                                hex_chars.append(f"\\r (0x{ord(char):02x})")
                            elif char == '\t':
                                hex_chars.append(f"\\t (0x{ord(char):02x})")
                            else:
                                hex_chars.append(f"{char} (0x{ord(char):02x})")
                        print(" ".join(hex_chars))
                    else:
                        print("Definition:")
                        print(definition)
                return
        # Not found: show suggestions
        print(f"Object '{object_name}' not found in schema '{schema_name}'")
        candidates = []
        for obj_type in OBJECT_QUERIES:
            db_objs = get_db_objects(cursor, obj_type, schema_name, include_null=True)
            for name in db_objs.keys():
                if object_name.lower() in name.lower():
                    candidates.append(f"{obj_type}:{name}")
        if candidates:
            print("Did you mean:")
            for c in candidates[:10]:
                print(f"  - {c}")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description='Test object extraction by name')
    parser.add_argument('-c', '--config', default='config.yaml', help='Path to YAML/JSON config (default: config.yaml)')
    parser.add_argument('--schema-name', help='Schema name (overrides config)')
    parser.add_argument('--hex', action='store_true', help='Output definition in hex format')
    parser.add_argument('-o', '--output', help='Write definition to file instead of console')
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
                hex_output=args.hex,
                output_file=args.output)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
