#!/usr/bin/env python3
"""
Azure SQL Database Data Copy Tool

Copies table data from a source Azure SQL database to a target Azure SQL database.

Features:
- Source/Target profiles (SQL auth or Azure AD)
- Tables from config, CLI, or file
- Optional truncate before load
- Batch copy with fast_executemany
- Identity insert handling (auto|on|off)
- Per-table transactions and retry on transient errors
- Dry-run preview and summary
"""

import sys
import yaml
import json
import logging
import argparse
from typing import Dict, List, Tuple, Optional
from pathlib import Path
import time

import pyodbc


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('azure_sql_copy.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


TRANSIENT_ERROR_CODES = {
    4060,   # Cannot open database requested by the login
    40197,  # The service has encountered an error processing your request
    40501,  # The service is currently busy
    49918,  # Cannot process request. Not enough resources
    49919,  # Cannot process create or update request
    49920,  # Cannot process request. Too many operations in progress
    10928,  # Resource ID limit reached
    10929,  # Resource ID limit reached
}


def load_config(config_file: str) -> Dict:
    try:
        with open(config_file, 'r') as f:
            if config_file.endswith(('.yaml', '.yml')):
                return yaml.safe_load(f)
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load config {config_file}: {e}")
        sys.exit(1)


def build_connection(config_section: Dict) -> pyodbc.Connection:
    server = config_section['server']
    database = config_section['database']
    driver = config_section.get('driver', 'ODBC Driver 17 for SQL Server')
    auth_type = config_section.get('authentication_type', 'sql')

    if auth_type == 'azure_ad':
        conn_str = (
            f"DRIVER={{{{}}}};SERVER={server};DATABASE={database};"
            f"Authentication=ActiveDirectoryDefault;TrustServerCertificate=yes;"
        ).format(driver)
    else:
        username = config_section['username']
        password = config_section['password']
        conn_str = (
            f"DRIVER={{{{}}}};SERVER={server};DATABASE={database};"
            f"UID={username};PWD={password};TrustServerCertificate=yes;"
        ).format(driver)

    return pyodbc.connect(conn_str)


def parse_tables(cli_tables: Optional[str], tables_file: Optional[str], config_tables: Optional[List[str]], default_schema: str) -> List[Tuple[str, str]]:
    tables: List[Tuple[str, str]] = []

    if config_tables:
        for t in config_tables:
            tables.append(normalize_table_name(t, default_schema))

    if cli_tables:
        for t in [s.strip() for s in cli_tables.split(',') if s.strip()]:
            tables.append(normalize_table_name(t, default_schema))

    if tables_file:
        p = Path(tables_file)
        if not p.exists():
            logger.error(f"Tables file not found: {tables_file}")
            sys.exit(1)
        for line in p.read_text().splitlines():
            s = line.strip()
            if not s or s.startswith('#'):
                continue
            tables.append(normalize_table_name(s, default_schema))

    # De-duplicate while preserving order
    seen = set()
    unique_tables: List[Tuple[str, str]] = []
    for sch, tbl in tables:
        key = (sch.lower(), tbl.lower())
        if key not in seen:
            seen.add(key)
            unique_tables.append((sch, tbl))
    return unique_tables


def normalize_table_name(name: str, default_schema: str) -> Tuple[str, str]:
    if '.' in name:
        sch, tbl = name.split('.', 1)
        return sch.strip('[]'), tbl.strip('[]')
    return default_schema, name.strip('[]')


def get_columns(cursor: pyodbc.Cursor, schema_name: str, table_name: str) -> List[str]:
    cursor.execute(
        """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """,
        schema_name, table_name
    )
    return [row[0] for row in cursor.fetchall()]


def has_identity_column(cursor: pyodbc.Cursor, schema_name: str, table_name: str) -> bool:
    cursor.execute(
        """
        SELECT 1
        FROM sys.columns c
        JOIN sys.tables t ON c.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = ? AND t.name = ? AND c.is_identity = 1
        """,
        schema_name, table_name
    )
    return cursor.fetchone() is not None


def fetch_batch(cursor: pyodbc.Cursor, schema_name: str, table_name: str, offset: int, batch_size: int) -> List[tuple]:
    sql = f"""
        SELECT *
        FROM [{schema_name}].[{table_name}]
        ORDER BY (SELECT NULL)
        OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY
    """
    cursor.execute(sql)
    return cursor.fetchall()


def get_row_count(cursor: pyodbc.Cursor, schema_name: str, table_name: str) -> int:
    cursor.execute(f"SELECT COUNT(*) FROM [{schema_name}].[{table_name}]")
    return int(cursor.fetchone()[0])


def execute_with_retry(operation, retries: int, sleep_seconds: float) -> bool:
    attempt = 0
    while True:
        try:
            operation()
            return True
        except pyodbc.Error as e:
            attempt += 1
            code = extract_sqlstate_or_number(e)
            if attempt <= retries and (code in TRANSIENT_ERROR_CODES or code is None):
                logger.warning(f"Transient error (code={code}). Retry {attempt}/{retries} in {sleep_seconds:.1f}s...")
                time.sleep(sleep_seconds)
                continue
            raise


def extract_sqlstate_or_number(e: pyodbc.Error) -> Optional[int]:
    try:
        # e.args can be (msg) or (msg, code)
        if len(e.args) >= 2 and isinstance(e.args[1], int):
            return e.args[1]
        # Some drivers expose e.args[0] like '42000 [Microsoft][ODBC Driver] (40501) ...'
        import re
        m = re.search(r'\((\d{4,5})\)', str(e))
        if m:
            return int(m.group(1))
    except Exception:
        pass
    return None


def copy_table(source_conn: pyodbc.Connection,
               target_conn: pyodbc.Connection,
               schema_name: str,
               table_name: str,
               batch_size: int,
               truncate: bool,
               identity_mode: str,
               dry_run: bool,
               retries: int,
               retry_sleep: float) -> Tuple[bool, str, int]:
    src_cur = source_conn.cursor()
    tgt_cur = target_conn.cursor()
    tgt_cur.fast_executemany = True

    full_name = f"{schema_name}.{table_name}"

    try:
        columns = get_columns(src_cur, schema_name, table_name)
        if not columns:
            return False, f"No columns found for {full_name}", 0

        total_rows = get_row_count(src_cur, schema_name, table_name)
        logger.info(f"{full_name}: {total_rows} rows to copy")

        if dry_run:
            return True, "Dry-run", total_rows

        # Begin per-table transaction
        execute_with_retry(lambda: tgt_cur.execute("BEGIN TRANSACTION"), retries, retry_sleep)

        # Optional truncate
        if truncate and total_rows > 0:
            logger.info(f"{full_name}: TRUNCATE target")
            execute_with_retry(lambda: tgt_cur.execute(f"TRUNCATE TABLE [{schema_name}].[{table_name}]"), retries, retry_sleep)

        # Identity handling
        identity_exists = has_identity_column(src_cur, schema_name, table_name)
        identity_on = False
        if identity_mode == 'on' or (identity_mode == 'auto' and identity_exists):
            logger.info(f"{full_name}: SET IDENTITY_INSERT ON")
            identity_on = True
            execute_with_retry(lambda: tgt_cur.execute(f"SET IDENTITY_INSERT [{schema_name}].[{table_name}] ON"), retries, retry_sleep)

        placeholders = ",".join(["?" for _ in columns])
        insert_sql = f"INSERT INTO [{schema_name}].[{table_name}] ([{'] ,['.join(columns)}]) VALUES ({placeholders})"

        copied = 0
        for offset in range(0, total_rows, batch_size):
            rows = fetch_batch(src_cur, schema_name, table_name, offset, batch_size)
            if not rows:
                break
            try:
                tgt_cur.executemany(insert_sql, rows)
            except Exception:
                # Fallback row-by-row to identify problem row
                for row in rows:
                    tgt_cur.execute(insert_sql, row)
            copied += len(rows)

        if identity_on:
            logger.info(f"{full_name}: SET IDENTITY_INSERT OFF")
            execute_with_retry(lambda: tgt_cur.execute(f"SET IDENTITY_INSERT [{schema_name}].[{table_name}] OFF"), retries, retry_sleep)

        tgt_cur.execute("COMMIT TRANSACTION")
        return True, "OK", copied
    except Exception as e:
        try:
            tgt_cur.execute("ROLLBACK TRANSACTION")
        except Exception:
            pass
        return False, str(e), 0
    finally:
        src_cur.close()
        tgt_cur.close()


def main():
    parser = argparse.ArgumentParser(description='Copy table data between Azure SQL databases')
    parser.add_argument('--config', default='config.copy.yaml', help='Configuration file path (YAML/JSON)')
    parser.add_argument('--schema', help='Default schema (overrides config)')
    parser.add_argument('--tables', help='Comma-separated list of tables (schema.table or table)')
    parser.add_argument('--tables-file', help='Path to file with table names (one per line)')
    parser.add_argument('--truncate', action='store_true', help='Truncate target tables before copy')
    parser.add_argument('--batch-size', type=int, help='Batch size for copy')
    parser.add_argument('--identity-insert', choices=['auto', 'on', 'off'], default=None, help='Identity insert mode')
    parser.add_argument('--dry-run', action='store_true', help='Show actions without executing')
    parser.add_argument('--retries', type=int, default=None, help='Retries for transient errors')
    parser.add_argument('--retry-sleep', type=float, default=None, help='Seconds to sleep between retries')

    args = parser.parse_args()

    cfg = load_config(args.config)

    # Config layout
    # source_db: {...}
    # target_db: {...}
    # copy:
    #   schema: "dbo"
    #   tables: ["dbo.Table1", "Table2"]
    #   truncate: false
    #   batch_size: 1000
    #   identity_insert: "auto"
    #   retries: 3
    #   retry_sleep_seconds: 2.0

    copy_cfg = cfg.get('copy', {})
    default_schema = args.schema or copy_cfg.get('schema', 'dbo')
    batch_size = args.batch_size or int(copy_cfg.get('batch_size', 1000))
    truncate = args.truncate or bool(copy_cfg.get('truncate', False))
    identity_mode = args.identity_insert or copy_cfg.get('identity_insert', 'auto')
    retries = args.retries if args.retries is not None else int(copy_cfg.get('retries', 3))
    retry_sleep = args.retry_sleep if args.retry_sleep is not None else float(copy_cfg.get('retry_sleep_seconds', 2.0))

    tables = parse_tables(
        args.tables,
        args.tables_file or copy_cfg.get('tables_file'),
        copy_cfg.get('tables'),
        default_schema
    )

    if not tables:
        logger.error("No tables specified. Use --tables, --tables-file, or copy.tables in config.")
        sys.exit(1)

    logger.info(f"Tables to copy: {', '.join([f'{s}.{t}' for s,t in tables])}")
    if args.dry_run:
        logger.info("Dry-run mode: no changes will be made")

    # Connect source and target
    try:
        source_conn = build_connection(cfg['source_db'])
        target_conn = build_connection(cfg['target_db'])
    except Exception as e:
        logger.error(f"Failed to connect: {e}")
        sys.exit(1)

    successes = 0
    failures = 0
    total_copied = 0

    try:
        for schema_name, table_name in tables:
            ok, msg, copied = copy_table(
                source_conn,
                target_conn,
                schema_name,
                table_name,
                batch_size,
                truncate,
                identity_mode,
                args.dry_run,
                retries,
                retry_sleep
            )
            if ok:
                logger.info(f"Copied {schema_name}.{table_name}: {copied} rows")
                successes += 1
                total_copied += copied
            else:
                logger.error(f"Failed {schema_name}.{table_name}: {msg}")
                failures += 1
    finally:
        source_conn.close()
        target_conn.close()

    logger.info(f"Summary: {successes} succeeded, {failures} failed, {total_copied} rows copied")
    sys.exit(0 if failures == 0 else 2)


if __name__ == "__main__":
    main()


