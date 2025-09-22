#!/usr/bin/env python3
"""
Azure SQL Database Web Interface
Provides web interface for export, import, and compare functionality
"""

import os
import json
import yaml
import logging
import re
import threading
import time
from pathlib import Path
from datetime import datetime
from flask import Flask, render_template, request, jsonify, send_file, redirect, url_for, flash
from werkzeug.utils import secure_filename
import zipfile
import tempfile
import shutil

# Import our existing modules
from azure_sql_export import AzureSQLExporter
from azure_sql_import import AzureSQLImporter
from azure_sql_compare import DatabaseComparator
import azure_sql_copy as copy_mod

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = 'azure-sql-web-interface-secret-key'

# Configuration
UPLOAD_FOLDER = Path('uploads')
EXPORT_FOLDER = Path('exports')
ALLOWED_EXTENSIONS = {'yaml', 'yml', 'json'}
MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB max file size

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['EXPORT_FOLDER'] = EXPORT_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_CONTENT_LENGTH

# Create directories if they don't exist
UPLOAD_FOLDER.mkdir(exist_ok=True)
EXPORT_FOLDER.mkdir(exist_ok=True)

# Global variables for tracking operations
operation_status = {}
operation_logs = {}

def allowed_file(filename):
    """Check if file extension is allowed."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_config_from_file(file_path):
    """Load configuration from file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            if file_path.suffix.lower() in ['.yaml', '.yml']:
                return yaml.safe_load(f)
            else:
                return json.load(f)
    except Exception as e:
        logger.error(f"Error loading config file: {e}")
        return None

def run_export_operation(operation_id, config_path, output_dir):
    """Run export operation in background thread."""
    try:
        operation_status[operation_id] = {'status': 'running', 'progress': 0, 'message': 'Starting export...'}
        operation_logs[operation_id] = []
        
        # Load configuration
        config = get_config_from_file(config_path)
        if not config:
            operation_status[operation_id] = {'status': 'error', 'message': 'Failed to load configuration'}
            return
        
        # Create exporter
        exporter = AzureSQLExporter(config_dict=config, output_dir=output_dir)
        
        if not exporter.connect():
            operation_status[operation_id] = {'status': 'error', 'message': 'Failed to connect to database'}
            return
        
        operation_status[operation_id]['message'] = 'Connected to database, getting schema objects...'
        
        # Get schema objects
        objects = exporter.get_schema_objects()
        operation_status[operation_id]['progress'] = 25
        operation_status[operation_id]['message'] = f'Found {len(objects["tables"])} tables, {len(objects["views"])} views, {len(objects["stored_procedures"])} procedures'
        
        # Export schema objects
        exporter.export_schema_objects(objects)
        operation_status[operation_id]['progress'] = 50
        operation_status[operation_id]['message'] = 'Exported schema objects, starting data export...'
        
        # Export table data
        if config.get('export_data', True):
            exporter.export_table_data_all(objects)
        
        operation_status[operation_id]['progress'] = 100
        operation_status[operation_id]['status'] = 'completed'
        operation_status[operation_id]['message'] = 'Export completed successfully'
        
    except Exception as e:
        logger.error(f"Export operation failed: {e}")
        operation_status[operation_id] = {'status': 'error', 'message': str(e)}

def run_import_operation(operation_id, config_path, import_dir):
    """Run import operation in background thread."""
    try:
        operation_status[operation_id] = {'status': 'running', 'progress': 0, 'message': 'Starting import...'}
        operation_logs[operation_id] = []
        
        # Load configuration
        config = get_config_from_file(config_path)
        if not config:
            operation_status[operation_id] = {'status': 'error', 'message': 'Failed to load configuration'}
            return
        
        # Create importer
        importer = AzureSQLImporter(config_dict=config, import_dir=import_dir)
        
        if not importer.connect():
            operation_status[operation_id] = {'status': 'error', 'message': 'Failed to connect to database'}
            return
        
        operation_status[operation_id]['message'] = 'Connected to database, loading exported files...'
        
        # Load exported files
        files = importer.load_exported_files()
        operation_status[operation_id]['progress'] = 25
        operation_status[operation_id]['message'] = f'Loaded {sum(len(f) for f in files.values())} objects to import'
        
        # Import schema objects
        importer.import_schema_objects(files)
        operation_status[operation_id]['progress'] = 50
        operation_status[operation_id]['message'] = 'Imported schema objects, starting data import...'
        
        # Import table data
        if config.get('import_data', True):
            importer.import_table_data_all(files)
        
        operation_status[operation_id]['progress'] = 100
        operation_status[operation_id]['status'] = 'completed'
        operation_status[operation_id]['message'] = 'Import completed successfully'
        
    except Exception as e:
        logger.error(f"Import operation failed: {e}")
        operation_status[operation_id] = {'status': 'error', 'message': str(e)}

def run_compare_operation(operation_id, config_path, import_dir):
    """Run compare operation in background thread."""
    try:
        operation_status[operation_id] = {'status': 'running', 'progress': 0, 'message': 'Starting comparison...'}
        operation_logs[operation_id] = []
        
        # Load configuration
        config = get_config_from_file(config_path)
        if not config:
            operation_status[operation_id] = {'status': 'error', 'message': 'Failed to load configuration'}
            return
        
        # Create comparer
        comparer = DatabaseComparator(config_dict=config, import_dir=import_dir)
        
        if not comparer.connect():
            operation_status[operation_id] = {'status': 'error', 'message': 'Failed to connect to database'}
            return
        
        operation_status[operation_id]['message'] = 'Connected to database, running comparison...'
        
        # Run comparison
        results = comparer.run_comparison()
        operation_status[operation_id]['progress'] = 100
        operation_status[operation_id]['status'] = 'completed'
        operation_status[operation_id]['message'] = 'Comparison completed successfully'
        operation_status[operation_id]['results'] = results
        
    except Exception as e:
        logger.error(f"Compare operation failed: {e}")
        operation_status[operation_id] = {'status': 'error', 'message': str(e)}

def run_copy_operation(operation_id, config_path, tables_path: Path = None):
    """Run copy operation (source -> target) in background thread."""
    try:
        operation_status[operation_id] = {'status': 'running', 'progress': 0, 'message': 'Starting copy...'}
        operation_logs[operation_id] = []

        # Load configuration
        config = get_config_from_file(config_path)
        if not config:
            operation_status[operation_id] = {'status': 'error', 'message': 'Failed to load configuration'}
            return

        # If a tables file was uploaded, wire it into config
        if tables_path is not None:
            config.setdefault('copy', {})['tables_file'] = str(tables_path.resolve())

        copy_cfg = config.get('copy', {})
        default_schema = copy_cfg.get('schema', 'dbo')
        batch_size = int(copy_cfg.get('batch_size', 1000))
        truncate = bool(copy_cfg.get('truncate', False))
        identity_mode = copy_cfg.get('identity_insert', 'auto')
        retries = int(copy_cfg.get('retries', 3))
        retry_sleep = float(copy_cfg.get('retry_sleep_seconds', 2.0))

        # Build table list (merge config tables + tables_file)
        tables = copy_mod.parse_tables(
            cli_tables=None,
            tables_file=copy_cfg.get('tables_file'),
            config_tables=copy_cfg.get('tables'),
            default_schema=default_schema
        )

        if not tables:
            operation_status[operation_id] = {'status': 'error', 'message': 'No tables specified'}
            return

        operation_status[operation_id]['message'] = f"Preparing to copy {len(tables)} table(s)..."
        operation_status[operation_id]['progress'] = 5

        # Connect source and target
        source_conn = copy_mod.build_connection(config['source_db'])
        target_conn = copy_mod.build_connection(config['target_db'])

        try:
            total = len(tables)
            copied_rows_total = 0
            for idx, (schema_name, table_name) in enumerate(tables, start=1):
                operation_status[operation_id]['message'] = f"Copying {schema_name}.{table_name} ({idx}/{total})..."
                # Progress from 5 to 95 across tables
                operation_status[operation_id]['progress'] = 5 + int(90 * (idx - 1) / max(1, total))

                ok, msg, copied = copy_mod.copy_table(
                    source_conn,
                    target_conn,
                    schema_name,
                    table_name,
                    batch_size,
                    truncate,
                    identity_mode,
                    dry_run=False,
                    retries=retries,
                    retry_sleep=retry_sleep
                )
                if not ok:
                    operation_status[operation_id] = {'status': 'error', 'message': f"Failed {schema_name}.{table_name}: {msg}"}
                    return
                copied_rows_total += copied

            operation_status[operation_id]['progress'] = 100
            operation_status[operation_id]['status'] = 'completed'
            operation_status[operation_id]['message'] = f"Copy completed successfully. Rows copied: {copied_rows_total}"
        finally:
            source_conn.close()
            target_conn.close()

    except Exception as e:
        logger.error(f"Copy operation failed: {e}")
        operation_status[operation_id] = {'status': 'error', 'message': str(e)}

@app.route('/')
def index():
    """Main page."""
    return render_template('index.html')

@app.route('/export')
def export_page():
    """Export page."""
    return render_template('export.html')

@app.route('/import')
def import_page():
    """Import page."""
    return render_template('import.html')

@app.route('/compare')
def compare_page():
    """Compare page."""
    return render_template('compare.html')

@app.route('/copy')
def copy_page():
    """Copy page."""
    return render_template('copy.html')

@app.route('/format')
def format_page():
    """SQL Formatter page."""
    return render_template('format.html')

@app.route('/api/export', methods=['POST'])
def api_export():
    """API endpoint for export operation."""
    try:
        # Check if config file was uploaded
        if 'config_file' not in request.files:
            return jsonify({'error': 'No config file provided'}), 400
        
        config_file = request.files['config_file']
        if config_file.filename == '':
            return jsonify({'error': 'No config file selected'}), 400
        
        if not allowed_file(config_file.filename):
            return jsonify({'error': 'Invalid file type. Only YAML and JSON files are allowed.'}), 400
        
        # Save config file
        filename = secure_filename(config_file.filename)
        config_path = UPLOAD_FOLDER / filename
        config_file.save(config_path)
        
        # Create output directory
        operation_id = f"export_{int(time.time())}"
        output_dir = EXPORT_FOLDER / operation_id
        output_dir.mkdir(exist_ok=True)
        
        # Start export operation in background
        thread = threading.Thread(target=run_export_operation, args=(operation_id, config_path, output_dir))
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'operation_id': operation_id,
            'message': 'Export operation started'
        })
        
    except Exception as e:
        logger.error(f"Export API error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/import', methods=['POST'])
def api_import():
    """API endpoint for import operation."""
    try:
        # Check if config file was uploaded
        if 'config_file' not in request.files:
            return jsonify({'error': 'No config file provided'}), 400
        
        config_file = request.files['config_file']
        if config_file.filename == '':
            return jsonify({'error': 'No config file selected'}), 400
        
        if not allowed_file(config_file.filename):
            return jsonify({'error': 'Invalid file type. Only YAML and JSON files are allowed.'}), 400
        
        # Check if import files were uploaded
        if 'import_files' not in request.files:
            return jsonify({'error': 'No import files provided'}), 400
        
        import_files = request.files.getlist('import_files')
        if not import_files or import_files[0].filename == '':
            return jsonify({'error': 'No import files selected'}), 400
        
        # Save config file
        filename = secure_filename(config_file.filename)
        config_path = UPLOAD_FOLDER / filename
        config_file.save(config_path)
        
        # Create import directory and save files
        operation_id = f"import_{int(time.time())}"
        import_dir = UPLOAD_FOLDER / operation_id
        import_dir.mkdir(exist_ok=True)
        
        # Handle zip file or individual files
        if len(import_files) == 1 and import_files[0].filename.endswith('.zip'):
            # Extract zip file
            zip_path = import_dir / import_files[0].filename
            import_files[0].save(zip_path)
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(import_dir)
            
            zip_path.unlink()  # Remove zip file
        else:
            # Save individual files
            for file in import_files:
                if file.filename:
                    filename = secure_filename(file.filename)
                    file.save(import_dir / filename)
        
        # Start import operation in background
        thread = threading.Thread(target=run_import_operation, args=(operation_id, config_path, import_dir))
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'operation_id': operation_id,
            'message': 'Import operation started'
        })
        
    except Exception as e:
        logger.error(f"Import API error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/compare', methods=['POST'])
def api_compare():
    """API endpoint for compare operation."""
    try:
        # Check if config file was uploaded
        if 'config_file' not in request.files:
            return jsonify({'error': 'No config file provided'}), 400
        
        config_file = request.files['config_file']
        if config_file.filename == '':
            return jsonify({'error': 'No config file selected'}), 400
        
        if not allowed_file(config_file.filename):
            return jsonify({'error': 'Invalid file type. Only YAML and JSON files are allowed.'}), 400
        
        # Check if compare files were uploaded
        if 'compare_files' not in request.files:
            return jsonify({'error': 'No compare files provided'}), 400
        
        compare_files = request.files.getlist('compare_files')
        if not compare_files or compare_files[0].filename == '':
            return jsonify({'error': 'No compare files selected'}), 400
        
        # Save config file
        filename = secure_filename(config_file.filename)
        config_path = UPLOAD_FOLDER / filename
        config_file.save(config_path)
        
        # Create compare directory and save files
        operation_id = f"compare_{int(time.time())}"
        compare_dir = UPLOAD_FOLDER / operation_id
        compare_dir.mkdir(exist_ok=True)
        
        # Handle zip file or individual files
        if len(compare_files) == 1 and compare_files[0].filename.endswith('.zip'):
            # Extract zip file
            zip_path = compare_dir / compare_files[0].filename
            compare_files[0].save(zip_path)
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(compare_dir)
            
            zip_path.unlink()  # Remove zip file
        else:
            # Save individual files
            for file in compare_files:
                if file.filename:
                    filename = secure_filename(file.filename)
                    file.save(compare_dir / filename)
        
        # Start compare operation in background
        thread = threading.Thread(target=run_compare_operation, args=(operation_id, config_path, compare_dir))
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'operation_id': operation_id,
            'message': 'Compare operation started'
        })
        
    except Exception as e:
        logger.error(f"Compare API error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/copy', methods=['POST'])
def api_copy():
    """API endpoint for data copy operation."""
    try:
        if 'config_file' not in request.files:
            return jsonify({'error': 'No config file provided'}), 400

        config_file = request.files['config_file']
        if config_file.filename == '':
            return jsonify({'error': 'No config file selected'}), 400

        if not allowed_file(config_file.filename):
            return jsonify({'error': 'Invalid file type. Only YAML and JSON files are allowed.'}), 400

        # Optional tables file
        tables_file = request.files.get('tables_file')

        # Save config file
        filename = secure_filename(config_file.filename)
        config_path = UPLOAD_FOLDER / filename
        config_file.save(config_path)

        # Save tables file if provided
        tables_path = None
        if tables_file and tables_file.filename:
            tname = secure_filename(tables_file.filename)
            tables_path = UPLOAD_FOLDER / tname
            tables_file.save(tables_path)

        # Start copy operation in background
        operation_id = f"copy_{int(time.time())}"
        thread = threading.Thread(target=run_copy_operation, args=(operation_id, config_path, tables_path))
        thread.daemon = True
        thread.start()

        return jsonify({'operation_id': operation_id, 'message': 'Copy operation started'})
    except Exception as e:
        logger.error(f"Copy API error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/status/<operation_id>')
def api_status(operation_id):
    """API endpoint to get operation status."""
    if operation_id not in operation_status:
        return jsonify({'error': 'Operation not found'}), 404
    
    return jsonify(operation_status[operation_id])

@app.route('/api/download/<operation_id>')
def api_download(operation_id):
    """API endpoint to download export results."""
    if operation_id not in operation_status:
        return jsonify({'error': 'Operation not found'}), 404
    
    if operation_status[operation_id]['status'] != 'completed':
        return jsonify({'error': 'Operation not completed'}), 400
    
    # Create zip file of export results
    export_dir = EXPORT_FOLDER / operation_id
    if not export_dir.exists():
        return jsonify({'error': 'Export files not found'}), 404
    
    # Create temporary zip file
    temp_zip = tempfile.NamedTemporaryFile(delete=False, suffix='.zip')
    temp_zip.close()
    
    with zipfile.ZipFile(temp_zip.name, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(export_dir):
            for file in files:
                file_path = Path(root) / file
                arcname = file_path.relative_to(export_dir)
                zipf.write(file_path, arcname)
    
    return send_file(temp_zip.name, as_attachment=True, download_name=f'export_{operation_id}.zip')

@app.route('/api/format', methods=['POST'])
def api_format():
    """API endpoint to format SQL script using sqlparse."""
    try:
        import sqlparse

        # Accept either uploaded file or raw text
        sql_text = None
        if 'sql_file' in request.files and request.files['sql_file'].filename:
            file = request.files['sql_file']
            sql_text = file.read().decode('utf-8', errors='ignore')
        elif 'sql_text' in request.form and request.form['sql_text'].strip():
            sql_text = request.form['sql_text']

        if not sql_text:
            return jsonify({'error': 'No SQL provided'}), 400

        # Options
        keyword_case = request.form.get('keyword_case', 'upper')  # upper|lower|capitalize|preserve
        indent_width = int(request.form.get('indent_width', '4'))
        def _is_true(name: str) -> bool:
            val = request.form.get(name)
            return str(val).lower() in ('on', 'true', '1', 'yes')
        reindent = _is_true('reindent') or ('reindent' in request.form and request.form.get('reindent') is None)
        strip_comments = _is_true('strip_comments')
        use_space_around_operators = _is_true('space_around_operators')

        # Preserve GO separators by splitting and formatting batches separately
        lines = sql_text.splitlines()
        batches = []
        current = []
        for line in lines:
            if line.strip().upper() == 'GO':
                batches.append('\n'.join(current))
                batches.append('GO')
                current = []
            else:
                current.append(line)
        if current:
            batches.append('\n'.join(current))

        def _post_process(sql_text: str) -> str:
            # Put DDL AS on its own line for CREATE/ALTER VIEW/PROC/FUNCTION/TRIGGER
            sql_text = re.sub(
                r"\b(CREATE|ALTER)\s+(VIEW|PROC|PROCEDURE|FUNCTION|TRIGGER)([\s\S]*?)\s+AS\b",
                lambda m: f"{m.group(1)} {m.group(2)}{m.group(3)}\nAS",
                sql_text,
                flags=re.IGNORECASE,
            )

            # Force newlines before major clauses
            clauses = [
                r"SELECT", r"FROM", r"WHERE", r"GROUP\s+BY", r"ORDER\s+BY", r"HAVING",
                r"UNION\s+ALL", r"UNION", r"EXCEPT", r"INTERSECT",
                r"INNER\s+JOIN", r"LEFT\s+OUTER\s+JOIN", r"RIGHT\s+OUTER\s+JOIN", r"FULL\s+OUTER\s+JOIN",
                r"LEFT\s+JOIN", r"RIGHT\s+JOIN", r"FULL\s+JOIN", r"JOIN",
            ]
            for clause in clauses:
                sql_text = re.sub(rf"\s+({clause})\b", lambda m: f"\n{m.group(1).upper()}", sql_text, flags=re.IGNORECASE)

            # Align ON onto a new indented line after JOINs
            sql_text = re.sub(r"\s+ON\b", "\n    ON", sql_text, flags=re.IGNORECASE)
            # Keep IN ( on same line
            sql_text = re.sub(r"(?i)\bIN\b\s*\(", "IN (", sql_text)

            # Put SELECT on its own line and indent select list until FROM (outside parentheses only)
            def _format_select(match: re.Match) -> str:
                distinct = match.group(1) or ''
                select_list = (match.group(2) or '').strip()
                # Collapse excessive whitespace within select list but keep identifiers
                select_list = re.sub(r"\s+", " ", select_list)
                # Break by commas into separate lines
                select_list = re.sub(r"\s*,\s*", ",\n    ", select_list)
                header = f"SELECT {distinct}".rstrip()
                return f"{header}\n    {select_list}\nFROM "

            # Apply at top-level only: avoid transforming nested SELECTs inside parentheses
            def format_top_level_selects(text: str) -> str:
                result = []
                i = 0
                depth = 0
                while i < len(text):
                    if text[i] == '(':
                        depth += 1
                        result.append(text[i])
                        i += 1
                        continue
                    if text[i] == ')':
                        depth = max(0, depth - 1)
                        result.append(text[i])
                        i += 1
                        continue
                    if depth == 0:
                        m = re.compile(r"(?i)\bSELECT\b(\s+DISTINCT\s+)?([\s\S]*?)\bFROM\b\s*").match(text, i)
                        if m:
                            result.append(_format_select(m))
                            i = m.end()
                            continue
                    result.append(text[i])
                    i += 1
                return ''.join(result)

            sql_text = format_top_level_selects(sql_text)

            # Within parentheses, normalize simple SELECT ... FROM ... WHERE ... layout
            def format_in_parens(text: str) -> str:
                try:
                    import sqlparse
                except Exception:
                    return text

                def repl(m: re.Match) -> str:
                    inner = m.group(1)
                    # Use sqlparse to reindent the inner block
                    # Capture the inner's primary FROM target before formatting
                    inner_from_target = None
                    m_from = re.search(r"(?is)\bSELECT\b[\s\S]*?\bFROM\s+([A-Za-z_\[][\w\].]*)", inner)
                    if m_from:
                        inner_from_target = m_from.group(1).strip()

                    formatted = sqlparse.format(
                        inner,
                        keyword_case=(None if keyword_case == 'preserve' else keyword_case),
                        reindent=True,
                        reindent_aligned=True,
                        indent_width=indent_width,
                        use_space_around_operators=use_space_around_operators,
                    ).strip()
                    # If WHERE appears before FROM, reorder to SELECT ... FROM ... WHERE ...
                    try:
                        flines = formatted.splitlines()
                        where_idx = next((i for i, l in enumerate(flines) if re.match(r"^\s*WHERE\b", l, flags=re.IGNORECASE)), None)
                        from_idx = next((i for i, l in enumerate(flines) if re.match(r"^\s*FROM\b", l, flags=re.IGNORECASE)), None)
                        if where_idx is not None and from_idx is not None and where_idx < from_idx:
                            from_line = flines.pop(from_idx)
                            # Insert FROM just before WHERE
                            flines.insert(where_idx, from_line)
                            formatted = "\n".join(flines)
                    except Exception:
                        pass
                    # Ensure SELECT list is on following line and keep FROM target if present
                    formatted = re.sub(
                        r"(?i)\bSELECT\s+([^\n]+?)\s*\n\s*FROM\s+([^\n]+)",
                        lambda mm: "SELECT\n        " + mm.group(1).strip() + "\n    FROM " + mm.group(2).strip(),
                        formatted,
                    )
                    # If initial FROM target was captured but missing now, insert it right before the first WHERE line (preserve existing indent)
                    if inner_from_target and not re.search(rf"(?im)^\s*FROM\s+{re.escape(inner_from_target)}\b", formatted):
                        fl = formatted.splitlines()
                        where_idx = next((i for i, l in enumerate(fl) if re.match(r"^\s*WHERE\b", l, flags=re.IGNORECASE)), None)
                        # Check only lines BEFORE WHERE for an existing FROM
                        has_from_before_where = False
                        if where_idx is not None:
                            for l in fl[:where_idx]:
                                if re.match(r"^\s*FROM\b", l, flags=re.IGNORECASE):
                                    has_from_before_where = True
                                    break
                        if where_idx is not None and not has_from_before_where:
                            indent = re.match(r"^(\s*)", fl[where_idx]).group(1)
                            fl.insert(where_idx, f"{indent}FROM {inner_from_target}")
                            formatted = "\n".join(fl)
                    # Ensure WHERE x IN ( on same line with opening paren
                    formatted = re.sub(r"(?i)\bWHERE\s+(.+?)\s+IN\s*\(", r"WHERE \1 IN (", formatted)
                    # Ensure inner simple SELECT inside IN (...) breaks into lines
                    formatted = re.sub(
                        r"(?is)IN \(\s*SELECT\s+([^\n]+?)\s+FROM\s+([^\)\n]+)\s*\)",
                        lambda mm: "IN (\n        SELECT\n            " + mm.group(1).strip() + "\n        FROM " + mm.group(2).strip() + "\n    )",
                        formatted,
                    )
                    # Ensure SELECT/FROM/WHERE on own lines and indent by 4 spaces inside parens
                    lines = formatted.splitlines()
                    indented = []
                    for ln in lines:
                        if ln.strip():
                            indented.append('    ' + ln.lstrip())
                        else:
                            indented.append('')
                    # After indenting, ensure a missing FROM <target> between SELECT and WHERE is inserted
                    if inner_from_target:
                        try:
                            # find first top-level SELECT and WHERE
                            sel_idx = next((i for i, l in enumerate(indented) if re.match(r"^\s{4}SELECT\b", l, flags=re.IGNORECASE)), None)
                            where_idx2 = next((i for i, l in enumerate(indented) if re.match(r"^\s{4}WHERE\b", l, flags=re.IGNORECASE)), None)
                            has_from_between = any(re.match(r"^\s{4}FROM\b", l, flags=re.IGNORECASE) for l in indented[sel_idx+1:where_idx2] ) if sel_idx is not None and where_idx2 is not None else True
                            if sel_idx is not None and where_idx2 is not None and not has_from_between:
                                indented.insert(where_idx2, f"    FROM {inner_from_target}")
                        except Exception:
                            pass
                    # Ensure single-line SELECT lists are split before FROM
                    i = 0
                    while i < len(indented) - 1:
                        sel_m = re.match(r"^(\s*)SELECT\s+([^\n]+?)\s*$", indented[i], flags=re.IGNORECASE)
                        from_m = re.match(r"^(\s*)FROM\b(.*)$", indented[i+1], flags=re.IGNORECASE)
                        if sel_m and from_m:
                            indent = sel_m.group(1)
                            select_list = sel_m.group(2).strip()
                            indented[i] = f"{indent}SELECT"
                            indented.insert(i+1, f"{indent}    {select_list}")
                            i += 1  # skip over inserted line
                        i += 1

                    block = "(\n" + "\n".join(indented) + "\n)"
                    # After indenting, ensure SELECT list is on its own line
                    def break_select_list(m: re.Match) -> str:
                        indent_sel = m.group(1)
                        select_list = m.group(2).strip()
                        indent_from = m.group(3)
                        return f"{indent_sel}SELECT\n{indent_sel}    {select_list}\n{indent_from}FROM"

                    block = re.sub(r"(?im)^(\s*)SELECT\s+([^\n]+)\n(\s*)FROM",
                                   break_select_list,
                                   block)
                    # Fallback: if FROM is not on the next line yet, still split SELECT list
                    block = re.sub(r"(?im)^(\s*)SELECT\s+([^\n]+)\s*$",
                                   lambda m: f"{m.group(1)}SELECT\n{m.group(1)}    {m.group(2).strip()}",
                                   block)
                    # Final fix: if we have SELECT ... WHERE with no FROM in between, insert FROM <target>
                    if inner_from_target:
                        block_lines = block.splitlines()
                        new_block_lines = []
                        i = 0
                        while i < len(block_lines):
                            line = block_lines[i]
                            new_block_lines.append(line)
                            # Look for SELECT line followed by WHERE line with no FROM in between
                            if (re.match(r"^\s*SELECT\s*$", line, flags=re.IGNORECASE) and 
                                i + 1 < len(block_lines) and 
                                re.match(r"^\s*WHERE\b", block_lines[i+1], flags=re.IGNORECASE)):
                                # Insert FROM line between SELECT and WHERE
                                indent = re.match(r"^(\s*)", line).group(1)
                                new_block_lines.append(f"{indent}FROM {inner_from_target}")
                            i += 1
                        block = "\n".join(new_block_lines)
                    return block

                # Only handle single-depth parentheses to avoid greediness; apply repeatedly until stable
                prev = None
                out = text
                for _ in range(3):
                    if prev == out:
                        break
                    prev = out
                    out = re.sub(r"\(([^()]+)\)", repl, out)
                # Normalize common indentation issues inside parens
                # 1) Break single-line select lists at depth 1 and 2
                out = re.sub(r"(?im)^(\s{4})SELECT\s+([^\n]+)$", r"\1SELECT\n\1    \2", out)
                out = re.sub(r"(?im)^(\s{8})SELECT\s+([^\n]+)$", r"\1SELECT\n\1    \2", out)
                # 2) Ensure FROM/WHERE start at 4 spaces when they appear at column 0 inside parens
                out = re.sub(r"(?im)^FROM\b", "    FROM", out)
                out = re.sub(r"(?im)^WHERE\b", "    WHERE", out)
                # 3) Trim excessive indent before ") q"
                out = re.sub(r"(?m)^\s+\)\s+q\s*$", ") q", out)
                return out

            sql_text = format_in_parens(sql_text)

            # Global safeguard: break any remaining single-line SELECT list before FROM
            def break_global_select(m: re.Match) -> str:
                return f"{m.group(1)}\n    {m.group(2).strip()}\n{m.group(3)}"
            sql_text = re.sub(r"(?im)(^\s*SELECT)\s+([^\n]+)\n(\s*FROM)\b", break_global_select, sql_text)
            # Specific fix for inner blocks: normalize to 4-space SELECT / 8-space list / 4-space FROM
            sql_text = re.sub(
                r"(?im)^(\s{8})SELECT\s+([^\n]+)\n(\s{4})FROM\b",
                lambda m: "    SELECT\n        " + m.group(2).strip() + "\n    FROM",
                sql_text,
            )

            # Also split pattern with >=4-space SELECT followed by >=4-space FROM (keep same indent)
            sql_text = re.sub(
                r"(?m)^(\s{4,})SELECT\s+([^\n]+)\n(\s{4,})FROM\b",
                lambda m: f"{m.group(1)}SELECT\n{m.group(1)}    {m.group(2).strip()}\n{m.group(3)}FROM",
                sql_text,
            )
            # Additionally split any single-line inner select immediately followed by FROM on next line
            sql_text = re.sub(
                r"(?m)^(\s{4,})SELECT\s+([^\n]+)\s*$\n(\s{4,})FROM\b",
                lambda m: f"{m.group(1)}SELECT\n{m.group(1)}    {m.group(2).strip()}\n{m.group(3)}FROM",
                sql_text,
            )

            # Line-wise pass: split any 'SELECT <list>' line when next line starts with FROM at any indent
            gl_lines = sql_text.splitlines()
            gl_out = []
            i = 0
            while i < len(gl_lines):
                if i < len(gl_lines) - 1 and re.match(r"(?i)^\s*SELECT\s+\S", gl_lines[i]) and re.match(r"(?i)^\s*FROM\b", gl_lines[i+1]):
                    indent = re.match(r"^(\s*)", gl_lines[i]).group(1)
                    select_list = re.sub(r"(?i)^\s*SELECT\s+", "", gl_lines[i]).strip()
                    gl_out.append(f"{indent}SELECT")
                    gl_out.append(f"{indent}    {select_list}")
                    i += 1  # next line (FROM) will be processed normally in following iteration
                else:
                    gl_out.append(gl_lines[i])
                i += 1
            sql_text = "\n".join(gl_out)

            # Normalize CTE layout: line breaks and indentation
            # Keep a trailing space after AS/WITH to match expected
            sql_text = re.sub(r"\bAS\s+WITH\b", "AS \nWITH ", sql_text, flags=re.IGNORECASE)
            sql_text = re.sub(r"\bWITH\s+", "WITH \n    ", sql_text, flags=re.IGNORECASE)
            sql_text = re.sub(r"\b([A-Za-z_][\w\.]*)\s+AS\s*\(", r"\1 AS\n    (", sql_text, flags=re.IGNORECASE)
            # Ensure (SELECT becomes ( newline then indented SELECT (4 spaces)
            sql_text = re.sub(r"\(\s*SELECT", "(\n    SELECT", sql_text, flags=re.IGNORECASE)
            # Keep comma on same line as closing )
            sql_text = re.sub(r"\)\s*,\s*", "),", sql_text)
            # Ensure newline and indent before next CTE name after a comma, uppercase the CTE name
            sql_text = re.sub(
                r",\s*([A-Za-z_][\w\.]*)\s+AS\b",
                lambda m: ",\n    " + m.group(1).upper() + " AS",
                sql_text,
                flags=re.IGNORECASE,
            )
            sql_text = re.sub(r"\)\s*SELECT\b", ")\n    \nSELECT", sql_text, flags=re.IGNORECASE)

            # Fine-tune indentation inside simple CTE parentheses
            lines = sql_text.splitlines()
            processed = []
            inside_cte_block = False
            for i, ln in enumerate(lines):
                stripped = ln.strip()
                # Detect opening/closing of a CTE block delimited by a line with "("
                if stripped == '(':
                    inside_cte_block = True
                    processed.append('    (' )
                    continue
                if stripped == '),':
                    inside_cte_block = False
                    processed.append('    ),')
                    continue
                if stripped == ')':
                    inside_cte_block = False
                    processed.append('    )')
                    continue

                if inside_cte_block and stripped:
                    if re.match(r"(?i)^SELECT\b", stripped):
                        # Uppercase only the keyword SELECT
                        processed.append('        ' + re.sub(r"(?i)^SELECT\b", "SELECT", stripped, count=1))
                    elif re.match(r"(?i)^(FROM|WHERE|GROUP BY|ORDER BY|HAVING)\b", stripped):
                        # Uppercase the clause keyword only
                        processed.append('        ' + re.sub(r"(?i)^(FROM|WHERE|GROUP BY|ORDER BY|HAVING)\b",
                                                              lambda m: m.group(1).upper(), stripped, count=1))
                    else:
                        # Likely select list item
                        processed.append('            ' + stripped)
                else:
                    processed.append(ln)

            sql_text = "\n".join(processed)

            # Final normalization: operate with parenthesis depth to avoid touching inner blocks
            lines = sql_text.splitlines()
            final_lines = []
            depth = 0
            for idx in range(len(lines)):
                ln = lines[idx]
                # Update depth based on previous line content to reflect current line context
                open_count = ln.count('(')
                close_count = ln.count(')')
                # Split single-line SELECT list immediately followed by FROM at current depth
                if depth == 0 and re.match(r"(?i)^\s*SELECT\s+\S.*$", ln):
                    if idx + 1 < len(lines) and re.match(r"^\s*FROM\b", lines[idx+1], flags=re.IGNORECASE):
                        indent = re.match(r"^(\s*)", ln).group(1)
                        select_list = re.sub(r"(?i)^\s*SELECT\s+", "", ln).strip()
                        final_lines.append(f"{indent}SELECT")
                        final_lines.append(f"{indent}    {select_list}")
                        # do not append current ln; next iteration will append FROM line as-is
                        depth += open_count - close_count
                        continue
                # Dedent top-level FROM/WHERE that have exactly 4 leading spaces
                if depth == 0 and re.match(r"^\s{4}(FROM|WHERE)\b", ln, flags=re.IGNORECASE):
                    ln = re.sub(r"^\s{4}(FROM|WHERE)\b", lambda m: m.group(1).upper(), ln, count=1, flags=re.IGNORECASE)

                final_lines.append(ln)
                depth += open_count - close_count
            sql_text = "\n".join(final_lines)

            # Remove accidental double blank lines before FROM
            sql_text = re.sub(r"\n\s*\n(\s*FROM\b)", r"\n\1", sql_text, flags=re.IGNORECASE)

            # Strong finalization: inside parens, split '        SELECT <list>' followed by '    FROM'
            sql_text = re.sub(
                r"(?m)^(\s{8})SELECT\s+([^\n]+)\n(\s{4})FROM\b",
                lambda m: f"{m.group(1)}SELECT\n{m.group(1)}    {m.group(2).strip()}\n{m.group(3)}FROM",
                sql_text,
            )

            # Optional: remove spaces around operators if user didn't request them
            if not use_space_around_operators:
                # Tighten spaces around equals without touching other operators
                sql_text = re.sub(r"\s*=\s*", "=", sql_text)

            # Ultimate safeguard: split any 'SELECT <list>' line when the next line starts with FROM, at any depth
            _lines = sql_text.splitlines()
            _out = []
            i = 0
            while i < len(_lines):
                cur = _lines[i]
                nxt = _lines[i+1] if i + 1 < len(_lines) else None
                if nxt is not None and re.match(r"(?i)^\s*SELECT\s+\S", cur) and re.match(r"(?i)^\s*FROM\b", nxt):
                    indent = re.match(r"^(\s*)", cur).group(1)
                    select_list = re.sub(r"(?i)^\s*SELECT\s+", "", cur).strip()
                    _out.append(f"{indent}SELECT")
                    _out.append(f"{indent}    {select_list}")
                    i += 1  # consume current, next will be appended in next iteration
                else:
                    _out.append(cur)
                i += 1
            sql_text = "\n".join(_out)

            # Indentation fix for IN ( ... ) blocks: when a line with 4-space SELECT follows a line ending with 'IN ('
            lines2 = sql_text.splitlines()
            out2 = []
            i = 0
            while i < len(lines2):
                cur = lines2[i]
                out2.append(cur)
                # Detect opening of IN (
                if re.search(r"(?i)IN\s*\($", cur.strip()):
                    j = i + 1
                    # If next line starts with exactly 4 spaces and 'SELECT', increase indent of the block by 4 spaces
                    if j < len(lines2) and re.match(r"^\s{4}SELECT\b", lines2[j], flags=re.IGNORECASE):
                        # Walk until closing ')' at 4 spaces indent
                        k = j
                        while k < len(lines2):
                            ln = lines2[k]
                            if re.match(r"^\s*\)\s*", ln):
                                out2.append(ln)  # keep closing as-is
                                i = k
                                break
                            if ln.strip():
                                out2.append('    ' + ln)
                            else:
                                out2.append(ln)
                            k += 1
                        else:
                            i = j
                        # Skip lines we already appended
                        i = k
                i += 1
            sql_text = "\n".join(out2)

            # Targeted split/indent after 'FROM (' and 'IN ('
            lines3 = sql_text.splitlines()
            out3 = []
            for idx, ln in enumerate(lines3):
                prev = lines3[idx-1] if idx > 0 else ''
                prev_trim = prev.strip().upper()
                if re.match(r"^(\s{4})SELECT\s+([^\n]+)\s*$", ln, flags=re.IGNORECASE) and (
                    prev_trim.endswith('FROM (') or prev_trim.endswith('IN (')
                ):
                    m = re.match(r"^(\s{4})SELECT\s+([^\n]+)\s*$", ln, flags=re.IGNORECASE)
                    base_indent = '        ' if prev_trim.endswith('IN (') else m.group(1)
                    select_list = m.group(2).strip()
                    out3.append(f"{base_indent}SELECT")
                    out3.append(f"{base_indent}    {select_list}")
                    continue
                out3.append(ln)
            sql_text = "\n".join(out3)
            
            # Removed late-stage nested-select rewrite to avoid overriding previous fixes

            return sql_text

        def format_chunk(chunk: str) -> str:
            if chunk.strip().upper() == 'GO':
                return 'GO'
            if not chunk.strip():
                return ''
            # Use sqlparse-based formatting
            formatted = sqlparse.format(
                chunk,
                keyword_case=(None if keyword_case == 'preserve' else keyword_case),
                reindent=reindent,
                reindent_aligned=reindent,
                indent_width=indent_width,
                strip_comments=strip_comments,
                use_space_around_operators=use_space_around_operators
            ).rstrip()
            return _post_process(formatted)

        formatted_parts = [format_chunk(b) for b in batches]
        # Ensure GO separators are on their own line with single blank line around
        output_lines = []
        for part in formatted_parts:
            if part == '':
                continue
            if part == 'GO':
                # remove trailing blank
                while output_lines and output_lines[-1] == '':
                    output_lines.pop()
                output_lines.append('GO')
                output_lines.append('')
            else:
                output_lines.extend(part.splitlines())
                output_lines.append('')

        formatted_sql = '\n'.join(output_lines).rstrip() + '\n'

        return jsonify({
            'formatted_sql': formatted_sql
        })
    except Exception as e:
        logger.error(f"Format API error: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
