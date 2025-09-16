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

        # Options (HTML checkboxes send "on" when checked; missing when unchecked)
        keyword_case = request.form.get('keyword_case', 'upper')  # upper|lower|capitalize|preserve
        indent_width = int(request.form.get('indent_width', '4'))
        reindent = 'reindent' in request.form
        strip_comments = 'strip_comments' in request.form
        use_space_around_operators = 'space_around_operators' in request.form

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

            # Put SELECT on its own line and indent select list until FROM
            def _format_select(match: re.Match) -> str:
                distinct = match.group(1) or ''
                select_list = (match.group(2) or '').strip()
                # Collapse excessive whitespace within select list but keep identifiers
                select_list = re.sub(r"\s+", " ", select_list)
                # Break by commas into separate lines
                select_list = re.sub(r"\s*,\s*", ",\n    ", select_list)
                header = f"SELECT {distinct}".rstrip()
                return f"{header}\n    {select_list}\nFROM "

            sql_text = re.sub(r"\bSELECT\b(\s+DISTINCT\s+)?([\s\S]*?)\bFROM\b\s*",
                              _format_select,
                              sql_text,
                              flags=re.IGNORECASE)

            # Normalize CTE layout: line breaks and indentation
            sql_text = re.sub(r"\bAS\s+WITH\b", "AS\nWITH", sql_text, flags=re.IGNORECASE)
            sql_text = re.sub(r"\bWITH\s+", "WITH\n    ", sql_text, flags=re.IGNORECASE)
            sql_text = re.sub(r"\b([A-Za-z_][\w\.]*)\s+AS\s*\(", r"\1 AS\n    (", sql_text, flags=re.IGNORECASE)
            sql_text = re.sub(r"\)\s*,\s*", ")\n,\n    ", sql_text)
            sql_text = re.sub(r"\)\s*SELECT\b", ")\n\nSELECT", sql_text, flags=re.IGNORECASE)

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
                if stripped == ')':
                    inside_cte_block = False
                    processed.append('    )')
                    continue

                if inside_cte_block and stripped:
                    if re.match(r"(?i)^SELECT\b", stripped):
                        processed.append('        ' + stripped.upper())
                    elif re.match(r"(?i)^(FROM|WHERE|GROUP BY|ORDER BY|HAVING)\b", stripped):
                        processed.append('        ' + stripped.upper())
                    else:
                        # Likely select list item
                        processed.append('            ' + stripped)
                else:
                    processed.append(ln)

            sql_text = "\n".join(processed)

            return sql_text

        def format_chunk(chunk: str) -> str:
            if chunk.strip().upper() == 'GO':
                return 'GO'
            if not chunk.strip():
                return ''
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
