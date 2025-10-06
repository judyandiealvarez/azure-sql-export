from typing import Dict


OBJECT_QUERIES = {
    'Tables': """
        SELECT t.name, OBJECT_DEFINITION(t.object_id) AS definition
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = ?
    """,
    'Views': """
        SELECT v.name, OBJECT_DEFINITION(v.object_id) AS definition
        FROM sys.views v
        JOIN sys.schemas s ON v.schema_id = s.schema_id
        WHERE s.name = ?
    """,
    'StoredProcedures': """
        SELECT p.name, OBJECT_DEFINITION(p.object_id) AS definition
        FROM sys.procedures p
        JOIN sys.schemas s ON p.schema_id = s.schema_id
        WHERE s.name = ?
    """,
    'Functions': """
        SELECT f.name, OBJECT_DEFINITION(f.object_id) AS definition
        FROM sys.objects f
        JOIN sys.schemas s ON f.schema_id = s.schema_id
        WHERE s.name = ? AND f.type IN ('FN','TF','IF')
    """,
    'Triggers': """
        SELECT tr.name, OBJECT_DEFINITION(tr.object_id) AS definition
        FROM sys.triggers tr
        JOIN sys.objects o ON tr.parent_id = o.object_id
        JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE s.name = ?
    """
}


def get_db_objects(cursor, obj_type: str, schema_name: str) -> Dict[str, str]:
    """Extract object definitions from database for given object type and schema."""
    cursor.execute(OBJECT_QUERIES[obj_type], schema_name)
    return {row.name: row.definition for row in cursor.fetchall() if row.definition}


def write_definition_to_file(definition: str, output_file: str) -> None:
    """Write definition to file preserving exact newlines."""
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        f.write(definition)
