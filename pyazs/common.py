from typing import Dict


OBJECT_QUERIES = {
    'Tables': """
        SELECT t.name, OBJECT_DEFINITION(t.object_id) AS definition
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = %s
    """,
    'Views': """
        SELECT v.name, OBJECT_DEFINITION(v.object_id) AS definition
        FROM sys.views v
        JOIN sys.schemas s ON v.schema_id = s.schema_id
        WHERE s.name = %s
    """,
    'StoredProcedures': """
        SELECT p.name, OBJECT_DEFINITION(p.object_id) AS definition
        FROM sys.procedures p
        JOIN sys.schemas s ON p.schema_id = s.schema_id
        WHERE s.name = %s
    """,
    'Functions': """
        SELECT f.name, OBJECT_DEFINITION(f.object_id) AS definition
        FROM sys.objects f
        JOIN sys.schemas s ON f.schema_id = s.schema_id
        WHERE s.name = %s AND f.type IN ('FN','TF','IF')
    """,
    'Triggers': """
        SELECT tr.name, OBJECT_DEFINITION(tr.object_id) AS definition
        FROM sys.triggers tr
        JOIN sys.objects o ON tr.parent_id = o.object_id
        JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE s.name = %s
    """
}


def get_db_objects(cursor, obj_type: str, schema_name: str, include_null: bool = False) -> Dict[str, str | None]:
    """Extract object definitions from database for given object type and schema.
    When include_null is True, include entries where definition is NULL (e.g., encrypted objects).
    """
    cursor.execute(OBJECT_QUERIES[obj_type], (schema_name,))
    rows = cursor.fetchall()
    result: Dict[str, str | None] = {}
    for row in rows:
        # pytds returns tuples by default: (name, definition)
        name = row[0]
        definition = row[1]
        if definition is None and not include_null:
            continue
        result[name] = definition
    return result


def write_definition_to_file(definition: str, output_file: str) -> None:
    """Write definition to file preserving exact newlines."""
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        f.write(definition)
