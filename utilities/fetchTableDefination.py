import psycopg2
from psycopg2.extras import RealDictCursor
from utilities.migrationActivityLog import migration_activity_log

# Function to fetch source table definition
def fetch_table_definition(connection, table_schema, table_name):
    # Query for table structure
    structure_query = f"""
        SELECT column_name, data_type, case when is_nullable = 'NO' then 'NOT NULL' 
        ELSE  'NULL' END AS nullable, case when TEXT(column_default) != 'NULL' then column_default 
        ELSE  'NULL' END AS column_default
        FROM information_schema.columns
        WHERE table_name = %s AND table_schema = %s
        ORDER BY column_name;
    """
    
    # Query for constraints
    constraints_query = f"""
        SELECT conname, pg_get_constraintdef(c.oid) AS condef
        FROM pg_constraint c
        JOIN pg_namespace n ON n.oid = c.connamespace
        JOIN pg_class cl ON cl.oid = c.conrelid
        WHERE cl.relname = %s AND n.nspname = %s;
    """
    
    # Query for indexes
    indexes_query = f"""
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE tablename = %s AND schemaname = %s
        ORDER BY indexname;
    """

    # Query for Enum values
    enum_query = f"""
        SELECT table_schema,
            table_name,
            column_name,
            data_type,
            udt_name AS enum_type
        FROM information_schema.columns
        WHERE table_name = %s
        and data_type = 'USER-DEFINED'
    """

    cursor = None
    try:
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Execute structure query
        cursor.execute(structure_query, (table_name, table_schema))
        structure = cursor.fetchall()
        if structure:
            migration_activity_log(cursor, table_schema, table_name, 'SUCCESS', f"Table structure fetched successfully for {table_schema}.{table_name}", structure_query)
        else:
            migration_activity_log(cursor, table_schema, table_name, 'SUCCESS', f"No table structure found for {table_schema}.{table_name}", structure_query)

        # Execute constraints query
        cursor.execute(constraints_query, (table_name, table_schema))
        constraints = cursor.fetchall()
        if constraints:
            migration_activity_log(cursor, table_schema, table_name, 'SUCCESS', f"Constraints fetched successfully for {table_schema}.{table_name}", constraints_query)
        else:
            migration_activity_log(cursor, table_schema, table_name, 'SUCCESS', f"No constraints found for {table_schema}.{table_name}", constraints_query)

        # Execute indexes query
        cursor.execute(indexes_query, (table_name, table_schema))
        indexes = cursor.fetchall()
        if indexes:
            migration_activity_log(cursor, table_schema, table_name, 'SUCCESS', f"Indexes fetched successfully for {table_schema}.{table_name}", indexes_query)
        else:
            migration_activity_log(cursor, table_schema, table_name, 'SUCCESS', f"No indexes found for {table_schema}.{table_name}", indexes_query)

        # Execute enum query
        cursor.execute(enum_query, (table_name,))
        enums = cursor.fetchall()
        if enums:
            migration_activity_log(cursor, table_schema, table_name, 'SUCCESS', f"Enums fetched successfully for {table_schema}.{table_name}", enum_query)
        else:
            migration_activity_log(cursor, table_schema, table_name, 'SUCCESS', f"No enums found for {table_schema}.{table_name}", enum_query)

        return {
            'structure': sorted(structure, key=lambda x: x['column_name']),
            'constraints': sorted(constraints, key=lambda x: x['conname']),
            'indexes': sorted(indexes, key=lambda x: x['indexname']),
            'enums': sorted(enums, key=lambda x: x['enum_type'])
        }

    except psycopg2.Error as e:
        connection.rollback()
        print(f"Failed to fetch table schema for {table_schema}.{table_name}. Error: {e}")
        migration_activity_log(cursor, table_schema, table_name, 'FAILED', f"Failed to fetch table schema for {table_schema}.{table_name}. Error: {e}", structure_query)
        return None