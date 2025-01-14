import psycopg2
from utilities.migrationActivityLog import migration_activity_log

# Function to get primary key column
def get_primary_key_column(cursor, table_schema, table_name):
    """Get primary key columns for a table."""
    query = f"""
        SELECT a.attname
        FROM   pg_index i
        JOIN   pg_attribute a ON a.attrelid = i.indrelid
                             AND a.attnum = ANY(i.indkey)
        WHERE  i.indrelid = '"{table_schema}"."{table_name}"'::regclass
        AND    i.indisprimary;
    """
    
    try:
        cursor.execute(query)
        result = cursor.fetchall()

        if result:
            migration_activity_log(cursor, table_schema, table_name, 'SUCCESS', f"Primary key columns fetched successfully", query)
        else:
            migration_activity_log(cursor, table_schema, table_name, 'SUCCESS', f"No primary key columns found", query)

        return [row['attname'] for row in result]
    
    except psycopg2.Error as e:
        cursor.connection.rollback()
        print(f"Failed to fetch primary key columns. Error: {e}")
        migration_activity_log(cursor, table_schema, table_name, 'ERROR', f"Failed to fetch primary key columns. Error: {e}", query)
        return []