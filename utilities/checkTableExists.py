import psycopg2
from utilities.migrationActivityLog import migration_activity_log

# Function to check if a table exists
def table_exists(cursor, table_schema, table_name):
    query = f"""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = '{table_schema}'
            AND table_name = '{table_name}'
        );
    """
    try:
        cursor.execute(query)
        result = cursor.fetchone()['exists']

        if result:
            migration_activity_log(cursor, table_schema, table_name, 'SUCCESS', f"Table {table_schema}.{table_name} exists", query)
        else:
            migration_activity_log(cursor, table_schema, table_name, 'SUCCESS', f"Table {table_schema}.{table_name} does not exist", query)

        return result
    
    except psycopg2.Error as e:
        cursor.connection.rollback()
        migration_activity_log(cursor, table_schema, table_name, 'ERROR', f"Failed to check if table exists. Error: {e}", query)
        print(f"Failed to check if table exists. Error: {e}")
        return False