import psycopg2
from utilities.migrationActivityLog import migration_activity_log

# Function to fetch row counts
def fetch_row_count(cursor, table_schema, table_name):
    query = f'SELECT COUNT(*) FROM "{table_schema}"."{table_name}";'
    try:
        cursor.execute(query)
        result = cursor.fetchone()

        if result:
            migration_activity_log(cursor, table_schema, table_name, 'SUCCESS', f"Row count fetched successfully", query)
        else:
            migration_activity_log(cursor, table_schema, table_name, 'SUCCESS', f"No row count found", query)

        return result['count']
    
    except psycopg2.Error:
        cursor.connection.rollback()
        print(f"Table {table_name} does not exist. Assuming row count: 0")
        migration_activity_log(cursor, table_schema, table_name, 'ERROR', f"Table {table_name} does not exist. Assuming row count: 0", query)
        return 0