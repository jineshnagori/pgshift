import psycopg2
from utilities.migrationActivityLog import migration_activity_log

# Function to fetch data from a table
def fetch_table_data(cursor, table_schema, table_name):
    query = f'SELECT * FROM "{table_schema}"."{table_name}";'
    try:
        cursor.execute(query)
        result = cursor.fetchall()

        if result:
            migration_activity_log(cursor, table_schema, table_name, 'SUCCESS', f"Table data fetched successfully", query)
        else:
            migration_activity_log(cursor, table_schema, table_name, 'SUCCESS', f"No table data found", query)

        return result
    
    except psycopg2.Error as e:
        cursor.connection.rollback()
        print(f"Failed to fetch table data. Error: {e}")
        migration_activity_log(cursor, table_schema, table_name, 'ERROR', f"Failed to fetch table data. Error: {e}", query)
        return []