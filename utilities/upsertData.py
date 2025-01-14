import json
import psycopg2
from psycopg2.extras import execute_values
from utilities.migrationActivityLog import migration_activity_log

# Function to insert or upsert data
def upsert_data(cursor, table_schema, table_name, table_data, primary_key_columns):
    if not table_data:
        print(f"No data to import for table {table_name}. Skipping.\n")
        return
    
    column_names = ", ".join(list(table_data[0].keys()))
    primary_keys = ", ".join(primary_key_columns)
    update_set = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in table_data[0].keys() if col not in primary_key_columns])
    
    argslist = [
        [
            json.dumps(value) if isinstance(value, dict) else str(value) if value is not None else None
            for value in row.values()
        ]
        for row in table_data
    ]
    
    query = f'''
        INSERT INTO "{table_schema}"."{table_name}" ({column_names})
        VALUES %s
        ON CONFLICT ({primary_keys})
        DO UPDATE SET {update_set};
    '''
    
    try:
        execute_values(cursor, query, argslist, page_size=100)
        migration_activity_log(cursor, table_schema, table_name, 'SUCCESS', f"Data upserted successfully", query)

    except psycopg2.Error as e:
        cursor.connection.rollback()
        print(f"Failed to insert data. Error: {e}")
        migration_activity_log(cursor, table_schema, table_name, 'ERROR', f"Failed to upsert data. Error: {e}", query)