import psycopg2
import uuid
from datetime import datetime
from utilities import load_config

# Load configuration
config = load_config('./config.yaml')

# Function to insert migration activity logs into both source and destination databases
def migration_activity_log(cursor, table_schema, table_name, status, message, script):
    migration_log_id = str(uuid.uuid4())
    updated_by = ''
    updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    schema = config['table']['migration_log']['schema']
    table = config['table']['migration_log']['table']
    query = f"""
        INSERT INTO {schema}.{table}
        (migration_log_id, table_schema, table_name, status, message, script, updated_at, updated_by)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    try:
        cursor.execute(query, (migration_log_id, table_schema, table_name, status, message, script, updated_at, updated_by))
        cursor.connection.commit()
        print(f"Migration activity logged successfully for {table_schema}.{table_name}")
        
    except psycopg2.Error as e:
        cursor.connection.rollback()
        print(f"Failed to log migration activity for {table_schema}.{table_name}. Error: {e}")