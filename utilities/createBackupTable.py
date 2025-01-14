import psycopg2
from datetime import datetime
from utilities.migrationActivityLog import migration_activity_log
from utilities import load_config

# Load configuration
config = load_config('./config.yaml')

# Function to create a backup of the table
def create_backup_table(cursor, table_schema, table_name):
    timestamp = datetime.now().strftime("%Y%m%d")
    backup_table_name = f"backup_{config['destination']['environment']}_{table_schema}_{table_name}_{timestamp}"
    query = f"""
    CREATE TABLE IF NOT EXISTS {backup_table_name} AS
    SELECT * FROM {table_schema}.{table_name};
    """
    try:
        cursor.execute(query)
        print(f"Backup table created successfully: {backup_table_name}")
        migration_activity_log(cursor, table_schema, table_name, 'SUCCESS', f"Backup table created successfully: {backup_table_name}", query)

    except psycopg2.Error as e:
        cursor.connection.rollback()
        print(f"Failed to create backup table for {table_schema}.{table_name}. Error: {e}")
        migration_activity_log(cursor, table_schema, table_name, 'ERROR', f"Failed to create backup table for {table_schema}.{table_name}. Error: {e}", query)