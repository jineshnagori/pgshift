import psycopg2
from utilities.migrationActivityLog import migration_activity_log
from utilities import load_config

# Load configuration
config = load_config('./config.yaml')

# Function to fetch migration info
def fetch_migration_info(cursor):
    table_schema = config['schema']['default']
    table_name = config['table']['migration_activity']
    query = f"""
        SELECT migration_activity_id, table_schema, table_name, 
            requires_data_migration, truncate_load, "sequence"
        FROM {table_schema}.{table_name}
        ORDER BY "sequence";
    """
    try:
        cursor.execute(query)

        result = cursor.fetchall()

        if result:
            migration_activity_log(cursor, table_schema, table_name, 'SUCCESS', f"Migration info fetched successfully", query)
        else:
            migration_activity_log(cursor, table_schema, table_name, 'SUCCESS', f"No migration info found", query)    
        
        return result
    
    except psycopg2.Error as e:
        cursor.connection.rollback()
        print(f"Failed to fetch migration info. Error: {e}")
        migration_activity_log(cursor, table_schema, table_name, 'ERROR', f"Failed to fetch migration info. Error: {e}", query)
        return []