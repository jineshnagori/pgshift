import psycopg2
from psycopg2.extras import RealDictCursor
from utilities.fetchMigrationInfo import fetch_migration_info
from utilities.handleMigration import handle_migration
from utilities import load_config

# Load configuration
config = load_config('./config.yaml')

# Source database connection details
SRC_DB = config['source']['database']
SRC_USERNAME = config['source']['username']
SRC_PASSWD = config['source']['password']
SRC_URL = config['source']['host']
SRC_PORT = config['source']['port']

# Destination database connection details
DEST_DB = config['destination']['database']
DEST_USERNAME = config['destination']['username']
DEST_PASSWD = config['destination']['password']
DEST_URL = config['destination']['host']
DEST_PORT = config['destination']['port']

# Main function
if __name__ == '__main__':
    try:
        with psycopg2.connect(database=SRC_DB, user=SRC_USERNAME, password=SRC_PASSWD,
                              host=SRC_URL, port=SRC_PORT, cursor_factory=RealDictCursor) as conn_src, \
             psycopg2.connect(database=DEST_DB, user=DEST_USERNAME, password=DEST_PASSWD,
                              host=DEST_URL, port=DEST_PORT, cursor_factory=RealDictCursor) as conn_dest:
            conn_dest.autocommit = True
            
            with conn_src.cursor() as cursor_src, conn_dest.cursor() as cursor_dest:
                migration_activities = fetch_migration_info(cursor_src)
                
                for migration_info in migration_activities:
                    handle_migration(cursor_src, cursor_dest, migration_info)
                
                print("Migration completed successfully.")

    except psycopg2.Error as e:
        conn_dest.rollback()
        conn_src.close()
        conn_dest.close()
        print('An error occurred. All actions have been rolled back.\n')
        print(e, '\n')