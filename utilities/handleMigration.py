from utilities.fetchTableDefination import fetch_table_definition
from utilities.updateEnum import update_enum
from utilities.checkTableExists import table_exists
from utilities.createTable import get_create_table_query, create_table, grant_privileges
from utilities.migrationActivityLog import migration_activity_log
from utilities.createBackupTable import create_backup_table
from utilities.updateStructure import update_structural_change
from utilities.updateConstraint import update_constraint
from utilities.updateIndex import update_index
from utilities.fetchRowCount import fetch_row_count
from utilities.fetchTableData import fetch_table_data
from utilities.insertData import insert_data
from utilities.upsertData import upsert_data
from utilities.getPrimaryKey import get_primary_key_column

# Function to handle migration logic
def handle_migration(cursor_src, cursor_dest, migration_info):
    table_schema = migration_info['schema_name']
    table_name = migration_info['table_name']
    requires_data_migration = migration_info['requires_data_migration']
    truncate_load = migration_info['truncate_load']
    sequence = migration_info['sequence']
    src_table_schema = fetch_table_definition(cursor_src.connection, table_schema, table_name)
    dest_table_schema = fetch_table_definition(cursor_dest.connection, table_schema, table_name)

    # Update enum
    update_enum(src_table_schema, dest_table_schema, cursor_src, cursor_dest, table_schema, table_name)
    
    if requires_data_migration:
        print(f"Processing {table_schema}.{table_name} with sequence {sequence}")

        if not table_exists(cursor_dest, table_schema, table_name):
            print(f"Table {table_schema}.{table_name} does not exist in destination. Creating...")
            create_table_query = get_create_table_query(cursor_src, cursor_src.connection, table_schema, table_name)

            # Create table in destination
            create_table(cursor_dest, cursor_dest.connection, create_table_query, table_schema, table_name)

            # Grant privileges
            grant_privileges(cursor_dest, cursor_dest.connection, table_schema, table_name)
        else:
            print(f"Table {table_schema}.{table_name} exists in destination. Skipping creation.")
            migration_activity_log(cursor_dest, table_schema, table_name, 'SUCCESS', f"Table {table_schema}.{table_name} exists in destination. Skipping creation", "Table exists in destination. Skipping creation")

            # Create backup table
            create_backup_table(cursor_dest, table_schema, table_name)

            # Update structural changes
            update_structural_change(src_table_schema, dest_table_schema, table_schema, table_name, cursor_dest)

            # Update constraints
            update_constraint(src_table_schema, dest_table_schema, table_schema, table_name, cursor_dest)

            # Update indexes
            update_index(src_table_schema, dest_table_schema, cursor_dest, table_schema, table_name)

        if requires_data_migration and truncate_load:
            print(f"Truncate and Load for {table_schema}.{table_name}")
            cursor_dest.execute(f'TRUNCATE TABLE "{table_schema}"."{table_name}";')
        
        source_count = fetch_row_count(cursor_src, table_schema, table_name)
        print(f"Source table row count: {source_count}")
        
        table_data = fetch_table_data(cursor_src, table_schema, table_name)
        
        if truncate_load:
            # Direct insert for truncate-and-load
            insert_data(cursor_dest, table_schema, table_name, table_data)
        else:
            # Upsert for tables with primary keys
            primary_key_columns = get_primary_key_column(cursor_dest, table_schema, table_name)
            if not primary_key_columns:
                print(f"WARNING: No primary key found for table {table_name}. Skipping.\n")
                migration_activity_log(cursor_dest, table_schema, table_name, 'WARNING', f"No primary key found for table {table_name}. Skipping.", "No primary key found for table. Skipping.")
                return
            
            upsert_data(cursor_dest, table_schema, table_name, table_data, primary_key_columns)
        
        dest_count = fetch_row_count(cursor_dest, table_schema, table_name)
        print(f"Destination table updated row count: {dest_count}\n")