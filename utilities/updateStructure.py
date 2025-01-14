import psycopg2
from utilities.migrationActivityLog import migration_activity_log

# Function to update structural change
def update_structural_change(src_table_schema, dest_table_schema, table_schema, table_name, cursor_dest):
    # Extract column names from the destination table schema
    dest_columns = [col['column_name'] for col in dest_table_schema['structure']]

    # Identify columns missing in the destination table
    missing_columns = [
        (col['column_name'], col['data_type'], col['nullable'], col['column_default']) 
        for col in src_table_schema['structure'] 
        if col['column_name'] not in dest_columns
    ]

    # Prepare ALTER TABLE statements for missing columns
    additional_columns = []
    try:
        for col in missing_columns:
            if col[1] == 'ARRAY':
                additional_columns.append(
                    f"ALTER TABLE {table_schema}.{table_name} ADD COLUMN {col[0]} TEXT[] DEFAULT {col[3]}"
                )
            elif col[1] == 'USER-DEFINED':
                additional_columns.append(
                    f"ALTER TABLE {table_schema}.{table_name} ADD COLUMN {col[0]} {col[0]} DEFAULT {col[3]}"
                )
            else:
                additional_columns.append(
                    f"ALTER TABLE {table_schema}.{table_name} ADD COLUMN {col[0]} {col[1]} DEFAULT {col[3]}"
                )
        
        # Execute ALTER TABLE statements if there are missing columns
        if additional_columns:
            for query in additional_columns:
                result = cursor_dest.execute(query)
                print(f"Column {query.split('ADD COLUMN ')[1].split(' ')[0]} added successfully\n")
                if result:
                    migration_activity_log(cursor_dest, table_schema, table_name, 'SUCCESS', f"Column {query.split('ADD COLUMN ')[1].split(' ')[0]} added successfully", query)
                else:
                    migration_activity_log(cursor_dest, table_schema, table_name, 'ERROR', f"Failed to add column {query.split('ADD COLUMN ')[1].split(' ')[0]}", query)
        else:
            print("No additional columns to add\n")
            migration_activity_log(cursor_dest, table_schema, table_name, 'SUCCESS', f"No additional columns to add", "No additional columns to add")
    
    except psycopg2.Error as e:
        cursor_dest.connection.rollback()
        print(f"Failed to update structural changes. Error: {e}")
        migration_activity_log(cursor_dest, table_schema, table_name, 'ERROR', f"Failed to update structural changes. Error: {e}", "Failed to update structural changes")
