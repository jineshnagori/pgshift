import psycopg2
from utilities.migrationActivityLog import migration_activity_log

# Function to update index
def update_index(src_table_schema, dest_table_schema, cursor_dest, table_schema, table_name):
    # Extract index names from the destination table schema
    dest_indexes = [index['indexname'] for index in dest_table_schema['indexes']]

    # Identify indexes missing in the destination table
    missing_indexes = [
        (index['indexname'], index['indexdef']) 
        for index in src_table_schema['indexes'] 
        if index['indexname'] not in dest_indexes
    ]

    # Prepare CREATE INDEX statements for missing indexes, removing brackets and extra spaces
    additional_indexes = []
    try:
        for index in missing_indexes:
            cleaned_index = index[1].replace('[', '').replace(']', '').replace('  ', ' ').strip()
            additional_indexes.append(cleaned_index)

        if additional_indexes:
            for query in additional_indexes:
                print(f"Creating index: {query}")
                result = cursor_dest.execute(query)
                index_name = query.split(' ')[2]
                print(f"Index {index_name} created successfully\n")
                if result:
                    migration_activity_log(cursor_dest, table_schema, table_name, 'SUCCESS', f"Index {index_name} created successfully", query)
                else:
                    migration_activity_log(cursor_dest, table_schema, table_name, 'ERROR', f"Failed to create index {index_name}", query)
        else:
            print("No additional indexes to add\n")
            migration_activity_log(cursor_dest, table_schema, table_name, 'SUCCESS', f"No additional indexes to add", "No additional indexes to add")

    except psycopg2.Error as e:
        cursor_dest.connection.rollback()
        print(f"Failed to update indexes. Error: {e}")
        migration_activity_log(cursor_dest, table_schema, table_name, 'ERROR', f"Failed to update indexes. Error: {e}", "Failed to update indexes")
