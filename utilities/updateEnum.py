import psycopg2
from utilities.migrationActivityLog import migration_activity_log

def update_enum(src_table_schema, dest_table_schema, cursor_src, cursor_dest, table_schema, table_name):
    # Extract enum names from the destination table schema
    dest_enums = [enum['enum_type'] for enum in dest_table_schema['enums']]

    # Identify enums missing in the destination table
    missing_enums = [
        (enum['enum_type']) 
        for enum in src_table_schema['enums'] 
        if enum['enum_type'] not in dest_enums
    ]

    # Prepare CREATE TYPE statements for missing enums
    additional_enums = []
    try:
        for enum in missing_enums:
            cursor_src.execute(f"SELECT enum_range(null::{enum}) AS enum_values;")
            result = cursor_src.fetchone()
            enum_values = result['enum_values'].replace('enum_range', '').replace('}', '').replace('{', '').replace("'", '').split(',')
            enum_values = [f"'{value}'" for value in enum_values]
            enum_values = ', '.join(enum_values)
            additional_enums.append(f"CREATE TYPE {enum} AS ENUM({enum_values});")
            if result:
                migration_activity_log(cursor_src, table_schema, table_name, 'SUCCESS', f"Enum values fetched successfully for {enum}", f"SELECT enum_range(null::{enum}) AS enum_values;")
            else:
                migration_activity_log(cursor_src, table_schema, table_name, 'SUCCESS', f"No enum values found for {enum}", f"SELECT enum_range(null::{enum}) AS enum_values;")

        if additional_enums:
            for query in additional_enums:
                print(f"Creating enum: {query}")
                result = cursor_dest.execute(query)
                enum_name = query.split(' ')[2]
                print(f"Enum {enum_name} created successfully\n")
                if result:
                    migration_activity_log(cursor_dest, table_schema, table_name, 'SUCCESS', f"Enum {enum_name} created successfully", query)
                else:
                    migration_activity_log(cursor_dest, table_schema, table_name, 'ERROR', f"Failed to create enum {enum_name}", query)
        else:
            print("No additional enums to add\n")
            migration_activity_log(cursor_dest, table_schema, table_name, 'SUCCESS', f"No additional enums to add", "No additional enums to add")

    except psycopg2.Error as e:
        cursor_src.connection.rollback()
        cursor_dest.connection.rollback()
        print(f"Failed to update enums. Error: {e}")
        migration_activity_log(cursor_dest, table_schema, table_name, 'ERROR', f"Failed to update enums. Error: {e}", "Failed to update enums")