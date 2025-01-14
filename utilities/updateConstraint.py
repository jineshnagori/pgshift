import psycopg2
from utilities.migrationActivityLog import migration_activity_log

# Function to update constraint
def update_constraint(src_table_schema, dest_table_schema, table_schema, table_name, cursor_dest):
    # Extract constraint names from the destination table schema
    dest_constraints = {constraint['conname']: constraint['condef'] for constraint in dest_table_schema['constraints']}
    
    # Identify constraints missing in the destination schema
    missing_constraints = [
        {'conname': constraint['conname'], 'condef': constraint['condef']}
        for constraint in src_table_schema['constraints']
        if constraint['conname'] not in dest_constraints
    ]
    
    # Prepare ALTER TABLE statements for missing constraints
    alter_commands = []
    try:
        for constraint in missing_constraints:
            constraint_name = constraint['conname']
            constraint_def = constraint['condef']
            
            if constraint_def.startswith("PRIMARY KEY"):
                columns = constraint_def.split("(")[1].strip(")").strip()
                alter_commands.append(
                    f"ALTER TABLE {table_schema}.{table_name} ADD CONSTRAINT {constraint_name} PRIMARY KEY ({columns});"
                )
            elif constraint_def.startswith("UNIQUE"):
                columns = constraint_def.split("(")[1].strip(")").strip()
                alter_commands.append(
                    f"ALTER TABLE {table_schema}.{table_name} ADD CONSTRAINT {constraint_name} UNIQUE ({columns});"
                )
            elif constraint_def.startswith("FOREIGN KEY"):
                columns = constraint_def.split("(")[1].strip(")").strip()
                alter_commands.append(
                    f"ALTER TABLE {table_schema}.{table_name} ADD CONSTRAINT {constraint_name} {constraint_def};"
                )
            elif constraint_def.startswith("CHECK"):
                columns = constraint_def.split("(")[1].strip(")").strip()
                alter_commands.append(
                    f"ALTER TABLE {table_schema}.{table_name} ADD CONSTRAINT {constraint_name} {constraint_def};"
                )
            else:
                columns = constraint_def.split("(")[1].strip(")").strip()
                alter_commands.append(
                    f"ALTER TABLE {table_schema}.{table_name} ADD CONSTRAINT {constraint_name} {constraint_def};"
                )
        
        # Execute ALTER TABLE commands if there are missing constraints
        if alter_commands:
            for command in alter_commands:
                print(f"Executing: {command}")
                result = cursor_dest.execute(command)
                print(f"Constraint {command.split('ADD CONSTRAINT ')[1].split(' ')[0]} added successfully\n")
                if result:
                    migration_activity_log(cursor_dest, table_schema, table_name, 'SUCCESS', f"Constraint {command.split('ADD CONSTRAINT ')[1].split(' ')[0]} added successfully", command)
                else:
                    migration_activity_log(cursor_dest, table_schema, table_name, 'ERROR', f"Failed to add constraint {command.split('ADD CONSTRAINT ')[1].split(' ')[0]}", command)
        else:
            print("No additional constraints to add\n")
            migration_activity_log(cursor_dest, table_schema, table_name, 'SUCCESS', f"No additional constraints to add", "No additional constraints to add")

    except psycopg2.Error as e:
        cursor_dest.connection.rollback()
        print(f"Failed to update constraints. Error: {e}")
        migration_activity_log(cursor_dest, table_schema, table_name, 'ERROR', f"Failed to update constraints. Error: {e}", "Failed to update constraints")
