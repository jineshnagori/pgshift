import psycopg2
from utilities.migrationActivityLog import migration_activity_log

# Function to get the CREATE TABLE query
def get_create_table_query(cursor, connection, table_schema, table_name):
    nr_notices = len(connection.notices)
    query = f"""
        DO $$
        DECLARE
            schema_name TEXT := '{table_schema}';
            table_name TEXT := '{table_name}';
            column_def RECORD;
            pk_constraint RECORD;
            unique_constraint RECORD;
            fk_constraint RECORD;
            check_constraint RECORD;
            create_table_query TEXT := '';
        BEGIN
            -- Start CREATE TABLE statement
            create_table_query := format('CREATE TABLE IF NOT EXISTS %I.%I (\n', schema_name, table_name);

            -- Add columns
            FOR column_def IN 
                EXECUTE format(
                    'SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable, column_default, udt_name
                    FROM information_schema.columns 
                    WHERE table_schema = %L AND table_name = %L',
                    schema_name, table_name
                )
            LOOP
                create_table_query := create_table_query || 
                    format('    %I %s%s%s%s,\n',
                        column_def.column_name,
                        CASE
                            WHEN column_def.data_type = 'numeric' THEN format('numeric(%s, %s)', column_def.numeric_precision, column_def.numeric_scale)
                            WHEN column_def.data_type = 'USER-DEFINED' THEN column_def.udt_name
                            ELSE column_def.data_type
                        END,
                        CASE WHEN column_def.is_nullable = 'NO' THEN ' NOT NULL' ELSE '' END,
                        CASE WHEN column_def.column_default IS NOT NULL THEN format(' DEFAULT %s', column_def.column_default) ELSE '' END,
                        CASE WHEN column_def.data_type = 'character varying' THEN ' COLLATE pg_catalog."default"' ELSE '' END
                    );
            END LOOP;

            -- Add primary key constraint
            FOR pk_constraint IN
                EXECUTE format(
                    'SELECT conname, pg_get_constraintdef(oid, true) AS definition 
                    FROM pg_constraint 
                    WHERE conrelid = %L::regclass AND contype = %L',
                    format('%I.%I', schema_name, table_name), 'p' -- 'p' = Primary Key
                )
            LOOP
                create_table_query := create_table_query || 
                    format('    CONSTRAINT %I %s,\n', pk_constraint.conname, pk_constraint.definition);
            END LOOP;

            -- Add unique constraints
            FOR unique_constraint IN
                EXECUTE format(
                    'SELECT conname, pg_get_constraintdef(oid, true) AS definition 
                    FROM pg_constraint 
                    WHERE conrelid = %L::regclass AND contype = %L',
                    format('%I.%I', schema_name, table_name), 'u' -- 'u' = Unique Constraint
                )
            LOOP
                create_table_query := create_table_query || 
                    format('    CONSTRAINT %I %s,\n', unique_constraint.conname, unique_constraint.definition);
            END LOOP;

            -- Add foreign key constraints
            FOR fk_constraint IN
                EXECUTE format(
                    'SELECT conname, pg_get_constraintdef(oid, true) AS definition 
                    FROM pg_constraint 
                    WHERE conrelid = %L::regclass AND contype = %L',
                    format('%I.%I', schema_name, table_name), 'f' -- 'f' = Foreign Key
                )
            LOOP
                create_table_query := create_table_query || 
                    format('    CONSTRAINT %I %s,\n', fk_constraint.conname, fk_constraint.definition);
            END LOOP;

            -- Add check constraints
            FOR check_constraint IN
                EXECUTE format(
                    'SELECT conname, pg_get_constraintdef(oid, true) AS definition 
                    FROM pg_constraint 
                    WHERE conrelid = %L::regclass AND contype = %L',
                    format('%I.%I', schema_name, table_name), 'c' -- 'c' = Check Constraint
                )
            LOOP
                create_table_query := create_table_query || 
                    format('    CONSTRAINT %I %s,\n', check_constraint.conname, check_constraint.definition);
            END LOOP;

            -- Remove last comma and close the statement
            create_table_query := regexp_replace(create_table_query, ',\n$', '\n');
            create_table_query := create_table_query || ');';

            -- Print the generated query
            RAISE NOTICE '%', create_table_query;
        END $$;
    """
    try:
        cursor.execute(query)
        for notice in connection.notices[nr_notices:]:
            fetch_create_table_query = notice.replace("NOTICE:  ", "", 1).strip()
            print(fetch_create_table_query)
            migration_activity_log(cursor, table_schema, table_name, 'SUCCESS', f"CREATE TABLE query generated successfully for {table_schema}.{table_name}", query)
            return fetch_create_table_query
        
    except psycopg2.Error as e:
        connection.rollback()
        print(f"Failed to generate CREATE TABLE query for {table_schema}.{table_name}. Error: {e}")
        migration_activity_log(cursor, table_schema, table_name, 'ERROR', f"Failed to generate CREATE TABLE query for {table_schema}.{table_name}. Error: {e}", query)
        return None

# Function to execute the CREATE TABLE query
def create_table(cursor, connection, create_table_query, table_schema, table_name):
    try:
        cursor.execute(create_table_query)
        result = connection.commit()
        print(f'Table created successfully\n')
        if result:
            migration_activity_log(cursor, table_schema, table_name, 'SUCCESS', f"Table {table_schema}.{table_name} created successfully", create_table_query)
        else:
            migration_activity_log(cursor, table_schema, table_name, 'ERROR', f"Failed to create table {table_schema}.{table_name}", create_table_query)
    
    except psycopg2.Error as e:
        connection.rollback()
        print(f"Failed to create table {table_schema}.{table_name}. Error: {e}")
        migration_activity_log(cursor, table_schema, table_name, 'ERROR', f"Failed to create table {table_schema}.{table_name}. Error: {e}", create_table_query)

# Function to grant privileges
def grant_privileges(cursor, connection, table_schema, table_name):
    query = f"""
        ALTER TABLE IF EXISTS {table_schema}.{table_name} OWNER to psqladmin;

        GRANT ALL ON TABLE {table_schema}.{table_name} TO psqladmin;
    """
    try:
        cursor.execute(query)
        result = connection.commit()
        print(f'Privileges granted successfully\n')
        if result:
            migration_activity_log(cursor, table_schema, table_name, 'SUCCESS', f"Privileges granted successfully for {table_schema}.{table_name}", query)
        else:
            migration_activity_log(cursor, table_schema, table_name, 'ERROR', f"Failed to grant privileges for {table_schema}.{table_name}", query)

    except psycopg2.Error as e:
        connection.rollback()
        print(f"Failed to grant privileges for {table_schema}.{table_name}. Error: {e}")
        migration_activity_log(cursor, table_schema, table_name, 'ERROR', f"Failed to grant privileges for {table_schema}.{table_name}. Error: {e}", query)