# pgshift

pgShift is a open source python script designed to migrate DDL (Data Definition Language) and DML (Data Manipulation Language) changes between PostgreSQL databases. It simplifies database migration by providing utilities for managing schema updates, data transfer, and maintaining database integrity.

## Features

### DDL Migrations
- Create tables (if not exist)
- Update schema definitions, constraints, indexes, and enums.

### DML Migrations
- Fetch and insert data between databases.
- Handle UPSERT operations (based on primary key)
- Log migration activities.

### Backup Support
- Create backup of tables before applying changes.

## Installation

pgShift uses a single package, `psycopg2`, to handle all database operations. To install, run:

```sh
pip install psycopg2
```

## How to Use

### Step 1: Prepare the Databases

Before using **pgShift**, ensure the schemas are properly set up in both the source and destination databases.

### Step 2: Create the Migration Activity Table

In the **source database**, create the `tbl_migration_activity` table using the following command:

```sql
CREATE TABLE IF NOT EXISTS public.tbl_migration_activity (
    migration_activity_id UUID NOT NULL DEFAULT gen_random_uuid(),
    db_name TEXT COLLATE pg_catalog."default",
    table_schema TEXT COLLATE pg_catalog."default",
    table_name TEXT COLLATE pg_catalog."default",
    requires_data_migration BOOLEAN,
    truncate_load BOOLEAN,
    sequence INTEGER,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by TEXT COLLATE pg_catalog."default",
    CONSTRAINT pk_migration_activity_id PRIMARY KEY (migration_activity_id)
);
```

This table is used to define the migration strategy for each table, specifying:

- **requires_data_migration:** Whether the table data needs to be migrated.
- **truncate_load:** Indicates if the table should be truncat and load.
- **sequence**: Specifies the order of table migration, ensuring that tables with dependencies (e.g., foreign keys) are migrated in the correct sequence.

**Note:** For tables using UPSERT operations, ensure they have a primary key. If no primary key is present, use the truncate and load approach.

### Step 3: Create the Migration Log Table

Create the `tbl_migration_log` table in both the source and destination databases using the command below:

```sql
CREATE TABLE IF NOT EXISTS trans_disclosure.tbl_migration_log (
    migration_log_id UUID NOT NULL DEFAULT gen_random_uuid(),
    table_schema TEXT COLLATE pg_catalog."default" NOT NULL,
    table_name TEXT COLLATE pg_catalog."default" NOT NULL,
    status TEXT COLLATE pg_catalog."default" NOT NULL,
    message TEXT COLLATE pg_catalog."default",
    script TEXT COLLATE pg_catalog."default",
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by TEXT COLLATE pg_catalog."default",
    CONSTRAINT pk_migration_log_id PRIMARY KEY (migration_log_id)
);
```

This table logs details of the migration process, including:

- **status:** Indicates success or failure of the operation.
- **message:** Provides additional information about the operation.
- **script:** Stores the SQL script executed during the migration.

These logs help track migration activities and troubleshoot any issues.

### Step 4: Update `config.yaml`

Configure the `config.yaml` file to specify the source and destination database details, along with the required table settings. Below is an example configuration:

```yaml
# Source Database Configuration
source:
  environment: development
  host: localhost
  port: 5432
  user: postgres
  password: postgres
  database: postgres

# Destination Database Configuration
destination:
  environment: production
  host: localhost
  port: 5432
  user: postgres
  password: postgres
  database: postgres

# Table Configuration
table:
  migration_log:
    table_name: tbl_migration_log
    table_schema: public
  migration_activity:
    table_name: tbl_migration_activity
    table_schema: public
```

### Configuration Details

1. **Source Database:**
    - Defines the connection details for the database where the data is being migrated from.
    - The `environment` parameter helps distinguish between development, staging, or other environments.

2. **Destination Database:**
    - Specifies the target database where data will be migrated.
    - Use credentials and host information for your production or staging environment.

3. **Table Configuration:**
    - `migration_log`: Specifies the schema and table name for logging migration activities.
    - `migration_activity`: Defines the schema and table name for tracking migration plans and settings.

Ensure the `config.yaml` file is updated with your specific database details before running **pgShift**.

## Contributing

Contributions are welcome and encouraged! Here are some ways you can help improve **pgShift**:

- [Report Bugs](https://github.com/jineshnagori/pgshift/issues): If you encounter any issues, let us know.
- Fix Bugs: Identify and fix issues, then [submit a pull request](https://github.com/jineshnagori/pgshift/pulls).
- Improve Documentation: Clarify, correct, or expand the existing documentation.
- Suggest or Add Features: Propose new ideas or implement features to enhance the tool.

### Getting Started with Development

Follow these steps to set up the project locally:

1. Clone the repository:
```sh
git clone https://github.com/jineshnagori/pgshift.git
```

2. Navigate to the project directory:
```sh
cd pgshift
```

3. Install the required package:
```sh
pip install pycopg2
```

Once set up, you’re ready to start contributing to **pgShift**!