This folder contains a minimal PL/SQL ETL example: extract from a staging table, transform, and load into dimension and fact tables.

Files:
- create_schema.sql  -- DDL for staging, dims, fact, sequences
- load_staging.sql   -- small INSERTs to populate staging with sample data
- etl_package.pks    -- package specification for ETL
- etl_package.pk b   -- package body implementing the ETL logic
- run_etl.sql        -- script to run the ETL package

Instructions:
1. Run `create_schema.sql` in your Oracle schema to create tables and sequences.
2. Populate staging with `load_staging.sql`.
3. Run `run_etl.sql` to execute the ETL.

This is a minimal, self-contained example suitable for local testing with Oracle XE or an Oracle Docker image.
