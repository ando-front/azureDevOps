-- 02_create_staging_tables.sql
-- Staging tables for E2E tests

USE TGMATestDB;
GO

-- Create staging.client_raw table
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'staging')
BEGIN
    EXEC('CREATE SCHEMA staging');
END
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[staging].[client_raw]') AND type in (N'U'))
BEGIN
    CREATE TABLE [staging].[client_raw]
    (
        [source_system] NVARCHAR(50) NOT NULL,
        [raw_data] NVARCHAR(MAX),
        [file_name] NVARCHAR(255),
        [processed] BIT DEFAULT 0,
        [created_at] DATETIME2 DEFAULT GETDATE()
    );
    PRINT 'staging.client_raw table created successfully';
END
ELSE
BEGIN
    PRINT 'staging.client_raw table already exists.';
END
GO

-- Create etl.pipeline_execution_log table (if not already created by 07_additional_schema_objects.sql)
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'etl')
BEGIN
    EXEC('CREATE SCHEMA etl');
END
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[etl].[pipeline_execution_log]') AND type in (N'U'))
BEGIN
    CREATE TABLE [etl].[pipeline_execution_log]
    (
        [log_id] INT IDENTITY(1,1) PRIMARY KEY,
        [pipeline_name] NVARCHAR(100) NOT NULL,
        [activity_name] NVARCHAR(100),
        [start_time] DATETIME2,
        [end_time] DATETIME2,
        [status] NVARCHAR(20),
        [input_rows] INT,
        [output_rows] INT,
        [error_message] NVARCHAR(MAX),
        [created_at] DATETIME2 DEFAULT GETDATE()
    );
    PRINT 'etl.pipeline_execution_log table created successfully';
END
ELSE
BEGIN
    PRINT 'etl.pipeline_execution_log table already exists.';
END
GO
