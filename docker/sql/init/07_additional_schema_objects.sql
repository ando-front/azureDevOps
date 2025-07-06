-- 07_additional_schema_objects.sql
-- Additional schema objects for E2E tests - Simplified to use dbo schema only

USE TGMATestDB;

-- Simplified approach: use dbo schema only to avoid schema creation issues
-- This ensures compatibility with all SQL Server versions in Docker

-- Create test_data_management table (using dbo schema) - with idempotent check
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'test_data_management' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.test_data_management (
        TestDataID INT IDENTITY(1,1) PRIMARY KEY,
        TestSuite NVARCHAR(100) NOT NULL,
        TableName NVARCHAR(100) NOT NULL,
        RecordCount INT NOT NULL,
        LastUpdated DATETIME2 DEFAULT GETDATE()
    );
    PRINT 'test_data_management table created successfully';
END
ELSE
BEGIN
    PRINT 'test_data_management table already exists - skipping creation';
END

-- Create pipeline_execution_log table for ETL tracking (using dbo schema) - with idempotent check
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'pipeline_execution_log' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.pipeline_execution_log (
        log_id INT IDENTITY(1,1) PRIMARY KEY,
        pipeline_name NVARCHAR(100) NOT NULL,
        execution_start DATETIME2 DEFAULT GETDATE(),
        execution_end DATETIME2,
        status NVARCHAR(20),
        records_processed INT,
        error_message NVARCHAR(MAX)
    );
    PRINT 'pipeline_execution_log table created successfully';
END
ELSE
BEGIN
    PRINT 'pipeline_execution_log table already exists - skipping creation';
END

PRINT 'Additional schema objects created successfully in dbo schema';

PRINT 'Additional schema objects created successfully';
