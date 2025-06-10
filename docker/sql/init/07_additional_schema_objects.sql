-- 07_additional_schema_objects.sql
-- Additional schema objects for E2E tests - Simplified to use dbo schema only

USE TGMATestDB;

-- Simplified approach: use dbo schema only to avoid schema creation issues
-- This ensures compatibility with all SQL Server versions in Docker

-- Create test_data_management table (using dbo schema)
CREATE TABLE dbo.test_data_management (
    TestDataID INT IDENTITY(1,1) PRIMARY KEY,
    TestSuite NVARCHAR(100) NOT NULL,
    TableName NVARCHAR(100) NOT NULL,
    RecordCount INT NOT NULL,
    LastUpdated DATETIME2 DEFAULT GETDATE()
);

-- Create pipeline_execution_log table for ETL tracking (using dbo schema)
CREATE TABLE dbo.pipeline_execution_log (
    log_id INT IDENTITY(1,1) PRIMARY KEY,
    pipeline_name NVARCHAR(100) NOT NULL,
    execution_start DATETIME2 DEFAULT GETDATE(),
    execution_end DATETIME2,
    status NVARCHAR(20),
    records_processed INT,
    error_message NVARCHAR(MAX)
);

PRINT 'Additional schema objects created successfully in dbo schema';

PRINT 'Additional schema objects created successfully';
