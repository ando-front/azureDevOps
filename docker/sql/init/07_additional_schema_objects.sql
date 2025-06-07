-- 07_additional_schema_objects.sql
-- Additional schema objects for E2E tests

USE TGMATestDB;

-- Create staging schema if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'staging')
BEGIN
    EXEC('CREATE SCHEMA staging')
END;

-- Create test_data_management table
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'staging' AND TABLE_NAME = 'test_data_management')
BEGIN
    CREATE TABLE staging.test_data_management (
        TestDataID INT IDENTITY(1,1) PRIMARY KEY,
        TestSuite NVARCHAR(100) NOT NULL,
        TableName NVARCHAR(100) NOT NULL,
        RecordCount INT NOT NULL,
        LastUpdated DATETIME2 DEFAULT GETDATE()
    );
END;

PRINT 'Staging schema and tables created successfully';
GO

-- Create GetE2ETestSummary procedure
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'GetE2ETestSummary')
    DROP PROCEDURE GetE2ETestSummary;
GO

CREATE PROCEDURE GetE2ETestSummary
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        'client_dm' as TableName,
        COUNT(*) as TotalRecords,
        COUNT(CASE WHEN client_id LIKE 'E2E_%' THEN 1 END) as E2ERecords
    FROM client_dm
    
    UNION ALL
    
    SELECT 
        'ClientDmBx' as TableName,
        COUNT(*) as TotalRecords,
        COUNT(CASE WHEN client_id LIKE 'E2E_%' THEN 1 END) as E2ERecords
    FROM ClientDmBx
    
    UNION ALL
    
    SELECT 
        'point_grant_email' as TableName,
        COUNT(*) as TotalRecords,
        COUNT(CASE WHEN client_id LIKE 'E2E_%' THEN 1 END) as E2ERecords
    FROM point_grant_email
    
    UNION ALL
    
    SELECT 
        'marketing_client_dm' as TableName,
        COUNT(*) as TotalRecords,
        COUNT(CASE WHEN client_id LIKE 'E2E_%' THEN 1 END) as E2ERecords
    FROM marketing_client_dm;
END;
GO

PRINT 'GetE2ETestSummary procedure created successfully';
