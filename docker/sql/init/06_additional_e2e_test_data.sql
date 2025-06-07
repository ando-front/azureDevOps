-- 06_additional_e2e_test_data.sql
-- Additional E2E test data to meet test requirements

USE TGMATestDB;

-- Add more client_dm test data (need at least 12 records)
INSERT INTO client_dm (client_id, client_name, email, phone, address, registration_date, status, created_at, updated_at)
VALUES 
    ('E2E006', 'E2E Test Client 6', 'test6@example.com', '090-1234-5566', 'Test Address 6', GETDATE(), 'Active', GETDATE(), GETDATE()),
    ('E2E007', 'E2E Test Client 7', 'test7@example.com', '090-1234-5577', 'Test Address 7', GETDATE(), 'Active', GETDATE(), GETDATE()),
    ('E2E008', 'E2E Test Client 8', 'test8@example.com', '090-1234-5588', 'Test Address 8', GETDATE(), 'Active', GETDATE(), GETDATE()),
    ('E2E009', 'E2E Test Client 9', 'test9@example.com', '090-1234-5599', 'Test Address 9', GETDATE(), 'Active', GETDATE(), GETDATE()),
    ('E2E010', 'E2E Test Client 10', 'test10@example.com', '090-1234-5510', 'Test Address 10', GETDATE(), 'Active', GETDATE(), GETDATE()),
    ('E2E011', 'E2E Test Client 11', 'test11@example.com', '090-1234-5511', 'Test Address 11', GETDATE(), 'Active', GETDATE(), GETDATE()),
    ('E2E012', 'E2E Test Client 12', 'test12@example.com', '090-1234-5512', 'Test Address 12', GETDATE(), 'Active', GETDATE(), GETDATE()),
    ('E2E013', 'E2E Test Client 13', 'test13@example.com', '090-1234-5513', 'Test Address 13', GETDATE(), 'Active', GETDATE(), GETDATE()),
    ('E2E014', 'E2E Test Client 14', 'test14@example.com', '090-1234-5514', 'Test Address 14', GETDATE(), 'Active', GETDATE(), GETDATE()),
    ('E2E015', 'E2E Test Client 15', 'test15@example.com', '090-1234-5515', 'Test Address 15', GETDATE(), 'Active', GETDATE(), GETDATE());

-- Create ETL schema if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'etl')
BEGIN
    EXEC('CREATE SCHEMA etl')
END;

-- Create E2E test execution log table
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'etl' AND TABLE_NAME = 'e2e_test_execution_log')
BEGIN
    CREATE TABLE etl.e2e_test_execution_log (
        ExecutionID INT IDENTITY(1,1) PRIMARY KEY,
        TestName NVARCHAR(255) NOT NULL,
        ExecutionTime DATETIME2 DEFAULT GETDATE(),
        Status NVARCHAR(50) NOT NULL, -- 'Success', 'Failed', 'Skipped'
        Duration FLOAT, -- in seconds
        ErrorMessage NVARCHAR(MAX),
        TestData NVARCHAR(MAX) -- JSON format for test metadata
    );
END;

-- Create ValidateE2ETestData stored procedure
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'ValidateE2ETestData')
    DROP PROCEDURE ValidateE2ETestData;
GO

CREATE PROCEDURE ValidateE2ETestData
    @TableName NVARCHAR(128),
    @ExpectedCount INT = 0,
    @ValidationResult BIT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @ActualCount INT;
    DECLARE @SQL NVARCHAR(MAX);
    
    SET @SQL = N'SELECT @Count = COUNT(*) FROM ' + QUOTENAME(@TableName);
    
    EXEC sp_executesql @SQL, N'@Count INT OUTPUT', @Count = @ActualCount OUTPUT;
    
    IF @ActualCount >= @ExpectedCount
        SET @ValidationResult = 1;
    ELSE
        SET @ValidationResult = 0;
        
    SELECT 
        @TableName as TableName,
        @ActualCount as ActualCount,
        @ExpectedCount as ExpectedCount,
        @ValidationResult as ValidationPassed;
END;
GO

PRINT 'Additional E2E test data and structures created successfully';
