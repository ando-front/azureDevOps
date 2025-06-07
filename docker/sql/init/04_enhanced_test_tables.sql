-- E2Eテスト強化用テーブル作成スクリプト
-- 実行順序: 04_enhanced_test_tables.sql
-- 目的: E2Eテストで失敗したテーブルの確実な作成と検証

USE TGMATestDB;
GO

-- =============================================================================
-- 基本テーブル存在確認と作成（冪等性対応）
-- =============================================================================

-- client_dm テーブルの拡張確認
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[client_dm]') AND type in (N'U'))
BEGIN
    PRINT 'Creating client_dm table...';
    CREATE TABLE [dbo].[client_dm]
    (
        [client_id] NVARCHAR(50) NOT NULL PRIMARY KEY,
        [client_name] NVARCHAR(100),
        [email] NVARCHAR(100),
        [phone] NVARCHAR(20),
        [address] NVARCHAR(200),
        [registration_date] DATETIME2,
        [status] NVARCHAR(20) DEFAULT 'ACTIVE',
        [created_at] DATETIME2 DEFAULT GETDATE(),
        [updated_at] DATETIME2 DEFAULT GETDATE()
    );
    PRINT 'client_dm table created successfully';
END
ELSE
BEGIN
    PRINT 'client_dm table already exists - verifying structure...';
    
    -- 必要なカラムが存在することを確認
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'client_dm' AND COLUMN_NAME = 'status')
    BEGIN
        ALTER TABLE [dbo].[client_dm] ADD [status] NVARCHAR(20) DEFAULT 'ACTIVE';
        PRINT 'Added status column to client_dm table';
    END
END
GO

-- ClientDmBx テーブルの拡張確認
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ClientDmBx]') AND type in (N'U'))
BEGIN
    PRINT 'Creating ClientDmBx table...';
    CREATE TABLE [dbo].[ClientDmBx]
    (
        [id] INT IDENTITY(1,1) PRIMARY KEY,
        [client_id] NVARCHAR(50),
        [segment] NVARCHAR(50),
        [score] DECIMAL(10,2),
        [last_transaction_date] DATETIME2,
        [total_amount] DECIMAL(18,2),
        [processed_date] DATETIME2 DEFAULT GETDATE(),
        [data_source] NVARCHAR(50) DEFAULT 'ETL_PIPELINE',
        [bx_flag] BIT DEFAULT 1,
        [created_at] DATETIME2 DEFAULT GETDATE(),
        [updated_at] DATETIME2 DEFAULT GETDATE()
    );
    PRINT 'ClientDmBx table created successfully';
END
ELSE
BEGIN
    PRINT 'ClientDmBx table already exists - verifying structure...';
    
    -- 必要なカラムが存在することを確認
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'ClientDmBx' AND COLUMN_NAME = 'bx_flag')
    BEGIN
        ALTER TABLE [dbo].[ClientDmBx] ADD [bx_flag] BIT DEFAULT 1;
        PRINT 'Added bx_flag column to ClientDmBx table';
    END
    
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'ClientDmBx' AND COLUMN_NAME = 'updated_at')
    BEGIN
        ALTER TABLE [dbo].[ClientDmBx] ADD [updated_at] DATETIME2 DEFAULT GETDATE();
        PRINT 'Added updated_at column to ClientDmBx table';
    END
END
GO

-- point_grant_email テーブルの拡張確認
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[point_grant_email]') AND type in (N'U'))
BEGIN
    PRINT 'Creating point_grant_email table...';
    CREATE TABLE [dbo].[point_grant_email]
    (
        [id] INT IDENTITY(1,1) PRIMARY KEY,
        [client_id] NVARCHAR(50),
        [email] NVARCHAR(100),
        [points_granted] INT DEFAULT 0,
        [email_sent_date] DATETIME2,
        [campaign_id] NVARCHAR(50),
        [status] NVARCHAR(20) DEFAULT 'PENDING',
        [grant_reason] NVARCHAR(100),
        [created_at] DATETIME2 DEFAULT GETDATE(),
        [updated_at] DATETIME2 DEFAULT GETDATE()
    );
    PRINT 'point_grant_email table created successfully';
END
ELSE
BEGIN
    PRINT 'point_grant_email table already exists - verifying structure...';
    
    -- 必要なカラムが存在することを確認
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'point_grant_email' AND COLUMN_NAME = 'grant_reason')
    BEGIN
        ALTER TABLE [dbo].[point_grant_email] ADD [grant_reason] NVARCHAR(100);
        PRINT 'Added grant_reason column to point_grant_email table';
    END
END
GO

-- marketing_client_dm テーブルの拡張確認
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[marketing_client_dm]') AND type in (N'U'))
BEGIN
    PRINT 'Creating marketing_client_dm table...';
    CREATE TABLE [dbo].[marketing_client_dm]
    (
        [id] INT IDENTITY(1,1) PRIMARY KEY,
        [client_id] NVARCHAR(50),
        [marketing_segment] NVARCHAR(50),
        [preference_category] NVARCHAR(50),
        [engagement_score] DECIMAL(5,2),
        [last_campaign_response] DATETIME2,
        [opt_in_email] BIT DEFAULT 1,
        [opt_in_sms] BIT DEFAULT 0,
        [customer_value] NVARCHAR(20) DEFAULT 'STANDARD',
        [created_at] DATETIME2 DEFAULT GETDATE(),
        [updated_at] DATETIME2 DEFAULT GETDATE()
    );
    PRINT 'marketing_client_dm table created successfully';
END
ELSE
BEGIN
    PRINT 'marketing_client_dm table already exists - verifying structure...';
    
    -- 必要なカラムが存在することを確認
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'marketing_client_dm' AND COLUMN_NAME = 'customer_value')
    BEGIN
        ALTER TABLE [dbo].[marketing_client_dm] ADD [customer_value] NVARCHAR(20) DEFAULT 'STANDARD';
        PRINT 'Added customer_value column to marketing_client_dm table';
    END
END
GO

-- =============================================================================
-- E2Eテスト専用のテーブル作成
-- =============================================================================

-- E2Eテスト実行ログテーブル
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[etl].[e2e_test_execution_log]') AND type in (N'U'))
BEGIN
    PRINT 'Creating e2e_test_execution_log table...';
    CREATE TABLE [etl].[e2e_test_execution_log]
    (
        [execution_id] UNIQUEIDENTIFIER DEFAULT NEWID() PRIMARY KEY,
        [test_suite] NVARCHAR(100),
        [test_name] NVARCHAR(200),
        [test_status] NVARCHAR(20),
        [start_time] DATETIME2,
        [end_time] DATETIME2,
        [duration_seconds] INT,
        [error_message] NVARCHAR(MAX),
        [test_data_count] INT,
        [created_at] DATETIME2 DEFAULT GETDATE()
    );
    PRINT 'e2e_test_execution_log table created successfully';
END
GO

-- テストデータ管理テーブル
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[staging].[test_data_management]') AND type in (N'U'))
BEGIN
    PRINT 'Creating test_data_management table...';
    CREATE TABLE [staging].[test_data_management]
    (
        [data_id] INT IDENTITY(1,1) PRIMARY KEY,
        [table_name] NVARCHAR(100),
        [test_scenario] NVARCHAR(100),
        [test_data] NVARCHAR(MAX),
        [data_status] NVARCHAR(20) DEFAULT 'ACTIVE',
        [cleanup_required] BIT DEFAULT 1,
        [created_at] DATETIME2 DEFAULT GETDATE(),
        [cleaned_at] DATETIME2
    );
    PRINT 'test_data_management table created successfully';
END
GO

-- =============================================================================
-- テストデータ検証用ビューの作成
-- =============================================================================

-- 統合テストデータサマリービュー
CREATE OR ALTER VIEW [dbo].[v_e2e_test_data_summary]
AS
SELECT 
    'client_dm' as table_name,
    COUNT(*) as record_count,
    MAX(created_at) as last_updated,
    'Core client data' as description
FROM [dbo].[client_dm]
WHERE status = 'ACTIVE'

UNION ALL

SELECT 
    'ClientDmBx' as table_name,
    COUNT(*) as record_count,
    MAX(processed_date) as last_updated,
    'Client data mart with BX flags' as description
FROM [dbo].[ClientDmBx]
WHERE bx_flag = 1

UNION ALL

SELECT 
    'point_grant_email' as table_name,
    COUNT(*) as record_count,
    MAX(created_at) as last_updated,
    'Point grant email logs' as description
FROM [dbo].[point_grant_email]

UNION ALL

SELECT 
    'marketing_client_dm' as table_name,
    COUNT(*) as record_count,
    MAX(created_at) as last_updated,
    'Marketing client data mart' as description
FROM [dbo].[marketing_client_dm];
GO

-- テーブル構造検証用プロシージャ
CREATE OR ALTER PROCEDURE [dbo].[sp_validate_e2e_table_structure]
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @missing_tables TABLE (table_name NVARCHAR(100), issue NVARCHAR(200));
    
    -- 必須テーブルの存在確認
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[client_dm]'))
        INSERT INTO @missing_tables VALUES ('client_dm', 'Table does not exist');
    
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ClientDmBx]'))
        INSERT INTO @missing_tables VALUES ('ClientDmBx', 'Table does not exist');
    
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[point_grant_email]'))
        INSERT INTO @missing_tables VALUES ('point_grant_email', 'Table does not exist');
    
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[marketing_client_dm]'))
        INSERT INTO @missing_tables VALUES ('marketing_client_dm', 'Table does not exist');
    
    -- 必須カラムの存在確認
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'client_dm' AND COLUMN_NAME = 'status')
        INSERT INTO @missing_tables VALUES ('client_dm', 'Missing status column');
    
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'ClientDmBx' AND COLUMN_NAME = 'bx_flag')
        INSERT INTO @missing_tables VALUES ('ClientDmBx', 'Missing bx_flag column');
    
    -- 結果出力
    IF EXISTS (SELECT * FROM @missing_tables)
    BEGIN
        SELECT 'VALIDATION FAILED' as status, table_name, issue FROM @missing_tables;
    END
    ELSE
    BEGIN
        SELECT 'VALIDATION PASSED' as status, 'All required tables and columns exist' as message;
    END
END
GO

-- 初期検証実行
PRINT 'Running initial table structure validation...';
EXEC [dbo].[sp_validate_e2e_table_structure];

PRINT 'Enhanced E2E test tables setup completed successfully';
GO
