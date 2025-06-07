-- E2Eテスト用テーブル作成スクリプト
-- 実行順序: 02_create_test_tables.sql

USE TGMATestDB;
GO

-- Client Master Table (顧客マスタ)
IF NOT EXISTS (SELECT *
FROM sys.objects
WHERE object_id = OBJECT_ID(N'[dbo].[client_dm]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[client_dm]
    (
        [client_id] NVARCHAR(50) NOT NULL PRIMARY KEY,
        [client_name] NVARCHAR(100),
        [email] NVARCHAR(100),
        [phone] NVARCHAR(20),
        [address] NVARCHAR(200),
        [registration_date] DATETIME2,
        [status] NVARCHAR(20),
        [created_at] DATETIME2 DEFAULT GETDATE(),
        [updated_at] DATETIME2 DEFAULT GETDATE()
    );
END
GO

-- Client Data Mart (顧客データマート)
IF NOT EXISTS (SELECT *
FROM sys.objects
WHERE object_id = OBJECT_ID(N'[dbo].[ClientDmBx]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[ClientDmBx]
    (
        [id] INT IDENTITY(1,1) PRIMARY KEY,
        [client_id] NVARCHAR(50),
        [segment] NVARCHAR(50),
        [score] DECIMAL(10,2),
        [last_transaction_date] DATETIME2,
        [total_amount] DECIMAL(18,2),
        [processed_date] DATETIME2 DEFAULT GETDATE(),
        [data_source] NVARCHAR(50)
    );
END
GO

-- Point Grant Email Log (ポイント付与メール履歴)
IF NOT EXISTS (SELECT *
FROM sys.objects
WHERE object_id = OBJECT_ID(N'[dbo].[point_grant_email]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[point_grant_email]
    (
        [id] INT IDENTITY(1,1) PRIMARY KEY,
        [client_id] NVARCHAR(50),
        [email] NVARCHAR(100),
        [points_granted] INT,
        [email_sent_date] DATETIME2,
        [campaign_id] NVARCHAR(50),
        [status] NVARCHAR(20),
        [created_at] DATETIME2 DEFAULT GETDATE()
    );
END
GO

-- Marketing Client Data Mart (マーケティング顧客データマート)
IF NOT EXISTS (SELECT *
FROM sys.objects
WHERE object_id = OBJECT_ID(N'[dbo].[marketing_client_dm]') AND type in (N'U'))
BEGIN
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
        [created_at] DATETIME2 DEFAULT GETDATE(),
        [updated_at] DATETIME2 DEFAULT GETDATE()
    );
END
GO

-- Staging Tables for ETL processes
IF NOT EXISTS (SELECT *
FROM sys.objects
WHERE object_id = OBJECT_ID(N'[staging].[client_raw]') AND type in (N'U'))
BEGIN
    CREATE TABLE [staging].[client_raw]
    (
        [raw_id] INT IDENTITY(1,1) PRIMARY KEY,
        [source_system] NVARCHAR(50),
        [raw_data] NVARCHAR(MAX),
        [file_name] NVARCHAR(100),
        [processed] BIT DEFAULT 0,
        [created_at] DATETIME2 DEFAULT GETDATE()
    );
END
GO

-- ETL Control Table
IF NOT EXISTS (SELECT *
FROM sys.objects
WHERE object_id = OBJECT_ID(N'[etl].[pipeline_execution_log]') AND type in (N'U'))
BEGIN
    CREATE TABLE [etl].[pipeline_execution_log]
    (
        [execution_id] UNIQUEIDENTIFIER DEFAULT NEWID() PRIMARY KEY,
        [pipeline_name] NVARCHAR(100),
        [activity_name] NVARCHAR(100),
        [start_time] DATETIME2,
        [end_time] DATETIME2,
        [status] NVARCHAR(20),
        [input_rows] INT,
        [output_rows] INT,
        [error_message] NVARCHAR(MAX),
        [created_at] DATETIME2 DEFAULT GETDATE()
    );
END
GO

-- Test Data Validation View
CREATE OR ALTER VIEW [dbo].[v_test_data_summary]
AS
                    SELECT
            'client_dm' as table_name,
            COUNT(*) as record_count,
            MAX(created_at) as last_updated
        FROM [dbo].[client_dm]
    UNION ALL
        SELECT
            'ClientDmBx' as table_name,
            COUNT(*) as record_count,
            MAX(processed_date) as last_updated
        FROM [dbo].[ClientDmBx]
    UNION ALL
        SELECT
            'point_grant_email' as table_name,
            COUNT(*) as record_count,
            MAX(created_at) as last_updated
        FROM [dbo].[point_grant_email]
    UNION ALL
        SELECT
            'marketing_client_dm' as table_name,
            COUNT(*) as record_count,
            MAX(updated_at) as last_updated
        FROM [dbo].[marketing_client_dm];
GO

PRINT 'Test tables created successfully';
GO
