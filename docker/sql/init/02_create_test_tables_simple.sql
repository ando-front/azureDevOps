-- 02_create_test_tables_simple.sql
-- 最もシンプルなテーブル作成（スキーマエラー完全回避版）

USE TGMATestDB;

-- 既存テーブル削除
IF OBJECT_ID('dbo.client_dm', 'U') IS NOT NULL DROP TABLE dbo.client_dm;
IF OBJECT_ID('dbo.ClientDmBx', 'U') IS NOT NULL DROP TABLE dbo.ClientDmBx;
IF OBJECT_ID('dbo.point_grant_email', 'U') IS NOT NULL DROP TABLE dbo.point_grant_email;
IF OBJECT_ID('dbo.marketing_client_dm', 'U') IS NOT NULL DROP TABLE dbo.marketing_client_dm;
IF OBJECT_ID('dbo.test_data_management', 'U') IS NOT NULL DROP TABLE dbo.test_data_management;
IF OBJECT_ID('dbo.pipeline_execution_log', 'U') IS NOT NULL DROP TABLE dbo.pipeline_execution_log;

-- client_dmテーブル作成（テストコードが期待するcreated_dateカラム付き）
CREATE TABLE dbo.client_dm (
    client_id NVARCHAR(50) NOT NULL PRIMARY KEY,
    client_name NVARCHAR(100),
    email NVARCHAR(100),
    phone NVARCHAR(20),
    address NVARCHAR(200),
    registration_date DATETIME2,
    status NVARCHAR(20),
    created_date DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE()
);

-- ClientDmBxテーブル作成
CREATE TABLE dbo.ClientDmBx (
    id INT IDENTITY(1,1) PRIMARY KEY,
    client_id NVARCHAR(50),
    segment NVARCHAR(50),
    score DECIMAL(10,2),
    last_transaction_date DATETIME2,
    total_amount DECIMAL(18,2),
    processed_date DATETIME2 DEFAULT GETDATE(),
    data_source NVARCHAR(50)
);

-- point_grant_emailテーブル作成
CREATE TABLE dbo.point_grant_email (
    id INT IDENTITY(1,1) PRIMARY KEY,
    client_id NVARCHAR(50),
    email NVARCHAR(100),
    points_granted INT,
    email_sent_date DATETIME2,
    campaign_id NVARCHAR(50),
    status NVARCHAR(20),
    created_at DATETIME2 DEFAULT GETDATE()
);

-- marketing_client_dmテーブル作成
CREATE TABLE dbo.marketing_client_dm (
    id INT IDENTITY(1,1) PRIMARY KEY,
    client_id NVARCHAR(50),
    marketing_segment NVARCHAR(50),
    preference_category NVARCHAR(50),
    engagement_score DECIMAL(5,2),
    last_campaign_response DATETIME2,
    opt_in_email BIT DEFAULT 1,
    opt_in_sms BIT DEFAULT 0,
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE()
);

-- test_data_managementテーブル作成（dboスキーマ使用）
CREATE TABLE dbo.test_data_management (
    TestDataID INT IDENTITY(1,1) PRIMARY KEY,
    TestSuite NVARCHAR(100) NOT NULL,
    TableName NVARCHAR(100) NOT NULL,
    RecordCount INT NOT NULL,
    LastUpdated DATETIME2 DEFAULT GETDATE()
);

-- pipeline_execution_logテーブル作成（dboスキーマ使用）
CREATE TABLE dbo.pipeline_execution_log (
    log_id INT IDENTITY(1,1) PRIMARY KEY,
    pipeline_name NVARCHAR(100) NOT NULL,
    execution_start DATETIME2 DEFAULT GETDATE(),
    execution_end DATETIME2,
    status NVARCHAR(20),
    records_processed INT,
    error_message NVARCHAR(MAX)
);

PRINT 'All tables created successfully with dbo schema only - no schema creation errors';
