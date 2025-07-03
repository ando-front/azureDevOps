-- E2E テスト用データベース初期化スクリプト
-- このスクリプトはSQL Server起動後に自動実行されます

-- TGMATestDB データベースの作成
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'TGMATestDB')
BEGIN
    CREATE DATABASE TGMATestDB;
    PRINT 'TGMATestDB database created successfully';
END
ELSE
BEGIN
    PRINT 'TGMATestDB database already exists';
END
GO

-- SynapseTestDB データベースの作成
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'SynapseTestDB')
BEGIN
    CREATE DATABASE SynapseTestDB;
    PRINT 'SynapseTestDB database created successfully';
END
ELSE
BEGIN
    PRINT 'SynapseTestDB database already exists';
END
GO

-- TGMATestDB にテーブルを作成
USE TGMATestDB;
GO
-- ClientDmBxテーブルを削除して再作成
IF EXISTS (SELECT * FROM sysobjects WHERE name='ClientDmBx' AND xtype='U')
BEGIN
    DROP TABLE ClientDmBx;
    PRINT 'Dropped existing ClientDmBx table in TGMATestDB';
END
CREATE TABLE ClientDmBx (
    id INT IDENTITY(1,1) PRIMARY KEY,
    client_id NVARCHAR(50),
    name NVARCHAR(100),
    segment NVARCHAR(50),
    score INT,
    last_transaction_date DATE,
    total_amount DECIMAL(15,2),
    processed_date DATETIME2,
    data_source NVARCHAR(50)
);
PRINT 'ClientDmBx table created in TGMATestDB';
GO

-- SynapseTestDB にテーブルを作成
USE SynapseTestDB;
GO
-- ClientDmBxテーブルを削除して再作成
IF EXISTS (SELECT * FROM sysobjects WHERE name='ClientDmBx' AND xtype='U')
BEGIN
    DROP TABLE ClientDmBx;
    PRINT 'Dropped existing ClientDmBx table in SynapseTestDB';
END
CREATE TABLE ClientDmBx (
    id INT IDENTITY(1,1) PRIMARY KEY,
    client_id NVARCHAR(50),
    name NVARCHAR(100),
    segment NVARCHAR(50),
    score INT,
    last_transaction_date DATE,
    total_amount DECIMAL(15,2),
    processed_date DATETIME2,
    data_source NVARCHAR(50)
);
PRINT 'ClientDmBx table created in SynapseTestDB';

-- client_dm テーブルを作成
IF EXISTS (SELECT * FROM sysobjects WHERE name='client_dm' AND xtype='U')
BEGIN
    DROP TABLE client_dm;
    PRINT 'Dropped existing client_dm table in SynapseTestDB';
END
CREATE TABLE client_dm (
    id INT IDENTITY(1,1) PRIMARY KEY,
    client_id NVARCHAR(50),
    name NVARCHAR(100),
    email NVARCHAR(255),
    segment NVARCHAR(50),
    last_transaction_date DATETIME2,
    created_date DATETIME2,
    updated_date DATETIME2
);
PRINT 'client_dm table created in SynapseTestDB';

-- point_grant_email テーブルを作成
IF EXISTS (SELECT * FROM sysobjects WHERE name='point_grant_email' AND xtype='U')
BEGIN
    DROP TABLE point_grant_email;
    PRINT 'Dropped existing point_grant_email table in SynapseTestDB';
END
CREATE TABLE point_grant_email (
    id INT IDENTITY(1,1) PRIMARY KEY,
    client_id NVARCHAR(50),
    email NVARCHAR(255),
    points_granted INT,
    email_sent_date DATETIME2,
    campaign_id NVARCHAR(50),
    status NVARCHAR(50),
    created_at DATETIME2
);
PRINT 'point_grant_email table created in SynapseTestDB';

-- marketing_client_dm テーブルを作成  
IF EXISTS (SELECT * FROM sysobjects WHERE name='marketing_client_dm' AND xtype='U')
BEGIN
    DROP TABLE marketing_client_dm;
    PRINT 'Dropped existing marketing_client_dm table in SynapseTestDB';
END
CREATE TABLE marketing_client_dm (
    id INT IDENTITY(1,1) PRIMARY KEY,
    client_id NVARCHAR(50),
    segment NVARCHAR(50),
    marketing_segment NVARCHAR(50),
    preference_category NVARCHAR(50),
    engagement_score DECIMAL(5,2),
    last_campaign_date DATETIME2,
    last_campaign_response DATETIME2,
    response_rate DECIMAL(5,2),
    lifetime_value DECIMAL(15,2),
    opt_in_email BIT,
    opt_in_sms BIT,
    created_at DATETIME2,
    updated_at DATETIME2
);
PRINT 'marketing_client_dm table created in SynapseTestDB';

-- e2e_test_execution_log テーブルを作成
IF EXISTS (SELECT * FROM sysobjects WHERE name='e2e_test_execution_log' AND xtype='U')
BEGIN
    DROP TABLE e2e_test_execution_log;
    PRINT 'Dropped existing e2e_test_execution_log table in SynapseTestDB';
END
CREATE TABLE e2e_test_execution_log (
    id INT IDENTITY(1,1) PRIMARY KEY,
    test_name NVARCHAR(255),
    execution_time DATETIME2,
    status NVARCHAR(50),
    result NVARCHAR(MAX)
);
PRINT 'e2e_test_execution_log table created in SynapseTestDB';

-- test_data_management テーブルを作成
IF EXISTS (SELECT * FROM sysobjects WHERE name='test_data_management' AND xtype='U')
BEGIN
    DROP TABLE test_data_management;
    PRINT 'Dropped existing test_data_management table in SynapseTestDB';
END
CREATE TABLE test_data_management (
    id INT IDENTITY(1,1) PRIMARY KEY,
    data_type NVARCHAR(100),
    data_value NVARCHAR(MAX),
    created_date DATETIME2,
    status NVARCHAR(50)
);
PRINT 'test_data_management table created in SynapseTestDB';

-- staging スキーマを作成
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'staging')
BEGIN
    EXEC('CREATE SCHEMA staging');
    PRINT 'staging schema created in SynapseTestDB';
END

-- audit スキーマを作成
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'audit')
BEGIN
    EXEC('CREATE SCHEMA audit');
    PRINT 'audit schema created in SynapseTestDB';
END

-- etl スキーマを作成
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'etl')
BEGIN
    EXEC('CREATE SCHEMA etl');
    PRINT 'etl schema created in SynapseTestDB';
END

-- omni スキーマを作成
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'omni')
BEGIN
    EXEC('CREATE SCHEMA omni');
    PRINT 'omni schema created in SynapseTestDB';
END

-- staging.data_quality_test テーブルを作成
IF EXISTS (SELECT * FROM sys.objects WHERE name='data_quality_test' AND schema_id = SCHEMA_ID('staging'))
BEGIN
    DROP TABLE staging.data_quality_test;
    PRINT 'Dropped existing staging.data_quality_test table in SynapseTestDB';
END
CREATE TABLE staging.data_quality_test (
    id INT IDENTITY(1,1) PRIMARY KEY,
    record_id NVARCHAR(50),
    name NVARCHAR(100),
    email NVARCHAR(255),
    date_field DATE,
    amount DECIMAL(15,2)
);
PRINT 'staging.data_quality_test table created in SynapseTestDB';

-- staging.encryption_test テーブルを作成
IF EXISTS (SELECT * FROM sys.objects WHERE name='encryption_test' AND schema_id = SCHEMA_ID('staging'))
BEGIN
    DROP TABLE staging.encryption_test;
    PRINT 'Dropped existing staging.encryption_test table in SynapseTestDB';
END
CREATE TABLE staging.encryption_test (
    id INT IDENTITY(1,1) PRIMARY KEY,
    record_id NVARCHAR(50),
    data_field NVARCHAR(255),
    created_date DATE
);
PRINT 'staging.encryption_test table created in SynapseTestDB';

-- staging.sensitive_data_test テーブルを作成
IF EXISTS (SELECT * FROM sys.objects WHERE name='sensitive_data_test' AND schema_id = SCHEMA_ID('staging'))
BEGIN
    DROP TABLE staging.sensitive_data_test;
    PRINT 'Dropped existing staging.sensitive_data_test table in SynapseTestDB';
END
CREATE TABLE staging.sensitive_data_test (
    id INT IDENTITY(1,1) PRIMARY KEY,
    customer_id NVARCHAR(50),
    credit_card NVARCHAR(50),
    ssn NVARCHAR(50),
    email NVARCHAR(255)
);
PRINT 'staging.sensitive_data_test table created in SynapseTestDB';

-- audit.system_logs テーブルを作成
IF EXISTS (SELECT * FROM sys.objects WHERE name='system_logs' AND schema_id = SCHEMA_ID('audit'))
BEGIN
    DROP TABLE audit.system_logs;
    PRINT 'Dropped existing audit.system_logs table in SynapseTestDB';
END
CREATE TABLE audit.system_logs (
    id INT IDENTITY(1,1) PRIMARY KEY,
    session_id UNIQUEIDENTIFIER,
    event_type NVARCHAR(100),
    user_id NVARCHAR(100),
    event_timestamp DATETIME2,
    status NVARCHAR(50)
);
PRINT 'audit.system_logs table created in SynapseTestDB';

-- omni.omni_ods_cloak_trn_usageservice テーブルを作成
IF EXISTS (SELECT * FROM sys.objects WHERE name='omni_ods_cloak_trn_usageservice' AND schema_id = SCHEMA_ID('omni'))
BEGIN
    DROP TABLE omni.omni_ods_cloak_trn_usageservice;
    PRINT 'Dropped existing omni.omni_ods_cloak_trn_usageservice table in SynapseTestDB';
END
CREATE TABLE omni.omni_ods_cloak_trn_usageservice (
    id INT IDENTITY(1,1) PRIMARY KEY,
    BX NVARCHAR(50),
    SERVICE_KEY1 NVARCHAR(50),
    SERVICE_KEY_TYPE1 NVARCHAR(10),
    SERVICE_TYPE NVARCHAR(10),
    TRANSFER_TYPE NVARCHAR(10),
    TRANSFER_YMD DATE,
    SERVICE_KEY_START_YMD DATE,
    OUTPUT_DATE DATETIME2,
    USER_KEY NVARCHAR(50),
    USER_KEY_TYPE NVARCHAR(10),
    KEY_3X NVARCHAR(50)
);
PRINT 'omni.omni_ods_cloak_trn_usageservice table created in SynapseTestDB';

-- omni.omni_ods_cloak_trn_usageservice_bx4x_temp テーブルを作成
IF EXISTS (SELECT * FROM sys.objects WHERE name='omni_ods_cloak_trn_usageservice_bx4x_temp' AND schema_id = SCHEMA_ID('omni'))
BEGIN
    DROP TABLE omni.omni_ods_cloak_trn_usageservice_bx4x_temp;
    PRINT 'Dropped existing omni.omni_ods_cloak_trn_usageservice_bx4x_temp table in SynapseTestDB';
END
CREATE TABLE omni.omni_ods_cloak_trn_usageservice_bx4x_temp (
    id INT IDENTITY(1,1) PRIMARY KEY,
    BX NVARCHAR(50),
    SERVICE_KEY1 NVARCHAR(50),
    SERVICE_KEY_TYPE1 NVARCHAR(10),
    SERVICE_TYPE NVARCHAR(10),
    TRANSFER_TYPE NVARCHAR(10),
    TRANSFER_YMD DATE,
    SERVICE_KEY_START_YMD DATE,
    OUTPUT_DATE DATETIME2,
    KEY_4X NVARCHAR(50),
    INDEX_ID INT
);
PRINT 'omni.omni_ods_cloak_trn_usageservice_bx4x_temp table created in SynapseTestDB';

-- omni.omni_ods_cloak_trn_usageservice_bx3xsaid_temp テーブルを作成
IF EXISTS (SELECT * FROM sys.objects WHERE name='omni_ods_cloak_trn_usageservice_bx3xsaid_temp' AND schema_id = SCHEMA_ID('omni'))
BEGIN
    DROP TABLE omni.omni_ods_cloak_trn_usageservice_bx3xsaid_temp;
    PRINT 'Dropped existing omni.omni_ods_cloak_trn_usageservice_bx3xsaid_temp table in SynapseTestDB';
END
CREATE TABLE omni.omni_ods_cloak_trn_usageservice_bx3xsaid_temp (
    id INT IDENTITY(1,1) PRIMARY KEY,
    BX NVARCHAR(50),
    USER_KEY NVARCHAR(50),
    USER_KEY_TYPE NVARCHAR(10),
    SERVICE_KEY1 NVARCHAR(50),
    SERVICE_KEY_TYPE1 NVARCHAR(10),
    SERVICE_TYPE NVARCHAR(10),
    TRANSFER_TYPE NVARCHAR(10),
    TRANSFER_YMD DATE,
    SERVICE_KEY_START_YMD DATE,
    OUTPUT_DATE DATETIME2,
    KEY_3X NVARCHAR(50),
    INDEX_ID INT
);
PRINT 'omni.omni_ods_cloak_trn_usageservice_bx3xsaid_temp table created in SynapseTestDB';

-- omni.omni_ods_marketing_trn_client_dm テーブルを作成
IF EXISTS (SELECT * FROM sys.objects WHERE name='omni_ods_marketing_trn_client_dm' AND schema_id = SCHEMA_ID('omni'))
BEGIN
    DROP TABLE omni.omni_ods_marketing_trn_client_dm;
    PRINT 'Dropped existing omni.omni_ods_marketing_trn_client_dm table in SynapseTestDB';
END
CREATE TABLE omni.omni_ods_marketing_trn_client_dm (
    CUSTOMER_ID NVARCHAR(50) PRIMARY KEY,
    EMAIL_ADDRESS NVARCHAR(255),
    LIV0EU_4X NVARCHAR(50),
    EPCISCRT_3X NVARCHAR(50),
    EPCISCRT_LIGHTING_SA_ID NVARCHAR(50),
    CLIENT_KEY_AX NVARCHAR(50),
    marketing_segment NVARCHAR(50),
    preference_category NVARCHAR(50),
    engagement_score DECIMAL(5,2),
    last_campaign_date DATETIME2,
    created_at DATETIME2,
    updated_at DATETIME2
);
PRINT 'omni.omni_ods_marketing_trn_client_dm table created in SynapseTestDB';

-- omni.omni_ods_marketing_trn_client_dm_bx_temp テーブルを作成
IF EXISTS (SELECT * FROM sys.objects WHERE name='omni_ods_marketing_trn_client_dm_bx_temp' AND schema_id = SCHEMA_ID('omni'))
BEGIN
    DROP TABLE omni.omni_ods_marketing_trn_client_dm_bx_temp;
    PRINT 'Dropped existing omni.omni_ods_marketing_trn_client_dm_bx_temp table in SynapseTestDB';
END
CREATE TABLE omni.omni_ods_marketing_trn_client_dm_bx_temp (
    id INT IDENTITY(1,1) PRIMARY KEY,
    CUSTOMER_ID NVARCHAR(50),
    LIV0EU_4X NVARCHAR(50),
    EPCISCRT_3X NVARCHAR(50),
    EPCISCRT_LIGHTING_SA_ID NVARCHAR(50),
    KEY_4X NVARCHAR(50),
    KEY_3X NVARCHAR(50),
    INDEX_ID INT,
    BX NVARCHAR(50),
    created_at DATETIME2,
    updated_at DATETIME2
);
PRINT 'omni.omni_ods_marketing_trn_client_dm_bx_temp table created in SynapseTestDB';

-- data_watermarks テーブルを作成（ETLテスト用）
IF EXISTS (SELECT * FROM sysobjects WHERE name='data_watermarks' AND xtype='U')
BEGIN
    DROP TABLE data_watermarks;
    PRINT 'Dropped existing data_watermarks table in SynapseTestDB';
END
CREATE TABLE data_watermarks (
    id INT IDENTITY(1,1) PRIMARY KEY,
    table_name NVARCHAR(100),
    last_updated DATETIME2,
    watermark_value NVARCHAR(100),
    created_at DATETIME2 DEFAULT GETDATE()
);
PRINT 'data_watermarks table created in SynapseTestDB';

-- etl.e2e_test_execution_log テーブルを作成
IF EXISTS (SELECT * FROM sys.objects WHERE name='e2e_test_execution_log' AND schema_id = SCHEMA_ID('etl'))
BEGIN
    DROP TABLE etl.e2e_test_execution_log;
    PRINT 'Dropped existing etl.e2e_test_execution_log table in SynapseTestDB';
END
CREATE TABLE etl.e2e_test_execution_log (
    id INT IDENTITY(1,1) PRIMARY KEY,
    test_name NVARCHAR(255),
    execution_time DATETIME2,
    status NVARCHAR(50),
    result NVARCHAR(MAX),
    created_at DATETIME2 DEFAULT GETDATE()
);
PRINT 'etl.e2e_test_execution_log table created in SynapseTestDB';

-- staging.test_data_management テーブルを作成
IF EXISTS (SELECT * FROM sys.objects WHERE name='test_data_management' AND schema_id = SCHEMA_ID('staging'))
BEGIN
    DROP TABLE staging.test_data_management;
    PRINT 'Dropped existing staging.test_data_management table in SynapseTestDB';
END
CREATE TABLE staging.test_data_management (
    id INT IDENTITY(1,1) PRIMARY KEY,
    data_type NVARCHAR(100),
    data_value NVARCHAR(MAX),
    created_date DATETIME2,
    status NVARCHAR(50)
);
PRINT 'staging.test_data_management table created in SynapseTestDB';

-- raw_data_source テーブルを作成（ETLテスト用）
IF EXISTS (SELECT * FROM sysobjects WHERE name='raw_data_source' AND xtype='U')
BEGIN
    DROP TABLE raw_data_source;
    PRINT 'Dropped existing raw_data_source table in SynapseTestDB';
END
CREATE TABLE raw_data_source (
    id INT IDENTITY(1,1) PRIMARY KEY,
    source_type NVARCHAR(50) NOT NULL,
    data_json NVARCHAR(MAX),
    created_at DATETIME2 DEFAULT GETDATE()
);

-- raw_data_sourceテーブルに初期データを挿入
INSERT INTO raw_data_source (source_type, data_json, created_at) VALUES 
('web', '{"user_id": "user1", "action": "purchase", "amount": 100}', GETDATE()),
('api', '{"user_id": "user2", "action": "view", "product": "item1"}', GETDATE()),
('batch', '{"user_id": "user3", "action": "purchase", "amount": 250}', GETDATE()),
('streaming', '{"user_id": "user4", "action": "login"}', GETDATE()),
('file', '{"user_id": "user5", "action": "download", "file": "report.pdf"}', GETDATE());

PRINT 'raw_data_source table created and populated in SynapseTestDB';
GO

-- 初期化完了マーカー
USE master;
GO
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='InitializationComplete' AND xtype='U')
BEGIN
    CREATE TABLE InitializationComplete (completed BIT);
    INSERT INTO InitializationComplete VALUES (1);
    PRINT 'Database initialization completed successfully';
END
ELSE
BEGIN
    PRINT 'Initialization marker already exists';
END
GO

PRINT 'All database initialization scripts completed';
GO
