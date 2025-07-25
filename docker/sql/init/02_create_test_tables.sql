-- シンプルなテーブル作成スクリプト（E2Eテスト用）
-- スキーマエラーを回避するためにdboスキーマのみ使用

USE TGMATestDB;

-- client_dmテーブル作成
DROP TABLE IF EXISTS dbo.client_dm;
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
DROP TABLE IF EXISTS dbo.ClientDmBx;
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
DROP TABLE IF EXISTS dbo.point_grant_email;
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
DROP TABLE IF EXISTS dbo.marketing_client_dm;
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

PRINT 'Simple tables created successfully';
