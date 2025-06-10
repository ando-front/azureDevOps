-- 手動実行用：最もシンプルなデータベース・テーブル作成スクリプト

-- データベース作成
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'TGMATestDB')
BEGIN
    CREATE DATABASE TGMATestDB COLLATE Japanese_CI_AS;
END;

IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'SynapseTestDB')
BEGIN
    CREATE DATABASE SynapseTestDB COLLATE Japanese_CI_AS;
    PRINT 'SynapseTestDB created successfully';
END
ELSE
BEGIN
    PRINT 'SynapseTestDB already exists';
END;

-- TGMATestDBに切り替え
USE TGMATestDB;

-- 既存テーブル削除（冪等性対応）
DROP TABLE IF EXISTS dbo.client_dm;
DROP TABLE IF EXISTS dbo.ClientDmBx;
DROP TABLE IF EXISTS dbo.point_grant_email;
DROP TABLE IF EXISTS dbo.marketing_client_dm;

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

-- テスト用のサンプルデータを挿入
INSERT INTO dbo.client_dm (client_id, client_name, email, status, created_date)
VALUES 
    ('TEST001', 'テストユーザー1', 'test1@example.com', 'ACTIVE', GETDATE()),
    ('TEST002', 'テストユーザー2', 'test2@example.com', 'ACTIVE', GETDATE());

INSERT INTO dbo.ClientDmBx (client_id, segment, score, data_source)
VALUES 
    ('TEST001', 'PREMIUM', 85.5, 'E2E_TEST'),
    ('TEST002', 'STANDARD', 72.3, 'E2E_TEST');

PRINT 'Database and tables created successfully with test data!';
