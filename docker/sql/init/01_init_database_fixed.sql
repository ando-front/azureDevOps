-- E2Eテスト用TGMATestDBデータベース初期化スクリプト（ODBCドライバー問題対応）
-- 実行順序: 01_init_database.sql
-- 冪等性担保: 複数回実行しても安全

SET NOCOUNT ON;
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

PRINT '=== Starting TGMATestDB Idempotent Initialization ===';

USE master;
GO

-- TGMATestDB データベースの作成（冪等性対応）
PRINT 'Checking for TGMATestDB existence...';
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'TGMATestDB')
BEGIN
    PRINT 'Creating TGMATestDB database...';
    CREATE DATABASE TGMATestDB
    COLLATE Japanese_CI_AS;
    PRINT 'TGMATestDB created successfully';
END
ELSE
BEGIN
    PRINT 'TGMATestDB already exists - skipping creation';
END
GO

-- テスト用ユーザーの作成（冪等性対応）
PRINT 'Creating test_user login if not exists...';
IF NOT EXISTS (SELECT * FROM sys.server_principals WHERE name = 'test_user')
BEGIN
    CREATE LOGIN test_user WITH PASSWORD = 'TestUser123!';
    PRINT 'test_user login created successfully';
END
ELSE
BEGIN
    PRINT 'test_user login already exists - skipping creation';
END
GO

-- TGMATestDBに切り替え
PRINT 'Switching to TGMATestDB...';
USE TGMATestDB;
GO

-- テスト用ユーザーをデータベースに追加（冪等性対応）
PRINT 'Adding test_user to database if not exists...';
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'test_user')
BEGIN
    CREATE USER test_user FOR LOGIN test_user;
    ALTER ROLE db_owner ADD MEMBER test_user;
    PRINT 'test_user added to database successfully';
END
ELSE
BEGIN
    PRINT 'test_user already exists in database - skipping creation';
END
GO

-- 基本的なスキーマの作成（バッチ分割）
-- dboスキーマは既定で存在するため作成をスキップ
PRINT 'dbo schema verification completed';
GO

-- staging スキーマ作成
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'staging')
BEGIN
    PRINT 'Creating staging schema...';
    EXEC('CREATE SCHEMA staging');
    PRINT 'staging schema created successfully';
END
ELSE
BEGIN
    PRINT 'staging schema already exists - skipping creation';
END
GO

-- etl スキーマ作成
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'etl')
BEGIN
    PRINT 'Creating etl schema...';
    EXEC('CREATE SCHEMA etl');
    PRINT 'etl schema created successfully';
END
ELSE
BEGIN
    PRINT 'etl schema already exists - skipping creation';
END
GO

-- 接続テスト用テーブル作成
PRINT 'Creating connection_test table if not exists...';
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'connection_test')
BEGIN
    CREATE TABLE connection_test (
        id INT IDENTITY(1,1) PRIMARY KEY,
        test_name NVARCHAR(100) NOT NULL,
        connection_type NVARCHAR(50) NOT NULL,
        status NVARCHAR(20) NOT NULL,
        test_time DATETIME2 DEFAULT GETDATE(),
        details NVARCHAR(MAX)
    );
    
    -- 初期接続テストデータ挿入
    INSERT INTO connection_test (test_name, connection_type, status, details)
    VALUES ('Initial Connection', 'SQL_Server', 'SUCCESS', 'Database initialized successfully');
    
    PRINT 'connection_test table created and initialized successfully';
END
ELSE
BEGIN
    PRINT 'connection_test table already exists - skipping creation';
END
GO

PRINT '=== TGMATestDB Idempotent Initialization Completed Successfully ===';
GO
