-- E2Eテスト用データベース初期化スクリプト
-- 実行順序: 01_init_database.sql

USE master;
GO

-- テスト用データベースの作成
IF NOT EXISTS (SELECT *
FROM sys.databases
WHERE name = 'TGMATestDB')
BEGIN
    CREATE DATABASE TGMATestDB
    COLLATE Japanese_CI_AS;
END
GO

-- SynapseTestDB データベースの作成（冪等性対応）
IF NOT EXISTS (SELECT *
FROM sys.databases
WHERE name = 'SynapseTestDB')
BEGIN
    CREATE DATABASE SynapseTestDB
    COLLATE Japanese_CI_AS;
    PRINT 'SynapseTestDB created successfully';
END
ELSE
BEGIN
    PRINT 'SynapseTestDB already exists';
END
GO

-- テスト用ユーザーの作成
IF NOT EXISTS (SELECT *
FROM sys.server_principals
WHERE name = 'test_user')
BEGIN
    CREATE LOGIN test_user WITH PASSWORD = 'TestUser123!';
END
GO

USE TGMATestDB;
GO

-- テスト用ユーザーをデータベースに追加
IF NOT EXISTS (SELECT *
FROM sys.database_principals
WHERE name = 'test_user')
BEGIN
    CREATE USER test_user FOR LOGIN test_user;
    ALTER ROLE db_owner ADD MEMBER test_user;
END
GO

-- 基本的なスキーマの作成
IF NOT EXISTS (SELECT *
FROM sys.schemas
WHERE name = 'dbo')
BEGIN
    CREATE SCHEMA dbo;
END
GO

IF NOT EXISTS (SELECT *
FROM sys.schemas
WHERE name = 'staging')
BEGIN
    CREATE SCHEMA staging;
END
GO

IF NOT EXISTS (SELECT *
FROM sys.schemas
WHERE name = 'etl')
BEGIN
    CREATE SCHEMA etl;
END
GO

PRINT 'Database TGMATestDB initialized successfully';
GO
