-- filepath: c:\Users\0190402\git\tgma-MA-POC\docker\sql\init\00_create_synapse_db.sql
-- E2Eテスト用SynapseTestDBデータベース初期化スクリプト（ODBCドライバー問題対応）
-- 実行順序: 00_create_synapse_db.sql (最初に実行)
-- 冪等性担保: 複数回実行しても安全

SET NOCOUNT ON;
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

PRINT '=== Starting SynapseTestDB Idempotent Initialization ===';

USE master;
GO

-- SynapseTestDB データベースの作成（冪等性対応）
PRINT 'Checking for SynapseTestDB existence...';
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'SynapseTestDB')
BEGIN
    PRINT 'Creating SynapseTestDB database...';
    CREATE DATABASE SynapseTestDB
    COLLATE Japanese_CI_AS;
    PRINT 'SynapseTestDB created successfully';
END
ELSE
BEGIN
    PRINT 'SynapseTestDB already exists - skipping creation';
END
GO

-- SynapseTestDBに切り替えて基本テーブルを作成
USE SynapseTestDB;
GO

-- 基本的なスキーマの作成
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dbo')
BEGIN
    CREATE SCHEMA dbo;
END
GO

-- テスト用の基本テーブル作成（冪等性対応）
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'test_data')
BEGIN
    CREATE TABLE test_data (
        id INT IDENTITY(1,1) PRIMARY KEY,
        name NVARCHAR(255) NOT NULL,
        value NVARCHAR(MAX),
        created_at DATETIME2 DEFAULT GETDATE(),
        updated_at DATETIME2 DEFAULT GETDATE()
    );
    PRINT 'test_data table created successfully';
END
ELSE
BEGIN
    PRINT 'test_data table already exists';
END
GO

-- 初期データの挿入（冪等性対応）
IF NOT EXISTS (SELECT * FROM test_data WHERE name = 'test_entry')
BEGIN
    INSERT INTO test_data (name, value) VALUES ('test_entry', 'initial_value');
    PRINT 'Initial test data inserted successfully';
END
ELSE
BEGIN
    PRINT 'Initial test data already exists';
END
GO

PRINT 'SynapseTestDB initialization completed successfully';
GO
