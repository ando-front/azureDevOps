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
PRINT 'Switching to SynapseTestDB for table creation...';
USE SynapseTestDB;
GO

-- 基本的なスキーマの作成（バッチ分割）
-- dboスキーマは既定で存在するため作成をスキップ
PRINT 'dbo schema verification completed';
GO

-- テスト用の基本テーブル作成（冪等性対応）
PRINT 'Creating test_data table if not exists...';
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
    PRINT 'test_data table already exists - skipping creation';
END
GO

-- 初期データの挿入（冪等性対応）
PRINT 'Inserting initial test data if not exists...';
IF NOT EXISTS (SELECT * FROM test_data WHERE name = 'test_entry')
BEGIN
    INSERT INTO test_data (name, value) VALUES ('test_entry', 'initial_value');
    PRINT 'Initial test data inserted successfully';
END
ELSE
BEGIN
    PRINT 'Initial test data already exists - skipping insertion';
END
GO

-- 追加のテスト用テーブル作成（IR Simulator用）
PRINT 'Creating simulator_status table if not exists...';
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'simulator_status')
BEGIN
    CREATE TABLE simulator_status (
        id INT IDENTITY(1,1) PRIMARY KEY,
        service_name NVARCHAR(100) NOT NULL,
        status NVARCHAR(50) NOT NULL,
        last_check DATETIME2 DEFAULT GETDATE(),
        details NVARCHAR(MAX)
    );
    
    -- 初期ステータス挿入
    INSERT INTO simulator_status (service_name, status, details) 
    VALUES ('IR_Simulator', 'INITIALIZED', 'Database connection established');
    
    PRINT 'simulator_status table created and initialized successfully';
END
ELSE
BEGIN
    PRINT 'simulator_status table already exists - skipping creation';
END
GO

PRINT '=== SynapseTestDB Idempotent Initialization Completed Successfully ===';
GO
