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
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='ClientDmBx' AND xtype='U')
BEGIN
    CREATE TABLE ClientDmBx (
        id INT PRIMARY KEY,
        name NVARCHAR(100)
    );
    PRINT 'ClientDmBx table created in TGMATestDB';
END
ELSE
BEGIN
    PRINT 'ClientDmBx table already exists in TGMATestDB';
END
GO

-- SynapseTestDB にテーブルを作成
USE SynapseTestDB;
GO
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='ClientDmBx' AND xtype='U')
BEGIN
    CREATE TABLE ClientDmBx (
        id INT PRIMARY KEY,
        name NVARCHAR(100)
    );
    PRINT 'ClientDmBx table created in SynapseTestDB';
END
ELSE
BEGIN
    PRINT 'ClientDmBx table already exists in SynapseTestDB';
END
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
