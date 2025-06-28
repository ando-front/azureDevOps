-- Create test database if not exists
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'TGMATestDB')
BEGIN
    CREATE DATABASE TGMATestDB;
END;
GO

-- Switch to TGMATestDB
USE TGMATestDB;
GO

-- ODS 用テーブル定義
-- Synapse にロードする前段階のデータ保持テーブルを定義します

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ods_TGCRM_ClientDM')
BEGIN
    CREATE TABLE dbo.ods_TGCRM_ClientDM (
        id BIGINT IDENTITY(1,1) PRIMARY KEY,
        client_id NVARCHAR(50) NOT NULL,
        hash NVARCHAR(128) NOT NULL,
        data_json NVARCHAR(MAX) NULL,
        last_modified_date DATETIME2 NOT NULL,
        created_at DATETIME2 DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 DEFAULT SYSUTCDATETIME()
    );
END;

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ods_TGCRM_AnotherTable')
BEGIN
    -- TODO: 他の ODS テーブル定義をここに追加
END;

-- デイリー差分用 ODS テーブル作成
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'omni_ods_cloak_trn_contract_key_daily')
BEGIN
    CREATE TABLE dbo.omni_ods_cloak_trn_contract_key_daily (
        contract_key NVARCHAR(50) NOT NULL PRIMARY KEY,
        last_modified_date DATETIME2 NOT NULL,
        data_hash NVARCHAR(128) NULL,
        is_deleted BIT DEFAULT 0,
        created_at DATETIME2 DEFAULT SYSUTCDATETIME()
    );
END;
