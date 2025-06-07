-- =========================================================================
-- Electricity Contract Thanks パイプライン用SQLクエリ集
-- テスト対象: pi_Send_ElectricityContractThanks
-- =========================================================================

-- 基本的な電気契約感謝状データ抽出クエリ
-- @name: basic_extraction_query
SELECT TOP 100
    CONTRACT_ID,
    CUSTOMER_ID,
    CONTRACT_START_DATE,
    ELECTRICITY_PLAN_TYPE,
    CONTRACT_STATUS,
    NOTIFICATION_EMAIL
FROM [staging].[electricity_contract_thanks_data]
WHERE created_date >= DATEADD(hour, -2, GETUTCDATE());

-- 電気契約特定シナリオテスト用データ投入
-- @name: contract_scenarios_insert
INSERT INTO [staging].[electricity_contract_thanks_scenarios_test]
VALUES
    ('ELEC001', 1001, '2024/01/15', 'BASIC', 'ACTIVE', 'basic@example.com'),
    ('ELEC002', 1002, '2024/02/10', 'TIME_OF_USE', 'CONFIRMED', 'tou@example.com'),
    ('ELEC003', 1003, '2024/03/05', 'PEAK_SHIFT', 'PENDING', 'peak@example.com'),
    ('ELEC004', 1004, '2024/01/20', 'FAMILY', 'ACTIVE', 'family@example.com'),
    ('ELEC005', 1005, '2024/02/28', 'BUSINESS', 'CONFIRMED', 'business@example.com');

-- 大容量データセット作成（パフォーマンステスト用）
-- @name: large_dataset_insert
INSERT INTO [staging].[electricity_contract_thanks_test]
SELECT TOP 10000
    NEWID() as CONTRACT_ID,
    ABS(CHECKSUM(NEWID())) % 1000000 as CUSTOMER_ID,
    DATEADD(day, -ABS(CHECKSUM(NEWID())) % 365, GETDATE()) as CONTRACT_START_DATE,
    CASE ABS(CHECKSUM(NEWID())) % 5
        WHEN 0 THEN 'BASIC'
        WHEN 1 THEN 'TIME_OF_USE'
        WHEN 2 THEN 'PEAK_SHIFT'
        WHEN 3 THEN 'FAMILY'
        ELSE 'BUSINESS'
    END as ELECTRICITY_PLAN_TYPE,
    'ACTIVE' as CONTRACT_STATUS,
    CONCAT('customer', ABS(CHECKSUM(NEWID())) % 1000000, '@example.com') as NOTIFICATION_EMAIL
FROM sys.objects a, sys.objects b;

-- エラーハンドリングテスト用不正データ投入
-- @name: error_handling_insert
INSERT INTO [staging].[electricity_contract_thanks_error_test]
VALUES
    (NULL, NULL, NULL, 'INVALID_PLAN', 'INVALID_STATUS', 'invalid-email'),
    ('', 'invalid_customer', '2024-13-45', 'UNKNOWN', '', 'no-at-symbol.com');

-- =========================================================================
-- Action Point Current Month Entry パイプライン用SQLクエリ集
-- テスト対象: pi_Send_ActionPointCurrentMonthEntryList
-- =========================================================================

-- アクションポイントエントリーテスト用テーブル作成
-- @name: action_point_test_table_setup
IF NOT EXISTS (SELECT *
FROM sys.objects
WHERE object_id = OBJECT_ID(N'[dbo].[ActionPointEntryTest]') AND type in (N'U'))
CREATE TABLE [dbo].[ActionPointEntryTest]
(
    TestId int IDENTITY(1,1) PRIMARY KEY,
    MtgId varchar(20) NOT NULL,
    ActionPointType varchar(50) NOT NULL,
    EntryDate date NOT NULL,
    EntryAmount decimal(10,2) DEFAULT 0,
    OutputDateTime varchar(20),
    CreatedAt datetime DEFAULT GETDATE()
);

-- アクションポイント実行ログテーブル作成
-- @name: action_point_log_table_setup
IF NOT EXISTS (SELECT *
FROM sys.objects
WHERE object_id = OBJECT_ID(N'[dbo].[ActionPointExecutionLog]') AND type in (N'U'))
CREATE TABLE [dbo].[ActionPointExecutionLog]
(
    LogId int IDENTITY(1,1) PRIMARY KEY,
    ExecutionId varchar(50) NOT NULL,
    TargetMonth varchar(7) NOT NULL,
    ProcessedRecords int DEFAULT 0,
    CurrentMonthRecords int DEFAULT 0,
    ExecutionStatus varchar(20) DEFAULT 'Running',
    TimezoneAccuracy decimal(5,4) DEFAULT 1.0,
    StartTime datetime DEFAULT GETDATE(),
    EndTime datetime NULL,
    ErrorMessage varchar(max) NULL
);

-- アクションポイント実行ログ投入
-- @name: action_point_execution_log_insert
INSERT INTO [dbo].[ActionPointExecutionLog]
    (ExecutionId, TargetMonth, ProcessedRecords, ExecutionStatus, StartTime)
VALUES
    (@execution_id, @target_month, 0, 'Running', GETDATE());

-- アクションポイントテストデータ投入
-- @name: action_point_test_data_insert
INSERT INTO [dbo].[ActionPointEntryTest]
    (MtgId, ActionPointType, EntryDate, EntryAmount, OutputDateTime)
VALUES
    (@mtg_id, @action_type, @entry_date, @entry_amount, @output_datetime);

-- アクションポイント実行ログ更新
-- @name: action_point_execution_log_update
UPDATE [dbo].[ActionPointExecutionLog] 
SET ProcessedRecords = @processed_records, CurrentMonthRecords = @current_month_records, TimezoneAccuracy = @timezone_accuracy, 
    ExecutionStatus = 'Succeeded', EndTime = GETDATE()
WHERE ExecutionId = @execution_id;

-- アクションポイント実行エラーログ更新
-- @name: action_point_execution_error_update
UPDATE [dbo].[ActionPointExecutionLog] 
SET ExecutionStatus = 'Failed', EndTime = GETDATE(), ErrorMessage = @error_message
WHERE ExecutionId = @execution_id;

-- 当月フィルタリング検証クエリ
-- @name: action_point_current_month_verification
SELECT COUNT(*) as total_count,
    COUNT(CASE WHEN YEAR(EntryDate) = YEAR(GETDATE())
        AND MONTH(EntryDate) = MONTH(GETDATE()) 
                   THEN 1 END) as current_month_count
FROM [dbo].[ActionPointEntryTest]
WHERE CreatedAt >= DATEADD(minute, -5, GETDATE());

-- タイムゾーン処理検証クエリ
-- @name: action_point_timezone_verification
SELECT TOP 5
    OutputDateTime, EntryDate
FROM [dbo].[ActionPointEntryTest]
WHERE CreatedAt >= DATEADD(minute, -5, GETDATE())
    AND OutputDateTime IS NOT NULL;

-- テーブルクリーンアップクエリ
-- @name: cleanup_tables
DELETE FROM [staging].[electricity_contract_thanks_scenarios_test];
DELETE FROM [staging].[electricity_contract_thanks_test];
DELETE FROM [staging].[electricity_contract_thanks_error_test];
DELETE FROM [staging].[electricity_contract_thanks_data];

-- =========================================================================
-- Point Grant Email パイプライン用SQLクエリ集
-- テスト対象: pi_Send_PointGrantEmail
-- =========================================================================

-- ポイントグラントメールテスト用テーブル作成
-- @name: point_grant_email_test_table_setup
IF NOT EXISTS (SELECT *
FROM sys.objects
WHERE object_id = OBJECT_ID(N'[dbo].[PointGrantEmailTest]') AND type in (N'U'))
CREATE TABLE [dbo].[PointGrantEmailTest]
(
    TestId int IDENTITY(1,1) PRIMARY KEY,
    MtgId varchar(20) NOT NULL,
    PointAmount decimal(10,2) NOT NULL,
    GrantDate datetime NOT NULL,
    EmailAddress varchar(100),
    CreatedAt datetime DEFAULT GETDATE()
);

-- ポイントグラントメールログテーブル作成
-- @name: point_grant_email_log_table_setup
IF NOT EXISTS (SELECT *
FROM sys.objects
WHERE object_id = OBJECT_ID(N'[dbo].[PointGrantEmailLog]') AND type in (N'U'))
CREATE TABLE [dbo].[PointGrantEmailLog]
(
    LogId int IDENTITY(1,1) PRIMARY KEY,
    ExecutionId varchar(50) NOT NULL,
    ProcessedRecords int DEFAULT 0,
    EmailsSent int DEFAULT 0,
    ExecutionStatus varchar(20) DEFAULT 'Running',
    StartTime datetime DEFAULT GETDATE(),
    EndTime datetime NULL,
    ErrorMessage varchar(max) NULL
);

-- ポイントロストメールテスト用テーブル作成
-- @name: point_lost_email_test_table_setup
IF NOT EXISTS (SELECT *
FROM sys.objects
WHERE object_id = OBJECT_ID(N'[dbo].[PointLostEmailTest]') AND type in (N'U'))
CREATE TABLE [dbo].[PointLostEmailTest]
(
    TestId int IDENTITY(1,1) PRIMARY KEY,
    MtgId varchar(20) NOT NULL,
    LostPointAmount decimal(10,2) NOT NULL,
    ExpirationDate datetime NOT NULL,
    WarningType varchar(20) NOT NULL,
    EmailAddress varchar(100),
    CreatedAt datetime DEFAULT GETDATE()
);

-- ポイントロストメールログテーブル作成
-- @name: point_lost_email_log_table_setup
IF NOT EXISTS (SELECT *
FROM sys.objects
WHERE object_id = OBJECT_ID(N'[dbo].[PointLostEmailLog]') AND type in (N'U'))
CREATE TABLE [dbo].[PointLostEmailLog]
(
    LogId int IDENTITY(1,1) PRIMARY KEY,
    ExecutionId varchar(50) NOT NULL,
    WarningType varchar(20) NOT NULL,
    ProcessedRecords int DEFAULT 0,
    EmailsSent int DEFAULT 0,
    ExecutionStatus varchar(20) DEFAULT 'Running',
    StartTime datetime DEFAULT GETDATE(),
    EndTime datetime NULL,
    ErrorMessage varchar(max) NULL
);

-- ポイントグラントメールテストデータ投入
-- @name: point_grant_email_test_data_insert
INSERT INTO [dbo].[PointGrantEmailTest]
    (MtgId, PointAmount, GrantDate, EmailAddress)
VALUES
    (@mtg_id, @point_amount, @grant_date, @email_address);

-- ポイントグラントメール実行ログ投入
-- @name: point_grant_email_execution_log_insert
INSERT INTO [dbo].[PointGrantEmailLog]
    (ExecutionId, ProcessedRecords, EmailsSent, ExecutionStatus, StartTime)
VALUES
    (@execution_id, @processed_records, @emails_sent, 'Running', GETDATE());

-- ポイントロストメールテストデータ投入
-- @name: point_lost_email_test_data_insert
INSERT INTO [dbo].[PointLostEmailTest]
    (MtgId, LostPointAmount, ExpirationDate, WarningType, EmailAddress)
VALUES
    (@mtg_id, @lost_point_amount, @expiration_date, @warning_type, @email_address);

-- ポイントロストメール実行ログ投入
-- @name: point_lost_email_execution_log_insert
INSERT INTO [dbo].[PointLostEmailLog]
    (ExecutionId, WarningType, ProcessedRecords, EmailsSent, ExecutionStatus, StartTime)
VALUES
    (@execution_id, @warning_type, @processed_records, @emails_sent, 'Running', GETDATE());

-- ポイントグラントメールクリーンアップ
-- @name: point_grant_email_cleanup_tables
DELETE FROM [dbo].[PointGrantEmailTest] WHERE CreatedAt < DATEADD(day, -@retention_days, GETDATE());
DELETE FROM [dbo].[PointGrantEmailLog] WHERE StartTime < DATEADD(day, -@retention_days, GETDATE());

-- ポイントロストメールクリーンアップ
-- @name: point_lost_email_cleanup_tables
DELETE FROM [dbo].[PointLostEmailTest] WHERE CreatedAt < DATEADD(day, -@retention_days, GETDATE());
DELETE FROM [dbo].[PointLostEmailLog] WHERE StartTime < DATEADD(day, -@retention_days, GETDATE());

-- アクションポイントテーブルクリーンアップ
-- @name: action_point_cleanup_tables
DELETE FROM [dbo].[ActionPointEntryTest] WHERE CreatedAt < DATEADD(day, -@retention_days, GETDATE());
DELETE FROM [dbo].[ActionPointExecutionLog] WHERE StartTime < DATEADD(day, -@retention_days, GETDATE());
