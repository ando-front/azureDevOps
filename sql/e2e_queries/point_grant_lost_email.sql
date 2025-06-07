-- ポイントグラント・ロストメール関連パイプラインE2Eテスト用SQLクエリ
-- ファイル: point_grant_lost_email.sql
-- 対象パイプライン: pi_PointGrantEmail, pi_PointLostEmail
-- 作成日: 2024-01-16

-- ===========================
-- ポイント付与メール関連クエリ
-- ===========================

-- @name: create_point_grant_email_test_table
-- @description: ポイント付与メール用テストテーブル作成
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

-- @name: create_point_grant_email_log_table
-- @description: ポイント付与メールログテーブル作成
IF NOT EXISTS (SELECT *
FROM sys.objects
WHERE object_id = OBJECT_ID(N'[dbo].[PointGrantEmailLog]') AND type in (N'U'))
CREATE TABLE [dbo].[PointGrantEmailLog]
(
    LogId int IDENTITY(1,1) PRIMARY KEY,
    ExecutionId varchar(50) NOT NULL,
    ProcessedRecords int DEFAULT 0,
    ExecutionStatus varchar(20) DEFAULT 'Running',
    StartTime datetime DEFAULT GETDATE(),
    EndTime datetime NULL,
    ErrorMessage varchar(max) NULL
);

-- @name: insert_point_grant_email_log
-- @description: ポイント付与メール実行ログ挿入
INSERT INTO [dbo].[PointGrantEmailLog]
    (ExecutionId, ProcessedRecords, ExecutionStatus, StartTime)
VALUES
    (?, 0, 'Running', GETDATE());

-- @name: insert_point_grant_email_test_data
-- @description: ポイント付与メールテストデータ挿入
INSERT INTO [dbo].[PointGrantEmailTest]
    (MtgId, PointAmount, GrantDate, EmailAddress)
VALUES
    (?, ?, ?, ?);

-- @name: update_point_grant_email_log_success
-- @description: ポイント付与メール実行ログ成功更新
UPDATE [dbo].[PointGrantEmailLog] 
SET ProcessedRecords = ?, ExecutionStatus = 'Succeeded', EndTime = GETDATE()
WHERE ExecutionId = ?;

-- @name: update_point_grant_email_log_error
-- @description: ポイント付与メール実行ログエラー更新
UPDATE [dbo].[PointGrantEmailLog] 
SET ExecutionStatus = 'Failed', EndTime = GETDATE(), ErrorMessage = ?
WHERE ExecutionId = ?;

-- @name: check_point_grant_duplicates
-- @description: ポイント付与メール重複チェック
SELECT COUNT(*) - COUNT(DISTINCT MtgId)
FROM [dbo].[PointGrantEmailTest];

-- @name: check_point_grant_null_values
-- @description: ポイント付与メールNULL値チェック
SELECT COUNT(*)
FROM [dbo].[PointGrantEmailTest]
WHERE MtgId IS NULL OR PointAmount IS NULL;

-- @name: check_point_grant_invalid_amounts
-- @description: ポイント付与メール無効金額チェック
SELECT COUNT(*)
FROM [dbo].[PointGrantEmailTest]
WHERE PointAmount <= 0;

-- @name: check_point_grant_future_dates
-- @description: ポイント付与メール未来日付チェック
SELECT COUNT(*)
FROM [dbo].[PointGrantEmailTest]
WHERE GrantDate > GETDATE();

-- @name: cleanup_point_grant_email_test_data
-- @description: ポイント付与メールテストデータクリーンアップ
DELETE FROM [dbo].[PointGrantEmailTest] WHERE CreatedAt < DATEADD(day, -?, GETDATE());

-- @name: cleanup_point_grant_email_log_data
-- @description: ポイント付与メールログデータクリーンアップ
DELETE FROM [dbo].[PointGrantEmailLog] WHERE StartTime < DATEADD(day, -?, GETDATE());

-- ===========================
-- ポイント失効メール関連クエリ
-- ===========================

-- @name: create_point_lost_email_test_table
-- @description: ポイント失効メール用テストテーブル作成
IF NOT EXISTS (SELECT *
FROM sys.objects
WHERE object_id = OBJECT_ID(N'[dbo].[PointLostEmailTest]') AND type in (N'U'))
CREATE TABLE [dbo].[PointLostEmailTest]
(
    TestId int IDENTITY(1,1) PRIMARY KEY,
    ServiceId varchar(20) NOT NULL,
    CustomerKey varchar(50) NOT NULL,
    ClientId varchar(20) NOT NULL,
    LostPointAmount decimal(10,2) NOT NULL,
    LostDate datetime NOT NULL,
    EmailAddress varchar(100),
    CreatedAt datetime DEFAULT GETDATE()
);

-- @name: create_point_lost_email_log_table
-- @description: ポイント失効メールログテーブル作成
IF NOT EXISTS (SELECT *
FROM sys.objects
WHERE object_id = OBJECT_ID(N'[dbo].[PointLostEmailLog]') AND type in (N'U'))
CREATE TABLE [dbo].[PointLostEmailLog]
(
    LogId int IDENTITY(1,1) PRIMARY KEY,
    ExecutionId varchar(50) NOT NULL,
    ProcessedRecords int DEFAULT 0,
    ExecutionStatus varchar(20) DEFAULT 'Running',
    StartTime datetime DEFAULT GETDATE(),
    EndTime datetime NULL,
    ErrorMessage varchar(max) NULL,
    FileSizeMB decimal(10,3) NULL,
    CompressionRatio decimal(5,3) NULL
);

-- @name: insert_point_lost_email_log
-- @description: ポイント失効メール実行ログ挿入
INSERT INTO [dbo].[PointLostEmailLog]
    (ExecutionId, ProcessedRecords, ExecutionStatus, StartTime)
VALUES
    (?, 0, 'Running', GETDATE());

-- @name: insert_point_lost_email_test_data
-- @description: ポイント失効メールテストデータ挿入
INSERT INTO [dbo].[PointLostEmailTest]
    (ServiceId, CustomerKey, ClientId, LostPointAmount, LostDate, EmailAddress)
VALUES
    (?, ?, ?, ?, ?, ?);

-- @name: update_point_lost_email_log_success
-- @description: ポイント失効メール実行ログ成功更新
UPDATE [dbo].[PointLostEmailLog] 
SET ProcessedRecords = ?, ExecutionStatus = 'Succeeded', 
    EndTime = GETDATE(), FileSizeMB = ?, CompressionRatio = ?
WHERE ExecutionId = ?;

-- @name: update_point_lost_email_log_error
-- @description: ポイント失効メール実行ログエラー更新
UPDATE [dbo].[PointLostEmailLog] 
SET ExecutionStatus = 'Failed', EndTime = GETDATE(), ErrorMessage = ?
WHERE ExecutionId = ?;

-- @name: check_point_lost_duplicates
-- @description: ポイント失効メール重複チェック
SELECT COUNT(*) - COUNT(DISTINCT CONCAT(ServiceId, CustomerKey))
FROM [dbo].[PointLostEmailTest];

-- @name: check_point_lost_null_values
-- @description: ポイント失効メールNULL値チェック
SELECT COUNT(*)
FROM [dbo].[PointLostEmailTest]
WHERE ServiceId IS NULL OR CustomerKey IS NULL OR LostPointAmount IS NULL;

-- @name: check_point_lost_invalid_amounts
-- @description: ポイント失効メール無効金額チェック
SELECT COUNT(*)
FROM [dbo].[PointLostEmailTest]
WHERE LostPointAmount <= 0;

-- @name: check_point_lost_past_dates
-- @description: ポイント失効メール過去日付チェック
SELECT COUNT(*)
FROM [dbo].[PointLostEmailTest]
WHERE LostDate <= GETDATE();

-- @name: cleanup_point_lost_email_test_data
-- @description: ポイント失効メールテストデータクリーンアップ
DELETE FROM [dbo].[PointLostEmailTest] WHERE CreatedAt < DATEADD(day, -?, GETDATE());

-- @name: cleanup_point_lost_email_log_data
-- @description: ポイント失効メールログデータクリーンアップ
DELETE FROM [dbo].[PointLostEmailLog] WHERE StartTime < DATEADD(day, -?, GETDATE());

-- ===========================
-- 共通品質チェッククエリ
-- ===========================

-- @name: get_basic_test_query
-- @description: 基本的なテストクエリ
SELECT 1 as test_column;

-- @name: check_table_exists_point_grant_email_test
-- @description: ポイント付与メールテストテーブル存在チェック
SELECT CASE WHEN EXISTS (
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dbo].[PointGrantEmailTest]') AND type in (N'U')
) THEN 1 ELSE 0 END as table_exists;

-- @name: check_table_exists_point_lost_email_test
-- @description: ポイント失効メールテストテーブル存在チェック
SELECT CASE WHEN EXISTS (
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dbo].[PointLostEmailTest]') AND type in (N'U')
) THEN 1 ELSE 0 END as table_exists;

-- @name: get_point_grant_email_test_count
-- @description: ポイント付与メールテストデータ件数取得
SELECT COUNT(*) as row_count
FROM [dbo].[PointGrantEmailTest];

-- @name: get_point_lost_email_test_count
-- @description: ポイント失効メールテストデータ件数取得
SELECT COUNT(*) as row_count
FROM [dbo].[PointLostEmailTest];

-- @name: get_high_value_point_grants
-- @description: 高額ポイント付与取得
SELECT MtgId, PointAmount, GrantDate
FROM [dbo].[PointGrantEmailTest]
WHERE PointAmount >= ?;

-- @name: get_high_value_point_lost
-- @description: 高額ポイント失効取得
SELECT ServiceId, CustomerKey, LostPointAmount, LostDate
FROM [dbo].[PointLostEmailTest]
WHERE LostPointAmount >= ?;

-- @name: drop_point_grant_email_test_table
-- @description: ポイント付与メールテストテーブル削除
DROP TABLE IF EXISTS [dbo].[PointGrantEmailTest];

-- @name: drop_point_lost_email_test_table
-- @description: ポイント失効メールテストテーブル削除
DROP TABLE IF EXISTS [dbo].[PointLostEmailTest];

-- @name: drop_point_grant_email_log_table
-- @description: ポイント付与メールログテーブル削除
DROP TABLE IF EXISTS [dbo].[PointGrantEmailLog];

-- @name: drop_point_lost_email_log_table
-- @description: ポイント失効メールログテーブル削除
DROP TABLE IF EXISTS [dbo].[PointLostEmailLog];

-- ===========================
-- 簡易版テスト専用クエリ
-- ===========================

-- @name: create_test_point_grant_email_table
-- @description: テスト用ポイント付与メールテーブル作成（簡易版）
CREATE TABLE test_point_grant_email
(
    customer_id varchar(20) PRIMARY KEY,
    points_granted int NOT NULL,
    grant_type varchar(50) NOT NULL,
    grant_date date NOT NULL,
    email_sent bit DEFAULT 0,
    campaign_id varchar(20),
    expiry_date date
);

-- @name: insert_test_point_grant_email_data
-- @description: テスト用ポイント付与メールデータ挿入（簡易版）
INSERT INTO test_point_grant_email
    (customer_id, points_granted, grant_type, grant_date, email_sent, campaign_id, expiry_date)
VALUES
    (?, ?, ?, ?, ?, ?, ?);

-- @name: get_test_point_grant_email_count
-- @description: テスト用ポイント付与メールデータ件数取得（簡易版）
SELECT COUNT(*) as row_count
FROM test_point_grant_email;

-- @name: check_test_table_exists
-- @description: テストテーブル存在チェック（簡易版）
SELECT CASE WHEN EXISTS (
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'test_point_grant_email') AND type in (N'U')
) THEN 1 ELSE 0 END as table_exists;

-- @name: drop_test_point_grant_email_table
-- @description: テスト用ポイント付与メールテーブル削除（簡易版）
DROP TABLE IF EXISTS test_point_grant_email;
