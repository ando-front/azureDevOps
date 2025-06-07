-- E2Eテスト強化用テストデータ挿入スクリプト
-- 実行順序: 05_comprehensive_test_data.sql
-- 目的: E2Eテスト用の包括的なテストデータセットアップ

USE TGMATestDB;
GO

-- =============================================================================
-- テストデータクリーンアップと初期化
-- =============================================================================

PRINT 'Starting comprehensive test data setup...';

-- 既存テストデータのクリーンアップ（外部キー制約考慮）
DELETE FROM [etl].[e2e_test_execution_log] WHERE test_suite LIKE 'E2E_%';
DELETE FROM [staging].[test_data_management] WHERE test_scenario LIKE 'E2E_%';
DELETE FROM [dbo].[marketing_client_dm] WHERE client_id LIKE 'E2E_%';
DELETE FROM [dbo].[point_grant_email] WHERE client_id LIKE 'E2E_%';
DELETE FROM [dbo].[ClientDmBx] WHERE client_id LIKE 'E2E_%';
DELETE FROM [dbo].[client_dm] WHERE client_id LIKE 'E2E_%';

PRINT 'Existing E2E test data cleaned up';

-- =============================================================================
-- 基本顧客データ（client_dm）
-- =============================================================================

PRINT 'Inserting core client data...';

INSERT INTO [dbo].[client_dm]
    ([client_id], [client_name], [email], [phone], [address], [registration_date], [status])
VALUES
    ('E2E_CLIENT_001', 'E2Eテスト顧客_アクティブ', 'e2e.active@test.com', '090-1111-0001', '東京都新宿区1-1-1', '2024-01-15', 'ACTIVE'),
    ('E2E_CLIENT_002', 'E2Eテスト顧客_プレミアム', 'e2e.premium@test.com', '090-1111-0002', '東京都渋谷区2-2-2', '2024-02-15', 'ACTIVE'),
    ('E2E_CLIENT_003', 'E2Eテスト顧客_スタンダード', 'e2e.standard@test.com', '090-1111-0003', '東京都港区3-3-3', '2024-03-15', 'ACTIVE'),
    ('E2E_CLIENT_004', 'E2Eテスト顧客_非アクティブ', 'e2e.inactive@test.com', '090-1111-0004', '東京都千代田区4-4-4', '2024-04-15', 'INACTIVE'),
    ('E2E_CLIENT_005', 'E2Eテスト顧客_削除予定', 'e2e.pending@test.com', '090-1111-0005', '東京都中央区5-5-5', '2024-05-15', 'PENDING'),
    
    -- 大量データテスト用
    ('E2E_BULK_001', 'E2E大量データ001', 'e2e.bulk001@test.com', '090-2000-0001', '大阪府大阪市1-1-1', '2024-01-01', 'ACTIVE'),
    ('E2E_BULK_002', 'E2E大量データ002', 'e2e.bulk002@test.com', '090-2000-0002', '大阪府大阪市1-1-2', '2024-01-02', 'ACTIVE'),
    ('E2E_BULK_003', 'E2E大量データ003', 'e2e.bulk003@test.com', '090-2000-0003', '大阪府大阪市1-1-3', '2024-01-03', 'ACTIVE'),
    ('E2E_BULK_004', 'E2E大量データ004', 'e2e.bulk004@test.com', '090-2000-0004', '大阪府大阪市1-1-4', '2024-01-04', 'ACTIVE'),
    ('E2E_BULK_005', 'E2E大量データ005', 'e2e.bulk005@test.com', '090-2000-0005', '大阪府大阪市1-1-5', '2024-01-05', 'ACTIVE'),
    
    -- エラーハンドリングテスト用
    ('E2E_ERROR_001', 'E2Eエラーテスト', 'e2e.error@test.com', '090-9999-0001', '無効な住所データ', '2024-12-31', 'ACTIVE'),
    ('E2E_SPECIAL_001', 'E2E特殊文字テスト©™®', 'e2e.special@test.com', '090-0000-0001', '特殊文字住所！＠＃', '2024-06-01', 'ACTIVE');

PRINT 'Core client data inserted: ' + CAST(@@ROWCOUNT AS NVARCHAR(10)) + ' records';

-- =============================================================================
-- 顧客データマート（ClientDmBx）
-- =============================================================================

PRINT 'Inserting ClientDmBx data...';

INSERT INTO [dbo].[ClientDmBx]
    ([client_id], [segment], [score], [last_transaction_date], [total_amount], [data_source], [bx_flag])
VALUES
    ('E2E_CLIENT_001', 'PREMIUM', 95.5, '2024-06-01', 250000.00, 'E2E_TEST_PIPELINE', 1),
    ('E2E_CLIENT_002', 'HIGH_VALUE', 88.3, '2024-05-30', 180000.00, 'E2E_TEST_PIPELINE', 1),
    ('E2E_CLIENT_003', 'STANDARD', 72.1, '2024-05-25', 95000.00, 'E2E_TEST_PIPELINE', 1),
    ('E2E_CLIENT_004', 'LOW_VALUE', 45.8, '2024-04-20', 25000.00, 'E2E_TEST_PIPELINE', 0),
    ('E2E_CLIENT_005', 'PENDING', 60.0, '2024-05-15', 50000.00, 'E2E_TEST_PIPELINE', 1),
    
    -- 大量データ
    ('E2E_BULK_001', 'STANDARD', 70.0, '2024-06-01', 100000.00, 'E2E_BULK_PIPELINE', 1),
    ('E2E_BULK_002', 'STANDARD', 71.0, '2024-06-01', 101000.00, 'E2E_BULK_PIPELINE', 1),
    ('E2E_BULK_003', 'STANDARD', 72.0, '2024-06-01', 102000.00, 'E2E_BULK_PIPELINE', 1),
    ('E2E_BULK_004', 'STANDARD', 73.0, '2024-06-01', 103000.00, 'E2E_BULK_PIPELINE', 1),
    ('E2E_BULK_005', 'STANDARD', 74.0, '2024-06-01', 104000.00, 'E2E_BULK_PIPELINE', 1),
    
    -- 境界値テスト
    ('E2E_ERROR_001', 'ERROR_TEST', 0.0, '1900-01-01', 0.00, 'E2E_ERROR_PIPELINE', 0),
    ('E2E_SPECIAL_001', 'SPECIAL_CHAR', 100.0, '2024-06-07', 999999.99, 'E2E_SPECIAL_PIPELINE', 1);

PRINT 'ClientDmBx data inserted: ' + CAST(@@ROWCOUNT AS NVARCHAR(10)) + ' records';

-- =============================================================================
-- ポイント付与メール履歴（point_grant_email）
-- =============================================================================

PRINT 'Inserting point grant email data...';

INSERT INTO [dbo].[point_grant_email]
    ([client_id], [email], [points_granted], [email_sent_date], [campaign_id], [status], [grant_reason])
VALUES
    ('E2E_CLIENT_001', 'e2e.active@test.com', 1000, '2024-06-01 10:00:00', 'E2E_CAMP_001', 'SENT', '新規登録ボーナス'),
    ('E2E_CLIENT_002', 'e2e.premium@test.com', 2000, '2024-06-01 11:00:00', 'E2E_CAMP_001', 'SENT', 'プレミアム会員特典'),
    ('E2E_CLIENT_003', 'e2e.standard@test.com', 500, '2024-06-01 12:00:00', 'E2E_CAMP_002', 'SENT', '購入特典'),
    ('E2E_CLIENT_004', 'e2e.inactive@test.com', 750, '2024-05-15 14:00:00', 'E2E_CAMP_002', 'FAILED', '配信失敗'),
    ('E2E_CLIENT_005', 'e2e.pending@test.com', 300, '2024-06-07 09:00:00', 'E2E_CAMP_003', 'PENDING', '処理中'),
    
    -- 大量データ
    ('E2E_BULK_001', 'e2e.bulk001@test.com', 100, '2024-06-01 13:00:00', 'E2E_BULK_CAMP', 'SENT', '大量テスト'),
    ('E2E_BULK_002', 'e2e.bulk002@test.com', 150, '2024-06-01 13:01:00', 'E2E_BULK_CAMP', 'SENT', '大量テスト'),
    ('E2E_BULK_003', 'e2e.bulk003@test.com', 200, '2024-06-01 13:02:00', 'E2E_BULK_CAMP', 'SENT', '大量テスト'),
    ('E2E_BULK_004', 'e2e.bulk004@test.com', 250, '2024-06-01 13:03:00', 'E2E_BULK_CAMP', 'SENT', '大量テスト'),
    ('E2E_BULK_005', 'e2e.bulk005@test.com', 300, '2024-06-01 13:04:00', 'E2E_BULK_CAMP', 'SENT', '大量テスト'),
    
    -- エラーケース
    ('E2E_ERROR_001', 'e2e.error@test.com', 0, '2024-06-07 10:00:00', 'E2E_ERROR_CAMP', 'ERROR', 'システムエラーテスト'),
    ('E2E_SPECIAL_001', 'e2e.special@test.com', 999, '2024-06-07 11:00:00', 'E2E_SPECIAL_CAMP', 'SENT', '特殊文字テスト');

PRINT 'Point grant email data inserted: ' + CAST(@@ROWCOUNT AS NVARCHAR(10)) + ' records';

-- =============================================================================
-- マーケティング顧客データマート（marketing_client_dm）
-- =============================================================================

PRINT 'Inserting marketing client data...';

INSERT INTO [dbo].[marketing_client_dm]
    ([client_id], [marketing_segment], [preference_category], [engagement_score], [last_campaign_response], [opt_in_email], [opt_in_sms], [customer_value])
VALUES
    ('E2E_CLIENT_001', 'HIGH_ENGAGEMENT', 'ELECTRONICS', 9.5, '2024-06-01', 1, 1, 'PREMIUM'),
    ('E2E_CLIENT_002', 'HIGH_ENGAGEMENT', 'FASHION', 8.8, '2024-05-30', 1, 0, 'HIGH'),
    ('E2E_CLIENT_003', 'MEDIUM_ENGAGEMENT', 'BOOKS', 7.2, '2024-05-25', 1, 0, 'STANDARD'),
    ('E2E_CLIENT_004', 'LOW_ENGAGEMENT', 'TRAVEL', 4.5, '2024-04-20', 0, 0, 'LOW'),
    ('E2E_CLIENT_005', 'PENDING_ANALYSIS', 'UNKNOWN', 6.0, '2024-05-15', 1, 1, 'STANDARD'),
    
    -- 大量データ
    ('E2E_BULK_001', 'BULK_SEGMENT', 'HOUSEHOLD', 7.0, '2024-06-01', 1, 0, 'STANDARD'),
    ('E2E_BULK_002', 'BULK_SEGMENT', 'HOUSEHOLD', 7.1, '2024-06-01', 1, 0, 'STANDARD'),
    ('E2E_BULK_003', 'BULK_SEGMENT', 'HOUSEHOLD', 7.2, '2024-06-01', 1, 0, 'STANDARD'),
    ('E2E_BULK_004', 'BULK_SEGMENT', 'HOUSEHOLD', 7.3, '2024-06-01', 1, 0, 'STANDARD'),
    ('E2E_BULK_005', 'BULK_SEGMENT', 'HOUSEHOLD', 7.4, '2024-06-01', 1, 0, 'STANDARD'),
    
    -- 特殊ケース
    ('E2E_ERROR_001', 'ERROR_SEGMENT', 'ERROR_CATEGORY', 0.0, '1900-01-01', 0, 0, 'ERROR'),
    ('E2E_SPECIAL_001', 'SPECIAL_SEGMENT', 'SPECIAL_CATEGORY', 10.0, '2024-06-07', 1, 1, 'PREMIUM');

PRINT 'Marketing client data inserted: ' + CAST(@@ROWCOUNT AS NVARCHAR(10)) + ' records';

-- =============================================================================
-- E2Eテスト実行ログの初期化
-- =============================================================================

PRINT 'Inserting E2E test execution log entries...';

INSERT INTO [etl].[e2e_test_execution_log]
    ([test_suite], [test_name], [test_status], [start_time], [end_time], [duration_seconds], [test_data_count])
VALUES
    ('E2E_SETUP', 'Comprehensive Test Data Setup', 'COMPLETED', GETDATE(), GETDATE(), 1, 12),
    ('E2E_VALIDATION', 'Table Structure Validation', 'PENDING', GETDATE(), NULL, NULL, 0),
    ('E2E_CONNECTION', 'Database Connection Test', 'PENDING', GETDATE(), NULL, NULL, 0);

PRINT 'E2E test execution log initialized: ' + CAST(@@ROWCOUNT AS NVARCHAR(10)) + ' records';

-- =============================================================================
-- テストデータ管理エントリの作成
-- =============================================================================

PRINT 'Inserting test data management entries...';

INSERT INTO [staging].[test_data_management]
    ([table_name], [test_scenario], [test_data], [data_status], [cleanup_required])
VALUES
    ('client_dm', 'E2E_BASIC_CRUD', 'E2E_CLIENT_001,E2E_CLIENT_002,E2E_CLIENT_003', 'ACTIVE', 1),
    ('ClientDmBx', 'E2E_DATAMART_PIPELINE', 'E2E_CLIENT_001,E2E_CLIENT_002,E2E_CLIENT_003', 'ACTIVE', 1),
    ('point_grant_email', 'E2E_EMAIL_PIPELINE', 'E2E_CLIENT_001,E2E_CLIENT_002,E2E_CLIENT_003', 'ACTIVE', 1),
    ('marketing_client_dm', 'E2E_MARKETING_PIPELINE', 'E2E_CLIENT_001,E2E_CLIENT_002,E2E_CLIENT_003', 'ACTIVE', 1),
    ('client_dm', 'E2E_BULK_PROCESSING', 'E2E_BULK_001,E2E_BULK_002,E2E_BULK_003,E2E_BULK_004,E2E_BULK_005', 'ACTIVE', 1),
    ('client_dm', 'E2E_ERROR_HANDLING', 'E2E_ERROR_001,E2E_SPECIAL_001', 'ACTIVE', 1);

PRINT 'Test data management entries created: ' + CAST(@@ROWCOUNT AS NVARCHAR(10)) + ' records';

-- =============================================================================
-- データ整合性検証
-- =============================================================================

PRINT 'Running data integrity validation...';

-- 基本データ数確認
DECLARE @client_count INT, @clientdmbx_count INT, @email_count INT, @marketing_count INT;

SELECT @client_count = COUNT(*) FROM [dbo].[client_dm] WHERE client_id LIKE 'E2E_%';
SELECT @clientdmbx_count = COUNT(*) FROM [dbo].[ClientDmBx] WHERE client_id LIKE 'E2E_%';
SELECT @email_count = COUNT(*) FROM [dbo].[point_grant_email] WHERE client_id LIKE 'E2E_%';
SELECT @marketing_count = COUNT(*) FROM [dbo].[marketing_client_dm] WHERE client_id LIKE 'E2E_%';

PRINT 'Data integrity check results:';
PRINT '  - client_dm records: ' + CAST(@client_count AS NVARCHAR(10));
PRINT '  - ClientDmBx records: ' + CAST(@clientdmbx_count AS NVARCHAR(10));
PRINT '  - point_grant_email records: ' + CAST(@email_count AS NVARCHAR(10));
PRINT '  - marketing_client_dm records: ' + CAST(@marketing_count AS NVARCHAR(10));

-- データ整合性確認
IF @client_count = @clientdmbx_count AND @client_count = @email_count AND @client_count = @marketing_count
BEGIN
    PRINT '✅ Data integrity validation PASSED - All tables have matching record counts';
END
ELSE
BEGIN
    PRINT '⚠️  Data integrity validation WARNING - Record counts do not match across tables';
END

-- テストデータサマリーの表示
PRINT 'E2E Test Data Summary:';
SELECT * FROM [dbo].[v_e2e_test_data_summary] ORDER BY table_name;

PRINT '🎉 Comprehensive test data setup completed successfully!';
PRINT 'Ready for enhanced E2E testing with improved data coverage and error handling scenarios.';
GO
