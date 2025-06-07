-- =========================================================================
-- パイプライン監視・実行ログ関連テーブル作成クエリ
-- =========================================================================

-- パイプライン実行ログテーブル作成
-- @name: pipeline_execution_log_table_create
CREATE TABLE pipeline_execution_log
(
    execution_id varchar(50) PRIMARY KEY,
    pipeline_name varchar(100) NOT NULL,
    run_id varchar(50),
    status varchar(20),
    start_time datetime2,
    end_time datetime2,
    duration_seconds int,
    triggered_by varchar(100),
    error_message varchar(max),
    activities_completed int DEFAULT 0,
    activities_failed int DEFAULT 0,
    data_read bigint DEFAULT 0,
    data_written bigint DEFAULT 0,
    created_at datetime2 DEFAULT GETDATE()
);

-- パイプラインリソース使用量テーブル作成
-- @name: pipeline_resource_usage_table_create
CREATE TABLE pipeline_resource_usage
(
    usage_id varchar(50) PRIMARY KEY,
    execution_id varchar(50),
    cpu_usage_percent decimal(5,2),
    memory_usage_mb bigint,
    disk_io_mb bigint,
    network_io_mb bigint,
    concurrent_activities int,
    peak_memory_mb bigint,
    recorded_at datetime2 DEFAULT GETDATE(),
    FOREIGN KEY (execution_id) REFERENCES pipeline_execution_log(execution_id)
);

-- パイプラインスケジュール設定テーブル作成
-- @name: pipeline_schedule_config_table_create
CREATE TABLE pipeline_schedule_config
(
    config_id varchar(50) PRIMARY KEY,
    pipeline_name varchar(100) NOT NULL,
    schedule_type varchar(20),
    cron_expression varchar(100),
    timezone varchar(50),
    enabled bit DEFAULT 1,
    next_run_time datetime2,
    last_updated datetime2 DEFAULT GETDATE()
);

-- パイプライン依存関係テーブル作成
-- @name: pipeline_dependencies_table_create
CREATE TABLE pipeline_dependencies
(
    dependency_id varchar(50) PRIMARY KEY,
    parent_pipeline varchar(100),
    child_pipeline varchar(100),
    dependency_type varchar(20),
    condition_expression varchar(500),
    enabled bit DEFAULT 1,
    created_at datetime2 DEFAULT GETDATE()
);

-- 条件付きトリガーテーブル作成
-- @name: conditional_triggers_table_create
CREATE TABLE conditional_triggers
(
    trigger_id varchar(50) PRIMARY KEY,
    trigger_name varchar(100) NOT NULL,
    pipeline_name varchar(100),
    condition_type varchar(50),
    condition_value varchar(500),
    enabled bit DEFAULT 1,
    last_evaluated datetime2,
    created_at datetime2 DEFAULT GETDATE()
);

-- トリガー評価ログテーブル作成
-- @name: trigger_evaluation_log_table_create
CREATE TABLE trigger_evaluation_log
(
    log_id varchar(50) PRIMARY KEY,
    trigger_id varchar(50),
    evaluation_time datetime2,
    condition_met bit,
    pipeline_triggered bit,
    evaluation_result varchar(max),
    FOREIGN KEY (trigger_id) REFERENCES conditional_triggers(trigger_id)
);

-- =========================================================================
-- パイプライン実行・統合テスト用SQLクエリ集
-- テスト対象: 統合パイプライン実行、エラーハンドリング、パフォーマンステスト
-- =========================================================================

-- 統合テスト用データ準備ベースクエリ
-- @name: integration_base_data_insert
INSERT INTO [omni].[omni_ods_cloak_trn_usageservice]
    (BX, SERVICE_KEY1, SERVICE_KEY_TYPE1, USER_KEY, USER_KEY_TYPE,
    SERVICE_TYPE, TRANSFER_TYPE, TRANSFER_YMD, SERVICE_KEY_START_YMD, OUTPUT_DATE)
VALUES
    ('INTEGRATION_BX001', 'PREP_4X_001', '004', NULL, NULL, '001', '02', '2024-01-01', '2024-01-01', GETDATE()),
    ('INTEGRATION_BX002', 'PREP_SA_001', '007', 'PREP_3X_001', '003', '006', '02', '2024-01-01', '2024-01-01', GETDATE());

-- @name: integration_client_dm_insert
INSERT INTO [omni].[omni_ods_marketing_trn_client_dm]
    (CUSTOMER_ID, LIV0EU_4X, EPCISCRT_3X, EPCISCRT_LIGHTING_SA_ID)
VALUES
    ('INTEGRATION_CLIENT_001', 'PREP_4X_001', NULL, NULL),
    ('INTEGRATION_CLIENT_002', NULL, 'PREP_3X_001', 'PREP_SA_001');

-- ポイント付与テストファイル用データ
-- @name: point_grant_test_data
/*
CustomerID	Email	PointAmount	CampaignCode	ProcessDate
CUST001	test1@example.com	1000	CAMP001	2024-01-15
CUST002	test2@example.com	1500	CAMP001	2024-01-15
CUST003	test3@example.com	2000	CAMP002	2024-01-15
CUST004	test4@example.com	500	CAMP001	2024-01-15
CUST005	test5@example.com	3000	CAMP003	2024-01-15
*/

-- ガス・電気契約分離確認クエリ
-- @name: contract_type_distribution
SELECT
    COUNT(CASE WHEN LIV0EU_4X IS NOT NULL THEN 1 END) as gas_contracts,
    COUNT(CASE WHEN LIV0EU_4X IS NULL AND EPCISCRT_3X IS NOT NULL THEN 1 END) as electric_only
FROM [omni].[omni_ods_marketing_trn_client_dm_bx_temp];

-- 統合テスト用クリーンアップ
-- @name: integration_cleanup
DELETE FROM [omni].[omni_ods_cloak_trn_usageservice] WHERE BX LIKE 'INTEGRATION_%';
DELETE FROM [omni].[omni_ods_marketing_trn_client_dm] WHERE CUSTOMER_ID LIKE 'INTEGRATION_%';

-- パフォーマンステスト用大容量データ生成
-- @name: performance_test_data_generation
INSERT INTO [omni].[omni_ods_marketing_trn_client_dm]
SELECT TOP 10000
    CONCAT('PERF_CLIENT_', ROW_NUMBER() OVER (ORDER BY (SELECT NULL))) as CUSTOMER_ID,
    CASE WHEN ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) % 2 = 0 
         THEN CONCAT('4X_PERF_', ROW_NUMBER() OVER (ORDER BY (SELECT NULL)))
         ELSE NULL END as LIV0EU_4X,
    CASE WHEN ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) % 3 = 0 
         THEN CONCAT('3X_PERF_', ROW_NUMBER() OVER (ORDER BY (SELECT NULL)))
         ELSE NULL END as EPCISCRT_3X,
    CONCAT('perf_test_', ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), '@example.com') as EMAIL_ADDRESS
FROM sys.objects a, sys.objects b;

-- エラー処理テスト用不正データ
-- @name: error_handling_invalid_data
INSERT INTO [omni].[omni_ods_marketing_trn_client_dm]
    (CUSTOMER_ID, LIV0EU_4X, EPCISCRT_3X)
VALUES
    ('ERROR_TEST_001', NULL, NULL),
    ('ERROR_TEST_002', 'INVALID_FORMAT_4X', 'INVALID_FORMAT_3X'),
    (NULL, 'VALID_4X', 'VALID_3X');

-- データ整合性チェック
-- @name: data_consistency_check
SELECT
    COUNT(*) as total_records,
    COUNT(CASE WHEN LIV0EU_4X IS NULL AND EPCISCRT_3X IS NULL THEN 1 END) as no_contract_records,
    COUNT(CASE WHEN LIV0EU_4X IS NOT NULL AND EPCISCRT_3X IS NOT NULL THEN 1 END) as dual_contract_records,
    COUNT(CASE WHEN CUSTOMER_ID IS NULL THEN 1 END) as null_customer_id_records
FROM [omni].[omni_ods_marketing_trn_client_dm_bx_temp];

-- ビジネスルール整合性確認（電気契約）
-- @name: electric_business_rule_check
SELECT COUNT(*) as mismatch_count
FROM [omni].[omni_ods_marketing_trn_client_dm_bx_temp] cdm
    LEFT JOIN [omni].[omni_ods_cloak_trn_usageservice_bx3xsaid_temp] bx3x
    ON cdm.EPCISCRT_3X = bx3x.KEY_3X
        AND ISNULL(cdm.EPCISCRT_LIGHTING_SA_ID, cdm.EPCISCRT_POWER_SA_ID) = bx3x.KEY_SA_ID
WHERE cdm.LIV0EU_4X IS NULL
    AND cdm.EPCISCRT_3X IS NOT NULL
    AND bx3x.KEY_3X IS NULL;

-- ビジネスルール整合性確認（ガス契約）
-- @name: gas_business_rule_check
SELECT COUNT(*) as mismatch_count
FROM [omni].[omni_ods_marketing_trn_client_dm_bx_temp] cdm
    LEFT JOIN [omni].[omni_ods_cloak_trn_usageservice_bx4x_temp] bx4x
    ON cdm.LIV0EU_4X = bx4x.KEY_4X
WHERE cdm.LIV0EU_4X IS NOT NULL
    AND bx4x.KEY_4X IS NULL;

-- パイプライン実行前後のカウント確認
-- @name: temp_table_record_count
SELECT COUNT(*)
FROM [omni].[omni_ods_marketing_trn_client_dm_bx_temp];

-- データフロー完全テスト用結果検証
-- @name: data_flow_verification
SELECT COUNT(*) as total_records,
    COUNT(DISTINCT BX) as unique_bx_count,
    COUNT(CASE WHEN BX IS NOT NULL THEN 1 END) as records_with_bx
FROM [omni].[omni_ods_marketing_trn_client_dm_bx_temp];

-- 重複チェック用クエリ
-- @name: duplicate_bx_check
SELECT BX, COUNT(*) as count
FROM [omni].[omni_ods_marketing_trn_client_dm_bx_temp]
GROUP BY BX
HAVING COUNT(*) > 1;

-- 利用サービステストデータクリア
-- @name: usage_service_test_data_cleanup
DELETE FROM [omni].[omni_ods_cloak_trn_usageservice] WHERE BX LIKE 'E2E_TEST_%';

-- ガス契約テストデータ挿入
-- @name: gas_contract_test_data_insert
INSERT INTO [omni].[omni_ods_cloak_trn_usageservice]
    (BX, SERVICE_KEY1, SERVICE_KEY_TYPE1, SERVICE_TYPE, TRANSFER_TYPE,
    TRANSFER_YMD, SERVICE_KEY_START_YMD, OUTPUT_DATE)
VALUES
    (@bx, @service_key1, '004', '001', '02', @transfer_ymd, @service_key_start_ymd, @output_date);

-- 電気契約テストデータ挿入
-- @name: electric_contract_test_data_insert
INSERT INTO [omni].[omni_ods_cloak_trn_usageservice]
    (BX, USER_KEY, USER_KEY_TYPE, SERVICE_KEY1, SERVICE_KEY_TYPE1,
    SERVICE_TYPE, TRANSFER_TYPE, TRANSFER_YMD, SERVICE_KEY_START_YMD, OUTPUT_DATE)
VALUES
    (@bx, @user_key, '003', @service_key1, '007', '006', '02', @transfer_ymd, @service_key_start_ymd, @output_date);

-- 顧客DMテストデータクリア
-- @name: client_dm_test_data_cleanup
DELETE FROM [omni].[omni_ods_marketing_trn_client_dm] WHERE CUSTOMER_ID LIKE 'E2E_TEST_%';

-- ガス契約クライアントテストデータ挿入
-- @name: gas_client_test_data_insert
INSERT INTO [omni].[omni_ods_marketing_trn_client_dm]
    (CUSTOMER_ID, LIV0EU_4X, EPCISCRT_3X)
VALUES
    (@customer_id, @liv0eu_4x, NULL);

-- 電気契約クライアントテストデータ挿入
-- @name: electric_client_test_data_insert
INSERT INTO [omni].[omni_ods_marketing_trn_client_dm]
    (CUSTOMER_ID, LIV0EU_4X, EPCISCRT_3X, EPCISCRT_LIGHTING_SA_ID)
VALUES
    (@customer_id, NULL, @epciscrt_3x, @epciscrt_lighting_sa_id);

-- エラーハンドリングテスト用不正データ挿入
-- @name: error_test_invalid_client_insert
INSERT INTO [omni].[omni_ods_marketing_trn_client_dm]
    (CUSTOMER_ID, LIV0EU_4X, EPCISCRT_3X)
VALUES
    (@customer_id, NULL, NULL);

-- エラーデータ結果確認
-- @name: error_data_result_check
SELECT COUNT(*)
FROM [omni].[omni_ods_marketing_trn_client_dm_bx_temp]
WHERE CUSTOMER_ID = @customer_id AND BX IS NULL;

-- パフォーマンステスト用大量データ挿入（単体）
-- @name: performance_test_single_insert
INSERT INTO [omni].[omni_ods_marketing_trn_client_dm]
    (CUSTOMER_ID, LIV0EU_4X, EPCISCRT_3X)
VALUES
    (@customer_id, @liv0eu_4x, @epciscrt_3x);

-- パフォーマンステスト用処理レコード数確認
-- @name: performance_test_processed_count
SELECT COUNT(*)
FROM [omni].[omni_ods_marketing_trn_client_dm_bx_temp]
WHERE CUSTOMER_ID LIKE 'PERF_TEST_%';

-- パイプライン処理の一時テーブルクリア
-- @name: pipeline_temp_tables_truncate
TRUNCATE TABLE [omni].[omni_ods_cloak_trn_usageservice_bx4x_temp];
TRUNCATE TABLE [omni].[omni_ods_cloak_trn_usageservice_bx3xsaid_temp];
TRUNCATE TABLE [omni].[omni_ods_marketing_trn_client_dm_bx_temp];

-- Bx4xテンポラリテーブル作成（ガス契約）
-- @name: create_bx4x_temp_table
INSERT INTO [omni].[omni_ods_cloak_trn_usageservice_bx4x_temp]
SELECT BX, SERVICE_KEY1 as KEY_4X, 1 as INDEX_ID,
    TRANSFER_YMD, SERVICE_KEY_START_YMD, OUTPUT_DATE
FROM [omni].[omni_ods_cloak_trn_usageservice]
WHERE SERVICE_TYPE='001' AND TRANSFER_TYPE='02'
    AND SERVICE_KEY_TYPE1='004';

-- Bx3xSAIDテンポラリテーブル作成（電気契約）
-- @name: create_bx3xsaid_temp_table
INSERT INTO [omni].[omni_ods_cloak_trn_usageservice_bx3xsaid_temp]
SELECT BX, USER_KEY as KEY_3X, SERVICE_KEY1 as KEY_SA_ID,
    1 as INDEX_ID, TRANSFER_YMD, SERVICE_KEY_START_YMD, OUTPUT_DATE
FROM [omni].[omni_ods_cloak_trn_usageservice]
WHERE SERVICE_TYPE='006' AND TRANSFER_TYPE='02'
    AND USER_KEY_TYPE='003' AND SERVICE_KEY_TYPE1='007';

-- 最終結果テーブル作成（ガス契約ありの場合）
-- @name: create_final_result_gas_contracts
INSERT INTO [omni].[omni_ods_marketing_trn_client_dm_bx_temp]
SELECT usv.BX, usv.INDEX_ID, usv.TRANSFER_YMD,
    usv.SERVICE_KEY_START_YMD, usv.OUTPUT_DATE, cldm.*
FROM [omni].[omni_ods_marketing_trn_client_dm] cldm
    INNER JOIN [omni].[omni_ods_cloak_trn_usageservice_bx4x_temp] usv
    ON cldm.LIV0EU_4X = usv.KEY_4X
WHERE cldm.LIV0EU_4X IS NOT NULL;

-- 最終結果テーブル作成（電気契約のみの場合）
-- @name: create_final_result_electric_contracts
INSERT INTO [omni].[omni_ods_marketing_trn_client_dm_bx_temp]
SELECT usv.BX, usv.INDEX_ID, usv.TRANSFER_YMD,
    usv.SERVICE_KEY_START_YMD, usv.OUTPUT_DATE, cldm.*
FROM [omni].[omni_ods_marketing_trn_client_dm] cldm
    INNER JOIN [omni].[omni_ods_cloak_trn_usageservice_bx3xsaid_temp] usv
    ON cldm.EPCISCRT_3X = usv.KEY_3X
        AND ISNULL(cldm.EPCISCRT_LIGHTING_SA_ID, cldm.EPCISCRT_POWER_SA_ID) = usv.KEY_SA_ID
WHERE cldm.LIV0EU_4X IS NULL;

-- 統合テスト結果検証
-- @name: integration_test_result_verification
SELECT
    COUNT(*) as total_processed,
    COUNT(DISTINCT BX) as unique_customers,
    MAX(OUTPUT_DATE) as latest_processing_date
FROM [omni].[omni_ods_marketing_trn_client_dm_bx_temp]
WHERE OUTPUT_DATE >= @execution_date;

-- 依存テーブル存在確認
-- @name: dependency_table_exists_check
SELECT COUNT(*)
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME = @table_name;

-- テーブル間関連性確認
-- @name: table_relationship_validation
SELECT
    COUNT(*) as records_with_valid_relationships
FROM [omni].[omni_ods_marketing_trn_client_dm] cd
    LEFT JOIN [omni].[omni_ods_cloak_trn_usageservice] us
    ON cd.LIV0EU_4X = us.SERVICE_KEY1
        OR cd.EPCISCRT_3X = us.USER_KEY
WHERE us.BX IS NOT NULL;

-- 後処理パイプライン用OUTPUT_DATE更新
-- @name: post_processing_output_date_update
UPDATE [omni].[omni_ods_marketing_trn_client_dm_bx_temp]
SET OUTPUT_DATE = GETDATE()
WHERE OUTPUT_DATE IS NULL OR OUTPUT_DATE < @execution_date;
