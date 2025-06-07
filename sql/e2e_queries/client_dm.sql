-- =========================================================================
-- Client DM パイプライン用SQLクエリ集  
-- テスト対象: pi_Send_ClientDM, pi_Insert_ClientDmBx
-- =========================================================================

-- 基本的な顧客DMテストデータ準備
-- @name: basic_client_dm_setup
DELETE FROM [omni].[omni_ods_marketing_trn_client_dm] WHERE CUSTOMER_ID LIKE 'E2E_TEST_%';

-- ガス契約ありのクライアント
-- @name: gas_contract_client_insert
INSERT INTO [omni].[omni_ods_marketing_trn_client_dm]
    (CUSTOMER_ID, LIV0EU_4X, EPCISCRT_3X)
VALUES
    ('E2E_TEST_CLIENT_001', '4X_TEST_001', NULL),
    ('E2E_TEST_CLIENT_003', '4X_TEST_003', '3X_TEST_003');

-- 電気契約のみのクライアント
-- @name: electric_only_client_insert
INSERT INTO [omni].[omni_ods_marketing_trn_client_dm]
    (CUSTOMER_ID, LIV0EU_4X, EPCISCRT_3X, EPCISCRT_LIGHTING_SA_ID)
VALUES
    ('E2E_TEST_CLIENT_002', NULL, '3X_TEST_001', 'SA_ID_001');

-- 利用サービステストデータクリーンアップ
-- @name: usage_service_cleanup
DELETE FROM [omni].[omni_ods_cloak_trn_usageservice] WHERE BX LIKE 'E2E_TEST_%';

-- ガス契約テストデータ投入
-- @name: gas_usage_service_insert
INSERT INTO [omni].[omni_ods_cloak_trn_usageservice]
    (BX, SERVICE_KEY1, SERVICE_KEY_TYPE1, SERVICE_TYPE, TRANSFER_TYPE,
    TRANSFER_YMD, SERVICE_KEY_START_YMD, OUTPUT_DATE)
VALUES
    ('E2E_TEST_BX001', '4X_TEST_001', '004', '001', '02', '2024-01-01', '2024-01-01', GETDATE()),
    ('E2E_TEST_BX003', '4X_TEST_003', '004', '001', '02', '2024-01-01', '2024-01-01', GETDATE());

-- 電気契約テストデータ投入
-- @name: electric_usage_service_insert
INSERT INTO [omni].[omni_ods_cloak_trn_usageservice]
    (BX, USER_KEY, USER_KEY_TYPE, SERVICE_KEY1, SERVICE_KEY_TYPE1,
    SERVICE_TYPE, TRANSFER_TYPE, TRANSFER_YMD, SERVICE_KEY_START_YMD, OUTPUT_DATE)
VALUES
    ('E2E_TEST_BX002', '3X_TEST_001', '007', 'SA_ID_001', '006', '003', '02', '2024-01-01', '2024-01-01', GETDATE());

-- エラーハンドリング用不正データ投入
-- @name: error_handling_insert
INSERT INTO [omni].[omni_ods_marketing_trn_client_dm]
    (CUSTOMER_ID, LIV0EU_4X, EPCISCRT_3X)
VALUES
    ('E2E_ERROR_TEST', NULL, NULL);

-- Bx3x作業テーブル更新（電気契約）
-- @name: bx3x_temp_table_insert
INSERT INTO [omni].[omni_ods_cloak_trn_usageservice_bx3xsaid_temp]
SELECT BX, USER_KEY as KEY_3X, SERVICE_KEY1 as KEY_SA_ID,
    1 as INDEX_ID, TRANSFER_YMD, SERVICE_KEY_START_YMD, OUTPUT_DATE
FROM [omni].[omni_ods_cloak_trn_usageservice]
WHERE SERVICE_TYPE='006' AND TRANSFER_TYPE='02';

-- Bx4x作業テーブル更新（ガス契約）
-- @name: bx4x_temp_table_insert
INSERT INTO [omni].[omni_ods_cloak_trn_usageservice_bx4x_temp]
SELECT BX, SERVICE_KEY1 as KEY_4X, 1 as INDEX_ID,
    TRANSFER_YMD, SERVICE_KEY_START_YMD, OUTPUT_DATE
FROM [omni].[omni_ods_cloak_trn_usageservice]
WHERE SERVICE_TYPE='001' AND BX IS NOT NULL;

-- 重複チェッククエリ（Bx3x）
-- @name: bx3x_duplicate_check
SELECT COUNT(*) as duplicate_count
FROM (
    SELECT BX, KEY_3X, KEY_SA_ID, COUNT(*) as cnt
    FROM [omni].[omni_ods_cloak_trn_usageservice_bx3xsaid_temp]
    WHERE BX IS NOT NULL AND KEY_3X IS NOT NULL AND KEY_SA_ID IS NOT NULL
    GROUP BY BX, KEY_3X, KEY_SA_ID
    HAVING COUNT(*) > 1
) duplicates;

-- 重複チェッククエリ（Bx4x）
-- @name: bx4x_duplicate_check
SELECT COUNT(*) as duplicate_count
FROM (
    SELECT BX, KEY_4X, COUNT(*) as cnt
    FROM [omni].[omni_ods_cloak_trn_usageservice_bx4x_temp]
    WHERE BX IS NOT NULL AND KEY_4X IS NOT NULL
    GROUP BY BX, KEY_4X
    HAVING COUNT(*) > 1
) duplicates;

-- ガス契約レコード数確認
-- @name: gas_contract_count
SELECT COUNT(*) as gas_contract_count
FROM [omni].[omni_ods_marketing_trn_client_dm_bx_temp]
WHERE LIV0EU_4X IS NOT NULL;

-- 電気契約単独レコード確認
-- @name: electric_only_count
SELECT COUNT(*) as electric_only_count
FROM [omni].[omni_ods_marketing_trn_client_dm_bx_temp]
WHERE LIV0EU_4X IS NULL
    AND (EPCISCRT_LIGHTING_SA_ID IS NOT NULL OR EPCISCRT_POWER_SA_ID IS NOT NULL);

-- NULL値チェック（BX）
-- @name: null_bx_check
SELECT COUNT(*) as null_count
FROM [omni].[omni_ods_cloak_trn_usageservice_bx4x_temp]
WHERE BX IS NULL;

-- 日付形式チェック
-- @name: date_format_check
SELECT COUNT(*) as invalid_count
FROM [omni].[omni_ods_cloak_trn_usageservice_bx4x_temp]
WHERE OUTPUT_DATE IS NOT NULL
    AND TRY_CONVERT(datetime, OUTPUT_DATE) IS NULL;
