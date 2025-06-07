-- =========================================================================
-- Marketing Client DM パイプライン用包括的E2EテストSQLクエリ集
-- テスト対象: pi_Copy_marketing_client_dm
-- 
-- 実際のパイプラインは533カラムを処理する巨大なデータ変換です。
-- このファイルは実際のパイプラインの複雑性を反映したテストを提供します。
-- =========================================================================

-- ========================================
-- セットアップとクリーンアップクエリ
-- ========================================

-- テストデータクリーンアップ
-- @name: comprehensive_test_cleanup
DELETE FROM [omni].[omni_ods_marketing_trn_client_dm_temp] WHERE CLIENT_KEY_AX LIKE 'E2E_TEST_%';
DELETE FROM [omni].[omni_ods_marketing_trn_client_dm] WHERE CLIENT_KEY_AX LIKE 'E2E_TEST_%';

-- ========================================
-- 包括的テストデータセットアップ
-- ========================================

-- 包括的顧客データマスター作成（実際の533カラム構造を模倣）
-- @name: comprehensive_client_master_setup
INSERT INTO [omni].[顧客DM]
    (
    -- 基本情報
    [顧客キー_Ax],
    [LIV0EU_ガスメータ設置場所番号＿１ｘ],
    [LIV0EU_カスタマ番号＿８ｘ],
    [LIV0EU_受持ＮＷ箇所コード],
    [LIV0EU_受持ＮＷ箇所_法人名],
    [LIV0EU_新社集計箇所コード],
    [LIV0EU_適用開始年月日],
    [LIV0EU_適用終了年月日],
    [LIV0EU_使用契約番号＿４ｘ],
    [LIV0EU_支払契約番号＿２ｘ],
    [LIV0EU_郵便番号],
    [LIV0EU_都道府県名漢字],
    [LIV0EU_行政区画コード],
    [LIV0EU_行政区名],
    [LIV0EU_町コード],
    [LIV0EU_町名],
    [LIV0EU_丁目＿字コード],
    [LIV0EU_建物番号],
    [LIV0EU_新設年月],
    [LIV0EU_供内管設備状態コード],
    [LIV0EU_ガスメータ開閉栓状態コード],
    [LIV0EU_用途コード],
    [LIV0EU_ガス用途_集合・戸建分類],
    [LIV0EU_ガス用途_大分類],
    [LIV0EU_ガス用途_中分類],
    [LIV0EU_ガス用途_小分類],
    [LIV0EU_ガス用途_家庭用詳細],
    [LIV0EU_ガス用途_業務用詳細],
    [LIV0EU_ガス用途_12セグメント分類],
    [LIV0EU_ガス用途_都市エネ大分類],
    [LIV0EU_ガス用途_都市エネ小分類],
    [LIV0EU_ガス用途_都市エネ官公庁フラグ],
    [LIV0EU_メータ号数流量],
    [LIV0EU_検針方法名称],
    [LIV0EU_ガス料金支払方法コード],
    [LIV0EU_ガス料金支払方法],
    [LIV0EU_料金Ｇコード],
    [LIV0EU_料金Ｇエリア区分コード],
    [LIV0EU_ブロック番号],
    [LIV0EU_ガス料金契約種別コード],
    [LIV0EU_ガス料金契約種別コード契約種名],
    [LIV0EU_ガス料金契約種別コード契約種細目名],
    [LIV0EU_管理課所名称],

    -- 機器情報
    [LIV0共有所有機器_コンロ種別],
    [LIV0共有所有機器_コンロ種別名],
    [LIV0共有所有機器_コンロ_所有機器番号],
    [LIV0共有所有機器_コンロ_販売先コード],
    [LIV0共有所有機器_コンロ_製造年月],
    [LIV0共有所有機器_コンロ_経年数],
    [LIV0共有所有機器_コンロ_経年数カテゴリ],
    [LIV0共有所有機器_給湯機種別],
    [LIV0共有所有機器_給湯機種名称],
    [LIV0共有所有機器_給湯器_所有機器番号],
    [LIV0共有所有機器_給湯器_販売先コード],
    [LIV0共有所有機器_給湯器_製造年月],
    [LIV0共有所有機器_給湯器_経年数],
    [LIV0共有所有機器_給湯器_経年数カテゴリ],

    -- TES情報
    [TES熱源機情報_製造番号],
    [TES熱源機情報_システム種別],
    [TES熱源機情報_システム落成日],
    [TES熱源機情報_メンテ区分コード],
    [TES熱源機情報_所有形態コード],
    [TES熱源機情報_取付年月日],
    [TES熱源機情報_取外年月日],
    [TES熱源機情報_TESメンテ加入資格有無],
    [TES放熱器情報_床暖房有無],
    [TES放熱器情報_バス暖房有無],
    [TESサービス情報_TESメンテ加入有無],
    [TESサービス情報_サービス番号],
    [TESサービス情報_サービス満了年月],
    [TESサービス情報_サービス種別],

    -- 警報機情報
    [警報機情報_警報器台数],
    [警報機情報_警報器取外し済みフラグ],
    [警報機情報_警報器リース中フラグ],
    [警報機情報_警報器リース終了フラグ],

    -- 電力契約情報
    [電力CIS契約情報_お客様登録番号_3x],
    [電力CIS契約情報_電灯_電力契約番号],
    [電力CIS契約情報_電灯_使用開始日],
    [電力CIS契約情報_電灯_使用中止日],
    [電力CIS契約情報_電灯_契約ステータス],
    [電力CIS契約情報_電灯_使用中止理由],
    [電力CIS契約情報_電灯_契約タイプ],
    [電力CIS契約情報_電灯_契約タイプ_名称],
    [電力CIS契約情報_電灯_支払方法],
    [電力CIS契約情報_電灯_支払方法名],
    [電力CIS契約情報_電灯_料金メニュー],
    [電力CIS契約情報_電灯_料金メニュー名],
    [電力CIS契約情報_電灯_契約電流],
    [電力CIS契約情報_電灯_契約容量],
    [電力CIS契約情報_電灯_契約電力],
    [電力CIS契約情報_電灯_契約電力kW換算値],
    [電力CIS契約情報_電灯_用途コード],
    [電力CIS契約情報_電灯_電気用途名],
    [電力CIS契約情報_電灯_電気用途分類],
    [電力CIS契約情報_電灯_電気用途家庭用詳細],

    -- WEB履歴情報（一部）
    [WEB履歴_基準日],
    [WEB履歴_1003_GP_各種手続き_引越し_直近1か月フラグ],
    [WEB履歴_1003_GP_各種手続き_引越し_直近1年フラグ],
    [WEB履歴_1005_GP_各種手続き_申込切替_直近1か月フラグ],
    [WEB履歴_1005_GP_各種手続き_申込切替_直近1年フラグ],
    [WEB履歴_1011_GP_ガス・電気_さすてな_直近1か月フラグ],
    [WEB履歴_1011_GP_ガス・電気_さすてな_直近1年フラグ],

    -- MyTG会員情報
    [myTG会員情報_myTGフラグ],
    [myTG会員情報_myTG会員登録日],
    [myTG会員情報_myTG会員ID],
    [myTG会員情報_myTG性別],
    [myTG会員情報_myTG年代],

    -- 年度別フラグ情報（2021年度）
    [LIV1案件業務_開栓フラグ_2021年度],
    [LIV1案件業務_閉栓フラグ_2021年度],
    [LIV1案件業務_販売フラグ_2021年度],
    [LIV1案件業務_機器修理・点検フラグ_2021年度],
    [LIV1案件業務_能動的接点活動フラグ_2021年度],
    [LIV1案件業務_アフターフォローフラグ_2021年度],

    -- メタデータ
    [レコード作成日時],
    [レコード更新日時]
    )
VALUES
    -- テストケース1: 包括的ガス顧客（家庭用）
    ('E2E_TEST_COMP_001', '1X001', '8X001', 'NW001', '東京ガス株式会社', 'AGG001',
        '2020-01-01', '2099-12-31', '4X_TEST_001', '2X_TEST_001', '1000001', '東京都',
        '13101', '千代田区', 'C001', '丸の内', 'B001', '1-1-1', '202001',
        '01', '01', '001', '01', '家庭用', '給湯', '風呂', '一般家庭', NULL,
        'セグメント01', '都市エネ01', '都市エネ詳細01', 0, '2.5', '自動検針',
        '01', '口座振替', 'G001', '01', 'B001', 'C001', '一般ガス料金',
        '家庭用一般', '東京ガス本社',
        -- 機器情報
        '01', 'ビルトインコンロ', 'EQ001', 'DEALER01', '202001', 3, '3年未満',
        '01', 'エコジョーズ', 'EQ002', 'DEALER02', '202002', 2, '2年未満',
        -- TES情報
        'SN001', 'SYS01', '2020-01-15', '01', '01', '2020-01-15', NULL, 1,
        1, 0, 1, 'SV001', '202512', 'TES01',
        -- 警報機情報
        2, 0, 1, 0,
        -- 電力契約情報
        '3X_TEST_001', 'SA001', '2020-02-01', NULL, '有効', NULL,
        'CT01', '従量電灯B', 'PM01', '口座振替', 'MENU01', 'ずっとも電気1',
        30, NULL, NULL, 3.0, 'UC01', '家庭用', '住宅', '一般家庭',
        -- WEB履歴
        '2024-01-01', 0, 1, 1, 1, 0, 1,
        -- MyTG情報
        1, '2019-06-01', 'MYTG001', '男性', '40代',
        -- 年度別フラグ
        1, 0, 1, 0, 1, 1,
        -- メタデータ
        GETDATE(), GETDATE()),

    -- テストケース2: 電力のみ顧客（業務用）
    ('E2E_TEST_COMP_002', NULL, NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL, '1000002', '東京都',
        '13102', '中央区', 'C002', '銀座', 'B002', '1-2-3', NULL,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
        NULL, NULL,
        -- 機器情報（NULL）
        NULL, NULL, NULL, NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL,
        -- TES情報（NULL）
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL, NULL,
        -- 警報機情報（NULL）
        NULL, NULL, NULL, NULL,
        -- 電力契約情報（業務用）
        '3X_TEST_002', 'SA002', '2021-04-01', NULL, '有効', NULL,
        'CT02', '低圧電力', 'PM02', '請求書', 'MENU02', '業務用電力プラン',
        NULL, 50, 45, 45.0, 'UC02', '業務用', '事業用', '小規模事業所',
        -- WEB履歴
        '2024-01-01', 1, 1, 0, 0, 1, 1,
        -- MyTG情報（未登録）
        0, NULL, NULL, NULL, NULL,
        -- 年度別フラグ
        0, 0, 0, 1, 0, 0,
        -- メタデータ
        GETDATE(), GETDATE()),

    -- テストケース3: ガス・電力セット顧客（高使用量）
    ('E2E_TEST_COMP_003', '1X003', '8X003', 'NW003', '東京ガス株式会社', 'AGG003',
        '2019-04-01', '2099-12-31', '4X_TEST_003', '2X_TEST_003', '1000003', '神奈川県',
        '14201', '横浜市', 'C003', '青葉台', 'B003', '2-3-4', '201904',
        '01', '01', '002', '02', '業務用', '給湯・暖房', '大容量', '飲食店', '飲食業',
        'セグメント02', '都市エネ02', '都市エネ詳細02', 0, '16', '遠隔検針',
        '02', '請求書', 'G002', '02', 'B002', 'C002', '業務用ガス料金',
        '飲食店向け', '神奈川支店',
        -- 機器情報（業務用）
        '03', '業務用コンロ', 'EQ003', 'DEALER03', '201904', 5, '5年以上',
        '03', '業務用給湯器', 'EQ004', 'DEALER04', '201905', 4, '4年未満',
        -- TES情報
        'SN003', 'SYS03', '2019-04-15', '02', '01', '2019-04-15', NULL, 1,
        1, 1, 1, 'SV003', '202612', 'TES03',
        -- 警報機情報
        5, 0, 0, 1,
        -- 電力契約情報（高圧）
        '3X_TEST_003', 'SA003', '2019-04-01', NULL, '有効', NULL,
        'CT03', '高圧電力', 'PM01', '口座振替', 'MENU03', '業務用高圧プラン',
        NULL, 200, 180, 180.0, 'UC03', '業務用', '産業用', '飲食店',
        -- WEB履歴
        '2024-01-01', 0, 0, 1, 1, 1, 1,
        -- MyTG情報
        1, '2020-01-15', 'MYTG003', '法人', '法人',
        -- 年度別フラグ
        1, 1, 1, 1, 1, 1,
        -- メタデータ
        GETDATE(), GETDATE());

-- ========================================
-- データ品質検証クエリ
-- ========================================

-- 全カラム数検証
-- @name: column_count_validation
SELECT COUNT(*) as total_columns
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'omni'
    AND TABLE_NAME = 'omni_ods_marketing_trn_client_dm';

-- 必須フィールド存在チェック
-- @name: required_fields_check
SELECT
    COUNT(CASE WHEN CLIENT_KEY_AX IS NOT NULL THEN 1 END) as client_key_count,
    COUNT(CASE WHEN LIV0EU_1X IS NOT NULL THEN 1 END) as gas_meter_count,
    COUNT(CASE WHEN EPCISCRT_3X IS NOT NULL THEN 1 END) as electric_customer_count,
    COUNT(CASE WHEN LIV0EU_4X IS NOT NULL THEN 1 END) as gas_contract_count,
    COUNT(CASE WHEN EPCISCRT_LIGHTING_SA_ID IS NOT NULL THEN 1 END) as electric_contract_count,
    COUNT(*) as total_records
FROM [omni].[omni_ods_marketing_trn_client_dm]
WHERE CLIENT_KEY_AX LIKE 'E2E_TEST_%';

-- データ型整合性チェック
-- @name: data_type_validation
SELECT
    CLIENT_KEY_AX,
    CASE WHEN TRY_CONVERT(INT, LIV0EU_ガスメータ開閉栓状態コード) IS NOT NULL
        OR LIV0EU_ガスメータ開閉栓状態コード IS NULL 
         THEN 'OK' ELSE 'ERROR' END as gas_meter_status_format,
    CASE WHEN TRY_CONVERT(DATE, EPCISCRT_LIGHTING_START_USE_DATE) IS NOT NULL
        OR EPCISCRT_LIGHTING_START_USE_DATE IS NULL 
         THEN 'OK' ELSE 'ERROR' END as electric_start_date_format,
    CASE WHEN TRY_CONVERT(DECIMAL(10,2), EPCISCRT_LIGHTING_CRT_ELECT_POWER_KW) IS NOT NULL
        OR EPCISCRT_LIGHTING_CRT_ELECT_POWER_KW IS NULL 
         THEN 'OK' ELSE 'ERROR' END as electric_power_format
FROM [omni].[omni_ods_marketing_trn_client_dm]
WHERE CLIENT_KEY_AX LIKE 'E2E_TEST_%';

-- ビジネスルール検証
-- @name: business_rules_validation
SELECT
    CLIENT_KEY_AX,
    CASE WHEN LIV0EU_4X IS NOT NULL AND LIV0EU_1X IS NULL 
         THEN 'WARNING: ガス契約があるがメーター番号なし' 
         ELSE 'OK' END as gas_consistency_check,
    CASE WHEN EPCISCRT_3X IS NOT NULL AND EPCISCRT_LIGHTING_SA_ID IS NULL 
         THEN 'WARNING: 電気顧客番号があるが契約番号なし' 
         ELSE 'OK' END as electric_consistency_check,
    CASE WHEN LIV0EU_ガス用途_大分類 = '家庭用' AND EPCISCRT_LIGHTING_ELECT_USAGE_BRI = '産業用'
         THEN 'WARNING: ガス家庭用・電気産業用の組み合わせ'
         ELSE 'OK' END as usage_consistency_check
FROM [omni].[omni_ods_marketing_trn_client_dm]
WHERE CLIENT_KEY_AX LIKE 'E2E_TEST_%';

-- ========================================
-- パフォーマンス検証クエリ
-- ========================================

-- レコード処理量確認
-- @name: record_processing_validation
SELECT
    COUNT(*) as total_processed_records,
    COUNT(CASE WHEN CLIENT_KEY_AX IS NOT NULL THEN 1 END) as valid_client_keys,
    COUNT(CASE WHEN LIV0EU_4X IS NOT NULL THEN 1 END) as gas_contracts,
    COUNT(CASE WHEN EPCISCRT_3X IS NOT NULL THEN 1 END) as electric_customers,
    MIN(REC_REG_YMD) as earliest_created,
    MAX(REC_UPD_YMD) as latest_updated
FROM [omni].[omni_ods_marketing_trn_client_dm];

-- NULL値分布確認
-- @name: null_distribution_analysis
SELECT
    -- 基本情報のNULL率
    COUNT(CLIENT_KEY_AX) * 100.0 / COUNT(*) as client_key_coverage_pct,
    COUNT(LIV0EU_1X) * 100.0 / COUNT(*) as gas_meter_coverage_pct,
    COUNT(EPCISCRT_3X) * 100.0 / COUNT(*) as electric_customer_coverage_pct,

    -- 機器情報のNULL率
    COUNT([LIV0共有所有機器_コンロ種別]) * 100.0 / COUNT(*) as konro_coverage_pct,
    COUNT([LIV0共有所有機器_給湯機種別]) * 100.0 / COUNT(*) as kyutoki_coverage_pct,

    -- TES情報のNULL率
    COUNT([TES熱源機情報_製造番号]) * 100.0 / COUNT(*) as tes_coverage_pct,

    -- WEB履歴のNULL率
    COUNT([WEB履歴_基準日]) * 100.0 / COUNT(*) as web_history_coverage_pct,

    COUNT(*) as total_records
FROM [omni].[omni_ods_marketing_trn_client_dm]
WHERE CLIENT_KEY_AX LIKE 'E2E_TEST_%';

-- ========================================
-- エラーシナリオテスト
-- ========================================

-- 不正データ挿入テスト
-- @name: invalid_data_insert_test
INSERT INTO [omni].[顧客DM]
    (
    [顧客キー_Ax],
    [LIV0EU_ガス料金支払方法コード],
    [電力CIS契約情報_電灯_契約電流],
    [レコード作成日時]
    )
VALUES
    ('E2E_TEST_ERROR_001', '99', -50, '1900-01-01'),
    -- 不正な支払方法コード、負の契約電流、古い日付
    ('E2E_TEST_ERROR_002', NULL, 999999, NULL);
-- NULL値、異常に大きい値

-- データ整合性エラー検証
-- @name: data_integrity_error_check
SELECT
    CLIENT_KEY_AX,
    'ERROR' as status,
    CASE 
        WHEN LIV0EU_ガス料金支払方法コード NOT IN ('01', '02', '03') 
        THEN 'Invalid payment method code'
        WHEN EPCISCRT_LIGHTING_CRT_ELECT_CURRENT < 0 
        THEN 'Negative electric current'
        WHEN REC_REG_YMD < '1990-01-01' 
        THEN 'Invalid creation date'
        ELSE 'Unknown error'
    END as error_description
FROM [omni].[omni_ods_marketing_trn_client_dm]
WHERE CLIENT_KEY_AX LIKE 'E2E_TEST_ERROR_%'
    AND (LIV0EU_ガス料金支払方法コード NOT IN ('01', '02', '03', NULL)
    OR EPCISCRT_LIGHTING_CRT_ELECT_CURRENT < 0
    OR REC_REG_YMD < '1990-01-01');

-- ========================================
-- パイプライン段階別検証
-- ========================================

-- 第1段階: temp テーブルへのコピー検証
-- @name: stage1_temp_table_validation
SELECT
    'TEMP_TABLE' as stage,
    COUNT(*) as record_count,
    COUNT(DISTINCT CLIENT_KEY_AX) as unique_clients,
    MIN(REC_REG_YMD) as min_created_date,
    MAX(REC_UPD_YMD) as max_updated_date
FROM [omni].[omni_ods_marketing_trn_client_dm_temp]
WHERE CLIENT_KEY_AX LIKE 'E2E_TEST_%';

-- 第2段階: 本テーブルへのコピー検証
-- @name: stage2_main_table_validation
SELECT
    'MAIN_TABLE' as stage,
    COUNT(*) as record_count,
    COUNT(DISTINCT CLIENT_KEY_AX) as unique_clients,
    MIN(REC_REG_YMD) as min_created_date,
    MAX(REC_UPD_YMD) as max_updated_date
FROM [omni].[omni_ods_marketing_trn_client_dm]
WHERE CLIENT_KEY_AX LIKE 'E2E_TEST_%';

-- 段階間データ整合性確認
-- @name: inter_stage_consistency_check
    SELECT
        t.CLIENT_KEY_AX,
        CASE WHEN m.CLIENT_KEY_AX IS NOT NULL THEN 'OK' ELSE 'MISSING_IN_MAIN' END as consistency_status,
        t.REC_UPD_YMD as temp_updated,
        m.REC_UPD_YMD as main_updated
    FROM [omni].[omni_ods_marketing_trn_client_dm_temp] t
        LEFT JOIN [omni].[omni_ods_marketing_trn_client_dm] m
        ON t.CLIENT_KEY_AX = m.CLIENT_KEY_AX
    WHERE t.CLIENT_KEY_AX LIKE 'E2E_TEST_%'
UNION
    SELECT
        m.CLIENT_KEY_AX,
        CASE WHEN t.CLIENT_KEY_AX IS NOT NULL THEN 'OK' ELSE 'MISSING_IN_TEMP' END as consistency_status,
        t.REC_UPD_YMD as temp_updated,
        m.REC_UPD_YMD as main_updated
    FROM [omni].[omni_ods_marketing_trn_client_dm] m
        LEFT JOIN [omni].[omni_ods_marketing_trn_client_dm_temp] t
        ON m.CLIENT_KEY_AX = t.CLIENT_KEY_AX
    WHERE m.CLIENT_KEY_AX LIKE 'E2E_TEST_%';

-- ========================================
-- 最終クリーンアップ
-- ========================================

-- テストデータ削除
-- @name: final_cleanup
DELETE FROM [omni].[omni_ods_marketing_trn_client_dm_temp] WHERE CLIENT_KEY_AX LIKE 'E2E_TEST_%';
DELETE FROM [omni].[omni_ods_marketing_trn_client_dm] WHERE CLIENT_KEY_AX LIKE 'E2E_TEST_%';
DELETE FROM [omni].[顧客DM] WHERE [顧客キー_Ax] LIKE 'E2E_TEST_%';
