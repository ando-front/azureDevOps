-- =========================================================================
-- LIM Settlement Breakdown Repair パイプライン用SQLクエリ集
-- テスト対象: pi_Send_LIMSettlementBreakdownRepair
-- =========================================================================

-- データ品質テスト用データセット（正常・異常混在）
-- @name: settlement_quality_test_data
/*
正常データ:
{
    "connection_key": "QUALITY001",
    "settlement_period": "2024Q1",
    "amount_original": "1500000.00",
    "amount_corrected": "1520000.00",
    "correction_reason": "品質テスト正常データ",
    "processing_date": "2024-04-15",
    "status": "pending",
    "category": "electricity_settlement"
}

大金額データ:
{
    "connection_key": "QUALITY002",
    "settlement_period": "2024Q1",
    "amount_original": "999999999.99",
    "amount_corrected": "1000000000.00",
    "correction_reason": "大金額テスト",
    "processing_date": "2024-04-15",
    "status": "pending",
    "category": "gas_settlement"
}

特殊文字含有データ:
{
    "connection_key": "QUALITY003",
    "settlement_period": "2024Q1",
    "amount_original": "750000.00",
    "amount_corrected": "755000.00",
    "correction_reason": "特殊文字テスト\"'<>&",
    "processing_date": "2024-04-15",
    "status": "pending",
    "category": "electricity_settlement"
}
*/

-- LIM決算修正データテーブル作成
-- @name: create_lim_settlement_table
CREATE TABLE lim_settlement_quality_test
(
    id INT IDENTITY(1,1) PRIMARY KEY,
    connection_key NVARCHAR(100),
    settlement_period NVARCHAR(20),
    amount_original DECIMAL(18,2),
    amount_corrected DECIMAL(18,2),
    correction_reason NVARCHAR(MAX),
    processing_date DATE,
    status NVARCHAR(50),
    category NVARCHAR(100),
    created_at DATETIME2 DEFAULT GETDATE()
);

-- LIM決算テストデータ投入
-- @name: settlement_test_data_insert
INSERT INTO lim_settlement_quality_test
    (connection_key, settlement_period, amount_original, amount_corrected,
    correction_reason, processing_date, status, category)
VALUES
    ('QUALITY001', '2024Q1', 1500000.00, 1520000.00, '品質テスト正常データ', '2024-04-15', 'pending', 'electricity_settlement'),
    ('QUALITY002', '2024Q1', 999999999.99, 1000000000.00, '大金額テスト', '2024-04-15', 'pending', 'gas_settlement'),
    ('QUALITY003', '2024Q1', 750000.00, 755000.00, '特殊文字テスト"''<>&', '2024-04-15', 'pending', 'electricity_settlement');

-- 完全性チェック（必須フィールド）
-- @name: completeness_check
SELECT
    connection_key,
    settlement_period,
    amount_original,
    amount_corrected,
    correction_reason,
    processing_date,
    status,
    category,
    CASE 
        WHEN connection_key IS NULL THEN 0
        WHEN settlement_period IS NULL THEN 0
        WHEN amount_original IS NULL THEN 0
        WHEN amount_corrected IS NULL THEN 0
        WHEN correction_reason IS NULL THEN 0
        WHEN processing_date IS NULL THEN 0
        WHEN status IS NULL THEN 0
        WHEN category IS NULL THEN 0
        ELSE 1
    END as is_complete
FROM lim_settlement_quality_test;

-- 金額フォーマット検証
-- @name: amount_format_validation
SELECT
    connection_key,
    amount_original,
    amount_corrected,
    CASE 
        WHEN amount_original < 0 THEN 0
        WHEN amount_corrected < 0 THEN 0
        WHEN amount_original > 999999999.99 THEN 0
        WHEN amount_corrected > 999999999.99 THEN 0
        ELSE 1
    END as amount_valid
FROM lim_settlement_quality_test;

-- 期間検証
-- @name: period_validation
SELECT
    connection_key,
    settlement_period,
    CASE 
        WHEN settlement_period LIKE '____Q[1-4]' THEN 1
        WHEN settlement_period LIKE '____-__' THEN 1
        ELSE 0
    END as period_valid
FROM lim_settlement_quality_test;

-- 特殊文字処理検証
-- @name: special_char_validation
SELECT
    connection_key,
    correction_reason,
    LEN(correction_reason) as reason_length,
    CASE 
        WHEN correction_reason LIKE '%<%' OR correction_reason LIKE '%>%'
        OR correction_reason LIKE '%&%' OR correction_reason LIKE '%"%'
        OR correction_reason LIKE '%''%' THEN 1
        ELSE 0
    END as has_special_chars
FROM lim_settlement_quality_test;

-- テストデータクリーンアップ
-- @name: cleanup_settlement_test_data
DROP TABLE IF EXISTS lim_settlement_quality_test;
