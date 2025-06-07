-- =========================================================================
-- データ品質・セキュリティテスト用SQLクエリ集
-- テスト対象: データ品質管理、データ系譜追跡、セキュリティテスト
-- =========================================================================

-- データ品質ルールテーブル作成
-- @name: data_quality_rules_table_create
CREATE TABLE data_quality_rules
(
    id INT IDENTITY(1,1) PRIMARY KEY,
    rule_name NVARCHAR(200) NOT NULL,
    table_name NVARCHAR(200) NOT NULL,
    rule_type NVARCHAR(50) NOT NULL,
    column_name NVARCHAR(100),
    rule_condition NVARCHAR(MAX) NOT NULL,
    threshold_percent DECIMAL(5,2) NOT NULL,
    severity NVARCHAR(20) NOT NULL,
    is_active BIT DEFAULT 1,
    created_at DATETIME2 DEFAULT GETDATE()
);

-- データ品質結果テーブル作成
-- @name: data_quality_results_table_create
CREATE TABLE data_quality_results
(
    id INT IDENTITY(1,1) PRIMARY KEY,
    rule_name NVARCHAR(200) NOT NULL,
    execution_time DATETIME2 NOT NULL,
    total_records INT,
    valid_records INT,
    invalid_records INT,
    quality_score DECIMAL(5,2),
    passed BIT,
    error_details NVARCHAR(MAX),
    threshold_met BIT,
    issues_found NVARCHAR(MAX),
    created_at DATETIME2 DEFAULT GETDATE()
);

-- データ系譜追跡テーブル作成
-- @name: data_lineage_tracking_table_create
CREATE TABLE data_lineage_tracking
(
    lineage_id varchar(50) PRIMARY KEY,
    pipeline_name varchar(100) NOT NULL,
    activity_name varchar(100) NOT NULL,
    source_system varchar(100),
    source_table varchar(200),
    target_system varchar(100),
    target_table varchar(200),
    transformation_type varchar(50),
    data_transformation_rules varchar(max),
    execution_time datetime2,
    records_processed bigint,
    business_date date,
    created_at datetime2 DEFAULT GETDATE()
);

-- データプロファイリング結果テーブル作成
-- @name: data_profiling_results_table_create
CREATE TABLE data_profiling_results
(
    profile_id varchar(50) PRIMARY KEY,
    table_name varchar(200) NOT NULL,
    column_name varchar(100) NOT NULL,
    data_type varchar(50),
    total_count bigint,
    null_count bigint,
    distinct_count bigint,
    min_value varchar(500),
    max_value varchar(500),
    avg_value decimal(18,6),
    std_deviation decimal(18,6),
    pattern_analysis varchar(max),
    profiled_at datetime2,
    business_date date,
    created_at datetime2 DEFAULT GETDATE()
);

-- セキュリティアクセス監査テーブル作成
-- @name: security_access_audit_table_create
CREATE TABLE security_access_audit
(
    audit_id varchar(50) PRIMARY KEY,
    user_identity varchar(200) NOT NULL,
    resource_accessed varchar(500) NOT NULL,
    access_type varchar(50),
    access_granted bit,
    access_time datetime2,
    ip_address varchar(45),
    user_agent varchar(500),
    created_at datetime2 DEFAULT GETDATE()
);

-- セキュリティ暗号化監査テーブル作成
-- @name: security_encryption_audit_table_create
CREATE TABLE security_encryption_audit
(
    encryption_id varchar(50) PRIMARY KEY,
    resource_path varchar(500) NOT NULL,
    encryption_type varchar(100),
    key_version varchar(50),
    encryption_status varchar(50),
    validation_time datetime2,
    created_at datetime2 DEFAULT GETDATE()
);

-- 監査ログエントリテーブル作成
-- @name: audit_log_entries_table_create
CREATE TABLE audit_log_entries
(
    log_id varchar(50) PRIMARY KEY,
    event_type varchar(100) NOT NULL,
    event_details varchar(max),
    user_identity varchar(200),
    timestamp datetime2,
    severity varchar(20),
    created_at datetime2 DEFAULT GETDATE()
);

-- =========================================================================
-- データ品質・セキュリティテスト用サンプルデータ投入クエリ
-- =========================================================================

-- データ品質ルールサンプル投入
-- @name: data_quality_rules_sample_insert
INSERT INTO data_quality_rules
    (rule_name, table_name, rule_type, column_name, rule_condition, threshold_percent, severity)
VALUES
    ('not_null_check', 'test_table', 'NOT_NULL', 'customer_id', 'customer_id IS NOT NULL', 95.0, 'HIGH'),
    ('email_format_check', 'test_table', 'REGEX', 'email', 'email LIKE ''%@%.%''', 90.0, 'MEDIUM'),
    ('range_check', 'test_table', 'RANGE', 'age', 'age BETWEEN 0 AND 150', 99.0, 'HIGH');

-- データ品質テスト実行結果投入
-- @name: data_quality_test_results_insert
INSERT INTO data_quality_results
    (rule_name, execution_time, total_records, valid_records, invalid_records, quality_score, passed, threshold_met)
VALUES
    ('not_null_check', GETDATE(), 1000, 950, 50, 95.0, 1, 1),
    ('email_format_check', GETDATE(), 1000, 920, 80, 92.0, 1, 1),
    ('range_check', GETDATE(), 1000, 995, 5, 99.5, 1, 1);

-- データ系譜追跡サンプル投入
-- @name: data_lineage_sample_insert
INSERT INTO data_lineage_tracking
    (lineage_id, pipeline_name, activity_name, source_system, source_table, target_system, target_table, transformation_type, records_processed, business_date)
VALUES
    ('LIN001', 'pi_test_pipeline', 'copy_activity', 'source_db', 'customers', 'target_db', 'customers_processed', 'ETL', 10000, GETDATE()),
    ('LIN002', 'pi_test_pipeline', 'lookup_activity', 'ref_db', 'product_codes', 'target_db', 'enriched_data', 'LOOKUP', 5000, GETDATE());

-- セキュリティ監査サンプル投入
-- @name: security_audit_sample_insert
INSERT INTO security_access_audit
    (audit_id, user_identity, resource_accessed, access_type, access_granted, access_time, ip_address)
VALUES
    ('AUD001', 'test_user@company.com', '/data/sensitive_table', 'READ', 1, GETDATE(), '192.168.1.100'),
    ('AUD002', 'test_admin@company.com', '/data/configuration', 'WRITE', 1, GETDATE(), '192.168.1.50');

-- =========================================================================
-- データ品質・セキュリティテスト用クエリ
-- =========================================================================

-- データ品質スコア集計クエリ
-- @name: data_quality_score_summary
SELECT
    rule_name,
    AVG(quality_score) as avg_quality_score,
    COUNT(*) as total_executions,
    SUM(CASE WHEN passed = 1 THEN 1 ELSE 0 END) as passed_count
FROM data_quality_results
WHERE execution_time >= DATEADD(day, -7, GETDATE())
GROUP BY rule_name;

-- データ系譜レポートクエリ
-- @name: data_lineage_report
SELECT
    pipeline_name,
    COUNT(DISTINCT source_table) as source_tables,
    COUNT(DISTINCT target_table) as target_tables,
    SUM(records_processed) as total_records_processed
FROM data_lineage_tracking
WHERE business_date >= DATEADD(day, -30, GETDATE())
GROUP BY pipeline_name;

-- セキュリティアクセス分析クエリ
-- @name: security_access_analysis
SELECT
    user_identity,
    COUNT(*) as access_attempts,
    SUM(CASE WHEN access_granted = 1 THEN 1 ELSE 0 END) as successful_access,
    COUNT(DISTINCT resource_accessed) as unique_resources
FROM security_access_audit
WHERE access_time >= DATEADD(day, -1, GETDATE())
GROUP BY user_identity;

-- テーブルクリーンアップクエリ
-- @name: data_quality_security_cleanup
DELETE FROM data_quality_results WHERE created_at < DATEADD(day, -30, GETDATE());
DELETE FROM data_lineage_tracking WHERE created_at < DATEADD(day, -90, GETDATE());
DELETE FROM security_access_audit WHERE created_at < DATEADD(day, -180, GETDATE());
DELETE FROM security_encryption_audit WHERE created_at < DATEADD(day, -180, GETDATE());
DELETE FROM audit_log_entries WHERE created_at < DATEADD(day, -365, GETDATE());
