-- Client DM Backup Pipeline E2E Test Queries
-- テスト対象: pi_Send_ClientDM パイプライン

-- Query: create_client_dm_test_table
-- Description: Client DMテスト用テーブル作成
CREATE TABLE
IF NOT EXISTS client_dm_test_table
(
    client_id NVARCHAR
(50) PRIMARY KEY,
    client_name NVARCHAR
(255) NOT NULL,
    email NVARCHAR
(255) NOT NULL,
    registration_date DATE,
    status NVARCHAR
(50),
    segment NVARCHAR
(50),
    created_at DATETIME2 DEFAULT GETDATE
(),
    updated_at DATETIME2 DEFAULT GETDATE
()
);

-- Query: insert_client_dm_test_data
-- Description: Client DMテストデータ投入
INSERT INTO client_dm_test_table
    (
    client_id, client_name, email, registration_date, status, segment
    )
VALUES
    (
        @client_id, @client_name, @email, @registration_date, @status, @segment
);

-- Query: cleanup_client_dm_test_data
-- Description: Client DMテストデータクリーンアップ
DELETE FROM client_dm_test_table;
DROP TABLE IF EXISTS client_dm_test_table;

-- Query: create_client_dm_large_test_table
-- Description: 大容量データテスト用テーブル作成
CREATE TABLE
IF NOT EXISTS client_dm_large_test_table
(
    client_id NVARCHAR
(50) PRIMARY KEY,
    client_name NVARCHAR
(255) NOT NULL,
    email NVARCHAR
(255) NOT NULL,
    registration_date DATE,
    status NVARCHAR
(50),
    segment NVARCHAR
(50),
    created_at DATETIME2 DEFAULT GETDATE
(),
    updated_at DATETIME2 DEFAULT GETDATE
(),
    INDEX idx_client_dm_large_status
(status),
    INDEX idx_client_dm_large_segment
(segment)
);

-- Query: insert_client_dm_bulk_data
-- Description: 大容量データ投入用SQL
INSERT INTO client_dm_large_test_table
    (
    client_id, client_name, email, registration_date, status, segment
    )
VALUES
    (
        @client_id, @client_name, @email, @registration_date, @status, @segment
);

-- Query: cleanup_client_dm_large_test_data
-- Description: 大容量テストデータクリーンアップ
DELETE FROM client_dm_large_test_table;
DROP TABLE IF EXISTS client_dm_large_test_table;

-- Query: create_client_dm_quality_test_table
-- Description: データ品質テスト用テーブル作成
CREATE TABLE
IF NOT EXISTS client_dm_quality_test_table
(
    client_id NVARCHAR
(50) PRIMARY KEY,
    client_name NVARCHAR
(255),
    email NVARCHAR
(255),
    registration_date DATE,
    status NVARCHAR
(50),
    segment NVARCHAR
(50),
    created_at DATETIME2 DEFAULT GETDATE
(),
    updated_at DATETIME2 DEFAULT GETDATE
(),
    CONSTRAINT chk_client_dm_quality_email CHECK
(email LIKE '%@%')
);

-- Query: insert_client_dm_quality_data
-- Description: データ品質テスト用データ投入
INSERT INTO client_dm_quality_test_table
    (
    client_id, client_name, email, registration_date, status, segment
    )
VALUES
    (
        @client_id, @client_name, @email, @registration_date, @status, @segment
);

-- Query: cleanup_client_dm_quality_test_data
-- Description: データ品質テストデータクリーンアップ
DELETE FROM client_dm_quality_test_table;
DROP TABLE IF EXISTS client_dm_quality_test_table;

-- Query: get_client_dm_data_quality_metrics
-- Description: データ品質メトリクス取得
SELECT
    COUNT(*) as total_records,
    COUNT(CASE WHEN client_name IS NULL OR client_name = '' THEN 1 END) as empty_names,
    COUNT(CASE WHEN email IS NULL OR email = '' THEN 1 END) as empty_emails,
    COUNT(CASE WHEN email NOT LIKE '%@%' THEN 1 END) as invalid_emails,
    COUNT(CASE WHEN status NOT IN ('active', 'inactive') THEN 1 END) as invalid_status,
    COUNT(DISTINCT client_id) as unique_clients
FROM client_dm_quality_test_table;

-- Query: get_client_dm_performance_data
-- Description: 性能テスト用データ取得
SELECT
    client_id,
    client_name,
    email,
    registration_date,
    status,
    segment,
    created_at
FROM client_dm_large_test_table
ORDER BY created_at
OFFSET @offset ROWS
FETCH NEXT @limit ROWS ONLY;
