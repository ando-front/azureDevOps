-- 08_create_raw_data_source_table.sql
-- ETLパイプライン操作テスト用のraw_data_sourceテーブル作成とデータ投入
-- E2Eテストで使用される重要なテーブル

USE TGMATestDB;
GO

-- =============================================================================
-- raw_data_sourceテーブルの作成（ETLパイプライン操作テスト用）
-- =============================================================================

PRINT 'Creating raw_data_source table for ETL pipeline operations tests...';

-- 既存テーブルのチェックと作成
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[raw_data_source]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[raw_data_source]
    (
        [id] INT NOT NULL PRIMARY KEY,
        [source_type] NVARCHAR(50) NOT NULL,
        [data_json] NVARCHAR(MAX),
        [created_at] DATETIME2 NOT NULL,
        [entity_id] NVARCHAR(50),
        [status] NVARCHAR(20) DEFAULT 'ACTIVE',
        [updated_at] DATETIME2 DEFAULT GETDATE()
    );
    PRINT 'raw_data_source table created successfully';
END
ELSE
BEGIN
    PRINT 'raw_data_source table already exists - clearing existing data';
    DELETE FROM [dbo].[raw_data_source];
END
GO

-- =============================================================================
-- data_watermarksテーブルの作成（増分処理用）
-- =============================================================================

PRINT 'Creating data_watermarks table for incremental processing...';

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[data_watermarks]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[data_watermarks]
    (
        [source_name] NVARCHAR(100) NOT NULL PRIMARY KEY,
        [last_processed_timestamp] DATETIME2 NOT NULL,
        [last_processed_id] INT NOT NULL,
        [processing_status] NVARCHAR(20) DEFAULT 'READY',
        [updated_at] DATETIME2 DEFAULT GETDATE()
    );
    PRINT 'data_watermarks table created successfully';
END
ELSE
BEGIN
    PRINT 'data_watermarks table already exists - clearing existing data';
    DELETE FROM [dbo].[data_watermarks];
END
GO

-- =============================================================================
-- ETLパイプライン操作テスト用のサンプルデータ投入
-- =============================================================================

PRINT 'Inserting sample data for ETL pipeline operations tests...';

-- raw_data_source テーブルへのサンプルデータ投入
INSERT INTO [dbo].[raw_data_source] ([id], [source_type], [data_json], [created_at], [entity_id])
VALUES
    -- 顧客データ
    (1, 'customer', '{"id": 1, "name": "John Doe", "email": "john@example.com", "status": "active"}', '2024-01-01 10:00:00', '1'),
    (2, 'customer', '{"id": 2, "name": "Jane Smith", "email": "jane@example.com", "status": "inactive"}', '2024-01-02 09:00:00', '2'),
    (3, 'customer', '{"id": 3, "name": "Bob Johnson", "email": "bob@example.com", "status": "active"}', '2024-01-03 11:00:00', '3'),
    
    -- 注文データ
    (4, 'order', '{"order_id": 101, "customer_id": 1, "amount": 150.50, "status": "completed"}', '2024-01-01 11:00:00', '101'),
    (5, 'order', '{"order_id": 102, "customer_id": 2, "amount": 75.25, "status": "pending"}', '2024-01-02 10:00:00', '102'),
    (6, 'order', '{"order_id": 103, "customer_id": 3, "amount": 299.99, "status": "completed"}', '2024-01-03 12:00:00', '103'),
    
    -- 商品データ
    (7, 'product', '{"product_id": 201, "name": "Laptop", "category": "Electronics", "price": 999.99}', '2024-01-01 12:00:00', '201'),
    (8, 'product', '{"product_id": 202, "name": "Mouse", "category": "Electronics", "price": 29.99}', '2024-01-02 13:00:00', '202'),
    (9, 'product', '{"product_id": 203, "name": "Keyboard", "category": "Electronics", "price": 59.99}', '2024-01-03 14:00:00', '203'),
    
    -- サービスデータ
    (10, 'service', '{"service_id": 301, "name": "Premium Support", "type": "subscription", "price": 19.99}', '2024-01-01 15:00:00', '301'),
    (11, 'service', '{"service_id": 302, "name": "Basic Support", "type": "one-time", "price": 9.99}', '2024-01-02 16:00:00', '302'),
    
    -- イベントデータ
    (12, 'event', '{"event_id": 401, "type": "login", "user_id": 1, "timestamp": "2024-01-01T10:30:00"}', '2024-01-01 16:00:00', '401'),
    (13, 'event', '{"event_id": 402, "type": "purchase", "user_id": 2, "amount": 150.50}', '2024-01-02 17:00:00', '402'),
    
    -- トランザクションデータ
    (14, 'transaction', '{"transaction_id": 501, "type": "payment", "amount": 150.50, "currency": "JPY"}', '2024-01-01 18:00:00', '501'),
    (15, 'transaction', '{"transaction_id": 502, "type": "refund", "amount": -75.25, "currency": "JPY"}', '2024-01-02 19:00:00', '502'),
    
    -- ログデータ
    (16, 'log', '{"log_id": 601, "level": "INFO", "message": "User logged in", "user_id": 1}', '2024-01-01 20:00:00', '601'),
    (17, 'log', '{"log_id": 602, "level": "ERROR", "message": "Payment failed", "user_id": 2}', '2024-01-02 21:00:00', '602'),
    
    -- 分析データ
    (18, 'analytics', '{"session_id": 701, "page_views": 15, "duration": 1800, "user_id": 1}', '2024-01-01 22:00:00', '701'),
    (19, 'analytics', '{"session_id": 702, "page_views": 8, "duration": 900, "user_id": 2}', '2024-01-02 23:00:00', '702'),
    
    -- 品質テスト用の不正データ
    (20, 'invalid', '{"invalid": "json"', '2024-01-03 10:00:00', NULL),
    (21, 'customer', '{"id": null, "name": "", "email": "invalid-email", "status": "unknown"}', '2024-01-03 11:00:00', NULL);

PRINT 'Inserted ' + CAST(@@ROWCOUNT AS NVARCHAR(10)) + ' records into raw_data_source table';

-- data_watermarks テーブルへの初期データ投入
INSERT INTO [dbo].[data_watermarks] ([source_name], [last_processed_timestamp], [last_processed_id])
VALUES
    ('customer_source', '2024-01-01 08:00:00', 0),
    ('order_source', '2024-01-01 08:00:00', 0),
    ('product_source', '2024-01-01 08:00:00', 0),
    ('service_source', '2024-01-01 08:00:00', 0),
    ('event_source', '2024-01-01 08:00:00', 0),
    ('transaction_source', '2024-01-01 08:00:00', 0),
    ('log_source', '2024-01-01 08:00:00', 0),
    ('analytics_source', '2024-01-01 08:00:00', 0);

PRINT 'Inserted ' + CAST(@@ROWCOUNT AS NVARCHAR(10)) + ' records into data_watermarks table';

-- =============================================================================
-- 検証とサマリー表示
-- =============================================================================

-- データ投入結果の検証
PRINT 'ETL Pipeline Tables Verification:';

SELECT 
    'raw_data_source' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT source_type) as distinct_source_types,
    MIN(created_at) as earliest_record,
    MAX(created_at) as latest_record
FROM [dbo].[raw_data_source]

UNION ALL

SELECT 
    'data_watermarks' as table_name,
    COUNT(*) as total_records,
    COUNT(*) as distinct_source_types,
    MIN(last_processed_timestamp) as earliest_record,
    MAX(last_processed_timestamp) as latest_record
FROM [dbo].[data_watermarks];

-- ソースタイプ別のデータ分布確認
PRINT 'Source Type Distribution:';
SELECT 
    source_type,
    COUNT(*) as record_count,
    MIN(created_at) as earliest,
    MAX(created_at) as latest
FROM [dbo].[raw_data_source]
GROUP BY source_type
ORDER BY record_count DESC;

PRINT '🎉 ETL Pipeline Operations Test Tables created and populated successfully!';
PRINT 'Ready for advanced ETL pipeline operations E2E testing.';
GO
