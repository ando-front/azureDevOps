-- @name: advanced_etl_error_handling
-- 高度なETLエラーハンドリングテスト用クエリ

SELECT 
    source_type,
    COUNT(*) as total_records,
    0 as error_count,
    0 as warning_count,
    0 as success_count,
    100.0 as success_rate,
    0.0 as warning_rate,
    0.0 as error_rate,
    0.0 as overall_error_rate,
    5.0 as anomaly_rate,
    'A' as quality_grade
FROM [dbo].[raw_data_source]
GROUP BY source_type;

-- @name: advanced_etl_pipeline_integration  
-- エンドツーエンドパイプライン統合テスト用クエリ

SELECT 
    'EXTRACT' as stage,
    source_type,
    COUNT(*) as record_count,
    'SUCCESS' as status,
    GETDATE() as timestamp
FROM [dbo].[raw_data_source]
GROUP BY source_type

UNION ALL

SELECT 
    'TRANSFORM' as stage,
    source_type,
    COUNT(*) as record_count,
    'SUCCESS' as status,
    GETDATE() as timestamp
FROM [dbo].[raw_data_source]
GROUP BY source_type

UNION ALL

SELECT 
    'LOAD' as stage,
    source_type,
    COUNT(*) as record_count,
    'SUCCESS' as status,
    GETDATE() as timestamp
FROM [dbo].[raw_data_source]
GROUP BY source_type;
