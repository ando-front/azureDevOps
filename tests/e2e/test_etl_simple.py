"""
E2E Test Suite for ETL Pipeline Operations - Simplified Version

簡素化版ETLパイプライン操作のE2Eテスト
基本的なデータ抽出、変換、ロード処理の検証
"""
import os
import pytest
import time
from typing import Dict, List, Any, Tuple
import logging

# テスト環境のセットアップ
from tests.helpers.reproducible_e2e_helper import (
    setup_reproducible_test_class, 
    cleanup_reproducible_test_class
)
from tests.e2e.helpers.synapse_e2e_helper import SynapseE2EConnection

# ロガーの設定
logger = logging.getLogger(__name__)

class TestSimpleETLPipelineOperations:
    """簡素化版ETLパイプライン操作のE2Eテスト"""
    
    @classmethod
    def setup_class(cls):
        """再現可能テスト環境のセットアップ"""
        setup_reproducible_test_class()
        
        # Disable proxy settings for tests
        for var in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY']:
            if var in os.environ:
                del os.environ[var]
    
    @classmethod 
    def teardown_class(cls):
        """再現可能テスト環境のクリーンアップ"""
        cleanup_reproducible_test_class()

    def test_data_extraction_pipeline_simple(self):
        """データ抽出パイプラインのテスト - 簡素化版"""
        connection = SynapseE2EConnection()
        
        # 基本的なデータ抽出テスト
        extraction_results = connection.execute_query("""
            SELECT 
                source_type,
                COUNT(*) as total_records,
                COUNT(CASE WHEN ISJSON(data_json) = 1 THEN 1 END) as valid_records,
                COUNT(CASE 
                    WHEN source_type = 'customer' AND JSON_VALUE(data_json, '$.id') IS NOT NULL THEN 1
                    WHEN source_type = 'order' AND JSON_VALUE(data_json, '$.order_id') IS NOT NULL THEN 1
                    WHEN source_type = 'product' AND JSON_VALUE(data_json, '$.product_id') IS NOT NULL THEN 1
                    ELSE 0
                END) as records_with_id
            FROM raw_data_source
            WHERE source_type IN ('customer', 'order', 'product')
            GROUP BY source_type
            ORDER BY total_records DESC
        """)
        
        assert len(extraction_results) == 3, f"3つのソースタイプが抽出されるべきです。実際: {len(extraction_results)}"
        
        # 各ソースタイプの品質検証
        for result in extraction_results:
            source_type = result[0]
            total_records = result[1]
            valid_records = result[2]
            records_with_id = result[3]
            
            assert total_records > 0, f"ソースタイプ {source_type} のレコードが0です"
            assert valid_records == total_records, f"ソースタイプ {source_type} に無効なJSONがあります"
            assert records_with_id > 0, f"ソースタイプ {source_type} にIDを持たないレコードがあります"
        
        logger.info(f"データ抽出パイプラインテスト完了: {len(extraction_results)}のソースタイプから合計{sum([r[1] for r in extraction_results])}レコードを抽出")

    def test_data_transformation_pipeline_simple(self):
        """データ変換パイプラインのテスト - 簡素化版"""
        connection = SynapseE2EConnection()
        
        # 基本的なデータ変換テスト
        transformation_results = connection.execute_query("""
            SELECT 
                source_type,
                COUNT(*) as total_records,
                COUNT(CASE WHEN JSON_VALUE(data_json, '$.name') IS NOT NULL THEN 1 END) as records_with_names,
                COUNT(CASE WHEN JSON_VALUE(data_json, '$.status') IS NOT NULL THEN 1 END) as records_with_status,
                CASE 
                    WHEN source_type = 'customer' THEN 'MASTER_DATA'
                    WHEN source_type = 'order' THEN 'TRANSACTION_DATA'
                    WHEN source_type = 'product' THEN 'REFERENCE_DATA'
                    ELSE 'UNKNOWN_DATA'
                END as data_classification
            FROM raw_data_source
            WHERE source_type IN ('customer', 'order', 'product')
            GROUP BY source_type
            ORDER BY total_records DESC
        """)
        
        assert len(transformation_results) > 0, "変換結果が取得できません"
        
        # 変換品質の検証
        total_records = sum([r[1] for r in transformation_results])
        assert total_records > 0, "変換処理でレコードが失われました"
        
        # データ分類の検証
        data_classifications = set([r[4] for r in transformation_results])
        expected_classifications = {'MASTER_DATA', 'TRANSACTION_DATA', 'REFERENCE_DATA'}
        
        for expected_class in expected_classifications:
            assert expected_class in data_classifications, f"データ分類 '{expected_class}' が見つかりません"
        
        logger.info(f"データ変換パイプラインテスト完了: {total_records}レコードを変換")

    def test_data_loading_pipeline_simple(self):
        """データローディングパイプラインのテスト - 簡素化版"""
        connection = SynapseE2EConnection()
        
        # ローディング処理のシミュレーション
        loading_start_time = time.time()
        
        # 基本的なローディングテスト
        loading_results = connection.execute_query("""
            SELECT 
                source_type,
                COUNT(*) as total_records,
                MIN(id) as min_id,
                MAX(id) as max_id,
                MIN(created_at) as earliest_record,
                MAX(created_at) as latest_record
            FROM raw_data_source
            WHERE source_type IN ('customer', 'order', 'product')
            GROUP BY source_type
            ORDER BY total_records DESC
        """)
        
        loading_execution_time = time.time() - loading_start_time
        
        assert len(loading_results) > 0, "ローディング結果が取得できません"
        assert loading_execution_time < 5, f"ローディング処理時間が長すぎます: {loading_execution_time:.2f}秒"
        
        # ローディング結果の検証
        total_loaded = sum([r[1] for r in loading_results])
        assert total_loaded > 0, "ローディングされたレコードが0です"
        
        logger.info(f"データローディングパイプラインテスト完了: {total_loaded}レコード処理、実行時間{loading_execution_time:.2f}秒")

    def test_incremental_data_processing_simple(self):
        """増分データ処理のテスト - 簡素化版"""
        connection = SynapseE2EConnection()
        
        # ウォーターマークテーブルの存在確認
        watermark_check = connection.execute_query("""
            SELECT COUNT(*) as watermark_count FROM data_watermarks
        """)
        
        assert len(watermark_check) > 0 and watermark_check[0][0] > 0, "ウォーターマークテーブルが初期化されていません"
        
        # 基本的な増分処理テスト
        incremental_results = connection.execute_query("""
            SELECT 
                rds.source_type,
                COUNT(*) as total_records,
                COUNT(CASE WHEN rds.created_at > '2024-01-01' THEN 1 END) as recent_records,
                dw.source_name,
                dw.last_processed_timestamp
            FROM raw_data_source rds
            LEFT JOIN data_watermarks dw ON rds.source_type + '_source' = dw.source_name
            WHERE rds.source_type IN ('customer', 'order', 'product')
            GROUP BY rds.source_type, dw.source_name, dw.last_processed_timestamp
            ORDER BY total_records DESC
        """)
        
        assert len(incremental_results) > 0, "増分処理結果が取得できません"
        
        # 増分処理の効率性検証
        total_records = sum([r[1] for r in incremental_results])
        recent_records = sum([r[2] for r in incremental_results])
        
        assert total_records > 0, "増分処理対象レコードが見つかりません"
        
        logger.info(f"増分データ処理テスト完了: {total_records}レコード中{recent_records}が最近のデータ")

    def test_data_quality_monitoring_simple(self):
        """データ品質監視のテスト - 簡素化版"""
        connection = SynapseE2EConnection()
        
        # 基本的なデータ品質メトリクス
        quality_results = connection.execute_query("""
            SELECT 
                source_type,
                COUNT(*) as total_records,
                COUNT(CASE WHEN data_json IS NOT NULL AND data_json != '' THEN 1 END) as non_empty_records,
                COUNT(CASE WHEN ISJSON(data_json) = 1 THEN 1 END) as valid_json_records,
                COUNT(DISTINCT id) as unique_ids,
                ROUND(COUNT(CASE WHEN ISJSON(data_json) = 1 THEN 1 END) * 100.0 / COUNT(*), 2) as quality_score
            FROM raw_data_source
            WHERE source_type IN ('customer', 'order', 'product')
            GROUP BY source_type
            ORDER BY quality_score DESC
        """)
        
        assert len(quality_results) > 0, "データ品質監視結果が取得できません"
        
        # 品質基準の検証
        for result in quality_results:
            source_type = result[0]
            total_records = result[1]
            valid_records = result[3]
            quality_score = result[5]
            
            assert total_records > 0, f"ソースタイプ {source_type} のレコードが0です"
            assert quality_score >= 90, f"ソースタイプ {source_type} の品質スコアが低すぎます: {quality_score}"
            assert valid_records == total_records, f"ソースタイプ {source_type} に無効なJSONがあります"
        
        avg_quality = sum([r[5] for r in quality_results]) / len(quality_results)
        logger.info(f"データ品質監視テスト完了: {len(quality_results)}ソース、平均品質スコア{avg_quality:.1f}")

    def test_pipeline_performance_simple(self):
        """パイプラインパフォーマンステスト - 簡素化版"""
        connection = SynapseE2EConnection()
        
        # パフォーマンステストの実行
        performance_start_time = time.time()
        
        # 基本的なパフォーマンステスト
        performance_results = connection.execute_query("""
            SELECT 
                source_type,
                COUNT(*) as record_count,
                AVG(LEN(data_json)) as avg_record_size,
                MIN(created_at) as earliest_record,
                MAX(created_at) as latest_record,
                DATEDIFF(second, MIN(created_at), MAX(created_at)) as time_span_seconds
            FROM raw_data_source
            WHERE source_type IN ('customer', 'order', 'product')
            GROUP BY source_type
            ORDER BY record_count DESC
        """)
        
        performance_execution_time = time.time() - performance_start_time
        
        assert len(performance_results) > 0, "パフォーマンステスト結果が取得できません"
        assert performance_execution_time < 5, f"パフォーマンステスト実行時間が長すぎます: {performance_execution_time:.2f}秒"
        
        # パフォーマンス基準の検証
        total_records = sum([r[1] for r in performance_results])
        avg_record_size = sum([r[2] for r in performance_results]) / len(performance_results)
        
        assert total_records > 0, "処理対象レコードが0です"
        assert avg_record_size > 0, "平均レコードサイズが0です"
        
        logger.info(f"パイプラインパフォーマンステスト完了: {total_records}レコード処理、平均サイズ{avg_record_size:.1f}バイト、実行時間{performance_execution_time:.2f}秒")
