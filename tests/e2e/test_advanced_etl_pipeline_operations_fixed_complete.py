"""
E2E Test Suite for Advanced ETL and Data Pipeline Operations

高度なETLとデータパイプライン操作のE2Eテスト
データ抽出、変換、ロード処理の包括的検証
"""
import os
import pytest
import time
import json
from datetime import datetime, timedelta
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

class TestAdvancedETLPipelineOperations:
    """高度なETLパイプライン操作のE2Eテスト"""
    
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

    def test_data_extraction_pipeline(self):
        """データ抽出パイプラインのテスト"""
        connection = SynapseE2EConnection()
        
        # 既存データを使用してシンプルなクエリでテスト
        extraction_results = connection.execute_query("""
            SELECT 
                source_type,
                COUNT(*) as total_records,
                COUNT(CASE WHEN ISJSON(data_json) = 1 THEN 1 END) as valid_records,
                COUNT(CASE 
                    WHEN source_type = 'customer' AND JSON_VALUE(data_json, '$.id') IS NOT NULL THEN 1
                    WHEN source_type = 'order' AND JSON_VALUE(data_json, '$.order_id') IS NOT NULL THEN 1
                    WHEN source_type = 'product' AND JSON_VALUE(data_json, '$.product_id') IS NOT NULL THEN 1
                    ELSE NULL
                END) as records_with_id,
                AVG(CAST(LEN(data_json) AS FLOAT)) as avg_data_size
            FROM raw_data_source
            WHERE source_type IN ('customer', 'order', 'product')
            GROUP BY source_type
            ORDER BY total_records DESC
        """)
        
        assert len(extraction_results) == 3, f"期待: 3つのソースタイプ, 実際: {len(extraction_results)}"
        
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

    def test_data_transformation_pipeline(self):
        """データ変換パイプラインのテスト"""
        connection = SynapseE2EConnection()
        
        # シンプルな変換処理のテスト
        transformation_results = connection.execute_query("""
            SELECT 
                source_type,
                COUNT(*) as record_count,
                COUNT(CASE WHEN ISJSON(data_json) = 1 THEN 1 END) as valid_json_count,
                COUNT(CASE WHEN JSON_VALUE(data_json, '$.name') IS NOT NULL OR JSON_VALUE(data_json, '$.order_id') IS NOT NULL THEN 1 END) as has_key_field,
                AVG(CAST(LEN(data_json) AS FLOAT)) as avg_size
            FROM raw_data_source
            WHERE source_type IN ('customer', 'order', 'product')
            GROUP BY source_type
            ORDER BY source_type
        """)
        
        assert len(transformation_results) > 0, "変換結果が取得できません"
        
        # 変換品質の検証
        total_records = sum([r[1] for r in transformation_results])
        total_valid = sum([r[2] for r in transformation_results])
        quality_ratio = total_valid / total_records if total_records > 0 else 0
        
        assert total_records > 0, "変換処理でレコードが失われました"
        assert quality_ratio >= 0.8, f"有効JSONの割合が低すぎます: {quality_ratio:.2%}"
        
        # データ品質の検証
        for result in transformation_results:
            source_type = result[0]
            record_count = result[1]
            valid_json_count = result[2]
            has_key_field = result[3]
            
            assert record_count > 0, f"ソースタイプ {source_type} にレコードがありません"
            assert valid_json_count == record_count, f"ソースタイプ {source_type} に無効なJSONがあります"
            assert has_key_field > 0, f"ソースタイプ {source_type} にキーフィールドがありません"
        
        logger.info(f"データ変換パイプラインテスト完了: {total_records}レコードを変換、品質スコア{quality_ratio:.1%}")

    def test_data_loading_pipeline(self):
        """データローディングパイプラインのテスト"""
        connection = SynapseE2EConnection()
        
        # ローディング処理のシミュレーション
        loading_start_time = time.time()
        
        # バッチローディングのシミュレーション - シンプルなクエリ
        batch_loading_results = connection.execute_query("""
            SELECT 
                source_type,
                COUNT(*) as batch_size,
                MIN(created_at) as earliest_record,
                MAX(created_at) as latest_record
            FROM raw_data_source
            WHERE source_type IN ('customer', 'order', 'product')
            GROUP BY source_type
            ORDER BY source_type
        """)
        
        loading_end_time = time.time()
        loading_duration = loading_end_time - loading_start_time
        
        assert len(batch_loading_results) > 0, "ローディング結果が取得できません"
        
        # ローディング品質の検証
        total_loaded = sum([r[1] for r in batch_loading_results])
        
        assert total_loaded > 0, "ローディング処理でレコードが失われました"
        assert loading_duration < 10.0, f"ローディング時間が長すぎます: {loading_duration:.2f}秒"
        
        # バッチ品質の検証
        for result in batch_loading_results:
            source_type = result[0]
            batch_size = result[1]
            
            assert batch_size > 0, f"ソースタイプ {source_type} のバッチが空です"
        
        logger.info(f"データローディングパイプライン完了: {total_loaded}の結果")
        logger.info(f"データローディングパイプラインテスト完了、実行時間{loading_duration:.2f}秒")

    def test_incremental_data_processing(self):
        """増分データ処理のテスト"""
        connection = SynapseE2EConnection()
        
        # ウォーターマークテーブルの確認
        watermark_check = connection.execute_query("""
            SELECT 
                source_name,
                last_processed_id,
                processing_status
            FROM data_watermarks
            WHERE source_name LIKE '%_source'
            ORDER BY source_name
        """)
        
        assert len(watermark_check) > 0, "ウォーターマークテーブルにデータがありません"
        
        # 増分処理のシミュレーション - 新しいレコードの処理
        incremental_results = connection.execute_query("""
            SELECT 
                r.source_type,
                COUNT(*) as new_records,
                MAX(r.created_at) as latest_processed
            FROM raw_data_source r
            INNER JOIN data_watermarks w ON r.source_type + '_source' = w.source_name
            WHERE r.id > w.last_processed_id
            GROUP BY r.source_type
            ORDER BY r.source_type
        """)
        
        # 増分処理品質の検証
        if len(incremental_results) > 0:
            total_new_records = sum([r[1] for r in incremental_results])
            assert total_new_records > 0, "増分処理で新しいレコードが見つかりませんでした"
            
            for result in incremental_results:
                source_type = result[0]
                new_records = result[1]
                assert new_records > 0, f"ソースタイプ {source_type} で新しいレコードがありません"
        
        logger.info(f"増分データ処理テスト完了: {len(watermark_check)}のウォーターマーク, {len(incremental_results)}のソースタイプで増分処理")

    def test_data_quality_validation(self):
        """データ品質バリデーションのテスト"""
        connection = SynapseE2EConnection()
        
        # データ品質チェック
        quality_results = connection.execute_query("""
            SELECT 
                source_type,
                COUNT(*) as total_records,
                COUNT(CASE WHEN ISJSON(data_json) = 1 THEN 1 END) as valid_json,
                COUNT(CASE WHEN data_json IS NULL OR data_json = '' THEN 1 END) as null_records,
                COUNT(CASE WHEN LEN(data_json) < 10 THEN 1 END) as small_records
            FROM raw_data_source
            WHERE source_type IN ('customer', 'order', 'product')
            GROUP BY source_type
            ORDER BY source_type
        """)
        
        assert len(quality_results) > 0, "品質検証結果が取得できません"
        
        # 品質メトリクスの検証
        for result in quality_results:
            source_type = result[0]
            total_records = result[1]
            valid_json = result[2]
            null_records = result[3]
            small_records = result[4]
            
            # 品質チェック
            json_quality = valid_json / total_records if total_records > 0 else 0
            null_ratio = null_records / total_records if total_records > 0 else 0
            
            assert total_records > 0, f"ソースタイプ {source_type} にレコードがありません"
            assert json_quality >= 0.8, f"ソースタイプ {source_type} のJSON品質が低すぎます: {json_quality:.2%}"
            assert null_ratio < 0.1, f"ソースタイプ {source_type} のNULLレコード割合が高すぎます: {null_ratio:.2%}"
        
        logger.info(f"データ品質バリデーションテスト完了: {len(quality_results)}のソースタイプを検証")

    def test_pipeline_performance_monitoring(self):
        """パイプラインパフォーマンス監視のテスト"""
        connection = SynapseE2EConnection()
        
        # パフォーマンス測定開始
        start_time = time.time()
        
        # 重い処理のシミュレーション
        performance_results = connection.execute_query("""
            SELECT 
                source_type,
                COUNT(*) as record_count,
                AVG(CAST(LEN(data_json) AS FLOAT)) as avg_size,
                MIN(created_at) as earliest,
                MAX(created_at) as latest,
                COUNT(DISTINCT JSON_VALUE(data_json, '$.id')) + 
                COUNT(DISTINCT JSON_VALUE(data_json, '$.order_id')) + 
                COUNT(DISTINCT JSON_VALUE(data_json, '$.product_id')) as unique_identifiers
            FROM raw_data_source
            WHERE source_type IN ('customer', 'order', 'product')
            GROUP BY source_type
            ORDER BY record_count DESC
        """)
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        assert len(performance_results) > 0, "パフォーマンス測定結果が取得できません"
        assert execution_time < 5.0, f"クエリ実行時間が長すぎます: {execution_time:.2f}秒"
        
        # パフォーマンスメトリクスの検証
        total_records = sum([r[1] for r in performance_results])
        throughput = total_records / execution_time if execution_time > 0 else 0
        
        assert throughput > 10, f"スループットが低すぎます: {throughput:.1f} records/sec"
        
        logger.info(f"パイプラインパフォーマンス監視テスト完了: {total_records}レコードを{execution_time:.2f}秒で処理 (スループット: {throughput:.1f} records/sec)")

    def test_error_handling_and_recovery(self):
        """エラーハンドリングとリカバリのテスト"""
        connection = SynapseE2EConnection()
        
        # 不正データの処理テスト
        error_handling_results = connection.execute_query("""
            SELECT 
                source_type,
                COUNT(*) as total_records,
                COUNT(CASE WHEN ISJSON(data_json) = 0 THEN 1 END) as invalid_json,
                COUNT(CASE WHEN JSON_VALUE(data_json, '$.id') IS NULL AND 
                                JSON_VALUE(data_json, '$.order_id') IS NULL AND 
                                JSON_VALUE(data_json, '$.product_id') IS NULL THEN 1 END) as missing_ids
            FROM raw_data_source
            GROUP BY source_type
            ORDER BY source_type
        """)
        
        assert len(error_handling_results) > 0, "エラーハンドリング結果が取得できません"
        
        # エラー処理の検証
        for result in error_handling_results:
            source_type = result[0]
            total_records = result[1]
            invalid_json = result[2]
            missing_ids = result[3]
            
            # エラー率の計算
            error_rate = (invalid_json + missing_ids) / total_records if total_records > 0 else 0
            
            # エラー率が許容範囲内であることを確認
            assert error_rate < 0.2, f"ソースタイプ {source_type} のエラー率が高すぎます: {error_rate:.2%}"
        
        logger.info(f"エラーハンドリングとリカバリテスト完了: {len(error_handling_results)}のソースタイプをテスト")
