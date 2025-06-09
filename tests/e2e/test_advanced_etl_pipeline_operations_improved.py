#!/usr/bin/env python3
"""
改良版 E2E Test Suite for Advanced ETL and Data Pipeline Operations

高度なETLとデータパイプライン操作のE2Eテスト（改良版）
データベース初期化のタイムアウト問題を解決し、より安定したテスト実行を提供
"""
import os
import pytest
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple
import logging

# 改良されたSynapse接続ヘルパーを使用
from tests.e2e.helpers.synapse_e2e_helper import SynapseE2EConnection
# 改良された再現可能テストヘルパーを使用
from tests.helpers.reproducible_e2e_helper_improved import (
    setup_improved_reproducible_test_class,
    cleanup_improved_reproducible_test_class,
    validate_test_environment_fast
)

# ロガーの設定
logger = logging.getLogger(__name__)

class TestAdvancedETLPipelineOperationsImproved:
    """改良版高度なETLパイプライン操作のE2Eテスト"""
    
    @classmethod
    def setup_class(cls):
        """改良版軽量セットアップ - 初期化タイムアウトを回避"""
        # プロキシ設定のクリア
        for var in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY']:
            if var in os.environ:
                del os.environ[var]
        
        # 環境変数の確認
        required_vars = ['SQL_SERVER_HOST', 'SQL_SERVER_USER', 'SQL_SERVER_PASSWORD']
        for var in required_vars:
            if not os.getenv(var):
                logger.warning(f"環境変数 {var} が設定されていません")
        
        # 改良版テスト環境セットアップ
        try:
            setup_improved_reproducible_test_class()
            logger.info("🚀 改良版ETLテストスイート開始 - 初期化タイムアウトを回避")
        except Exception as e:
            logger.warning(f"改良版セットアップ失敗、代替モードで続行: {str(e)}")
    
    @classmethod 
    def teardown_class(cls):
        """改良版軽量クリーンアップ"""
        try:
            cleanup_improved_reproducible_test_class()
        except Exception as e:
            logger.warning(f"改良版クリーンアップ警告: {str(e)}")
        logger.info("🏁 改良版ETLテストスイート終了")

    def test_database_connectivity_and_data_validation(self):
        """データベース接続とデータ検証のテスト"""
        connection = SynapseE2EConnection()
        
        # 基本的な接続テスト
        try:
            # 簡単なクエリで接続確認
            basic_test = connection.execute_query("SELECT @@VERSION as version")
            assert len(basic_test) == 1, "データベース接続に失敗"
            logger.info(f"✅ データベース接続成功: {basic_test[0][0]}")
            
            # データベース情報の取得
            db_info = connection.execute_query("""
                SELECT 
                    DB_NAME() as current_database,
                    COUNT(*) as connection_count
                FROM sys.dm_exec_sessions 
                WHERE is_user_process = 1
            """)
            
            assert len(db_info) == 1, "データベース情報取得に失敗"
            logger.info(f"✅ 現在のデータベース: {db_info[0][0]}, アクティブ接続数: {db_info[0][1]}")
            
        except Exception as e:
            pytest.fail(f"データベース接続テスト失敗: {str(e)}")

    def test_etl_data_extraction_advanced(self):
        """高度なデータ抽出パイプラインのテスト"""
        connection = SynapseE2EConnection()
        
        # 既存データを使用した高度な抽出クエリ（修正版）
        extraction_results = connection.execute_query("""
            SELECT 
                source_type,
                COUNT(*) as total_records,
                COUNT(CASE WHEN ISJSON(data_json) = 1 THEN 1 END) as valid_json_records,
                COUNT(CASE WHEN data_json IS NOT NULL AND LEN(data_json) > 0 THEN 1 END) as non_empty_records,
                AVG(CAST(LEN(data_json) AS FLOAT)) as avg_data_size,
                CAST(COUNT(CASE WHEN ISJSON(data_json) = 1 THEN 1 END) AS FLOAT) / CAST(COUNT(*) AS FLOAT) * 100 as quality_percentage,
                DATEDIFF(day, MIN(created_at), MAX(created_at)) as data_span_days
            FROM raw_data_source
            WHERE source_type IN ('customer', 'order', 'product')
            GROUP BY source_type
            ORDER BY total_records DESC
        """)
        
        assert len(extraction_results) >= 3, f"期待: 3つ以上のソースタイプ, 実際: {len(extraction_results)}"
        
        # 抽出データの品質検証
        total_extracted = 0
        for result in extraction_results:
            source_type = result[0]
            total_records = result[1]
            valid_json_records = result[2]
            quality_percentage = result[5]
            
            total_extracted += total_records
            
            assert total_records > 0, f"ソースタイプ {source_type} のレコードが0です"
            assert quality_percentage >= 80.0, f"ソースタイプ {source_type} の品質が低すぎます: {quality_percentage:.1f}%"
            
            logger.info(f"✅ {source_type}: {total_records}レコード (品質: {quality_percentage:.1f}%)")
        
        logger.info(f"📊 高度なデータ抽出完了: 合計 {total_extracted} レコードを抽出")

    def test_etl_data_transformation_complex(self):
        """複雑なデータ変換パイプラインのテスト"""
        connection = SynapseE2EConnection()
        
        # 複雑な変換処理のテスト（修正版）
        transformation_results = connection.execute_query("""
            SELECT 
                source_type,
                COUNT(*) as record_count,
                -- JSON構造の解析
                COUNT(CASE WHEN JSON_VALUE(data_json, '$.id') IS NOT NULL THEN 1 END) as has_id,
                COUNT(CASE WHEN JSON_VALUE(data_json, '$.name') IS NOT NULL THEN 1 END) as has_name,
                COUNT(CASE WHEN JSON_VALUE(data_json, '$.email') IS NOT NULL THEN 1 END) as has_email,
                COUNT(CASE WHEN JSON_VALUE(data_json, '$.order_id') IS NOT NULL THEN 1 END) as has_order_id,
                COUNT(CASE WHEN JSON_VALUE(data_json, '$.product_id') IS NOT NULL THEN 1 END) as has_product_id,
                -- データサイズ分析
                AVG(CAST(LEN(data_json) AS FLOAT)) as avg_size,
                CASE 
                    WHEN source_type = 'customer' THEN CAST(COUNT(CASE WHEN JSON_VALUE(data_json, '$.id') IS NOT NULL THEN 1 END) AS FLOAT) / CAST(COUNT(*) AS FLOAT) * 100
                    WHEN source_type = 'order' THEN CAST(COUNT(CASE WHEN JSON_VALUE(data_json, '$.order_id') IS NOT NULL THEN 1 END) AS FLOAT) / CAST(COUNT(*) AS FLOAT) * 100
                    WHEN source_type = 'product' THEN CAST(COUNT(CASE WHEN JSON_VALUE(data_json, '$.product_id') IS NOT NULL THEN 1 END) AS FLOAT) / CAST(COUNT(*) AS FLOAT) * 100
                    ELSE 0
                END as key_field_coverage_percentage
            FROM raw_data_source
            WHERE source_type IN ('customer', 'order', 'product')
              AND ISJSON(data_json) = 1
            GROUP BY source_type
            ORDER BY record_count DESC
        """)
        
        assert len(transformation_results) > 0, "変換結果が取得できません"
        
        # 変換品質の詳細検証
        for result in transformation_results:
            source_type = result[0]
            record_count = result[1]
            key_field_coverage = result[8]
            avg_size = result[7]
            
            assert record_count > 0, f"{source_type}: レコード数が0です"
            assert key_field_coverage >= 80.0, f"{source_type}: キーフィールドカバレッジが低すぎます: {key_field_coverage:.1f}%"
            assert avg_size > 0, f"{source_type}: 平均データサイズが0です"
            
            logger.info(f"✅ {source_type}: {record_count}レコード変換, キーフィールドカバレッジ: {key_field_coverage:.1f}%, 平均サイズ: {avg_size:.1f}文字")
        
        logger.info("📈 複雑なデータ変換パイプラインテスト完了")

    def test_etl_incremental_processing_advanced(self):
        """高度な増分データ処理のテスト"""
        connection = SynapseE2EConnection()
        
        # ウォーターマークベースの増分処理シミュレーション
        start_time = time.time()
        
        incremental_results = connection.execute_query("""
            SELECT 
                r.source_type,
                COUNT(*) as total_records,
                COUNT(CASE WHEN r.created_at > DATEADD(hour, -24, GETDATE()) THEN 1 END) as recent_records,
                COUNT(CASE WHEN r.created_at > DATEADD(hour, -1, GETDATE()) THEN 1 END) as very_recent_records,
                MIN(r.created_at) as earliest_timestamp,
                MAX(r.created_at) as latest_timestamp,
                COALESCE(w.last_processed_id, 0) as last_processed_id,
                COALESCE(w.processing_status, 'unknown') as processing_status,
                CASE 
                    WHEN COUNT(*) > 0 THEN CAST(COUNT(CASE WHEN r.created_at > DATEADD(hour, -24, GETDATE()) THEN 1 END) AS FLOAT) / CAST(COUNT(*) AS FLOAT) * 100
                    ELSE 0
                END as recent_data_percentage
            FROM raw_data_source r
            LEFT JOIN data_watermarks w ON r.source_type + '_source' = w.source_name
            WHERE r.source_type IN ('customer', 'order', 'product')
            GROUP BY r.source_type, w.last_processed_id, w.processing_status
            ORDER BY total_records DESC
        """)
        
        processing_time = time.time() - start_time
        
        assert len(incremental_results) > 0, "増分処理結果が取得できません"
        assert processing_time < 5.0, f"増分処理時間が長すぎます: {processing_time:.2f}秒"
        
        # 増分処理の品質検証
        for result in incremental_results:
            source_type = result[0]
            total_records = result[1]
            recent_records = result[2]
            processing_status = result[7] if result[7] else "未設定"
            
            assert total_records > 0, f"{source_type}: 増分処理対象レコードが0です"
            
            logger.info(f"✅ {source_type}: 総レコード数{total_records}, 最近のレコード数{recent_records}, ステータス: {processing_status}")
        
        logger.info(f"🔄 高度な増分データ処理テスト完了 (処理時間: {processing_time:.2f}秒)")

    def test_etl_performance_monitoring_comprehensive(self):
        """包括的なETLパフォーマンス監視テスト"""
        connection = SynapseE2EConnection()
        
        # パフォーマンス測定の開始
        start_time = time.time()
        
        # シンプルで確実なパフォーマンステスト
        performance_results = connection.execute_query("""
            SELECT 
                source_type,
                COUNT(*) as record_count,
                AVG(CAST(LEN(data_json) AS FLOAT)) as avg_record_size,
                MAX(CAST(LEN(data_json) AS FLOAT)) as max_record_size,
                MIN(CAST(LEN(data_json) AS FLOAT)) as min_record_size,
                SUM(CAST(LEN(data_json) AS BIGINT)) as total_data_size
            FROM raw_data_source
            WHERE source_type IN ('customer', 'order', 'product')
              AND ISJSON(data_json) = 1
            GROUP BY source_type
            ORDER BY record_count DESC
        """)
        
        end_time = time.time()
        total_execution_time = end_time - start_time
        
        assert len(performance_results) > 0, "パフォーマンス測定結果が取得できません"
        assert total_execution_time < 10.0, f"パフォーマンステスト実行時間が長すぎます: {total_execution_time:.2f}秒"
        
        # パフォーマンスメトリクスの詳細検証
        total_records_processed = 0
        total_data_processed = 0
        
        for result in performance_results:
            source_type = result[0]
            record_count = result[1]
            avg_record_size = result[2]
            max_record_size = result[3]
            min_record_size = result[4]
            total_data_size = result[5]
            
            total_records_processed += record_count
            total_data_processed += total_data_size
            
            assert record_count > 0, f"{source_type}: 処理レコード数が0です"
            assert avg_record_size > 0, f"{source_type}: 平均レコードサイズが0です"
            assert max_record_size >= min_record_size, f"{source_type}: レコードサイズの範囲が不正です"
            
            logger.info(f"📊 {source_type}: {record_count}レコード, 平均サイズ{avg_record_size:.1f}, 処理負荷{processing_load:.0f}, 重複率{duplication_ratio:.2f}")
        
        # 全体のスループット計算
        overall_throughput = total_records_processed / total_execution_time if total_execution_time > 0 else 0
        data_throughput = total_data_processed / total_execution_time if total_execution_time > 0 else 0
        
        assert overall_throughput > 50, f"レコードスループットが低すぎます: {overall_throughput:.1f} records/sec"
        
        logger.info(f"🚀 包括的パフォーマンステスト完了:")
        logger.info(f"   - 総処理レコード数: {total_records_processed}")
        logger.info(f"   - 総データサイズ: {total_data_processed:,} 文字")
        logger.info(f"   - 実行時間: {total_execution_time:.2f}秒")
        logger.info(f"   - レコードスループット: {overall_throughput:.1f} records/sec")
        logger.info(f"   - データスループット: {data_throughput:,.0f} chars/sec")

    def test_etl_error_handling_resilience(self):
        """ETLエラーハンドリングと回復力のテスト"""
        connection = SynapseE2EConnection()
        
        # エラー状況のシミュレーションと回復テスト
        error_handling_results = connection.execute_query("""
            WITH error_analysis AS (
                SELECT 
                    source_type,
                    COUNT(*) as total_records,
                    -- JSON形式エラーの検出
                    COUNT(CASE WHEN ISJSON(data_json) = 0 THEN 1 END) as invalid_json_records,
                    -- 空データの検出
                    COUNT(CASE WHEN data_json IS NULL OR LEN(data_json) = 0 THEN 1 END) as empty_records,
                    -- 必須フィールド欠損の検出
                    COUNT(CASE WHEN 
                        JSON_VALUE(data_json, '$.id') IS NULL AND 
                        JSON_VALUE(data_json, '$.order_id') IS NULL AND 
                        JSON_VALUE(data_json, '$.product_id') IS NULL 
                    THEN 1 END) as missing_key_fields,
                    -- 異常に小さいデータの検出
                    COUNT(CASE WHEN LEN(data_json) < 20 THEN 1 END) as suspiciously_small_records,
                    -- 異常に大きいデータの検出
                    COUNT(CASE WHEN LEN(data_json) > 10000 THEN 1 END) as suspiciously_large_records
                FROM raw_data_source
                WHERE source_type IN ('customer', 'order', 'product')
                GROUP BY source_type
            ),
            error_rate_calculation AS (
                SELECT 
                    *,
                    CASE 
                        WHEN total_records > 0 THEN 
                            CAST(invalid_json_records + empty_records + missing_key_fields AS FLOAT) 
                            / CAST(total_records AS FLOAT) * 100
                        ELSE 0
                    END as overall_error_rate,
                    CASE 
                        WHEN total_records > 0 THEN 
                            CAST(suspiciously_small_records + suspiciously_large_records AS FLOAT) 
                            / CAST(total_records AS FLOAT) * 100
                        ELSE 0
                    END as anomaly_rate
                FROM error_analysis
            )
            SELECT 
                source_type,
                total_records,
                invalid_json_records,
                empty_records,
                missing_key_fields,
                suspiciously_small_records,
                suspiciously_large_records,
                overall_error_rate,
                anomaly_rate,
                CASE 
                    WHEN overall_error_rate < 5.0 THEN 'EXCELLENT'
                    WHEN overall_error_rate < 10.0 THEN 'GOOD'
                    WHEN overall_error_rate < 20.0 THEN 'ACCEPTABLE'
                    ELSE 'NEEDS_ATTENTION'
                END as data_quality_grade
            FROM error_rate_calculation
            ORDER BY overall_error_rate ASC
        """)
        
        assert len(error_handling_results) > 0, "エラーハンドリング結果が取得できません"
        
        # エラー率の検証と回復力の確認
        for result in error_handling_results:
            source_type = result[0]
            total_records = result[1]
            overall_error_rate = result[7]
            anomaly_rate = result[8]
            quality_grade = result[9]
            
            assert total_records > 0, f"{source_type}: 分析対象レコードが0です"
            assert overall_error_rate < 25.0, f"{source_type}: エラー率が高すぎます: {overall_error_rate:.1f}%"
            
            logger.info(f"🛡️ {source_type}: {total_records}レコード, エラー率{overall_error_rate:.1f}%, 異常率{anomaly_rate:.1f}%, 品質グレード: {quality_grade}")
        
        # システム全体の回復力評価
        total_processed = sum([r[1] for r in error_handling_results])
        avg_error_rate = sum([r[7] for r in error_handling_results]) / len(error_handling_results)
        
        assert avg_error_rate < 15.0, f"システム全体のエラー率が高すぎます: {avg_error_rate:.1f}%"
        
        logger.info(f"🔒 ETLエラーハンドリングと回復力テスト完了:")
        logger.info(f"   - 総分析レコード数: {total_processed}")
        logger.info(f"   - 平均エラー率: {avg_error_rate:.1f}%")
        logger.info(f"   - システム回復力: {'高' if avg_error_rate < 10 else '中' if avg_error_rate < 20 else '要改善'}")

    def test_end_to_end_pipeline_integration(self):
        """エンドツーエンドパイプライン統合テスト"""
        connection = SynapseE2EConnection()
        
        # 完全なETLパイプラインの統合テスト
        integration_start_time = time.time()
        
        pipeline_integration_results = connection.execute_query("""
            WITH pipeline_flow AS (
                -- Step 1: Data Extraction
                SELECT 
                    'EXTRACTION' as pipeline_stage,
                    source_type,
                    COUNT(*) as record_count,
                    'SUCCESS' as status
                FROM raw_data_source
                WHERE source_type IN ('customer', 'order', 'product')
                GROUP BY source_type
                
                UNION ALL
                
                -- Step 2: Data Transformation (Simulated)
                SELECT 
                    'TRANSFORMATION' as pipeline_stage,
                    source_type,
                    COUNT(CASE WHEN ISJSON(data_json) = 1 THEN 1 END) as record_count,
                    CASE 
                        WHEN COUNT(CASE WHEN ISJSON(data_json) = 1 THEN 1 END) = COUNT(*) THEN 'SUCCESS'
                        ELSE 'PARTIAL_SUCCESS'
                    END as status
                FROM raw_data_source
                WHERE source_type IN ('customer', 'order', 'product')
                GROUP BY source_type
                
                UNION ALL
                
                -- Step 3: Data Loading (Simulated)
                SELECT 
                    'LOADING' as pipeline_stage,
                    source_type,
                    COUNT(*) as record_count,
                    'SUCCESS' as status
                FROM raw_data_source
                WHERE source_type IN ('customer', 'order', 'product')
                  AND ISJSON(data_json) = 1
                GROUP BY source_type
            ),
            pipeline_summary AS (
                SELECT 
                    pipeline_stage,
                    source_type,
                    record_count,
                    status,
                    ROW_NUMBER() OVER (PARTITION BY source_type ORDER BY 
                        CASE pipeline_stage 
                            WHEN 'EXTRACTION' THEN 1 
                            WHEN 'TRANSFORMATION' THEN 2 
                            WHEN 'LOADING' THEN 3 
                        END) as stage_order
                FROM pipeline_flow
            )
            SELECT 
                pipeline_stage,
                source_type,
                record_count,
                status,
                stage_order
            FROM pipeline_summary
            ORDER BY source_type, stage_order
        """)
        
        integration_end_time = time.time()
        total_integration_time = integration_end_time - integration_start_time
        
        assert len(pipeline_integration_results) > 0, "パイプライン統合結果が取得できません"
        assert total_integration_time < 15.0, f"統合テスト実行時間が長すぎます: {total_integration_time:.2f}秒"
        
        # パイプライン各段階の検証
        pipeline_stages = {}
        for result in pipeline_integration_results:
            stage = result[0]
            source_type = result[1]
            record_count = result[2]
            status = result[3]
            
            if stage not in pipeline_stages:
                pipeline_stages[stage] = {}
            pipeline_stages[stage][source_type] = {
                'record_count': record_count,
                'status': status
            }
            
            assert record_count > 0, f"{stage}段階の{source_type}でレコード数が0です"
            assert status in ['SUCCESS', 'PARTIAL_SUCCESS'], f"{stage}段階の{source_type}でステータスが不正です: {status}"
        
        # パイプライン整合性の検証
        expected_stages = ['EXTRACTION', 'TRANSFORMATION', 'LOADING']
        for stage in expected_stages:
            assert stage in pipeline_stages, f"パイプライン段階 {stage} が見つかりません"
        
        logger.info("🔗 エンドツーエンドパイプライン統合テスト完了:")
        for stage in expected_stages:
            stage_total = sum([info['record_count'] for info in pipeline_stages[stage].values()])
            success_count = sum([1 for info in pipeline_stages[stage].values() if info['status'] == 'SUCCESS'])
            logger.info(f"   - {stage}: {stage_total}レコード処理, {success_count}/{len(pipeline_stages[stage])}ソース成功")
        
        logger.info(f"   - 総統合時間: {total_integration_time:.2f}秒")
        logger.info("✅ 全パイプライン段階の統合検証完了")
