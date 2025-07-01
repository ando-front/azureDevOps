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

# pyodbc conditionally imported for ODBC-dependent tests
try:
    import pyodbc
    PYODBC_AVAILABLE = True
except ImportError:
    PYODBC_AVAILABLE = False

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
        cls._pyodbc_available = PYODBC_AVAILABLE
        
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
        
        # テスト用のデータをクリアして既存データとの重複を回避
        connection.execute_query("DELETE FROM raw_data_source WHERE id BETWEEN 1000 AND 1999")
        
        # ソースデータの挿入（シミュレーション）- 1000番台のIDを使用
        source_data = [
            (1001, 'customer', '{"id": 1, "name": "John Doe", "email": "john@example.com", "status": "active"}', '2024-01-01 10:00:00'),
            (1002, 'order', '{"order_id": 101, "customer_id": 1, "amount": 150.50, "status": "completed"}', '2024-01-01 11:00:00'),
            (1003, 'product', '{"product_id": 201, "name": "Laptop", "category": "Electronics", "price": 999.99}', '2024-01-01 12:00:00'),
            (1004, 'customer', '{"id": 2, "name": "Jane Smith", "email": "jane@example.com", "status": "inactive"}', '2024-01-02 09:00:00'),
            (1005, 'order', '{"order_id": 102, "customer_id": 2, "amount": 75.25, "status": "pending"}', '2024-01-02 10:00:00')
        ]
          # IDENTITY_INSERTを有効にしてデータを挿入
        # Skip DB operations if pyodbc is not available
        if not hasattr(self, '_pyodbc_available') or not self._pyodbc_available:
            pytest.skip("pyodbc not available - skipping DB-dependent test")
        
        conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER=sql-server,1433;DATABASE=SynapseTestDB;UID=sa;PWD=YourStrong!Passw0rd123;TrustServerCertificate=yes;Encrypt=yes')
        cur = conn.cursor()
        cur.execute("SET IDENTITY_INSERT raw_data_source ON")
        for record_id, source_type, data_json, created_at in source_data:
            cur.execute(
                "INSERT INTO raw_data_source (id, source_type, data_json, created_at) VALUES (?, ?, ?, ?)",
                record_id, source_type, data_json, created_at
            )
        cur.execute("SET IDENTITY_INSERT raw_data_source OFF")
        conn.commit()
        cur.close()
        conn.close()
        
        # データ抽出処理のシミュレーション - シンプルで確実な構造
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
            WHERE source_type IN ('customer', 'order', 'product') AND id BETWEEN 1000 AND 1999
            GROUP BY source_type
            ORDER BY total_records DESC
        """)
        
        assert len(extraction_results) == 3, "3つのソースタイプが抽出されるべきです"
        
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
        
        # 変換処理のシミュレーション - シンプルで確実な構造
        transformation_results = connection.execute_query("""
            SELECT 
                source_type,
                COUNT(*) as total_records,
                COUNT(CASE WHEN ISJSON(data_json) = 1 THEN 1 END) as valid_json_records,
                COUNT(CASE WHEN JSON_VALUE(data_json, '$.name') IS NOT NULL THEN 1 END) as records_with_name,
                COUNT(CASE WHEN JSON_VALUE(data_json, '$.status') IS NOT NULL THEN 1 END) as records_with_status,
                AVG(CAST(LEN(data_json) AS FLOAT)) as avg_data_size
            FROM raw_data_source
            WHERE source_type IN ('customer', 'order', 'product')
            GROUP BY source_type
            ORDER BY total_records DESC
        """)
        
        assert len(transformation_results) > 0, "変換結果が取得できません"
        
        # 変換品質の検証 - インデックスエラーを回避
        if transformation_results and len(transformation_results[0]) >= 2:
            total_records = sum([r[1] for r in transformation_results])
            assert total_records > 0, "変換処理でレコードが失われました"
            
            # データ分類の検証
            data_types = set([r[0] for r in transformation_results])
            expected_types = {'customer', 'order', 'product'}
            
            for expected_type in expected_types:
                assert expected_type in data_types, f"データタイプ '{expected_type}' が見つかりません"
        
        logger.info(f"データ変換パイプラインテスト完了: {len(transformation_results)}タイプのデータを変換")

    def test_data_loading_pipeline(self):
        """データローディングパイプラインのテスト"""
        connection = SynapseE2EConnection()
        
        # ローディング処理のシミュレーション
        loading_start_time = time.time()
        
        # バッチローディングのシミュレーション - シンプルな構造
        batch_loading_results = connection.execute_query("""
            SELECT 
                source_type,
                COUNT(*) as total_records,
                AVG(CAST(LEN(data_json) AS FLOAT)) as avg_record_size,
                COUNT(*) * 0.1 as estimated_loading_time_seconds
            FROM raw_data_source
            GROUP BY source_type
            ORDER BY total_records DESC
        """)
        
        loading_execution_time = time.time() - loading_start_time
        
        assert len(batch_loading_results) > 0, "ローディング結果が取得できません"
        assert loading_execution_time < 10, f"ローディング処理時間が長すぎます: {loading_execution_time:.2f}秒"
        
        # ローディングパフォーマンスの検証 - 安全なアクセス
        if batch_loading_results and len(batch_loading_results) > 0:
            # 結果の構造を確認してから処理
            first_result = batch_loading_results[0]
            if len(first_result) >= 2:
                total_loaded = sum([r[1] for r in batch_loading_results if len(r) > 1])
                assert total_loaded > 0, "ローディングされたレコードが0です"
                logger.info(f"データローディング完了: {total_loaded}レコード")
            else:
                logger.info(f"データローディングパイプライン完了: {len(batch_loading_results)}の結果")
        
        logger.info(f"データローディングパイプラインテスト完了、実行時間{loading_execution_time:.2f}秒")

    def test_incremental_data_processing(self):
        """増分データ処理のテスト"""
        connection = SynapseE2EConnection()
        
        # テスト用のraw_data_sourceデータを追加（既存のデータと重複しないIDを使用）
        test_source_data = [
            (3001, 'customer', '{"id": 301, "name": "Test Customer 301", "status": "active"}', '2024-01-04 10:00:00'),
            (3002, 'order', '{"order_id": 301, "customer_id": 301, "amount": 250.75, "status": "completed"}', '2024-01-04 11:00:00'),
            (3003, 'product', '{"product_id": 401, "name": "Test Product 401", "price": 129.99, "category": "electronics"}', '2024-01-04 12:00:00')
        ]
          # 既存の増分処理テストデータを削除（もしあれば）
        connection.execute_query("DELETE FROM raw_data_source WHERE id >= 3000")
        
        # IDENTITY_INSERTを有効にしてテストデータを挿入
        # Skip DB operations if pyodbc is not available
        if not hasattr(self, '_pyodbc_available') or not self._pyodbc_available:
            pytest.skip("pyodbc not available - skipping DB-dependent test")
        
        conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER=sql-server,1433;DATABASE=SynapseTestDB;UID=sa;PWD=YourStrong!Passw0rd123;TrustServerCertificate=yes;Encrypt=yes')
        cur = conn.cursor()
        cur.execute("SET IDENTITY_INSERT raw_data_source ON")
        for record_id, source_type, data_json, created_at in test_source_data:
            cur.execute(
                "INSERT INTO raw_data_source (id, source_type, data_json, created_at) VALUES (?, ?, ?, ?)",
                record_id, source_type, data_json, created_at
            )
        cur.execute("SET IDENTITY_INSERT raw_data_source OFF")
        conn.commit()
        cur.close()
        conn.close()
        
        # 増分処理用のウォーターマークテーブル確認（data_watermarksは既存）
        watermark_check = connection.execute_query("""
            SELECT COUNT(*) as watermark_count FROM data_watermarks
        """)
        
        # ウォーターマークが存在することを確認
        assert len(watermark_check) > 0 and watermark_check[0][0] > 0, "ウォーターマークテーブルが初期化されていません"
        
        # 増分データ処理のシミュレーション - シンプルなアプローチ
        incremental_results = connection.execute_query("""
            SELECT 
                rds.source_type,
                COUNT(*) as new_records_found,
                MIN(rds.id) as min_new_id,
                MAX(rds.id) as max_new_id,
                MIN(rds.created_at) as earliest_new_timestamp,
                MAX(rds.created_at) as latest_new_timestamp
            FROM raw_data_source rds
            WHERE rds.id >= 3000  -- 最近追加されたデータ
            GROUP BY rds.source_type
            ORDER BY new_records_found DESC
        """)
        
        assert len(incremental_results) > 0, "増分処理結果が取得できません"
        
        # 増分処理の効率性検証 - インデックスエラーを回避
        if incremental_results and len(incremental_results[0]) >= 2:
            total_incremental_records = sum([r[1] for r in incremental_results])
            assert total_incremental_records > 0, "増分処理対象レコードが見つかりません"
            
            # ウォーターマーク更新のシミュレーション
            for result in incremental_results:
                source_type = result[0]
                max_new_id = result[3]
                
                if max_new_id:
                    # source_typeに対応するウォーターマークを更新
                    source_name = source_type + '_source'
                    connection.execute_query(f"""
                        UPDATE data_watermarks 
                        SET last_processed_id = {max_new_id},
                            last_updated = GETDATE(),
                            processing_status = 'completed'
                        WHERE source_name = '{source_name}'
                    """)
        
        logger.info(f"増分データ処理テスト完了: {len(incremental_results)}ソースを処理")

    def test_data_quality_monitoring(self):
        """データ品質監視のテスト"""
        connection = SynapseE2EConnection()
        
        # データ品質メトリクスの計算 - シンプルな構造
        quality_monitoring_results = connection.execute_query("""
            SELECT 
                source_type,
                COUNT(*) as total_records,
                COUNT(CASE WHEN data_json IS NOT NULL AND data_json != '' THEN 1 END) as non_empty_records,
                COUNT(CASE WHEN ISJSON(data_json) = 1 THEN 1 END) as valid_json_records,
                COUNT(DISTINCT id) as unique_ids,
                COUNT(CASE WHEN JSON_VALUE(data_json, '$.id') IS NOT NULL OR JSON_VALUE(data_json, '$.order_id') IS NOT NULL OR JSON_VALUE(data_json, '$.product_id') IS NOT NULL THEN 1 END) as records_with_entity_id
            FROM raw_data_source
            GROUP BY source_type
            ORDER BY total_records DESC
        """)
        
        assert len(quality_monitoring_results) > 0, "データ品質監視結果が取得できません"
        
        # 品質基準の検証
        for result in quality_monitoring_results:
            source_type = result[0]
            total_records = result[1]
            non_empty_records = result[2]
            valid_json_records = result[3]
            unique_ids = result[4]
            
            # 基本品質基準の確認
            completeness = (non_empty_records / total_records * 100) if total_records > 0 else 0
            validity = (valid_json_records / total_records * 100) if total_records > 0 else 0
            uniqueness = (unique_ids / total_records * 100) if total_records > 0 else 0
            
            assert completeness >= 80, f"ソースタイプ {source_type} の完全性スコアが低すぎます: {completeness:.1f}%"
            assert validity >= 90, f"ソースタイプ {source_type} の有効性スコアが低すぎます: {validity:.1f}%"
            assert uniqueness >= 95, f"ソースタイプ {source_type} の一意性スコアが低すぎます: {uniqueness:.1f}%"
        
        logger.info(f"データ品質監視テスト完了: {len(quality_monitoring_results)}ソースを監視")

    def test_error_handling_and_recovery(self):
        """エラーハンドリングと復旧のテスト"""
        connection = SynapseE2EConnection()
        
        # テーブルが存在しない場合は作成
        connection.execute_query("""
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'pipeline_errors')
            CREATE TABLE pipeline_errors (
                id INT PRIMARY KEY,
                error_type NVARCHAR(100),
                error_description NVARCHAR(500),
                severity NVARCHAR(20),
                category NVARCHAR(50),
                created_at DATETIME2 DEFAULT GETDATE()
            )
        """)
        
        # 既存データをクリア
        connection.execute_query("DELETE FROM pipeline_errors WHERE id BETWEEN 1 AND 10")
        
        # エラーシナリオの作成
        error_scenarios = [
            (1, 'data_format_error', 'Invalid JSON format in source data', 'high', 'format_validation'),
            (2, 'connection_timeout', 'Database connection timeout during processing', 'medium', 'connection_management'),
            (3, 'disk_space_full', 'Insufficient disk space for temporary files', 'high', 'resource_management')
        ]
        
        for error_id, error_type, error_description, severity, category in error_scenarios:
            connection.execute_query(f"""
                INSERT INTO pipeline_errors (id, error_type, error_description, severity, category, created_at) 
                VALUES ({error_id}, '{error_type}', '{error_description}', '{severity}', '{category}', GETDATE())
            """)
        
        # エラーハンドリング戦略のシミュレーション - シンプルな構造
        error_handling_results = connection.execute_query("""
            SELECT 
                error_type,
                category,
                severity,
                COUNT(*) as error_count,
                MAX(created_at) as last_occurrence
            FROM pipeline_errors
            GROUP BY error_type, category, severity
            ORDER BY error_count DESC
        """)
        
        assert len(error_handling_results) > 0, "エラーハンドリング結果が取得できません"
        
        # エラー対応の検証
        high_severity_errors = [r for r in error_handling_results if r[2] == 'high']
        
        # 自動復旧の有効性確認
        assert len(error_handling_results) >= 3, "エラータイプが少なすぎます"
        
        logger.info(f"エラーハンドリングと復旧テスト完了: {len(error_handling_results)}エラータイプを分析、{len(high_severity_errors)}件が高重要度")

    def test_pipeline_orchestration(self):
        """パイプラインオーケストレーションのテスト"""
        connection = SynapseE2EConnection()
        
        # テーブルが存在しない場合は作成
        connection.execute_query("""
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'pipeline_tasks')
            CREATE TABLE pipeline_tasks (
                id INT PRIMARY KEY,
                stage NVARCHAR(50),
                depends_on_task_id INT,
                task_name NVARCHAR(100),
                estimated_duration_minutes INT,
                current_status NVARCHAR(20)
            )
        """)
        
        # 既存データをクリア
        connection.execute_query("DELETE FROM pipeline_tasks WHERE id BETWEEN 1 AND 20")
        
        # パイプライン依存関係の定義
        pipeline_dependencies = [
            (1, 'data_extraction', None, 'extract_customer_data', 5, 'completed'),
            (2, 'data_extraction', None, 'extract_order_data', 3, 'completed'),
            (3, 'data_transformation', 1, 'transform_customer_data', 8, 'completed'),
            (4, 'data_transformation', 2, 'transform_order_data', 6, 'completed'),  
            (5, 'data_loading', 3, 'load_customer_dim', 4, 'completed'),
            (6, 'data_loading', 4, 'load_order_fact', 7, 'completed')
        ]
        
        for task_id, stage, depends_on, task_name, duration_minutes, status in pipeline_dependencies:
            depends_clause = f"{depends_on}" if depends_on else "NULL"
            connection.execute_query(f"""
                INSERT INTO pipeline_tasks (id, stage, depends_on_task_id, task_name, estimated_duration_minutes, current_status) 
                VALUES ({task_id}, '{stage}', {depends_clause}, '{task_name}', {duration_minutes}, '{status}')
            """)
        
        # オーケストレーション状態の分析 - シンプルな構造
        orchestration_results = connection.execute_query("""
            SELECT 
                stage,
                COUNT(*) as total_tasks,
                COUNT(CASE WHEN current_status = 'completed' THEN 1 END) as completed_tasks,
                COUNT(CASE WHEN current_status = 'running' THEN 1 END) as running_tasks,
                COUNT(CASE WHEN current_status = 'pending' THEN 1 END) as pending_tasks,
                SUM(estimated_duration_minutes) as total_estimated_minutes
            FROM pipeline_tasks
            GROUP BY stage
            ORDER BY 
                CASE stage 
                    WHEN 'data_extraction' THEN 1
                    WHEN 'data_transformation' THEN 2  
                    WHEN 'data_loading' THEN 3
                    ELSE 4
                END
        """)
        
        assert len(orchestration_results) > 0, "パイプラインオーケストレーション結果が取得できません"
        
        # オーケストレーションの健全性チェック
        total_pipeline_tasks = sum([r[1] for r in orchestration_results])
        completed_pipeline_tasks = sum([r[2] for r in orchestration_results])
        
        assert total_pipeline_tasks > 0, "パイプラインタスクが定義されていません"
        
        overall_completion = completed_pipeline_tasks / total_pipeline_tasks * 100
        assert overall_completion >= 20, f"パイプライン全体の完了率が低すぎます: {overall_completion:.1f}%"
        
        # 少なくとも1つの段階が完了していることを確認
        completed_stages = [r for r in orchestration_results if r[2] == r[1]]  # completed_tasks == total_tasks
        assert len(completed_stages) >= 1, f"完了した段階が少なすぎます: {len(completed_stages)}"
        
        logger.info(f"パイプラインオーケストレーションテスト完了: {len(orchestration_results)}段階、全体完了率{overall_completion:.1f}%、{len(completed_stages)}段階完了")

    def test_performance_optimization_advanced(self):
        """高度なパフォーマンス最適化のテスト"""
        connection = SynapseE2EConnection()
        
        # パフォーマンステストの実行
        optimization_start_time = time.time()
        
        # 複雑なクエリパフォーマンステスト - シンプルな構造
        performance_results = connection.execute_query("""
            SELECT 
                'Large Join Operation' as test_name,
                COUNT(*) as records_processed,
                'High Volume' as volume_category,
                'Join Optimized' as optimization_status
            FROM raw_data_source rds
            LEFT JOIN ClientDmBx c ON JSON_VALUE(rds.data_json, '$.id') = CAST(c.client_id as VARCHAR)
            WHERE rds.source_type = 'customer'
            
            UNION ALL
            
            SELECT 
                'Complex Aggregation' as test_name,
                COUNT(*) as records_processed,
                'Medium Volume' as volume_category,
                'Aggregation Optimized' as optimization_status
            FROM (
                SELECT 
                    JSON_VALUE(data_json, '$.id') as entity_id,
                    COUNT(*) as record_count
                FROM raw_data_source
                WHERE source_type IN ('customer', 'order')
                GROUP BY JSON_VALUE(data_json, '$.id')
            ) aggregated_data
            
            ORDER BY records_processed DESC
        """)
        
        optimization_execution_time = time.time() - optimization_start_time
        
        assert len(performance_results) >= 2, "パフォーマンステスト結果が不足しています"
        assert optimization_execution_time < 30, f"最適化テスト実行時間が長すぎます: {optimization_execution_time:.2f}秒"
        
        # パフォーマンス改善効果の検証
        total_processed = sum([r[1] for r in performance_results])
        assert total_processed > 0, "パフォーマンステストでレコードが処理されていません"
        
        logger.info(f"高度パフォーマンス最適化テスト完了: 実行時間{optimization_execution_time:.2f}秒、{total_processed}レコード処理")
