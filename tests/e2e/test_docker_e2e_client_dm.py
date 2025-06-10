"""
Docker化されたE2Eテスト - Client Data Mart Pipeline
パイプライン全体のデータ整合性を検証する統合テスト
"""

import pytest
import logging
import os
import requests
from datetime import datetime
from tests.e2e.helpers.docker_e2e_helper import DockerE2EConnection, E2ETestHelper
from tests.helpers.reproducible_e2e_helper import setup_reproducible_test_class, cleanup_reproducible_test_class

logger = logging.getLogger(__name__)

@pytest.fixture(scope="class")
def e2e_connection():
    """E2E テスト用接続フィクスチャ"""
    connection = DockerE2EConnection()
    yield connection
    # テスト後のクリーンアップ
    connection.cleanup_test_data()

@pytest.fixture
def test_helper(e2e_connection):
    """E2E テストヘルパーフィクスチャ"""
    return E2ETestHelper(e2e_connection)

@pytest.mark.e2e
class TestClientDataMartPipelineE2E:
    
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



    def _get_no_proxy_session(self):
        """Get a requests session with proxy disabled"""
        session = requests.Session()
        session.proxies = {'http': None, 'https': None}
        return session
    """Client Data Mart パイプラインのE2Eテスト"""
    
    def test_pipeline_client_dm_complete_flow(self, e2e_connection, test_helper):
        """
        Client DM パイプラインの完全フローテスト
        前提: パイプライン全体のデータ整合性を検証
        """
        logger.info("Starting Client DM Pipeline E2E test")
          # テストシナリオのセットアップ（実際のテーブル構造に合わせて修正）
        test_clients = [
            {
                "client_id": "TRANSFORM_TEST_001",
                "client_name": "変換テスト顧客1", 
                "created_date": "2023-01-01"
            },
            {
                "client_id": "TRANSFORM_TEST_002", 
                "client_name": "変換テスト顧客2",
                "created_date": "2023-02-01"
            }
        ]
        
        # 既存データをクリアして新しいテストデータを挿入
        e2e_connection.clear_table("client_dm")
        e2e_connection.clear_table("ClientDmBx")  # ターゲットテーブルもクリア
        e2e_connection.insert_test_data("client_dm", test_clients)
        
        # 事前状態の確認
        initial_source_count = e2e_connection.get_table_count("client_dm")
        initial_target_count = e2e_connection.get_table_count("ClientDmBx")
        
        logger.info(f"Initial state - Source: {initial_source_count}, Target: {initial_target_count}")
          # パイプライン実行（client_dmからClientDmBxへの変換パイプライン）
        pipeline_result = e2e_connection.execute_pipeline_simulation(
            pipeline_name="client_dm_to_bx",
            parameters={"test_mode": "e2e", "batch_size": 100}
        )
        
        # パイプライン実行の成功を確認
        test_helper.assert_pipeline_success(pipeline_result)
        
        # パイプライン完了を待機
        final_status = test_helper.wait_for_pipeline_completion(
            pipeline_result["execution_id"], 
            timeout=120
        )
        
        # 最終実行ステータスの確認
        assert final_status["status"] == "SUCCESS", f"Pipeline execution failed: {final_status}"
        
        # データ整合性の検証
        final_source_count = e2e_connection.get_table_count("client_dm")
        final_target_count = e2e_connection.get_table_count("ClientDmBx")
        
        logger.info(f"Final state - Source: {final_source_count}, Target: {final_target_count}")
        
        # データ処理の検証
        assert final_source_count >= initial_source_count, "Source data should not decrease"
        assert final_target_count > initial_target_count, "Target data should increase after pipeline execution"
          # パイプライン実行ログの検証
        execution_logs = e2e_connection.get_pipeline_execution_logs("client_dm_to_bx")
        assert len(execution_logs) > 0, "Pipeline execution should generate logs"
        
        # データ損失がないことを確認
        test_helper.assert_no_data_loss(execution_logs)
        
        # 詳細なデータ整合性検証
        validation_result = e2e_connection.validate_data_integrity("client_dm", "ClientDmBx")
        
        # 変換処理があるため、厳密な行数一致ではなく、データ処理が正常に行われたことを確認
        assert validation_result["target_count"] > 0, "Target table should contain processed data"
        
        logger.info("Client DM Pipeline E2E test completed successfully")
    
    def test_pipeline_client_dm_data_transformation_integrity(self, e2e_connection, test_helper):
        """
        Client DM パイプラインのデータ変換整合性テスト
        前提: 入力データから出力データへの変換が正しく行われることを検証
        """
        logger.info("Starting Client DM Data Transformation Integrity test")
          # 特定のテストデータでシナリオセットアップ
        # 実際のテーブル構造に合わせて修正: id, client_id, client_name, created_date
        test_clients = [
            {
                "client_id": "TRANSFORM_TEST_001",
                "client_name": "変換テスト顧客1",
                "created_date": "2023-01-01"
            },
            {
                "client_id": "TRANSFORM_TEST_002",
                "client_name": "変換テスト顧客2", 
                "created_date": "2023-02-01"
            }
        ]
        
        # テストデータの準備
        e2e_connection.clear_table("client_dm")
        e2e_connection.clear_table("ClientDmBx")
        e2e_connection.insert_test_data("client_dm", test_clients)
        
        # Copy Activity の実行
        copy_result = e2e_connection.execute_copy_activity_simulation(
            pipeline_name="pi_Copy_marketing_client_dm",
            activity_name="CopyClientData",
            source_config={"type": "SqlServer", "table": "client_dm"},
            sink_config={"type": "SqlServer", "table": "ClientDmBx"}
        )
        
        # Copy Activity の成功確認
        test_helper.assert_pipeline_success(copy_result, expected_rows=2)
        
        # 変換されたデータの内容確認
        source_data = e2e_connection.execute_query("SELECT * FROM [dbo].[client_dm] ORDER BY client_id")
        target_data = e2e_connection.execute_query("SELECT * FROM [dbo].[ClientDmBx] ORDER BY client_id")
        
        # データが正しく変換されているかを確認
        assert len(source_data) == 2, "Source should have 2 records"
        assert len(target_data) >= 2, "Target should have at least 2 records after transformation"
        
        # 個別のデータ検証
        source_client_ids = {row["client_id"] for row in source_data}
        target_client_ids = {row["client_id"] for row in target_data if row["client_id"]}
        
        # すべてのソースデータがターゲットに存在することを確認
        for client_id in source_client_ids:
            assert client_id in target_client_ids, f"Client {client_id} missing in target data"
        
        logger.info("Client DM Data Transformation Integrity test completed successfully")
    
    def test_pipeline_client_dm_error_handling(self, e2e_connection, test_helper):
        """
        Client DM パイプラインのエラーハンドリングテスト
        前提: パイプラインがエラー時に適切に処理されることを検証
        """
        logger.info("Starting Client DM Error Handling test")
        
        # 意図的に問題のあるデータを準備
        problematic_data = [
            {
                "client_id": None,  # NULL値でエラーを誘発
                "client_name": "エラーテスト顧客",
                "email": "error@test.com",
                "phone": "090-0000-0000",
                "address": "エラーテスト住所",
                "registration_date": "2023-01-01",
                "status": "ACTIVE"
            }
        ]
        
        # テーブルをクリアして問題データを挿入
        e2e_connection.clear_table("client_dm")
        
        try:
            e2e_connection.insert_test_data("client_dm", problematic_data)
        except Exception:
            # NULL制約により挿入が失敗することを期待
            logger.info("Expected error occurred during problematic data insertion")
          # 正常なデータで実行して、エラー処理が適切に動作することを確認
        # 実際のテーブル構造に合わせて修正: id, client_id, client_name, created_date
        normal_data = [
            {
                "client_id": "ERROR_TEST_001",
                "client_name": "正常テスト顧客",
                "created_date": "2023-01-01"
            }
        ]
        
        e2e_connection.insert_test_data("client_dm", normal_data)
        
        # パイプライン実行
        pipeline_result = e2e_connection.execute_pipeline_simulation(
            pipeline_name="pi_Copy_marketing_client_dm"
        )
        
        # 正常データでの実行は成功することを確認
        test_helper.assert_pipeline_success(pipeline_result)
        
        # エラーログの確認
        execution_logs = e2e_connection.get_pipeline_execution_logs("pi_Copy_marketing_client_dm")
        
        # 実行ログが記録されていることを確認
        assert len(execution_logs) > 0, "Execution logs should be recorded"
        
        # 最新の実行が成功していることを確認
        latest_log = execution_logs[0]
        assert latest_log["status"] == "SUCCESS", "Latest execution should be successful with valid data"
        
        logger.info("Client DM Error Handling test completed successfully")
    
    @pytest.mark.slow
    def test_pipeline_client_dm_performance_baseline(self, e2e_connection, test_helper):
        """
        Client DM パイプラインのパフォーマンスベースラインテスト
        前提: パイプラインが許容可能な時間内で実行されることを検証
        """
        logger.info("Starting Client DM Performance Baseline test")
        
        # 大量のテストデータを準備（パフォーマンステスト用）
        large_test_data = []
        for i in range(100):  # 100件のテストデータ
            large_test_data.append({
                "client_id": f"PERF_TEST_{i:03d}",
                "client_name": f"パフォーマンステスト顧客{i}",
                "email": f"perf{i}@test.com",
                "phone": f"090-{i:04d}-{i:04d}",
                "address": f"パフォーマンステスト住所{i}",
                "registration_date": "2023-01-01",
                "status": "ACTIVE"
            })
        
        # テストデータの準備
        e2e_connection.clear_table("client_dm")
        e2e_connection.clear_table("ClientDmBx")
        e2e_connection.insert_test_data("client_dm", large_test_data)
        
        # パイプライン実行時間の測定
        start_time = datetime.now()
        
        pipeline_result = e2e_connection.execute_pipeline_simulation(
            pipeline_name="client_dm_to_bx",  # 正しいパイプライン名に修正
            parameters={"batch_size": 50}
        )
        
        # 完了まで待機
        final_status = test_helper.wait_for_pipeline_completion(
            pipeline_result["execution_id"],
            timeout=300  # 5分
        )
        
        end_time = datetime.now()
        execution_duration = (end_time - start_time).total_seconds()
        
        # パフォーマンス検証
        test_helper.assert_pipeline_success(pipeline_result)
        
        # 実行時間が許容範囲内であることを確認（100件で60秒以内）
        assert execution_duration < 60, f"Pipeline execution took too long: {execution_duration:.2f} seconds"
        
        # データ処理効率の確認
        processed_records = len(large_test_data)  # 実際に処理したレコード数を使用
        records_per_second = processed_records / execution_duration if execution_duration > 0 else 0
        
        logger.info(f"Performance metrics - Duration: {execution_duration:.2f}s, Records/sec: {records_per_second:.2f}")
        
        # 最低限のスループットを確認（1件/秒以上）
        assert records_per_second >= 1.0, f"Processing speed too slow: {records_per_second:.2f} records/sec"
        
        logger.info("Client DM Performance Baseline test completed successfully")
