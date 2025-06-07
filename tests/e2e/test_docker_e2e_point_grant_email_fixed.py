"""
Docker化されたE2Eテスト - Point Grant Email Pipeline
パイプライン全体のデータ整合性とメール送信処理を検証する統合テスト
"""

import pytest
import logging
import os
import requests
from datetime import datetime, timedelta
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
class TestPointGrantEmailPipelineE2E:
 
       
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
    """Point Grant Email パイプラインのE2Eテスト"""
    
    def test_pipeline_point_grant_email_complete_flow(self, e2e_connection, test_helper):
        """
        Point Grant Email パイプラインの完全フローテスト
        前提: パイプライン全体のデータ整合性とメール処理を検証
        """
        logger.info("Starting Point Grant Email Pipeline E2E test")
        
        # テストシナリオのセットアップ
        test_helper.setup_test_scenario("point_grant_email")
        
        # 顧客データの準備（実際のclient_dmテーブル構造に対応）
        test_clients = [
            {
                "client_id": "POINT_TEST_001",
                "client_name": "ポイントテスト顧客1",
                "created_date": "2023-01-01"
            },
            {
                "client_id": "POINT_TEST_002",
                "client_name": "ポイントテスト顧客2",
                "created_date": "2023-02-01"
            }
        ]
        
        e2e_connection.clear_table("client_dm")
        e2e_connection.insert_test_data("client_dm", test_clients)
        
        # 事前状態の確認
        initial_email_count = e2e_connection.get_table_count("point_grant_email")
        
        logger.info(f"Initial email records: {initial_email_count}")
        
        # パイプライン実行
        pipeline_result = e2e_connection.execute_pipeline_simulation(
            pipeline_name="point_grant_email",
            parameters={
                "campaign_id": "TEST_CAMPAIGN_001",
                "points_to_grant": 1000,
                "test_mode": "e2e"
            }
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
        
        # メール送信データの検証
        final_email_count = e2e_connection.get_table_count("point_grant_email")
        
        logger.info(f"Final email records: {final_email_count}")
        
        # メールレコードが正常に処理されていることを確認（レコード数は変わらない）
        assert final_email_count == initial_email_count, f"Email records count should remain the same: expected {initial_email_count}, got {final_email_count}"
        
        # パイプライン実行ログが記録されていることを確認
        pipeline_logs = e2e_connection.get_pipeline_execution_logs("point_grant_email", hours_back=1)
        assert len(pipeline_logs) > 0, "Pipeline execution should be logged"
        
        # ログの内容確認
        latest_log = pipeline_logs[0]
        assert latest_log["status"] == "SUCCESS", f"Pipeline log should show SUCCESS status: {latest_log}"
        assert latest_log["input_rows"] == initial_email_count, f"Input rows should match initial email count: {latest_log}"
        
        # メール送信データの内容確認（実際のテーブル構造に対応）
        email_records = e2e_connection.execute_query("""
            SELECT client_id, email_address, points, grant_date
            FROM [dbo].[point_grant_email]
            ORDER BY grant_date DESC
        """)
        
        assert len(email_records) >= 2, "Should have email records for test clients"
        
        # 各顧客にメールレコードが存在することを確認
        client_ids_with_emails = {record["client_id"] for record in email_records}
        expected_client_ids = {"POINT_TEST_001", "POINT_TEST_002"}
        
        assert expected_client_ids.issubset(client_ids_with_emails), "All test clients should have email records"
        
        # ポイント付与額の確認（実際のテーブル構造に対応）
        for record in email_records:
            if record["client_id"] in expected_client_ids:
                assert record["points"] == 1000 or record["points"] == 1500, f"Points should be 1000 or 1500, got {record['points']}"
                assert record["email_address"] is not None, "Email address should not be null"
        
        logger.info("Point Grant Email Pipeline E2E test completed successfully")

    def test_pipeline_point_grant_email_data_validation(self, e2e_connection, test_helper):
        """
        Point Grant Email パイプラインのデータ検証テスト
        """
        logger.info("Starting Point Grant Email Data Validation test")
        
        # テーブル構造の確認
        client_dm_structure = e2e_connection.get_table_structure("client_dm")
        point_grant_email_structure = e2e_connection.get_table_structure("point_grant_email")
        
        # 必要なカラムが存在することを確認
        client_dm_columns = {col["COLUMN_NAME"] for col in client_dm_structure}
        expected_client_dm_columns = {"id", "client_id", "client_name", "created_date"}
        assert expected_client_dm_columns.issubset(client_dm_columns), f"Missing columns in client_dm: {expected_client_dm_columns - client_dm_columns}"
        
        point_grant_email_columns = {col["COLUMN_NAME"] for col in point_grant_email_structure}
        expected_point_grant_email_columns = {"id", "client_id", "email_address", "points", "grant_date"}
        assert expected_point_grant_email_columns.issubset(point_grant_email_columns), f"Missing columns in point_grant_email: {expected_point_grant_email_columns - point_grant_email_columns}"
        
        logger.info("Data validation test completed successfully")

    def test_pipeline_point_grant_email_error_handling(self, e2e_connection, test_helper):
        """
        Point Grant Email パイプラインのエラーハンドリングテスト
        """
        logger.info("Starting Point Grant Email Error Handling test")
        
        # 無効なパラメータでパイプライン実行を試行
        pipeline_result = e2e_connection.execute_pipeline_simulation(
            pipeline_name="point_grant_email",
            parameters={
                "campaign_id": "",  # 空のキャンペーンID
                "points_to_grant": -100,  # 負の値
                "test_mode": "error_test"
            }
        )
        
        # エラーが適切に処理されることを確認
        assert pipeline_result["status"] in ["FAILED", "ERROR"], f"Pipeline should fail with invalid parameters: {pipeline_result}"
        
        logger.info("Error handling test completed successfully")

    def test_pipeline_point_grant_email_performance(self, e2e_connection, test_helper):
        """
        Point Grant Email パイプラインのパフォーマンステスト
        """
        logger.info("Starting Point Grant Email Performance test")
        
        # 大量のテストデータを準備
        large_test_clients = [
            {
                "client_id": f"PERF_TEST_{i:03d}",
                "client_name": f"パフォーマンステスト顧客{i}",
                "created_date": "2023-01-01"
            }
            for i in range(100)
        ]
        
        e2e_connection.clear_table("client_dm")
        e2e_connection.insert_test_data("client_dm", large_test_clients)
        
        # パフォーマンステストの実行
        start_time = datetime.now()
        
        pipeline_result = e2e_connection.execute_pipeline_simulation(
            pipeline_name="point_grant_email",
            parameters={
                "campaign_id": "PERF_TEST_CAMPAIGN",
                "points_to_grant": 500,
                "test_mode": "performance"
            }
        )
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        # パフォーマンス要件の確認（5分以内）
        assert execution_time < 300, f"Pipeline execution took too long: {execution_time} seconds"
        
        # 実行結果の確認
        test_helper.assert_pipeline_success(pipeline_result)
        
        logger.info(f"Performance test completed in {execution_time} seconds")
