"""
E2E Test for LINE ID Link Info Pipeline (Simple Version)

実際のSQL Server接続を使用したLINE ID Link Infoパイプラインの簡易版E2Eテスト
"""

import pytest
import logging
import os
import requests
from tests.e2e.helpers.synapse_e2e_helper import SynapseE2EConnection

logger = logging.getLogger(__name__)


class TestPipelineLineIdLinkInfo:

    @classmethod
    def setup_class(cls):
        """Disable proxy settings for tests"""
        # Store and clear proxy environment variables
        for var in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY']:
            if var in os.environ:
                del os.environ[var]

    def _get_no_proxy_session(self):
        """Get a requests session with proxy disabled"""
        session = requests.Session()
        session.proxies = {'http': None, 'https': None}
        return session
    """LINE ID Link Info Pipeline E2E Test (Simple Version)"""

    def test_database_connection_basic(self, e2e_synapse_connection: SynapseE2EConnection):
        """基本的なデータベース接続テスト"""
        logger.info("Testing basic database connection for LINE ID Link Info")
        
        # 接続テスト
        assert e2e_synapse_connection.wait_for_connection(max_retries=3), "Database connection should be available"
        
        # 基本クエリテスト
        result = e2e_synapse_connection.execute_query("SELECT 1 as test_column")
        assert len(result) == 1
        assert result[0][0] == 1
        
        logger.info("Basic database connection test passed")

    def test_pipeline_simulation_basic(self, e2e_synapse_connection: SynapseE2EConnection):
        """基本的なパイプライン実行シミュレーションテスト"""
        logger.info("Testing LINE ID Link Info pipeline simulation")
        
        # パイプライン実行シミュレーション
        pipeline_result = e2e_synapse_connection.run_pipeline(
            "send_line_id_link_info_pipeline",
            parameters={"execution_date": "2024-01-15"}
        )
        
        # 結果検証
        assert pipeline_result["status"] == "Succeeded"
        assert pipeline_result["pipeline_name"] == "send_line_id_link_info_pipeline"
        assert "run_id" in pipeline_result
        
        # 完了待機のシミュレーション
        completion_result = e2e_synapse_connection.wait_for_pipeline_completion(pipeline_result["run_id"])
        assert completion_result["status"] == "Succeeded"
        
        logger.info("Pipeline simulation test passed")

    def test_line_id_link_data_quality_basic(self, e2e_synapse_connection: SynapseE2EConnection):
        """LINE ID連携データの基本的な品質チェック"""
        logger.info("Testing LINE ID Link Info data quality")
        
        test_table = "test_line_id_link_info"
        test_data = [
            {
                "customer_id": "CUST001",
                "line_id": "LINE001",
                "link_status": "linked",
                "link_date": "2024-01-15",
                "notification_sent": False
            },
            {
                "customer_id": "CUST002", 
                "line_id": "LINE002",
                "link_status": "pending",
                "link_date": "2024-01-16",
                "notification_sent": False
            }
        ]
        
        # テストデータのセットアップ
        setup_success = e2e_synapse_connection.setup_test_data(test_table, test_data)
        assert setup_success, "Test data setup should succeed"
        
        try:
            # データの存在確認
            assert e2e_synapse_connection.table_exists(test_table), "Test table should exist"
            
            # データ件数確認
            row_count = e2e_synapse_connection.get_table_row_count(test_table)
            assert row_count == len(test_data), f"Row count should be {len(test_data)}, got {row_count}"
            
            # データ取得テスト
            retrieved_data = e2e_synapse_connection.get_test_data(test_table, limit=10)
            assert len(retrieved_data) == len(test_data)
            
            logger.info("LINE ID Link Info data quality test passed")
        
        finally:
            # クリーンアップ
            e2e_synapse_connection.cleanup_test_data(test_table, drop_table=True)
