"""
E2E Test for LIM Settlement Breakdown Repair Pipeline (Simple Version)

実際のSQL Server接続を使用したLIM決済内訳修復パイプラインの簡易版E2Eテスト
"""

import pytest
import logging
import os
import requests
from tests.e2e.helpers.synapse_e2e_helper import SynapseE2EConnection

logger = logging.getLogger(__name__)


class TestPipelineLimSettlementBreakdownRepair:

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
    """LIM Settlement Breakdown Repair Pipeline E2E Test (Simple Version)"""

    def test_database_connection_basic(self, e2e_synapse_connection: SynapseE2EConnection):
        """基本的なデータベース接続テスト"""
        logger.info("Testing basic database connection for LIM Settlement Breakdown Repair")
        
        # 接続テスト
        assert e2e_synapse_connection.wait_for_connection(max_retries=3), "Database connection should be available"
        
        # 基本クエリテスト
        result = e2e_synapse_connection.execute_query("SELECT 1 as test_column")
        assert len(result) == 1
        assert result[0][0] == 1
        
        logger.info("Basic database connection test passed")

    def test_pipeline_simulation_basic(self, e2e_synapse_connection: SynapseE2EConnection):
        """基本的なパイプライン実行シミュレーションテスト"""
        logger.info("Testing LIM Settlement Breakdown Repair pipeline simulation")
        
        # パイプライン実行シミュレーション
        pipeline_result = e2e_synapse_connection.run_pipeline(
            "send_lim_settlement_breakdown_repair_pipeline",
            parameters={"execution_date": "2024-01-15", "repair_mode": "auto"}
        )
        
        # 結果検証
        assert pipeline_result["status"] == "Succeeded"
        assert pipeline_result["pipeline_name"] == "send_lim_settlement_breakdown_repair_pipeline"
        assert "run_id" in pipeline_result
        
        # 完了待機のシミュレーション
        completion_result = e2e_synapse_connection.wait_for_pipeline_completion(pipeline_result["run_id"])
        assert completion_result["status"] == "Succeeded"
        
        logger.info("Pipeline simulation test passed")

    def test_settlement_data_quality_basic(self, e2e_synapse_connection: SynapseE2EConnection):
        """決済データの基本的な品質チェック"""
        logger.info("Testing LIM Settlement data quality")
        
        test_table = "test_lim_settlement_breakdown"
        test_data = [
            {
                "settlement_id": "SETT001",
                "customer_id": "CUST001",
                "amount": 15000.00,
                "settlement_date": "2024-01-15",
                "status": "error",
                "repair_needed": True
            },
            {
                "settlement_id": "SETT002",
                "customer_id": "CUST002",
                "amount": 25000.00,
                "settlement_date": "2024-01-16",
                "status": "completed",
                "repair_needed": False
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
            
            # エラー状態の決済を検索
            error_settlements_query = f"""
            SELECT settlement_id, customer_id 
            FROM {test_table} 
            WHERE status = 'error' AND repair_needed = 1
            """
            error_settlements = e2e_synapse_connection.execute_query(error_settlements_query)
            assert len(error_settlements) == 1, "Should have 1 settlement requiring repair"
            
            logger.info("LIM Settlement data quality test passed")
        
        finally:
            # クリーンアップ
            e2e_synapse_connection.cleanup_test_data(test_table, drop_table=True)
