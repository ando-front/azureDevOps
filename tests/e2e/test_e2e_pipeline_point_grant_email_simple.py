"""
E2E Test for Point Grant Email Pipeline (Simple Version)

実際のSQL Server接続を使用したポイント付与メールパイプラインの簡易版E2Eテスト
"""

import pytest
import logging
from tests.e2e.helpers.synapse_e2e_helper import SynapseE2EConnection
from tests.e2e.helpers.sql_query_manager import E2ESQLQueryManager

logger = logging.getLogger(__name__)


class TestPipelinePointGrantEmail:
    """Point Grant Email Pipeline E2E Test (Simple Version)"""

    def setup_method(self):
        self.sql_manager = E2ESQLQueryManager('point_grant_lost_email.sql')

    def test_database_connection_basic(self, e2e_synapse_connection: SynapseE2EConnection):
        """基本的なデータベース接続テスト"""
        logger.info("Testing basic database connection for Point Grant Email")
        
        # 接続テスト
        assert e2e_synapse_connection.wait_for_connection(max_retries=3), "Database connection should be available"
        
        # 基本クエリテスト
        result = e2e_synapse_connection.execute_query(self.sql_manager.get_query('get_basic_test_query'))
        assert len(result) == 1
        assert result[0][0] == 1
        
        logger.info("Basic database connection test passed")

    def test_pipeline_simulation_basic(self, e2e_synapse_connection: SynapseE2EConnection):
        """基本的なパイプライン実行シミュレーションテスト"""
        logger.info("Testing Point Grant Email pipeline simulation")
        
        # パイプライン実行シミュレーション
        pipeline_result = e2e_synapse_connection.run_pipeline(
            "send_point_grant_email_pipeline",
            parameters={"execution_date": "2024-01-15", "email_template": "point_grant_v2"}
        )
        
        # 結果検証
        assert pipeline_result["status"] == "Succeeded"
        assert pipeline_result["pipeline_name"] == "send_point_grant_email_pipeline"
        assert "run_id" in pipeline_result
        
        # 完了待機のシミュレーション
        completion_result = e2e_synapse_connection.wait_for_pipeline_completion(pipeline_result["run_id"])
        assert completion_result["status"] == "Succeeded"
        
        logger.info("Pipeline simulation test passed")

    def test_point_grant_data_quality_basic(self, e2e_synapse_connection: SynapseE2EConnection):
        """ポイント付与データの基本的な品質チェック"""
        logger.info("Testing Point Grant Email data quality")
        
        test_table = "test_point_grant_email"
        test_data = [
            {
                "customer_id": "CUST001",
                "points_granted": 500,
                "grant_type": "welcome_bonus",
                "grant_date": "2024-01-15",
                "email_sent": False,
                "campaign_id": "CAMP001",
                "expiry_date": "2024-12-31"
            },
            {
                "customer_id": "CUST002",
                "points_granted": 1000,
                "grant_type": "purchase_bonus",
                "grant_date": "2024-01-16",
                "email_sent": False,
                "campaign_id": "CAMP002",
                "expiry_date": "2024-12-31"
            }
        ]
          # テストデータのセットアップ
        setup_success = e2e_synapse_connection.setup_test_data(test_table, test_data)
        assert setup_success, "Test data setup should succeed"
        
        try:
            # テーブル存在確認（外部SQLクエリを使用）
            table_exists_query = self.sql_manager.get_query('check_test_table_exists')
            table_exists_result = e2e_synapse_connection.execute_query(table_exists_query)
            assert table_exists_result[0][0] == 1, "Test table should exist"
            
            # データ件数確認
            row_count_query = self.sql_manager.get_query('get_test_point_grant_email_count')
            row_count_result = e2e_synapse_connection.execute_query(row_count_query)
            row_count = row_count_result[0][0]
            assert row_count == len(test_data), f"Row count should be {len(test_data)}, got {row_count}"
            
            # 高額ポイント付与の確認（直接SQLで実行）
            high_value_query = """
            SELECT customer_id, points_granted, grant_type
            FROM test_point_grant_email 
            WHERE points_granted >= 1000
            """
            high_value = e2e_synapse_connection.execute_query(high_value_query)
            assert len(high_value) == 1, "Should have 1 high-value point grant"
            assert high_value[0][1] == 1000, "High-value grant should be 1000 points"
            
            logger.info("Point Grant Email data quality test passed")
        
        finally:
            # クリーンアップ
            e2e_synapse_connection.cleanup_test_data(test_table, drop_table=True)
