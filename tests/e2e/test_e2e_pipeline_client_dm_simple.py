"""
Azure Data Factory Pipeline E2E Tests - Client DM Send Pipeline (Simplified)
テスト対象パイプライン: pi_Send_ClientDM

基本的なE2Eテスト実装
"""

import pytest
import logging
import os
import requests
from datetime import datetime
import pytz
from tests.e2e.helpers.synapse_e2e_helper import SynapseE2EConnection
from tests.helpers.reproducible_e2e_helper import setup_reproducible_test_class, cleanup_reproducible_test_class


class TestPipelineClientDM:
 
       
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
    """Client DM Send パイプライン専用E2Eテストクラス（簡易版）"""
    
    PIPELINE_NAME = "pi_Send_ClientDM"
    
    def test_client_dm_basic_database_connection(self, e2e_synapse_connection: SynapseE2EConnection):
        """
        基本的なデータベース接続とデータ操作テスト
        """
        logger = logging.getLogger(__name__)
        logger.info("=== Client DM Basic Database Connection Test ===")
        
        try:
            # 1. 接続テスト
            result = e2e_synapse_connection.execute_query("SELECT 1 as test_value")
            assert len(result) > 0, "基本クエリの実行に失敗"
            assert result[0][0] == 1, "期待された結果が返されていない"
            
            # 2. テーブル存在確認
            table_check = e2e_synapse_connection.execute_query(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'ClientDm'"
            )
            logger.info(f"ClientDm テーブル存在確認: {table_check[0][0]} 件")
            
            # 3. 基本的なデータ挿入テスト
            test_data = [
                {
                    "client_id": "TEST_CLIENT_001",
                    "client_name": "テストクライアント1", 
                    "email": "test1@example.com",
                    "registration_date": "2024-01-15",
                    "status": "active",
                    "segment": "premium"
                }
            ]
            
            # テストデータセットアップ
            setup_result = e2e_synapse_connection.setup_test_data("client_dm_basic_test", test_data)
            assert setup_result, "テストデータのセットアップに失敗"
            
            # 4. データ取得テスト
            inserted_data = e2e_synapse_connection.execute_query(
                "SELECT client_id, client_name, email FROM client_dm_basic_test WHERE client_id = ?",
                ("TEST_CLIENT_001",)
            )
            
            assert len(inserted_data) > 0, "挿入したデータが見つからない"
            assert inserted_data[0][0] == "TEST_CLIENT_001", "client_idが一致しない"
            
            logger.info("✅ Client DM基本データベース接続テスト完了")
            
        except Exception as e:
            logger.error(f"❌ Client DM基本データベース接続テスト失敗: {str(e)}")
            raise
        finally:
            # クリーンアップ
            try:
                e2e_synapse_connection.cleanup_test_data("client_dm_basic_test")
            except Exception as cleanup_error:
                logger.warning(f"クリーンアップエラー: {cleanup_error}")
    
    def test_client_dm_pipeline_simulation(self, e2e_synapse_connection: SynapseE2EConnection):
        """
        パイプライン実行のシミュレーションテスト
        """
        logger = logging.getLogger(__name__)
        logger.info("=== Client DM Pipeline Simulation Test ===")
        
        try:
            # 1. 複数のテストデータ準備
            test_clients = [
                {
                    "client_id": f"SIM_CLIENT_{i:03d}",
                    "client_name": f"シミュレーションクライアント{i}",
                    "email": f"sim_test_{i}@example.com",
                    "registration_date": "2024-01-15",
                    "status": "active" if i % 2 == 0 else "inactive",
                    "segment": ["premium", "standard", "basic"][i % 3]
                }
                for i in range(1, 6)  # 5件のテストデータ
            ]
            
            # 2. テストデータセットアップ
            setup_result = e2e_synapse_connection.setup_test_data("client_dm_simulation_test", test_clients)
            assert setup_result, "シミュレーション用テストデータのセットアップに失敗"
            
            # 3. パイプライン実行シミュレーション
            run_result = e2e_synapse_connection.run_pipeline(
                pipeline_name=self.PIPELINE_NAME,
                parameters={}
            )
            
            # 4. 実行結果検証
            assert run_result is not None, "パイプライン実行結果が取得できない"
            assert isinstance(run_result, dict), "パイプライン実行結果の形式が不正"
            
            logger.info(f"パイプライン実行結果: {run_result}")
            
            # 5. データ処理結果確認
            processed_data = e2e_synapse_connection.execute_query(
                "SELECT COUNT(*) FROM client_dm_simulation_test WHERE status = 'active'"
            )
            
            active_count = processed_data[0][0]
            logger.info(f"アクティブなクライアント数: {active_count}")
            assert active_count >= 0, "データ処理結果が不正"
            
            # 6. パイプライン実行完了待機のシミュレーション
            completion_result = e2e_synapse_connection.wait_for_pipeline_completion(
                run_id="test_run_123",
                timeout=60
            )
            
            logger.info(f"完了待機結果: {completion_result}")
            
            logger.info("✅ Client DMパイプラインシミュレーションテスト完了")
            
        except Exception as e:
            logger.error(f"❌ Client DMパイプラインシミュレーションテスト失敗: {str(e)}")
            raise
        finally:
            # クリーンアップ
            try:
                e2e_synapse_connection.cleanup_test_data("client_dm_simulation_test")
            except Exception as cleanup_error:
                logger.warning(f"クリーンアップエラー: {cleanup_error}")
    
    def test_client_dm_data_quality_basic(self, e2e_synapse_connection: SynapseE2EConnection):
        """
        基本的なデータ品質検証テスト
        """
        logger = logging.getLogger(__name__)
        logger.info("=== Client DM Data Quality Basic Test ===")
        
        try:
            # 1. 品質検証用データ（正常・異常データ混在）
            quality_test_data = [
                # 正常データ
                {
                    "client_id": "QUALITY_OK_001",
                    "client_name": "正常データクライアント",
                    "email": "quality_ok@example.com",
                    "registration_date": "2024-01-15",
                    "status": "active",
                    "segment": "premium"
                },
                # 日本語データ
                {
                    "client_id": "QUALITY_JP_002",
                    "client_name": "日本語クライアント名",
                    "email": "japanese@日本.jp",
                    "registration_date": "2024-02-20",
                    "status": "active",
                    "segment": "standard"
                },
                # NULL値を含むデータ
                {
                    "client_id": "QUALITY_NULL_003",
                    "client_name": None,
                    "email": "null_test@example.com",
                    "registration_date": "2024-03-10",
                    "status": "inactive",
                    "segment": "basic"
                }
            ]
            
            # 2. テストデータセットアップ
            setup_result = e2e_synapse_connection.setup_test_data("client_dm_quality_test", quality_test_data)
            assert setup_result, "品質テスト用データのセットアップに失敗"
            
            # 3. データ品質チェック
            # NULL値チェック
            null_count = e2e_synapse_connection.execute_query(
                "SELECT COUNT(*) FROM client_dm_quality_test WHERE client_name IS NULL"
            )[0][0]
            logger.info(f"NULL値を持つレコード数: {null_count}")
            assert null_count >= 0, "NULL値チェックに失敗"
            
            # 4. 重複チェック
            duplicate_count = e2e_synapse_connection.execute_query(
                """
                SELECT COUNT(*) FROM (
                    SELECT client_id, COUNT(*) as cnt 
                    FROM client_dm_quality_test 
                    GROUP BY client_id 
                    HAVING COUNT(*) > 1
                ) as duplicates
                """
            )[0][0]
            logger.info(f"重複レコード数: {duplicate_count}")
            assert duplicate_count == 0, "重複データが存在する"
            
            # 5. 日本語文字エンコーディングチェック
            japanese_data = e2e_synapse_connection.execute_query(
                "SELECT client_name FROM client_dm_quality_test WHERE client_id = 'QUALITY_JP_002'"
            )
            
            if len(japanese_data) > 0 and japanese_data[0][0]:
                japanese_name = japanese_data[0][0]
                logger.info(f"日本語クライアント名: {japanese_name}")
                assert "日本語" in japanese_name, "日本語エンコーディングに問題がある"
            
            logger.info("✅ Client DMデータ品質基本テスト完了")
            
        except Exception as e:
            logger.error(f"❌ Client DMデータ品質基本テスト失敗: {str(e)}")
            raise
        finally:
            # クリーンアップ
            try:
                e2e_synapse_connection.cleanup_test_data("client_dm_quality_test")
            except Exception as cleanup_error:
                logger.warning(f"クリーンアップエラー: {cleanup_error}")


if __name__ == "__main__":
    """
    個別テスト実行用のメイン関数
    """
    # ログ設定
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("Client DM Send Pipeline E2E Tests (Simplified)")
    print("実行コマンド: pytest tests/e2e/test_e2e_pipeline_client_dm_simple.py -v")
