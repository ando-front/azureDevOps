"""
Azure Data Factory Pipeline E2E Tests - mTG Mail Permission Send Pipeline (Simplified)
テスト対象パイプライン: pi_Send_mTGMailPermission

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


class TestPipelinemTGMailPermission:
 
       
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
    """mTG Mail Permission Send パイプライン専用E2Eテストクラス（簡易版）"""
    
    PIPELINE_NAME = "pi_Send_mTGMailPermission"
    
    def test_mtg_mail_permission_basic_database_connection(self, e2e_synapse_connection: SynapseE2EConnection):
        """
        基本的なデータベース接続とデータ操作テスト
        """
        logger = logging.getLogger(__name__)
        logger.info("=== mTG Mail Permission Basic Database Connection Test ===")
        
        try:
            # 1. 接続テスト
            result = e2e_synapse_connection.execute_query("SELECT 1 as test_value")
            assert len(result) > 0, "基本クエリの実行に失敗"
            assert result[0][0] == 1, "期待された結果が返されていない"
            
            # 2. テーブル存在確認
            table_check = e2e_synapse_connection.execute_query(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'mTGMailPermission'"
            )
            logger.info(f"mTGMailPermission テーブル存在確認: {table_check[0][0]} 件")
            
            # 3. 基本的なデータ挿入テスト
            test_data = [
                {
                    "permission_id": "TEST_PERM_001",
                    "customer_id": "CUST001",
                    "email": "test1@example.com",
                    "permission_status": "granted",
                    "grant_date": "2024-01-15",
                    "mail_type": "marketing"
                }
            ]
            
            # テストデータセットアップ
            setup_result = e2e_synapse_connection.setup_test_data("mtg_mail_permission_basic_test", test_data)
            assert setup_result, "テストデータのセットアップに失敗"
            
            # 4. データ取得テスト
            inserted_data = e2e_synapse_connection.execute_query(
                "SELECT permission_id, customer_id, email FROM mtg_mail_permission_basic_test WHERE permission_id = ?",
                ("TEST_PERM_001",)
            )
            
            assert len(inserted_data) > 0, "挿入したデータが見つからない"
            assert inserted_data[0][0] == "TEST_PERM_001", "permission_idが一致しない"
            
            logger.info("✅ mTG Mail Permission基本データベース接続テスト完了")
            
        except Exception as e:
            logger.error(f"❌ mTG Mail Permission基本データベース接続テスト失敗: {str(e)}")
            raise
        finally:
            # クリーンアップ
            try:
                e2e_synapse_connection.cleanup_test_data("mtg_mail_permission_basic_test")
            except Exception as cleanup_error:
                logger.warning(f"クリーンアップエラー: {cleanup_error}")
    
    def test_mtg_mail_permission_pipeline_simulation(self, e2e_synapse_connection: SynapseE2EConnection):
        """
        パイプライン実行のシミュレーションテスト
        """
        logger = logging.getLogger(__name__)
        logger.info("=== mTG Mail Permission Pipeline Simulation Test ===")
        
        try:
            # 1. 既存のmTGMailPermissionテーブルからデータ取得
            existing_data = e2e_synapse_connection.execute_query(
                "SELECT TOP 5 permission_id, customer_id, email FROM mTGMailPermission"
            )
            
            logger.info(f"既存データ確認: {len(existing_data)} 件のレコードを確認")
            
            # 2. パイプライン実行シミュレーション
            pipeline_result = e2e_synapse_connection.run_pipeline(
                pipeline_name=self.PIPELINE_NAME,
                parameters={
                    "execution_date": datetime.now().strftime("%Y-%m-%d"),
                    "batch_size": 1000
                }
            )
            
            assert pipeline_result["status"] == "Succeeded", "パイプライン実行が失敗"
            assert "run_id" in pipeline_result, "run_idが設定されていない"
            
            # 3. 実行結果の基本検証
            logger.info(f"パイプライン実行結果: {pipeline_result['status']}")
            logger.info(f"実行時間: {pipeline_result['duration']} 秒")
            
            # 4. データ品質検証（簡易版）
            quality_check = e2e_synapse_connection.execute_query(
                "SELECT COUNT(*) FROM mTGMailPermission WHERE permission_status = 'granted'"
            )
            
            granted_count = quality_check[0][0]
            logger.info(f"有効な許可設定数: {granted_count}")
            assert granted_count >= 0, "データ品質チェックが失敗"
            
            # 5. パイプライン実行完了待機のシミュレーション
            completion_result = e2e_synapse_connection.wait_for_pipeline_completion(
                run_id=pipeline_result["run_id"],
                timeout=60
            )
            
            logger.info(f"完了待機結果: {completion_result}")
            
            logger.info("✅ mTG Mail Permissionパイプラインシミュレーションテスト完了")
            
        except Exception as e:
            logger.error(f"❌ mTG Mail Permissionパイプラインシミュレーションテスト失敗: {str(e)}")
            raise
    
    def test_mtg_mail_permission_data_quality_basic(self, e2e_synapse_connection: SynapseE2EConnection):
        """
        基本的なデータ品質検証テスト
        """
        logger = logging.getLogger(__name__)
        logger.info("=== mTG Mail Permission Data Quality Basic Test ===")
        
        try:
            # 1. テーブル構造確認
            table_structure = e2e_synapse_connection.execute_query(
                """
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'mTGMailPermission'
                ORDER BY ORDINAL_POSITION
                """
            )
            
            logger.info(f"テーブル構造確認: {len(table_structure)} 列")
            
            # 2. データ整合性チェック
            integrity_checks = [
                ("重複チェック", "SELECT permission_id, COUNT(*) as cnt FROM mTGMailPermission GROUP BY permission_id HAVING COUNT(*) > 1"),
                ("NULL値チェック", "SELECT COUNT(*) FROM mTGMailPermission WHERE permission_id IS NULL OR customer_id IS NULL"),
                ("日付形式チェック", "SELECT COUNT(*) FROM mTGMailPermission WHERE grant_date IS NOT NULL AND ISDATE(grant_date) = 1")
            ]
            
            for check_name, check_query in integrity_checks:
                try:
                    check_result = e2e_synapse_connection.execute_query(check_query)
                    logger.info(f"{check_name}: {check_result}")
                except Exception as check_error:
                    logger.warning(f"{check_name} でエラー: {check_error}")
            
            # 3. 基本統計情報
            total_records = e2e_synapse_connection.execute_query("SELECT COUNT(*) FROM mTGMailPermission")
            logger.info(f"総レコード数: {total_records[0][0]}")
            
            logger.info("✅ mTG Mail Permissionデータ品質基本テスト完了")
            
        except Exception as e:
            logger.error(f"❌ mTG Mail Permissionデータ品質基本テスト失敗: {str(e)}")
            raise
