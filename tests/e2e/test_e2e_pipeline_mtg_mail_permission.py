"""
Azure Data Factory Pipeline E2E Tests - mTG Mail Permission Send Pipeline
テスト対象パイプライン: pi_Send_mTGMailPermission

このパイプラインの主要機能:
1. データベースからmTGメール許可データを抽出してCSV作成
2. 作成したCSVファイルをgzip圧縮してBlobStorageに保存
3. gzipファイルをSFMCにSFTP転送

テストシナリオ:
- 正常データ処理（標準的なmTGメール許可データセット）
- 大容量データ処理（性能テスト）
- データ品質検証（必須フィールド、フォーマット）
- SFTP転送検証（ファイル完全性、転送時間）
- エラーハンドリング（接続障害、無効データ）
- 日次実行シミュレーション（タイムゾーン処理含む）
"""

import pytest
import asyncio
import os
import requests
from datetime import datetime, timedelta
import pytz
from typing import Dict, List, Any
import logging
import gzip
import csv
import json
from unittest.mock import patch, MagicMock

# テスト用ヘルパーモジュール
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
    """mTG Mail Permission Send パイプライン専用E2Eテストクラス"""
    
    PIPELINE_NAME = "pi_Send_mTGMailPermission"
    
    @pytest.fixture(scope="class")
    def pipeline_helper(self, e2e_synapse_connection):
        """パイプライン専用のヘルパーインスタンス"""
        return e2e_synapse_connection
    
    @pytest.fixture(scope="class")
    def test_data_setup(self, pipeline_helper):
        """テストデータのセットアップ"""
        # mTGメール許可テスト用データ
        test_mtg_permission_data = [
            {
                "connection_key": "MTG001",
                "customer_key": "CUST001",
                "email_address": "test1@example.com",
                "permission_status": "GRANTED",
                "permission_date": "2024-01-15",
                "update_datetime": "2024-01-15 10:30:00",
                "output_datetime": datetime.now().strftime('%Y/%m/%d %H:%M:%S')
            },
            {
                "connection_key": "MTG002", 
                "customer_key": "CUST002",
                "email_address": "test2@example.com",
                "permission_status": "REVOKED",
                "permission_date": "2024-01-16",
                "update_datetime": "2024-01-16 14:45:00",
                "output_datetime": datetime.now().strftime('%Y/%m/%d %H:%M:%S')
            }
        ]
        
        return {
            "test_data": test_mtg_permission_data,
            "expected_columns": ["connection_key", "customer_key", "email_address", 
                               "permission_status", "permission_date", "update_datetime", "output_datetime"]
        }
    
    @pytest.mark.asyncio
    async def test_basic_pipeline_execution(self, pipeline_helper, test_data_setup):
        """基本パイプライン実行テスト"""
        logger = logging.getLogger(__name__)
        logger.info(f"Starting basic execution test for {self.PIPELINE_NAME}")
        
        try:
            # パイプライン実行
            run_id = await pipeline_helper.run_pipeline(self.PIPELINE_NAME)
            
            # 実行完了まで監視
            status = await pipeline_helper.wait_for_pipeline_completion(
                run_id, timeout_minutes=30
            )
            
            assert status == "Succeeded", f"Pipeline failed with status: {status}"
            
            # 出力ファイル検証
            output_files = await pipeline_helper.list_output_files(
                container_name="mtg-mail-permission", 
                date_path=datetime.now().strftime('%Y/%m/%d')
            )
            
            assert len(output_files) > 0, "No output files found"
            
            # CSVファイル内容検証
            csv_content = await pipeline_helper.download_and_extract_csv(output_files[0])
            assert len(csv_content) > 0, "CSV content is empty"
            
            # カラム検証
            expected_cols = test_data_setup["expected_columns"]
            actual_cols = list(csv_content[0].keys())
            
            for col in expected_cols:
                assert col in actual_cols, f"Required column {col} not found in CSV"
            
            logger.info(f"Basic execution test passed for {self.PIPELINE_NAME}")
            
        except Exception as e:
            logger.error(f"Basic execution test failed: {str(e)}")
            raise
    
    @pytest.mark.asyncio
    async def test_large_dataset_performance(self, pipeline_helper):
        """大容量データセット性能テスト"""
        logger = logging.getLogger(__name__)
        logger.info(f"Starting large dataset performance test for {self.PIPELINE_NAME}")
        
        # 大容量データを想定したパラメータ設定
        large_dataset_params = {
            "test_mode": "performance",
            "record_limit": 100000,
            "memory_limit_mb": 2048
        }
        
        start_time = datetime.now()
        
        try:
            run_id = await pipeline_helper.run_pipeline(
                self.PIPELINE_NAME, 
                parameters=large_dataset_params
            )
            
            status = await pipeline_helper.wait_for_pipeline_completion(
                run_id, timeout_minutes=60
            )
            
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            assert status == "Succeeded", f"Large dataset test failed: {status}"
            assert execution_time < 3600, f"Execution took too long: {execution_time}s"
            
            logger.info(f"Large dataset test completed in {execution_time}s")
            
        except Exception as e:
            logger.error(f"Large dataset performance test failed: {str(e)}")
            raise
    
    @pytest.mark.asyncio
    async def test_data_quality_validation(self, pipeline_helper, test_data_setup):
        """データ品質検証テスト"""
        logger = logging.getLogger(__name__)
        logger.info(f"Starting data quality validation for {self.PIPELINE_NAME}")
        
        try:
            run_id = await pipeline_helper.run_pipeline(self.PIPELINE_NAME)
            
            status = await pipeline_helper.wait_for_pipeline_completion(
                run_id, timeout_minutes=30
            )
            
            assert status == "Succeeded", f"Pipeline failed: {status}"
            
            # 出力ファイル取得
            output_files = await pipeline_helper.list_output_files(
                container_name="mtg-mail-permission",
                date_path=datetime.now().strftime('%Y/%m/%d')
            )
            
            csv_content = await pipeline_helper.download_and_extract_csv(output_files[0])
            
            # データ品質チェック
            for row in csv_content:
                # 必須フィールドの非NULL検証
                assert row.get("connection_key"), "connection_key cannot be empty"
                assert row.get("customer_key"), "customer_key cannot be empty"
                assert row.get("email_address"), "email_address cannot be empty"
                assert row.get("permission_status"), "permission_status cannot be empty"
                
                # メールアドレス形式検証
                email = row.get("email_address", "")
                assert "@" in email and "." in email, f"Invalid email format: {email}"
                
                # 許可ステータスの値検証
                status_val = row.get("permission_status", "")
                assert status_val in ["GRANTED", "REVOKED", "PENDING"], f"Invalid permission status: {status_val}"
                
                # 日時フォーマット検証
                output_dt = row.get("output_datetime", "")
                assert len(output_dt) > 0, "output_datetime cannot be empty"
            
            logger.info(f"Data quality validation passed for {self.PIPELINE_NAME}")
            
        except Exception as e:
            logger.error(f"Data quality validation failed: {str(e)}")
            raise
    
    @pytest.mark.asyncio
    async def test_sftp_transfer_validation(self, pipeline_helper):
        """SFTP転送検証テスト"""
        logger = logging.getLogger(__name__)
        logger.info(f"Starting SFTP transfer validation for {self.PIPELINE_NAME}")
        
        try:
            run_id = await pipeline_helper.run_pipeline(self.PIPELINE_NAME)
            
            status = await pipeline_helper.wait_for_pipeline_completion(
                run_id, timeout_minutes=30
            )
            
            assert status == "Succeeded", f"Pipeline failed: {status}"
            
            # SFTP転送後のファイル存在確認
            sftp_files = await pipeline_helper.list_sftp_files(
                remote_path="/mtg_mail_permission",
                file_pattern="*.gz"
            )
            
            assert len(sftp_files) > 0, "No files found on SFTP server"
            
            # ファイルサイズとMD5チェック
            for file_info in sftp_files:
                assert file_info["size"] > 0, f"File {file_info['name']} is empty"
                
                # ローカルファイルとの整合性確認
                local_hash = await pipeline_helper.calculate_file_hash(file_info["local_path"])
                remote_hash = await pipeline_helper.calculate_sftp_file_hash(file_info["remote_path"])
                
                assert local_hash == remote_hash, f"File integrity check failed for {file_info['name']}"
            
            logger.info(f"SFTP transfer validation passed for {self.PIPELINE_NAME}")
            
        except Exception as e:
            logger.error(f"SFTP transfer validation failed: {str(e)}")
            raise
    
    @pytest.mark.asyncio
    async def test_error_handling_scenarios(self, pipeline_helper):
        """エラーハンドリングシナリオテスト"""
        logger = logging.getLogger(__name__)
        logger.info(f"Starting error handling tests for {self.PIPELINE_NAME}")
        
        error_scenarios = [
            {
                "name": "database_connection_failure",
                "params": {"force_db_error": True},
                "expected_status": "Failed"
            },
            {
                "name": "sftp_connection_failure", 
                "params": {"force_sftp_error": True},
                "expected_status": "Failed"
            },
            {
                "name": "invalid_data_format",
                "params": {"inject_invalid_data": True},
                "expected_status": "Failed"
            }
        ]
        
        for scenario in error_scenarios:
            try:
                logger.info(f"Testing error scenario: {scenario['name']}")
                
                run_id = await pipeline_helper.run_pipeline(
                    self.PIPELINE_NAME,
                    parameters=scenario["params"]
                )
                
                status = await pipeline_helper.wait_for_pipeline_completion(
                    run_id, timeout_minutes=15
                )
                
                assert status == scenario["expected_status"], \
                    f"Expected {scenario['expected_status']}, got {status}"
                
                logger.info(f"Error scenario {scenario['name']} handled correctly")
                
            except Exception as e:
                logger.error(f"Error handling test failed for {scenario['name']}: {str(e)}")
                raise
    
    @pytest.mark.asyncio
    async def test_timezone_handling(self, pipeline_helper):
        """タイムゾーン処理テスト"""
        logger = logging.getLogger(__name__)
        logger.info(f"Starting timezone handling test for {self.PIPELINE_NAME}")
        
        # 異なるタイムゾーンでのテスト
        timezones = ["UTC", "Asia/Tokyo", "America/New_York"]
        
        for tz_name in timezones:
            try:
                tz_params = {
                    "execution_timezone": tz_name,
                    "output_timezone": "Asia/Tokyo"  # 日本時間での出力
                }
                
                run_id = await pipeline_helper.run_pipeline(
                    self.PIPELINE_NAME,
                    parameters=tz_params
                )
                
                status = await pipeline_helper.wait_for_pipeline_completion(
                    run_id, timeout_minutes=30
                )
                
                assert status == "Succeeded", f"Timezone test failed for {tz_name}: {status}"
                
                # 出力時間の検証
                output_files = await pipeline_helper.list_output_files(
                    container_name="mtg-mail-permission",
                    date_path=datetime.now().strftime('%Y/%m/%d')
                )
                
                csv_content = await pipeline_helper.download_and_extract_csv(output_files[0])
                
                # 出力時間が日本時間で記録されていることを確認
                for row in csv_content:
                    output_dt_str = row.get("output_datetime", "")
                    if output_dt_str:
                        # 日本時間フォーマットの確認
                        assert len(output_dt_str) >= 16, f"Invalid datetime format: {output_dt_str}"
                
                logger.info(f"Timezone handling test passed for {tz_name}")
                
            except Exception as e:
                logger.error(f"Timezone handling test failed for {tz_name}: {str(e)}")
                raise
    
    @pytest.mark.asyncio
    async def test_daily_execution_simulation(self, pipeline_helper):
        """日次実行シミュレーションテスト"""
        logger = logging.getLogger(__name__)
        logger.info(f"Starting daily execution simulation for {self.PIPELINE_NAME}")
        
        # 複数日の実行をシミュレート
        simulation_dates = [
            datetime.now() - timedelta(days=2),
            datetime.now() - timedelta(days=1),
            datetime.now()
        ]
        
        for exec_date in simulation_dates:
            try:
                daily_params = {
                    "execution_date": exec_date.strftime('%Y-%m-%d'),
                    "batch_mode": "daily"
                }
                
                run_id = await pipeline_helper.run_pipeline(
                    self.PIPELINE_NAME,
                    parameters=daily_params
                )
                
                status = await pipeline_helper.wait_for_pipeline_completion(
                    run_id, timeout_minutes=30
                )
                
                assert status == "Succeeded", f"Daily simulation failed for {exec_date}: {status}"
                
                # 日付別ファイル出力確認
                date_path = exec_date.strftime('%Y/%m/%d')
                output_files = await pipeline_helper.list_output_files(
                    container_name="mtg-mail-permission",
                    date_path=date_path
                )
                
                assert len(output_files) > 0, f"No output files for date {date_path}"
                
                logger.info(f"Daily execution simulation passed for {exec_date.strftime('%Y-%m-%d')}")
                
            except Exception as e:
                logger.error(f"Daily execution simulation failed for {exec_date}: {str(e)}")
                raise
        
        logger.info(f"All daily execution simulations completed for {self.PIPELINE_NAME}")


if __name__ == "__main__":
    # テスト実行のためのエントリーポイント
    pytest.main([__file__, "-v", "--tb=short"])
