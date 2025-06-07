"""
Azure Data Factory Pipeline E2E Tests - Moving Promotion List Send Pipeline
テスト対象パイプライン: pi_Send_MovingPromotionList

このパイプラインの主要機能:
1. データベースから引越しプロモーションリストデータを抽出してCSV作成
2. 作成したCSVファイルをgzip圧縮してBlobStorageに保存
3. gzipファイルをSFMCにSFTP転送

テストシナリオ:
- 正常データ処理（標準的な引越しプロモーションデータセット）
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


class TestPipelineMovingPromotionList:
 
       
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
    """Moving Promotion List Send パイプライン専用E2Eテストクラス"""
    
    PIPELINE_NAME = "pi_Send_MovingPromotionList"
    
    @pytest.fixture(scope="class")
    def pipeline_helper(self, e2e_synapse_connection):
        """パイプライン専用のヘルパーインスタンス"""
        return e2e_synapse_connection
    
    @pytest.fixture(scope="class")
    def test_data_setup(self, pipeline_helper):
        """テストデータのセットアップ"""
        # 引越しプロモーションリストテスト用データ
        test_moving_promotion_data = [
            {
                "connection_key": "MOVE001",
                "customer_key": "CUST001",
                "moving_date": "2024-03-15",
                "current_address": "東京都渋谷区",
                "new_address": "東京都新宿区",
                "promotion_type": "NEW_CUSTOMER",
                "campaign_code": "MOVE2024Q1",
                "discount_amount": 5000,
                "contact_preference": "EMAIL",
                "status": "ACTIVE",
                "output_datetime": datetime.now().strftime('%Y/%m/%d %H:%M:%S')
            },
            {
                "connection_key": "MOVE002",
                "customer_key": "CUST002", 
                "moving_date": "2024-03-20",
                "current_address": "神奈川県横浜市",
                "new_address": "東京都世田谷区",
                "promotion_type": "EXISTING_CUSTOMER",
                "campaign_code": "MOVE2024Q1",
                "discount_amount": 3000,
                "contact_preference": "PHONE",
                "status": "PENDING",
                "output_datetime": datetime.now().strftime('%Y/%m/%d %H:%M:%S')
            }
        ]
        
        return {
            "test_data": test_moving_promotion_data,
            "expected_columns": ["connection_key", "customer_key", "moving_date", 
                               "current_address", "new_address", "promotion_type", 
                               "campaign_code", "discount_amount", "contact_preference", 
                               "status", "output_datetime"]
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
                container_name="moving-promotion-list", 
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
            "record_limit": 50000,
            "memory_limit_mb": 1024
        }
        
        start_time = datetime.now()
        
        try:
            run_id = await pipeline_helper.run_pipeline(
                self.PIPELINE_NAME, 
                parameters=large_dataset_params
            )
            
            status = await pipeline_helper.wait_for_pipeline_completion(
                run_id, timeout_minutes=45
            )
            
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            assert status == "Succeeded", f"Large dataset test failed: {status}"
            assert execution_time < 2700, f"Execution took too long: {execution_time}s"
            
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
                container_name="moving-promotion-list",
                date_path=datetime.now().strftime('%Y/%m/%d')
            )
            
            csv_content = await pipeline_helper.download_and_extract_csv(output_files[0])
            
            # データ品質チェック
            for row in csv_content:
                # 必須フィールドの非NULL検証
                assert row.get("connection_key"), "connection_key cannot be empty"
                assert row.get("customer_key"), "customer_key cannot be empty"
                assert row.get("moving_date"), "moving_date cannot be empty"
                assert row.get("promotion_type"), "promotion_type cannot be empty"
                
                # 引越し日の形式検証
                moving_date = row.get("moving_date", "")
                assert len(moving_date) >= 10, f"Invalid moving_date format: {moving_date}"
                
                # プロモーションタイプの値検証
                promo_type = row.get("promotion_type", "")
                assert promo_type in ["NEW_CUSTOMER", "EXISTING_CUSTOMER", "VIP"], \
                    f"Invalid promotion_type: {promo_type}"
                
                # 割引金額の数値検証
                discount_str = row.get("discount_amount", "0")
                try:
                    discount_val = float(discount_str)
                    assert discount_val >= 0, f"Discount amount cannot be negative: {discount_val}"
                except ValueError:
                    assert False, f"Invalid discount amount format: {discount_str}"
                
                # 連絡方法の値検証
                contact_pref = row.get("contact_preference", "")
                assert contact_pref in ["EMAIL", "PHONE", "MAIL", "SMS"], \
                    f"Invalid contact_preference: {contact_pref}"
                
                # ステータスの値検証
                status_val = row.get("status", "")
                assert status_val in ["ACTIVE", "PENDING", "COMPLETED", "CANCELLED"], \
                    f"Invalid status: {status_val}"
            
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
                remote_path="/moving_promotion_list",
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
    async def test_campaign_specific_scenarios(self, pipeline_helper):
        """キャンペーン固有シナリオテスト"""
        logger = logging.getLogger(__name__)
        logger.info(f"Starting campaign-specific tests for {self.PIPELINE_NAME}")
        
        campaign_scenarios = [
            {
                "name": "spring_campaign",
                "params": {"campaign_period": "2024-03-01_2024-05-31", "season": "SPRING"},
                "expected_campaigns": ["MOVE2024SPRING", "NEWLIFE2024"]
            },
            {
                "name": "summer_campaign",
                "params": {"campaign_period": "2024-06-01_2024-08-31", "season": "SUMMER"},
                "expected_campaigns": ["MOVE2024SUMMER", "VACATION2024"]
            },
            {
                "name": "year_end_campaign",
                "params": {"campaign_period": "2024-12-01_2024-12-31", "season": "YEAR_END"},
                "expected_campaigns": ["MOVE2024YEAREND", "NEWYEAR2025"]
            }
        ]
        
        for scenario in campaign_scenarios:
            try:
                logger.info(f"Testing campaign scenario: {scenario['name']}")
                
                run_id = await pipeline_helper.run_pipeline(
                    self.PIPELINE_NAME,
                    parameters=scenario["params"]
                )
                
                status = await pipeline_helper.wait_for_pipeline_completion(
                    run_id, timeout_minutes=30
                )
                
                assert status == "Succeeded", f"Campaign scenario {scenario['name']} failed: {status}"
                
                # キャンペーン固有データの検証
                output_files = await pipeline_helper.list_output_files(
                    container_name="moving-promotion-list",
                    date_path=datetime.now().strftime('%Y/%m/%d')
                )
                
                csv_content = await pipeline_helper.download_and_extract_csv(output_files[0])
                
                # キャンペーンコードの検証
                found_campaigns = set()
                for row in csv_content:
                    campaign_code = row.get("campaign_code", "")
                    if campaign_code:
                        found_campaigns.add(campaign_code)
                
                # 期待されるキャンペーンが含まれているか確認
                for expected_campaign in scenario["expected_campaigns"]:
                    assert any(expected_campaign in campaign for campaign in found_campaigns), \
                        f"Expected campaign {expected_campaign} not found"
                
                logger.info(f"Campaign scenario {scenario['name']} passed")
                
            except Exception as e:
                logger.error(f"Campaign scenario test failed for {scenario['name']}: {str(e)}")
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
                "name": "invalid_moving_date",
                "params": {"inject_invalid_dates": True},
                "expected_status": "Failed"
            },
            {
                "name": "missing_promotion_data",
                "params": {"inject_missing_promo_data": True},
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
                    container_name="moving-promotion-list",
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
    async def test_seasonal_data_distribution(self, pipeline_helper):
        """季節データ分布テスト"""
        logger = logging.getLogger(__name__)
        logger.info(f"Starting seasonal data distribution test for {self.PIPELINE_NAME}")
        
        # 季節ごとのデータ分布をテスト
        seasonal_params = {
            "analysis_period": "2024-01-01_2024-12-31",
            "include_seasonal_analysis": True
        }
        
        try:
            run_id = await pipeline_helper.run_pipeline(
                self.PIPELINE_NAME,
                parameters=seasonal_params
            )
            
            status = await pipeline_helper.wait_for_pipeline_completion(
                run_id, timeout_minutes=45
            )
            
            assert status == "Succeeded", f"Seasonal analysis failed: {status}"
            
            # 季節分布の検証
            output_files = await pipeline_helper.list_output_files(
                container_name="moving-promotion-list",
                date_path=datetime.now().strftime('%Y/%m/%d')
            )
            
            csv_content = await pipeline_helper.download_and_extract_csv(output_files[0])
            
            # 月別引越し件数の分布確認
            monthly_counts = {}
            for row in csv_content:
                moving_date = row.get("moving_date", "")
                if moving_date and len(moving_date) >= 7:
                    month = moving_date[:7]  # YYYY-MM
                    monthly_counts[month] = monthly_counts.get(month, 0) + 1
            
            # 3月、4月の引越しピーク確認（一般的な傾向）
            march_count = monthly_counts.get("2024-03", 0)
            april_count = monthly_counts.get("2024-04", 0)
            peak_months = march_count + april_count
            
            total_count = sum(monthly_counts.values())
            if total_count > 0:
                peak_ratio = peak_months / total_count
                logger.info(f"Peak months (Mar-Apr) ratio: {peak_ratio:.2%}")
                # ピーク月の比率が適切な範囲内であることを確認
                assert peak_ratio >= 0.1, f"Peak months ratio too low: {peak_ratio:.2%}"
            
            logger.info(f"Seasonal data distribution test passed for {self.PIPELINE_NAME}")
            
        except Exception as e:
            logger.error(f"Seasonal data distribution test failed: {str(e)}")
            raise


if __name__ == "__main__":
    # テスト実行のためのエントリーポイント
    pytest.main([__file__, "-v", "--tb=short"])
