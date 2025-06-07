"""
Azure Data Factory Pipeline E2E Tests - LIM Settlement Breakdown Repair Send Pipeline
テスト対象パイプライン: pi_Send_LIMSettlementBreakdownRepair

このパイプラインの主要機能:
1. データベースからLIM決算内訳修正データを抽出してCSV作成
2. 作成したCSVファイルをgzip圧縮してBlobStorageに保存
3. gzipファイルをSFMCにSFTP転送

テストシナリオ:
- 正常データ処理（標準的なLIM決算内訳修正データセット）
- 大容量データ処理（性能テスト）
- データ品質検証（必須フィールド、金額フォーマット）
- SFTP転送検証（ファイル完全性、転送時間）
- エラーハンドリング（接続障害、無効データ）
- 決算データ固有の検証（期間、金額整合性）
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
from decimal import Decimal, InvalidOperation
from unittest.mock import patch, MagicMock

# テスト用ヘルパーモジュール
from tests.e2e.helpers.synapse_e2e_helper import SynapseE2EConnection
from tests.helpers.reproducible_e2e_helper import setup_reproducible_test_class, cleanup_reproducible_test_class


class TestPipelineLIMSettlementBreakdownRepair:
 
       
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
    """LIM Settlement Breakdown Repair Send パイプライン専用E2Eテストクラス"""
    
    PIPELINE_NAME = "pi_Send_LIMSettlementBreakdownRepair"
    
    @pytest.fixture(scope="class")
    def pipeline_helper(self, e2e_synapse_connection):
        """パイプライン専用のヘルパーインスタンス"""
        return e2e_synapse_connection
    
    @pytest.fixture(scope="class")
    def test_data_setup(self, pipeline_helper):
        """テストデータのセットアップ"""
        # LIM決算内訳修正テスト用データ
        test_lim_settlement_data = [
            {
                "connection_key": "LIM001",
                "settlement_period": "2024Q1",
                "account_code": "ACC001",
                "account_name": "ガス売上",
                "original_amount": 1500000,
                "adjustment_amount": -50000,
                "adjusted_amount": 1450000,
                "adjustment_reason": "計算誤差修正",
                "adjustment_type": "ERROR_CORRECTION",
                "adjustment_date": "2024-04-15",
                "approver_id": "APPR001",
                "department_code": "DEPT001",
                "cost_center": "CC001",
                "business_segment": "GAS_RETAIL",
                "currency_code": "JPY",
                "exchange_rate": 1.0,
                "notes": "四半期決算修正",
                "output_datetime": datetime.now().strftime('%Y/%m/%d %H:%M:%S')
            },
            {
                "connection_key": "LIM002",
                "settlement_period": "2024Q1",
                "account_code": "ACC002",
                "account_name": "電力売上",
                "original_amount": 800000,
                "adjustment_amount": 25000,
                "adjusted_amount": 825000,
                "adjustment_reason": "追加売上計上",
                "adjustment_type": "ADDITIONAL_REVENUE",
                "adjustment_date": "2024-04-16",
                "approver_id": "APPR002",
                "department_code": "DEPT002",
                "cost_center": "CC002",
                "business_segment": "ELECTRICITY_RETAIL",
                "currency_code": "JPY",
                "exchange_rate": 1.0,
                "notes": "追加売上反映",
                "output_datetime": datetime.now().strftime('%Y/%m/%d %H:%M:%S')
            }
        ]
        
        return {
            "test_data": test_lim_settlement_data,
            "expected_columns": ["connection_key", "settlement_period", "account_code", 
                               "account_name", "original_amount", "adjustment_amount", 
                               "adjusted_amount", "adjustment_reason", "adjustment_type",
                               "adjustment_date", "approver_id", "department_code",
                               "cost_center", "business_segment", "currency_code",
                               "exchange_rate", "notes", "output_datetime"]
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
                container_name="lim-settlement-breakdown-repair", 
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
            "record_limit": 75000,
            "memory_limit_mb": 1536
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
                container_name="lim-settlement-breakdown-repair",
                date_path=datetime.now().strftime('%Y/%m/%d')
            )
            
            csv_content = await pipeline_helper.download_and_extract_csv(output_files[0])
            
            # データ品質チェック
            for row in csv_content:
                # 必須フィールドの非NULL検証
                assert row.get("connection_key"), "connection_key cannot be empty"
                assert row.get("settlement_period"), "settlement_period cannot be empty"
                assert row.get("account_code"), "account_code cannot be empty"
                assert row.get("adjustment_type"), "adjustment_type cannot be empty"
                
                # 決算期間の形式検証
                settlement_period = row.get("settlement_period", "")
                assert len(settlement_period) >= 6, f"Invalid settlement_period format: {settlement_period}"
                assert "Q" in settlement_period or "H" in settlement_period or len(settlement_period) == 4, \
                    f"Invalid settlement_period format: {settlement_period}"
                
                # 金額フィールドの数値検証
                original_amount_str = row.get("original_amount", "0")
                adjustment_amount_str = row.get("adjustment_amount", "0")
                adjusted_amount_str = row.get("adjusted_amount", "0")
                
                try:
                    original_amount = Decimal(str(original_amount_str))
                    adjustment_amount = Decimal(str(adjustment_amount_str))
                    adjusted_amount = Decimal(str(adjusted_amount_str))
                    
                    # 金額計算の整合性確認
                    calculated_adjusted = original_amount + adjustment_amount
                    assert abs(calculated_adjusted - adjusted_amount) < Decimal('0.01'), \
                        f"Amount calculation mismatch: {original_amount} + {adjustment_amount} != {adjusted_amount}"
                    
                except (InvalidOperation, ValueError) as e:
                    assert False, f"Invalid amount format: {str(e)}"
                
                # 修正タイプの値検証
                adjustment_type = row.get("adjustment_type", "")
                valid_types = ["ERROR_CORRECTION", "ADDITIONAL_REVENUE", "ADDITIONAL_COST", 
                              "RECLASSIFICATION", "ACCRUAL_ADJUSTMENT", "OTHER"]
                assert adjustment_type in valid_types, f"Invalid adjustment_type: {adjustment_type}"
                
                # 承認者IDの存在確認
                approver_id = row.get("approver_id", "")
                assert approver_id and len(approver_id) > 0, "approver_id cannot be empty"
                
                # 事業セグメントの値検証
                business_segment = row.get("business_segment", "")
                valid_segments = ["GAS_RETAIL", "ELECTRICITY_RETAIL", "CORPORATE", 
                                "REAL_ESTATE", "OTHER"]
                assert business_segment in valid_segments, f"Invalid business_segment: {business_segment}"
                
                # 通貨コードの検証
                currency_code = row.get("currency_code", "")
                assert currency_code in ["JPY", "USD", "EUR"], f"Invalid currency_code: {currency_code}"
                
                # 為替レートの検証
                exchange_rate_str = row.get("exchange_rate", "1.0")
                try:
                    exchange_rate = float(exchange_rate_str)
                    assert exchange_rate > 0, f"Exchange rate must be positive: {exchange_rate}"
                except ValueError:
                    assert False, f"Invalid exchange_rate format: {exchange_rate_str}"
                
                # 修正日の形式検証
                adjustment_date = row.get("adjustment_date", "")
                if adjustment_date:
                    assert len(adjustment_date) >= 10, f"Invalid adjustment_date format: {adjustment_date}"
            
            logger.info(f"Data quality validation passed for {self.PIPELINE_NAME}")
            
        except Exception as e:
            logger.error(f"Data quality validation failed: {str(e)}")
            raise
    
    @pytest.mark.asyncio
    async def test_financial_integrity_validation(self, pipeline_helper):
        """財務整合性検証テスト"""
        logger = logging.getLogger(__name__)
        logger.info(f"Starting financial integrity validation for {self.PIPELINE_NAME}")
        
        try:
            run_id = await pipeline_helper.run_pipeline(self.PIPELINE_NAME)
            
            status = await pipeline_helper.wait_for_pipeline_completion(
                run_id, timeout_minutes=30
            )
            
            assert status == "Succeeded", f"Pipeline failed: {status}"
            
            # 出力ファイル取得
            output_files = await pipeline_helper.list_output_files(
                container_name="lim-settlement-breakdown-repair",
                date_path=datetime.now().strftime('%Y/%m/%d')
            )
            
            csv_content = await pipeline_helper.download_and_extract_csv(output_files[0])
            
            # 財務整合性チェック
            period_totals = {}
            adjustment_types_summary = {}
            
            for row in csv_content:
                settlement_period = row.get("settlement_period", "")
                original_amount = Decimal(str(row.get("original_amount", "0")))
                adjustment_amount = Decimal(str(row.get("adjustment_amount", "0")))
                adjusted_amount = Decimal(str(row.get("adjusted_amount", "0")))
                adjustment_type = row.get("adjustment_type", "")
                
                # 期間別集計
                if settlement_period not in period_totals:
                    period_totals[settlement_period] = {
                        "original_total": Decimal('0'),
                        "adjustment_total": Decimal('0'),
                        "adjusted_total": Decimal('0'),
                        "record_count": 0
                    }
                
                period_totals[settlement_period]["original_total"] += original_amount
                period_totals[settlement_period]["adjustment_total"] += adjustment_amount
                period_totals[settlement_period]["adjusted_total"] += adjusted_amount
                period_totals[settlement_period]["record_count"] += 1
                
                # 修正タイプ別集計
                if adjustment_type not in adjustment_types_summary:
                    adjustment_types_summary[adjustment_type] = {
                        "count": 0,
                        "total_adjustment": Decimal('0')
                    }
                
                adjustment_types_summary[adjustment_type]["count"] += 1
                adjustment_types_summary[adjustment_type]["total_adjustment"] += adjustment_amount
            
            # 期間別整合性確認
            for period, totals in period_totals.items():
                calculated_total = totals["original_total"] + totals["adjustment_total"]
                actual_total = totals["adjusted_total"]
                
                assert abs(calculated_total - actual_total) < Decimal('1.00'), \
                    f"Period {period} total mismatch: {calculated_total} vs {actual_total}"
                
                logger.info(f"Period {period}: {totals['record_count']} records, "
                          f"adjustment total: {totals['adjustment_total']}")
            
            # 修正タイプ別分析
            for adj_type, summary in adjustment_types_summary.items():
                logger.info(f"Adjustment type {adj_type}: {summary['count']} records, "
                          f"total: {summary['total_adjustment']}")
            
            logger.info(f"Financial integrity validation passed for {self.PIPELINE_NAME}")
            
        except Exception as e:
            logger.error(f"Financial integrity validation failed: {str(e)}")
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
                remote_path="/lim_settlement_breakdown_repair",
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
    async def test_quarterly_settlement_scenarios(self, pipeline_helper):
        """四半期決算シナリオテスト"""
        logger = logging.getLogger(__name__)
        logger.info(f"Starting quarterly settlement scenarios for {self.PIPELINE_NAME}")
        
        quarterly_scenarios = [
            {
                "name": "q1_settlement",
                "params": {"settlement_period": "2024Q1", "quarter": "Q1"},
                "expected_months": ["01", "02", "03"]
            },
            {
                "name": "q2_settlement",
                "params": {"settlement_period": "2024Q2", "quarter": "Q2"},
                "expected_months": ["04", "05", "06"]
            },
            {
                "name": "q3_settlement",
                "params": {"settlement_period": "2024Q3", "quarter": "Q3"},
                "expected_months": ["07", "08", "09"]
            },
            {
                "name": "q4_settlement",
                "params": {"settlement_period": "2024Q4", "quarter": "Q4"},
                "expected_months": ["10", "11", "12"]
            }
        ]
        
        for scenario in quarterly_scenarios:
            try:
                logger.info(f"Testing quarterly scenario: {scenario['name']}")
                
                run_id = await pipeline_helper.run_pipeline(
                    self.PIPELINE_NAME,
                    parameters=scenario["params"]
                )
                
                status = await pipeline_helper.wait_for_pipeline_completion(
                    run_id, timeout_minutes=30
                )
                
                assert status == "Succeeded", f"Quarterly scenario {scenario['name']} failed: {status}"
                
                # 四半期固有データの検証
                output_files = await pipeline_helper.list_output_files(
                    container_name="lim-settlement-breakdown-repair",
                    date_path=datetime.now().strftime('%Y/%m/%d')
                )
                
                csv_content = await pipeline_helper.download_and_extract_csv(output_files[0])
                
                # 四半期データの検証
                for row in csv_content:
                    settlement_period = row.get("settlement_period", "")
                    assert scenario["params"]["settlement_period"] in settlement_period, \
                        f"Settlement period mismatch: expected {scenario['params']['settlement_period']}"
                
                logger.info(f"Quarterly scenario {scenario['name']} passed")
                
            except Exception as e:
                logger.error(f"Quarterly scenario test failed for {scenario['name']}: {str(e)}")
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
                "name": "invalid_amount_format",
                "params": {"inject_invalid_amounts": True},
                "expected_status": "Failed"
            },
            {
                "name": "missing_approver",
                "params": {"inject_missing_approver": True},
                "expected_status": "Failed"
            },
            {
                "name": "calculation_mismatch",
                "params": {"inject_calculation_errors": True},
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
                    container_name="lim-settlement-breakdown-repair",
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


if __name__ == "__main__":
    # テスト実行のためのエントリーポイント
    pytest.main([__file__, "-v", "--tb=short"])
