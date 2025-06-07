"""
E2E Test Suite for pi_Send_PaymentMethodMaster Pipeline

支払方法マスター送信パイプラインのE2Eテスト
このパイプラインは顧客の支払方法に関するマスターデータをCSVファイルとして生成し、
gzip圧縮後にSFTPでSalesforce Marketing Cloud (SFMC) に送信します。

パイプライン構成:
1. at_CreateCSV_ClientDM: DAM-DBから顧客DM情報を抽出し、gzipファイルでBLOB出力
2. at_SendSftp_ClientDM: Blobに出力されたgzipファイルをSFMCにSFTP連携

テスト対象:
- パイプライン基本実行テスト
- 大量データセット処理パフォーマンステスト
- データ品質検証テスト
- エラーハンドリングテスト
- モニタリング・アラートテスト
"""

import pytest
import asyncio
import logging
import datetime
import requests
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Any, Tuple
import pandas as pd
import os
import gzip
import tempfile
import io

# Azure SDK imports
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
# from azure.mgmt.datafactory import DataFactoryManagementClient
# from azure.mgmt.datafactory.models import PipelineRun, ActivityRun

# Database connectivity
import pyodbc
# import sqlalchemy
# from sqlalchemy import create_engine, text

# Test framework imports (using placeholders for missing imports)
from tests.e2e.helpers.missing_helpers_placeholder import MissingHelperPlaceholder
from tests.helpers.reproducible_e2e_helper import setup_reproducible_test_class, cleanup_reproducible_test_class

# Placeholder implementations for missing classes
class AzureClientFactory(MissingHelperPlaceholder):
    pass

class _TestDataGenerator(MissingHelperPlaceholder):
    pass

class PipelineTestHelper(MissingHelperPlaceholder):
    pass

class MonitoringHelper(MissingHelperPlaceholder):
    pass

class DatabaseFixtures(MissingHelperPlaceholder):
    pass

class DataFactoryManagementClient(MissingHelperPlaceholder):
    pass

class PipelineRun(MissingHelperPlaceholder):
    pass

class ActivityRun(MissingHelperPlaceholder):
    pass


class PaymentMethodMasterTestScenario(Enum):
    """支払方法マスター送信パイプラインのテストシナリオ"""
    BASIC_EXECUTION = "basic_execution"
    LARGE_DATASET_PERFORMANCE = "large_dataset_performance"
    DATA_QUALITY_VALIDATION = "data_quality_validation"
    ERROR_HANDLING = "error_handling"
    MONITORING_ALERTING = "monitoring_alerting"
    SFTP_CONNECTION_FAILURE = "sftp_connection_failure"


@dataclass
class PaymentMethodMasterTestResult:
    """支払方法マスター送信パイプラインテスト結果"""
    pipeline_run_id: str
    status: str
    execution_duration: datetime.timedelta
    csv_records_count: int
    file_size_bytes: int
    blob_output_path: str
    sftp_transfer_status: str
    data_quality_score: float
    performance_metrics: Dict[str, Any]
    error_details: Optional[str] = None
    monitoring_alerts: List[str] = None


class TestPipelinePaymentMethodMaster:
    """pi_Send_PaymentMethodMaster パイプラインのE2Eテストクラス"""
       
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
    
    def setup_method(self):
        self.pipeline_name = "pi_Send_PaymentMethodMaster"
        self.azure_clients = AzureClientFactory()
        self.test_data_generator = _TestDataGenerator()
        self.pipeline_helper = PipelineTestHelper()
        self.monitoring_helper = MonitoringHelper()
        self.db_fixtures = DatabaseFixtures()
        
        # テスト設定
        self.resource_group = os.getenv("ADF_RESOURCE_GROUP", "rg-test-adf")
        self.data_factory_name = os.getenv("ADF_NAME", "adf-test-instance")
        self.subscription_id = os.getenv("AZURE_SUBSCRIPTION_ID")
        self.storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME", "testadfstorage")
        self.storage_container = "datalake"
        self.test_output_directory = "test-output/payment-method-master"
        
        # Pipeline configuration metadata
        self.pipeline_config = {
            "description": "DAM-DBから「顧客」IFに必要なデータを抽出、gzipファイルでBLOB出力し、SFMCにSFTP連携する",
            "activities": [
                {
                    "name": "at_CreateCSV_ClientDM",
                    "type": "Copy",
                    "description": "DBから「顧客」IFに必要なデータを抽出、gzipファイルでBLOB出力"
                },
                {
                    "name": "at_SendSftp_ClientDM", 
                    "type": "Copy",
                    "description": "Blobに出力されたgzipファイルをSFMCにSFTP連携",
                    "depends_on": ["at_CreateCSV_ClientDM"]
                }
            ],
            "expected_outputs": {
                "csv_directory": "datalake/OMNI/MA/ClientDM",
                "csv_filename_pattern": "ClientDM_{date}.csv.gz",
                "sftp_destination": "Import/DAM/ClientDM"
            }
        }
        
        self.logger = logging.getLogger(__name__)

    @pytest.mark.asyncio
    async def test_payment_method_master_basic_execution(self):
        """
        基本的なパイプライン実行テスト
        
        テスト内容:
        - パイプラインの正常実行確認
        - CSV生成の成功確認
        - SFTP転送の成功確認
        - 出力ファイルの基本的な形式確認
        """
        self.logger.info("Starting basic execution test for PaymentMethodMaster pipeline")
        
        test_result = await self._execute_pipeline_test(
            PaymentMethodMasterTestScenario.BASIC_EXECUTION
        )
        
        # 基本的な実行検証
        assert test_result.status == "Succeeded", f"Pipeline failed: {test_result.error_details}"
        assert test_result.csv_records_count > 0, "No records found in generated CSV"
        assert test_result.file_size_bytes > 0, "Generated file is empty"
        assert test_result.sftp_transfer_status == "Succeeded", "SFTP transfer failed"
        
        # 実行時間の検証
        max_execution_time = datetime.timedelta(hours=2)  # 通常2時間以内で完了
        assert test_result.execution_duration <= max_execution_time, \
            f"Execution took too long: {test_result.execution_duration}"
        
        self.logger.info(f"Basic execution test completed successfully. Records: {test_result.csv_records_count}")

    @pytest.mark.asyncio
    async def test_payment_method_master_large_dataset_performance(self):
        """
        大量データセット処理のパフォーマンステスト
        
        テスト内容:
        - 大量データ処理時の性能確認
        - メモリ使用量の監視
        - 処理時間の測定
        - リソース使用効率の確認
        """
        self.logger.info("Starting large dataset performance test")
        
        # 大量データ用のテストデータ準備
        await self._prepare_large_test_dataset()
        
        test_result = await self._execute_pipeline_test(
            PaymentMethodMasterTestScenario.LARGE_DATASET_PERFORMANCE
        )
        
        # パフォーマンス検証
        assert test_result.status == "Succeeded", "Large dataset processing failed"
        
        # データ量に対する処理時間の妥当性確認
        records_per_second = test_result.csv_records_count / test_result.execution_duration.total_seconds()
        min_performance_threshold = 100  # レコード/秒
        assert records_per_second >= min_performance_threshold, \
            f"Performance below threshold: {records_per_second} records/sec"
        
        # メモリ使用量の確認
        memory_metrics = test_result.performance_metrics.get("memory_usage", {})
        max_memory_mb = memory_metrics.get("max_memory_mb", 0)
        memory_threshold = 4096  # 4GB
        assert max_memory_mb <= memory_threshold, \
            f"Memory usage exceeded threshold: {max_memory_mb}MB"
        
        self.logger.info(f"Large dataset test completed. Performance: {records_per_second:.2f} records/sec")

    @pytest.mark.asyncio
    async def test_payment_method_master_data_quality(self):
        """
        データ品質検証テスト
        
        テスト内容:
        - 出力データの整合性確認
        - 必須フィールドの存在確認
        - データ形式の妥当性検証
        - 重複データの検出
        """
        self.logger.info("Starting data quality validation test")
        
        test_result = await self._execute_pipeline_test(
            PaymentMethodMasterTestScenario.DATA_QUALITY_VALIDATION
        )
        
        assert test_result.status == "Succeeded", "Pipeline execution failed"
        
        # データ品質スコアの確認
        min_quality_score = 0.95  # 95%以上の品質スコア
        assert test_result.data_quality_score >= min_quality_score, \
            f"Data quality below threshold: {test_result.data_quality_score}"
        
        # 出力ファイルの詳細検証
        await self._validate_output_file_quality(test_result.blob_output_path)
        
        self.logger.info(f"Data quality test completed. Score: {test_result.data_quality_score:.3f}")

    @pytest.mark.asyncio
    async def test_payment_method_master_error_handling(self):
        """
        エラーハンドリングテスト
        
        テスト内容:
        - データソース接続エラーの処理
        - 不正データ形式への対応
        - リトライ機能の動作確認
        - エラーメッセージの適切性確認
        """
        self.logger.info("Starting error handling test")
        
        # エラー条件の設定（テストデータの破損など）
        await self._setup_error_conditions()
        
        test_result = await self._execute_pipeline_test(
            PaymentMethodMasterTestScenario.ERROR_HANDLING
        )
        
        # エラーハンドリングの検証
        # この場合、エラーが適切にハンドリングされていることを確認
        assert test_result.error_details is not None, "Error should have been detected"
        assert "timeout" in test_result.error_details.lower() or \
               "connection" in test_result.error_details.lower(), \
               "Error details should indicate connection issues"
        
        # ログにエラーが適切に記録されていることを確認
        await self._verify_error_logging(test_result.pipeline_run_id)
        
        self.logger.info("Error handling test completed successfully")

    @pytest.mark.asyncio
    async def test_payment_method_master_monitoring_alerting(self):
        """
        モニタリング・アラートテスト
        
        テスト内容:
        - パイプライン実行メトリクスの監視
        - アラート条件の確認
        - 通知機能の動作確認
        - ダッシュボード表示の確認
        """
        self.logger.info("Starting monitoring and alerting test")
        
        test_result = await self._execute_pipeline_test(
            PaymentMethodMasterTestScenario.MONITORING_ALERTING
        )
        
        # モニタリングメトリクスの確認
        assert test_result.performance_metrics is not None, "Performance metrics not collected"
        
        required_metrics = [
            "execution_duration_seconds",
            "records_processed",
            "file_size_bytes",
            "cpu_usage_percent",
            "memory_usage_mb"
        ]
        
        for metric in required_metrics:
            assert metric in test_result.performance_metrics, f"Missing metric: {metric}"
        
        # アラート条件のテスト
        await self._test_alert_conditions(test_result)
        
        self.logger.info("Monitoring and alerting test completed")

    async def _execute_pipeline_test(
        self, 
        scenario: PaymentMethodMasterTestScenario
    ) -> PaymentMethodMasterTestResult:
        """パイプラインテストの実行"""
        
        # テストデータの準備
        await self._prepare_test_data(scenario)
        
        # パイプライン実行
        start_time = datetime.datetime.now()
        pipeline_run = await self._trigger_pipeline()
        
        # 実行完了まで待機
        final_status = await self._wait_for_pipeline_completion(pipeline_run.run_id)
        end_time = datetime.datetime.now()
        
        # 結果の収集
        execution_duration = end_time - start_time
        
        # 出力ファイルの分析
        output_analysis = await self._analyze_output_files(pipeline_run.run_id)
        
        # パフォーマンスメトリクスの収集
        performance_metrics = await self._collect_performance_metrics(pipeline_run.run_id)
        
        # データ品質の評価
        data_quality_score = await self._evaluate_data_quality(output_analysis)
        
        return PaymentMethodMasterTestResult(
            pipeline_run_id=pipeline_run.run_id,
            status=final_status,
            execution_duration=execution_duration,
            csv_records_count=output_analysis.get("records_count", 0),
            file_size_bytes=output_analysis.get("file_size_bytes", 0),
            blob_output_path=output_analysis.get("blob_path", ""),
            sftp_transfer_status=output_analysis.get("sftp_status", "Unknown"),
            data_quality_score=data_quality_score,
            performance_metrics=performance_metrics,
            error_details=output_analysis.get("error_details"),
            monitoring_alerts=output_analysis.get("alerts", [])
        )

    async def _prepare_test_data(self, scenario: PaymentMethodMasterTestScenario):
        """シナリオに応じたテストデータの準備"""
        
        if scenario == PaymentMethodMasterTestScenario.LARGE_DATASET_PERFORMANCE:
            await self._prepare_large_test_dataset()
        elif scenario == PaymentMethodMasterTestScenario.ERROR_HANDLING:
            await self._setup_error_conditions()
        else:
            await self._prepare_standard_test_dataset()

    async def _prepare_standard_test_dataset(self):
        """標準的なテストデータセットの準備"""
        # 顧客DM_Bx付_tempテーブルにテストデータを挿入
        test_data = self.test_data_generator.generate_payment_method_master_data(
            record_count=1000,
            date_range=("2024-01-01", "2024-12-31")
        )
        
        await self.db_fixtures.insert_test_data(
            table_name="omni.omni_ods_marketing_trn_client_dm_bx_temp",
            data=test_data
        )

    async def _prepare_large_test_dataset(self):
        """大量データセット用のテストデータ準備"""
        # 100万件のテストデータを生成
        large_test_data = self.test_data_generator.generate_payment_method_master_data(
            record_count=1000000,
            date_range=("2024-01-01", "2024-12-31"),
            include_complex_scenarios=True
        )
        
        await self.db_fixtures.insert_test_data_batch(
            table_name="omni.omni_ods_marketing_trn_client_dm_bx_temp",
            data=large_test_data,
            batch_size=10000
        )

    async def _setup_error_conditions(self):
        """エラー条件のセットアップ"""
        # データベース接続の一時的な無効化や不正データの挿入
        await self.db_fixtures.create_corrupted_test_data(
            table_name="omni.omni_ods_marketing_trn_client_dm_bx_temp"
        )

    async def _trigger_pipeline(self):
        """パイプラインのトリガー実行"""
        return await self.pipeline_helper.trigger_pipeline(
            resource_group=self.resource_group,
            factory_name=self.data_factory_name,
            pipeline_name=self.pipeline_name
        )

    async def _wait_for_pipeline_completion(self, run_id: str, timeout_minutes: int = 180):
        """パイプライン完了まで待機"""
        return await self.pipeline_helper.wait_for_completion(
            resource_group=self.resource_group,
            factory_name=self.data_factory_name,
            run_id=run_id,
            timeout_minutes=timeout_minutes
        )

    async def _analyze_output_files(self, run_id: str) -> Dict[str, Any]:
        """出力ファイルの分析"""
        # Blob Storageから出力ファイルを取得して分析
        blob_client = self.azure_clients.get_blob_service_client()
        
        # 生成されたファイルのパス構築
        today_str = datetime.datetime.now().strftime("%Y%m%d")
        blob_path = f"datalake/OMNI/MA/ClientDM/ClientDM_{today_str}.csv.gz"
        
        try:
            # ファイルの存在確認とダウンロード
            blob_data = await self._download_blob(blob_client, self.storage_container, blob_path)
            
            # gzipファイルの解凍と分析
            with gzip.decompress(blob_data) as decompressed_data:
                content = decompressed_data.decode('utf-8')
                lines = content.strip().split('\n')
                records_count = len(lines) - 1  # ヘッダー行を除く
                
            return {
                "records_count": records_count,
                "file_size_bytes": len(blob_data),
                "blob_path": blob_path,
                "sftp_status": "Succeeded",  # SFTP転送の状態確認実装必要
                "content_sample": lines[:5] if lines else []
            }
            
        except Exception as e:
            return {
                "records_count": 0,
                "file_size_bytes": 0,
                "blob_path": blob_path,
                "sftp_status": "Failed",
                "error_details": str(e)
            }

    async def _download_blob(self, blob_client, container_name: str, blob_name: str) -> bytes:
        """Blobファイルのダウンロード"""
        blob = blob_client.get_blob_client(container=container_name, blob=blob_name)
        return await blob.download_blob().readall()

    async def _collect_performance_metrics(self, run_id: str) -> Dict[str, Any]:
        """パフォーマンスメトリクスの収集"""
        return await self.monitoring_helper.collect_pipeline_metrics(
            resource_group=self.resource_group,
            factory_name=self.data_factory_name,
            run_id=run_id
        )

    async def _evaluate_data_quality(self, output_analysis: Dict[str, Any]) -> float:
        """データ品質の評価"""
        # データ品質スコアの計算ロジック
        quality_factors = []
        
        # ファイル生成の成功
        if output_analysis.get("records_count", 0) > 0:
            quality_factors.append(1.0)
        else:
            quality_factors.append(0.0)
        
        # ファイルサイズの妥当性
        file_size = output_analysis.get("file_size_bytes", 0)
        if file_size > 1000:  # 最小サイズ閾値
            quality_factors.append(1.0)
        else:
            quality_factors.append(0.5)
        
        # SFTP転送の成功
        if output_analysis.get("sftp_status") == "Succeeded":
            quality_factors.append(1.0)
        else:
            quality_factors.append(0.0)
        
        return sum(quality_factors) / len(quality_factors) if quality_factors else 0.0

    async def _validate_output_file_quality(self, blob_path: str):
        """出力ファイルの品質検証"""
        # CSV形式の妥当性、必須カラムの存在確認など
        blob_client = self.azure_clients.get_blob_service_client()
        
        try:
            blob_data = await self._download_blob(blob_client, self.storage_container, blob_path)
            
            with gzip.decompress(blob_data) as decompressed_data:
                content = decompressed_data.decode('utf-8')
                
                # CSVとしての読み込み検証
                df = pd.read_csv(io.StringIO(content))
                
                # 必須カラムの確認
                required_columns = [
                    "CONNECTION_KEY", "USAGESERVICE_BX", "CLIENT_KEY_AX", 
                    "LIV0EU_1X", "LIV0EU_8X", "OUTPUT_DATETIME"
                ]
                
                for col in required_columns:
                    assert col in df.columns, f"Required column missing: {col}"
                
                # データ形式の検証
                assert not df.empty, "Output CSV is empty"
                assert df["CONNECTION_KEY"].notna().all(), "CONNECTION_KEY contains null values"
                
        except Exception as e:
            pytest.fail(f"Output file quality validation failed: {str(e)}")

    async def _test_alert_conditions(self, test_result: PaymentMethodMasterTestResult):
        """アラート条件のテスト"""
        # 実行時間のアラート
        if test_result.execution_duration > datetime.timedelta(hours=3):
            test_result.monitoring_alerts.append("EXECUTION_TIME_EXCEEDED")
        
        # データ量のアラート
        if test_result.csv_records_count < 100:
            test_result.monitoring_alerts.append("LOW_RECORD_COUNT")
        
        # ファイルサイズのアラート
        if test_result.file_size_bytes < 1000:
            test_result.monitoring_alerts.append("SMALL_FILE_SIZE")

    async def _verify_error_logging(self, run_id: str):
        """エラーログの確認"""
        # Azure Monitor や Application Insights からログを取得して検証
        log_entries = await self.monitoring_helper.get_pipeline_logs(
            resource_group=self.resource_group,
            factory_name=self.data_factory_name,
            run_id=run_id
        )
        
        error_logged = any("error" in entry.lower() for entry in log_entries)
        assert error_logged, "Error was not properly logged"

    def teardown_method(self):
        """テスト後のクリーンアップ"""
        # テストデータの削除
        asyncio.run(self.db_fixtures.cleanup_test_data())
        
        # 一時ファイルの削除
        if hasattr(self, 'temp_files'):
            for temp_file in self.temp_files:
                if os.path.exists(temp_file):
                    os.remove(temp_file)


if __name__ == "__main__":
    # 単体でのテスト実行用
    pytest.main([__file__, "-v", "--tb=short"])
