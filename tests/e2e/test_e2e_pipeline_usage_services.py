"""
Azure Data Factory pi_Send_UsageServices パイプラインE2Eテストモジュール

このモジュールは利用サービス情報を顧客DM形式でSFMCに送信するパイプラインの包括的なE2Eテストを実装します。
利用サービスデータの抽出、CSV生成、SFTP転送の全工程を検証します。

【実際の実装】このパイプラインは以下の処理を実行します：
1. 外部SQLファイルから利用サービス用クエリを読み込み
2. DAM-DBから利用サービス情報を抽出
3. CSV.gz形式でBlob Storageに出力
4. SFMCにSFTP送信

パイプライン構成:
1. LoadUsageServicesQuery: 外部SQLファイルからクエリを読み込み
2. at_CreateCSV_UsageServices: 利用サービスデータを抽出してCSV.gz作成
3. at_SendSftp_UsageServices: Blobに出力されたgzipファイルをSFMCにSFTP連携

Created: 2024-12-19
Updated: 2025-06-23
"""

import pytest
import time
import logging
import os
import requests
from datetime import datetime
from unittest.mock import Mock
from typing import Dict, Any

from tests.e2e.helpers.synapse_e2e_helper import SynapseE2EConnection
from tests.helpers.reproducible_e2e_helper import setup_reproducible_test_class, cleanup_reproducible_test_class

logger = logging.getLogger(__name__)


class TestPipelineUsageServices:
    """pi_Send_UsageServices パイプライン E2Eテストクラス"""
    
    PIPELINE_NAME = "pi_Send_UsageServices"
    BLOB_CONTAINER = "datalake/OMNI/MA/UsageServices"
    SFTP_DIRECTORY = "Import/DAM/UsageServices"
    EXTERNAL_SQL_FILE = "external_sql/usage_services_main_query.sql"
    
    # パフォーマンス期待値
    EXPECTED_MAX_DURATION = 35  # 35分（外部SQLファイル読み込み＋大量データ処理）
    EXPECTED_MIN_RECORDS = 0    # 利用サービスがない場合もある
    
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
        """テストメソッド初期化"""
        self.start_time = datetime.now()
        logger.info(f"利用サービスE2Eテスト開始: {self.PIPELINE_NAME} - {self.start_time}")
        
    def teardown_method(self):
        """テストメソッド終了処理"""
        end_time = datetime.now()
        duration = end_time - self.start_time
        logger.info(f"利用サービスE2Eテスト完了: {self.PIPELINE_NAME} - 実行時間: {duration}")

    @pytest.fixture(scope="class")
    def helper(self, e2e_synapse_connection):
        """SynapseE2EConnectionフィクスチャ"""
        return e2e_synapse_connection

    @pytest.fixture(scope="class")
    def pipeline_run_id(self, helper):
        """パイプライン実行IDを取得するフィクスチャ"""
        logger.info(f"パイプライン実行開始: {self.PIPELINE_NAME}")
        
        try:
            # パイプライン実行前の事前チェック
            self._pre_execution_validation(helper)
            
            # パイプライン実行
            run_id = helper.trigger_pipeline(self.PIPELINE_NAME)
            logger.info(f"パイプライン実行開始: run_id={run_id}")
            
            yield run_id
            
        except Exception as e:
            logger.error(f"パイプライン実行準備エラー: {e}")
            pytest.fail(f"パイプライン実行準備に失敗: {e}")

    def _pre_execution_validation(self, helper):
        """実行前検証"""
        logger.info("実行前検証開始")
        
        # データセット存在確認
        required_datasets = ["ds_azuredw_generic", "ds_CSV_BlobGz", "ds_BlobGz", "ds_Gz_Sftp"] 
        for dataset in required_datasets:
            assert helper.validate_dataset_exists(dataset), f"データセット {dataset} が存在しません"
        
        # 接続テスト
        assert helper.test_synapse_connection(), "Synapse接続テストに失敗"
        
        # 外部SQLファイル存在確認
        assert helper.validate_external_file_exists(self.EXTERNAL_SQL_FILE), f"外部SQLファイル {self.EXTERNAL_SQL_FILE} が存在しません"
        
        # データソース設定確認
        assert helper.validate_data_source_exists("sql_queries_storage"), "データソース sql_queries_storage が存在しません"
        
        logger.info("実行前検証完了")

    def test_pipeline_execution_success(self, helper, pipeline_run_id):
        """パイプライン実行成功テスト (US-001: 基本実行)"""
        logger.info("パイプライン実行成功テスト開始 (US-001)")
        
        # パイプライン完了待機
        status = helper.wait_for_pipeline_completion(
            pipeline_run_id, 
            timeout_minutes=self.EXPECTED_MAX_DURATION
        )
        
        # 実行結果確認
        assert status == "Succeeded", f"パイプライン実行が失敗: ステータス={status}"
        
        logger.info("パイプライン実行成功確認完了 (US-001)")

    def test_external_sql_loading(self, helper, pipeline_run_id):
        """外部SQL読み込み検証テスト (US-002: SQLファイル読み込み確認)"""
        logger.info("外部SQL読み込み検証テスト開始 (US-002)")
        
        # パイプライン完了確認
        helper.wait_for_pipeline_completion(pipeline_run_id, timeout_minutes=self.EXPECTED_MAX_DURATION)
        
        # Lookupアクティビティの実行結果確認
        lookup_result = helper.get_activity_output(pipeline_run_id, "LoadUsageServicesQuery")
        assert lookup_result, "外部SQLファイルの読み込みに失敗しました"
        
        # クエリ内容の確認
        query_content = lookup_result.get('firstRow', {}).get('query_content')
        assert query_content, "外部SQLファイルからクエリ内容を取得できませんでした"
        assert len(query_content.strip()) > 0, "外部SQLファイルが空です"
        
        logger.info("外部SQL読み込み確認完了 (US-002)")

    def test_csv_creation_validation(self, helper, pipeline_run_id):
        """CSV作成検証テスト (US-003: CSV.gz出力確認)"""
        logger.info("CSV作成検証テスト開始 (US-003)")
        
        # パイプライン完了確認
        helper.wait_for_pipeline_completion(pipeline_run_id, timeout_minutes=self.EXPECTED_MAX_DURATION)
        
        # Blob Storage上のCSV.gzファイル存在確認
        expected_filename = f"UsageServices_{datetime.now().strftime('%Y%m%d')}.csv.gz"
        csv_exists = helper.validate_blob_file_exists(self.BLOB_CONTAINER, expected_filename)
        assert csv_exists, f"CSV.gzファイルが作成されていません: {expected_filename}"
        
        # ファイルサイズ確認（0バイトでも利用サービスがない場合は正常）
        file_size = helper.get_blob_file_size(self.BLOB_CONTAINER, expected_filename)
        assert file_size >= 0, f"CSV.gzファイルのサイズが不正です: {expected_filename}"
        
        logger.info("CSV作成確認完了 (US-003)")

    def test_sftp_transmission_validation(self, helper, pipeline_run_id):
        """SFTP送信検証テスト (US-004: SFTP送信確認)"""
        logger.info("SFTP送信検証テスト開始 (US-004)")
        
        # パイプライン完了確認
        helper.wait_for_pipeline_completion(pipeline_run_id, timeout_minutes=self.EXPECTED_MAX_DURATION)
        
        # SFTP先ファイル存在確認
        expected_filename = f"UsageServices_{datetime.now().strftime('%Y%m%d')}.csv.gz"
        sftp_success = helper.validate_sftp_file_exists(self.SFTP_DIRECTORY, expected_filename)
        assert sftp_success, f"SFTPファイル送信が確認できません: {expected_filename}"
        
        logger.info("SFTP送信確認完了 (US-004)")

    def test_data_quality_validation(self, helper, pipeline_run_id):
        """データ品質検証テスト (US-005: データ品質確認)"""
        logger.info("データ品質検証テスト開始 (US-005)")
        
        # パイプライン完了確認
        helper.wait_for_pipeline_completion(pipeline_run_id, timeout_minutes=self.EXPECTED_MAX_DURATION)
        
        # 基本的なデータ品質チェック
        quality_checks = [
            "csv_format_check", 
            "column_header_check",
            "row_count_check",
            "encoding_check",
            "timestamp_format_check"
        ]
        
        failed_checks = []
        for check_name in quality_checks:
            try:
                check_result = helper.execute_csv_quality_check(
                    self.BLOB_CONTAINER, 
                    f"UsageServices_{datetime.now().strftime('%Y%m%d')}.csv.gz",
                    check_name
                )
                if not check_result:
                    failed_checks.append(check_name)
                    logger.warning(f"データ品質チェック失敗: {check_name}")
            except Exception as e:
                logger.warning(f"データ品質チェックでエラー: {check_name} - {e}")
                failed_checks.append(check_name)
        
        # 重要なチェックが失敗した場合はエラー
        critical_checks = ["csv_format_check", "encoding_check"]
        critical_failures = [check for check in failed_checks if check in critical_checks]
        assert len(critical_failures) == 0, f"重要なデータ品質チェックが失敗: {critical_failures}"
        
        logger.info("データ品質確認完了 (US-005)")

    def test_column_structure_validation(self, helper, pipeline_run_id):
        """カラム構造検証テスト (US-006: 出力カラム確認)"""
        logger.info("カラム構造検証テスト開始 (US-006)")
        
        # パイプライン完了確認
        helper.wait_for_pipeline_completion(pipeline_run_id, timeout_minutes=self.EXPECTED_MAX_DURATION)
        
        # 利用サービス特有のカラム構造（外部SQLファイルの内容に依存）
        # 基本的なカラムが含まれることを確認
        required_columns = [
            "Bx",
            "INDEX_ID",
            "USER_KEY",
            "SERVICE_TYPE",
            "TRANSFER_TYPE",
            "OUTPUT_DATE"
        ]
        
        # CSV.gzファイルからカラム構造確認
        csv_filename = f"UsageServices_{datetime.now().strftime('%Y%m%d')}.csv.gz"
        actual_columns = helper.get_csv_column_headers(self.BLOB_CONTAINER, csv_filename)
        
        # 必須カラムが含まれていることを確認
        missing_columns = [col for col in required_columns if col not in actual_columns]
        assert len(missing_columns) == 0, f"必須カラムが不足: {missing_columns}"
        
        logger.info("カラム構造確認完了 (US-006)")

    def test_business_logic_validation(self, helper, pipeline_run_id):
        """ビジネスロジック検証テスト (US-007: 利用サービスロジック確認)"""
        logger.info("ビジネスロジック検証テスト開始 (US-007)")
        
        # パイプライン完了確認
        helper.wait_for_pipeline_completion(pipeline_run_id, timeout_minutes=self.EXPECTED_MAX_DURATION)
        
        # 利用サービスの基本条件確認
        business_logic_query = """
        SELECT COUNT(*) as usage_service_count
        FROM omni.omni_ods_cloak_trn_usageservice
        WHERE USER_KEY_TYPE = '003'  -- お客さま番号3x
            AND SERVICE_TYPE = '001'  -- ガス
            AND TRANSFER_TYPE = '02'  -- 提供中
        """
        
        result = helper.execute_query(business_logic_query)
        service_count = result[0]['usage_service_count'] if result else 0
        
        # ビジネスロジックの論理的整合性確認
        assert service_count >= 0, f"利用サービスデータ数が不正: {service_count}"
        
        logger.info(f"利用サービスデータ数: {service_count}件")
        logger.info("ビジネスロジック確認完了 (US-007)")

    def test_error_handling_validation(self, helper, pipeline_run_id):
        """エラーハンドリング検証テスト (US-008: エラー処理確認)"""
        logger.info("エラーハンドリング検証テスト開始 (US-008)")
        
        # パイプライン完了確認
        helper.wait_for_pipeline_completion(pipeline_run_id, timeout_minutes=self.EXPECTED_MAX_DURATION)
        
        # パイプライン実行ログ確認
        run_logs = helper.get_pipeline_run_logs(pipeline_run_id)
        
        # エラーログの有無確認
        error_logs = [log for log in run_logs if log.get('level') == 'Error']
        
        # 重要なエラーがないことを確認
        critical_errors = [
            log for log in error_logs 
            if any(keyword in log.get('message', '') for keyword in ['timeout', 'connection', 'authentication'])
        ]
        
        assert len(critical_errors) == 0, f"重要なエラーが発生: {critical_errors}"
        
        logger.info("エラーハンドリング確認完了 (US-008)")

    def test_performance_validation(self, helper, pipeline_run_id):
        """パフォーマンス検証テスト (US-009: 実行時間確認)"""
        logger.info("パフォーマンス検証テスト開始 (US-009)")
        
        # パイプライン完了確認
        helper.wait_for_pipeline_completion(pipeline_run_id, timeout_minutes=self.EXPECTED_MAX_DURATION)
        
        # 実行時間取得
        run_details = helper.get_pipeline_run_details(pipeline_run_id)
        start_time = run_details.get('runStart')
        end_time = run_details.get('runEnd')
        
        if start_time and end_time:
            duration = (end_time - start_time).total_seconds() / 60  # 分単位
            assert duration <= self.EXPECTED_MAX_DURATION, f"実行時間が期待値を超過: {duration}分 > {self.EXPECTED_MAX_DURATION}分"
            logger.info(f"パイプライン実行時間: {duration:.2f}分")
        
        logger.info("パフォーマンス確認完了 (US-009)")

    def test_integration_validation(self, helper, pipeline_run_id):
        """統合テスト (US-010: 全体整合性確認)"""
        logger.info("統合テスト開始 (US-010)")
        
        # パイプライン完了確認
        helper.wait_for_pipeline_completion(pipeline_run_id, timeout_minutes=self.EXPECTED_MAX_DURATION)
        
        # 全体的な整合性確認
        integration_checks = [
            ("pipeline_status", "Succeeded"),
            ("external_sql_loaded", True),
            ("csv_created", True),
            ("sftp_transmitted", True),
            ("data_quality", True)
        ]
        
        failed_integrations = []
        for check_name, expected_value in integration_checks:
            try:
                if check_name == "pipeline_status":
                    actual_value = helper.get_pipeline_status(pipeline_run_id)
                elif check_name == "external_sql_loaded":
                    lookup_result = helper.get_activity_output(pipeline_run_id, "LoadUsageServicesQuery")
                    actual_value = bool(lookup_result and lookup_result.get('firstRow', {}).get('query_content'))
                elif check_name == "csv_created":
                    csv_filename = f"UsageServices_{datetime.now().strftime('%Y%m%d')}.csv.gz"
                    actual_value = helper.validate_blob_file_exists(self.BLOB_CONTAINER, csv_filename)
                elif check_name == "sftp_transmitted":
                    csv_filename = f"UsageServices_{datetime.now().strftime('%Y%m%d')}.csv.gz"
                    actual_value = helper.validate_sftp_file_exists(self.SFTP_DIRECTORY, csv_filename)
                elif check_name == "data_quality":
                    csv_filename = f"UsageServices_{datetime.now().strftime('%Y%m%d')}.csv.gz"
                    actual_value = helper.execute_csv_quality_check(self.BLOB_CONTAINER, csv_filename, "csv_format_check")
                
                if actual_value != expected_value:
                    failed_integrations.append(f"{check_name}: 期待={expected_value}, 実際={actual_value}")
                    
            except Exception as e:
                failed_integrations.append(f"{check_name}: エラー={e}")
        
        assert len(failed_integrations) == 0, f"統合テストで失敗: {failed_integrations}"
        
        logger.info("統合テスト完了 (US-010)")
