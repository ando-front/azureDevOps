"""
E2Eテスト: pi_Send_Cpkiyk パイプライン（本人特定契約・CSV・SFTP送信）

このテストは、pi_Send_Cpkiyk パイプラインの実際の機能を検証します。
パイプラインは以下の処理を実行します：
1. 本人特定契約テーブルから契約データを抽出  
2. CSV.gz形式でBlob Storageに出力
3. SFMCにSFTP送信

【実際の実装】このパイプラインは単純なデータ転送処理のみを行います：
- at_CreateCSV_Cpkiyk: SQLクエリ実行 → CSV.gz生成（19列）
- at_SendSftp_Cpkiyk: Blob Storage → SFTP転送

【19列構造】プロダクション仕様に基づく契約データ：
- MTGID: 会員ID
- EDA_NO: 枝番
- GMT_SET_NO: ガスメータ設置場所番号
- SYOKY_NO: 使用契約番号
- CST_REG_NO: お客さま登録番号
- SHRKY_NO: 支払契約番号
- HJ_CST_NAME: 表示名称
- YUSEN_JNJ_NO: 優先順位
- TKTIYMD: 特定年月日
- TRKSBTCD: 種別コード
- CST_NO: 表示用お客さま番号
- INTRA_TRK_ID: イントラ登録ID
- SND_UM_CD: ホスト送信有無コード
- TRK_SBT_CD: 登録種別コード
- REC_REG_YMD: レコード登録年月日
- REC_REG_JKK: レコード登録時刻
- REC_UPD_YMD: レコード更新年月日
- REC_UPD_JKK: レコード更新時刻
- TAIKAI_FLAG: 退会フラグ
- OUTPUT_DATETIME: 出力日時

テスト内容（実装に基づく10の必要十分なケース）：
- パイプライン実行成功確認
- CSV.gz作成検証
- SFTP送信確認
- データ品質チェック
- 19列構造検証
- パフォーマンス検証
- エラーハンドリング
- セキュリティ検証
- データ整合性確認
- ファイル形式検証
"""

import pytest
import time
import logging
import os
import requests
from datetime import datetime
from unittest.mock import Mock
from typing import Dict, Any, List

from tests.e2e.helpers.synapse_e2e_helper import SynapseE2EConnection
from tests.helpers.reproducible_e2e_helper import setup_reproducible_test_class, cleanup_reproducible_test_class
# from tests.conftest import azure_data_factory_client


logger = logging.getLogger(__name__)


class TestPipelineCpkiyk:
    """pi_Send_Cpkiyk パイプライン 本人特定契約・CSV・SFTP送信E2Eテストクラス"""
    
    PIPELINE_NAME = "pi_Send_Cpkiyk"
    SOURCE_TABLE_NAME = "omni_ods_mytginfo_trn_cpkiyk"
    BLOB_CONTAINER = "datalake/OMNI/MA/Cpkiyk"
    SFTP_DIRECTORY = "Import/DAM/Cpkiyk"
    SCHEMA_NAME = "omni"
    
    # パフォーマンス期待値
    EXPECTED_MAX_DURATION = 20  # 20分
    EXPECTED_MIN_RECORDS = 10   # 最小レコード数
    EXPECTED_COLUMN_COUNT = 19  # 期待される列数（実際のプロダクション構造）
    
    # 19列の重要なカラム（プロダクションコードに基づく）
    CRITICAL_COLUMNS = [
        "MTGID",           # 会員ID
        "EDA_NO",          # 枝番
        "GMT_SET_NO",      # ガスメータ設置場所番号
        "SYOKY_NO",        # 使用契約番号
        "CST_REG_NO",      # お客さま登録番号
        "SHRKY_NO",        # 支払契約番号
        "HJ_CST_NAME",     # 表示名称
        "YUSEN_JNJ_NO",    # 優先順位
        "TKTIYMD",         # 特定年月日
        "TRKSBTCD",        # 種別コード
        "CST_NO",          # 表示用お客さま番号
        "INTRA_TRK_ID",    # イントラ登録ID
        "SND_UM_CD",       # ホスト送信有無コード
        "TRK_SBT_CD",      # 登録種別コード
        "REC_REG_YMD",     # レコード登録年月日
        "REC_REG_JKK",     # レコード登録時刻
        "REC_UPD_YMD",     # レコード更新年月日
        "REC_UPD_JKK",     # レコード更新時刻
        "TAIKAI_FLAG",     # 退会フラグ
        "OUTPUT_DATETIME"  # 出力日時
    ]

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
        logger.info(f"本人特定契約・CSV・SFTP送信E2Eテスト開始: {self.PIPELINE_NAME} - {self.start_time}")
        
    def teardown_method(self):
        """テストメソッド終了処理"""
        end_time = datetime.now()
        duration = end_time - self.start_time
        logger.info(f"本人特定契約・CSV・SFTP送信E2Eテスト完了: {self.PIPELINE_NAME} - 実行時間: {duration}")

    @pytest.fixture(scope="class")
    def helper(self, e2e_synapse_connection):
        """SynapseE2EConnectionフィクスチャ"""
        return e2e_synapse_connection
    
    @pytest.fixture(scope="class")
    def pipeline_run_id(self, helper):
        """パイプライン実行IDを取得するフィクスチャ"""
        logger.info(f"パイプライン実行開始: {self.PIPELINE_NAME}")
        
        try:
            # 本人特定契約・CSV・SFTP送信テストデータのセットアップ
            logger.info("本人特定契約・CSV・SFTP送信テストデータセットアップ開始")
            setup_success = helper.setup_cpkiyk_csv_sftp_test_data()
            assert setup_success, "本人特定契約・CSV・SFTP送信テストデータのセットアップに失敗"
            
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
        required_datasets = ["ds_DamDwhTable_shir", "ds_CSV_BlobGz", "ds_BlobGz", "ds_Gz_Sftp"] 
        for dataset in required_datasets:
            assert helper.validate_dataset_exists(dataset), f"データセット {dataset} が存在しません"
          # ソーステーブル存在確認
        source_table_exists = helper.validate_table_exists(self.SCHEMA_NAME, self.SOURCE_TABLE_NAME)
        assert source_table_exists, f"ソーステーブル {self.SCHEMA_NAME}.{self.SOURCE_TABLE_NAME} が存在しません"
        
        # 接続テスト
        assert helper.test_synapse_connection(), "Synapse接続テストに失敗"
        
        logger.info("実行前検証完了")

    def test_pipeline_execution_success(self, helper, pipeline_run_id):
        """パイプライン実行成功テスト (CP-001: 基本実行)"""
        logger.info("パイプライン実行成功テスト開始 (CP-001)")
        
        # パイプライン完了待機
        status = helper.wait_for_pipeline_completion(
            pipeline_run_id, 
            timeout_minutes=self.EXPECTED_MAX_DURATION
        )
        
        # 実行結果確認
        assert status == "Succeeded", f"パイプライン実行が失敗: ステータス={status}"
        
        logger.info("パイプライン実行成功確認完了 (CP-001)")

    def test_csv_creation_validation(self, helper, pipeline_run_id):
        """CSV作成検証テスト (CP-002: CSV.gz出力確認)"""
        logger.info("CSV作成検証テスト開始 (CP-002)")
        
        # パイプライン完了確認
        helper.wait_for_pipeline_completion(pipeline_run_id, timeout_minutes=self.EXPECTED_MAX_DURATION)
        
        # Blob Storage上のCSV.gzファイル存在確認
        expected_filename = f"Cpkiyk_{datetime.now().strftime('%Y%m%d')}.csv.gz"
        csv_exists = helper.validate_blob_file_exists(self.BLOB_CONTAINER, expected_filename)
        assert csv_exists, f"CSV.gzファイルが作成されていません: {expected_filename}"
        
        # ファイルサイズ確認（空でないことを確認）
        file_size = helper.get_blob_file_size(self.BLOB_CONTAINER, expected_filename)
        assert file_size > 0, f"CSV.gzファイルが空です: {expected_filename}"
        
        logger.info("CSV作成確認完了 (CP-002)")

    def test_sftp_transmission_validation(self, helper, pipeline_run_id):
        """SFTP送信検証テスト (CP-003: SFTP送信確認)"""
        logger.info("SFTP送信検証テスト開始 (CP-003)")
        
        # パイプライン完了確認
        helper.wait_for_pipeline_completion(pipeline_run_id, timeout_minutes=self.EXPECTED_MAX_DURATION)
        
        # SFTP先ファイル存在確認
        expected_filename = f"Cpkiyk_{datetime.now().strftime('%Y%m%d')}.csv.gz"
        sftp_success = helper.validate_sftp_file_exists(self.SFTP_DIRECTORY, expected_filename)
        assert sftp_success, f"SFTPファイル送信が確認できません: {expected_filename}"
        
        logger.info("SFTP送信確認完了 (CP-003)")

    def test_data_quality_validation(self, helper, pipeline_run_id):
        """データ品質検証テスト (CP-004: データ品質確認)"""
        logger.info("データ品質検証テスト開始 (CP-004)")
        
        # パイプライン完了確認
        helper.wait_for_pipeline_completion(pipeline_run_id, timeout_minutes=self.EXPECTED_MAX_DURATION)
        
        # 基本的なデータ品質チェック
        quality_checks = [
            "csv_format_check", 
            "column_header_check",
            "row_count_check",
            "encoding_check"
        ]
        
        failed_checks = []
        for check_name in quality_checks:
            check_result = helper.execute_csv_quality_check(
                self.BLOB_CONTAINER, 
                f"Cpkiyk_{datetime.now().strftime('%Y%m%d')}.csv.gz",
                check_name
            )
            if not check_result:
                failed_checks.append(check_name)
                logger.warning(f"データ品質チェック失敗: {check_name}")
        
        # 重要なチェックが失敗した場合はエラー
        critical_failures = [check for check in failed_checks if "format" in check or "encoding" in check]
        assert len(critical_failures) == 0, f"重要なデータ品質チェックに失敗: {critical_failures}"
        
        logger.info("データ品質検証完了 (CP-004)")

    def test_19_column_structure_validation(self, helper, pipeline_run_id):
        """19列構造検証テスト (CP-005: 19列構造確認)"""
        logger.info("19列構造検証テスト開始 (CP-005)")
        
        # パイプライン完了確認
        helper.wait_for_pipeline_completion(pipeline_run_id, timeout_minutes=self.EXPECTED_MAX_DURATION)
        
        # CSV列構造確認
        expected_filename = f"Cpkiyk_{datetime.now().strftime('%Y%m%d')}.csv.gz"
        
        column_info = helper.get_csv_column_info(self.BLOB_CONTAINER, expected_filename)
        
        # 期待される19列の確認
        assert len(column_info) == self.EXPECTED_COLUMN_COUNT, \
            f"列数が期待値と異なります: 実際={len(column_info)}, 期待={self.EXPECTED_COLUMN_COUNT}"
        
        # 重要な列の存在確認
        missing_columns = []
        for critical_column in self.CRITICAL_COLUMNS:
            if critical_column not in column_info:
                missing_columns.append(critical_column)
        
        assert len(missing_columns) == 0, f"重要な列が不足: {missing_columns}"
        
        # ヘッダー行の存在確認
        has_header = helper.validate_csv_has_header(self.BLOB_CONTAINER, expected_filename)
        assert has_header, "CSVファイルにヘッダー行が存在しません"
        
        logger.info(f"19列構造検証完了: {len(column_info)}列確認 (CP-005)")

    def test_performance_validation(self, helper, pipeline_run_id):
        """パフォーマンス検証テスト (CP-006: パフォーマンス確認)"""
        logger.info("パフォーマンス検証テスト開始 (CP-006)")
        
        # パイプライン完了確認
        start_time = time.time()
        status = helper.wait_for_pipeline_completion(
            pipeline_run_id, 
            timeout_minutes=self.EXPECTED_MAX_DURATION
        )
        end_time = time.time()
        
        execution_time = end_time - start_time
        
        # 実行時間検証
        max_execution_time = self.EXPECTED_MAX_DURATION * 60  # 分 → 秒
        assert execution_time <= max_execution_time, \
            f"実行時間が期待値を超過: {execution_time}秒 > {max_execution_time}秒"
        
        logger.info(f"パフォーマンス検証完了 (CP-006): 実行時間={execution_time}秒")

    def test_error_handling_validation(self, helper):
        """エラーハンドリング検証テスト (CP-007: エラーハンドリング確認)"""
        logger.info("エラーハンドリング検証テスト開始 (CP-007)")
        
        # SFTP接続エラーのシミュレーション
        error_test_result = helper.test_pipeline_error_handling(
            self.PIPELINE_NAME,
            error_type="sftp_connection"
        )
        
        # エラーが適切にハンドリングされることを確認
        assert error_test_result.get("error_handled", False), \
            "SFTP接続エラーが適切にハンドリングされていません"
        
        logger.info("エラーハンドリング検証完了 (CP-007)")

    def test_security_validation(self, helper, pipeline_run_id):
        """セキュリティ検証テスト (CP-008: セキュリティ確認)"""
        logger.info("セキュリティ検証テスト開始 (CP-008)")
        
        # パイプライン完了確認
        helper.wait_for_pipeline_completion(pipeline_run_id, timeout_minutes=self.EXPECTED_MAX_DURATION)
        
        # SFTP接続のセキュリティ確認
        sftp_security_check = helper.validate_sftp_security(self.SFTP_DIRECTORY)
        assert sftp_security_check, "SFTP接続のセキュリティ検証に失敗"
        
        # CSV.gzファイルの暗号化確認
        expected_filename = f"Cpkiyk_{datetime.now().strftime('%Y%m%d')}.csv.gz"
        file_security_check = helper.validate_file_security(self.BLOB_CONTAINER, expected_filename)
        assert file_security_check, "ファイルセキュリティ検証に失敗"
        
        logger.info("セキュリティ検証完了 (CP-008)")

    def test_data_consistency_validation(self, helper, pipeline_run_id):
        """データ整合性検証テスト (CP-009: データ整合性確認)"""
        logger.info("データ整合性検証テスト開始 (CP-009)")
        
        # パイプライン完了確認
        helper.wait_for_pipeline_completion(pipeline_run_id, timeout_minutes=self.EXPECTED_MAX_DURATION)
        
        # CSV.gzファイルの行数確認
        expected_filename = f"Cpkiyk_{datetime.now().strftime('%Y%m%d')}.csv.gz"
        csv_count = helper.get_csv_row_count(self.BLOB_CONTAINER, expected_filename)
        
        # 最小レコード数確認
        assert csv_count >= self.EXPECTED_MIN_RECORDS, \
            f"CSV出力のレコード数が不足: {csv_count} < {self.EXPECTED_MIN_RECORDS}"
        
        logger.info(f"データ整合性検証完了 (CP-009): csv={csv_count}行")

    def test_file_format_validation(self, helper, pipeline_run_id):
        """ファイル形式検証テスト (CP-010: ファイル形式確認)"""
        logger.info("ファイル形式検証テスト開始 (CP-010)")
        
        # パイプライン完了確認
        helper.wait_for_pipeline_completion(pipeline_run_id, timeout_minutes=self.EXPECTED_MAX_DURATION)
        
        expected_filename = f"Cpkiyk_{datetime.now().strftime('%Y%m%d')}.csv.gz"
        
        # gzip形式の確認
        is_gzip = helper.validate_file_is_gzip(self.BLOB_CONTAINER, expected_filename)
        assert is_gzip, f"ファイルがgzip形式ではありません: {expected_filename}"
        
        # CSV形式の確認（解凍後）
        is_csv = helper.validate_file_is_csv(self.BLOB_CONTAINER, expected_filename)
        assert is_csv, f"解凍後のファイルがCSV形式ではありません: {expected_filename}"
        
        logger.info("ファイル形式検証完了 (CP-010)")
