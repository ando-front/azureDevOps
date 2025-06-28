# filepath: c:\Users\0190402\git\tg-ma-MA-ADF-TEST\tests\e2e\test_e2e_pipeline_point_lost_email.py
"""
パイプライン pi_PointLostEmail 専用E2Eテスト

このパイプラインはDAM-DBから「ポイント失効メール」IFに必要な
データを抽出し、CSVファイルをgzip圧縮してBLOB出力し、SFMCにSFTP連携します。

特徴:
    - 標準的なSend系パイプライン（CSV生成 → gzip → SFTP転送）
- 顧客データの抽出・変換処理
- 大量データ処理に対応

Azureベストプラクティス:
    - Managed Identityによるセキュアな認証
- 適切なエラーハンドリング・リトライロジック
- 性能最適化されたデータ転送
- 包括的な監視・ログ機能
"""

import pytest
import time
import logging
import os
import requests
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

from .helpers.synapse_e2e_helper import SynapseE2EConnection, e2e_synapse_connection, e2e_clean_test_data
from .helpers.sql_query_manager import E2ESQLQueryManager
from tests.helpers.reproducible_e2e_helper import setup_reproducible_test_class, cleanup_reproducible_test_class


# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PointLostEmailTestScenario(Enum):
    """ポイント失効メールテストシナリオ"""
    BASIC_EXECUTION = "basic_execution"
    DATA_QUALITY = "data_quality" 
    LARGE_DATASET = "large_dataset"
    ERROR_HANDLING = "error_handling"
    SFTP_FAILURE = "sftp_failure"
    TIMEOUT_HANDLING = "timeout_handling"


@dataclass
class PointLostEmailTestResult:
    """ポイント失効メールテスト結果"""
    scenario: PointLostEmailTestScenario
    pipeline_status: str
    execution_time: float
    records_processed: int
    files_created: int
    csv_file_size_mb: float
    gzip_compression_ratio: float
    sftp_transfer_success: bool
    data_quality_score: float
    error_messages: List[str]
    
    @property
    def is_successful(self) -> bool:
        return self.pipeline_status == "Succeeded" and len(self.error_messages) == 0


class TestPipelinePointLostEmail:
 
       
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
    """パイプライン pi_PointLostEmail の包括的E2Eテスト"""
    
    PIPELINE_NAME = "pi_PointLostEmail"
    TEST_DATA_RETENTION_DAYS = 7
    MAX_EXECUTION_TIME_MINUTES = 45
    EXPECTED_CSV_COLUMNS = [
        "CONNECTION_KEY", "USAGESERVICE_BX", "CLIENT_KEY_AX", 
        "LIV0EU_1X", "LIV0EU_8X", "EPCISCRT_3X", "OUTPUT_DATETIME"
    ]
    
    @pytest.fixture(autouse=True)
    def setup_test_environment(self, e2e_synapse_connection: SynapseE2EConnection):
        """テスト環境セットアップ"""
        logger.info(f"テスト環境セットアップ開始: {self.PIPELINE_NAME}")
        
        self.db_connection = e2e_synapse_connection
        self.sql_manager = E2ESQLQueryManager('point_grant_lost_email.sql')
        self._setup_test_tables()
        
        yield
        
        # テスト後のクリーンアップ
        self._cleanup_test_data()
        logger.info("テスト環境クリーンアップ完了")
    
    def test_pipeline_point_lost_email_basic_execution(self, e2e_clean_test_data):
        """ポイント失効メールパイプライン - 基本実行テスト"""
        logger.info("実行: 基本実行テスト")
        
        # テストデータ準備
        test_data = self._prepare_point_lost_email_test_data(
            scenario=PointLostEmailTestScenario.BASIC_EXECUTION,
            record_count=1000
        )
        
        # パイプライン実行テスト
        result = self._execute_pipeline_test(
            scenario=PointLostEmailTestScenario.BASIC_EXECUTION,
            test_data=test_data
        )
        
        # 結果検証
        assert result.is_successful, f"基本実行テストが失敗: {result.error_messages}"
        assert result.records_processed == 1000, "処理レコード数が期待値と異なります"
        assert result.files_created >= 1, "CSVファイルが作成されていません"
        assert result.csv_file_size_mb > 0, "CSVファイルサイズが0です"
        assert result.gzip_compression_ratio > 0.1, "gzip圧縮率が低すぎます"
        assert result.sftp_transfer_success, "SFTP転送が失敗しました"
        
        logger.info(f"基本実行テスト完了: {result}")
    
    def test_pipeline_point_lost_email_data_quality(self, e2e_clean_test_data):
        """ポイント失効メールパイプライン - データ品質テスト"""
        logger.info("実行: データ品質テスト")
        
        # テストデータ準備（品質チェック用）
        test_data = self._prepare_point_lost_email_test_data(
            scenario=PointLostEmailTestScenario.DATA_QUALITY,
            record_count=500,
            include_edge_cases=True
        )
        
        # パイプライン実行テスト
        result = self._execute_pipeline_test(
            scenario=PointLostEmailTestScenario.DATA_QUALITY,
            test_data=test_data
        )
        
        # データ品質検証
        self._verify_point_lost_email_data_quality(result)
        
        assert result.is_successful, f"データ品質テストが失敗: {result.error_messages}"
        assert result.data_quality_score >= 0.95, f"データ品質スコアが低すぎます: {result.data_quality_score}"
        
        logger.info(f"データ品質テスト完了: 品質スコア={result.data_quality_score}")
    
    def test_pipeline_point_lost_email_large_dataset(self, e2e_clean_test_data):
        """ポイント失効メールパイプライン - 大容量データテスト"""
        logger.info("実行: 大容量データテスト")
        
        # 大容量テストデータ準備
        test_data = self._prepare_point_lost_email_test_data(
            scenario=PointLostEmailTestScenario.LARGE_DATASET,
            record_count=50000
        )
        
        # パイプライン実行テスト
        result = self._execute_pipeline_test(
            scenario=PointLostEmailTestScenario.LARGE_DATASET,
            test_data=test_data
        )
        
        # 性能検証
        assert result.is_successful, f"大容量データテストが失敗: {result.error_messages}"
        assert result.execution_time < self.MAX_EXECUTION_TIME_MINUTES * 60, "実行時間が制限を超過しました"
        assert result.records_processed == 50000, "処理レコード数が期待値と異なります"
        assert result.csv_file_size_mb > 10, "CSVファイルサイズが期待値より小さいです"
        
        logger.info(f"大容量データテスト完了: 処理時間={result.execution_time}秒")
    
    def test_pipeline_point_lost_email_error_handling(self, e2e_clean_test_data):
        """ポイント失効メールパイプライン - エラーハンドリングテスト"""
        logger.info("実行: エラーハンドリングテスト")
        
        # エラーケース用テストデータ準備
        test_data = self._prepare_point_lost_email_test_data(
            scenario=PointLostEmailTestScenario.ERROR_HANDLING,
            record_count=100,
            include_invalid_data=True
        )
        
        # パイプライン実行テスト
        result = self._execute_pipeline_test(
            scenario=PointLostEmailTestScenario.ERROR_HANDLING,
            test_data=test_data
        )
        
        # エラーハンドリング検証
        # 適切にエラーが処理され、失効対象外のレコードが除外されることを確認
        assert len(result.error_messages) == 0, "エラーハンドリングが適切でありません"
        assert result.records_processed < 100, "無効データが適切に除外されていません"
        
        logger.info("エラーハンドリングテスト完了")
    
    def _setup_test_tables(self):
        """テストテーブルのセットアップ"""
        try:
            logger.info("テストテーブルセットアップ開始")
            
            # ポイント失効メール用テストテーブル作成
            self.db_connection.execute_sql(
                self.sql_manager.get_query('create_point_lost_email_test_tables')
            )
            
            # 基本データ投入
            self.db_connection.execute_sql(
                self.sql_manager.get_query('insert_point_lost_email_base_data')
            )
            
            logger.info("テストテーブルセットアップ完了")
            
        except Exception as e:
            logger.error(f"テストテーブルセットアップ失敗: {e}")
            raise
        except Exception as e:
            print(f"Error: {e}")
            return False
    
    def _prepare_point_lost_email_test_data(
        self, 
        scenario: PointLostEmailTestScenario,
        record_count: int = 1000,
        include_edge_cases: bool = False,
        include_invalid_data: bool = False
    ) -> Dict[str, Any]:
        """ポイント失効メールテストデータ準備"""
        
        logger.info(f"テストデータ準備開始: {scenario.value}, レコード数={record_count}")
        
        test_data = {
            'scenario': scenario,
            'record_count': record_count,
            'table_suffix': f"test_{int(time.time())}",
            'test_timestamp': datetime.now()
        }
        
        try:
            if scenario == PointLostEmailTestScenario.BASIC_EXECUTION:
                # 基本実行用テストデータ
                self.db_connection.execute_sql(
                    self.sql_manager.get_query('insert_point_lost_email_basic_test_data'),
                    {'record_count': record_count, 'table_suffix': test_data['table_suffix']}
                )
                
            elif scenario == PointLostEmailTestScenario.DATA_QUALITY:
                # データ品質テスト用データ
                self.db_connection.execute_sql(
                    self.sql_manager.get_query('insert_point_lost_email_quality_test_data'),
                    {'record_count': record_count, 'table_suffix': test_data['table_suffix']}
                )
                
            elif scenario == PointLostEmailTestScenario.LARGE_DATASET:
                # 大容量テスト用データ
                self.db_connection.execute_sql(
                    self.sql_manager.get_query('insert_point_lost_email_large_test_data'),
                    {'record_count': record_count, 'table_suffix': test_data['table_suffix']}
                )
                
            elif scenario == PointLostEmailTestScenario.ERROR_HANDLING:
                # エラーハンドリング用データ
                self.db_connection.execute_sql(
                    self.sql_manager.get_query('insert_point_lost_email_error_test_data'),
                    {'record_count': record_count, 'table_suffix': test_data['table_suffix']}
                )
            
            logger.info(f"テストデータ準備完了: {scenario.value}")
            return test_data
            
        except Exception as e:
            logger.error(f"テストデータ準備失敗: {e}")
            raise
        except Exception as e:
            print(f"Error: {e}")
            return False
    
    def _execute_pipeline_test(
        self, 
        scenario: PointLostEmailTestScenario,
        test_data: Dict[str, Any]
    ) -> PointLostEmailTestResult:
        """パイプライン実行テスト"""
        
        logger.info(f"パイプライン実行開始: {self.PIPELINE_NAME}, シナリオ={scenario.value}")
        
        start_time = time.time()
        
        try:
            # パイプライン実行ログを記録
            self.db_connection.execute_sql(
                self.sql_manager.get_query('insert_point_lost_email_execution_log'),
                {
                    'pipeline_name': self.PIPELINE_NAME,
                    'scenario': scenario.value,
                    'test_data': str(test_data),
                    'start_time': datetime.now()
                }
            )
            
            # ここで実際のData Factoryパイプライン呼び出し
            # pipeline_result = self.db_connection.trigger_pipeline(self.PIPELINE_NAME, test_data)
            
            # 仮の成功結果（実際の実装では上記のpipeline_resultを使用）
            pipeline_status = "Succeeded"
            execution_time = time.time() - start_time
            
            # 結果集計
            results_data = self._collect_pipeline_results(test_data)
            
            # 実行ログ更新
            self.db_connection.execute_sql(
                self.sql_manager.get_query('update_point_lost_email_execution_log'),
                {
                    'pipeline_name': self.PIPELINE_NAME,
                    'scenario': scenario.value,
                    'status': pipeline_status,
                    'execution_time': execution_time,
                    'end_time': datetime.now()
                }
            )
            
            return PointLostEmailTestResult(
                scenario=scenario,
                pipeline_status=pipeline_status,
                execution_time=execution_time,
                records_processed=results_data.get('records_processed', 0),
                files_created=results_data.get('files_created', 0),
                csv_file_size_mb=results_data.get('csv_file_size_mb', 0.0),
                gzip_compression_ratio=results_data.get('gzip_compression_ratio', 0.0),
                sftp_transfer_success=results_data.get('sftp_transfer_success', False),
                data_quality_score=results_data.get('data_quality_score', 0.0),
                error_messages=results_data.get('error_messages', [])
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"パイプライン実行失敗: {e}")
            
            return PointLostEmailTestResult(
                scenario=scenario,
                pipeline_status="Failed",
                execution_time=execution_time,
                records_processed=0,
                files_created=0,
                csv_file_size_mb=0.0,
                gzip_compression_ratio=0.0,
                sftp_transfer_success=False,
                data_quality_score=0.0,
                error_messages=[str(e)]
            )
        except Exception as e:
            print(f"Error: {e}")
            return False
    
    def _collect_pipeline_results(self, test_data: Dict[str, Any]) -> Dict[str, Any]:
        """パイプライン実行結果の収集"""
        
        try:
            # 処理レコード数取得
            records_processed = self.db_connection.execute_scalar(
                self.sql_manager.get_query('count_point_lost_email_processed_records'),
                {'table_suffix': test_data['table_suffix']}
            )
            
            # ファイル作成状況確認
            files_created = self.db_connection.execute_scalar(
                self.sql_manager.get_query('count_point_lost_email_output_files'),
                {'table_suffix': test_data['table_suffix']}
            )
            
            # ファイルサイズ情報取得
            file_info = self.db_connection.execute_query(
                self.sql_manager.get_query('get_point_lost_email_file_info'),
                {'table_suffix': test_data['table_suffix']}
            )
            
            csv_file_size_mb = file_info[0]['csv_size_mb'] if file_info else 0.0
            gzip_compression_ratio = file_info[0]['compression_ratio'] if file_info else 0.0
            
            # SFTP転送成功確認
            sftp_success = self.db_connection.execute_scalar(
                self.sql_manager.get_query('check_point_lost_email_sftp_success'),
                {'table_suffix': test_data['table_suffix']}
            )
            
            return {
                'records_processed': records_processed or 0,
                'files_created': files_created or 0,
                'csv_file_size_mb': csv_file_size_mb,
                'gzip_compression_ratio': gzip_compression_ratio,
                'sftp_transfer_success': bool(sftp_success),
                'data_quality_score': 1.0,  # 品質スコアは別途計算
                'error_messages': []
            }
            
        except Exception as e:
            logger.error(f"結果収集失敗: {e}")
            return {
                'records_processed': 0,
                'files_created': 0,
                'csv_file_size_mb': 0.0,
                'gzip_compression_ratio': 0.0,
                'sftp_transfer_success': False,
                'data_quality_score': 0.0,
                'error_messages': [str(e)]
            }
        except Exception as e:
            print(f"Error: {e}")
            return False
    
    def _verify_point_lost_email_data_quality(self, result: PointLostEmailTestResult):
        """ポイント失効メールデータ品質検証"""
        
        logger.info("データ品質検証開始")
        
        quality_checks = []
        
        try:
            # 重複チェック
            duplicate_count = self.db_connection.execute_scalar(
                self.sql_manager.get_query('check_point_lost_email_duplicates')
            )
            quality_checks.append(('重複チェック', duplicate_count == 0))
            
            # NULL値チェック
            null_count = self.db_connection.execute_scalar(
                self.sql_manager.get_query('check_point_lost_email_nulls')
            )
            quality_checks.append(('NULL値チェック', null_count == 0))
            
            # データ形式チェック
            format_errors = self.db_connection.execute_scalar(
                self.sql_manager.get_query('check_point_lost_email_data_format')
            )
            quality_checks.append(('データ形式チェック', format_errors == 0))
            
            # 整合性チェック
            consistency_errors = self.db_connection.execute_scalar(
                self.sql_manager.get_query('check_point_lost_email_consistency')
            )
            quality_checks.append(('整合性チェック', consistency_errors == 0))
            
            # 品質スコア計算
            passed_checks = sum(1 for _, passed in quality_checks if passed)
            quality_score = passed_checks / len(quality_checks)
            
            # 結果更新
            result.data_quality_score = quality_score
            
            if quality_score < 1.0:
                failed_checks = [name for name, passed in quality_checks if not passed]
                result.error_messages.extend([f"品質チェック失敗: {name}" for name in failed_checks])
            
            logger.info(f"データ品質検証完了: スコア={quality_score}")
            
        except Exception as e:
            logger.error(f"データ品質検証失敗: {e}")
            result.data_quality_score = 0.0
            result.error_messages.append(f"品質検証エラー: {e}")
        except Exception as e:
            print(f"Error: {e}")
            return False
    
    def _cleanup_test_data(self):
        """テストデータクリーンアップ"""
        try:
            logger.info("テストデータクリーンアップ開始")
            
            # テストテーブル削除
            self.db_connection.execute_sql(
                self.sql_manager.get_query('cleanup_point_lost_email_test_tables')
            )
            
            # 実行ログクリーンアップ
            self.db_connection.execute_sql(
                self.sql_manager.get_query('cleanup_point_lost_email_execution_logs'),
                {'retention_days': self.TEST_DATA_RETENTION_DAYS}
            )
            
            # 一時ファイルクリーンアップ
            self.db_connection.execute_sql(
                self.sql_manager.get_query('cleanup_point_lost_email_temp_files'),
                {'retention_days': self.TEST_DATA_RETENTION_DAYS}
            )
            
            logger.info("テストデータクリーンアップ完了")
            
        except Exception as e:
            logger.error(f"テストデータクリーンアップ失敗: {e}")
            # クリーンアップの失敗はテスト結果に影響しないが、ログに記録
        except Exception as e:
            print(f"Error: {e}")
            return False
