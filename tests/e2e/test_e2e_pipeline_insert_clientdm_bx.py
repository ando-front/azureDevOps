"""
E2Eテスト: pi_Insert_ClientDmBx パイプライン

このテストは、pi_Insert_ClientDmBx パイプラインの動作を独立して検証します。
パイプラインは以下の処理を実行します：
1. 利用サービスBx4x作業テーブルの更新（ガス契約）
2. 利用サービスBx3xSA_ID作業テーブルの更新（電気契約）
3. 顧客DMにBxを付与した作業テーブルの生成

テスト内容：
- パイプライン実行の成功確認
- データ品質チェック
- Bx付与ロジック検証
- パフォーマンス検証
"""

import pytest
import time
import logging
import os
import requests
from datetime import datetime
from typing import Dict, Any, List

from tests.e2e.helpers.synapse_e2e_helper import SynapseE2EConnection
# from tests.conftest import azure_data_factory_client  # このフィクスチャは存在しないためコメントアウト

logger = logging.getLogger(__name__)


class TestPipelineInsertClientDmBx:

    @classmethod
    def setup_class(cls):
        """Disable proxy settings for tests"""
        # Store and clear proxy environment variables
        for var in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY']:
            if var in os.environ:
                del os.environ[var]

    def _get_no_proxy_session(self):
        """Get a requests session with proxy disabled"""
        session = requests.Session()
        session.proxies = {'http': None, 'https': None}
        return session
    """pi_Insert_ClientDmBx パイプライン専用のE2Eテストクラス"""
    
    PIPELINE_NAME = "pi_Insert_ClientDmBx"
    
    # 関連テーブル
    TEMP_TABLES = {
        "usage_service_bx4x": "omni_ods_cloak_trn_usageservice_bx4x_temp",
        "usage_service_bx3x": "omni_ods_cloak_trn_usageservice_bx3xsaid_temp",
        "client_dm_bx": "omni_ods_marketing_trn_client_dm_bx_temp"
    }
    
    SOURCE_TABLES = {
        "client_dm": "omni_ods_marketing_trn_client_dm",
        "usage_service": "omni_ods_cloak_trn_usageservice"
    }
    
    SCHEMA_NAME = "omni"
    
    # パフォーマンス期待値
    EXPECTED_MAX_DURATION = 25  # 25分
    EXPECTED_MIN_RECORDS = 100  # 最小レコード数
    
    def setup_class(self):
        """テストクラス初期化"""
        self.start_time = datetime.now()
        logger.info(f"E2Eテスト開始: {self.PIPELINE_NAME} - {self.start_time}")
        
    def teardown_class(self):
        """テストクラス終了処理"""
        end_time = datetime.now()
        duration = end_time - self.start_time
        logger.info(f"E2Eテスト完了: {self.PIPELINE_NAME} - 実行時間: {duration}")    @pytest.fixture(scope="class")
    def helper(self, e2e_synapse_connection):
        """SynapseE2EConnectionフィクスチャ"""
        return e2e_synapse_connection

    @pytest.fixture(scope="class")
    def pipeline_run_id(self, helper):
        """パイプライン実行IDを取得するフィクスチャ"""
        logger.info(f"パイプライン実行開始: {self.PIPELINE_NAME}")
        
        try:
            # パイプライン実行前の事前チェック
            self._pre_execution_checks(helper)
            
            # パイプライン実行
            run_id = helper.trigger_pipeline(self.PIPELINE_NAME)
            logger.info(f"パイプライン実行ID: {run_id}")
            
            return run_id
            
        except Exception as e:
            logger.error(f"パイプライン実行開始に失敗: {str(e)}")
            pytest.fail(f"パイプライン実行開始に失敗: {str(e)}")

    def _pre_execution_checks(self, helper):
        """実行前チェック"""
        logger.info("実行前チェック開始")
        
        # データソース接続確認
        assert helper.test_synapse_connection(), "Synapse接続に失敗"
        
        # 必要なリンクサービス存在確認
        linked_services = ["li_dam_dwh"]
        for service in linked_services:
            assert helper.validate_linked_service_exists(service), \
                f"リンクサービス {service} が存在しません"
        
        # ソーステーブル存在確認
        for table_key, table_name in self.SOURCE_TABLES.items():
            assert helper.check_table_exists(self.SCHEMA_NAME, table_name), \
                f"ソーステーブル {table_name} が存在しません"
            logger.info(f"ソーステーブル確認完了: {table_name}")
        
        logger.info("実行前チェック完了")

    def test_pipeline_execution_success(self, helper, pipeline_run_id):
        """パイプライン実行成功テスト"""
        logger.info(f"パイプライン実行成功テスト開始: {pipeline_run_id}")
        
        # パイプライン実行完了まで待機
        status = helper.wait_for_pipeline_completion(
            pipeline_run_id, 
            timeout_minutes=self.EXPECTED_MAX_DURATION
        )
        
        # 実行結果確認
        assert status == "Succeeded", f"パイプライン実行が失敗: ステータス={status}"
        
        # 実行時間チェック
        execution_time = helper.get_pipeline_execution_time(pipeline_run_id)
        assert execution_time <= self.EXPECTED_MAX_DURATION * 60, \
            f"実行時間が期待値を超過: {execution_time}秒 > {self.EXPECTED_MAX_DURATION * 60}秒"
        
        logger.info(f"パイプライン実行成功確認完了: 実行時間={execution_time}秒")

    def test_bx_assignment_logic_validation(self, helper, pipeline_run_id):
        """Bx付与ロジック検証テスト"""
        logger.info("Bx付与ロジック検証テスト開始")
        
        # パイプライン完了確認
        helper.wait_for_pipeline_completion(pipeline_run_id, timeout_minutes=self.EXPECTED_MAX_DURATION)
        
        # 各段階の検証
        self._validate_bx4x_temp_table(helper)
        self._validate_bx3x_temp_table(helper)
        self._validate_client_dm_bx_logic(helper)
        
        logger.info("Bx付与ロジック検証テスト完了")

    def _validate_bx4x_temp_table(self, helper):
        """ガス契約Bx4x作業テーブル検証"""
        logger.info("ガス契約Bx4x作業テーブル検証開始")
        
        table_name = self.TEMP_TABLES["usage_service_bx4x"]
        
        # テーブル存在確認
        assert helper.check_table_exists(self.SCHEMA_NAME, table_name), \
            f"作業テーブル {table_name} が存在しません"
        
        # レコード数確認
        record_count = helper.get_table_row_count(self.SCHEMA_NAME, table_name)
        assert record_count >= self.EXPECTED_MIN_RECORDS, \
            f"Bx4xテーブルのレコード数が不足: {record_count} < {self.EXPECTED_MIN_RECORDS}"
        
        # 重複除去確認（Bx, KEY_4Xの組み合わせでユニーク）
        duplicate_query = f"""
        SELECT COUNT(*) as duplicate_count
        FROM (
            SELECT BX, KEY_4X, COUNT(*) as cnt
            FROM [{self.SCHEMA_NAME}].[{table_name}]
            WHERE BX IS NOT NULL AND KEY_4X IS NOT NULL
            GROUP BY BX, KEY_4X
            HAVING COUNT(*) > 1
        ) duplicates
        """
        
        duplicate_result = helper.execute_query(duplicate_query)
        duplicate_count = duplicate_result[0]["duplicate_count"]
        assert duplicate_count == 0, f"Bx4xテーブルに重複レコードが存在: {duplicate_count}件"
        
        # データ品質チェック：必須フィールド
        null_bx_count = helper.execute_query(f"""
            SELECT COUNT(*) as null_count
            FROM [{self.SCHEMA_NAME}].[{table_name}]
            WHERE BX IS NULL
        """)[0]["null_count"]
        
        assert null_bx_count == 0, f"BXがNULLのレコードが存在: {null_bx_count}件"
        
        logger.info(f"Bx4x作業テーブル検証完了: {record_count}件のレコード")

    def _validate_bx3x_temp_table(self, helper):
        """電気契約Bx3x作業テーブル検証"""
        logger.info("電気契約Bx3x作業テーブル検証開始")
        
        table_name = self.TEMP_TABLES["usage_service_bx3x"]
        
        # テーブル存在確認
        assert helper.check_table_exists(self.SCHEMA_NAME, table_name), \
            f"作業テーブル {table_name} が存在しません"
        
        # レコード数確認
        record_count = helper.get_table_row_count(self.SCHEMA_NAME, table_name)
        logger.info(f"Bx3xテーブルレコード数: {record_count}")
        
        # 重複除去確認（Bx, KEY_3X, KEY_SA_IDの組み合わせでユニーク）
        duplicate_query = f"""
        SELECT COUNT(*) as duplicate_count
        FROM (
            SELECT BX, KEY_3X, KEY_SA_ID, COUNT(*) as cnt
            FROM [{self.SCHEMA_NAME}].[{table_name}]
            WHERE BX IS NOT NULL AND KEY_3X IS NOT NULL AND KEY_SA_ID IS NOT NULL
            GROUP BY BX, KEY_3X, KEY_SA_ID
            HAVING COUNT(*) > 1
        ) duplicates
        """
        
        duplicate_result = helper.execute_query(duplicate_query)
        duplicate_count = duplicate_result[0]["duplicate_count"]
        assert duplicate_count == 0, f"Bx3xテーブルに重複レコードが存在: {duplicate_count}件"
        
        logger.info(f"Bx3x作業テーブル検証完了: {record_count}件のレコード")

    def _validate_client_dm_bx_logic(self, helper):
        """顧客DM-Bx付与ロジック検証"""
        logger.info("顧客DM-Bx付与ロジック検証開始")
        
        table_name = self.TEMP_TABLES["client_dm_bx"]
        
        # テーブル存在確認
        assert helper.check_table_exists(self.SCHEMA_NAME, table_name), \
            f"顧客DM-Bx作業テーブル {table_name} が存在しません"
        
        # レコード数確認
        record_count = helper.get_table_row_count(self.SCHEMA_NAME, table_name)
        assert record_count >= self.EXPECTED_MIN_RECORDS, \
            f"顧客DM-Bxテーブルのレコード数が不足: {record_count} < {self.EXPECTED_MIN_RECORDS}"
        
        # ガス契約ありレコード確認
        gas_contract_query = f"""
        SELECT COUNT(*) as gas_contract_count
        FROM [{self.SCHEMA_NAME}].[{table_name}]
        WHERE LIV0EU_4X IS NOT NULL
        """
        
        gas_contract_result = helper.execute_query(gas_contract_query)
        gas_contract_count = gas_contract_result[0]["gas_contract_count"]
        logger.info(f"ガス契約ありレコード数: {gas_contract_count}")
        
        # 電気契約単独レコード確認
        electric_only_query = f"""
        SELECT COUNT(*) as electric_only_count
        FROM [{self.SCHEMA_NAME}].[{table_name}]
        WHERE LIV0EU_4X IS NULL 
        AND (EPCISCRT_LIGHTING_SA_ID IS NOT NULL OR EPCISCRT_POWER_SA_ID IS NOT NULL)
        """
        
        electric_only_result = helper.execute_query(electric_only_query)
        electric_only_count = electric_only_result[0]["electric_only_count"]
        logger.info(f"電気契約単独レコード数: {electric_only_count}")
        
        # Bx付与率確認
        bx_assigned_query = f"""
        SELECT COUNT(*) as bx_assigned_count
        FROM [{self.SCHEMA_NAME}].[{table_name}]
        WHERE BX IS NOT NULL
        """
        
        bx_assigned_result = helper.execute_query(bx_assigned_query)
        bx_assigned_count = bx_assigned_result[0]["bx_assigned_count"]
        bx_assignment_rate = (bx_assigned_count / record_count) * 100 if record_count > 0 else 0
        
        logger.info(f"Bx付与率: {bx_assignment_rate:.2f}% ({bx_assigned_count}/{record_count})")
        
        # 最低50%以上のBx付与率を期待
        assert bx_assignment_rate >= 50.0, f"Bx付与率が低すぎます: {bx_assignment_rate:.2f}%"
        
        logger.info(f"顧客DM-Bx付与ロジック検証完了: {record_count}件のレコード")

    def test_data_consistency_validation(self, helper, pipeline_run_id):
        """データ整合性検証テスト"""
        logger.info("データ整合性検証テスト開始")
        
        # パイプライン完了確認
        helper.wait_for_pipeline_completion(pipeline_run_id, timeout_minutes=self.EXPECTED_MAX_DURATION)
        
        # ソーステーブルとの整合性確認
        self._validate_source_consistency(helper)
        
        # ビジネスルール整合性確認
        self._validate_business_rule_consistency(helper)
        
        logger.info("データ整合性検証テスト完了")

    def _validate_source_consistency(self, helper):
        """ソーステーブルとの整合性確認"""
        logger.info("ソーステーブル整合性確認開始")
        
        # 顧客DMソースとの比較
        source_client_count = helper.get_table_row_count(
            self.SCHEMA_NAME, 
            self.SOURCE_TABLES["client_dm"]
        )
        
        result_client_count = helper.get_table_row_count(
            self.SCHEMA_NAME, 
            self.TEMP_TABLES["client_dm_bx"]
        )
        
        # 結果テーブルのレコード数がソースより少ない場合は許容（条件フィルタのため）
        assert result_client_count <= source_client_count, \
            f"結果テーブルのレコード数がソースを超過: result={result_client_count}, source={source_client_count}"
        
        # 最低80%以上のカバレッジを期待
        coverage_rate = (result_client_count / source_client_count) * 100 if source_client_count > 0 else 0
        assert coverage_rate >= 80.0, \
            f"カバレッジ率が低すぎます: {coverage_rate:.2f}% ({result_client_count}/{source_client_count})"
        
        logger.info(f"ソーステーブル整合性確認完了: カバレッジ率={coverage_rate:.2f}%")

    def _validate_business_rule_consistency(self, helper):
        """ビジネスルール整合性確認"""
        logger.info("ビジネスルール整合性確認開始")
        
        # ルール1: ガス契約ありレコードは4Xベースでマッチング
        gas_mismatch_query = f"""
        SELECT COUNT(*) as mismatch_count
        FROM [{self.SCHEMA_NAME}].[{self.TEMP_TABLES["client_dm_bx"]}] cdm
        LEFT JOIN [{self.SCHEMA_NAME}].[{self.TEMP_TABLES["usage_service_bx4x"]}] bx4x
            ON cdm.LIV0EU_4X = bx4x.KEY_4X
        WHERE cdm.LIV0EU_4X IS NOT NULL
        AND cdm.BX IS NOT NULL
        AND bx4x.BX IS NULL
        """
        
        gas_mismatch_result = helper.execute_query(gas_mismatch_query)
        gas_mismatch_count = gas_mismatch_result[0]["mismatch_count"]
        assert gas_mismatch_count == 0, \
            f"ガス契約Bxマッチングの不整合: {gas_mismatch_count}件"
        
        # ルール2: 電気契約単独レコードは3X+SA_IDベースでマッチング
        electric_mismatch_query = f"""
        SELECT COUNT(*) as mismatch_count
        FROM [{self.SCHEMA_NAME}].[{self.TEMP_TABLES["client_dm_bx"]}] cdm
        LEFT JOIN [{self.SCHEMA_NAME}].[{self.TEMP_TABLES["usage_service_bx3x"]}] bx3x
            ON cdm.EPCISCRT_3X = bx3x.KEY_3X
            AND ISNULL(cdm.EPCISCRT_LIGHTING_SA_ID, cdm.EPCISCRT_POWER_SA_ID) = bx3x.KEY_SA_ID
        WHERE cdm.LIV0EU_4X IS NULL
        AND (cdm.EPCISCRT_LIGHTING_SA_ID IS NOT NULL OR cdm.EPCISCRT_POWER_SA_ID IS NOT NULL)
        AND cdm.BX IS NOT NULL
        AND bx3x.BX IS NULL
        """
        
        electric_mismatch_result = helper.execute_query(electric_mismatch_query)
        electric_mismatch_count = electric_mismatch_result[0]["mismatch_count"]
        assert electric_mismatch_count == 0, \
            f"電気契約Bxマッチングの不整合: {electric_mismatch_count}件"
        
        logger.info("ビジネスルール整合性確認完了")

    def test_pipeline_performance_metrics(self, helper, pipeline_run_id):
        """パイプラインパフォーマンス検証テスト"""
        logger.info("パフォーマンス検証テスト開始")
        
        # パイプライン完了確認
        helper.wait_for_pipeline_completion(pipeline_run_id, timeout_minutes=self.EXPECTED_MAX_DURATION)
        
        # パフォーマンスメトリクス取得
        metrics = helper.get_pipeline_performance_metrics(pipeline_run_id)
        
        # 実行時間チェック
        execution_time = metrics.get("execution_time_seconds", 0)
        assert execution_time <= self.EXPECTED_MAX_DURATION * 60, \
            f"実行時間が期待値を超過: {execution_time}秒"
        
        # 処理効率チェック
        total_processed = sum([
            helper.get_table_row_count(self.SCHEMA_NAME, table) 
            for table in self.TEMP_TABLES.values()
        ])
        
        if execution_time > 0:
            throughput = total_processed / execution_time
            logger.info(f"総合スループット: {throughput:.2f} レコード/秒")
        
        logger.info(f"パフォーマンス検証完了: 実行時間={execution_time}秒, 総処理レコード={total_processed}")

    def test_data_quality_rules(self, helper, pipeline_run_id):
        """データ品質ルール検証テスト"""
        logger.info("データ品質ルール検証テスト開始")
        
        # パイプライン完了確認
        helper.wait_for_pipeline_completion(pipeline_run_id, timeout_minutes=self.EXPECTED_MAX_DURATION)
        
        # 各作業テーブルの品質チェック
        for table_key, table_name in self.TEMP_TABLES.items():
            logger.info(f"データ品質チェック開始: {table_name}")
            
            # 基本的な品質チェック
            quality_results = self._execute_table_quality_checks(helper, table_name)
            
            for rule_name, result in quality_results.items():
                assert result["passed"], \
                    f"データ品質ルール失敗 ({table_name}): {rule_name} - {result.get('message', '')}"
                logger.info(f"データ品質ルール合格 ({table_name}): {rule_name}")
        
        logger.info("データ品質ルール検証テスト完了")

    def _execute_table_quality_checks(self, helper, table_name) -> Dict[str, Dict[str, Any]]:
        """テーブル固有の品質チェック実行"""
        results = {}
        
        # 1. レコード存在チェック
        record_count = helper.get_table_row_count(self.SCHEMA_NAME, table_name)
        results["record_existence"] = {
            "passed": record_count > 0,
            "message": f"レコード数: {record_count}"
        }
        
        # 2. 日付形式チェック（OUTPUT_DATEなど）
        if "OUTPUT_DATE" in helper.get_table_columns(self.SCHEMA_NAME, table_name):
            invalid_date_count = helper.execute_query(f"""
                SELECT COUNT(*) as invalid_count
                FROM [{self.SCHEMA_NAME}].[{table_name}]
                WHERE OUTPUT_DATE IS NOT NULL 
                AND TRY_CONVERT(datetime, OUTPUT_DATE) IS NULL
            """)[0]["invalid_count"]
            
            results["date_format"] = {
                "passed": invalid_date_count == 0,
                "message": f"不正な日付形式レコード数: {invalid_date_count}"
            }
        
        return results

    def test_script_activity_validation(self, helper, pipeline_run_id):
        """スクリプトアクティビティ検証テスト"""
        logger.info("スクリプトアクティビティ検証テスト開始")
        
        # パイプライン完了確認
        helper.wait_for_pipeline_completion(pipeline_run_id, timeout_minutes=self.EXPECTED_MAX_DURATION)
        
        # アクティビティ実行結果取得
        activities = helper.get_pipeline_activity_results(pipeline_run_id)
        
        # スクリプトアクティビティの確認
        script_activity = next(
            (a for a in activities if a.get("name") == "at_Insert_ClientDmBx"), 
            None
        )
        
        assert script_activity is not None, "スクリプトアクティビティが見つかりません"
        assert script_activity.get("status") == "Succeeded", \
            f"スクリプトアクティビティが失敗: {script_activity.get('status')}"
        
        # スクリプト実行時間チェック
        activity_duration = script_activity.get("duration_seconds", 0)
        assert activity_duration <= 7200, \
            f"スクリプト実行時間が長すぎます: {activity_duration}秒 > 7200秒"
        
        logger.info(f"スクリプトアクティビティ検証完了: 実行時間={activity_duration}秒")

    def test_monitoring_and_alerting(self, helper, pipeline_run_id):
        """監視・アラート機能テスト"""
        logger.info("監視・アラート機能テスト開始")
        
        # パイプライン完了確認
        helper.wait_for_pipeline_completion(pipeline_run_id, timeout_minutes=self.EXPECTED_MAX_DURATION)
        
        # 監視メトリクス確認
        monitoring_data = helper.get_pipeline_monitoring_data(pipeline_run_id)
        
        # アラート条件チェック
        if monitoring_data.get("execution_time_seconds", 0) > 1500:  # 25分超過
            logger.warning("実行時間アラート: 25分を超過しました")
        
        if monitoring_data.get("error_count", 0) > 0:
            logger.warning(f"エラーアラート: {monitoring_data['error_count']}件のエラーが発生")
        
        # Bx付与率監視
        bx_assignment_rate = self._calculate_bx_assignment_rate(helper)
        if bx_assignment_rate < 50.0:
            logger.warning(f"Bx付与率アラート: {bx_assignment_rate:.2f}%と低い値です")
        
        logger.info(f"監視データ記録: {monitoring_data}")
        logger.info("監視・アラート機能テスト完了")

    def _calculate_bx_assignment_rate(self, helper) -> float:
        """Bx付与率計算"""
        table_name = self.TEMP_TABLES["client_dm_bx"]
        
        total_count = helper.get_table_row_count(self.SCHEMA_NAME, table_name)
        if total_count == 0:
            return 0.0
        
        bx_assigned_count = helper.execute_query(f"""
            SELECT COUNT(*) as bx_count
            FROM [{self.SCHEMA_NAME}].[{table_name}]
            WHERE BX IS NOT NULL
        """)[0]["bx_count"]
        
        return (bx_assigned_count / total_count) * 100


if __name__ == "__main__":
    # 単体実行用
    pytest.main([__file__, "-v", "-s"])
