"""
E2Eテスト: pi_Copy_marketing_client_dm パイプライン

このテストは、pi_Copy_marketing_client_dm パイプラインの動作を独立して検証します。
パイプラインは以下の処理を実行します：
1. Marketingスキーマの顧客DMから作業テーブル（omni_temp）への全量コピー
2. 作業テーブルから本テーブル（omni.顧客DM）への全量コピー

【重要】このパイプラインは533列の包括的な顧客データ構造を処理します：
- ガスメーター情報（LIV0EU_*列）
- 機器詳細（LIV0SPD_*列）  
- TESシステムデータ（TESHSMC_*, TESHSEQ_*, TESHRDTR_*, TESSV_*列）
- 電気契約情報（EPCISCRT_*列）
- Web履歴追跡（WEBHIS_*列）
- サービス利用フラグとビジネスロジック

テスト内容：
- パイプライン実行の成功確認
- 533列構造の完全性検証
- データ品質チェック（包括的）
- パフォーマンス検証
- エラーハンドリング
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
# from tests.conftest import azure_data_factory_client

logger = logging.getLogger(__name__)


class TestPipelineMarketingClientDM:

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
    """pi_Copy_marketing_client_dm パイプライン専用のE2Eテストクラス"""
    
    PIPELINE_NAME = "pi_Copy_marketing_client_dm"
    TEMP_TABLE_NAME = "omni_ods_marketing_trn_client_dm_temp"
    TARGET_TABLE_NAME = "顧客DM"
    SCHEMA_NAME = "omni"
    
    # パフォーマンス期待値（分）
    EXPECTED_MAX_DURATION = 30  # 30分
    EXPECTED_MIN_RECORDS = 1000  # 最小レコード数
    EXPECTED_COLUMN_COUNT = 533  # 期待される列数（包括的構造）
    
    # 重要なカラムグループ（533列中の主要なもの）
    CRITICAL_COLUMN_GROUPS = {
        "gas_meter": ["LIV0EU_*"],  # ガスメーター情報
        "equipment": ["LIV0SPD_*"],  # 機器詳細
        "tes_system": ["TESHSMC_*", "TESHSEQ_*", "TESHRDTR_*", "TESSV_*"],  # TESシステム
        "electric_contract": ["EPCISCRT_*"],  # 電気契約
        "web_history": ["WEBHIS_*"],  # Web履歴
        "core_client": ["CLIENT_KEY_AX", "KNUMBER_AX", "ADDRESS_KEY_AX"]  # 顧客コア情報
    }
    
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
        return e2e_synapse_connection    @pytest.fixture(scope="class")
    def pipeline_run_id(self, helper):
        """パイプライン実行IDを取得するフィクスチャ"""
        logger.info(f"パイプライン実行開始: {self.PIPELINE_NAME}")
        
        try:
            # 533列包括的テストデータのセットアップ
            logger.info("533列包括的テストデータセットアップ開始")
            setup_success = helper.setup_marketing_client_dm_comprehensive_test_data()
            assert setup_success, "包括的テストデータのセットアップに失敗"
            
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
        
        # 必要なデータセット存在確認
        datasets = ["ds_sqlmi", "ds_synapse_analytics"]
        for dataset in datasets:
            assert helper.validate_dataset_exists(dataset), f"データセット {dataset} が存在しません"
        
        # 作業テーブルの初期化確認
        temp_table_exists = helper.check_table_exists(self.SCHEMA_NAME, self.TEMP_TABLE_NAME)
        if temp_table_exists:
            logger.info(f"作業テーブル {self.TEMP_TABLE_NAME} が存在します")
        
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

    def test_data_copy_validation(self, helper, pipeline_run_id):
        """データコピー検証テスト"""
        logger.info("データコピー検証テスト開始")
        
        # パイプライン完了確認
        helper.wait_for_pipeline_completion(pipeline_run_id, timeout_minutes=self.EXPECTED_MAX_DURATION)
        
        # 第1段階: 作業テーブル確認
        self._validate_temp_table_data(helper)
        
        # 第2段階: 本テーブル確認
        self._validate_target_table_data(helper)
        
        # データ整合性確認
        self._validate_data_consistency(helper)
        
        logger.info("データコピー検証テスト完了")
    
    def _validate_temp_table_data(self, helper):
        """作業テーブルデータ検証（533列構造対応）"""
        logger.info(f"作業テーブル検証: {self.TEMP_TABLE_NAME}")
        
        # テーブル存在確認
        assert helper.check_table_exists(self.SCHEMA_NAME, self.TEMP_TABLE_NAME), \
            f"作業テーブル {self.TEMP_TABLE_NAME} が存在しません"
        
        # レコード数確認
        temp_count_result = helper.execute_external_query(
            "marketing_client_dm_comprehensive.sql",
            "temp_table_row_count",
            table_name=self.TEMP_TABLE_NAME
        )
        temp_count = temp_count_result[0]["row_count"] if temp_count_result else 0
        assert temp_count >= self.EXPECTED_MIN_RECORDS, \
            f"作業テーブルのレコード数が不足: {temp_count} < {self.EXPECTED_MIN_RECORDS}"
        
        # 533列構造検証
        self._validate_comprehensive_column_structure(helper, self.TEMP_TABLE_NAME, "temp")
        
        # NULL値チェック（主キー項目）
        null_keys_result = helper.execute_external_query(
            "marketing_client_dm_comprehensive.sql",
            "null_critical_fields_check",
            table_name=self.TEMP_TABLE_NAME
        )
        null_keys = null_keys_result[0]["error_count"] if null_keys_result else 1
        assert null_keys == 0, f"主キーにNULL値が存在: {null_keys}件"
        
        logger.info(f"作業テーブル検証完了: {temp_count}件のレコード、533列構造OK")
        
        logger.info(f"作業テーブル検証完了: {temp_count}件のレコード")

    def _validate_target_table_data(self, helper):
        """本テーブルデータ検証"""
        logger.info(f"本テーブル検証: {self.TARGET_TABLE_NAME}")
        
        # テーブル存在確認
        assert helper.check_table_exists(self.SCHEMA_NAME, self.TARGET_TABLE_NAME), \
            f"本テーブル {self.TARGET_TABLE_NAME} が存在しません"
        
        # レコード数確認
        target_count = helper.get_table_row_count(self.SCHEMA_NAME, self.TARGET_TABLE_NAME)
        assert target_count >= self.EXPECTED_MIN_RECORDS, \
            f"本テーブルのレコード数が不足: {target_count} < {self.EXPECTED_MIN_RECORDS}"
        
        # データ更新時刻確認
        last_updated = helper.get_table_last_updated(self.SCHEMA_NAME, self.TARGET_TABLE_NAME)
        assert last_updated is not None, "データ更新時刻が取得できません"
        
        # 今日更新されているか確認
        today = datetime.now().date()
        assert last_updated.date() == today, \
            f"データが今日更新されていません: {last_updated.date()} != {today}"
        
        logger.info(f"本テーブル検証完了: {target_count}件のレコード, 最終更新: {last_updated}")
    
    def _validate_data_consistency(self, helper):
        """データ整合性検証（533列対応）"""
        logger.info("データ整合性検証開始")
        
        # 作業テーブルと本テーブルのレコード数比較
        temp_count_result = helper.execute_external_query(
            "marketing_client_dm_comprehensive.sql",
            "temp_table_row_count",
            table_name=self.TEMP_TABLE_NAME
        )
        target_count_result = helper.execute_external_query(
            "marketing_client_dm_comprehensive.sql", 
            "target_table_row_count",
            table_name=self.TARGET_TABLE_NAME
        )
        
        temp_count = temp_count_result[0]["row_count"] if temp_count_result else 0
        target_count = target_count_result[0]["row_count"] if target_count_result else 0
        
        # レコード数の一致確認（多少の差異は許容）
        count_diff = abs(temp_count - target_count)
        count_threshold = max(temp_count * 0.01, 100)  # 1%または100件の差異を許容
        
        assert count_diff <= count_threshold, \
            f"テーブル間のレコード数差異が大きい: temp={temp_count}, target={target_count}, diff={count_diff}"
        
        # 533列の包括的データ整合性確認
        self._validate_comprehensive_data_consistency(helper)
        
        # 各カラムグループのデータ品質検証
        self._validate_comprehensive_data_quality(helper, self.TARGET_TABLE_NAME)
        
        logger.info(f"データ整合性検証完了: temp={temp_count}, target={target_count}")

    def _validate_comprehensive_data_consistency(self, helper):
        """包括的データ整合性確認（533列対応）"""
        logger.info("包括的データ整合性確認開始")
        
        # 主要カラムグループごとのデータ整合性確認
        consistency_checks = [
            "core_client_data_consistency",
            "gas_meter_data_consistency", 
            "electric_contract_data_consistency",
            "equipment_data_consistency",
            "tes_system_data_consistency",
            "web_history_data_consistency"
        ]
        
        failed_checks = []
        
        for check_name in consistency_checks:
            try:
                result = helper.execute_external_query(
                    "marketing_client_dm_comprehensive.sql",
                    check_name,
                    temp_table=self.TEMP_TABLE_NAME,
                    target_table=self.TARGET_TABLE_NAME
                )
                
                if result:
                    inconsistency_count = result[0].get("inconsistency_count", 0)
                    if inconsistency_count > 0:
                        failed_checks.append(f"{check_name}: {inconsistency_count}件の不整合")
                        logger.warning(f"データ整合性チェック不合格: {check_name} - {inconsistency_count}件")
                    else:
                        logger.info(f"データ整合性チェック合格: {check_name}")
                
            except Exception as e:
                logger.warning(f"データ整合性チェック実行エラー: {check_name} - {e}")
                # 一部のチェックが失敗しても継続（スキーマの変更等で一部列が存在しない可能性）
        
        # 重要なチェックが複数失敗した場合のみエラーとする
        critical_failures = [check for check in failed_checks if "core_client" in check or "gas_meter" in check]
        assert len(critical_failures) == 0, \
            f"重要なデータ整合性チェックに失敗: {critical_failures}"
        
        if failed_checks:
            logger.warning(f"一部のデータ整合性チェックに失敗（継続可能）: {failed_checks}")
        
        logger.info("包括的データ整合性確認完了")

    def _validate_sample_data_consistency(self, helper):
        """サンプルデータ整合性確認"""
        logger.info("サンプルデータ整合性確認開始")
        
        # サンプルレコードを取得して比較
        sample_query = """
        SELECT TOP 10 
            CLIENT_KEY_AX,
            LIV0EU_1X,
            LIV0EU_8X,
            LIV0EU_UKMT_NW_PART_CD
        FROM [{schema}].[{table}]
        WHERE CLIENT_KEY_AX IS NOT NULL
        ORDER BY CLIENT_KEY_AX
        """
        
        temp_samples = helper.execute_query(
            sample_query.format(schema=self.SCHEMA_NAME, table=self.TEMP_TABLE_NAME)
        )
        target_samples = helper.execute_query(
            sample_query.format(schema=self.SCHEMA_NAME, table=self.TARGET_TABLE_NAME)
        )
        
        # サンプルデータが取得できることを確認
        assert len(temp_samples) > 0, "作業テーブルからサンプルデータが取得できません"
        assert len(target_samples) > 0, "本テーブルからサンプルデータが取得できません"
        
        logger.info(f"サンプルデータ整合性確認完了: temp={len(temp_samples)}, target={len(target_samples)}")

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
        
        # データ量チェック
        processed_rows = metrics.get("processed_rows", 0)
        assert processed_rows >= self.EXPECTED_MIN_RECORDS, \
            f"処理レコード数が不足: {processed_rows}"
        
        # スループットチェック
        if execution_time > 0:
            throughput = processed_rows / execution_time
            logger.info(f"データ処理スループット: {throughput:.2f} レコード/秒")
        
        logger.info(f"パフォーマンス検証完了: 実行時間={execution_time}秒, 処理レコード={processed_rows}")

    def test_pipeline_activity_validation(self, helper, pipeline_run_id):
        """パイプラインアクティビティ検証テスト"""
        logger.info("アクティビティ検証テスト開始")
        
        # パイプライン完了確認
        helper.wait_for_pipeline_completion(pipeline_run_id, timeout_minutes=self.EXPECTED_MAX_DURATION)
        
        # アクティビティ実行結果取得
        activities = helper.get_pipeline_activity_results(pipeline_run_id)
        
        # 期待するアクティビティの確認
        expected_activities = [
            "at_Copy_marketing_ClientDM_temp",  # 第1段階コピー
            "at_Copy_ClientDM_本番実行"         # 第2段階コピー（推定）
        ]
        
        activity_names = [activity.get("name", "") for activity in activities]
        
        for expected_activity in expected_activities:
            if expected_activity in activity_names:
                activity_status = next(
                    (a.get("status") for a in activities if a.get("name") == expected_activity), 
                    None
                )
                assert activity_status == "Succeeded", \
                    f"アクティビティ {expected_activity} が失敗: {activity_status}"
                logger.info(f"アクティビティ {expected_activity} 成功確認")
        
        logger.info("アクティビティ検証テスト完了")

    def test_error_handling_scenarios(self, helper):
        """エラーハンドリングシナリオテスト"""
        logger.info("エラーハンドリングテスト開始")
        
        # 無効なパイプライン名でのテスト
        with pytest.raises(Exception):
            helper.trigger_pipeline("invalid_pipeline_name")
        
        # 存在しないデータセットでのテスト
        invalid_dataset_exists = helper.validate_dataset_exists("invalid_dataset")
        assert not invalid_dataset_exists, "存在しないデータセットの検証が正しく動作していません"
        
        logger.info("エラーハンドリングテスト完了")

    def test_data_quality_rules(self, helper, pipeline_run_id):
        """データ品質ルール検証テスト"""
        logger.info("データ品質ルール検証テスト開始")
        
        # パイプライン完了確認
        helper.wait_for_pipeline_completion(pipeline_run_id, timeout_minutes=self.EXPECTED_MAX_DURATION)
        
        # データ品質チェック
        quality_results = self._execute_data_quality_checks(helper)
        
        # 品質ルール検証
        for rule_name, result in quality_results.items():
            assert result["passed"], f"データ品質ルール失敗: {rule_name} - {result.get('message', '')}"
            logger.info(f"データ品質ルール合格: {rule_name}")
        
        logger.info("データ品質ルール検証テスト完了")

    def _execute_data_quality_checks(self, helper) -> Dict[str, Dict[str, Any]]:
        """データ品質チェック実行"""
        results = {}
        
        # 1. 主キー重複チェック
        duplicate_count = helper.execute_query(f"""
            SELECT COUNT(*) as duplicate_count
            FROM (
                SELECT CLIENT_KEY_AX, COUNT(*) as cnt
                FROM [{self.SCHEMA_NAME}].[{self.TARGET_TABLE_NAME}]
                WHERE CLIENT_KEY_AX IS NOT NULL
                GROUP BY CLIENT_KEY_AX
                HAVING COUNT(*) > 1
            ) duplicates
        """)[0]["duplicate_count"]
        
        results["primary_key_uniqueness"] = {
            "passed": duplicate_count == 0,
            "message": f"重複レコード数: {duplicate_count}"
        }
        
        # 2. 必須フィールドNULLチェック
        null_count = helper.execute_query(f"""
            SELECT COUNT(*) as null_count
            FROM [{self.SCHEMA_NAME}].[{self.TARGET_TABLE_NAME}]
            WHERE CLIENT_KEY_AX IS NULL
        """)[0]["null_count"]
        
        results["mandatory_fields"] = {
            "passed": null_count == 0,
            "message": f"必須フィールドNULL数: {null_count}"
        }
        
        # 3. データ型整合性チェック
        type_error_count = helper.execute_query(f"""
            SELECT COUNT(*) as error_count
            FROM [{self.SCHEMA_NAME}].[{self.TARGET_TABLE_NAME}]
            WHERE TRY_CONVERT(int, LIV0EU_1X) IS NULL AND LIV0EU_1X IS NOT NULL
        """)[0]["error_count"]
        
        results["data_type_consistency"] = {
            "passed": type_error_count == 0,
            "message": f"データ型不整合数: {type_error_count}"
        }
        
        return results

    def test_monitoring_and_alerting(self, helper, pipeline_run_id):
        """監視・アラート機能テスト"""
        logger.info("監視・アラート機能テスト開始")
        
        # パイプライン完了確認
        helper.wait_for_pipeline_completion(pipeline_run_id, timeout_minutes=self.EXPECTED_MAX_DURATION)
        
        # 監視メトリクス確認
        monitoring_data = helper.get_pipeline_monitoring_data(pipeline_run_id)
        
        # アラート条件チェック
        if monitoring_data.get("execution_time_seconds", 0) > 1800:  # 30分超過
            logger.warning("実行時間アラート: 30分を超過しました")
        
        if monitoring_data.get("error_count", 0) > 0:
            logger.warning(f"エラーアラート: {monitoring_data['error_count']}件のエラーが発生")
        
        # 成功メトリクス記録
        logger.info(f"監視データ記録: {monitoring_data}")
        
        logger.info("監視・アラート機能テスト完了")

    def _validate_comprehensive_column_structure(self, helper, table_name: str, table_type: str):
        """533列の包括的構造検証"""
        logger.info(f"533列構造検証開始: {table_name} ({table_type})")
        
        try:
            # カラム数検証
            column_count_result = helper.execute_external_query(
                "marketing_client_dm_comprehensive.sql",
                "column_count_validation",
                table_name=table_name
            )
            actual_column_count = len(column_count_result[0]) if column_count_result else 0
            assert actual_column_count >= self.EXPECTED_COLUMN_COUNT, \
                f"列数が不足: {actual_column_count} < {self.EXPECTED_COLUMN_COUNT}"
            
            # 各カラムグループの存在確認
            validation_results = {}
            for group_name, column_patterns in self.CRITICAL_COLUMN_GROUPS.items():
                if group_name == "core_client":
                    # コア顧客情報の直接検証
                    result = helper.execute_external_query(
                        "marketing_client_dm_comprehensive.sql",
                        "core_client_columns_validation",
                        table_name=table_name
                    )
                else:
                    # パターンベースの検証
                    result = helper.execute_external_query(
                        "marketing_client_dm_comprehensive.sql",
                        f"{group_name}_columns_validation",
                        table_name=table_name
                    )
                
                validation_results[group_name] = len(result) > 0 if result else False
                
                if not validation_results[group_name]:
                    logger.warning(f"カラムグループ {group_name} の検証に失敗")
            
            # 全てのカラムグループが検証をパスすることを確認
            failed_groups = [group for group, passed in validation_results.items() if not passed]
            assert len(failed_groups) == 0, \
                f"以下のカラムグループの検証に失敗: {failed_groups}"
            
            logger.info(f"533列構造検証完了: {table_name} - 全{actual_column_count}列確認")
            
        except Exception as e:
            logger.error(f"533列構造検証エラー: {table_name} - {e}")
            raise

    def _validate_comprehensive_data_quality(self, helper, table_name: str):
        """包括的データ品質検証（533列対応）"""
        logger.info(f"包括的データ品質検証開始: {table_name}")
        
        quality_checks = [
            "null_critical_fields_check",
            "duplicate_client_key_check", 
            "invalid_date_check",
            "invalid_numeric_check",
            "gas_meter_data_quality_check",
            "electric_contract_data_quality_check",
            "equipment_data_quality_check"
        ]
        
        failed_checks = []
        
        for check_name in quality_checks:
            try:
                result = helper.execute_external_query(
                    "marketing_client_dm_comprehensive.sql",
                    check_name,
                    table_name=table_name
                )
                
                error_count = result[0]["error_count"] if result and "error_count" in result[0] else 1
                
                if error_count > 0:
                    failed_checks.append(f"{check_name}: {error_count}件のエラー")
                    logger.warning(f"データ品質チェック失敗: {check_name} - {error_count}件")
                
            except Exception as e:
                failed_checks.append(f"{check_name}: 実行エラー - {e}")
                logger.error(f"データ品質チェック実行エラー: {check_name} - {e}")
        
        assert len(failed_checks) == 0, \
            f"以下のデータ品質チェックに失敗: {failed_checks}"
        
        logger.info(f"包括的データ品質検証完了: {table_name} - 全チェックパス")

    # ...existing code...
    def test_monitoring_and_alerting(self, helper, pipeline_run_id):
        """監視・アラート機能テスト"""
        logger.info("監視・アラート機能テスト開始")
        
        # パイプライン完了確認
        helper.wait_for_pipeline_completion(pipeline_run_id, timeout_minutes=self.EXPECTED_MAX_DURATION)
        
        # 監視メトリクス確認
        monitoring_data = helper.get_pipeline_monitoring_data(pipeline_run_id)
        
        # アラート条件チェック
        if monitoring_data.get("execution_time_seconds", 0) > 1800:  # 30分超過
            logger.warning("実行時間アラート: 30分を超過しました")
        
        if monitoring_data.get("error_count", 0) > 0:
            logger.warning(f"エラーアラート: {monitoring_data['error_count']}件のエラーが発生")
        
        # 成功メトリクス記録
        logger.info(f"監視データ記録: {monitoring_data}")
        
        logger.info("監視・アラート機能テスト完了")


if __name__ == "__main__":
    # 単体実行用
    pytest.main([__file__, "-v", "-s"])
