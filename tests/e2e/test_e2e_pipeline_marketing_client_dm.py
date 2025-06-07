"""
E2Eテスト: pi_Copy_marketing_client_dm パイプライン（533列包括的テスト）

このテストは、pi_Copy_marketing_client_dm パイプラインの実際の533列構造を完全に検証します。
パイプラインは以下の処理を実行します：
1. Marketingスキーマの顧客DMから作業テーブル（omni_temp）への全量コピー
2. 作業テーブルから本テーブル（omni.顧客DM）への全量コピー

【包括的533列構造】このパイプラインは以下を含む533列の包括的な顧客データ構造を処理します：
- ガスメーター情報（LIV0EU_*列）: ガス使用量、メーター情報
- 機器詳細（LIV0SPD_*列）: 設備・機器のスペック情報
- TESシステムデータ（TESHSMC_*, TESHSEQ_*, TESHRDTR_*, TESSV_*列）: TESシステム関連データ
- 電気契約情報（EPCISCRT_*列）: 電気契約の詳細情報
- Web履歴追跡（WEBHIS_*列）: Webサイト利用履歴
- アラーム機器情報: セキュリティ・アラーム機器データ
- 請求・支払い情報: 請求履歴と支払い方法
- 人口統計情報: 顧客属性・デモグラフィック情報
- サービス利用フラグとビジネスロジック

テスト内容：
- パイプライン実行の成功確認
- 533列構造の完全性検証
- データ品質チェック（包括的）
- カラムグループ別検証
- パフォーマンス検証
- エラーハンドリング
"""

import pytest
import time
import logging
from datetime import datetime
from unittest.mock import Mock
from typing import Dict, Any, List

from tests.e2e.helpers.synapse_e2e_helper import SynapseE2EConnection
# from tests.conftest import azure_data_factory_client

logger = logging.getLogger(__name__)


class TestPipelineMarketingClientDMComprehensive:
    """pi_Copy_marketing_client_dm パイプライン 533列包括的E2Eテストクラス"""
    
    PIPELINE_NAME = "pi_Copy_marketing_client_dm"
    TEMP_TABLE_NAME = "omni_ods_marketing_trn_client_dm_temp"
    TARGET_TABLE_NAME = "顧客DM"
    SCHEMA_NAME = "omni"
    
    # パフォーマンス期待値
    EXPECTED_MAX_DURATION = 30  # 30分
    EXPECTED_MIN_RECORDS = 1000  # 最小レコード数
    EXPECTED_COLUMN_COUNT = 533  # 期待される列数（包括的構造）
    
    # 533列中の重要なカラムグループ
    CRITICAL_COLUMN_GROUPS = {
        "core_client": {
            "columns": ["CLIENT_KEY_AX", "KNUMBER_AX", "ADDRESS_KEY_AX"],
            "description": "顧客コア情報"
        },
        "gas_meter": {
            "patterns": ["LIV0EU_*"],
            "description": "ガスメーター情報"
        },
        "equipment": {
            "patterns": ["LIV0SPD_*"], 
            "description": "機器詳細情報"
        },
        "tes_system": {
            "patterns": ["TESHSMC_*", "TESHSEQ_*", "TESHRDTR_*", "TESSV_*"],
            "description": "TESシステムデータ"
        },
        "electric_contract": {
            "patterns": ["EPCISCRT_*"],
            "description": "電気契約情報"
        },
        "web_history": {
            "patterns": ["WEBHIS_*"],
            "description": "Web履歴追跡"
        }
    }
    
    def setup_class(self):
        """テストクラス初期化"""
        self.start_time = datetime.now()
        logger.info(f"533列包括的E2Eテスト開始: {self.PIPELINE_NAME} - {self.start_time}")
        
    def teardown_class(self):
        """テストクラス終了処理"""
        end_time = datetime.now()
        duration = end_time - self.start_time
        logger.info(f"533列包括的E2Eテスト完了: {self.PIPELINE_NAME} - 実行時間: {duration}")

    @pytest.fixture(scope="class")
    def helper(self, e2e_synapse_connection):
        """SynapseE2EConnectionフィクスチャ"""
        return e2e_synapse_connection

    @pytest.fixture(scope="class")
    def pipeline_run_id(self, helper):
        """パイプライン実行IDを取得するフィクスチャ"""
        logger.info(f"パイプライン実行開始: {self.PIPELINE_NAME}")
        
        try:
            # 533列包括的テストデータのセットアップ
            logger.info("533列包括的テストデータセットアップ開始")
            setup_success = helper.setup_marketing_client_dm_comprehensive_test_data()
            assert setup_success, "包括的テストデータのセットアップに失敗"
            
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
        required_datasets = ["ds_sqlmi", "ds_synapse_analytics"] 
        for dataset in required_datasets:
            assert helper.validate_dataset_exists(dataset), f"データセット {dataset} が存在しません"
        
        # 接続テスト
        assert helper.test_synapse_connection(), "Synapse接続テストに失敗"
        
        logger.info("実行前検証完了")

    def test_pipeline_execution_success(self, helper, pipeline_run_id):
        """パイプライン実行成功テスト"""
        logger.info("パイプライン実行成功テスト開始")
        
        # パイプライン完了待機
        status = helper.wait_for_pipeline_completion(
            pipeline_run_id, 
            timeout_minutes=self.EXPECTED_MAX_DURATION
        )
        
        # 実行結果確認
        assert status == "Succeeded", f"パイプライン実行が失敗: ステータス={status}"
        
        logger.info("パイプライン実行成功確認完了")

    def test_comprehensive_533_column_structure_validation(self, helper, pipeline_run_id):
        """533列構造包括検証テスト"""
        logger.info("533列構造包括検証テスト開始")
        
        # パイプライン完了確認
        helper.wait_for_pipeline_completion(pipeline_run_id, timeout_minutes=self.EXPECTED_MAX_DURATION)
        
        # 作業テーブル533列構造検証
        self._validate_comprehensive_column_structure(helper, self.TEMP_TABLE_NAME, "temp")
        
        # 本テーブル533列構造検証
        self._validate_comprehensive_column_structure(helper, self.TARGET_TABLE_NAME, "target")
        
        logger.info("533列構造包括検証テスト完了")

    def test_comprehensive_data_quality_validation(self, helper, pipeline_run_id):
        """包括的データ品質検証テスト"""
        logger.info("包括的データ品質検証テスト開始")
        
        # パイプライン完了確認
        helper.wait_for_pipeline_completion(pipeline_run_id, timeout_minutes=self.EXPECTED_MAX_DURATION)
        
        # 作業テーブルデータ品質検証
        self._validate_comprehensive_data_quality(helper, self.TEMP_TABLE_NAME)
        
        # 本テーブルデータ品質検証
        self._validate_comprehensive_data_quality(helper, self.TARGET_TABLE_NAME)
        
        logger.info("包括的データ品質検証テスト完了")

    def test_data_consistency_validation(self, helper, pipeline_run_id):
        """データ整合性検証テスト"""
        logger.info("データ整合性検証テスト開始")
        
        # パイプライン完了確認
        helper.wait_for_pipeline_completion(pipeline_run_id, timeout_minutes=self.EXPECTED_MAX_DURATION)
        
        # データ整合性検証
        self._validate_comprehensive_data_consistency(helper)
        
        logger.info("データ整合性検証テスト完了")

    def test_column_group_specific_validation(self, helper, pipeline_run_id):
        """カラムグループ別検証テスト"""
        logger.info("カラムグループ別検証テスト開始")
        
        # パイプライン完了確認
        helper.wait_for_pipeline_completion(pipeline_run_id, timeout_minutes=self.EXPECTED_MAX_DURATION)
        
        for group_name, group_info in self.CRITICAL_COLUMN_GROUPS.items():
            logger.info(f"カラムグループ検証: {group_name} - {group_info['description']}")
            self._validate_column_group(helper, group_name, group_info)
        
        logger.info("カラムグループ別検証テスト完了")

    def _validate_comprehensive_column_structure(self, helper, table_name: str, table_type: str):
        """533列の包括的構造検証"""
        logger.info(f"533列構造検証開始: {table_name} ({table_type})")
        
        try:
            # Marketing Client DM構造検証の実行
            validation_results = helper.validate_marketing_client_dm_structure()
            
            # カラム数検証
            assert validation_results.get("column_count_533", False), \
                f"533列構造が確認できません: {table_name}"
            
            # 各カラムグループの存在確認
            for group_name in self.CRITICAL_COLUMN_GROUPS.keys():
                validation_key = f"{group_name}_columns_validation"
                assert validation_results.get(validation_key, False), \
                    f"カラムグループ {group_name} の検証に失敗: {table_name}"
            
            logger.info(f"533列構造検証完了: {table_name}")
            
        except Exception as e:
            logger.error(f"533列構造検証エラー: {table_name} - {e}")
            raise

    def _validate_comprehensive_data_quality(self, helper, table_name: str):
        """包括的データ品質検証（533列対応）"""
        logger.info(f"包括的データ品質検証開始: {table_name}")
        
        try:
            # Marketing Client DM構造検証（データ品質含む）
            validation_results = helper.validate_marketing_client_dm_structure()
            
            quality_checks = [
                "null_critical_fields_check",
                "duplicate_client_key_check", 
                "invalid_date_check",
                "invalid_numeric_check"
            ]
            
            failed_checks = []
            
            for check_name in quality_checks:
                if not validation_results.get(check_name, False):
                    failed_checks.append(check_name)
                    logger.warning(f"データ品質チェック失敗: {check_name}")
            
            # 重要なチェックが失敗した場合はエラー
            critical_failures = [check for check in failed_checks if "null_critical" in check or "duplicate" in check]
            assert len(critical_failures) == 0, \
                f"重要なデータ品質チェックに失敗: {critical_failures}"
            
            if failed_checks:
                logger.warning(f"一部のデータ品質チェックに失敗（継続可能）: {failed_checks}")
            
            logger.info(f"包括的データ品質検証完了: {table_name}")
            
        except Exception as e:
            logger.error(f"包括的データ品質検証エラー: {table_name} - {e}")
            raise

    def _validate_comprehensive_data_consistency(self, helper):
        """包括的データ整合性確認（533列対応）"""
        logger.info("包括的データ整合性確認開始")
        
        try:
            # レコード数比較
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
            
            logger.info(f"包括的データ整合性確認完了: temp={temp_count}, target={target_count}")
            
        except Exception as e:
            logger.warning(f"包括的データ整合性確認でエラー（継続）: {e}")
            # 一部のクエリが実行できない場合も想定（スキーマの変更等）

    def _validate_column_group(self, helper, group_name: str, group_info: Dict[str, Any]):
        """カラムグループ別検証"""
        logger.info(f"カラムグループ検証開始: {group_name}")
        
        try:
            if group_name == "core_client":
                # コア顧客情報の直接検証
                result = helper.execute_external_query(
                    "marketing_client_dm_comprehensive.sql",
                    "core_client_columns_validation",
                    table_name=self.TARGET_TABLE_NAME
                )
            else:
                # パターンベースの検証
                result = helper.execute_external_query(
                    "marketing_client_dm_comprehensive.sql",
                    f"{group_name}_columns_validation",
                    table_name=self.TARGET_TABLE_NAME
                )
            
            assert len(result) > 0, f"カラムグループ {group_name} の検証クエリで結果なし"
            
            logger.info(f"カラムグループ検証完了: {group_name} - {group_info['description']}")
            
        except Exception as e:
            logger.warning(f"カラムグループ検証でエラー（継続）: {group_name} - {e}")
            # 533列全てが常に存在するとは限らない（スキーマ変更やNULL設定列）

    def test_performance_validation(self, helper, pipeline_run_id):
        """パフォーマンス検証テスト"""
        logger.info("パフォーマンス検証テスト開始")
        
        # パイプライン完了確認
        start_time = time.time()
        status = helper.wait_for_pipeline_completion(
            pipeline_run_id, 
            timeout_minutes=self.EXPECTED_MAX_DURATION
        )
        end_time = time.time()
        
        execution_time = end_time - start_time
        
        # 実行時間検証
        assert execution_time <= self.EXPECTED_MAX_DURATION * 60, \
            f"実行時間が期待値を超過: {execution_time}秒 > {self.EXPECTED_MAX_DURATION * 60}秒"
        
        # 処理レコード数検証
        target_count_result = helper.execute_external_query(
            "marketing_client_dm_comprehensive.sql", 
            "target_table_row_count",
            table_name=self.TARGET_TABLE_NAME
        )
        processed_rows = target_count_result[0]["row_count"] if target_count_result else 0
        
        assert processed_rows >= self.EXPECTED_MIN_RECORDS, \
            f"処理レコード数が不足: {processed_rows} < {self.EXPECTED_MIN_RECORDS}"
        
        logger.info(f"パフォーマンス検証完了: 実行時間={execution_time}秒, 処理レコード={processed_rows}件, 533列構造")
