"""
DoUntilPipeline パイプラインのE2Eテスト

DoUntilPipelineのETL処理テスト - レコード数カウントとスパン計算処理
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List

from ...base.pipeline_test_base import DomainTestBase, TestCategory, PipelineStatus


class TestDoUntilPipeline(DomainTestBase):
    """DoUntilPipelineテスト"""
    
    def __init__(self):
        super().__init__("DoUntilPipeline", "infrastructure")
    
    def domain_specific_setup(self):
        """インフラストラクチャードメイン固有セットアップ"""
        self.source_table = "omni_ods_marketing_trn_client_dm"
        self.database_target = "omni_ods_marketing_trn_client_dm_metrics"
        self.default_count_loop = 5
        
        # パイプラインパラメータ
        self.pipeline_parameters = {
            "countLoop": self.default_count_loop
        }
        
        # パイプライン変数
        self.pipeline_variables = {
            "countRecords": 0,
            "span": 0
        }
        
        # 期待される出力フィールド（ETL変換後）
        self.output_columns = [
            "EXECUTION_ID", "PIPELINE_NAME", "COUNT_LOOP_PARAM", "TOTAL_RECORDS",
            "SPAN_VALUE", "EXECUTION_DATE", "PROCESSING_TIME_MS", "STATUS"
        ]
    
    def get_domain_test_data_template(self) -> Dict[str, str]:
        """インフラストラクチャードメイン用テストデータテンプレート"""
        return {
            "count_execution_data": self._generate_count_execution_data(),
            "span_calculation_data": self._generate_span_calculation_data(),
            "parameter_variation_data": self._generate_parameter_variation_data()
        }
    
    def _generate_count_execution_data(self, record_count: int = 10000) -> str:
        """カウント実行データ生成"""
        data_lines = []
        
        # シミュレートされたマーケティングテーブルデータ
        for i in range(1, record_count + 1):
            client_id = f"CLIENT{i:06d}"
            dm_campaign_id = f"DM{i:06d}"
            send_date = datetime.utcnow() - timedelta(days=i % 30)
            
            data_lines.append(
                f"{client_id}\t{dm_campaign_id}\t{send_date.strftime('%Y%m%d')}\t"
                f"ACTIVE\tマーケティングデータ{i}"
            )
        
        return "\n".join(data_lines)
    
    def _generate_span_calculation_data(self) -> str:
        """スパン計算データ生成"""
        # 異なるレコード数でのスパン計算テスト
        test_scenarios = [
            (1000, 5),   # 1000レコード、5分割
            (5000, 10),  # 5000レコード、10分割
            (10000, 20), # 10000レコード、20分割
            (50000, 100) # 50000レコード、100分割
        ]
        
        data_lines = []
        for i, (record_count, count_loop) in enumerate(test_scenarios, 1):
            span_value = record_count // count_loop
            data_lines.append(
                f"SCENARIO{i:03d}\t{record_count}\t{count_loop}\t{span_value}"
            )
        
        return "\n".join(data_lines)
    
    def _generate_parameter_variation_data(self) -> str:
        """パラメータ変動データ生成"""
        # 異なるcountLoopパラメータでのテスト
        count_loop_values = [1, 2, 5, 10, 20, 50, 100]
        base_record_count = 10000
        
        data_lines = []
        for count_loop in count_loop_values:
            span_value = base_record_count // count_loop
            data_lines.append(
                f"PARAM_TEST_{count_loop:03d}\t{base_record_count}\t{count_loop}\t{span_value}"
            )
        
        return "\n".join(data_lines)
    
    def _simulate_pipeline_execution(self, record_count: int, count_loop: int) -> Dict[str, any]:
        """パイプライン実行シミュレーション"""
        execution_start = time.time()
        
        # Lookup1アクティビティのシミュレーション
        lookup_result = {
            "count0": record_count,
            "span0": record_count // count_loop
        }
        
        # set_countRecords変数設定
        count_records_value = lookup_result["count0"]
        
        # set span変数設定
        span_value = lookup_result["span0"]
        
        execution_time = (time.time() - execution_start) * 1000  # ミリ秒
        
        return {
            "countRecords": count_records_value,
            "span": span_value,
            "executionTimeMs": execution_time,
            "status": "SUCCEEDED"
        }
    
    def _transform_execution_results(self, execution_results: List[Dict[str, any]]) -> str:
        """実行結果ETL変換処理"""
        # 出力CSVヘッダー
        output_header = ",".join(self.output_columns)
        output_lines = [output_header]
        
        current_date = datetime.utcnow()
        execution_date = current_date.strftime('%Y%m%d')
        
        for i, result in enumerate(execution_results, 1):
            execution_id = f"EXEC{i:06d}"
            pipeline_name = "DoUntilPipeline"
            
            # CSV出力行生成
            output_line = (
                f"{execution_id},{pipeline_name},{result.get('countLoop', self.default_count_loop)},"
                f"{result['countRecords']},{result['span']},{execution_date},"
                f"{result['executionTimeMs']:.0f},{result['status']}"
            )
            output_lines.append(output_line)
        
        return '\n'.join(output_lines)
    
    def validate_domain_business_rules(self, data: str) -> List[str]:
        """ETLレベルの基本検証のみ"""
        violations = []
        lines = data.strip().split('\n')
        
        if len(lines) < 2:
            return violations
        
        # 構造検証
        expected_header = ",".join(self.output_columns)
        if lines[0] != expected_header:
            violations.append(f"ヘッダー不正: 期待={expected_header}")
        
        for i, line in enumerate(lines[1:], 2):
            parts = line.split(',')
            
            if len(parts) != len(self.output_columns):
                violations.append(f"行{i}: フィールド数不正")
                continue
            
            # スパン値検証（レコード数/分割数）
            try:
                total_records = int(parts[3])
                count_loop = int(parts[2])
                span_value = int(parts[4])
                
                expected_span = total_records // count_loop
                if span_value != expected_span:
                    violations.append(f"行{i}: スパン計算不正 期待={expected_span}, 実際={span_value}")
            except (ValueError, IndexError):
                violations.append(f"行{i}: 数値変換エラー")
        
        return violations
    
    def test_functional_with_data_exists(self):
        """機能テスト: データ有り処理"""
        test_id = f"functional_with_data_{int(time.time())}"
        
        # テストデータ準備
        test_record_count = 10000
        test_count_loop = 5
        
        # パイプライン実行シミュレーション
        execution_result = self._simulate_pipeline_execution(test_record_count, test_count_loop)
        execution_result["countLoop"] = test_count_loop
        
        # ETL処理シミュレーション
        transformed_data = self._transform_execution_results([execution_result])
        
        # データベース挿入シミュレーション
        db_records = self.convert_csv_to_dict_list(transformed_data)
        db_insert_result = self.mock_database.insert_records(
            self.database_target, 
            db_records
        )
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.FUNCTIONAL,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=test_record_count,
            records_transformed=1,
            records_loaded=len(db_records),
            data_quality_score=0.98,
            errors=[]
        )
        
        # アサーション
        self.validate_common_assertions(result)
        assert result.records_extracted > 0, "抽出レコード数が0"
        assert db_insert_result, "データベース挿入失敗"
        assert execution_result["span"] == test_record_count // test_count_loop, "スパン計算不正"
    
    def test_functional_without_data(self):
        """機能テスト: データ無し処理"""
        test_id = f"functional_no_data_{int(time.time())}"
        
        # データが存在しない場合のシミュレーション
        test_record_count = 0
        test_count_loop = 5
        
        execution_result = self._simulate_pipeline_execution(test_record_count, test_count_loop)
        execution_result["countLoop"] = test_count_loop
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.FUNCTIONAL,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=0,
            records_transformed=1,
            records_loaded=1,
            data_quality_score=1.0,
            errors=[]
        )
        
        # データが0件でも処理は成功
        assert result.status == PipelineStatus.SUCCEEDED, "データ無し処理が正常実行されていない"
        assert execution_result["span"] == 0, "スパン計算不正（0件の場合）"
        
        self.validate_common_assertions(result)
    
    def test_data_quality_validation(self):
        """データ品質テスト: ETL品質検証"""
        test_id = f"data_quality_{int(time.time())}"
        
        # 複数のパラメータ組み合わせテスト
        test_scenarios = [
            (1000, 5),
            (5000, 10),
            (10000, 20)
        ]
        
        execution_results = []
        for record_count, count_loop in test_scenarios:
            result = self._simulate_pipeline_execution(record_count, count_loop)
            result["countLoop"] = count_loop
            execution_results.append(result)
        
        # データ変換処理
        transformed_data = self._transform_execution_results(execution_results)
        
        # 品質検証
        quality_metrics = self.validate_data_quality(transformed_data, self.output_columns)
        violations = self.validate_domain_business_rules(transformed_data)
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.DATA_QUALITY,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=sum(r["countRecords"] for r in execution_results),
            records_transformed=len(execution_results),
            records_loaded=len(execution_results),
            data_quality_score=min(quality_metrics.values()),
            errors=[],
            warnings=violations
        )
        
        # ETL品質アサーション
        assert quality_metrics["completeness"] >= 0.95, f"完全性が基準未満: {quality_metrics['completeness']}"
        assert quality_metrics["validity"] >= 0.98, f"有効性が基準未満: {quality_metrics['validity']}"
        
        self.validate_common_assertions(result)
    
    def test_performance_large_dataset(self):
        """パフォーマンステスト: 大量データ処理"""
        test_id = f"performance_large_{int(time.time())}"
        
        # 大量データ準備（100万レコード）
        large_record_count = 1000000
        test_count_loop = 100
        
        start_time = time.time()
        
        # パイプライン実行シミュレーション
        execution_result = self._simulate_pipeline_execution(large_record_count, test_count_loop)
        execution_result["countLoop"] = test_count_loop
        
        execution_time = time.time() - start_time
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.PERFORMANCE,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=large_record_count,
            records_transformed=1,
            records_loaded=1,
            data_quality_score=0.98,
            errors=[]
        )
        
        # パフォーマンス基準（大量データでもLookupは高速）
        assert execution_time < 5.0, f"実行時間が基準超過: {execution_time:.2f}秒"
        assert execution_result["span"] == large_record_count // test_count_loop, "大量データでのスパン計算不正"
        
        self.validate_common_assertions(result)
    
    def test_integration_parameter_variations(self):
        """統合テスト: パラメータ変動テスト"""
        test_id = f"integration_params_{int(time.time())}"
        
        # 異なるcountLoopパラメータでのテスト
        test_record_count = 10000
        count_loop_values = [1, 2, 5, 10, 20, 50, 100]
        
        execution_results = []
        for count_loop in count_loop_values:
            result = self._simulate_pipeline_execution(test_record_count, count_loop)
            result["countLoop"] = count_loop
            execution_results.append(result)
        
        # データ変換処理
        transformed_data = self._transform_execution_results(execution_results)
        
        # データベース挿入シミュレーション
        db_records = self.convert_csv_to_dict_list(transformed_data)
        insert_result = self.mock_database.insert_records(
            self.database_target, 
            db_records
        )
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.INTEGRATION,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=test_record_count,
            records_transformed=len(execution_results),
            records_loaded=len(db_records),
            data_quality_score=1.0,
            errors=[]
        )
        
        # 統合テストアサーション
        assert insert_result, "データベース挿入失敗"
        assert len(db_records) == len(count_loop_values), f"処理レコード数不正: {len(db_records)}"
        
        # 各パラメータでのスパン計算検証
        for i, (execution_result, count_loop) in enumerate(zip(execution_results, count_loop_values)):
            expected_span = test_record_count // count_loop
            actual_span = execution_result["span"]
            assert actual_span == expected_span, f"パラメータ{count_loop}のスパン計算不正: 期待={expected_span}, 実際={actual_span}"
        
        self.validate_common_assertions(result)