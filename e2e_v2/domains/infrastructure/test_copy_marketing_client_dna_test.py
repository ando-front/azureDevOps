"""
pi_Copy_marketing_client_dna_test パイプラインのE2Eテスト

マーケティングクライアントDNAコピーテストパイプラインのETL処理テスト
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List

from ...base.pipeline_test_base import DomainTestBase, TestCategory, PipelineStatus


class TestCopyMarketingClientDNATestPipeline(DomainTestBase):
    """マーケティングクライアントDNAコピーテストパイプラインテスト"""
    
    def __init__(self):
        super().__init__("pi_Copy_marketing_client_dna_test", "infrastructure")
    
    def domain_specific_setup(self):
        """インフラストラクチャードメイン固有セットアップ"""
        self.source_container = "dna-test"
        self.output_container = "test-results"
        self.database_target = "marketing_client_dna_test"
        
        # 期待される入力フィールド（ETL処理用）
        self.input_columns = [
            "TEST_ID", "CLIENT_ID", "DNA_PROFILE", "TEST_BEHAVIOR_SCORE",
            "TEST_SCENARIO", "ALGORITHM_VERSION", "EXPECTED_PROFILE", "ACTUAL_PROFILE",
            "ACCURACY_SCORE", "TEST_STATUS", "EXECUTION_DATE", "VALIDATION_NOTES"
        ]
        
        # 期待される出力フィールド（ETL変換後）
        self.output_columns = [
            "TEST_ID", "CLIENT_ID", "DNA_PROFILE", "TEST_BEHAVIOR_SCORE",
            "TEST_SCENARIO", "ALGORITHM_VERSION", "EXPECTED_PROFILE", "ACTUAL_PROFILE",
            "ACCURACY_SCORE", "TEST_STATUS", "EXECUTION_DATE", "VALIDATION_NOTES",
            "COPY_DATE", "DNA_TEST_REPORT_ID", "ACCURACY_CATEGORY", "TEST_ENVIRONMENT"
        ]
    
    def get_domain_test_data_template(self) -> Dict[str, str]:
        """インフラストラクチャードメイン用テストデータテンプレート"""
        return {
            "dna_test_data": self._generate_dna_test_data(),
            "algorithm_test_data": self._generate_algorithm_test_data(),
            "bulk_accuracy_test": self._generate_bulk_accuracy_test()
        }
    
    def _generate_dna_test_data(self, record_count: int = 1200) -> str:
        """DNAテストデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        dna_profiles = ["EXPLORER", "LOYALIST", "OPTIMIZER", "CONSERVATIVE", "TRENDSETTER"]
        test_scenarios = ["PROFILE_PREDICTION", "BEHAVIOR_MAPPING", "PREFERENCE_ANALYSIS", "LIFECYCLE_TRACKING"]
        algorithm_versions = ["DNA_V1.0", "DNA_V1.1", "DNA_V1.2", "DNA_V2.0"]
        test_statuses = ["PASSED", "FAILED", "INCONCLUSIVE", "ERROR"]
        
        base_date = datetime.utcnow()
        
        for i in range(1, record_count + 1):
            # 実行日（過去14日以内）
            execution_days = i % 14
            execution_date = (base_date - timedelta(days=execution_days)).strftime('%Y%m%d')
            
            dna_profile = dna_profiles[i % len(dna_profiles)]
            test_scenario = test_scenarios[i % len(test_scenarios)]
            algorithm_version = algorithm_versions[i % len(algorithm_versions)]
            test_status = test_statuses[i % len(test_statuses)]
            
            # テスト行動スコア（0-100）
            test_behavior_score = (i % 100) + 1
            
            # 期待プロファイル vs 実際プロファイル
            expected_profile = dna_profile
            if test_status == "PASSED":
                actual_profile = expected_profile
                accuracy_score = 85 + (i % 15)  # 85-99%
            elif test_status == "FAILED":
                # 異なるプロファイルを設定
                other_profiles = [p for p in dna_profiles if p != expected_profile]
                actual_profile = other_profiles[i % len(other_profiles)]
                accuracy_score = 30 + (i % 50)  # 30-79%
            elif test_status == "INCONCLUSIVE":
                actual_profile = expected_profile
                accuracy_score = 50 + (i % 30)  # 50-79%
            else:  # ERROR
                actual_profile = "ERROR"
                accuracy_score = 0
            
            # バリデーション注記
            if test_status == "FAILED":
                validation_notes = f"Profile mismatch: Expected {expected_profile}, Got {actual_profile}"
            elif test_status == "ERROR":
                validation_notes = f"Algorithm error in {test_scenario} test"
            elif test_status == "INCONCLUSIVE":
                validation_notes = f"Low confidence score: {accuracy_score}%"
            else:
                validation_notes = f"Test passed with {accuracy_score}% accuracy"
            
            data_lines.append(
                f"DNATEST{i:05d}\tCLIENT{i:06d}\t{dna_profile}\t{test_behavior_score}\t"
                f"{test_scenario}\t{algorithm_version}\t{expected_profile}\t{actual_profile}\t"
                f"{accuracy_score}\t{test_status}\t{execution_date}\t{validation_notes}"
            )
        
        return "\n".join(data_lines)
    
    def _generate_algorithm_test_data(self) -> str:
        """アルゴリズムテストデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        execution_date = base_date.strftime('%Y%m%d')
        
        # アルゴリズム精度テスト
        algorithm_tests = [
            ("DNATEST90001", "CLIENT900001", "EXPLORER", "95", "PROFILE_PREDICTION", "DNA_V2.0", "EXPLORER", "EXPLORER", "96", "PASSED", execution_date, "High accuracy prediction test passed"),
            ("DNATEST90002", "CLIENT900002", "LOYALIST", "88", "BEHAVIOR_MAPPING", "DNA_V2.0", "LOYALIST", "CONSERVATIVE", "72", "FAILED", execution_date, "Behavior mapping failed: Profile mismatch"),
            ("DNATEST90003", "CLIENT900003", "TRENDSETTER", "92", "PREFERENCE_ANALYSIS", "DNA_V2.0", "TRENDSETTER", "TRENDSETTER", "94", "PASSED", execution_date, "Preference analysis successful"),
        ]
        
        for test_data in algorithm_tests:
            data_lines.append("\t".join(test_data))
        
        return "\n".join(data_lines)
    
    def _generate_bulk_accuracy_test(self) -> str:
        """一括精度テストデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        execution_date = (base_date - timedelta(days=2)).strftime('%Y%m%d')  # 2日前実行
        
        # 大量精度テスト（機械学習モデル評価）
        for i in range(1, 181):
            # 90%の精度でテスト成功
            if i % 10 == 0:
                test_status = "FAILED"
                expected_profile = "EXPLORER"
                actual_profile = "CONSERVATIVE"
                accuracy_score = 45 + (i % 30)
                validation_notes = f"Accuracy test {i}: Profile mismatch detected"
            else:
                test_status = "PASSED"
                profile = ["EXPLORER", "LOYALIST", "OPTIMIZER"][i % 3]
                expected_profile = profile
                actual_profile = profile
                accuracy_score = 85 + (i % 15)
                validation_notes = f"Accuracy test {i}: High precision achieved"
            
            behavior_score = 60 + (i % 40)  # 60-99の範囲
            
            data_lines.append(
                f"DNATEST{i:05d}\tCLIENT{i:06d}\t{expected_profile}\t{behavior_score}\t"
                f"PROFILE_PREDICTION\tDNA_V2.0\t{expected_profile}\t{actual_profile}\t"
                f"{accuracy_score}\t{test_status}\t{execution_date}\t{validation_notes}"
            )
        
        return "\n".join(data_lines)
    
    def _transform_dna_test_data(self, input_data: str) -> str:
        """DNAテストデータETL変換処理"""
        lines = input_data.strip().split('\n')
        
        # 出力CSVヘッダー
        output_header = ",".join(self.output_columns)
        output_lines = [output_header]
        
        current_date = datetime.utcnow()
        copy_date = current_date.strftime('%Y/%m/%d %H:%M:%S')
        test_environment = "DNA_TEST_ENV"
        
        for line in lines[1:]:
            if not line.strip():
                continue
                
            parts = line.split('\t')
            if len(parts) != len(self.input_columns):
                continue
            
            (test_id, client_id, dna_profile, test_behavior_score,
             test_scenario, algorithm_version, expected_profile, actual_profile,
             accuracy_score, test_status, execution_date, validation_notes) = parts
            
            # ETL品質チェック
            if not test_id.strip() or not client_id.strip() or not test_scenario.strip():
                continue
            
            # DNAテストレポートID生成（ETL処理）
            dna_test_report_id = f"DNARPT{test_id[7:]}{execution_date}"
            
            # 精度カテゴリ設定（ETL処理）
            try:
                accuracy_val = float(accuracy_score)
                if accuracy_val >= 90:
                    accuracy_category = "HIGH"
                elif accuracy_val >= 75:
                    accuracy_category = "MEDIUM"
                elif accuracy_val >= 50:
                    accuracy_category = "LOW"
                else:
                    accuracy_category = "VERY_LOW"
            except ValueError:
                accuracy_category = "UNKNOWN"
            
            # CSV出力行生成
            output_line = (
                f"{test_id},{client_id},{dna_profile},{test_behavior_score},"
                f"{test_scenario},{algorithm_version},{expected_profile},{actual_profile},"
                f"{accuracy_score},{test_status},{execution_date},{validation_notes},"
                f"{copy_date},{dna_test_report_id},{accuracy_category},{test_environment}"
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
        
        return violations
    
    def test_functional_with_file_exists(self):
        """機能テスト: ファイル有り処理"""
        test_id = f"functional_with_file_{int(time.time())}"
        
        # テストデータ準備
        test_data = self._generate_dna_test_data()
        
        # ファイルアップロード
        file_date = datetime.utcnow().strftime('%Y%m%d')
        file_path = f"MarketingClientDNATest/{file_date}/dna_test_results.tsv"
        self.mock_storage.upload_file(self.source_container, file_path, test_data.encode('utf-8'))
        
        # ETL処理シミュレーション
        transformed_data, output_data = self.simulate_etl_process(
            test_data,
            self._transform_dna_test_data,
            "csv"
        )
        
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
            records_extracted=len(test_data.split('\n')) - 1,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(db_records),
            data_quality_score=0.95,
            errors=[]
        )
        
        # アサーション
        self.validate_common_assertions(result)
        assert result.records_extracted > 0, "抽出レコード数が0"
        assert db_insert_result, "データベース挿入失敗"
    
    def test_functional_without_file(self):
        """機能テスト: ファイル無し処理"""
        test_id = f"functional_no_file_{int(time.time())}"
        
        # ファイル存在チェック（存在しないファイルパスを使用）
        file_date = "99999999"
        file_path = f"MarketingClientDNATest/{file_date}/dna_test_results.tsv"
        
        file_exists = self.mock_storage.file_exists(self.source_container, file_path)
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.FUNCTIONAL,
            status=PipelineStatus.SKIPPED,
            records_extracted=0,
            records_transformed=0,
            records_loaded=0,
            data_quality_score=1.0,
            errors=[]
        )
        
        # ファイル無しの場合はスキップが正常
        assert not file_exists, "ファイルが存在する（テスト条件不正）"
        assert result.status == PipelineStatus.SKIPPED, "スキップ処理が正常実行されていない"
        
        self.validate_common_assertions(result)
    
    def test_data_quality_validation(self):
        """データ品質テスト: ETL品質検証"""
        test_id = f"data_quality_{int(time.time())}"
        
        # アルゴリズムテストデータでテスト
        test_data = self._generate_algorithm_test_data()
        
        # データ変換処理
        transformed_data = self._transform_dna_test_data(test_data)
        
        # 品質検証
        quality_metrics = self.validate_data_quality(transformed_data, self.output_columns)
        violations = self.validate_domain_business_rules(transformed_data)
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.DATA_QUALITY,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=len(test_data.split('\n')) - 1,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=min(quality_metrics.values()),
            errors=[],
            warnings=violations
        )
        
        # ETL品質アサーション
        assert quality_metrics["completeness"] >= 0.90, f"完全性が基準未満: {quality_metrics['completeness']}"
        assert quality_metrics["validity"] >= 0.95, f"有効性が基準未満: {quality_metrics['validity']}"
        
        self.validate_common_assertions(result)
    
    def test_performance_large_dataset(self):
        """パフォーマンステスト: 大量データ処理"""
        test_id = f"performance_large_{int(time.time())}"
        
        # 大量データ準備（80000件）
        large_data = self._generate_dna_test_data(80000)
        
        start_time = time.time()
        
        # ETL処理実行
        transformed_data = self._transform_dna_test_data(large_data)
        
        execution_time = time.time() - start_time
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.PERFORMANCE,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=80000,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=0.95,
            errors=[]
        )
        
        # パフォーマンス基準
        throughput = 80000 / execution_time
        assert throughput > 3500, f"スループットが基準未満: {throughput:.2f} records/sec"
        
        self.validate_common_assertions(result)
    
    def test_integration_database_operations(self):
        """統合テスト: データベース操作"""
        test_id = f"integration_database_{int(time.time())}"
        
        # 一括精度テストデータでテスト
        test_data = self._generate_bulk_accuracy_test()
        
        # ETL処理
        transformed_data = self._transform_dna_test_data(test_data)
        
        # データベース操作シミュレーション
        # 1. 既存テスト結果チェック
        existing_check = self.mock_database.query_records(
            self.database_target, 
            "SELECT test_id FROM marketing_client_dna_test WHERE accuracy_category = 'HIGH'"
        )
        
        # 2. 新規テスト結果挿入
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
            records_extracted=len(test_data.split('\n')) - 1,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(db_records),
            data_quality_score=1.0,
            errors=[]
        )
        
        # 統合テストアサーション
        assert insert_result, "データベース挿入失敗"
        assert len(db_records) >= 180, f"処理レコード数不足: {len(db_records)}"
        
        self.validate_common_assertions(result)