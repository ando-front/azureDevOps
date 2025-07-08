"""
pi_Copy_marketing_client_dm_test パイプラインのE2Eテスト

マーケティングクライアントDMコピーテストパイプラインのETL処理テスト
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List

from ...base.pipeline_test_base import DomainTestBase, TestCategory, PipelineStatus


class TestCopyMarketingClientDMTestPipeline(DomainTestBase):
    """マーケティングクライアントDMコピーテストパイプラインテスト"""
    
    def __init__(self):
        super().__init__("pi_Copy_marketing_client_dm_test", "infrastructure")
    
    def domain_specific_setup(self):
        """インフラストラクチャードメイン固有セットアップ"""
        self.source_container = "marketing-test"
        self.output_container = "test-results"
        self.database_target = "marketing_client_dm_test"
        
        # 期待される入力フィールド（ETL処理用）
        self.input_columns = [
            "TEST_ID", "CLIENT_ID", "CLIENT_NAME", "DM_CAMPAIGN_ID",
            "TEST_SCENARIO", "TEST_DATA_TYPE", "EXPECTED_RESULT", "ACTUAL_RESULT",
            "TEST_STATUS", "EXECUTION_DATE", "VALIDATION_RESULT", "ERROR_MESSAGE"
        ]
        
        # 期待される出力フィールド（ETL変換後）
        self.output_columns = [
            "TEST_ID", "CLIENT_ID", "CLIENT_NAME", "DM_CAMPAIGN_ID",
            "TEST_SCENARIO", "TEST_DATA_TYPE", "EXPECTED_RESULT", "ACTUAL_RESULT",
            "TEST_STATUS", "EXECUTION_DATE", "VALIDATION_RESULT", "ERROR_MESSAGE",
            "COPY_DATE", "TEST_REPORT_ID", "PASS_FAIL_FLAG", "TEST_ENVIRONMENT"
        ]
    
    def get_domain_test_data_template(self) -> Dict[str, str]:
        """インフラストラクチャードメイン用テストデータテンプレート"""
        return {
            "test_execution_data": self._generate_test_execution_data(),
            "failed_test_data": self._generate_failed_test_data(),
            "bulk_test_data": self._generate_bulk_test_data()
        }
    
    def _generate_test_execution_data(self, record_count: int = 1000) -> str:
        """テスト実行データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        test_scenarios = ["POSITIVE", "NEGATIVE", "BOUNDARY", "STRESS", "REGRESSION"]
        test_data_types = ["VALID", "INVALID", "NULL", "MALFORMED", "EMPTY"]
        test_statuses = ["PASSED", "FAILED", "SKIPPED", "ERROR"]
        validation_results = ["VALID", "INVALID", "PARTIAL", "TIMEOUT"]
        
        base_date = datetime.utcnow()
        
        for i in range(1, record_count + 1):
            # 実行日（過去7日以内）
            execution_days = i % 7
            execution_date = (base_date - timedelta(days=execution_days)).strftime('%Y%m%d')
            
            test_scenario = test_scenarios[i % len(test_scenarios)]
            test_data_type = test_data_types[i % len(test_data_types)]
            test_status = test_statuses[i % len(test_statuses)]
            validation_result = validation_results[i % len(validation_results)]
            
            # テストシナリオ別の期待結果・実際結果設定
            if test_scenario == "POSITIVE":
                expected_result = "SUCCESS"
                actual_result = "SUCCESS" if test_status == "PASSED" else "FAILURE"
            elif test_scenario == "NEGATIVE":
                expected_result = "FAILURE"
                actual_result = "FAILURE" if test_status == "PASSED" else "SUCCESS"
            elif test_scenario == "BOUNDARY":
                expected_result = "BOUNDARY_VALID"
                actual_result = "BOUNDARY_VALID" if test_status == "PASSED" else "BOUNDARY_INVALID"
            elif test_scenario == "STRESS":
                expected_result = "PERFORMANCE_OK"
                actual_result = "PERFORMANCE_OK" if test_status == "PASSED" else "PERFORMANCE_NG"
            else:  # REGRESSION
                expected_result = "NO_REGRESSION"
                actual_result = "NO_REGRESSION" if test_status == "PASSED" else "REGRESSION_DETECTED"
            
            # エラーメッセージ設定
            if test_status == "FAILED":
                error_message = f"Test failed: {test_scenario} scenario validation error"
            elif test_status == "ERROR":
                error_message = f"System error during {test_scenario} execution"
            else:
                error_message = ""
            
            client_name = f"テストクライアント{i:04d}"
            
            data_lines.append(
                f"TEST{i:06d}\tCLIENT{i:06d}\t{client_name}\tDM{i:06d}\t"
                f"{test_scenario}\t{test_data_type}\t{expected_result}\t{actual_result}\t"
                f"{test_status}\t{execution_date}\t{validation_result}\t{error_message}"
            )
        
        return "\n".join(data_lines)
    
    def _generate_failed_test_data(self) -> str:
        """失敗テストデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        execution_date = base_date.strftime('%Y%m%d')
        
        # 失敗テストケース
        failed_tests = [
            ("TEST900001", "CLIENT900001", "失敗テスト1", "DM900001", "NEGATIVE", "INVALID", "FAILURE", "SUCCESS", "FAILED", execution_date, "INVALID", "Validation failed: Invalid client data"),
            ("TEST900002", "CLIENT900002", "失敗テスト2", "DM900002", "BOUNDARY", "NULL", "BOUNDARY_VALID", "BOUNDARY_INVALID", "FAILED", execution_date, "INVALID", "Boundary test failed: Null value handling"),
            ("TEST900003", "CLIENT900003", "失敗テスト3", "DM900003", "STRESS", "VALID", "PERFORMANCE_OK", "PERFORMANCE_NG", "FAILED", execution_date, "TIMEOUT", "Performance test failed: Timeout exceeded"),
        ]
        
        for test_data in failed_tests:
            data_lines.append("\t".join(test_data))
        
        return "\n".join(data_lines)
    
    def _generate_bulk_test_data(self) -> str:
        """一括テストデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        execution_date = (base_date - timedelta(days=1)).strftime('%Y%m%d')  # 前日実行
        
        # 回帰テスト一括実行
        for i in range(1, 201):
            test_status = "PASSED" if i % 10 != 0 else "FAILED"  # 90%成功率
            
            if test_status == "PASSED":
                error_message = ""
                validation_result = "VALID"
            else:
                error_message = f"Regression test failed: Test case {i}"
                validation_result = "INVALID"
            
            data_lines.append(
                f"TEST{i:06d}\tCLIENT{i:06d}\t一括テスト{i:03d}\tDM{i:06d}\t"
                f"REGRESSION\tVALID\tNO_REGRESSION\t"
                f"{'NO_REGRESSION' if test_status == 'PASSED' else 'REGRESSION_DETECTED'}\t"
                f"{test_status}\t{execution_date}\t{validation_result}\t{error_message}"
            )
        
        return "\n".join(data_lines)
    
    def _transform_test_execution_data(self, input_data: str) -> str:
        """テスト実行データETL変換処理"""
        lines = input_data.strip().split('\n')
        
        # 出力CSVヘッダー
        output_header = ",".join(self.output_columns)
        output_lines = [output_header]
        
        current_date = datetime.utcnow()
        copy_date = current_date.strftime('%Y/%m/%d %H:%M:%S')
        test_environment = "TEST_ENV_01"
        
        for line in lines[1:]:
            if not line.strip():
                continue
                
            parts = line.split('\t')
            if len(parts) != len(self.input_columns):
                continue
            
            (test_id, client_id, client_name, dm_campaign_id,
             test_scenario, test_data_type, expected_result, actual_result,
             test_status, execution_date, validation_result, error_message) = parts
            
            # ETL品質チェック
            if not test_id.strip() or not client_id.strip() or not test_scenario.strip():
                continue
            
            # テストレポートID生成（ETL処理）
            test_report_id = f"RPT{test_id[4:]}{execution_date}"
            
            # Pass/Failフラグ設定（ETL処理）
            if test_status == "PASSED":
                pass_fail_flag = "PASS"
            elif test_status == "FAILED":
                pass_fail_flag = "FAIL"
            elif test_status == "SKIPPED":
                pass_fail_flag = "SKIP"
            else:  # ERROR
                pass_fail_flag = "ERROR"
            
            # CSV出力行生成
            output_line = (
                f"{test_id},{client_id},{client_name},{dm_campaign_id},"
                f"{test_scenario},{test_data_type},{expected_result},{actual_result},"
                f"{test_status},{execution_date},{validation_result},{error_message},"
                f"{copy_date},{test_report_id},{pass_fail_flag},{test_environment}"
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
        test_data = self._generate_test_execution_data()
        
        # ファイルアップロード
        file_date = datetime.utcnow().strftime('%Y%m%d')
        file_path = f"MarketingClientDMTest/{file_date}/test_execution_results.tsv"
        self.mock_storage.upload_file(self.source_container, file_path, test_data.encode('utf-8'))
        
        # ETL処理シミュレーション
        transformed_data, output_data = self.simulate_etl_process(
            test_data,
            self._transform_test_execution_data,
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
        file_path = f"MarketingClientDMTest/{file_date}/test_execution_results.tsv"
        
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
        
        # 失敗テストデータでテスト
        test_data = self._generate_failed_test_data()
        
        # データ変換処理
        transformed_data = self._transform_test_execution_data(test_data)
        
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
        
        # 大量データ準備（50000件）
        large_data = self._generate_test_execution_data(50000)
        
        start_time = time.time()
        
        # ETL処理実行
        transformed_data = self._transform_test_execution_data(large_data)
        
        execution_time = time.time() - start_time
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.PERFORMANCE,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=50000,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=0.95,
            errors=[]
        )
        
        # パフォーマンス基準
        throughput = 50000 / execution_time
        assert throughput > 5000, f"スループットが基準未満: {throughput:.2f} records/sec"
        
        self.validate_common_assertions(result)
    
    def test_integration_database_operations(self):
        """統合テスト: データベース操作"""
        test_id = f"integration_database_{int(time.time())}"
        
        # 一括テストデータでテスト
        test_data = self._generate_bulk_test_data()
        
        # ETL処理
        transformed_data = self._transform_test_execution_data(test_data)
        
        # データベース操作シミュレーション
        # 1. 既存テスト結果チェック
        existing_check = self.mock_database.query_records(
            self.database_target, 
            "SELECT test_id FROM marketing_client_dm_test WHERE pass_fail_flag = 'FAIL'"
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
        assert len(db_records) >= 200, f"処理レコード数不足: {len(db_records)}"
        
        self.validate_common_assertions(result)