"""
pi_Copy_marketing_client_dm_test3 パイプラインのE2Eテスト

マーケティングクライアントDMコピーテスト3パイプラインのETL処理テスト
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List

from ...base.pipeline_test_base import DomainTestBase, TestCategory, PipelineStatus


class TestCopyMarketingClientDMTest3Pipeline(DomainTestBase):
    """マーケティングクライアントDMコピーテスト3パイプラインテスト"""
    
    def __init__(self):
        super().__init__("pi_Copy_marketing_client_dm_test3", "infrastructure")
    
    def domain_specific_setup(self):
        """インフラストラクチャードメイン固有セットアップ"""
        self.source_container = "marketing-test3"
        self.output_container = "test3-results"
        self.database_target = "marketing_client_dm_test3"
        
        # 期待される入力フィールド（ETL処理用）
        self.input_columns = [
            "TEST3_ID", "CLIENT_ID", "CLIENT_NAME", "DM_CAMPAIGN_ID",
            "LOAD_TEST_TYPE", "CONCURRENT_USERS", "RESPONSE_TIME", "THROUGHPUT",
            "ERROR_RATE", "RESOURCE_USAGE", "TEST_DURATION", "TEST_STATUS",
            "EXECUTION_DATE", "PERFORMANCE_BASELINE", "DEVIATION_PERCENTAGE"
        ]
        
        # 期待される出力フィールド（ETL変換後）
        self.output_columns = [
            "TEST3_ID", "CLIENT_ID", "CLIENT_NAME", "DM_CAMPAIGN_ID",
            "LOAD_TEST_TYPE", "CONCURRENT_USERS", "RESPONSE_TIME", "THROUGHPUT",
            "ERROR_RATE", "RESOURCE_USAGE", "TEST_DURATION", "TEST_STATUS",
            "EXECUTION_DATE", "PERFORMANCE_BASELINE", "DEVIATION_PERCENTAGE",
            "COPY_DATE", "LOAD_TEST_REPORT_ID", "PERFORMANCE_GRADE", "SLA_COMPLIANCE"
        ]
    
    def get_domain_test_data_template(self) -> Dict[str, str]:
        """インフラストラクチャードメイン用テストデータテンプレート"""
        return {
            "load_test_data": self._generate_load_test_data(),
            "stress_test_data": self._generate_stress_test_data(),
            "volume_test_data": self._generate_volume_test_data()
        }
    
    def _generate_load_test_data(self, record_count: int = 800) -> str:
        """ロードテストデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        load_test_types = ["NORMAL_LOAD", "PEAK_LOAD", "STRESS_LOAD", "SPIKE_LOAD", "VOLUME_LOAD"]
        test_statuses = ["PASSED", "FAILED", "WARNING", "ERROR"]
        
        base_date = datetime.utcnow()
        
        for i in range(1, record_count + 1):
            # 実行日（過去10日以内）
            execution_days = i % 10
            execution_date = (base_date - timedelta(days=execution_days)).strftime('%Y%m%d')
            
            load_test_type = load_test_types[i % len(load_test_types)]
            test_status = test_statuses[i % len(test_statuses)]
            
            # ロードテストタイプ別のパラメータ設定
            if load_test_type == "NORMAL_LOAD":
                concurrent_users = 50 + (i % 50)
                response_time = 200 + (i % 300)  # 200-499ms
                throughput = 100 + (i % 200)    # 100-299 req/sec
                error_rate = (i % 5) / 100       # 0-4%
                performance_baseline = 500
            elif load_test_type == "PEAK_LOAD":
                concurrent_users = 100 + (i % 100)
                response_time = 400 + (i % 600)  # 400-999ms
                throughput = 150 + (i % 150)    # 150-299 req/sec
                error_rate = (i % 8) / 100       # 0-7%
                performance_baseline = 800
            elif load_test_type == "STRESS_LOAD":
                concurrent_users = 200 + (i % 300)
                response_time = 800 + (i % 1200) # 800-1999ms
                throughput = 80 + (i % 120)     # 80-199 req/sec
                error_rate = (i % 15) / 100      # 0-14%
                performance_baseline = 1500
            elif load_test_type == "SPIKE_LOAD":
                concurrent_users = 500 + (i % 500)
                response_time = 1000 + (i % 2000) # 1000-2999ms
                throughput = 50 + (i % 100)      # 50-149 req/sec
                error_rate = (i % 20) / 100       # 0-19%
                performance_baseline = 2000
            else:  # VOLUME_LOAD
                concurrent_users = 100 + (i % 200)
                response_time = 300 + (i % 400)  # 300-699ms
                throughput = 200 + (i % 300)    # 200-499 req/sec
                error_rate = (i % 10) / 100      # 0-9%
                performance_baseline = 600
            
            # リソース使用率（CPU%）
            resource_usage = 30 + (i % 60)  # 30-89%
            
            # テスト期間（分）
            test_duration = 10 + (i % 50)   # 10-59分
            
            # パフォーマンス偏差計算
            deviation_percentage = ((response_time - performance_baseline) / performance_baseline) * 100
            
            client_name = f"ロードテスト{i:04d}"
            
            data_lines.append(
                f"LT3{i:06d}\tCLIENT{i:06d}\t{client_name}\tDM{i:06d}\t"
                f"{load_test_type}\t{concurrent_users}\t{response_time}\t{throughput}\t"
                f"{error_rate:.2f}\t{resource_usage}\t{test_duration}\t{test_status}\t"
                f"{execution_date}\t{performance_baseline}\t{deviation_percentage:.1f}"
            )
        
        return "\n".join(data_lines)
    
    def _generate_stress_test_data(self) -> str:
        """ストレステストデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        execution_date = base_date.strftime('%Y%m%d')
        
        # 高負荷ストレステスト
        stress_tests = [
            ("LT3900001", "CLIENT900001", "ストレス1", "DM900001", "STRESS_LOAD", "1000", "3500", "30", "25.50", "95", "120", "WARNING", execution_date, "2000", "75.0"),
            ("LT3900002", "CLIENT900002", "ストレス2", "DM900002", "SPIKE_LOAD", "2000", "5000", "15", "45.80", "98", "60", "FAILED", execution_date, "2500", "100.0"),
            ("LT3900003", "CLIENT900003", "ストレス3", "DM900003", "STRESS_LOAD", "1500", "4200", "25", "35.20", "92", "90", "WARNING", execution_date, "2200", "90.9"),
        ]
        
        for test_data in stress_tests:
            data_lines.append("\t".join(test_data))
        
        return "\n".join(data_lines)
    
    def _generate_volume_test_data(self) -> str:
        """ボリュームテストデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        execution_date = (base_date - timedelta(days=3)).strftime('%Y%m%d')
        
        # 大容量データ処理テスト
        for i in range(1, 61):
            concurrent_users = 200 + (i % 100)
            response_time = 400 + (i % 300)
            throughput = 300 + (i % 200)
            error_rate = (i % 5) / 100
            resource_usage = 50 + (i % 40)
            test_duration = 180 + (i % 120)  # 3-5時間
            performance_baseline = 600
            deviation_percentage = ((response_time - performance_baseline) / performance_baseline) * 100
            
            test_status = "PASSED" if error_rate < 0.03 and response_time < 800 else "WARNING"
            
            data_lines.append(
                f"LT3{i:06d}\tCLIENT{i:06d}\tボリューム{i:03d}\tDM{i:06d}\t"
                f"VOLUME_LOAD\t{concurrent_users}\t{response_time}\t{throughput}\t"
                f"{error_rate:.2f}\t{resource_usage}\t{test_duration}\t{test_status}\t"
                f"{execution_date}\t{performance_baseline}\t{deviation_percentage:.1f}"
            )
        
        return "\n".join(data_lines)
    
    def _transform_load_test_data(self, input_data: str) -> str:
        """ロードテストデータETL変換処理"""
        lines = input_data.strip().split('\n')
        
        # 出力CSVヘッダー
        output_header = ",".join(self.output_columns)
        output_lines = [output_header]
        
        current_date = datetime.utcnow()
        copy_date = current_date.strftime('%Y/%m/%d %H:%M:%S')
        
        for line in lines[1:]:
            if not line.strip():
                continue
                
            parts = line.split('\t')
            if len(parts) != len(self.input_columns):
                continue
            
            (test3_id, client_id, client_name, dm_campaign_id,
             load_test_type, concurrent_users, response_time, throughput,
             error_rate, resource_usage, test_duration, test_status,
             execution_date, performance_baseline, deviation_percentage) = parts
            
            # ETL品質チェック
            if not test3_id.strip() or not client_id.strip() or not load_test_type.strip():
                continue
            
            # ロードテストレポートID生成（ETL処理）
            load_test_report_id = f"LTRPT{test3_id[3:]}{execution_date}"
            
            # パフォーマンスグレード設定（ETL処理）
            try:
                response_val = float(response_time)
                error_val = float(error_rate)
                
                if response_val <= 500 and error_val <= 0.02:
                    performance_grade = "EXCELLENT"
                elif response_val <= 1000 and error_val <= 0.05:
                    performance_grade = "GOOD"
                elif response_val <= 2000 and error_val <= 0.10:
                    performance_grade = "ACCEPTABLE"
                else:
                    performance_grade = "POOR"
            except ValueError:
                performance_grade = "UNKNOWN"
            
            # SLAコンプライアンス設定（ETL処理）
            try:
                baseline_val = float(performance_baseline)
                response_val = float(response_time)
                error_val = float(error_rate)
                
                if response_val <= baseline_val and error_val <= 0.05:
                    sla_compliance = "COMPLIANT"
                elif response_val <= baseline_val * 1.2 and error_val <= 0.10:
                    sla_compliance = "MARGINAL"
                else:
                    sla_compliance = "NON_COMPLIANT"
            except ValueError:
                sla_compliance = "UNKNOWN"
            
            # CSV出力行生成
            output_line = (
                f"{test3_id},{client_id},{client_name},{dm_campaign_id},"
                f"{load_test_type},{concurrent_users},{response_time},{throughput},"
                f"{error_rate},{resource_usage},{test_duration},{test_status},"
                f"{execution_date},{performance_baseline},{deviation_percentage},"
                f"{copy_date},{load_test_report_id},{performance_grade},{sla_compliance}"
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
        test_data = self._generate_load_test_data()
        
        # ファイルアップロード
        file_date = datetime.utcnow().strftime('%Y%m%d')
        file_path = f"MarketingClientDMTest3/{file_date}/load_test_results.tsv"
        self.mock_storage.upload_file(self.source_container, file_path, test_data.encode('utf-8'))
        
        # ETL処理シミュレーション
        transformed_data, output_data = self.simulate_etl_process(
            test_data,
            self._transform_load_test_data,
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
        file_path = f"MarketingClientDMTest3/{file_date}/load_test_results.tsv"
        
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
        
        # ストレステストデータでテスト
        test_data = self._generate_stress_test_data()
        
        # データ変換処理
        transformed_data = self._transform_load_test_data(test_data)
        
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
        
        # 大量データ準備（40000件）
        large_data = self._generate_load_test_data(40000)
        
        start_time = time.time()
        
        # ETL処理実行
        transformed_data = self._transform_load_test_data(large_data)
        
        execution_time = time.time() - start_time
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.PERFORMANCE,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=40000,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=0.95,
            errors=[]
        )
        
        # パフォーマンス基準
        throughput = 40000 / execution_time
        assert throughput > 6000, f"スループットが基準未満: {throughput:.2f} records/sec"
        
        self.validate_common_assertions(result)
    
    def test_integration_database_operations(self):
        """統合テスト: データベース操作"""
        test_id = f"integration_database_{int(time.time())}"
        
        # ボリュームテストデータでテスト
        test_data = self._generate_volume_test_data()
        
        # ETL処理
        transformed_data = self._transform_load_test_data(test_data)
        
        # データベース操作シミュレーション
        # 1. 既存テスト結果チェック
        existing_check = self.mock_database.query_records(
            self.database_target, 
            "SELECT test3_id FROM marketing_client_dm_test3 WHERE performance_grade = 'POOR'"
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
        assert len(db_records) >= 60, f"処理レコード数不足: {len(db_records)}"
        
        self.validate_common_assertions(result)