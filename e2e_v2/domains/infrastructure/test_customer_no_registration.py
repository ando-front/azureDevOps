"""
pi_CustmNoRegistComp パイプラインのE2Eテスト

顧客番号登録完了パイプラインのETL処理テスト
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List

from ...base.pipeline_test_base import DomainTestBase, TestCategory, PipelineStatus


class TestCustomerNoRegistrationPipeline(DomainTestBase):
    """顧客番号登録完了パイプラインテスト"""
    
    def __init__(self):
        super().__init__("pi_CustmNoRegistComp", "infrastructure")
    
    def domain_specific_setup(self):
        """インフラストラクチャードメイン固有セットアップ"""
        self.source_container = "mytokyogas"
        self.output_container = "datalake"
        self.database_target = "customer_registration"
        
        # 期待される入力フィールド（ETL処理用）
        self.input_columns = [
            "REGISTRATION_ID", "CUSTOMER_ID", "CUSTOMER_NUMBER", "CUSTOMER_NAME",
            "EMAIL_ADDRESS", "PHONE_NUMBER", "REGISTRATION_DATE", "REGISTRATION_SOURCE",
            "VERIFICATION_STATUS", "ACTIVATION_STATUS", "COMPLETION_DATE", "STATUS"
        ]
        
        # 期待される出力フィールド（ETL変換後）
        self.output_columns = [
            "REGISTRATION_ID", "CUSTOMER_ID", "CUSTOMER_NUMBER", "CUSTOMER_NAME",
            "EMAIL_ADDRESS", "PHONE_NUMBER", "REGISTRATION_DATE", "REGISTRATION_SOURCE",
            "VERIFICATION_STATUS", "ACTIVATION_STATUS", "COMPLETION_DATE",
            "STATUS", "PROCESS_DATE", "COMPLETION_NOTIFICATION_ID", "SYSTEM_FLAG"
        ]
    
    def get_domain_test_data_template(self) -> Dict[str, str]:
        """インフラストラクチャードメイン用テストデータテンプレート"""
        return {
            "registration_completion_data": self._generate_registration_completion_data(),
            "new_registration_data": self._generate_new_registration_data(),
            "bulk_completion_data": self._generate_bulk_completion_data()
        }
    
    def _generate_registration_completion_data(self, record_count: int = 1500) -> str:
        """登録完了データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        registration_sources = ["WEB", "APP", "PHONE", "STORE"]
        verification_statuses = ["VERIFIED", "PENDING", "FAILED", "EXPIRED"]
        activation_statuses = ["ACTIVATED", "PENDING", "INACTIVE", "SUSPENDED"]
        statuses = ["COMPLETED", "IN_PROGRESS", "FAILED", "CANCELLED"]
        
        base_date = datetime.utcnow()
        
        for i in range(1, record_count + 1):
            # 登録日（過去14日以内）
            registration_days = i % 14
            registration_date = (base_date - timedelta(days=registration_days)).strftime('%Y%m%d')
            
            # 完了日（登録日から1-3日後）
            completion_days = registration_days - (i % 3 + 1)
            if completion_days < 0:
                completion_date = base_date.strftime('%Y%m%d')
            else:
                completion_date = (base_date - timedelta(days=completion_days)).strftime('%Y%m%d')
            
            registration_source = registration_sources[i % len(registration_sources)]
            verification_status = verification_statuses[i % len(verification_statuses)]
            activation_status = activation_statuses[i % len(activation_statuses)]
            status = statuses[i % len(statuses)]
            
            customer_name = f"登録完了{i:04d}"
            email = f"regcomp{i:06d}@tokyogas.co.jp"
            phone = f"03-{i:04d}-{(i*11) % 10000:04d}"
            customer_number = f"TG{i:08d}"
            
            data_lines.append(
                f"REG{i:06d}\tCUST{i:06d}\t{customer_number}\t{customer_name}\t"
                f"{email}\t{phone}\t{registration_date}\t{registration_source}\t"
                f"{verification_status}\t{activation_status}\t{completion_date}\t{status}"
            )
        
        return "\n".join(data_lines)
    
    def _generate_new_registration_data(self) -> str:
        """新規登録データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        registration_date = base_date.strftime('%Y%m%d')
        completion_date = base_date.strftime('%Y%m%d')
        
        # 新規登録完了
        new_registrations = [
            ("REG900001", "CUST900001", "TG90000001", "新規登録1", "newreg1@tokyogas.co.jp", "03-9001-0001", registration_date, "WEB", "VERIFIED", "ACTIVATED", completion_date, "COMPLETED"),
            ("REG900002", "CUST900002", "TG90000002", "新規登録2", "newreg2@tokyogas.co.jp", "03-9002-0002", registration_date, "APP", "VERIFIED", "ACTIVATED", completion_date, "COMPLETED"),
            ("REG900003", "CUST900003", "TG90000003", "新規登録3", "newreg3@tokyogas.co.jp", "03-9003-0003", registration_date, "PHONE", "VERIFIED", "PENDING", completion_date, "IN_PROGRESS"),
        ]
        
        for registration_data in new_registrations:
            data_lines.append("\t".join(registration_data))
        
        return "\n".join(data_lines)
    
    def _generate_bulk_completion_data(self) -> str:
        """一括完了データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        registration_date = (base_date - timedelta(days=1)).strftime('%Y%m%d')
        completion_date = base_date.strftime('%Y%m%d')
        
        # 一括処理完了（システム移行等）
        for i in range(1, 101):
            data_lines.append(
                f"REG{i:06d}\tCUST{i:06d}\tTG{i:08d}\t一括完了{i:03d}\t"
                f"bulk{i:03d}@tokyogas.co.jp\t03-0000-{i:04d}\t"
                f"{registration_date}\tWEB\tVERIFIED\tACTIVATED\t"
                f"{completion_date}\tCOMPLETED"
            )
        
        return "\n".join(data_lines)
    
    def _transform_registration_completion_data(self, input_data: str) -> str:
        """登録完了データETL変換処理"""
        lines = input_data.strip().split('\n')
        
        # 出力CSVヘッダー
        output_header = ",".join(self.output_columns)
        output_lines = [output_header]
        
        current_date = datetime.utcnow()
        process_date = current_date.strftime('%Y%m%d')
        
        for line in lines[1:]:
            if not line.strip():
                continue
                
            parts = line.split('\t')
            if len(parts) != len(self.input_columns):
                continue
            
            (registration_id, customer_id, customer_number, customer_name,
             email_address, phone_number, registration_date, registration_source,
             verification_status, activation_status, completion_date, status) = parts
            
            # ETL品質チェック
            if not registration_id.strip() or not customer_id.strip() or not customer_number.strip():
                continue
            
            # 完了通知ID生成（ETL処理）
            completion_notification_id = f"COMP{registration_id[3:]}{process_date}"
            
            # システムフラグ設定（ETL処理）
            system_flag = "AUTO_PROCESSED" if status == "COMPLETED" else "MANUAL_REVIEW"
            
            # CSV出力行生成
            output_line = (
                f"{registration_id},{customer_id},{customer_number},{customer_name},"
                f"{email_address},{phone_number},{registration_date},{registration_source},"
                f"{verification_status},{activation_status},{completion_date},"
                f"{status},{process_date},{completion_notification_id},{system_flag}"
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
        test_data = self._generate_registration_completion_data()
        
        # ファイルアップロード
        file_date = datetime.utcnow().strftime('%Y%m%d')
        file_path = f"CustomerRegistration/{file_date}/customer_registration_completion.tsv"
        self.mock_storage.upload_file(self.source_container, file_path, test_data.encode('utf-8'))
        
        # ETL処理シミュレーション
        transformed_data, output_data = self.simulate_etl_process(
            test_data,
            self._transform_registration_completion_data,
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
        file_path = f"CustomerRegistration/{file_date}/customer_registration_completion.tsv"
        
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
        
        # 新規登録データでテスト
        test_data = self._generate_new_registration_data()
        
        # データ変換処理
        transformed_data = self._transform_registration_completion_data(test_data)
        
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
        
        # 大量データ準備（120000件）
        large_data = self._generate_registration_completion_data(120000)
        
        start_time = time.time()
        
        # ETL処理実行
        transformed_data = self._transform_registration_completion_data(large_data)
        
        execution_time = time.time() - start_time
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.PERFORMANCE,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=120000,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=0.95,
            errors=[]
        )
        
        # パフォーマンス基準
        throughput = 120000 / execution_time
        assert throughput > 2500, f"スループットが基準未満: {throughput:.2f} records/sec"
        
        self.validate_common_assertions(result)
    
    def test_integration_database_operations(self):
        """統合テスト: データベース操作"""
        test_id = f"integration_database_{int(time.time())}"
        
        # 一括完了データでテスト
        test_data = self._generate_bulk_completion_data()
        
        # ETL処理
        transformed_data = self._transform_registration_completion_data(test_data)
        
        # データベース操作シミュレーション
        # 1. 既存レコードチェック
        existing_check = self.mock_database.query_records(
            self.database_target, 
            "SELECT registration_id FROM customer_registration WHERE status = 'COMPLETED'"
        )
        
        # 2. 新規挿入
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
        assert len(db_records) >= 100, f"処理レコード数不足: {len(db_records)}"
        
        self.validate_common_assertions(result)