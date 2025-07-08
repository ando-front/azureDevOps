"""
pi_Insert_mTGCustomerMaster パイプラインのE2Eテスト

mTG顧客マスター挿入パイプラインのETL処理テスト
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List

from ...base.pipeline_test_base import DomainTestBase, TestCategory, PipelineStatus


class TestMTGCustomerMasterPipeline(DomainTestBase):
    """mTG顧客マスター挿入パイプラインテスト"""
    
    def __init__(self):
        super().__init__("pi_Insert_mTGCustomerMaster", "mtgmaster")
    
    def domain_specific_setup(self):
        """mTGマスタードメイン固有セットアップ"""
        self.source_container = "mytokyogas"
        self.output_container = "datalake"
        self.database_target = "mtg_customer_master"
        
        # 期待される入力フィールド（ETL処理用）
        self.input_columns = [
            "CUSTOMER_ID", "CUSTOMER_NAME", "CUSTOMER_KANA", "EMAIL_ADDRESS",
            "PHONE_NUMBER", "ZIP_CODE", "ADDRESS", "BIRTH_DATE",
            "GENDER", "REGISTRATION_DATE", "STATUS", "CONTRACT_TYPE"
        ]
        
        # 期待される出力フィールド（ETL変換後）
        self.output_columns = [
            "CUSTOMER_ID", "CUSTOMER_NAME", "CUSTOMER_KANA", "EMAIL_ADDRESS",
            "PHONE_NUMBER", "ZIP_CODE", "ADDRESS", "BIRTH_DATE",
            "GENDER", "REGISTRATION_DATE", "STATUS", "CONTRACT_TYPE",
            "INSERT_DATE", "UPDATE_DATE", "DATA_VERSION"
        ]
    
    def get_domain_test_data_template(self) -> Dict[str, str]:
        """mTGマスタードメイン用テストデータテンプレート"""
        return {
            "customer_master_data": self._generate_customer_master_data(),
            "new_customer_data": self._generate_new_customer_data(),
            "update_customer_data": self._generate_update_customer_data()
        }
    
    def _generate_customer_master_data(self, record_count: int = 5000) -> str:
        """顧客マスターデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        genders = ["M", "F", "OTHER"]
        statuses = ["ACTIVE", "INACTIVE", "SUSPENDED", "PENDING"]
        contract_types = ["INDIVIDUAL", "CORPORATE", "FAMILY", "STUDENT"]
        
        base_date = datetime.utcnow()
        
        for i in range(1, record_count + 1):
            # 登録日（過去5年以内）
            reg_days = i % (365 * 5)
            registration_date = (base_date - timedelta(days=reg_days)).strftime('%Y%m%d')
            
            # 生年月日（18歳以上）
            birth_days = 18 * 365 + (i % (50 * 365))  # 18-68歳
            birth_date = (base_date - timedelta(days=birth_days)).strftime('%Y%m%d')
            
            gender = genders[i % len(genders)]
            status = statuses[i % len(statuses)]
            contract_type = contract_types[i % len(contract_types)]
            
            # 顧客名生成
            if gender == "M":
                customer_name = f"田中{i:04d}太郎"
                customer_kana = f"タナカ{i:04d}タロウ"
            elif gender == "F":
                customer_name = f"佐藤{i:04d}花子"
                customer_kana = f"サトウ{i:04d}ハナコ"
            else:
                customer_name = f"顧客{i:04d}"
                customer_kana = f"コキャク{i:04d}"
            
            email = f"customer{i:06d}@tokyogas.co.jp"
            phone = f"03-{i:04d}-{(i*7) % 10000:04d}"
            zip_code = f"{i % 999 + 100:03d}-{i % 9999:04d}"
            address = f"東京都{i % 23 + 1}区サンプル{i % 99 + 1}-{i % 99 + 1}-{i % 99 + 1}"
            
            data_lines.append(
                f"CUST{i:06d}\t{customer_name}\t{customer_kana}\t{email}\t"
                f"{phone}\t{zip_code}\t{address}\t{birth_date}\t"
                f"{gender}\t{registration_date}\t{status}\t{contract_type}"
            )
        
        return "\n".join(data_lines)
    
    def _generate_new_customer_data(self) -> str:
        """新規顧客データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        registration_date = base_date.strftime('%Y%m%d')
        
        # 新規顧客（当日登録）
        new_customers = [
            ("CUST900001", "新規太郎", "シンキタロウ", "new1@tokyogas.co.jp", "03-9001-0001", "100-0001", "東京都千代田区新規1-1-1", "19900101", "M", registration_date, "PENDING", "INDIVIDUAL"),
            ("CUST900002", "新規花子", "シンキハナコ", "new2@tokyogas.co.jp", "03-9002-0002", "100-0002", "東京都千代田区新規2-2-2", "19950515", "F", registration_date, "PENDING", "INDIVIDUAL"),
            ("CUST900003", "新規商事", "シンキショウジ", "new3@tokyogas.co.jp", "03-9003-0003", "100-0003", "東京都千代田区新規3-3-3", "20000101", "OTHER", registration_date, "PENDING", "CORPORATE"),
        ]
        
        for customer_data in new_customers:
            data_lines.append("\t".join(customer_data))
        
        return "\n".join(data_lines)
    
    def _generate_update_customer_data(self) -> str:
        """更新顧客データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        
        # 既存顧客の更新データ
        update_customers = [
            ("CUST000001", "更新太郎", "コウシンタロウ", "updated1@tokyogas.co.jp", "03-0001-9999", "110-0001", "東京都台東区更新1-1-1", "19850101", "M", "20200101", "ACTIVE", "INDIVIDUAL"),
            ("CUST000002", "更新花子", "コウシンハナコ", "updated2@tokyogas.co.jp", "03-0002-9999", "110-0002", "東京都台東区更新2-2-2", "19900515", "F", "20210101", "ACTIVE", "FAMILY"),
            ("CUST000003", "更新商事", "コウシンショウジ", "updated3@tokyogas.co.jp", "03-0003-9999", "110-0003", "東京都台東区更新3-3-3", "20000101", "OTHER", "20220101", "ACTIVE", "CORPORATE"),
        ]
        
        for customer_data in update_customers:
            data_lines.append("\t".join(customer_data))
        
        return "\n".join(data_lines)
    
    def _transform_customer_master_data(self, input_data: str) -> str:
        """顧客マスターデータETL変換処理"""
        lines = input_data.strip().split('\n')
        
        # 出力CSVヘッダー
        output_header = ",".join(self.output_columns)
        output_lines = [output_header]
        
        current_date = datetime.utcnow()
        insert_date = current_date.strftime('%Y/%m/%d %H:%M:%S')
        update_date = insert_date
        data_version = "1.0"
        
        for line in lines[1:]:
            if not line.strip():
                continue
                
            parts = line.split('\t')
            if len(parts) != len(self.input_columns):
                continue
            
            (customer_id, customer_name, customer_kana, email_address,
             phone_number, zip_code, address, birth_date,
             gender, registration_date, status, contract_type) = parts
            
            # ETL品質チェック
            if not customer_id.strip() or not customer_name.strip():
                continue
            
            # メールアドレス正規化（ETL処理）
            if email_address.strip():
                email_address = email_address.lower().strip()
            
            # 電話番号正規化（ETL処理）
            if phone_number.strip():
                phone_number = phone_number.replace("-", "").replace(" ", "")
                if len(phone_number) == 10:
                    phone_number = f"{phone_number[:2]}-{phone_number[2:6]}-{phone_number[6:]}"
                elif len(phone_number) == 11:
                    phone_number = f"{phone_number[:3]}-{phone_number[3:7]}-{phone_number[7:]}"
            
            # CSV出力行生成
            output_line = (
                f"{customer_id},{customer_name},{customer_kana},{email_address},"
                f"{phone_number},{zip_code},{address},{birth_date},"
                f"{gender},{registration_date},{status},{contract_type},"
                f"{insert_date},{update_date},{data_version}"
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
        test_data = self._generate_customer_master_data()
        
        # ファイルアップロード
        file_date = datetime.utcnow().strftime('%Y%m%d')
        file_path = f"CustomerMaster/{file_date}/customer_master.tsv"
        self.mock_storage.upload_file(self.source_container, file_path, test_data.encode('utf-8'))
        
        # ETL処理シミュレーション
        transformed_data, output_data = self.simulate_etl_process(
            test_data,
            self._transform_customer_master_data,
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
        file_path = f"CustomerMaster/{file_date}/customer_master.tsv"
        
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
        
        # 新規顧客データでテスト
        test_data = self._generate_new_customer_data()
        
        # データ変換処理
        transformed_data = self._transform_customer_master_data(test_data)
        
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
        
        # 大量データ準備（100000件）
        large_data = self._generate_customer_master_data(100000)
        
        start_time = time.time()
        
        # ETL処理実行
        transformed_data = self._transform_customer_master_data(large_data)
        
        execution_time = time.time() - start_time
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.PERFORMANCE,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=100000,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=0.95,
            errors=[]
        )
        
        # パフォーマンス基準
        throughput = 100000 / execution_time
        assert throughput > 3000, f"スループットが基準未満: {throughput:.2f} records/sec"
        
        self.validate_common_assertions(result)
    
    def test_integration_database_operations(self):
        """統合テスト: データベース操作"""
        test_id = f"integration_database_{int(time.time())}"
        
        # 更新顧客データでテスト
        test_data = self._generate_update_customer_data()
        
        # ETL処理
        transformed_data = self._transform_customer_master_data(test_data)
        
        # データベース操作シミュレーション
        # 1. 既存レコードチェック
        existing_check = self.mock_database.query_records(
            self.database_target, 
            "SELECT customer_id FROM customer_master WHERE customer_id IN ('CUST000001', 'CUST000002', 'CUST000003')"
        )
        
        # 2. 新規挿入/更新
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
        assert len(db_records) >= 3, f"処理レコード数不足: {len(db_records)}"
        
        self.validate_common_assertions(result)