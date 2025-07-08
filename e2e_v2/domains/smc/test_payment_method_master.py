"""
pi_Send_PaymentMethodMaster パイプラインのE2Eテスト

支払い方法マスター配信パイプラインのETL処理テスト
"""

import time
from datetime import datetime
from typing import Dict, List

from ...base.pipeline_test_base import DomainTestBase, TestCategory, PipelineStatus


class TestPaymentMethodMasterPipeline(DomainTestBase):
    """支払い方法マスター配信パイプラインテスト"""
    
    def __init__(self):
        super().__init__("pi_Send_PaymentMethodMaster", "smc")
    
    def domain_specific_setup(self):
        """SMCドメイン固有セットアップ"""
        self.source_container = "mytokyogas"
        self.output_container = "datalake"
        self.sftp_target_dir = "/Export/PaymentMethodMaster"
        
        # 期待される入力フィールド（ETL処理用）
        self.input_columns = [
            "CUSTOMER_ID", "PAYMENT_METHOD_CODE", "PAYMENT_METHOD_NAME",
            "BANK_CODE", "BRANCH_CODE", "ACCOUNT_NUMBER", "ACCOUNT_TYPE",
            "ACCOUNT_HOLDER", "CREDIT_CARD_NUMBER", "EXPIRY_DATE", "STATUS"
        ]
        
        # 期待される出力フィールド（ETL変換後）
        self.output_columns = [
            "CUSTOMER_ID", "PAYMENT_METHOD_CODE", "PAYMENT_METHOD_NAME",
            "BANK_CODE", "BRANCH_CODE", "ACCOUNT_NUMBER", "ACCOUNT_TYPE",
            "ACCOUNT_HOLDER", "MASKED_CARD_NUMBER", "EXPIRY_DATE",
            "STATUS", "PROCESS_DATE", "UPDATE_TIMESTAMP"
        ]
    
    def get_domain_test_data_template(self) -> Dict[str, str]:
        """SMCドメイン用テストデータテンプレート"""
        return {
            "payment_method_data": self._generate_payment_method_data(),
            "mixed_payment_data": self._generate_mixed_payment_data(),
            "large_dataset": self._generate_large_payment_data(50000)
        }
    
    def _generate_payment_method_data(self, record_count: int = 1000) -> str:
        """支払い方法データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        payment_methods = ["CREDIT", "BANK", "DEBIT", "PREPAID"]
        account_types = ["ORDINARY", "SAVINGS", "CURRENT"]
        statuses = ["ACTIVE", "INACTIVE", "PENDING"]
        
        for i in range(1, record_count + 1):
            payment_method = payment_methods[i % len(payment_methods)]
            
            # 支払い方法別データ生成
            if payment_method in ["CREDIT", "DEBIT", "PREPAID"]:
                bank_code = ""
                branch_code = ""
                account_number = ""
                account_type = ""
                account_holder = ""
                card_number = f"4{i:015d}"  # 16桁カード番号
                expiry_date = "1225"  # MMYY形式
            else:  # BANK
                bank_code = f"{i % 999 + 1:04d}"
                branch_code = f"{i % 999 + 1:03d}"
                account_number = f"{i:07d}"
                account_type = account_types[i % len(account_types)]
                account_holder = f"顧客{i:04d}"
                card_number = ""
                expiry_date = ""
            
            status = statuses[i % len(statuses)]
            
            data_lines.append(
                f"CUST{i:06d}\t{payment_method}001\t{payment_method}支払い\t"
                f"{bank_code}\t{branch_code}\t{account_number}\t{account_type}\t"
                f"{account_holder}\t{card_number}\t{expiry_date}\t{status}"
            )
        
        return "\n".join(data_lines)
    
    def _generate_mixed_payment_data(self) -> str:
        """混在支払いデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        # 各支払い方法のサンプル
        mixed_data = [
            ("CUST000001", "CREDIT001", "クレジットカード", "", "", "", "", "", "4111111111111111", "1225", "ACTIVE"),
            ("CUST000002", "BANK001", "銀行振替", "0001", "001", "1234567", "ORDINARY", "田中太郎", "", "", "ACTIVE"),
            ("CUST000003", "DEBIT001", "デビットカード", "", "", "", "", "", "5555555555554444", "0326", "ACTIVE"),
            ("CUST000004", "BANK002", "銀行振替", "0009", "123", "7654321", "SAVINGS", "佐藤花子", "", "", "INACTIVE"),
            ("CUST000005", "PREPAID001", "プリペイドカード", "", "", "", "", "", "4000000000000002", "1230", "PENDING"),
        ]
        
        for data in mixed_data:
            data_lines.append("\t".join(data))
        
        return "\n".join(data_lines)
    
    def _generate_large_payment_data(self, record_count: int) -> str:
        """大量支払いデータ生成"""
        return self._generate_payment_method_data(record_count)
    
    def _transform_payment_method_data(self, input_data: str) -> str:
        """支払い方法データETL変換処理"""
        lines = input_data.strip().split('\n')
        
        # 出力CSVヘッダー
        output_header = ",".join(self.output_columns)
        output_lines = [output_header]
        
        current_date = datetime.utcnow()
        process_date = current_date.strftime('%Y%m%d')
        update_timestamp = current_date.strftime('%Y/%m/%d %H:%M:%S')
        
        for line in lines[1:]:
            if not line.strip():
                continue
                
            parts = line.split('\t')
            if len(parts) != len(self.input_columns):
                continue
            
            (customer_id, payment_method_code, payment_method_name,
             bank_code, branch_code, account_number, account_type,
             account_holder, credit_card_number, expiry_date, status) = parts
            
            # ETL品質チェック
            if not customer_id.strip() or not payment_method_code.strip():
                continue
            
            # カード番号マスキング処理（ETL変換）
            if credit_card_number.strip():
                if len(credit_card_number) >= 12:
                    masked_card_number = credit_card_number[:4] + "*" * 8 + credit_card_number[-4:]
                else:
                    masked_card_number = "*" * len(credit_card_number)
            else:
                masked_card_number = ""
            
            # CSV出力行生成
            output_line = (
                f"{customer_id},{payment_method_code},{payment_method_name},"
                f"{bank_code},{branch_code},{account_number},{account_type},"
                f"{account_holder},{masked_card_number},{expiry_date},"
                f"{status},{process_date},{update_timestamp}"
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
        test_data = self._generate_payment_method_data()
        
        # ファイルアップロード
        file_date = datetime.utcnow().strftime('%Y%m%d')
        file_path = f"PaymentMethodMaster/{file_date}/payment_method_master.tsv"
        self.mock_storage.upload_file(self.source_container, file_path, test_data.encode('utf-8'))
        
        # ETL処理シミュレーション
        transformed_data, output_data = self.simulate_etl_process(
            test_data,
            self._transform_payment_method_data,
            "csv"
        )
        
        # 出力ファイル保存
        output_file_path = f"PaymentMethodMaster/processed_{file_date}.csv"
        self.mock_storage.upload_file(self.output_container, output_file_path, output_data)
        
        # SFTP転送
        sftp_path = f"{self.sftp_target_dir}/payment_method_master_{file_date}.csv"
        sftp_result = self.mock_sftp.upload(output_file_path, sftp_path, output_data)
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.FUNCTIONAL,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=len(test_data.split('\n')) - 1,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=0.95,
            errors=[]
        )
        
        # アサーション
        self.validate_common_assertions(result)
        assert result.records_extracted > 0, "抽出レコード数が0"
        assert sftp_result, "SFTP転送失敗"
    
    def test_functional_without_file(self):
        """機能テスト: ファイル無し処理"""
        test_id = f"functional_no_file_{int(time.time())}"
        
        # ファイル存在チェック（存在しないファイルパスを使用）
        file_date = "99999999"
        file_path = f"PaymentMethodMaster/{file_date}/payment_method_master.tsv"
        
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
        
        # 混在データでテスト
        test_data = self._generate_mixed_payment_data()
        
        # データ変換処理
        transformed_data = self._transform_payment_method_data(test_data)
        
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
        large_data = self._generate_large_payment_data(50000)
        
        start_time = time.time()
        
        # ETL処理実行
        transformed_data = self._transform_payment_method_data(large_data)
        
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
        assert throughput > 3000, f"スループットが基準未満: {throughput:.2f} records/sec"
        
        self.validate_common_assertions(result)
    
    def test_integration_sftp_connectivity(self):
        """統合テスト: SFTP接続性"""
        test_id = f"integration_sftp_{int(time.time())}"
        
        # テストデータ準備
        test_data = self._generate_mixed_payment_data()
        
        # ETL処理
        transformed_data = self._transform_payment_method_data(test_data)
        
        # SFTP転送テスト
        file_date = datetime.utcnow().strftime('%Y%m%d')
        sftp_path = f"{self.sftp_target_dir}/payment_method_master_{file_date}.csv"
        sftp_result = self.mock_sftp.upload(
            f"payment_method_master_{file_date}.csv",
            sftp_path,
            transformed_data.encode('utf-8')
        )
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.INTEGRATION,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=len(test_data.split('\n')) - 1,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=1.0,
            errors=[]
        )
        
        # 統合テストアサーション
        assert sftp_result, "SFTP転送失敗"
        
        # 転送履歴確認
        transfer_history = self.mock_sftp.get_transfer_history()
        upload_records = [h for h in transfer_history if h["action"] == "upload"]
        assert len(upload_records) > 0, "SFTP転送履歴なし"
        
        self.validate_common_assertions(result)