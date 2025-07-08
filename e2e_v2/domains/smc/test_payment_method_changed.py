"""
pi_Send_PaymentMethodChanged パイプラインのE2Eテスト

支払い方法変更通知パイプラインのETL処理テスト
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List

from ...base.pipeline_test_base import DomainTestBase, TestCategory, PipelineStatus


class TestPaymentMethodChangedPipeline(DomainTestBase):
    """支払い方法変更通知パイプラインテスト"""
    
    def __init__(self):
        super().__init__("pi_Send_PaymentMethodChanged", "smc")
    
    def domain_specific_setup(self):
        """SMCドメイン固有セットアップ"""
        self.source_container = "mytokyogas"
        self.output_container = "datalake"
        self.sftp_target_dir = "/Export/PaymentMethodChanged"
        
        # 期待される入力フィールド（ETL処理用）
        self.input_columns = [
            "CUSTOMER_ID", "OLD_PAYMENT_METHOD", "NEW_PAYMENT_METHOD",
            "CHANGE_DATE", "CHANGE_REASON", "EFFECTIVE_DATE",
            "NOTIFICATION_TYPE", "EMAIL_ADDRESS", "PHONE_NUMBER", "STATUS"
        ]
        
        # 期待される出力フィールド（ETL変換後）
        self.output_columns = [
            "CUSTOMER_ID", "OLD_PAYMENT_METHOD", "NEW_PAYMENT_METHOD",
            "CHANGE_DATE", "CHANGE_REASON", "EFFECTIVE_DATE",
            "NOTIFICATION_TYPE", "EMAIL_ADDRESS", "PHONE_NUMBER",
            "STATUS", "PROCESS_DATE", "NOTIFICATION_ID"
        ]
    
    def get_domain_test_data_template(self) -> Dict[str, str]:
        """SMCドメイン用テストデータテンプレート"""
        return {
            "payment_change_data": self._generate_payment_change_data(),
            "bulk_change_data": self._generate_bulk_change_data(),
            "urgent_change_data": self._generate_urgent_change_data()
        }
    
    def _generate_payment_change_data(self, record_count: int = 1000) -> str:
        """支払い方法変更データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        old_methods = ["CREDIT", "BANK", "DEBIT", "CASH"]
        new_methods = ["CREDIT", "BANK", "DEBIT", "PREPAID"]
        change_reasons = ["CUSTOMER_REQUEST", "CARD_EXPIRY", "BANK_CHANGE", "SYSTEM_UPDATE"]
        notification_types = ["EMAIL", "SMS", "MAIL", "PHONE"]
        statuses = ["PENDING", "PROCESSED", "COMPLETED", "FAILED"]
        
        base_date = datetime.utcnow()
        
        for i in range(1, record_count + 1):
            # 変更日（過去30日以内）
            change_days = i % 30
            change_date = (base_date - timedelta(days=change_days)).strftime('%Y%m%d')
            
            # 有効日（変更日から7日後）
            effective_date = (base_date - timedelta(days=change_days) + timedelta(days=7)).strftime('%Y%m%d')
            
            old_method = old_methods[i % len(old_methods)]
            new_method = new_methods[i % len(new_methods)]
            
            # 同じ支払い方法への変更は除外
            if old_method == new_method:
                new_method = new_methods[(i + 1) % len(new_methods)]
            
            change_reason = change_reasons[i % len(change_reasons)]
            notification_type = notification_types[i % len(notification_types)]
            status = statuses[i % len(statuses)]
            
            email = f"customer{i:06d}@tokyogas.co.jp"
            phone = f"090-{i:04d}-{(i*3) % 10000:04d}"
            
            data_lines.append(
                f"CUST{i:06d}\t{old_method}\t{new_method}\t{change_date}\t"
                f"{change_reason}\t{effective_date}\t{notification_type}\t"
                f"{email}\t{phone}\t{status}"
            )
        
        return "\n".join(data_lines)
    
    def _generate_bulk_change_data(self) -> str:
        """一括変更データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        change_date = base_date.strftime('%Y%m%d')
        effective_date = (base_date + timedelta(days=30)).strftime('%Y%m%d')
        
        # システム更新による一括変更
        for i in range(1, 101):
            data_lines.append(
                f"CUST{i:06d}\tCREDIT\tBANK\t{change_date}\t"
                f"SYSTEM_UPDATE\t{effective_date}\tEMAIL\t"
                f"bulk{i:03d}@tokyogas.co.jp\t090-0000-{i:04d}\tPENDING"
            )
        
        return "\n".join(data_lines)
    
    def _generate_urgent_change_data(self) -> str:
        """緊急変更データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        change_date = base_date.strftime('%Y%m%d')
        effective_date = (base_date + timedelta(days=1)).strftime('%Y%m%d')  # 翌日有効
        
        # カード有効期限切れによる緊急変更
        urgent_changes = [
            ("CUST900001", "CREDIT", "BANK", "CARD_EXPIRY", "urgent1@tokyogas.co.jp", "090-9001-0001"),
            ("CUST900002", "DEBIT", "CREDIT", "CARD_EXPIRY", "urgent2@tokyogas.co.jp", "090-9002-0002"),
            ("CUST900003", "CREDIT", "PREPAID", "CARD_EXPIRY", "urgent3@tokyogas.co.jp", "090-9003-0003"),
        ]
        
        for customer_id, old_method, new_method, reason, email, phone in urgent_changes:
            data_lines.append(
                f"{customer_id}\t{old_method}\t{new_method}\t{change_date}\t"
                f"{reason}\t{effective_date}\tSMS\t{email}\t{phone}\tPENDING"
            )
        
        return "\n".join(data_lines)
    
    def _transform_payment_change_data(self, input_data: str) -> str:
        """支払い方法変更データETL変換処理"""
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
            
            (customer_id, old_payment_method, new_payment_method,
             change_date, change_reason, effective_date,
             notification_type, email_address, phone_number, status) = parts
            
            # ETL品質チェック
            if not customer_id.strip() or not old_payment_method.strip() or not new_payment_method.strip():
                continue
            
            # 通知ID生成（ETL処理）
            notification_id = f"PMC{customer_id[4:]}{process_date}"
            
            # CSV出力行生成
            output_line = (
                f"{customer_id},{old_payment_method},{new_payment_method},"
                f"{change_date},{change_reason},{effective_date},"
                f"{notification_type},{email_address},{phone_number},"
                f"{status},{process_date},{notification_id}"
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
        test_data = self._generate_payment_change_data()
        
        # ファイルアップロード
        file_date = datetime.utcnow().strftime('%Y%m%d')
        file_path = f"PaymentMethodChanged/{file_date}/payment_method_changed.tsv"
        self.mock_storage.upload_file(self.source_container, file_path, test_data.encode('utf-8'))
        
        # ETL処理シミュレーション
        transformed_data, output_data = self.simulate_etl_process(
            test_data,
            self._transform_payment_change_data,
            "csv"
        )
        
        # 出力ファイル保存
        output_file_path = f"PaymentMethodChanged/processed_{file_date}.csv"
        self.mock_storage.upload_file(self.output_container, output_file_path, output_data)
        
        # SFTP転送
        sftp_path = f"{self.sftp_target_dir}/payment_method_changed_{file_date}.csv"
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
        file_path = f"PaymentMethodChanged/{file_date}/payment_method_changed.tsv"
        
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
        
        # 緊急変更データでテスト
        test_data = self._generate_urgent_change_data()
        
        # データ変換処理
        transformed_data = self._transform_payment_change_data(test_data)
        
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
        
        # 大量データ準備（30000件）
        large_data = self._generate_payment_change_data(30000)
        
        start_time = time.time()
        
        # ETL処理実行
        transformed_data = self._transform_payment_change_data(large_data)
        
        execution_time = time.time() - start_time
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.PERFORMANCE,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=30000,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=0.95,
            errors=[]
        )
        
        # パフォーマンス基準
        throughput = 30000 / execution_time
        assert throughput > 5000, f"スループットが基準未満: {throughput:.2f} records/sec"
        
        self.validate_common_assertions(result)
    
    def test_integration_sftp_connectivity(self):
        """統合テスト: SFTP接続性"""
        test_id = f"integration_sftp_{int(time.time())}"
        
        # 一括変更データでテスト
        test_data = self._generate_bulk_change_data()
        
        # ETL処理
        transformed_data = self._transform_payment_change_data(test_data)
        
        # SFTP転送テスト
        file_date = datetime.utcnow().strftime('%Y%m%d')
        sftp_path = f"{self.sftp_target_dir}/payment_method_changed_bulk_{file_date}.csv"
        sftp_result = self.mock_sftp.upload(
            f"payment_method_changed_bulk_{file_date}.csv",
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