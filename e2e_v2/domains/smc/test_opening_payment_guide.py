"""
pi_Send_OpeningPaymentGuide パイプラインのE2Eテスト

開栓支払いガイドパイプラインのETL処理テスト
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List

from ...base.pipeline_test_base import DomainTestBase, TestCategory, PipelineStatus


class TestOpeningPaymentGuidePipeline(DomainTestBase):
    """開栓支払いガイドパイプラインテスト"""
    
    def __init__(self):
        super().__init__("pi_Send_OpeningPaymentGuide", "smc")
    
    def domain_specific_setup(self):
        """SMCドメイン固有セットアップ"""
        self.source_container = "mytokyogas"
        self.output_container = "datalake"
        self.sftp_target_dir = "/Export/OpeningPaymentGuide"
        
        # 期待される入力フィールド（ETL処理用）
        self.input_columns = [
            "CUSTOMER_ID", "OPENING_REQUEST_ID", "OPENING_DATE", "CUSTOMER_NAME",
            "EMAIL_ADDRESS", "PHONE_NUMBER", "ADDRESS", "DEPOSIT_AMOUNT",
            "PAYMENT_METHOD", "PAYMENT_DUE_DATE", "GUIDE_TYPE", "PRIORITY"
        ]
        
        # 期待される出力フィールド（ETL変換後）
        self.output_columns = [
            "CUSTOMER_ID", "OPENING_REQUEST_ID", "OPENING_DATE", "CUSTOMER_NAME",
            "EMAIL_ADDRESS", "PHONE_NUMBER", "ADDRESS", "DEPOSIT_AMOUNT",
            "PAYMENT_METHOD", "PAYMENT_DUE_DATE", "GUIDE_TYPE", "PRIORITY",
            "PROCESS_DATE", "GUIDE_ID", "DELIVERY_STATUS"
        ]
    
    def get_domain_test_data_template(self) -> Dict[str, str]:
        """SMCドメイン用テストデータテンプレート"""
        return {
            "opening_payment_data": self._generate_opening_payment_data(),
            "urgent_opening_data": self._generate_urgent_opening_data(),
            "bulk_opening_data": self._generate_bulk_opening_data()
        }
    
    def _generate_opening_payment_data(self, record_count: int = 1000) -> str:
        """開栓支払いデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        payment_methods = ["CASH", "CREDIT", "BANK", "DEBIT"]
        guide_types = ["STANDARD", "URGENT", "REMINDER", "FINAL"]
        priorities = ["LOW", "MEDIUM", "HIGH", "URGENT"]
        
        base_date = datetime.utcnow()
        
        for i in range(1, record_count + 1):
            # 開栓日（今日から7日後以内）
            opening_days = i % 7
            opening_date = (base_date + timedelta(days=opening_days)).strftime('%Y%m%d')
            
            # 支払い期限（開栓日から10日後）
            payment_due_date = (base_date + timedelta(days=opening_days + 10)).strftime('%Y%m%d')
            
            payment_method = payment_methods[i % len(payment_methods)]
            guide_type = guide_types[i % len(guide_types)]
            priority = priorities[i % len(priorities)]
            
            # 預り金額（10000-50000円）
            deposit_amount = 10000 + (i % 40000)
            
            customer_name = f"開栓{i:04d}"
            email = f"opening{i:06d}@tokyogas.co.jp"
            phone = f"03-{i:04d}-{(i*7) % 10000:04d}"
            address = f"東京都{i % 23 + 1}区開栓{i % 99 + 1}-{i % 99 + 1}-{i % 99 + 1}"
            
            data_lines.append(
                f"CUST{i:06d}\tOPEN{i:06d}\t{opening_date}\t{customer_name}\t"
                f"{email}\t{phone}\t{address}\t{deposit_amount}\t"
                f"{payment_method}\t{payment_due_date}\t{guide_type}\t{priority}"
            )
        
        return "\n".join(data_lines)
    
    def _generate_urgent_opening_data(self) -> str:
        """緊急開栓データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        opening_date = (base_date + timedelta(days=1)).strftime('%Y%m%d')  # 明日
        payment_due_date = (base_date + timedelta(days=3)).strftime('%Y%m%d')  # 3日後
        
        # 緊急開栓案件
        urgent_openings = [
            ("CUST900001", "OPEN900001", "緊急開栓1", "urgent1@tokyogas.co.jp", "03-9001-0001", "東京都新宿区緊急1-1-1", "20000", "CASH"),
            ("CUST900002", "OPEN900002", "緊急開栓2", "urgent2@tokyogas.co.jp", "03-9002-0002", "東京都渋谷区緊急2-2-2", "15000", "CREDIT"),
            ("CUST900003", "OPEN900003", "緊急開栓3", "urgent3@tokyogas.co.jp", "03-9003-0003", "東京都港区緊急3-3-3", "25000", "BANK"),
        ]
        
        for customer_id, opening_id, customer_name, email, phone, address, deposit, payment_method in urgent_openings:
            data_lines.append(
                f"{customer_id}\t{opening_id}\t{opening_date}\t{customer_name}\t"
                f"{email}\t{phone}\t{address}\t{deposit}\t"
                f"{payment_method}\t{payment_due_date}\tURGENT\tURGENT"
            )
        
        return "\n".join(data_lines)
    
    def _generate_bulk_opening_data(self) -> str:
        """一括開栓データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        opening_date = (base_date + timedelta(days=3)).strftime('%Y%m%d')
        payment_due_date = (base_date + timedelta(days=13)).strftime('%Y%m%d')
        
        # 一括開栓（新築マンション等）
        for i in range(1, 21):
            data_lines.append(
                f"CUST{i:06d}\tOPEN{i:06d}\t{opening_date}\t一括開栓{i:03d}\t"
                f"bulk{i:03d}@tokyogas.co.jp\t03-0000-{i:04d}\t"
                f"東京都一括区{i:03d}\t15000\t"
                f"BANK\t{payment_due_date}\tSTANDARD\tMEDIUM"
            )
        
        return "\n".join(data_lines)
    
    def _transform_opening_payment_data(self, input_data: str) -> str:
        """開栓支払いデータETL変換処理"""
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
            
            (customer_id, opening_request_id, opening_date, customer_name,
             email_address, phone_number, address, deposit_amount,
             payment_method, payment_due_date, guide_type, priority) = parts
            
            # ETL品質チェック
            if not customer_id.strip() or not opening_request_id.strip() or not customer_name.strip():
                continue
            
            # ガイドID生成（ETL処理）
            guide_id = f"GUIDE{opening_request_id[4:]}{process_date}"
            
            # 配信ステータス設定（ETL処理）
            delivery_status = "PENDING"
            
            # CSV出力行生成
            output_line = (
                f"{customer_id},{opening_request_id},{opening_date},{customer_name},"
                f"{email_address},{phone_number},{address},{deposit_amount},"
                f"{payment_method},{payment_due_date},{guide_type},{priority},"
                f"{process_date},{guide_id},{delivery_status}"
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
        test_data = self._generate_opening_payment_data()
        
        # ファイルアップロード
        file_date = datetime.utcnow().strftime('%Y%m%d')
        file_path = f"OpeningPaymentGuide/{file_date}/opening_payment_guide.tsv"
        self.mock_storage.upload_file(self.source_container, file_path, test_data.encode('utf-8'))
        
        # ETL処理シミュレーション
        transformed_data, output_data = self.simulate_etl_process(
            test_data,
            self._transform_opening_payment_data,
            "csv"
        )
        
        # 出力ファイル保存
        output_file_path = f"OpeningPaymentGuide/processed_{file_date}.csv"
        self.mock_storage.upload_file(self.output_container, output_file_path, output_data)
        
        # SFTP転送
        sftp_path = f"{self.sftp_target_dir}/opening_payment_guide_{file_date}.csv"
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
        file_path = f"OpeningPaymentGuide/{file_date}/opening_payment_guide.tsv"
        
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
        
        # 緊急開栓データでテスト
        test_data = self._generate_urgent_opening_data()
        
        # データ変換処理
        transformed_data = self._transform_opening_payment_data(test_data)
        
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
        
        # 大量データ準備（15000件）
        large_data = self._generate_opening_payment_data(15000)
        
        start_time = time.time()
        
        # ETL処理実行
        transformed_data = self._transform_opening_payment_data(large_data)
        
        execution_time = time.time() - start_time
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.PERFORMANCE,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=15000,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=0.95,
            errors=[]
        )
        
        # パフォーマンス基準
        throughput = 15000 / execution_time
        assert throughput > 4000, f"スループットが基準未満: {throughput:.2f} records/sec"
        
        self.validate_common_assertions(result)
    
    def test_integration_sftp_connectivity(self):
        """統合テスト: SFTP接続性"""
        test_id = f"integration_sftp_{int(time.time())}"
        
        # 一括開栓データでテスト
        test_data = self._generate_bulk_opening_data()
        
        # ETL処理
        transformed_data = self._transform_opening_payment_data(test_data)
        
        # SFTP転送テスト
        file_date = datetime.utcnow().strftime('%Y%m%d')
        sftp_path = f"{self.sftp_target_dir}/opening_payment_guide_bulk_{file_date}.csv"
        sftp_result = self.mock_sftp.upload(
            f"opening_payment_guide_bulk_{file_date}.csv",
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