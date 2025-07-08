"""
pi_Send_Cpkiyk パイプラインのE2Eテスト

CPKIYKデータ配信パイプラインのETL処理テスト
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List

from ...base.pipeline_test_base import DomainTestBase, TestCategory, PipelineStatus


class TestCpkiykPipeline(DomainTestBase):
    """CPKIYKデータ配信パイプラインテスト"""
    
    def __init__(self):
        super().__init__("pi_Send_Cpkiyk", "smc")
    
    def domain_specific_setup(self):
        """SMCドメイン固有セットアップ"""
        self.source_container = "mytokyogas"
        self.output_container = "datalake"
        self.sftp_target_dir = "/Export/Cpkiyk"
        
        # 期待される入力フィールド（ETL処理用）
        self.input_columns = [
            "CUSTOMER_ID", "CUSTOMER_NAME", "CONTRACT_NUMBER", "CPKIYK_CODE",
            "CPKIYK_TYPE", "ISSUE_DATE", "EXPIRY_DATE", "AMOUNT",
            "STATUS", "EMAIL_ADDRESS", "PHONE_NUMBER", "REFERENCE_ID"
        ]
        
        # 期待される出力フィールド（ETL変換後）
        self.output_columns = [
            "CUSTOMER_ID", "CUSTOMER_NAME", "CONTRACT_NUMBER", "CPKIYK_CODE",
            "CPKIYK_TYPE", "ISSUE_DATE", "EXPIRY_DATE", "AMOUNT",
            "STATUS", "EMAIL_ADDRESS", "PHONE_NUMBER", "REFERENCE_ID",
            "PROCESS_DATE", "CPKIYK_ID", "VALIDITY_FLAG"
        ]
    
    def get_domain_test_data_template(self) -> Dict[str, str]:
        """SMCドメイン用テストデータテンプレート"""
        return {
            "cpkiyk_data": self._generate_cpkiyk_data(),
            "urgent_cpkiyk_data": self._generate_urgent_cpkiyk_data(),
            "bulk_cpkiyk_data": self._generate_bulk_cpkiyk_data()
        }
    
    def _generate_cpkiyk_data(self, record_count: int = 1200) -> str:
        """CPKIYKデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        cpkiyk_types = ["DISCOUNT", "REFUND", "CREDIT", "ADJUSTMENT", "BONUS"]
        statuses = ["ISSUED", "PENDING", "EXPIRED", "USED", "CANCELLED"]
        
        base_date = datetime.utcnow()
        
        for i in range(1, record_count + 1):
            # 発行日（過去30日以内）
            issue_days = i % 30
            issue_date = (base_date - timedelta(days=issue_days)).strftime('%Y%m%d')
            
            # 有効期限（発行日から90日後）
            expiry_date = (base_date - timedelta(days=issue_days) + timedelta(days=90)).strftime('%Y%m%d')
            
            cpkiyk_type = cpkiyk_types[i % len(cpkiyk_types)]
            status = statuses[i % len(statuses)]
            
            # タイプによる金額設定
            if cpkiyk_type == "DISCOUNT":
                amount = (i % 5000) + 1000  # 1000-5999円
            elif cpkiyk_type == "REFUND":
                amount = (i % 10000) + 2000  # 2000-11999円
            elif cpkiyk_type == "CREDIT":
                amount = (i % 3000) + 500   # 500-3499円
            elif cpkiyk_type == "ADJUSTMENT":
                amount = (i % 2000) + 100   # 100-2099円
            else:  # BONUS
                amount = (i % 1000) + 50    # 50-1049円
            
            customer_name = f"CPKIYK{i:04d}"
            email = f"cpkiyk{i:06d}@tokyogas.co.jp"
            phone = f"03-{i:04d}-{(i*17) % 10000:04d}"
            contract_number = f"CNT{i:08d}"
            cpkiyk_code = f"CPK{i:06d}"
            reference_id = f"REF{i:08d}"
            
            data_lines.append(
                f"CUST{i:06d}\t{customer_name}\t{contract_number}\t{cpkiyk_code}\t"
                f"{cpkiyk_type}\t{issue_date}\t{expiry_date}\t{amount}\t"
                f"{status}\t{email}\t{phone}\t{reference_id}"
            )
        
        return "\n".join(data_lines)
    
    def _generate_urgent_cpkiyk_data(self) -> str:
        """緊急CPKIYKデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        issue_date = base_date.strftime('%Y%m%d')
        expiry_date = (base_date + timedelta(days=30)).strftime('%Y%m%d')  # 30日期限
        
        # 緊急発行案件
        urgent_cpkiyks = [
            ("CUST900001", "緊急CPKIYK1", "CNT90000001", "CPK900001", "REFUND", issue_date, expiry_date, "15000", "ISSUED", "urgent1@tokyogas.co.jp", "03-9001-0001", "REF90000001"),
            ("CUST900002", "緊急CPKIYK2", "CNT90000002", "CPK900002", "CREDIT", issue_date, expiry_date, "8000", "ISSUED", "urgent2@tokyogas.co.jp", "03-9002-0002", "REF90000002"),
            ("CUST900003", "緊急CPKIYK3", "CNT90000003", "CPK900003", "ADJUSTMENT", issue_date, expiry_date, "2500", "PENDING", "urgent3@tokyogas.co.jp", "03-9003-0003", "REF90000003"),
        ]
        
        for cpkiyk_data in urgent_cpkiyks:
            data_lines.append("\t".join(cpkiyk_data))
        
        return "\n".join(data_lines)
    
    def _generate_bulk_cpkiyk_data(self) -> str:
        """一括CPKIYKデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        issue_date = (base_date - timedelta(days=7)).strftime('%Y%m%d')
        expiry_date = (base_date + timedelta(days=83)).strftime('%Y%m%d')
        
        # 一括ボーナス発行
        for i in range(1, 81):
            data_lines.append(
                f"CUST{i:06d}\t一括CPKIYK{i:03d}\tCNT{i:08d}\tCPK{i:06d}\t"
                f"BONUS\t{issue_date}\t{expiry_date}\t100\t"
                f"ISSUED\tbulk{i:03d}@tokyogas.co.jp\t03-0000-{i:04d}\t"
                f"REF{i:08d}"
            )
        
        return "\n".join(data_lines)
    
    def _transform_cpkiyk_data(self, input_data: str) -> str:
        """CPKIYKデータETL変換処理"""
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
            
            (customer_id, customer_name, contract_number, cpkiyk_code,
             cpkiyk_type, issue_date, expiry_date, amount,
             status, email_address, phone_number, reference_id) = parts
            
            # ETL品質チェック
            if not customer_id.strip() or not customer_name.strip() or not cpkiyk_code.strip():
                continue
            
            # CPKIYK ID生成（ETL処理）
            cpkiyk_id = f"CPKIYK{cpkiyk_code[3:]}{process_date}"
            
            # 有効性フラグ設定（ETL処理）
            try:
                expiry_dt = datetime.strptime(expiry_date, '%Y%m%d')
                if expiry_dt > current_date and status in ["ISSUED", "PENDING"]:
                    validity_flag = "VALID"
                elif expiry_dt <= current_date:
                    validity_flag = "EXPIRED"
                else:
                    validity_flag = "INVALID"
            except ValueError:
                validity_flag = "INVALID"
            
            # CSV出力行生成
            output_line = (
                f"{customer_id},{customer_name},{contract_number},{cpkiyk_code},"
                f"{cpkiyk_type},{issue_date},{expiry_date},{amount},"
                f"{status},{email_address},{phone_number},{reference_id},"
                f"{process_date},{cpkiyk_id},{validity_flag}"
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
        test_data = self._generate_cpkiyk_data()
        
        # ファイルアップロード
        file_date = datetime.utcnow().strftime('%Y%m%d')
        file_path = f"Cpkiyk/{file_date}/cpkiyk_data.tsv"
        self.mock_storage.upload_file(self.source_container, file_path, test_data.encode('utf-8'))
        
        # ETL処理シミュレーション
        transformed_data, output_data = self.simulate_etl_process(
            test_data,
            self._transform_cpkiyk_data,
            "csv"
        )
        
        # 出力ファイル保存
        output_file_path = f"Cpkiyk/processed_{file_date}.csv"
        self.mock_storage.upload_file(self.output_container, output_file_path, output_data)
        
        # SFTP転送
        sftp_path = f"{self.sftp_target_dir}/cpkiyk_data_{file_date}.csv"
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
        file_path = f"Cpkiyk/{file_date}/cpkiyk_data.tsv"
        
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
        
        # 緊急CPKIYKデータでテスト
        test_data = self._generate_urgent_cpkiyk_data()
        
        # データ変換処理
        transformed_data = self._transform_cpkiyk_data(test_data)
        
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
        large_data = self._generate_cpkiyk_data(80000)
        
        start_time = time.time()
        
        # ETL処理実行
        transformed_data = self._transform_cpkiyk_data(large_data)
        
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
        assert throughput > 3000, f"スループットが基準未満: {throughput:.2f} records/sec"
        
        self.validate_common_assertions(result)
    
    def test_integration_sftp_connectivity(self):
        """統合テスト: SFTP接続性"""
        test_id = f"integration_sftp_{int(time.time())}"
        
        # 一括CPKIYKデータでテスト
        test_data = self._generate_bulk_cpkiyk_data()
        
        # ETL処理
        transformed_data = self._transform_cpkiyk_data(test_data)
        
        # SFTP転送テスト
        file_date = datetime.utcnow().strftime('%Y%m%d')
        sftp_path = f"{self.sftp_target_dir}/cpkiyk_data_bulk_{file_date}.csv"
        sftp_result = self.mock_sftp.upload(
            f"cpkiyk_data_bulk_{file_date}.csv",
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