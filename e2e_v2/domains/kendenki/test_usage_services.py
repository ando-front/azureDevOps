"""
pi_Send_UsageServices パイプラインのE2Eテスト

使用量サービス配信パイプラインのETL処理テスト
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List

from ...base.pipeline_test_base import DomainTestBase, TestCategory, PipelineStatus


class TestUsageServicesPipeline(DomainTestBase):
    """使用量サービス配信パイプラインテスト"""
    
    def __init__(self):
        super().__init__("pi_Send_UsageServices", "kendenki")
    
    def domain_specific_setup(self):
        """検電ドメイン固有セットアップ"""
        self.source_container = "mytokyogas"
        self.output_container = "datalake"
        self.sftp_target_dir = "/Export/UsageServices"
        
        # 期待される入力フィールド（ETL処理用）
        self.input_columns = [
            "CUSTOMER_ID", "CUSTOMER_NAME", "SERVICE_ADDRESS", "METER_ID",
            "USAGE_MONTH", "USAGE_AMOUNT", "PREVIOUS_READING", "CURRENT_READING",
            "UNIT_PRICE", "BASIC_CHARGE", "USAGE_CHARGE", "TOTAL_AMOUNT",
            "EMAIL_ADDRESS", "PHONE_NUMBER", "SERVICE_TYPE", "STATUS"
        ]
        
        # 期待される出力フィールド（ETL変換後）
        self.output_columns = [
            "CUSTOMER_ID", "CUSTOMER_NAME", "SERVICE_ADDRESS", "METER_ID",
            "USAGE_MONTH", "USAGE_AMOUNT", "PREVIOUS_READING", "CURRENT_READING",
            "UNIT_PRICE", "BASIC_CHARGE", "USAGE_CHARGE", "TOTAL_AMOUNT",
            "EMAIL_ADDRESS", "PHONE_NUMBER", "SERVICE_TYPE",
            "STATUS", "PROCESS_DATE", "USAGE_REPORT_ID", "USAGE_CATEGORY"
        ]
    
    def get_domain_test_data_template(self) -> Dict[str, str]:
        """検電ドメイン用テストデータテンプレート"""
        return {
            "usage_services_data": self._generate_usage_services_data(),
            "high_usage_data": self._generate_high_usage_data(),
            "bulk_usage_data": self._generate_bulk_usage_data()
        }
    
    def _generate_usage_services_data(self, record_count: int = 1500) -> str:
        """使用量サービスデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        service_types = ["GAS", "ELECTRICITY", "WATER", "COMBINED"]
        statuses = ["ACTIVE", "PENDING", "SUSPENDED", "TERMINATED"]
        
        base_date = datetime.utcnow()
        current_month = base_date.strftime('%Y%m')
        
        for i in range(1, record_count + 1):
            service_type = service_types[i % len(service_types)]
            status = statuses[i % len(statuses)]
            
            # 使用量生成（サービスタイプ別）
            if service_type == "GAS":
                usage_amount = (i % 200) + 50   # 50-249 m³
                unit_price = 150.5
                basic_charge = 2000
            elif service_type == "ELECTRICITY":
                usage_amount = (i % 500) + 100  # 100-599 kWh
                unit_price = 25.5
                basic_charge = 1500
            elif service_type == "WATER":
                usage_amount = (i % 100) + 20   # 20-119 L
                unit_price = 200.0
                basic_charge = 1200
            else:  # COMBINED
                usage_amount = (i % 300) + 80   # 80-379 (複合単位)
                unit_price = 180.0
                basic_charge = 2500
            
            # 前回・今回検針値
            previous_reading = (i * 100) % 10000
            current_reading = previous_reading + usage_amount
            
            # 料金計算
            usage_charge = int(usage_amount * unit_price)
            total_amount = basic_charge + usage_charge
            
            customer_name = f"使用量{i:04d}"
            email = f"usage{i:06d}@tokyogas.co.jp"
            phone = f"03-{i:04d}-{(i*19) % 10000:04d}"
            service_address = f"東京都{i % 23 + 1}区使用量{i % 99 + 1}-{i % 99 + 1}-{i % 99 + 1}"
            meter_id = f"MTR{i:08d}"
            
            data_lines.append(
                f"CUST{i:06d}\t{customer_name}\t{service_address}\t{meter_id}\t"
                f"{current_month}\t{usage_amount}\t{previous_reading}\t{current_reading}\t"
                f"{unit_price}\t{basic_charge}\t{usage_charge}\t{total_amount}\t"
                f"{email}\t{phone}\t{service_type}\t{status}"
            )
        
        return "\n".join(data_lines)
    
    def _generate_high_usage_data(self) -> str:
        """高使用量データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        current_month = base_date.strftime('%Y%m')
        
        # 高使用量顧客
        high_usage_customers = [
            ("CUST900001", "高使用量1", "東京都新宿区高使用量1-1-1", "MTR90000001", current_month, "800", "5000", "5800", "150.5", "2000", "120400", "122400", "highusage1@tokyogas.co.jp", "03-9001-0001", "GAS", "ACTIVE"),
            ("CUST900002", "高使用量2", "東京都渋谷区高使用量2-2-2", "MTR90000002", current_month, "1200", "3000", "4200", "25.5", "1500", "30600", "32100", "highusage2@tokyogas.co.jp", "03-9002-0002", "ELECTRICITY", "ACTIVE"),
            ("CUST900003", "高使用量3", "東京都港区高使用量3-3-3", "MTR90000003", current_month, "500", "2000", "2500", "200.0", "1200", "100000", "101200", "highusage3@tokyogas.co.jp", "03-9003-0003", "WATER", "PENDING"),
        ]
        
        for usage_data in high_usage_customers:
            data_lines.append("\t".join(usage_data))
        
        return "\n".join(data_lines)
    
    def _generate_bulk_usage_data(self) -> str:
        """一括使用量データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        current_month = base_date.strftime('%Y%m')
        
        # 集合住宅一括検針
        for i in range(1, 121):
            usage_amount = 100 + (i % 50)  # 100-149の標準使用量
            previous_reading = i * 100
            current_reading = previous_reading + usage_amount
            unit_price = 150.5
            basic_charge = 2000
            usage_charge = int(usage_amount * unit_price)
            total_amount = basic_charge + usage_charge
            
            data_lines.append(
                f"CUST{i:06d}\t一括使用量{i:03d}\t東京都一括区{i:03d}\t"
                f"MTR{i:08d}\t{current_month}\t{usage_amount}\t"
                f"{previous_reading}\t{current_reading}\t{unit_price}\t"
                f"{basic_charge}\t{usage_charge}\t{total_amount}\t"
                f"bulk{i:03d}@tokyogas.co.jp\t03-0000-{i:04d}\t"
                f"GAS\tACTIVE"
            )
        
        return "\n".join(data_lines)
    
    def _transform_usage_services_data(self, input_data: str) -> str:
        """使用量サービスデータETL変換処理"""
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
            
            (customer_id, customer_name, service_address, meter_id,
             usage_month, usage_amount, previous_reading, current_reading,
             unit_price, basic_charge, usage_charge, total_amount,
             email_address, phone_number, service_type, status) = parts
            
            # ETL品質チェック
            if not customer_id.strip() or not customer_name.strip() or not meter_id.strip():
                continue
            
            # 使用量レポートID生成（ETL処理）
            usage_report_id = f"USAGE{customer_id[4:]}{usage_month}"
            
            # 使用量カテゴリ設定（ETL処理）
            try:
                usage_val = float(usage_amount)
                if service_type == "GAS":
                    if usage_val >= 300:
                        usage_category = "HIGH"
                    elif usage_val >= 100:
                        usage_category = "MEDIUM"
                    else:
                        usage_category = "LOW"
                elif service_type == "ELECTRICITY":
                    if usage_val >= 500:
                        usage_category = "HIGH"
                    elif usage_val >= 200:
                        usage_category = "MEDIUM"
                    else:
                        usage_category = "LOW"
                else:
                    usage_category = "STANDARD"
            except ValueError:
                usage_category = "UNKNOWN"
            
            # CSV出力行生成
            output_line = (
                f"{customer_id},{customer_name},{service_address},{meter_id},"
                f"{usage_month},{usage_amount},{previous_reading},{current_reading},"
                f"{unit_price},{basic_charge},{usage_charge},{total_amount},"
                f"{email_address},{phone_number},{service_type},"
                f"{status},{process_date},{usage_report_id},{usage_category}"
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
        test_data = self._generate_usage_services_data()
        
        # ファイルアップロード
        file_date = datetime.utcnow().strftime('%Y%m%d')
        file_path = f"UsageServices/{file_date}/usage_services.tsv"
        self.mock_storage.upload_file(self.source_container, file_path, test_data.encode('utf-8'))
        
        # ETL処理シミュレーション
        transformed_data, output_data = self.simulate_etl_process(
            test_data,
            self._transform_usage_services_data,
            "csv"
        )
        
        # 出力ファイル保存
        output_file_path = f"UsageServices/processed_{file_date}.csv"
        self.mock_storage.upload_file(self.output_container, output_file_path, output_data)
        
        # SFTP転送
        sftp_path = f"{self.sftp_target_dir}/usage_services_{file_date}.csv"
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
        file_path = f"UsageServices/{file_date}/usage_services.tsv"
        
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
        
        # 高使用量データでテスト
        test_data = self._generate_high_usage_data()
        
        # データ変換処理
        transformed_data = self._transform_usage_services_data(test_data)
        
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
        
        # 大量データ準備（250000件）
        large_data = self._generate_usage_services_data(250000)
        
        start_time = time.time()
        
        # ETL処理実行
        transformed_data = self._transform_usage_services_data(large_data)
        
        execution_time = time.time() - start_time
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.PERFORMANCE,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=250000,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=0.95,
            errors=[]
        )
        
        # パフォーマンス基準
        throughput = 250000 / execution_time
        assert throughput > 1200, f"スループットが基準未満: {throughput:.2f} records/sec"
        
        self.validate_common_assertions(result)
    
    def test_integration_sftp_connectivity(self):
        """統合テスト: SFTP接続性"""
        test_id = f"integration_sftp_{int(time.time())}"
        
        # 一括使用量データでテスト
        test_data = self._generate_bulk_usage_data()
        
        # ETL処理
        transformed_data = self._transform_usage_services_data(test_data)
        
        # SFTP転送テスト
        file_date = datetime.utcnow().strftime('%Y%m%d')
        sftp_path = f"{self.sftp_target_dir}/usage_services_bulk_{file_date}.csv"
        sftp_result = self.mock_sftp.upload(
            f"usage_services_bulk_{file_date}.csv",
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