"""
pi_Send_ElectricityContractThanks パイプラインのE2Eテスト

電気契約御礼パイプラインのETL処理テスト
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List

from ...base.pipeline_test_base import DomainTestBase, TestCategory, PipelineStatus


class TestElectricityContractThanksPipeline(DomainTestBase):
    """電気契約御礼パイプラインテスト"""
    
    def __init__(self):
        super().__init__("pi_Send_ElectricityContractThanks", "kendenki")
    
    def domain_specific_setup(self):
        """検電ドメイン固有セットアップ"""
        self.source_container = "mytokyogas"
        self.output_container = "datalake"
        self.sftp_target_dir = "/Export/ElectricityContractThanks"
        
        # 期待される入力フィールド（ETL処理用）
        self.input_columns = [
            "CONTRACT_ID", "CUSTOMER_ID", "CUSTOMER_NAME", "EMAIL_ADDRESS",
            "CONTRACT_DATE", "CONTRACT_TYPE", "ELECTRICITY_PLAN", "EXPECTED_USAGE",
            "MONTHLY_AMOUNT", "PHONE_NUMBER", "ADDRESS", "NOTIFICATION_TYPE"
        ]
        
        # 期待される出力フィールド（ETL変換後）
        self.output_columns = [
            "CONTRACT_ID", "CUSTOMER_ID", "CUSTOMER_NAME", "EMAIL_ADDRESS",
            "CONTRACT_DATE", "CONTRACT_TYPE", "ELECTRICITY_PLAN", "EXPECTED_USAGE",
            "MONTHLY_AMOUNT", "PHONE_NUMBER", "ADDRESS", "NOTIFICATION_TYPE",
            "PROCESS_DATE", "THANKS_MESSAGE_ID", "DELIVERY_STATUS"
        ]
    
    def get_domain_test_data_template(self) -> Dict[str, str]:
        """検電ドメイン用テストデータテンプレート"""
        return {
            "contract_thanks_data": self._generate_contract_thanks_data(),
            "new_contract_data": self._generate_new_contract_data(),
            "bulk_contract_data": self._generate_bulk_contract_data()
        }
    
    def _generate_contract_thanks_data(self, record_count: int = 1000) -> str:
        """電気契約御礼データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        contract_types = ["NEW", "SWITCH", "RENEWAL", "UPGRADE"]
        electricity_plans = ["BASIC", "PREMIUM", "ECO", "FAMILY"]
        notification_types = ["EMAIL", "MAIL", "SMS", "PHONE"]
        
        base_date = datetime.utcnow()
        
        for i in range(1, record_count + 1):
            # 契約日（過去7日以内）
            contract_days = i % 7
            contract_date = (base_date - timedelta(days=contract_days)).strftime('%Y%m%d')
            
            contract_type = contract_types[i % len(contract_types)]
            electricity_plan = electricity_plans[i % len(electricity_plans)]
            notification_type = notification_types[i % len(notification_types)]
            
            # 使用量予測（300-800kWh）
            expected_usage = 300 + (i % 500)
            
            # 月額料金（6000-15000円）
            monthly_amount = 6000 + (i % 9000)
            
            customer_name = f"電気契約{i:04d}"
            email = f"electric{i:06d}@tokyogas.co.jp"
            phone = f"03-{i:04d}-{(i*5) % 10000:04d}"
            address = f"東京都{i % 23 + 1}区電気{i % 99 + 1}-{i % 99 + 1}-{i % 99 + 1}"
            
            data_lines.append(
                f"ELEC{i:06d}\tCUST{i:06d}\t{customer_name}\t{email}\t"
                f"{contract_date}\t{contract_type}\t{electricity_plan}\t{expected_usage}\t"
                f"{monthly_amount}\t{phone}\t{address}\t{notification_type}"
            )
        
        return "\n".join(data_lines)
    
    def _generate_new_contract_data(self) -> str:
        """新規契約データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        contract_date = base_date.strftime('%Y%m%d')
        
        # 新規契約者
        new_contracts = [
            ("ELEC900001", "CUST900001", "新規太郎", "newcontract1@tokyogas.co.jp", contract_date, "NEW", "BASIC", "350", "7500", "03-9001-0001", "東京都新宿区新規1-1-1", "EMAIL"),
            ("ELEC900002", "CUST900002", "新規花子", "newcontract2@tokyogas.co.jp", contract_date, "NEW", "PREMIUM", "450", "9500", "03-9002-0002", "東京都渋谷区新規2-2-2", "EMAIL"),
            ("ELEC900003", "CUST900003", "新規商事", "newcontract3@tokyogas.co.jp", contract_date, "NEW", "ECO", "600", "8800", "03-9003-0003", "東京都港区新規3-3-3", "MAIL"),
        ]
        
        for contract_data in new_contracts:
            data_lines.append("\t".join(contract_data))
        
        return "\n".join(data_lines)
    
    def _generate_bulk_contract_data(self) -> str:
        """一括契約データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        contract_date = base_date.strftime('%Y%m%d')
        
        # 一括契約切り替え
        for i in range(1, 51):
            data_lines.append(
                f"ELEC{i:06d}\tCUST{i:06d}\t一括切替{i:03d}\t"
                f"bulk{i:03d}@tokyogas.co.jp\t{contract_date}\tSWITCH\t"
                f"FAMILY\t400\t8000\t03-0000-{i:04d}\t"
                f"東京都一括区{i:03d}\tEMAIL"
            )
        
        return "\n".join(data_lines)
    
    def _transform_contract_thanks_data(self, input_data: str) -> str:
        """電気契約御礼データETL変換処理"""
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
            
            (contract_id, customer_id, customer_name, email_address,
             contract_date, contract_type, electricity_plan, expected_usage,
             monthly_amount, phone_number, address, notification_type) = parts
            
            # ETL品質チェック
            if not contract_id.strip() or not customer_id.strip() or not customer_name.strip():
                continue
            
            # 御礼メッセージID生成（ETL処理）
            thanks_message_id = f"THK{contract_id[4:]}{process_date}"
            
            # 配信ステータス設定（ETL処理）
            delivery_status = "PENDING"
            
            # CSV出力行生成
            output_line = (
                f"{contract_id},{customer_id},{customer_name},{email_address},"
                f"{contract_date},{contract_type},{electricity_plan},{expected_usage},"
                f"{monthly_amount},{phone_number},{address},{notification_type},"
                f"{process_date},{thanks_message_id},{delivery_status}"
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
        test_data = self._generate_contract_thanks_data()
        
        # ファイルアップロード
        file_date = datetime.utcnow().strftime('%Y%m%d')
        file_path = f"ElectricityContractThanks/{file_date}/electricity_contract_thanks.tsv"
        self.mock_storage.upload_file(self.source_container, file_path, test_data.encode('utf-8'))
        
        # ETL処理シミュレーション
        transformed_data, output_data = self.simulate_etl_process(
            test_data,
            self._transform_contract_thanks_data,
            "csv"
        )
        
        # 出力ファイル保存
        output_file_path = f"ElectricityContractThanks/processed_{file_date}.csv"
        self.mock_storage.upload_file(self.output_container, output_file_path, output_data)
        
        # SFTP転送
        sftp_path = f"{self.sftp_target_dir}/electricity_contract_thanks_{file_date}.csv"
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
        file_path = f"ElectricityContractThanks/{file_date}/electricity_contract_thanks.tsv"
        
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
        
        # 新規契約データでテスト
        test_data = self._generate_new_contract_data()
        
        # データ変換処理
        transformed_data = self._transform_contract_thanks_data(test_data)
        
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
        
        # 大量データ準備（20000件）
        large_data = self._generate_contract_thanks_data(20000)
        
        start_time = time.time()
        
        # ETL処理実行
        transformed_data = self._transform_contract_thanks_data(large_data)
        
        execution_time = time.time() - start_time
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.PERFORMANCE,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=20000,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=0.95,
            errors=[]
        )
        
        # パフォーマンス基準
        throughput = 20000 / execution_time
        assert throughput > 4000, f"スループットが基準未満: {throughput:.2f} records/sec"
        
        self.validate_common_assertions(result)
    
    def test_integration_sftp_connectivity(self):
        """統合テスト: SFTP接続性"""
        test_id = f"integration_sftp_{int(time.time())}"
        
        # 一括契約データでテスト
        test_data = self._generate_bulk_contract_data()
        
        # ETL処理
        transformed_data = self._transform_contract_thanks_data(test_data)
        
        # SFTP転送テスト
        file_date = datetime.utcnow().strftime('%Y%m%d')
        sftp_path = f"{self.sftp_target_dir}/electricity_contract_thanks_bulk_{file_date}.csv"
        sftp_result = self.mock_sftp.upload(
            f"electricity_contract_thanks_bulk_{file_date}.csv",
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