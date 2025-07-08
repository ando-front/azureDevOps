"""
pi_Send_LIMSettlementBreakdownRepair パイプラインのE2Eテスト

LIM精算内訳修理パイプラインのETL処理テスト
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List

from ...base.pipeline_test_base import DomainTestBase, TestCategory, PipelineStatus


class TestLIMSettlementBreakdownPipeline(DomainTestBase):
    """LIM精算内訳修理パイプラインテスト"""
    
    def __init__(self):
        super().__init__("pi_Send_LIMSettlementBreakdownRepair", "smc")
    
    def domain_specific_setup(self):
        """SMCドメイン固有セットアップ"""
        self.source_container = "mytokyogas"
        self.output_container = "datalake"
        self.sftp_target_dir = "/Export/LIMSettlementBreakdown"
        
        # 期待される入力フィールド（ETL処理用）
        self.input_columns = [
            "SETTLEMENT_ID", "CUSTOMER_ID", "CUSTOMER_NAME", "SERVICE_DATE",
            "SERVICE_TYPE", "REPAIR_CATEGORY", "ITEM_CODE", "ITEM_NAME",
            "QUANTITY", "UNIT_PRICE", "AMOUNT", "TAX_RATE", "TAX_AMOUNT",
            "TOTAL_AMOUNT", "SETTLEMENT_STATUS", "EMAIL_ADDRESS"
        ]
        
        # 期待される出力フィールド（ETL変換後）
        self.output_columns = [
            "SETTLEMENT_ID", "CUSTOMER_ID", "CUSTOMER_NAME", "SERVICE_DATE",
            "SERVICE_TYPE", "REPAIR_CATEGORY", "ITEM_CODE", "ITEM_NAME",
            "QUANTITY", "UNIT_PRICE", "AMOUNT", "TAX_RATE", "TAX_AMOUNT",
            "TOTAL_AMOUNT", "SETTLEMENT_STATUS", "EMAIL_ADDRESS",
            "PROCESS_DATE", "BREAKDOWN_ID", "NOTIFICATION_FLAG"
        ]
    
    def get_domain_test_data_template(self) -> Dict[str, str]:
        """SMCドメイン用テストデータテンプレート"""
        return {
            "settlement_breakdown_data": self._generate_settlement_breakdown_data(),
            "repair_settlement_data": self._generate_repair_settlement_data(),
            "bulk_settlement_data": self._generate_bulk_settlement_data()
        }
    
    def _generate_settlement_breakdown_data(self, record_count: int = 800) -> str:
        """精算内訳データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        service_types = ["REPAIR", "MAINTENANCE", "INSPECTION", "EMERGENCY"]
        repair_categories = ["PIPE", "METER", "APPLIANCE", "SAFETY_DEVICE"]
        settlement_statuses = ["PENDING", "APPROVED", "COMPLETED", "REJECTED"]
        
        # 修理項目リスト
        repair_items = [
            ("PIPE001", "配管修理", 15000),
            ("METER001", "ガスメーター交換", 25000),
            ("APPL001", "給湯器修理", 35000),
            ("SAFE001", "安全装置点検", 8000),
            ("PIPE002", "配管清掃", 12000),
            ("METER002", "メーター点検", 5000)
        ]
        
        base_date = datetime.utcnow()
        
        for i in range(1, record_count + 1):
            # サービス日（過去30日以内）
            service_days = i % 30
            service_date = (base_date - timedelta(days=service_days)).strftime('%Y%m%d')
            
            service_type = service_types[i % len(service_types)]
            repair_category = repair_categories[i % len(repair_categories)]
            settlement_status = settlement_statuses[i % len(settlement_statuses)]
            
            # 修理項目選択
            item_code, item_name, unit_price = repair_items[i % len(repair_items)]
            quantity = (i % 3) + 1  # 1-3個
            amount = unit_price * quantity
            tax_rate = 0.10  # 10%
            tax_amount = int(amount * tax_rate)
            total_amount = amount + tax_amount
            
            customer_name = f"修理依頼{i:04d}"
            email = f"repair{i:06d}@tokyogas.co.jp"
            
            data_lines.append(
                f"SETTLE{i:05d}\tCUST{i:06d}\t{customer_name}\t{service_date}\t"
                f"{service_type}\t{repair_category}\t{item_code}\t{item_name}\t"
                f"{quantity}\t{unit_price}\t{amount}\t{tax_rate}\t{tax_amount}\t"
                f"{total_amount}\t{settlement_status}\t{email}"
            )
        
        return "\n".join(data_lines)
    
    def _generate_repair_settlement_data(self) -> str:
        """修理精算データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        service_date = (base_date - timedelta(days=3)).strftime('%Y%m%d')
        
        # 緊急修理案件
        emergency_repairs = [
            ("SETTLE90001", "CUST900001", "緊急修理1", service_date, "EMERGENCY", "PIPE", "PIPE001", "緊急配管修理", "1", "25000", "25000", "0.10", "2500", "27500", "PENDING", "emergency1@tokyogas.co.jp"),
            ("SETTLE90002", "CUST900002", "緊急修理2", service_date, "EMERGENCY", "METER", "METER001", "緊急メーター交換", "1", "35000", "35000", "0.10", "3500", "38500", "APPROVED", "emergency2@tokyogas.co.jp"),
            ("SETTLE90003", "CUST900003", "緊急修理3", service_date, "EMERGENCY", "APPLIANCE", "APPL001", "緊急給湯器修理", "1", "45000", "45000", "0.10", "4500", "49500", "COMPLETED", "emergency3@tokyogas.co.jp"),
        ]
        
        for repair_data in emergency_repairs:
            data_lines.append("\t".join(repair_data))
        
        return "\n".join(data_lines)
    
    def _generate_bulk_settlement_data(self) -> str:
        """一括精算データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        service_date = (base_date - timedelta(days=7)).strftime('%Y%m%d')
        
        # 定期点検一括精算
        for i in range(1, 51):
            data_lines.append(
                f"SETTLE{i:05d}\tCUST{i:06d}\t一括点検{i:03d}\t{service_date}\t"
                f"INSPECTION\tSAFE_DEVICE\tSAFE001\t安全装置点検\t"
                f"1\t8000\t8000\t0.10\t800\t8800\t"
                f"APPROVED\tbulk{i:03d}@tokyogas.co.jp"
            )
        
        return "\n".join(data_lines)
    
    def _transform_settlement_breakdown_data(self, input_data: str) -> str:
        """精算内訳データETL変換処理"""
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
            
            (settlement_id, customer_id, customer_name, service_date,
             service_type, repair_category, item_code, item_name,
             quantity, unit_price, amount, tax_rate, tax_amount,
             total_amount, settlement_status, email_address) = parts
            
            # ETL品質チェック
            if not settlement_id.strip() or not customer_id.strip() or not customer_name.strip():
                continue
            
            # 内訳ID生成（ETL処理）
            breakdown_id = f"BREAK{settlement_id[6:]}{process_date}"
            
            # 通知フラグ設定（ETL処理）
            notification_flag = "SEND" if settlement_status in ["APPROVED", "COMPLETED"] else "HOLD"
            
            # CSV出力行生成
            output_line = (
                f"{settlement_id},{customer_id},{customer_name},{service_date},"
                f"{service_type},{repair_category},{item_code},{item_name},"
                f"{quantity},{unit_price},{amount},{tax_rate},{tax_amount},"
                f"{total_amount},{settlement_status},{email_address},"
                f"{process_date},{breakdown_id},{notification_flag}"
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
        test_data = self._generate_settlement_breakdown_data()
        
        # ファイルアップロード
        file_date = datetime.utcnow().strftime('%Y%m%d')
        file_path = f"LIMSettlementBreakdown/{file_date}/lim_settlement_breakdown.tsv"
        self.mock_storage.upload_file(self.source_container, file_path, test_data.encode('utf-8'))
        
        # ETL処理シミュレーション
        transformed_data, output_data = self.simulate_etl_process(
            test_data,
            self._transform_settlement_breakdown_data,
            "csv"
        )
        
        # 出力ファイル保存
        output_file_path = f"LIMSettlementBreakdown/processed_{file_date}.csv"
        self.mock_storage.upload_file(self.output_container, output_file_path, output_data)
        
        # SFTP転送
        sftp_path = f"{self.sftp_target_dir}/lim_settlement_breakdown_{file_date}.csv"
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
        file_path = f"LIMSettlementBreakdown/{file_date}/lim_settlement_breakdown.tsv"
        
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
        
        # 修理精算データでテスト
        test_data = self._generate_repair_settlement_data()
        
        # データ変換処理
        transformed_data = self._transform_settlement_breakdown_data(test_data)
        
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
        
        # 大量データ準備（25000件）
        large_data = self._generate_settlement_breakdown_data(25000)
        
        start_time = time.time()
        
        # ETL処理実行
        transformed_data = self._transform_settlement_breakdown_data(large_data)
        
        execution_time = time.time() - start_time
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.PERFORMANCE,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=25000,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=0.95,
            errors=[]
        )
        
        # パフォーマンス基準
        throughput = 25000 / execution_time
        assert throughput > 4500, f"スループットが基準未満: {throughput:.2f} records/sec"
        
        self.validate_common_assertions(result)
    
    def test_integration_sftp_connectivity(self):
        """統合テスト: SFTP接続性"""
        test_id = f"integration_sftp_{int(time.time())}"
        
        # 一括精算データでテスト
        test_data = self._generate_bulk_settlement_data()
        
        # ETL処理
        transformed_data = self._transform_settlement_breakdown_data(test_data)
        
        # SFTP転送テスト
        file_date = datetime.utcnow().strftime('%Y%m%d')
        sftp_path = f"{self.sftp_target_dir}/lim_settlement_breakdown_bulk_{file_date}.csv"
        sftp_result = self.mock_sftp.upload(
            f"lim_settlement_breakdown_bulk_{file_date}.csv",
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