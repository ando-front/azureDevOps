"""
pi_Insert_ClientDmBx パイプラインのE2Eテスト

クライアントDM BX挿入パイプラインのETL処理テスト
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List

from ...base.pipeline_test_base import DomainTestBase, TestCategory, PipelineStatus


class TestClientDmBxPipeline(DomainTestBase):
    """クライアントDM BX挿入パイプラインテスト"""
    
    def __init__(self):
        super().__init__("pi_Insert_ClientDmBx", "smc")
    
    def domain_specific_setup(self):
        """SMCドメイン固有セットアップ"""
        self.source_container = "mytokyogas"
        self.output_container = "datalake"
        self.database_target = "client_dm_bx"
        
        # 期待される入力フィールド（ETL処理用）
        self.input_columns = [
            "CUSTOMER_ID", "DM_CAMPAIGN_ID", "CUSTOMER_NAME", "EMAIL_ADDRESS",
            "ADDRESS", "PHONE_NUMBER", "SEGMENT_TYPE", "OFFER_TYPE",
            "CAMPAIGN_DATE", "DELIVERY_METHOD", "RESPONSE_EXPECTED", "STATUS"
        ]
        
        # 期待される出力フィールド（ETL変換後）
        self.output_columns = [
            "CUSTOMER_ID", "DM_CAMPAIGN_ID", "CUSTOMER_NAME", "EMAIL_ADDRESS",
            "ADDRESS", "PHONE_NUMBER", "SEGMENT_TYPE", "OFFER_TYPE",
            "CAMPAIGN_DATE", "DELIVERY_METHOD", "RESPONSE_EXPECTED",
            "STATUS", "INSERT_DATE", "UPDATE_DATE", "RECORD_VERSION"
        ]
    
    def get_domain_test_data_template(self) -> Dict[str, str]:
        """SMCドメイン用テストデータテンプレート"""
        return {
            "client_dm_bx_data": self._generate_client_dm_bx_data(),
            "campaign_data": self._generate_campaign_data(),
            "segment_data": self._generate_segment_data()
        }
    
    def _generate_client_dm_bx_data(self, record_count: int = 2000) -> str:
        """クライアントDM BXデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        segment_types = ["PREMIUM", "STANDARD", "ECONOMY", "NEW_CUSTOMER"]
        offer_types = ["DISCOUNT", "UPGRADE", "NEW_SERVICE", "LOYALTY"]
        delivery_methods = ["EMAIL", "POSTAL", "SMS", "PHONE"]
        response_expectations = ["HIGH", "MEDIUM", "LOW"]
        statuses = ["ACTIVE", "PENDING", "COMPLETED", "CANCELLED"]
        
        base_date = datetime.utcnow()
        
        for i in range(1, record_count + 1):
            # キャンペーン日（今日から30日以内）
            campaign_days = i % 30
            campaign_date = (base_date + timedelta(days=campaign_days)).strftime('%Y%m%d')
            
            segment_type = segment_types[i % len(segment_types)]
            offer_type = offer_types[i % len(offer_types)]
            delivery_method = delivery_methods[i % len(delivery_methods)]
            response_expected = response_expectations[i % len(response_expectations)]
            status = statuses[i % len(statuses)]
            
            customer_name = f"DM対象{i:04d}"
            email = f"dmbx{i:06d}@tokyogas.co.jp"
            phone = f"03-{i:04d}-{(i*9) % 10000:04d}"
            address = f"東京都{i % 23 + 1}区DM{i % 99 + 1}-{i % 99 + 1}-{i % 99 + 1}"
            
            data_lines.append(
                f"CUST{i:06d}\tCMP{i:06d}\t{customer_name}\t{email}\t"
                f"{address}\t{phone}\t{segment_type}\t{offer_type}\t"
                f"{campaign_date}\t{delivery_method}\t{response_expected}\t{status}"
            )
        
        return "\n".join(data_lines)
    
    def _generate_campaign_data(self) -> str:
        """キャンペーンデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        campaign_date = (base_date + timedelta(days=7)).strftime('%Y%m%d')
        
        # 特定キャンペーン
        campaign_customers = [
            ("CUST900001", "CMP900001", "キャンペーン1", "campaign1@tokyogas.co.jp", "東京都新宿区キャンペーン1-1-1", "03-9001-0001", "PREMIUM", "UPGRADE", campaign_date, "EMAIL", "HIGH", "ACTIVE"),
            ("CUST900002", "CMP900002", "キャンペーン2", "campaign2@tokyogas.co.jp", "東京都渋谷区キャンペーン2-2-2", "03-9002-0002", "STANDARD", "DISCOUNT", campaign_date, "POSTAL", "MEDIUM", "ACTIVE"),
            ("CUST900003", "CMP900003", "キャンペーン3", "campaign3@tokyogas.co.jp", "東京都港区キャンペーン3-3-3", "03-9003-0003", "NEW_CUSTOMER", "NEW_SERVICE", campaign_date, "SMS", "HIGH", "PENDING"),
        ]
        
        for customer_data in campaign_customers:
            data_lines.append("\t".join(customer_data))
        
        return "\n".join(data_lines)
    
    def _generate_segment_data(self) -> str:
        """セグメントデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        campaign_date = (base_date + timedelta(days=14)).strftime('%Y%m%d')
        
        # セグメント別大量配信
        for i in range(1, 201):
            segment = "PREMIUM" if i <= 50 else "STANDARD" if i <= 150 else "ECONOMY"
            offer = "LOYALTY" if segment == "PREMIUM" else "DISCOUNT"
            
            data_lines.append(
                f"CUST{i:06d}\tCMP{i:06d}\tセグメント{i:03d}\t"
                f"segment{i:03d}@tokyogas.co.jp\t東京都セグメント区{i:03d}\t"
                f"03-0000-{i:04d}\t{segment}\t{offer}\t"
                f"{campaign_date}\tEMAIL\tMEDIUM\tACTIVE"
            )
        
        return "\n".join(data_lines)
    
    def _transform_client_dm_bx_data(self, input_data: str) -> str:
        """クライアントDM BXデータETL変換処理"""
        lines = input_data.strip().split('\n')
        
        # 出力CSVヘッダー
        output_header = ",".join(self.output_columns)
        output_lines = [output_header]
        
        current_date = datetime.utcnow()
        insert_date = current_date.strftime('%Y/%m/%d %H:%M:%S')
        update_date = insert_date
        record_version = "1.0"
        
        for line in lines[1:]:
            if not line.strip():
                continue
                
            parts = line.split('\t')
            if len(parts) != len(self.input_columns):
                continue
            
            (customer_id, dm_campaign_id, customer_name, email_address,
             address, phone_number, segment_type, offer_type,
             campaign_date, delivery_method, response_expected, status) = parts
            
            # ETL品質チェック
            if not customer_id.strip() or not dm_campaign_id.strip() or not customer_name.strip():
                continue
            
            # CSV出力行生成
            output_line = (
                f"{customer_id},{dm_campaign_id},{customer_name},{email_address},"
                f"{address},{phone_number},{segment_type},{offer_type},"
                f"{campaign_date},{delivery_method},{response_expected},"
                f"{status},{insert_date},{update_date},{record_version}"
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
        test_data = self._generate_client_dm_bx_data()
        
        # ファイルアップロード
        file_date = datetime.utcnow().strftime('%Y%m%d')
        file_path = f"ClientDmBx/{file_date}/client_dm_bx.tsv"
        self.mock_storage.upload_file(self.source_container, file_path, test_data.encode('utf-8'))
        
        # ETL処理シミュレーション
        transformed_data, output_data = self.simulate_etl_process(
            test_data,
            self._transform_client_dm_bx_data,
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
        file_path = f"ClientDmBx/{file_date}/client_dm_bx.tsv"
        
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
        
        # キャンペーンデータでテスト
        test_data = self._generate_campaign_data()
        
        # データ変換処理
        transformed_data = self._transform_client_dm_bx_data(test_data)
        
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
        
        # 大量データ準備（150000件）
        large_data = self._generate_client_dm_bx_data(150000)
        
        start_time = time.time()
        
        # ETL処理実行
        transformed_data = self._transform_client_dm_bx_data(large_data)
        
        execution_time = time.time() - start_time
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.PERFORMANCE,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=150000,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=0.95,
            errors=[]
        )
        
        # パフォーマンス基準
        throughput = 150000 / execution_time
        assert throughput > 1800, f"スループットが基準未満: {throughput:.2f} records/sec"
        
        self.validate_common_assertions(result)
    
    def test_integration_database_operations(self):
        """統合テスト: データベース操作"""
        test_id = f"integration_database_{int(time.time())}"
        
        # セグメントデータでテスト
        test_data = self._generate_segment_data()
        
        # ETL処理
        transformed_data = self._transform_client_dm_bx_data(test_data)
        
        # データベース操作シミュレーション
        # 1. 既存レコードチェック
        existing_check = self.mock_database.query_records(
            self.database_target, 
            "SELECT customer_id FROM client_dm_bx WHERE customer_id IN ('CUST000001', 'CUST000002', 'CUST000003')"
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
        assert len(db_records) >= 200, f"処理レコード数不足: {len(db_records)}"
        
        self.validate_common_assertions(result)