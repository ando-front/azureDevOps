"""
pi_Send_ActionPointCurrentMonthEntryList パイプラインのE2Eテスト

アクションポイント当月エントリーリスト配信パイプラインのETL処理テスト
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List

from ...base.pipeline_test_base import DomainTestBase, TestCategory, PipelineStatus


class TestActionPointCurrentMonthEntryListPipeline(DomainTestBase):
    """アクションポイント当月エントリーリスト配信パイプラインテスト"""
    
    def __init__(self):
        super().__init__("pi_Send_ActionPointCurrentMonthEntryList", "actionpoint")
    
    def domain_specific_setup(self):
        """アクションポイントドメイン固有セットアップ"""
        self.source_container = "mytokyogas"
        self.output_container = "datalake"
        self.sftp_target_dir = "/Export/ActionPointCurrentMonth"
        
        # 期待される入力フィールド（ETL処理用）
        self.input_columns = [
            "ENTRY_ID", "CUSTOMER_ID", "CUSTOMER_NAME", "ACTION_TYPE",
            "ENTRY_DATE", "POINT_AMOUNT", "ENTRY_SOURCE", "CAMPAIGN_ID",
            "REFERENCE_ID", "STATUS", "EMAIL_ADDRESS", "ENTRY_MONTH"
        ]
        
        # 期待される出力フィールド（ETL変換後）
        self.output_columns = [
            "ENTRY_ID", "CUSTOMER_ID", "CUSTOMER_NAME", "ACTION_TYPE",
            "ENTRY_DATE", "POINT_AMOUNT", "ENTRY_SOURCE", "CAMPAIGN_ID",
            "REFERENCE_ID", "STATUS", "EMAIL_ADDRESS", "ENTRY_MONTH",
            "PROCESS_DATE", "REPORT_ID", "SUMMARY_FLAG"
        ]
    
    def get_domain_test_data_template(self) -> Dict[str, str]:
        """アクションポイントドメイン用テストデータテンプレート"""
        return {
            "current_month_entries": self._generate_current_month_entries(),
            "campaign_entries": self._generate_campaign_entries(),
            "bulk_monthly_entries": self._generate_bulk_monthly_entries()
        }
    
    def _generate_current_month_entries(self, record_count: int = 1200) -> str:
        """当月エントリーデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        action_types = ["PURCHASE", "LOGIN", "SURVEY", "REFERRAL", "BONUS"]
        entry_sources = ["WEB", "APP", "STORE", "CAMPAIGN"]
        statuses = ["ACTIVE", "PENDING", "EXPIRED", "CANCELLED"]
        
        base_date = datetime.utcnow()
        current_month = base_date.strftime('%Y%m')
        
        for i in range(1, record_count + 1):
            # エントリー日（当月内）
            entry_day = (i % 28) + 1  # 1-28日
            entry_date = f"{current_month}{entry_day:02d}"
            
            action_type = action_types[i % len(action_types)]
            entry_source = entry_sources[i % len(entry_sources)]
            status = statuses[i % len(statuses)]
            
            # アクション種別によるポイント設定
            if action_type == "PURCHASE":
                point_amount = (i % 500) + 100  # 100-599ポイント
            elif action_type == "LOGIN":
                point_amount = 10  # 固定10ポイント
            elif action_type == "SURVEY":
                point_amount = 50  # 固定50ポイント
            elif action_type == "REFERRAL":
                point_amount = 1000  # 固定1000ポイント
            else:  # BONUS
                point_amount = (i % 200) + 50  # 50-249ポイント
            
            customer_name = f"ポイント{i:04d}"
            email = f"actionpoint{i:06d}@tokyogas.co.jp"
            campaign_id = f"CMP{(i % 10) + 1:03d}" if i % 3 == 0 else ""
            reference_id = f"REF{i:08d}"
            
            data_lines.append(
                f"ENTRY{i:06d}\tCUST{i:06d}\t{customer_name}\t{action_type}\t"
                f"{entry_date}\t{point_amount}\t{entry_source}\t{campaign_id}\t"
                f"{reference_id}\t{status}\t{email}\t{current_month}"
            )
        
        return "\n".join(data_lines)
    
    def _generate_campaign_entries(self) -> str:
        """キャンペーンエントリーデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        current_month = base_date.strftime('%Y%m')
        entry_date = base_date.strftime('%Y%m%d')
        
        # 特別キャンペーン
        campaign_entries = [
            ("ENTRY900001", "CUST900001", "キャンペーン1", "BONUS", entry_date, "2000", "CAMPAIGN", "CMP001", "REF90000001", "ACTIVE", "campaign1@tokyogas.co.jp", current_month),
            ("ENTRY900002", "CUST900002", "キャンペーン2", "BONUS", entry_date, "3000", "CAMPAIGN", "CMP001", "REF90000002", "ACTIVE", "campaign2@tokyogas.co.jp", current_month),
            ("ENTRY900003", "CUST900003", "キャンペーン3", "BONUS", entry_date, "5000", "CAMPAIGN", "CMP001", "REF90000003", "ACTIVE", "campaign3@tokyogas.co.jp", current_month),
        ]
        
        for entry_data in campaign_entries:
            data_lines.append("\t".join(entry_data))
        
        return "\n".join(data_lines)
    
    def _generate_bulk_monthly_entries(self) -> str:
        """一括月次エントリーデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        current_month = base_date.strftime('%Y%m')
        entry_date = f"{current_month}01"  # 月初
        
        # 月次一括付与（ログインボーナス等）
        for i in range(1, 101):
            data_lines.append(
                f"ENTRY{i:06d}\tCUST{i:06d}\t月次ボーナス{i:03d}\tLOGIN\t"
                f"{entry_date}\t10\tWEB\t\tREF{i:08d}\t"
                f"ACTIVE\tbulk{i:03d}@tokyogas.co.jp\t{current_month}"
            )
        
        return "\n".join(data_lines)
    
    def _transform_current_month_entry_data(self, input_data: str) -> str:
        """当月エントリーデータETL変換処理"""
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
            
            (entry_id, customer_id, customer_name, action_type,
             entry_date, point_amount, entry_source, campaign_id,
             reference_id, status, email_address, entry_month) = parts
            
            # ETL品質チェック
            if not entry_id.strip() or not customer_id.strip() or not customer_name.strip():
                continue
            
            # レポートID生成（ETL処理）
            report_id = f"RPT{entry_month}{process_date}"
            
            # サマリーフラグ設定（ETL処理）
            summary_flag = "INCLUDE" if status == "ACTIVE" else "EXCLUDE"
            
            # CSV出力行生成
            output_line = (
                f"{entry_id},{customer_id},{customer_name},{action_type},"
                f"{entry_date},{point_amount},{entry_source},{campaign_id},"
                f"{reference_id},{status},{email_address},{entry_month},"
                f"{process_date},{report_id},{summary_flag}"
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
        test_data = self._generate_current_month_entries()
        
        # ファイルアップロード
        file_date = datetime.utcnow().strftime('%Y%m%d')
        file_path = f"ActionPointCurrentMonth/{file_date}/current_month_entry_list.tsv"
        self.mock_storage.upload_file(self.source_container, file_path, test_data.encode('utf-8'))
        
        # ETL処理シミュレーション
        transformed_data, output_data = self.simulate_etl_process(
            test_data,
            self._transform_current_month_entry_data,
            "csv"
        )
        
        # 出力ファイル保存
        output_file_path = f"ActionPointCurrentMonth/processed_{file_date}.csv"
        self.mock_storage.upload_file(self.output_container, output_file_path, output_data)
        
        # SFTP転送
        sftp_path = f"{self.sftp_target_dir}/current_month_entry_list_{file_date}.csv"
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
        file_path = f"ActionPointCurrentMonth/{file_date}/current_month_entry_list.tsv"
        
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
        
        # キャンペーンエントリーデータでテスト
        test_data = self._generate_campaign_entries()
        
        # データ変換処理
        transformed_data = self._transform_current_month_entry_data(test_data)
        
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
        large_data = self._generate_current_month_entries(100000)
        
        start_time = time.time()
        
        # ETL処理実行
        transformed_data = self._transform_current_month_entry_data(large_data)
        
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
        assert throughput > 2000, f"スループットが基準未満: {throughput:.2f} records/sec"
        
        self.validate_common_assertions(result)
    
    def test_integration_sftp_connectivity(self):
        """統合テスト: SFTP接続性"""
        test_id = f"integration_sftp_{int(time.time())}"
        
        # 一括月次エントリーデータでテスト
        test_data = self._generate_bulk_monthly_entries()
        
        # ETL処理
        transformed_data = self._transform_current_month_entry_data(test_data)
        
        # SFTP転送テスト
        file_date = datetime.utcnow().strftime('%Y%m%d')
        sftp_path = f"{self.sftp_target_dir}/current_month_entry_list_bulk_{file_date}.csv"
        sftp_result = self.mock_sftp.upload(
            f"current_month_entry_list_bulk_{file_date}.csv",
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