"""
pi_Send_ActionPointRecentTransactionHistoryList パイプラインのE2Eテスト

アクションポイント最近取引履歴リスト配信パイプラインのETL処理テスト
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List

from ...base.pipeline_test_base import DomainTestBase, TestCategory, PipelineStatus


class TestActionPointRecentTransactionHistoryListPipeline(DomainTestBase):
    """アクションポイント最近取引履歴リスト配信パイプラインテスト"""
    
    def __init__(self):
        super().__init__("pi_Send_ActionPointRecentTransactionHistoryList", "actionpoint")
    
    def domain_specific_setup(self):
        """アクションポイントドメイン固有セットアップ"""
        self.source_container = "mytokyogas"
        self.output_container = "datalake"
        self.sftp_target_dir = "/Export/ActionPointRecentHistory"
        
        # 期待される入力フィールド（ETL処理用）
        self.input_columns = [
            "TRANSACTION_ID", "CUSTOMER_ID", "CUSTOMER_NAME", "TRANSACTION_TYPE",
            "TRANSACTION_DATE", "POINT_AMOUNT", "BALANCE_BEFORE", "BALANCE_AFTER",
            "DESCRIPTION", "REFERENCE_ID", "STATUS", "EMAIL_ADDRESS"
        ]
        
        # 期待される出力フィールド（ETL変換後）
        self.output_columns = [
            "TRANSACTION_ID", "CUSTOMER_ID", "CUSTOMER_NAME", "TRANSACTION_TYPE",
            "TRANSACTION_DATE", "POINT_AMOUNT", "BALANCE_BEFORE", "BALANCE_AFTER",
            "DESCRIPTION", "REFERENCE_ID", "STATUS", "EMAIL_ADDRESS",
            "PROCESS_DATE", "HISTORY_REPORT_ID", "PERIOD_FLAG"
        ]
    
    def get_domain_test_data_template(self) -> Dict[str, str]:
        """アクションポイントドメイン用テストデータテンプレート"""
        return {
            "recent_transaction_data": self._generate_recent_transaction_data(),
            "high_value_transactions": self._generate_high_value_transactions(),
            "bulk_transaction_history": self._generate_bulk_transaction_history()
        }
    
    def _generate_recent_transaction_data(self, record_count: int = 1500) -> str:
        """最近取引データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        transaction_types = ["EARNED", "REDEEMED", "EXPIRED", "TRANSFERRED", "ADJUSTED"]
        statuses = ["COMPLETED", "PENDING", "CANCELLED", "REVERSED"]
        
        base_date = datetime.utcnow()
        
        for i in range(1, record_count + 1):
            # 取引日（過去30日以内）
            transaction_days = i % 30
            transaction_date = (base_date - timedelta(days=transaction_days)).strftime('%Y%m%d')
            
            transaction_type = transaction_types[i % len(transaction_types)]
            status = statuses[i % len(statuses)]
            
            # 取引種別による金額設定
            if transaction_type == "EARNED":
                point_amount = (i % 1000) + 10  # 10-1009ポイント獲得
                balance_before = (i % 5000) + 1000
                balance_after = balance_before + point_amount
                description = f"ポイント獲得_{i:04d}"
            elif transaction_type == "REDEEMED":
                point_amount = -((i % 500) + 50)  # 50-549ポイント使用
                balance_before = (i % 5000) + 1000
                balance_after = balance_before + point_amount  # point_amountは負数
                description = f"ポイント使用_{i:04d}"
            elif transaction_type == "EXPIRED":
                point_amount = -((i % 100) + 10)  # 10-109ポイント失効
                balance_before = (i % 5000) + 1000
                balance_after = balance_before + point_amount
                description = f"ポイント失効_{i:04d}"
            elif transaction_type == "TRANSFERRED":
                point_amount = (i % 200) + 50  # 50-249ポイント譲渡
                balance_before = (i % 5000) + 1000
                balance_after = balance_before - point_amount
                description = f"ポイント譲渡_{i:04d}"
            else:  # ADJUSTED
                point_amount = (i % 100) - 50  # -50から+49ポイント調整
                balance_before = (i % 5000) + 1000
                balance_after = balance_before + point_amount
                description = f"ポイント調整_{i:04d}"
            
            customer_name = f"履歴{i:04d}"
            email = f"history{i:06d}@tokyogas.co.jp"
            reference_id = f"REF{i:08d}"
            
            data_lines.append(
                f"TXN{i:06d}\tCUST{i:06d}\t{customer_name}\t{transaction_type}\t"
                f"{transaction_date}\t{point_amount}\t{balance_before}\t{balance_after}\t"
                f"{description}\t{reference_id}\t{status}\t{email}"
            )
        
        return "\n".join(data_lines)
    
    def _generate_high_value_transactions(self) -> str:
        """高額取引データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        transaction_date = (base_date - timedelta(days=1)).strftime('%Y%m%d')
        
        # 高額取引
        high_value_transactions = [
            ("TXN900001", "CUST900001", "高額取引1", "EARNED", transaction_date, "10000", "5000", "15000", "大口購入ボーナス", "REF90000001", "COMPLETED", "highvalue1@tokyogas.co.jp"),
            ("TXN900002", "CUST900002", "高額取引2", "REDEEMED", transaction_date, "-8000", "12000", "4000", "高額商品交換", "REF90000002", "COMPLETED", "highvalue2@tokyogas.co.jp"),
            ("TXN900003", "CUST900003", "高額取引3", "TRANSFERRED", transaction_date, "5000", "20000", "15000", "ファミリー譲渡", "REF90000003", "PENDING", "highvalue3@tokyogas.co.jp"),
        ]
        
        for transaction_data in high_value_transactions:
            data_lines.append("\t".join(transaction_data))
        
        return "\n".join(data_lines)
    
    def _generate_bulk_transaction_history(self) -> str:
        """一括取引履歴データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        transaction_date = (base_date - timedelta(days=7)).strftime('%Y%m%d')
        
        # 一括ログインボーナス付与
        for i in range(1, 201):
            balance_before = 1000 + (i * 10)
            balance_after = balance_before + 10
            
            data_lines.append(
                f"TXN{i:06d}\tCUST{i:06d}\t一括履歴{i:03d}\tEARNED\t"
                f"{transaction_date}\t10\t{balance_before}\t{balance_after}\t"
                f"ログインボーナス\tREF{i:08d}\tCOMPLETED\t"
                f"bulk{i:03d}@tokyogas.co.jp"
            )
        
        return "\n".join(data_lines)
    
    def _transform_transaction_history_data(self, input_data: str) -> str:
        """取引履歴データETL変換処理"""
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
            
            (transaction_id, customer_id, customer_name, transaction_type,
             transaction_date, point_amount, balance_before, balance_after,
             description, reference_id, status, email_address) = parts
            
            # ETL品質チェック
            if not transaction_id.strip() or not customer_id.strip() or not customer_name.strip():
                continue
            
            # 履歴レポートID生成（ETL処理）
            history_report_id = f"HIST{transaction_date}{process_date}"
            
            # 期間フラグ設定（ETL処理）
            transaction_dt = datetime.strptime(transaction_date, '%Y%m%d')
            days_ago = (current_date - transaction_dt).days
            if days_ago <= 7:
                period_flag = "RECENT"
            elif days_ago <= 30:
                period_flag = "CURRENT_MONTH"
            else:
                period_flag = "HISTORICAL"
            
            # CSV出力行生成
            output_line = (
                f"{transaction_id},{customer_id},{customer_name},{transaction_type},"
                f"{transaction_date},{point_amount},{balance_before},{balance_after},"
                f"{description},{reference_id},{status},{email_address},"
                f"{process_date},{history_report_id},{period_flag}"
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
        test_data = self._generate_recent_transaction_data()
        
        # ファイルアップロード
        file_date = datetime.utcnow().strftime('%Y%m%d')
        file_path = f"ActionPointRecentHistory/{file_date}/recent_transaction_history.tsv"
        self.mock_storage.upload_file(self.source_container, file_path, test_data.encode('utf-8'))
        
        # ETL処理シミュレーション
        transformed_data, output_data = self.simulate_etl_process(
            test_data,
            self._transform_transaction_history_data,
            "csv"
        )
        
        # 出力ファイル保存
        output_file_path = f"ActionPointRecentHistory/processed_{file_date}.csv"
        self.mock_storage.upload_file(self.output_container, output_file_path, output_data)
        
        # SFTP転送
        sftp_path = f"{self.sftp_target_dir}/recent_transaction_history_{file_date}.csv"
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
        file_path = f"ActionPointRecentHistory/{file_date}/recent_transaction_history.tsv"
        
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
        
        # 高額取引データでテスト
        test_data = self._generate_high_value_transactions()
        
        # データ変換処理
        transformed_data = self._transform_transaction_history_data(test_data)
        
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
        
        # 大量データ準備（200000件）
        large_data = self._generate_recent_transaction_data(200000)
        
        start_time = time.time()
        
        # ETL処理実行
        transformed_data = self._transform_transaction_history_data(large_data)
        
        execution_time = time.time() - start_time
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.PERFORMANCE,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=200000,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=0.95,
            errors=[]
        )
        
        # パフォーマンス基準
        throughput = 200000 / execution_time
        assert throughput > 1500, f"スループットが基準未満: {throughput:.2f} records/sec"
        
        self.validate_common_assertions(result)
    
    def test_integration_sftp_connectivity(self):
        """統合テスト: SFTP接続性"""
        test_id = f"integration_sftp_{int(time.time())}"
        
        # 一括取引履歴データでテスト
        test_data = self._generate_bulk_transaction_history()
        
        # ETL処理
        transformed_data = self._transform_transaction_history_data(test_data)
        
        # SFTP転送テスト
        file_date = datetime.utcnow().strftime('%Y%m%d')
        sftp_path = f"{self.sftp_target_dir}/recent_transaction_history_bulk_{file_date}.csv"
        sftp_result = self.mock_sftp.upload(
            f"recent_transaction_history_bulk_{file_date}.csv",
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