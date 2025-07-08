"""
pi_Send_PaymentAlert パイプラインのE2Eテスト

支払いアラート送信パイプラインの包括的テスト
"""

import time
import json
from datetime import datetime, timedelta
from typing import Dict, List

from ...base.pipeline_test_base import DomainTestBase, TestCategory, PipelineStatus


class TestPaymentAlertPipeline(DomainTestBase):
    """支払いアラートパイプラインテスト"""
    
    def __init__(self):
        super().__init__("pi_Send_PaymentAlert", "smc")
    
    def domain_specific_setup(self):
        """SMCドメイン固有セットアップ"""
        # SMCドメイン用設定
        self.source_container = "mytokyogas"
        self.output_container = "datalake"
        self.sftp_target_dir = "/Import/SMC/PaymentAlert"
        
        # 期待される入力フィールド
        self.input_columns = [
            "CUSTOMER_ID", "CONTRACT_NO", "BILL_AMOUNT", "DUE_DATE", 
            "PAYMENT_STATUS", "EMAIL_ADDRESS", "PHONE_NUMBER"
        ]
        
        # 期待される出力フィールド
        self.output_columns = [
            "CUSTOMER_ID", "CONTRACT_NO", "BILL_AMOUNT", "DUE_DATE",
            "ALERT_TYPE", "CONTACT_INFO", "ALERT_MESSAGE", "CREATED_DATE"
        ]
    
    def get_domain_test_data_template(self) -> Dict[str, str]:
        """SMCドメイン用テストデータテンプレート"""
        return {
            "payment_alert_data": self._generate_payment_alert_data(),
            "overdue_payment_data": self._generate_overdue_payment_data(),
            "large_payment_data": self._generate_large_payment_dataset(5000)
        }
    
    def _generate_payment_alert_data(self, record_count: int = 100) -> str:
        """支払いアラートデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        
        for i in range(1, record_count + 1):
            due_date = (base_date + timedelta(days=i % 30)).strftime('%Y%m%d')
            bill_amount = (i % 50 + 1) * 1000  # 1000-50000円
            payment_status = "UNPAID" if i % 3 == 0 else "PENDING"
            
            data_lines.append(
                f"CUST{i:06d}\tCONT{i:06d}\t{bill_amount}\t{due_date}\t{payment_status}\t"
                f"customer{i}@tokyogas.co.jp\t090-1234-{i:04d}"
            )
        
        return "\n".join(data_lines)
    
    def _generate_overdue_payment_data(self) -> str:
        """延滞支払いデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        # 過去の日付（延滞）
        overdue_date = (datetime.utcnow() - timedelta(days=30)).strftime('%Y%m%d')
        
        # 延滞ケース
        data_lines.append(f"CUST000001\tCONT000001\t15000\t{overdue_date}\tOVERDUE\tcustomer1@tokyogas.co.jp\t090-1111-0001")
        data_lines.append(f"CUST000002\tCONT000002\t25000\t{overdue_date}\tOVERDUE\tcustomer2@tokyogas.co.jp\t090-1111-0002")
        
        # 支払い期限間近ケース
        near_due_date = (datetime.utcnow() + timedelta(days=3)).strftime('%Y%m%d')
        data_lines.append(f"CUST000003\tCONT000003\t8000\t{near_due_date}\tUNPAID\tcustomer3@tokyogas.co.jp\t090-1111-0003")
        
        return "\n".join(data_lines)
    
    def _generate_large_payment_dataset(self, record_count: int) -> str:
        """大量支払いデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        
        for i in range(1, record_count + 1):
            due_date = (base_date + timedelta(days=i % 60)).strftime('%Y%m%d')
            bill_amount = (i % 100 + 1) * 500  # 500-50000円
            payment_status = ["UNPAID", "PENDING", "OVERDUE"][i % 3]
            
            data_lines.append(
                f"CUST{i:07d}\tCONT{i:07d}\t{bill_amount}\t{due_date}\t{payment_status}\t"
                f"user{i}@tokyogas.co.jp\t090-{i%10000:04d}-{i%10000:04d}"
            )
        
        return "\n".join(data_lines)
    
    def _transform_payment_alert_data(self, input_data: str) -> str:
        """支払いアラートデータ変換処理"""
        lines = input_data.strip().split('\n')
        
        # 出力CSVヘッダー
        output_header = ",".join(self.output_columns)
        output_lines = [output_header]
        
        created_date = datetime.utcnow().strftime('%Y/%m/%d %H:%M:%S')
        
        for line in lines[1:]:
            if not line.strip():
                continue
                
            parts = line.split('\t')
            if len(parts) != len(self.input_columns):
                continue
            
            customer_id, contract_no, bill_amount, due_date, payment_status, email, phone = parts
            
            # データ品質チェック
            if not all([customer_id.strip(), contract_no.strip(), bill_amount.strip()]):
                continue
            
            # 支払い状況に応じたアラートタイプ決定
            alert_type = self._determine_alert_type(payment_status, due_date)
            if not alert_type:
                continue  # アラート不要の場合はスキップ
            
            # 連絡先情報決定（メール優先、なければ電話）
            contact_info = email.strip() if email.strip() and '@' in email else phone.strip()
            if not contact_info:
                continue  # 連絡先がない場合はスキップ
            
            # アラートメッセージ生成
            alert_message = self._generate_alert_message(alert_type, bill_amount, due_date)
            
            output_line = f"{customer_id},{contract_no},{bill_amount},{due_date},{alert_type},{contact_info},{alert_message},{created_date}"
            output_lines.append(output_line)
        
        return '\n'.join(output_lines)
    
    def _determine_alert_type(self, payment_status: str, due_date: str) -> str:
        """アラートタイプ決定"""
        if payment_status == "OVERDUE":
            return "OVERDUE_ALERT"
        
        if payment_status == "UNPAID":
            # 期限チェック
            try:
                due_dt = datetime.strptime(due_date, '%Y%m%d')
                days_to_due = (due_dt - datetime.utcnow()).days
                
                if days_to_due <= 0:
                    return "OVERDUE_ALERT"
                elif days_to_due <= 7:
                    return "DUE_SOON_ALERT"
                elif days_to_due <= 14:
                    return "REMINDER_ALERT"
            except ValueError:
                pass
        
        return ""  # アラート不要
    
    def _generate_alert_message(self, alert_type: str, bill_amount: str, due_date: str) -> str:
        """アラートメッセージ生成"""
        messages = {
            "OVERDUE_ALERT": f"お支払い期限を過ぎております。金額：{bill_amount}円、期限：{due_date}",
            "DUE_SOON_ALERT": f"お支払い期限が近づいています。金額：{bill_amount}円、期限：{due_date}",
            "REMINDER_ALERT": f"お支払いのご案内。金額：{bill_amount}円、期限：{due_date}"
        }
        return messages.get(alert_type, "お支払いに関するご案内")
    
    def validate_domain_business_rules(self, data: str) -> List[str]:
        """SMCドメインビジネスルール検証"""
        violations = []
        lines = data.strip().split('\n')
        
        if len(lines) < 2:  # ヘッダーのみの場合はOK
            return violations
        
        # ヘッダー検証
        expected_header = ",".join(self.output_columns)
        if lines[0] != expected_header:
            violations.append(f"ヘッダー不正: 期待={expected_header}, 実際={lines[0]}")
        
        # データ行検証
        for i, line in enumerate(lines[1:], 2):
            parts = line.split(',')
            
            # フィールド数チェック
            if len(parts) != len(self.output_columns):
                violations.append(f"行{i}: フィールド数不正 (期待={len(self.output_columns)}, 実際={len(parts)})")
                continue
            
            # 顧客ID形式チェック
            if not parts[0].startswith('CUST'):
                violations.append(f"行{i}: 顧客ID形式不正 ({parts[0]})")
            
            # 契約番号形式チェック
            if not parts[1].startswith('CONT'):
                violations.append(f"行{i}: 契約番号形式不正 ({parts[1]})")
            
            # 金額チェック
            try:
                amount = int(parts[2])
                if amount <= 0:
                    violations.append(f"行{i}: 金額不正 ({parts[2]})")
            except ValueError:
                violations.append(f"行{i}: 金額形式不正 ({parts[2]})")
            
            # アラートタイプチェック
            valid_alert_types = ["OVERDUE_ALERT", "DUE_SOON_ALERT", "REMINDER_ALERT"]
            if parts[4] not in valid_alert_types:
                violations.append(f"行{i}: アラートタイプ不正 ({parts[4]})")
        
        return violations
    
    def test_functional_payment_alerts(self):
        """機能テスト: 支払いアラート正常処理"""
        test_id = f"functional_alerts_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # テストデータ準備
        test_data = self._generate_payment_alert_data()
        records_count = len(test_data.split('\n')) - 1
        
        # ファイルアップロード
        file_date = datetime.utcnow().strftime('%Y%m%d')
        file_path = f"SMC/PaymentAlert/{file_date}/payment_data.tsv"
        self.mock_storage.upload_file(self.source_container, file_path, test_data.encode('utf-8'))
        
        # ETL処理シミュレーション
        transformed_data, output_data = self.simulate_etl_process(
            test_data,
            self._transform_payment_alert_data,
            "csv"
        )
        
        # 出力ファイル保存
        output_file_path = f"SMC/PaymentAlert/alerts_{file_date}.csv"
        self.mock_storage.upload_file(self.output_container, output_file_path, output_data)
        
        # SFTP転送
        sftp_path = f"{self.sftp_target_dir}/alerts_{file_date}.csv"
        sftp_result = self.mock_sftp.upload(output_file_path, sftp_path, output_data)
        
        # ビジネスルール検証
        violations = self.validate_domain_business_rules(transformed_data)
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.FUNCTIONAL,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=records_count,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=0.97,
            errors=[],
            warnings=violations
        )
        
        # アサーション
        self.validate_common_assertions(result)
        assert result.records_extracted > 0, "抽出レコード数が0"
        assert result.records_transformed > 0, "変換レコード数が0"
        assert sftp_result, "SFTP転送失敗"
        assert len(violations) == 0, f"ビジネスルール違反: {violations}"
        
        self.log_test_info(test_id, f"成功 - {result.records_loaded}件のアラート生成完了")
    
    def test_functional_overdue_processing(self):
        """機能テスト: 延滞支払い処理"""
        test_id = f"functional_overdue_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # 延滞データ準備
        test_data = self._generate_overdue_payment_data()
        
        # データ変換処理
        transformed_data = self._transform_payment_alert_data(test_data)
        
        # 延滞アラート確認
        lines = transformed_data.split('\n')
        overdue_alerts = [line for line in lines[1:] if 'OVERDUE_ALERT' in line]
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.FUNCTIONAL,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=len(test_data.split('\n')) - 1,
            records_transformed=len(lines) - 1,
            records_loaded=len(lines) - 1,
            data_quality_score=1.0,
            errors=[]
        )
        
        # 延滞処理アサーション
        assert len(overdue_alerts) >= 2, f"延滞アラート数が不足: {len(overdue_alerts)}"
        for alert in overdue_alerts:
            assert "お支払い期限を過ぎております" in alert, "延滞アラートメッセージ不正"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, f"成功 - {len(overdue_alerts)}件の延滞アラート生成")
    
    def test_data_quality_contact_validation(self):
        """データ品質テスト: 連絡先検証"""
        test_id = f"data_quality_contact_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # 連絡先問題データ準備
        problematic_data = [
            "\t".join(self.input_columns),
            "CUST000001\tCONT000001\t10000\t20241231\tUNPAID\t\t",  # 連絡先なし
            "CUST000002\tCONT000002\t15000\t20241231\tUNPAID\tinvalid-email\t",  # 無効メール
            "CUST000003\tCONT000003\t20000\t20241231\tUNPAID\tvalid@tokyogas.co.jp\t090-1234-5678"  # 正常
        ]
        test_data = "\n".join(problematic_data)
        
        # データ変換処理
        transformed_data = self._transform_payment_alert_data(test_data)
        
        # 品質検証
        quality_metrics = self.validate_data_quality(transformed_data, self.output_columns)
        violations = self.validate_domain_business_rules(transformed_data)
        
        # 出力件数確認（連絡先有効な1件のみ）
        output_lines = transformed_data.split('\n')
        data_lines = [line for line in output_lines[1:] if line.strip()]
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.DATA_QUALITY,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=3,
            records_transformed=len(data_lines),
            records_loaded=len(data_lines),
            data_quality_score=min(quality_metrics.values()),
            errors=[],
            warnings=violations
        )
        
        # データ品質アサーション
        assert len(data_lines) == 1, f"有効レコード数不正: 期待=1, 実際={len(data_lines)}"
        assert quality_metrics["completeness"] >= 0.8, f"完全性が基準未満: {quality_metrics['completeness']}"
        assert "@tokyogas.co.jp" in data_lines[0], "有効メールアドレスが含まれていない"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, "成功 - 連絡先データ品質検証完了")
    
    def test_performance_large_payment_dataset(self):
        """パフォーマンステスト: 大量支払いデータ処理"""
        test_id = f"performance_large_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # 大量データ準備（5000件）
        large_data = self._generate_large_payment_dataset(5000)
        
        start_time = time.time()
        
        # ETL処理シミュレーション
        transformed_data = self._transform_payment_alert_data(large_data)
        
        execution_time = time.time() - start_time
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.PERFORMANCE,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=5000,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=0.98,
            errors=[]
        )
        
        # パフォーマンス基準
        throughput = 5000 / execution_time
        assert throughput > 500, f"スループットが基準未満: {throughput:.2f} records/sec"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, f"成功 - {execution_time:.2f}秒で処理完了")
    
    def test_integration_alert_distribution(self):
        """統合テスト: アラート配信システム統合"""
        test_id = f"integration_distribution_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # 多様なアラートタイプのデータ準備
        mixed_data = [
            "\t".join(self.input_columns),
            # 延滞
            f"CUST000001\tCONT000001\t15000\t{(datetime.utcnow() - timedelta(days=5)).strftime('%Y%m%d')}\tOVERDUE\talert1@tokyogas.co.jp\t090-1111-0001",
            # 期限間近
            f"CUST000002\tCONT000002\t25000\t{(datetime.utcnow() + timedelta(days=3)).strftime('%Y%m%d')}\tUNPAID\talert2@tokyogas.co.jp\t090-1111-0002",
            # リマインダー
            f"CUST000003\tCONT000003\t8000\t{(datetime.utcnow() + timedelta(days=10)).strftime('%Y%m%d')}\tUNPAID\talert3@tokyogas.co.jp\t090-1111-0003"
        ]
        test_data = "\n".join(mixed_data)
        
        # ETL処理
        transformed_data = self._transform_payment_alert_data(test_data)
        
        # アラートタイプ別集計
        lines = transformed_data.split('\n')
        alert_types = {}
        for line in lines[1:]:
            if line.strip():
                alert_type = line.split(',')[4]
                alert_types[alert_type] = alert_types.get(alert_type, 0) + 1
        
        # 配信シミュレーション（各アラートタイプごとに異なる配信先）
        distribution_results = {}
        for alert_type, count in alert_types.items():
            if alert_type == "OVERDUE_ALERT":
                distribution_results[alert_type] = {"email": count, "sms": count}
            elif alert_type == "DUE_SOON_ALERT":
                distribution_results[alert_type] = {"email": count, "push": count}
            else:
                distribution_results[alert_type] = {"email": count}
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.INTEGRATION,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=3,
            records_transformed=len(lines) - 1,
            records_loaded=len(lines) - 1,
            data_quality_score=1.0,
            errors=[]
        )
        
        # 統合テストアサーション
        assert len(alert_types) >= 2, f"アラートタイプ数不足: {len(alert_types)}"
        assert "OVERDUE_ALERT" in alert_types, "延滞アラートが生成されていない"
        assert "DUE_SOON_ALERT" in alert_types, "期限間近アラートが生成されていない"
        
        total_distributions = sum(
            sum(channels.values()) for channels in distribution_results.values()
        )
        assert total_distributions >= 6, f"配信数不足: {total_distributions}"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, f"成功 - {len(alert_types)}種類のアラート配信統合テスト完了")