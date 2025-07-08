"""
pi_UtilityBills パイプラインのE2Eテスト

公共料金請求書処理パイプラインの包括的テスト
"""

import time
import json
from datetime import datetime, timedelta
from typing import Dict, List

from ...base.pipeline_test_base import DomainTestBase, TestCategory, PipelineStatus


class TestUtilityBillsPipeline(DomainTestBase):
    """公共料金請求書パイプラインテスト"""
    
    def __init__(self):
        super().__init__("pi_UtilityBills", "smc")
    
    def domain_specific_setup(self):
        """SMCドメイン固有セットアップ"""
        # 公共料金ドメイン用設定
        self.source_container = "mytokyogas"
        self.output_container = "datalake"
        self.sftp_target_dir = "/Import/SMC/UtilityBills"
        
        # 期待される入力フィールド
        self.input_columns = [
            "CUSTOMER_ID", "CONTRACT_NO", "BILLING_MONTH", "GAS_USAGE", 
            "GAS_AMOUNT", "ELECTRIC_USAGE", "ELECTRIC_AMOUNT", "TOTAL_AMOUNT",
            "DUE_DATE", "BILLING_ADDRESS", "METER_READING_DATE"
        ]
        
        # 期待される出力フィールド
        self.output_columns = [
            "CUSTOMER_ID", "CONTRACT_NO", "BILLING_MONTH", "GAS_USAGE", "GAS_AMOUNT",
            "ELECTRIC_USAGE", "ELECTRIC_AMOUNT", "TOTAL_AMOUNT", "DUE_DATE",
            "BILLING_ADDRESS", "USAGE_CATEGORY", "SEASON_ADJUSTMENT", 
            "CREATED_DATE", "PROCESS_STATUS"
        ]
    
    def get_domain_test_data_template(self) -> Dict[str, str]:
        """公共料金ドメイン用テストデータテンプレート"""
        return {
            "utility_bills_data": self._generate_utility_bills_data(),
            "seasonal_data": self._generate_seasonal_data(),
            "high_usage_data": self._generate_high_usage_data()
        }
    
    def _generate_utility_bills_data(self, record_count: int = 100) -> str:
        """公共料金データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        billing_month = base_date.strftime('%Y%m')
        
        for i in range(1, record_count + 1):
            # ガス使用量（10-500㎥）
            gas_usage = 50 + (i % 450)
            gas_amount = gas_usage * 150  # 1㎥あたり150円
            
            # 電気使用量（100-1000kWh）
            electric_usage = 200 + (i % 800)
            electric_amount = electric_usage * 25  # 1kWhあたり25円
            
            total_amount = gas_amount + electric_amount
            due_date = (base_date + timedelta(days=30)).strftime('%Y%m%d')
            meter_reading_date = (base_date - timedelta(days=5)).strftime('%Y%m%d')
            
            data_lines.append(
                f"CUST{i:06d}\tCONT{i:06d}\t{billing_month}\t{gas_usage}\t{gas_amount}\t"
                f"{electric_usage}\t{electric_amount}\t{total_amount}\t{due_date}\t"
                f"東京都渋谷区{i}番地\t{meter_reading_date}"
            )
        
        return "\n".join(data_lines)
    
    def _generate_seasonal_data(self) -> str:
        """季節別データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        # 夏季（7月）- 電気使用量大
        summer_month = "202407"
        data_lines.append(
            f"CUST000001\tCONT000001\t{summer_month}\t30\t4500\t"
            f"800\t20000\t24500\t20240830\t東京都新宿区1-1-1\t20240725"
        )
        
        # 冬季（1月）- ガス使用量大
        winter_month = "202401"
        data_lines.append(
            f"CUST000002\tCONT000002\t{winter_month}\t200\t30000\t"
            f"300\t7500\t37500\t20240228\t東京都港区2-2-2\t20240125"
        )
        
        # 春季（4月）- 標準的使用量
        spring_month = "202404"
        data_lines.append(
            f"CUST000003\tCONT000003\t{spring_month}\t80\t12000\t"
            f"250\t6250\t18250\t20240530\t東京都千代田区3-3-3\t20240425"
        )
        
        return "\n".join(data_lines)
    
    def _generate_high_usage_data(self) -> str:
        """高使用量データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        billing_month = datetime.utcnow().strftime('%Y%m')
        due_date = (datetime.utcnow() + timedelta(days=30)).strftime('%Y%m%d')
        meter_date = (datetime.utcnow() - timedelta(days=5)).strftime('%Y%m%d')
        
        # 高ガス使用量（事業者）
        data_lines.append(
            f"CUST900001\tCONT900001\t{billing_month}\t2000\t300000\t"
            f"500\t12500\t312500\t{due_date}\t東京都中央区1-1-1\t{meter_date}"
        )
        
        # 高電気使用量（大口需要家）
        data_lines.append(
            f"CUST900002\tCONT900002\t{billing_month}\t100\t15000\t"
            f"5000\t125000\t140000\t{due_date}\t東京都江東区2-2-2\t{meter_date}"
        )
        
        return "\n".join(data_lines)
    
    def _transform_utility_bills_data(self, input_data: str) -> str:
        """公共料金データ変換処理"""
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
            
            (customer_id, contract_no, billing_month, gas_usage, gas_amount,
             electric_usage, electric_amount, total_amount, due_date,
             billing_address, meter_reading_date) = parts
            
            # データ品質チェック
            if not all([customer_id.strip(), contract_no.strip(), billing_month.strip()]):
                continue
            
            # 使用量検証
            try:
                gas_usage_val = int(gas_usage)
                electric_usage_val = int(electric_usage)
                total_amount_val = int(total_amount)
                
                if gas_usage_val < 0 or electric_usage_val < 0 or total_amount_val < 0:
                    continue
                    
            except ValueError:
                continue
            
            # 使用量カテゴリ決定
            usage_category = self._determine_usage_category(gas_usage_val, electric_usage_val)
            
            # 季節調整計算
            season_adjustment = self._calculate_season_adjustment(billing_month, gas_usage_val, electric_usage_val)
            
            # 処理ステータス
            process_status = "PROCESSED"
            
            output_line = (
                f"{customer_id},{contract_no},{billing_month},{gas_usage},{gas_amount},"
                f"{electric_usage},{electric_amount},{total_amount},{due_date},"
                f"{billing_address},{usage_category},{season_adjustment},"
                f"{created_date},{process_status}"
            )
            output_lines.append(output_line)
        
        return '\n'.join(output_lines)
    
    def _determine_usage_category(self, gas_usage: int, electric_usage: int) -> str:
        """使用量カテゴリ決定"""
        if gas_usage >= 500 or electric_usage >= 1000:
            return "HIGH"
        elif gas_usage >= 100 or electric_usage >= 300:
            return "MEDIUM"
        else:
            return "LOW"
    
    def _calculate_season_adjustment(self, billing_month: str, gas_usage: int, electric_usage: int) -> str:
        """季節調整計算"""
        try:
            month = int(billing_month[-2:])
            
            # 夏季（6-8月）: 電気調整
            if 6 <= month <= 8:
                if electric_usage >= 500:
                    return "SUMMER_HIGH"
                else:
                    return "SUMMER_NORMAL"
            
            # 冬季（12-2月）: ガス調整
            elif month >= 12 or month <= 2:
                if gas_usage >= 150:
                    return "WINTER_HIGH"
                else:
                    return "WINTER_NORMAL"
            
            # その他の季節
            else:
                return "STANDARD"
                
        except (ValueError, IndexError):
            return "STANDARD"
    
    def validate_domain_business_rules(self, data: str) -> List[str]:
        """SMCドメイン公共料金ビジネスルール検証"""
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
            
            # 請求月形式チェック
            try:
                billing_month = parts[2]
                if len(billing_month) != 6:
                    violations.append(f"行{i}: 請求月形式不正 ({billing_month})")
                else:
                    datetime.strptime(billing_month, '%Y%m')
            except ValueError:
                violations.append(f"行{i}: 請求月形式不正 ({parts[2]})")
            
            # 使用量・金額チェック
            for idx, field_name in [(3, "ガス使用量"), (4, "ガス金額"), (5, "電気使用量"), (6, "電気金額"), (7, "合計金額")]:
                try:
                    value = int(parts[idx])
                    if value < 0:
                        violations.append(f"行{i}: {field_name}が負の値 ({parts[idx]})")
                except ValueError:
                    violations.append(f"行{i}: {field_name}形式不正 ({parts[idx]})")
            
            # 使用量カテゴリチェック
            valid_categories = ["LOW", "MEDIUM", "HIGH"]
            if parts[10] not in valid_categories:
                violations.append(f"行{i}: 使用量カテゴリ不正 ({parts[10]})")
            
            # 季節調整チェック
            valid_adjustments = ["SUMMER_HIGH", "SUMMER_NORMAL", "WINTER_HIGH", "WINTER_NORMAL", "STANDARD"]
            if parts[11] not in valid_adjustments:
                violations.append(f"行{i}: 季節調整不正 ({parts[11]})")
        
        return violations
    
    def test_functional_utility_bills_processing(self):
        """機能テスト: 公共料金請求書正常処理"""
        test_id = f"functional_bills_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # テストデータ準備
        test_data = self._generate_utility_bills_data()
        records_count = len(test_data.split('\n')) - 1
        
        # ファイルアップロード
        file_date = datetime.utcnow().strftime('%Y%m%d')
        file_path = f"SMC/UtilityBills/{file_date}/utility_bills.tsv"
        self.mock_storage.upload_file(self.source_container, file_path, test_data.encode('utf-8'))
        
        # ETL処理シミュレーション
        transformed_data, output_data = self.simulate_etl_process(
            test_data,
            self._transform_utility_bills_data,
            "csv"
        )
        
        # 出力ファイル保存
        output_file_path = f"SMC/UtilityBills/processed_{file_date}.csv"
        self.mock_storage.upload_file(self.output_container, output_file_path, output_data)
        
        # SFTP転送
        sftp_path = f"{self.sftp_target_dir}/utility_bills_{file_date}.csv"
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
        
        self.log_test_info(test_id, f"成功 - {result.records_loaded}件の請求書処理完了")
    
    def test_functional_seasonal_adjustment(self):
        """機能テスト: 季節調整処理"""
        test_id = f"functional_seasonal_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # 季節データ準備
        test_data = self._generate_seasonal_data()
        
        # データ変換処理
        transformed_data = self._transform_utility_bills_data(test_data)
        
        # 季節調整確認
        lines = transformed_data.split('\n')
        adjustments = []
        for line in lines[1:]:
            if line.strip():
                parts = line.split(',')
                if len(parts) >= 12:
                    adjustments.append(parts[11])  # 季節調整フィールド
        
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
        
        # 季節調整アサーション
        assert "SUMMER_HIGH" in adjustments, "夏季高使用量調整なし"
        assert "WINTER_HIGH" in adjustments, "冬季高使用量調整なし"
        assert "STANDARD" in adjustments, "標準調整なし"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, f"成功 - {len(adjustments)}件の季節調整処理完了")
    
    def test_data_quality_usage_validation(self):
        """データ品質テスト: 使用量検証"""
        test_id = f"data_quality_usage_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # 問題データ準備
        problematic_data = [
            "\t".join(self.input_columns),
            "CUST000001\tCONT000001\t202412\t100\t15000\t300\t7500\t22500\t20250131\t正常住所\t20241225",  # 正常
            "CUST000002\tCONT000002\t202412\t-50\t-7500\t200\t5000\t-2500\t20250131\t負の使用量\t20241225",  # 負の値
            "CUST000003\tCONT000003\t202412\tabc\txyz\t150\t3750\t3750\t20250131\t文字列使用量\t20241225",  # 非数値
            "CUST000004\tCONT000004\t999999\t80\t12000\t250\t6250\t18250\t20250131\t無効月\t20241225",  # 無効月
        ]
        test_data = "\n".join(problematic_data)
        
        # データ変換処理
        transformed_data = self._transform_utility_bills_data(test_data)
        
        # 品質検証
        quality_metrics = self.validate_data_quality(transformed_data, self.output_columns)
        violations = self.validate_domain_business_rules(transformed_data)
        
        # 有効レコード数確認（正常な1件のみ）
        output_lines = transformed_data.split('\n')
        data_lines = [line for line in output_lines[1:] if line.strip()]
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.DATA_QUALITY,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=4,
            records_transformed=len(data_lines),
            records_loaded=len(data_lines),
            data_quality_score=min(quality_metrics.values()),
            errors=[],
            warnings=violations
        )
        
        # データ品質アサーション
        assert len(data_lines) == 1, f"有効レコード数不正: 期待=1, 実際={len(data_lines)}"
        assert quality_metrics["completeness"] >= 0.8, f"完全性が基準未満: {quality_metrics['completeness']}"
        assert len(violations) == 0, f"ビジネスルール違反: {violations}"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, "成功 - 使用量データ品質検証完了")
    
    def test_performance_high_volume_bills(self):
        """パフォーマンステスト: 大量請求書処理"""
        test_id = f"performance_volume_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # 大量データ準備（10000件）
        large_data = self._generate_utility_bills_data(10000)
        
        start_time = time.time()
        
        # ETL処理シミュレーション
        transformed_data = self._transform_utility_bills_data(large_data)
        
        execution_time = time.time() - start_time
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.PERFORMANCE,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=10000,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=0.98,
            errors=[]
        )
        
        # パフォーマンス基準
        throughput = 10000 / execution_time
        assert throughput > 1000, f"スループットが基準未満: {throughput:.2f} records/sec"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, f"成功 - {execution_time:.2f}秒で処理完了")
    
    def test_integration_billing_system(self):
        """統合テスト: 請求システム統合"""
        test_id = f"integration_billing_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # 高使用量データ準備
        test_data = self._generate_high_usage_data()
        
        # ETL処理
        transformed_data = self._transform_utility_bills_data(test_data)
        
        # 高使用量レコード抽出
        lines = transformed_data.split('\n')
        high_usage_records = []
        for line in lines[1:]:
            if line.strip():
                parts = line.split(',')
                if len(parts) >= 11 and parts[10] == "HIGH":  # 使用量カテゴリ
                    high_usage_records.append(parts)
        
        # アラート生成シミュレーション
        alerts = []
        for record in high_usage_records:
            customer_id = record[0]
            total_amount = int(record[7])
            if total_amount > 100000:  # 10万円超
                alerts.append({
                    "customer_id": customer_id,
                    "alert_type": "HIGH_BILL_ALERT",
                    "amount": total_amount,
                    "created_at": datetime.utcnow().isoformat()
                })
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.INTEGRATION,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=len(test_data.split('\n')) - 1,
            records_transformed=len(lines) - 1,
            records_loaded=len(lines) - 1,
            data_quality_score=1.0,
            errors=[]
        )
        
        # 統合テストアサーション
        assert len(high_usage_records) >= 2, f"高使用量レコード数不足: {len(high_usage_records)}"
        assert len(alerts) >= 2, f"高額請求アラート数不足: {len(alerts)}"
        
        for alert in alerts:
            assert alert["amount"] > 100000, f"アラート金額が基準未満: {alert['amount']}"
            assert alert["alert_type"] == "HIGH_BILL_ALERT", "アラートタイプ不正"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, f"成功 - {len(alerts)}件の高額請求アラート生成完了")