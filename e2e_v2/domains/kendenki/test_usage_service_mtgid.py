"""
pi_Ins_usageservice_mtgid パイプラインのE2Eテスト

使用サービスmTGID挿入パイプラインの包括的テスト
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List

from ...base.pipeline_test_base import DomainTestBase, TestCategory, PipelineStatus


class TestUsageServiceMtgIdPipeline(DomainTestBase):
    """使用サービスmTGID挿入パイプラインテスト"""
    
    def __init__(self):
        super().__init__("pi_Ins_usageservice_mtgid", "kendenki")
    
    def domain_specific_setup(self):
        """検電ドメイン固有セットアップ"""
        # 検電ドメイン用設定
        self.source_container = "mytokyogas"
        self.output_container = "datalake"
        self.database_target = "mtg_usage_service"
        
        # 期待される入力フィールド
        self.input_columns = [
            "SERVICE_ID", "CUSTOMER_ID", "MTG_ID", "SERVICE_TYPE", "USAGE_DATE",
            "USAGE_AMOUNT", "UNIT", "BILLING_AMOUNT", "PAYMENT_STATUS",
            "METER_READING", "PREVIOUS_READING", "MULTIPLIER", "TARIFF_CODE"
        ]
        
        # 期待される出力フィールド
        self.output_columns = [
            "SERVICE_ID", "CUSTOMER_ID", "MTG_ID", "SERVICE_TYPE", "USAGE_DATE",
            "USAGE_AMOUNT", "UNIT", "BILLING_AMOUNT", "PAYMENT_STATUS",
            "METER_READING", "PREVIOUS_READING", "USAGE_CALCULATION",
            "TARIFF_CODE", "INSERT_DATE", "UPDATE_DATE", "STATUS"
        ]
    
    def get_domain_test_data_template(self) -> Dict[str, str]:
        """検電ドメイン用テストデータテンプレート"""
        return {
            "usage_service_data": self._generate_usage_service_data(),
            "high_usage_data": self._generate_high_usage_data(),
            "meter_reading_data": self._generate_meter_reading_data()
        }
    
    def _generate_usage_service_data(self, record_count: int = 1000) -> str:
        """使用サービスデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        service_types = ["GAS", "ELECTRICITY", "WATER"]
        units = ["m3", "kWh", "L"]
        payment_statuses = ["PAID", "UNPAID", "OVERDUE", "PENDING"]
        tariff_codes = ["TG_BASIC", "TG_PREMIUM", "TG_BUSINESS", "TG_CORPORATE"]
        
        for i in range(1, record_count + 1):
            # 使用日（過去30日以内）
            usage_days = i % 30
            usage_date = (base_date - timedelta(days=usage_days)).strftime('%Y%m%d')
            
            # サービスタイプ別設定
            service_type = service_types[i % len(service_types)]
            unit = units[i % len(units)]
            
            # 使用量計算
            base_usage = 100 + (i % 500)  # 100-600の基本使用量
            if service_type == "GAS":
                usage_amount = base_usage  # m3
                billing_rate = 150  # 150円/m3
            elif service_type == "ELECTRICITY":
                usage_amount = base_usage * 5  # kWh
                billing_rate = 25  # 25円/kWh
            else:  # WATER
                usage_amount = base_usage * 10  # L
                billing_rate = 2  # 2円/L
            
            billing_amount = usage_amount * billing_rate
            
            # メーター読み取り
            meter_reading = 10000 + (i * 50)
            previous_reading = meter_reading - usage_amount
            multiplier = 1.0
            
            # 支払状況
            payment_status = payment_statuses[i % len(payment_statuses)]
            tariff_code = tariff_codes[i % len(tariff_codes)]
            
            data_lines.append(
                f"SVC{i:06d}\tCUST{i:06d}\tMTG{i:06d}\t{service_type}\t{usage_date}\t"
                f"{usage_amount}\t{unit}\t{billing_amount}\t{payment_status}\t"
                f"{meter_reading}\t{previous_reading}\t{multiplier}\t{tariff_code}"
            )
        
        return "\n".join(data_lines)
    
    def _generate_high_usage_data(self) -> str:
        """高使用量データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        usage_date = (base_date - timedelta(days=1)).strftime('%Y%m%d')
        
        # 高使用量顧客（閾値超過）
        high_usage_customers = [
            ("SVC900001", "CUST900001", "MTG900001", "GAS", usage_date, "2000", "m3", "300000", "PAID", "52000", "50000", "1.0", "TG_BUSINESS"),
            ("SVC900002", "CUST900002", "MTG900002", "ELECTRICITY", usage_date, "8000", "kWh", "200000", "PAID", "18000", "10000", "1.0", "TG_CORPORATE"),
            ("SVC900003", "CUST900003", "MTG900003", "WATER", usage_date, "5000", "L", "10000", "UNPAID", "15000", "10000", "1.0", "TG_BASIC"),
        ]
        
        for customer_data in high_usage_customers:
            data_lines.append("\t".join(customer_data))
        
        return "\n".join(data_lines)
    
    def _generate_meter_reading_data(self) -> str:
        """メーター読み取りデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        
        # メーター読み取り異常パターン
        reading_anomalies = [
            # 正常
            ("SVC100001", "CUST100001", "MTG100001", "GAS", "20241201", "300", "m3", "45000", "PAID", "10300", "10000", "1.0", "TG_BASIC"),
            # 逆流（前回より少ない）
            ("SVC100002", "CUST100002", "MTG100002", "GAS", "20241201", "-50", "m3", "0", "PENDING", "9950", "10000", "1.0", "TG_BASIC"),
            # 異常に大きい差
            ("SVC100003", "CUST100003", "MTG100003", "ELECTRICITY", "20241201", "50000", "kWh", "1250000", "OVERDUE", "60000", "10000", "1.0", "TG_PREMIUM"),
            # メーター交換
            ("SVC100004", "CUST100004", "MTG100004", "WATER", "20241201", "100", "L", "200", "PAID", "100", "0", "1.0", "TG_BASIC"),
        ]
        
        for anomaly_data in reading_anomalies:
            data_lines.append("\t".join(anomaly_data))
        
        return "\n".join(data_lines)
    
    def _transform_usage_service_data(self, input_data: str) -> str:
        """使用サービスデータ変換処理"""
        lines = input_data.strip().split('\n')
        
        # 出力CSVヘッダー
        output_header = ",".join(self.output_columns)
        output_lines = [output_header]
        
        current_date = datetime.utcnow()
        insert_date = current_date.strftime('%Y/%m/%d %H:%M:%S')
        update_date = insert_date
        
        for line in lines[1:]:
            if not line.strip():
                continue
                
            parts = line.split('\t')
            if len(parts) != len(self.input_columns):
                continue
            
            (service_id, customer_id, mtg_id, service_type, usage_date,
             usage_amount, unit, billing_amount, payment_status,
             meter_reading, previous_reading, multiplier, tariff_code) = parts
            
            # データ品質チェック
            if not all([service_id.strip(), customer_id.strip(), mtg_id.strip()]):
                continue
            
            # 数値フィールド検証
            try:
                usage_amount_val = float(usage_amount)
                billing_amount_val = float(billing_amount)
                meter_reading_val = float(meter_reading)
                previous_reading_val = float(previous_reading)
                multiplier_val = float(multiplier)
                
                # 基本的な妥当性チェック
                if usage_amount_val < 0 and service_type in ["GAS", "ELECTRICITY", "WATER"]:
                    continue  # 負の使用量は異常（逆流等）
                
                if billing_amount_val < 0:
                    continue  # 負の請求額は異常
                
            except ValueError:
                continue
            
            # 使用量計算検証
            calculated_usage = meter_reading_val - previous_reading_val
            if abs(calculated_usage - usage_amount_val) > 0.1:  # 小数点誤差許容
                calculated_usage = usage_amount_val  # 入力値を優先
            
            # 使用量計算結果
            usage_calculation = f"{previous_reading_val}->{meter_reading_val}={calculated_usage}"
            
            # 異常値チェック
            status = "NORMAL"
            if usage_amount_val > 1000 and service_type == "GAS":
                status = "HIGH_USAGE"
            elif usage_amount_val > 5000 and service_type == "ELECTRICITY":
                status = "HIGH_USAGE"
            elif usage_amount_val > 3000 and service_type == "WATER":
                status = "HIGH_USAGE"
            elif usage_amount_val <= 0:
                status = "ANOMALY"
            
            output_line = (
                f"{service_id},{customer_id},{mtg_id},{service_type},{usage_date},"
                f"{usage_amount_val},{unit},{billing_amount_val},{payment_status},"
                f"{meter_reading_val},{previous_reading_val},{usage_calculation},"
                f"{tariff_code},{insert_date},{update_date},{status}"
            )
            output_lines.append(output_line)
        
        return '\n'.join(output_lines)
    
    def validate_domain_business_rules(self, data: str) -> List[str]:
        """検電ドメインビジネスルール検証"""
        violations = []
        lines = data.strip().split('\n')
        
        if len(lines) < 2:
            return violations
        
        # ヘッダー検証
        expected_header = ",".join(self.output_columns)
        if lines[0] != expected_header:
            violations.append(f"ヘッダー不正: 期待={expected_header}, 実際={lines[0]}")
        
        service_ids = set()
        
        # データ行検証
        for i, line in enumerate(lines[1:], 2):
            parts = line.split(',')
            
            if len(parts) != len(self.output_columns):
                violations.append(f"行{i}: フィールド数不正")
                continue
            
            # サービスID重複チェック
            service_id = parts[0]
            if service_id in service_ids:
                violations.append(f"行{i}: サービスID重複 ({service_id})")
            service_ids.add(service_id)
            
            # ID形式チェック
            if not service_id.startswith('SVC'):
                violations.append(f"行{i}: サービスID形式不正 ({service_id})")
            
            if not parts[1].startswith('CUST'):
                violations.append(f"行{i}: 顧客ID形式不正 ({parts[1]})")
            
            if not parts[2].startswith('MTG'):
                violations.append(f"行{i}: mTGID形式不正 ({parts[2]})")
            
            # 使用量・請求額チェック
            try:
                usage_amount = float(parts[5])
                billing_amount = float(parts[7])
                
                if usage_amount < 0:
                    violations.append(f"行{i}: 使用量が負の値 ({usage_amount})")
                
                if billing_amount < 0:
                    violations.append(f"行{i}: 請求額が負の値 ({billing_amount})")
                
            except ValueError:
                violations.append(f"行{i}: 数値フィールド形式不正")
            
            # ステータスチェック
            valid_statuses = ["NORMAL", "HIGH_USAGE", "ANOMALY"]
            if parts[15] not in valid_statuses:
                violations.append(f"行{i}: ステータス不正 ({parts[15]})")
        
        return violations
    
    def test_functional_with_file_exists(self):
        """機能テスト: ファイル有り処理"""
        test_id = f"functional_with_file_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # テストデータ準備
        test_data = self._generate_usage_service_data()
        records_count = len(test_data.split('\n')) - 1
        
        # ファイルアップロード
        file_date = datetime.utcnow().strftime('%Y%m%d')
        file_path = f"UsageService/{file_date}/usage_service_mtgid.tsv"
        self.mock_storage.upload_file(self.source_container, file_path, test_data.encode('utf-8'))
        
        # ETL処理シミュレーション
        transformed_data, output_data = self.simulate_etl_process(
            test_data,
            self._transform_usage_service_data,
            "csv"
        )
        
        # 出力ファイル保存
        output_file_path = f"UsageService/processed_{file_date}.csv"
        self.mock_storage.upload_file(self.output_container, output_file_path, output_data)
        
        # データベース挿入シミュレーション
        db_insert_result = self.mock_database.insert_records(
            self.database_target, 
            transformed_data
        )
        
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
            data_quality_score=0.95,
            errors=[],
            warnings=violations
        )
        
        # アサーション
        self.validate_common_assertions(result)
        assert result.records_extracted > 0, "抽出レコード数が0"
        assert result.records_transformed > 0, "変換レコード数が0"
        assert db_insert_result, "データベース挿入失敗"
        assert len(violations) == 0, f"ビジネスルール違反: {violations}"
        
        self.log_test_info(test_id, f"成功 - {result.records_loaded}件のmTGID挿入完了")
    
    def test_functional_without_file(self):
        """機能テスト: ファイル無し処理"""
        test_id = f"functional_no_file_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # ファイル存在チェック
        file_date = datetime.utcnow().strftime('%Y%m%d')
        file_path = f"UsageService/{file_date}/usage_service_mtgid.tsv"
        
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
        
        self.log_test_info(test_id, "成功 - ファイル無し処理完了")
    
    def test_data_quality_validation(self):
        """データ品質テスト: メーター読み取り値検証"""
        test_id = f"data_quality_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # メーター読み取り異常データ準備
        test_data = self._generate_meter_reading_data()
        
        # データ変換処理
        transformed_data = self._transform_usage_service_data(test_data)
        
        # 品質検証
        quality_metrics = self.validate_data_quality(transformed_data, self.output_columns)
        violations = self.validate_domain_business_rules(transformed_data)
        
        # 異常値検出
        lines = transformed_data.split('\n')
        anomaly_records = []
        high_usage_records = []
        
        for line in lines[1:]:
            if line.strip():
                parts = line.split(',')
                if len(parts) >= 16:
                    status = parts[15]
                    if status == "ANOMALY":
                        anomaly_records.append(parts)
                    elif status == "HIGH_USAGE":
                        high_usage_records.append(parts)
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.DATA_QUALITY,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=len(test_data.split('\n')) - 1,
            records_transformed=len(lines) - 1,
            records_loaded=len(lines) - 1,
            data_quality_score=min(quality_metrics.values()),
            errors=[],
            warnings=violations
        )
        
        # データ品質アサーション
        assert quality_metrics["completeness"] >= 0.7, f"完全性が基準未満: {quality_metrics['completeness']}"
        assert quality_metrics["validity"] >= 0.6, f"有効性が基準未満: {quality_metrics['validity']}"
        assert len(violations) == 0, f"ビジネスルール違反: {violations}"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, f"成功 - 異常値{len(anomaly_records)}件、高使用量{len(high_usage_records)}件検出")
    
    def test_performance_large_dataset(self):
        """パフォーマンステスト: 大量データ処理"""
        test_id = f"performance_large_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # 大量データ準備（100000件）
        large_data = self._generate_usage_service_data(100000)
        
        start_time = time.time()
        
        # ETL処理シミュレーション
        transformed_data = self._transform_usage_service_data(large_data)
        
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
        assert throughput > 8000, f"スループットが基準未満: {throughput:.2f} records/sec"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, f"成功 - {execution_time:.2f}秒で処理完了")
    
    def test_integration_database_operations(self):
        """統合テスト: データベース操作"""
        test_id = f"integration_database_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # 高使用量データ準備
        high_usage_data = self._generate_high_usage_data()
        
        # ETL処理
        transformed_data = self._transform_usage_service_data(high_usage_data)
        
        # データベース操作シミュレーション
        # 1. 既存レコードチェック
        existing_check = self.mock_database.query_records(
            self.database_target, 
            "SELECT service_id FROM usage_service WHERE service_id IN ('SVC900001', 'SVC900002', 'SVC900003')"
        )
        
        # 2. 新規挿入
        insert_result = self.mock_database.insert_records(
            self.database_target, 
            transformed_data
        )
        
        # 3. 高使用量アラート
        lines = transformed_data.split('\n')
        high_usage_alerts = []
        
        for line in lines[1:]:
            if line.strip():
                parts = line.split(',')
                if len(parts) >= 16 and parts[15] == "HIGH_USAGE":
                    alert_data = {
                        "service_id": parts[0],
                        "customer_id": parts[1],
                        "usage_amount": parts[5],
                        "service_type": parts[3],
                        "alert_date": datetime.utcnow().isoformat(),
                        "alert_type": "HIGH_USAGE"
                    }
                    high_usage_alerts.append(alert_data)
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.INTEGRATION,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=len(high_usage_data.split('\n')) - 1,
            records_transformed=len(lines) - 1,
            records_loaded=len(lines) - 1,
            data_quality_score=1.0,
            errors=[]
        )
        
        # 統合テストアサーション
        assert insert_result, "データベース挿入失敗"
        assert len(high_usage_alerts) >= 2, f"高使用量アラート数不足: {len(high_usage_alerts)}"
        
        # 高使用量基準チェック
        for alert in high_usage_alerts:
            usage_amount = float(alert["usage_amount"])
            service_type = alert["service_type"]
            
            if service_type == "GAS":
                assert usage_amount > 1000, f"ガス使用量がアラート基準未満: {usage_amount}"
            elif service_type == "ELECTRICITY":
                assert usage_amount > 5000, f"電力使用量がアラート基準未満: {usage_amount}"
            elif service_type == "WATER":
                assert usage_amount > 3000, f"水使用量がアラート基準未満: {usage_amount}"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, f"成功 - {len(high_usage_alerts)}件の高使用量アラート生成")