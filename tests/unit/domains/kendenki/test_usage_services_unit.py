"""
pi_Send_UsageServices パイプラインのユニットテスト

使用量サービス送信パイプラインの個別コンポーネントテスト
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any


class TestUsageServicesUnit(unittest.TestCase):
    """使用量サービスパイプラインユニットテスト"""
    
    def setUp(self):
        """テストセットアップ"""
        self.pipeline_name = "pi_Send_UsageServices"
        self.domain = "kendenki"
        
        # モックサービス
        self.mock_blob_storage = Mock()
        self.mock_database = Mock()
        self.mock_sftp_service = Mock()
        
        # テストデータ
        self.test_date = datetime.utcnow().strftime('%Y%m%d')
        self.test_file_path = f"UsageServices/{self.test_date}/usage_services_data.csv"
        
        # 基本的な使用量サービスデータ
        self.sample_usage_data = [
            {
                "SERVICE_ID": "SVC000001",
                "CUSTOMER_ID": "CUST123456",
                "CUSTOMER_NAME": "テストユーザー1",
                "SERVICE_TYPE": "ELECTRICITY",
                "USAGE_AMOUNT": 250.5,
                "USAGE_UNIT": "kWh",
                "BILLING_PERIOD": "202401",
                "METER_READING_DATE": "20240131",
                "RATE_PLAN": "STANDARD",
                "CALCULATED_CHARGE": 6512.5
            },
            {
                "SERVICE_ID": "SVC000002",
                "CUSTOMER_ID": "CUST123457",
                "CUSTOMER_NAME": "テストユーザー2",
                "SERVICE_TYPE": "GAS",
                "USAGE_AMOUNT": 45.2,
                "USAGE_UNIT": "m³",
                "BILLING_PERIOD": "202401",
                "METER_READING_DATE": "20240131",
                "RATE_PLAN": "FAMILY",
                "CALCULATED_CHARGE": 3840.0
            }
        ]
    
    def test_lookup_activity_customer_service_info(self):
        """Lookup Activity: 顧客サービス情報取得テスト"""
        # テストケース: 顧客のサービス契約情報取得
        mock_service_info = [
            {
                "CUSTOMER_ID": "CUST123456",
                "SERVICE_TYPE": "ELECTRICITY",
                "CONTRACT_STATUS": "ACTIVE",
                "RATE_PLAN": "STANDARD",
                "CONTRACT_START_DATE": "20230401"
            },
            {
                "CUSTOMER_ID": "CUST123457",
                "SERVICE_TYPE": "GAS",
                "CONTRACT_STATUS": "ACTIVE",
                "RATE_PLAN": "FAMILY",
                "CONTRACT_START_DATE": "20230501"
            }
        ]
        
        self.mock_database.query_records.return_value = mock_service_info
        
        # Lookup Activity実行シミュレーション
        lookup_query = """
        SELECT CUSTOMER_ID, SERVICE_TYPE, CONTRACT_STATUS, RATE_PLAN, CONTRACT_START_DATE
        FROM customer_service_contracts
        WHERE CONTRACT_STATUS = 'ACTIVE'
        """
        
        result = self.mock_database.query_records("customer_service_contracts", lookup_query)
        
        self.assertEqual(len(result), 2, "顧客サービス情報取得件数不正")
        self.assertEqual(result[0]["CUSTOMER_ID"], "CUST123456", "顧客ID確認失敗")
        self.assertEqual(result[0]["SERVICE_TYPE"], "ELECTRICITY", "サービスタイプ確認失敗")
        self.assertEqual(result[0]["CONTRACT_STATUS"], "ACTIVE", "契約ステータス確認失敗")
    
    def test_lookup_activity_meter_reading_data(self):
        """Lookup Activity: メーター読み取りデータ取得テスト"""
        # テストケース: メーター読み取りデータの取得
        mock_meter_data = [
            {
                "METER_ID": "MTR001",
                "CUSTOMER_ID": "CUST123456",
                "READING_DATE": "20240131",
                "CURRENT_READING": 1250.5,
                "PREVIOUS_READING": 1000.0,
                "USAGE_AMOUNT": 250.5,
                "METER_TYPE": "SMART"
            },
            {
                "METER_ID": "MTR002",
                "CUSTOMER_ID": "CUST123457",
                "READING_DATE": "20240131",
                "CURRENT_READING": 545.2,
                "PREVIOUS_READING": 500.0,
                "USAGE_AMOUNT": 45.2,
                "METER_TYPE": "ANALOG"
            }
        ]
        
        self.mock_database.query_records.return_value = mock_meter_data
        
        # Lookup Activity実行シミュレーション
        meter_query = """
        SELECT METER_ID, CUSTOMER_ID, READING_DATE, CURRENT_READING, PREVIOUS_READING, USAGE_AMOUNT
        FROM meter_readings
        WHERE READING_DATE = '20240131'
        """
        
        result = self.mock_database.query_records("meter_readings", meter_query)
        
        self.assertEqual(len(result), 2, "メーター読み取りデータ取得件数不正")
        self.assertEqual(result[0]["USAGE_AMOUNT"], 250.5, "使用量確認失敗")
        self.assertEqual(result[1]["USAGE_AMOUNT"], 45.2, "使用量確認失敗")
    
    def test_data_flow_usage_calculation(self):
        """Data Flow: 使用量計算処理テスト"""
        # テストケース: 使用量計算ロジック
        meter_data = {
            "CURRENT_READING": 1250.5,
            "PREVIOUS_READING": 1000.0,
            "METER_MULTIPLIER": 1.0,
            "ADJUSTMENT_FACTOR": 0.98
        }
        
        # 使用量計算ロジック（Data Flow内の処理）
        def calculate_usage_amount(meter_data):
            raw_usage = meter_data["CURRENT_READING"] - meter_data["PREVIOUS_READING"]
            adjusted_usage = raw_usage * meter_data["METER_MULTIPLIER"] * meter_data["ADJUSTMENT_FACTOR"]
            
            # 小数点以下2桁に丸める
            return round(adjusted_usage, 2)
        
        # 計算実行
        usage_amount = calculate_usage_amount(meter_data)
        
        # アサーション
        expected_usage = round((1250.5 - 1000.0) * 1.0 * 0.98, 2)  # 245.49
        self.assertEqual(usage_amount, expected_usage, "使用量計算結果不正")
    
    def test_data_flow_rate_calculation(self):
        """Data Flow: 料金計算処理テスト"""
        # テストケース: 料金計算ロジック
        usage_data = {
            "USAGE_AMOUNT": 250.5,
            "SERVICE_TYPE": "ELECTRICITY",
            "RATE_PLAN": "STANDARD"
        }
        
        # 料金計算ロジック（Data Flow内の処理）
        def calculate_service_charge(usage_data):
            usage_amount = usage_data["USAGE_AMOUNT"]
            service_type = usage_data["SERVICE_TYPE"]
            rate_plan = usage_data["RATE_PLAN"]
            
            # 料金テーブル（簡略化）
            rate_table = {
                "ELECTRICITY": {
                    "STANDARD": {"base_rate": 20.0, "usage_rate": 26.0},
                    "FAMILY": {"base_rate": 15.0, "usage_rate": 24.0}
                },
                "GAS": {
                    "STANDARD": {"base_rate": 800.0, "usage_rate": 85.0},
                    "FAMILY": {"base_rate": 700.0, "usage_rate": 80.0}
                }
            }
            
            rates = rate_table.get(service_type, {}).get(rate_plan, {"base_rate": 0, "usage_rate": 0})
            
            # 基本料金 + 使用量料金
            total_charge = rates["base_rate"] + (usage_amount * rates["usage_rate"])
            
            return round(total_charge, 2)
        
        # 計算実行
        calculated_charge = calculate_service_charge(usage_data)
        
        # アサーション
        expected_charge = round(20.0 + (250.5 * 26.0), 2)  # 6533.0
        self.assertEqual(calculated_charge, expected_charge, "料金計算結果不正")
    
    def test_data_flow_usage_validation(self):
        """Data Flow: 使用量データ検証テスト"""
        # テストケース: 使用量データの検証
        test_usage_records = [
            {"CUSTOMER_ID": "CUST123456", "USAGE_AMOUNT": 250.5, "SERVICE_TYPE": "ELECTRICITY"},
            {"CUSTOMER_ID": "", "USAGE_AMOUNT": 150.0, "SERVICE_TYPE": "GAS"},  # 不正: 空ID
            {"CUSTOMER_ID": "CUST123458", "USAGE_AMOUNT": -50.0, "SERVICE_TYPE": "ELECTRICITY"},  # 不正: 負の使用量
            {"CUSTOMER_ID": "CUST123459", "USAGE_AMOUNT": 300.0, "SERVICE_TYPE": "UNKNOWN"}  # 不正: 不明サービス
        ]
        
        # データ検証ロジック（Data Flow内の処理）
        def validate_usage_record(record):
            errors = []
            
            # 顧客ID検証
            if not record.get("CUSTOMER_ID", "").strip():
                errors.append("顧客ID必須")
            
            # 使用量検証
            usage_amount = record.get("USAGE_AMOUNT", 0)
            if usage_amount < 0:
                errors.append("使用量は正の値である必要があります")
            elif usage_amount > 10000:  # 異常値チェック
                errors.append("使用量が異常に高い値です")
            
            # サービスタイプ検証
            service_type = record.get("SERVICE_TYPE", "")
            if service_type not in ["ELECTRICITY", "GAS", "WATER"]:
                errors.append("サービスタイプが不正です")
            
            return errors
        
        # 検証実行
        validation_results = []
        for record in test_usage_records:
            errors = validate_usage_record(record)
            validation_results.append({
                "record": record,
                "errors": errors,
                "is_valid": len(errors) == 0
            })
        
        # アサーション
        self.assertEqual(len(validation_results), 4, "検証結果数不正")
        self.assertTrue(validation_results[0]["is_valid"], "正常レコードが不正判定")
        self.assertFalse(validation_results[1]["is_valid"], "不正レコード（空ID）が正常判定")
        self.assertFalse(validation_results[2]["is_valid"], "不正レコード（負の使用量）が正常判定")
        self.assertFalse(validation_results[3]["is_valid"], "不正レコード（不明サービス）が正常判定")
    
    def test_data_flow_usage_aggregation(self):
        """Data Flow: 使用量集計処理テスト"""
        # テストケース: 使用量の集計処理
        usage_data = [
            {"CUSTOMER_ID": "CUST123456", "SERVICE_TYPE": "ELECTRICITY", "USAGE_AMOUNT": 250.5},
            {"CUSTOMER_ID": "CUST123456", "SERVICE_TYPE": "GAS", "USAGE_AMOUNT": 45.2},
            {"CUSTOMER_ID": "CUST123457", "SERVICE_TYPE": "ELECTRICITY", "USAGE_AMOUNT": 180.0},
            {"CUSTOMER_ID": "CUST123457", "SERVICE_TYPE": "GAS", "USAGE_AMOUNT": 38.5}
        ]
        
        # 集計処理ロジック（Data Flow内の処理）
        def aggregate_usage_by_customer(usage_data):
            customer_aggregates = {}
            
            for record in usage_data:
                customer_id = record["CUSTOMER_ID"]
                service_type = record["SERVICE_TYPE"]
                usage_amount = record["USAGE_AMOUNT"]
                
                if customer_id not in customer_aggregates:
                    customer_aggregates[customer_id] = {}
                
                if service_type not in customer_aggregates[customer_id]:
                    customer_aggregates[customer_id][service_type] = 0
                
                customer_aggregates[customer_id][service_type] += usage_amount
            
            return customer_aggregates
        
        # 集計実行
        aggregated_data = aggregate_usage_by_customer(usage_data)
        
        # アサーション
        self.assertEqual(len(aggregated_data), 2, "集計顧客数不正")
        self.assertEqual(aggregated_data["CUST123456"]["ELECTRICITY"], 250.5, "電気使用量集計不正")
        self.assertEqual(aggregated_data["CUST123456"]["GAS"], 45.2, "ガス使用量集計不正")
        self.assertEqual(aggregated_data["CUST123457"]["ELECTRICITY"], 180.0, "電気使用量集計不正")
        self.assertEqual(aggregated_data["CUST123457"]["GAS"], 38.5, "ガス使用量集計不正")
    
    def test_copy_activity_usage_file_generation(self):
        """Copy Activity: 使用量ファイル生成テスト"""
        # テストケース: 使用量データファイルの生成
        usage_records = self.sample_usage_data
        
        # CSV生成（Copy Activity内の処理）
        def generate_usage_csv(usage_records):
            if not usage_records:
                return ""
            
            header = ",".join(usage_records[0].keys())
            rows = []
            
            for record in usage_records:
                row = ",".join(str(record[key]) for key in record.keys())
                rows.append(row)
            
            return header + "\n" + "\n".join(rows)
        
        # CSV生成実行
        csv_content = generate_usage_csv(usage_records)
        
        # アサーション
        self.assertIn("SERVICE_ID,CUSTOMER_ID", csv_content, "CSVヘッダー確認失敗")
        self.assertIn("SVC000001,CUST123456", csv_content, "データ行確認失敗")
        self.assertIn("ELECTRICITY", csv_content, "サービスタイプ確認失敗")
        self.assertIn("250.5", csv_content, "使用量確認失敗")
    
    def test_copy_activity_sftp_upload(self):
        """Copy Activity: SFTP アップロード処理テスト"""
        # テストケース: SFTPサーバーへのファイルアップロード
        usage_csv_content = self._create_usage_csv_content()
        sftp_path = f"/Import/UsageServices/usage_services_{self.test_date}.csv"
        
        self.mock_sftp_service.upload_file.return_value = {
            "status": "success",
            "file_path": sftp_path,
            "file_size": len(usage_csv_content),
            "upload_time": datetime.utcnow().isoformat()
        }
        
        # Copy Activity実行シミュレーション
        result = self.mock_sftp_service.upload_file(
            sftp_path,
            usage_csv_content,
            encoding="utf-8"
        )
        
        self.assertEqual(result["status"], "success", "SFTPアップロード失敗")
        self.assertEqual(result["file_path"], sftp_path, "SFTPファイルパス確認失敗")
        self.assertGreater(result["file_size"], 0, "SFTPファイルサイズ確認失敗")
        self.mock_sftp_service.upload_file.assert_called_once()
    
    def test_copy_activity_sftp_upload_failure(self):
        """Copy Activity: SFTP アップロード失敗処理テスト"""
        # テストケース: SFTPアップロード失敗
        self.mock_sftp_service.upload_file.side_effect = Exception("SFTP接続失敗")
        
        usage_csv_content = self._create_usage_csv_content()
        sftp_path = f"/Import/UsageServices/usage_services_{self.test_date}.csv"
        
        with self.assertRaises(Exception) as context:
            self.mock_sftp_service.upload_file(sftp_path, usage_csv_content)
        
        self.assertIn("SFTP接続失敗", str(context.exception), "SFTPエラーメッセージ確認失敗")
    
    def test_script_activity_usage_statistics(self):
        """Script Activity: 使用量統計処理テスト"""
        # テストケース: 使用量統計の生成
        usage_statistics = {
            "execution_date": self.test_date,
            "total_customers": 2,
            "total_electricity_usage": 430.5,
            "total_gas_usage": 83.7,
            "average_electricity_usage": 215.25,
            "average_gas_usage": 41.85,
            "total_charges": 10352.5,
            "processing_time_seconds": 12.5
        }
        
        self.mock_database.insert_records.return_value = True
        
        # Script Activity実行シミュレーション
        statistics_table = "usage_service_statistics"
        result = self.mock_database.insert_records(statistics_table, [usage_statistics])
        
        self.assertTrue(result, "使用量統計記録失敗")
        self.mock_database.insert_records.assert_called_once_with(statistics_table, [usage_statistics])
    
    def test_lookup_activity_rate_plan_details(self):
        """Lookup Activity: 料金プラン詳細取得テスト"""
        # テストケース: 料金プラン詳細情報の取得
        rate_plan_details = [
            {
                "RATE_PLAN": "STANDARD",
                "SERVICE_TYPE": "ELECTRICITY",
                "BASE_RATE": 20.0,
                "USAGE_RATE": 26.0,
                "DISCOUNT_RATE": 0.0,
                "EFFECTIVE_DATE": "20240101"
            },
            {
                "RATE_PLAN": "FAMILY",
                "SERVICE_TYPE": "GAS",
                "BASE_RATE": 700.0,
                "USAGE_RATE": 80.0,
                "DISCOUNT_RATE": 0.05,
                "EFFECTIVE_DATE": "20240101"
            }
        ]
        
        self.mock_database.query_records.return_value = rate_plan_details
        
        # Lookup Activity実行シミュレーション
        rate_query = """
        SELECT RATE_PLAN, SERVICE_TYPE, BASE_RATE, USAGE_RATE, DISCOUNT_RATE
        FROM rate_plan_master
        WHERE EFFECTIVE_DATE <= '20240131'
        """
        
        result = self.mock_database.query_records("rate_plan_master", rate_query)
        
        self.assertEqual(len(result), 2, "料金プラン取得件数不正")
        self.assertEqual(result[0]["RATE_PLAN"], "STANDARD", "料金プラン名確認失敗")
        self.assertEqual(result[0]["BASE_RATE"], 20.0, "基本料金確認失敗")
        self.assertEqual(result[0]["USAGE_RATE"], 26.0, "使用量料金確認失敗")
    
    def test_data_flow_seasonal_adjustment(self):
        """Data Flow: 季節調整処理テスト"""
        # テストケース: 季節調整係数の適用
        usage_data = {
            "USAGE_AMOUNT": 250.5,
            "SERVICE_TYPE": "ELECTRICITY",
            "BILLING_PERIOD": "202401"  # 1月
        }
        
        # 季節調整ロジック（Data Flow内の処理）
        def apply_seasonal_adjustment(usage_data):
            usage_amount = usage_data["USAGE_AMOUNT"]
            service_type = usage_data["SERVICE_TYPE"]
            billing_period = usage_data["BILLING_PERIOD"]
            
            # 月を取得
            month = int(billing_period[-2:])
            
            # 季節調整係数テーブル
            seasonal_factors = {
                "ELECTRICITY": {
                    1: 1.2, 2: 1.15, 3: 1.05, 4: 1.0, 5: 1.0, 6: 1.0,
                    7: 1.3, 8: 1.35, 9: 1.1, 10: 1.0, 11: 1.05, 12: 1.15
                },
                "GAS": {
                    1: 1.4, 2: 1.3, 3: 1.2, 4: 1.0, 5: 0.8, 6: 0.7,
                    7: 0.6, 8: 0.6, 9: 0.7, 10: 0.9, 11: 1.1, 12: 1.3
                }
            }
            
            factor = seasonal_factors.get(service_type, {}).get(month, 1.0)
            adjusted_usage = usage_amount * factor
            
            return round(adjusted_usage, 2)
        
        # 季節調整実行
        adjusted_usage = apply_seasonal_adjustment(usage_data)
        
        # アサーション（1月の電気は1.2倍）
        expected_adjusted = round(250.5 * 1.2, 2)  # 300.6
        self.assertEqual(adjusted_usage, expected_adjusted, "季節調整結果不正")
    
    def _create_usage_csv_content(self) -> str:
        """使用量データ用CSVコンテンツ生成"""
        header = "SERVICE_ID,CUSTOMER_ID,CUSTOMER_NAME,SERVICE_TYPE,USAGE_AMOUNT,USAGE_UNIT,BILLING_PERIOD,METER_READING_DATE,RATE_PLAN,CALCULATED_CHARGE"
        rows = []
        
        for item in self.sample_usage_data:
            row = f"{item['SERVICE_ID']},{item['CUSTOMER_ID']},{item['CUSTOMER_NAME']},{item['SERVICE_TYPE']},{item['USAGE_AMOUNT']},{item['USAGE_UNIT']},{item['BILLING_PERIOD']},{item['METER_READING_DATE']},{item['RATE_PLAN']},{item['CALCULATED_CHARGE']}"
            rows.append(row)
        
        return header + "\n" + "\n".join(rows)


if __name__ == "__main__":
    unittest.main()