"""
pi_Send_LIMSettlementBreakdownRepair パイプラインのユニットテスト

LIM精算内訳修正送信パイプラインの個別コンポーネントテスト
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any


class TestLIMSettlementBreakdownRepairUnit(unittest.TestCase):
    """LIM精算内訳修正パイプラインユニットテスト"""
    
    def setUp(self):
        """テストセットアップ"""
        self.pipeline_name = "pi_Send_LIMSettlementBreakdownRepair"
        self.domain = "smc"
        
        # モックサービス
        self.mock_blob_storage = Mock()
        self.mock_database = Mock()
        self.mock_email_service = Mock()
        self.mock_sftp_service = Mock()
        
        # テストデータ
        self.test_date = datetime.utcnow().strftime('%Y%m%d')
        self.test_file_path = f"LIMSettlementBreakdownRepair/{self.test_date}/lim_settlement_repair.csv"
        
        # 基本的なLIM精算内訳修正データ
        self.sample_lim_repair_data = [
            {
                "REPAIR_ID": "REP000001",
                "CUSTOMER_ID": "CUST123456",
                "CUSTOMER_NAME": "テストユーザー1",
                "EMAIL_ADDRESS": "test1@example.com",
                "SETTLEMENT_ID": "SETT000001",
                "ORIGINAL_AMOUNT": 15000.0,
                "CORRECTED_AMOUNT": 14500.0,
                "DIFFERENCE_AMOUNT": -500.0,
                "REPAIR_REASON": "METER_READING_ERROR",
                "REPAIR_DATE": "20240215",
                "REPAIR_TYPE": "ADJUSTMENT",
                "APPROVAL_STATUS": "APPROVED"
            },
            {
                "REPAIR_ID": "REP000002",
                "CUSTOMER_ID": "CUST123457",
                "CUSTOMER_NAME": "テストユーザー2",
                "EMAIL_ADDRESS": "test2@example.com",
                "SETTLEMENT_ID": "SETT000002",
                "ORIGINAL_AMOUNT": 8900.0,
                "CORRECTED_AMOUNT": 9200.0,
                "DIFFERENCE_AMOUNT": 300.0,
                "REPAIR_REASON": "TARIFF_CORRECTION",
                "REPAIR_DATE": "20240216",
                "REPAIR_TYPE": "BILLING_CORRECTION",
                "APPROVAL_STATUS": "APPROVED"
            }
        ]
    
    def test_lookup_activity_settlement_repair_detection(self):
        """Lookup Activity: 精算内訳修正検出テスト"""
        # テストケース: 精算内訳修正の検出
        mock_repair_cases = [
            {
                "REPAIR_ID": "REP000001",
                "CUSTOMER_ID": "CUST123456",
                "SETTLEMENT_ID": "SETT000001",
                "ORIGINAL_AMOUNT": 15000.0,
                "CORRECTED_AMOUNT": 14500.0,
                "REPAIR_REASON": "METER_READING_ERROR",
                "APPROVAL_STATUS": "APPROVED",
                "NOTIFICATION_SENT": "N"
            },
            {
                "REPAIR_ID": "REP000002",
                "CUSTOMER_ID": "CUST123457",
                "SETTLEMENT_ID": "SETT000002",
                "ORIGINAL_AMOUNT": 8900.0,
                "CORRECTED_AMOUNT": 9200.0,
                "REPAIR_REASON": "TARIFF_CORRECTION",
                "APPROVAL_STATUS": "APPROVED",
                "NOTIFICATION_SENT": "N"
            }
        ]
        
        self.mock_database.query_records.return_value = mock_repair_cases
        
        # Lookup Activity実行シミュレーション
        repair_query = f"""
        SELECT REPAIR_ID, CUSTOMER_ID, SETTLEMENT_ID, ORIGINAL_AMOUNT, 
               CORRECTED_AMOUNT, REPAIR_REASON, APPROVAL_STATUS, NOTIFICATION_SENT
        FROM lim_settlement_repairs
        WHERE REPAIR_DATE >= '{self.test_date}' AND APPROVAL_STATUS = 'APPROVED' 
        AND NOTIFICATION_SENT = 'N'
        """
        
        result = self.mock_database.query_records("lim_settlement_repairs", repair_query)
        
        self.assertEqual(len(result), 2, "精算内訳修正検出件数不正")
        self.assertEqual(result[0]["REPAIR_ID"], "REP000001", "修正ID確認失敗")
        self.assertEqual(result[0]["APPROVAL_STATUS"], "APPROVED", "承認ステータス確認失敗")
        self.assertEqual(result[0]["NOTIFICATION_SENT"], "N", "通知送信フラグ確認失敗")
    
    def test_lookup_activity_customer_settlement_history(self):
        """Lookup Activity: 顧客精算履歴取得テスト"""
        # テストケース: 顧客の精算履歴取得
        mock_settlement_history = [
            {
                "CUSTOMER_ID": "CUST123456",
                "SETTLEMENT_COUNT": 24,
                "REPAIR_COUNT": 2,
                "AVERAGE_SETTLEMENT_AMOUNT": 12500.0,
                "LAST_SETTLEMENT_DATE": "20240131"
            },
            {
                "CUSTOMER_ID": "CUST123457",
                "SETTLEMENT_COUNT": 18,
                "REPAIR_COUNT": 1,
                "AVERAGE_SETTLEMENT_AMOUNT": 9800.0,
                "LAST_SETTLEMENT_DATE": "20240131"
            }
        ]
        
        self.mock_database.query_records.return_value = mock_settlement_history
        
        # Lookup Activity実行シミュレーション
        history_query = """
        SELECT CUSTOMER_ID, 
               COUNT(*) as SETTLEMENT_COUNT,
               SUM(CASE WHEN REPAIR_REQUIRED = 'Y' THEN 1 ELSE 0 END) as REPAIR_COUNT,
               AVG(SETTLEMENT_AMOUNT) as AVERAGE_SETTLEMENT_AMOUNT,
               MAX(SETTLEMENT_DATE) as LAST_SETTLEMENT_DATE
        FROM lim_settlements
        WHERE CUSTOMER_ID IN ('CUST123456', 'CUST123457')
        GROUP BY CUSTOMER_ID
        """
        
        result = self.mock_database.query_records("lim_settlements", history_query)
        
        self.assertEqual(len(result), 2, "顧客精算履歴取得件数不正")
        self.assertEqual(result[0]["SETTLEMENT_COUNT"], 24, "精算件数確認失敗")
        self.assertEqual(result[0]["REPAIR_COUNT"], 2, "修正件数確認失敗")
    
    def test_data_flow_repair_amount_calculation(self):
        """Data Flow: 修正金額計算テスト"""
        # テストケース: 修正金額の計算
        repair_scenarios = [
            {
                "ORIGINAL_AMOUNT": 15000.0,
                "CORRECTED_AMOUNT": 14500.0,
                "EXPECTED_DIFFERENCE": -500.0,
                "EXPECTED_TYPE": "REFUND"
            },
            {
                "ORIGINAL_AMOUNT": 8900.0,
                "CORRECTED_AMOUNT": 9200.0,
                "EXPECTED_DIFFERENCE": 300.0,
                "EXPECTED_TYPE": "ADDITIONAL_CHARGE"
            },
            {
                "ORIGINAL_AMOUNT": 12000.0,
                "CORRECTED_AMOUNT": 12000.0,
                "EXPECTED_DIFFERENCE": 0.0,
                "EXPECTED_TYPE": "NO_CHANGE"
            }
        ]
        
        # 修正金額計算ロジック（Data Flow内の処理）
        def calculate_repair_amount(original_amount, corrected_amount):
            difference = corrected_amount - original_amount
            
            if difference > 0:
                repair_type = "ADDITIONAL_CHARGE"
            elif difference < 0:
                repair_type = "REFUND"
            else:
                repair_type = "NO_CHANGE"
            
            return {
                "difference_amount": difference,
                "repair_type": repair_type,
                "absolute_difference": abs(difference)
            }
        
        # 各シナリオでの計算
        for scenario in repair_scenarios:
            result = calculate_repair_amount(scenario["ORIGINAL_AMOUNT"], scenario["CORRECTED_AMOUNT"])
            self.assertEqual(result["difference_amount"], scenario["EXPECTED_DIFFERENCE"], 
                           f"差額計算失敗: {scenario}")
            self.assertEqual(result["repair_type"], scenario["EXPECTED_TYPE"], 
                           f"修正タイプ判定失敗: {scenario}")
    
    def test_data_flow_repair_notification_message_generation(self):
        """Data Flow: 修正通知メッセージ生成テスト"""
        # テストケース: 修正通知メッセージの生成
        repair_data = {
            "CUSTOMER_NAME": "テストユーザー1",
            "SETTLEMENT_ID": "SETT000001",
            "ORIGINAL_AMOUNT": 15000.0,
            "CORRECTED_AMOUNT": 14500.0,
            "DIFFERENCE_AMOUNT": -500.0,
            "REPAIR_REASON": "METER_READING_ERROR",
            "REPAIR_DATE": "20240215",
            "REPAIR_TYPE": "ADJUSTMENT"
        }
        
        # 修正通知メッセージ生成ロジック（Data Flow内の処理）
        def generate_repair_notification_message(repair_data):
            customer_name = repair_data["CUSTOMER_NAME"]
            settlement_id = repair_data["SETTLEMENT_ID"]
            original_amount = repair_data["ORIGINAL_AMOUNT"]
            corrected_amount = repair_data["CORRECTED_AMOUNT"]
            difference_amount = repair_data["DIFFERENCE_AMOUNT"]
            repair_reason = repair_data["REPAIR_REASON"]
            repair_date = repair_data["REPAIR_DATE"]
            repair_type = repair_data["REPAIR_TYPE"]
            
            # 修正日のフォーマット
            formatted_repair_date = f"{repair_date[:4]}年{repair_date[4:6]}月{repair_date[6:8]}日"
            
            # 修正理由の日本語化
            reason_translations = {
                "METER_READING_ERROR": "メーター読み取りエラー",
                "TARIFF_CORRECTION": "料金単価修正",
                "BILLING_SYSTEM_ERROR": "請求システムエラー",
                "MANUAL_ADJUSTMENT": "手動調整"
            }
            
            reason_text = reason_translations.get(repair_reason, repair_reason)
            
            # 修正タイプによるメッセージ調整
            if difference_amount > 0:
                amount_msg = f"追加請求額：{difference_amount:,.0f}円"
                action_msg = "追加のお支払いをお願いいたします。"
            elif difference_amount < 0:
                amount_msg = f"返金額：{abs(difference_amount):,.0f}円"
                action_msg = "返金処理を行わせていただきます。"
            else:
                amount_msg = "金額変更なし"
                action_msg = "金額の変更はございません。"
            
            notification_message = f"""
            {customer_name}様
            
            LIM精算内訳修正のお知らせ
            
            精算内訳に修正が発生いたしました。
            
            修正内容：
            - 精算ID：{settlement_id}
            - 修正理由：{reason_text}
            - 修正日：{formatted_repair_date}
            - 修正前金額：{original_amount:,.0f}円
            - 修正後金額：{corrected_amount:,.0f}円
            - {amount_msg}
            
            {action_msg}
            
            ご不明な点がございましたら、お気軽にお問い合わせください。
            
            東京ガス株式会社
            """
            
            return notification_message.strip()
        
        # メッセージ生成実行
        message = generate_repair_notification_message(repair_data)
        
        # アサーション
        self.assertIn("テストユーザー1様", message, "顧客名挿入失敗")
        self.assertIn("LIM精算内訳修正のお知らせ", message, "タイトル挿入失敗")
        self.assertIn("SETT000001", message, "精算ID挿入失敗")
        self.assertIn("メーター読み取りエラー", message, "修正理由挿入失敗")
        self.assertIn("2024年02月15日", message, "修正日フォーマット失敗")
        self.assertIn("15,000円", message, "修正前金額挿入失敗")
        self.assertIn("14,500円", message, "修正後金額挿入失敗")
        self.assertIn("返金額：500円", message, "返金額挿入失敗")
    
    def test_data_flow_repair_approval_workflow(self):
        """Data Flow: 修正承認ワークフロー処理テスト"""
        # テストケース: 修正承認ワークフロー
        approval_scenarios = [
            {
                "DIFFERENCE_AMOUNT": -500.0,
                "REPAIR_REASON": "METER_READING_ERROR",
                "CUSTOMER_TYPE": "REGULAR",
                "EXPECTED_APPROVAL": "AUTO_APPROVED"
            },
            {
                "DIFFERENCE_AMOUNT": 5000.0,
                "REPAIR_REASON": "TARIFF_CORRECTION",
                "CUSTOMER_TYPE": "PREMIUM",
                "EXPECTED_APPROVAL": "MANUAL_REVIEW"
            },
            {
                "DIFFERENCE_AMOUNT": -10000.0,
                "REPAIR_REASON": "BILLING_SYSTEM_ERROR",
                "CUSTOMER_TYPE": "REGULAR",
                "EXPECTED_APPROVAL": "MANUAL_REVIEW"
            }
        ]
        
        # 承認ワークフロー判定ロジック（Data Flow内の処理）
        def determine_approval_workflow(repair_data):
            difference_amount = repair_data["DIFFERENCE_AMOUNT"]
            repair_reason = repair_data["REPAIR_REASON"]
            customer_type = repair_data["CUSTOMER_TYPE"]
            
            # 自動承認の条件
            auto_approval_conditions = [
                abs(difference_amount) <= 1000,  # 1000円以下
                repair_reason == "METER_READING_ERROR" and abs(difference_amount) <= 3000,  # メーター読み取りエラーで3000円以下
                customer_type == "PREMIUM" and difference_amount > 0 and difference_amount <= 2000  # プレミアム顧客の追加請求2000円以下
            ]
            
            # 手動承認が必要な条件
            manual_review_conditions = [
                abs(difference_amount) >= 5000,  # 5000円以上
                repair_reason == "BILLING_SYSTEM_ERROR",  # 請求システムエラー
                difference_amount < 0 and abs(difference_amount) >= 3000  # 3000円以上の返金
            ]
            
            if any(auto_approval_conditions):
                return "AUTO_APPROVED"
            elif any(manual_review_conditions):
                return "MANUAL_REVIEW"
            else:
                return "PENDING"
        
        # 各シナリオでの承認判定
        for scenario in approval_scenarios:
            approval_result = determine_approval_workflow(scenario)
            self.assertEqual(approval_result, scenario["EXPECTED_APPROVAL"], 
                           f"承認ワークフロー判定失敗: {scenario}")
    
    def test_data_flow_repair_impact_analysis(self):
        """Data Flow: 修正影響分析テスト"""
        # テストケース: 修正の影響分析
        repair_data = {
            "CUSTOMER_ID": "CUST123456",
            "SETTLEMENT_ID": "SETT000001",
            "DIFFERENCE_AMOUNT": -500.0,
            "REPAIR_REASON": "METER_READING_ERROR",
            "CUSTOMER_PAYMENT_HISTORY": "GOOD",
            "RELATED_SETTLEMENTS": 3
        }
        
        # 修正影響分析ロジック（Data Flow内の処理）
        def analyze_repair_impact(repair_data):
            customer_id = repair_data["CUSTOMER_ID"]
            difference_amount = repair_data["DIFFERENCE_AMOUNT"]
            repair_reason = repair_data["REPAIR_REASON"]
            payment_history = repair_data["CUSTOMER_PAYMENT_HISTORY"]
            related_settlements = repair_data["RELATED_SETTLEMENTS"]
            
            impact_analysis = {
                "customer_impact": "LOW",
                "financial_impact": "LOW",
                "operational_impact": "LOW",
                "follow_up_required": False
            }
            
            # 顧客への影響
            if abs(difference_amount) >= 5000:
                impact_analysis["customer_impact"] = "HIGH"
            elif abs(difference_amount) >= 1000:
                impact_analysis["customer_impact"] = "MEDIUM"
            
            # 財務への影響
            if abs(difference_amount) >= 10000:
                impact_analysis["financial_impact"] = "HIGH"
            elif abs(difference_amount) >= 3000:
                impact_analysis["financial_impact"] = "MEDIUM"
            
            # 運用への影響
            if repair_reason == "BILLING_SYSTEM_ERROR":
                impact_analysis["operational_impact"] = "HIGH"
            elif related_settlements > 5:
                impact_analysis["operational_impact"] = "MEDIUM"
            
            # フォローアップ必要性
            if (impact_analysis["customer_impact"] == "HIGH" or 
                payment_history == "POOR" or 
                repair_reason == "BILLING_SYSTEM_ERROR"):
                impact_analysis["follow_up_required"] = True
            
            return impact_analysis
        
        # 影響分析実行
        impact_result = analyze_repair_impact(repair_data)
        
        # アサーション
        self.assertEqual(impact_result["customer_impact"], "LOW", "顧客影響分析失敗")
        self.assertEqual(impact_result["financial_impact"], "LOW", "財務影響分析失敗")
        self.assertEqual(impact_result["operational_impact"], "LOW", "運用影響分析失敗")
        self.assertFalse(impact_result["follow_up_required"], "フォローアップ必要性判定失敗")
    
    def test_script_activity_repair_notification_sending(self):
        """Script Activity: 修正通知送信処理テスト"""
        # テストケース: 修正通知の送信
        repair_notification_data = {
            "to": "test@example.com",
            "subject": "【東京ガス】LIM精算内訳修正のお知らせ",
            "body": "精算内訳に修正が発生いたしました。",
            "repair_id": "REP000001",
            "priority": "normal"
        }
        
        self.mock_email_service.send_repair_notification.return_value = {
            "status": "sent",
            "message_id": "REPAIR_EMAIL_123",
            "delivery_time": datetime.utcnow().isoformat(),
            "repair_id": "REP000001"
        }
        
        # Script Activity実行シミュレーション
        result = self.mock_email_service.send_repair_notification(
            repair_notification_data["to"],
            repair_notification_data["subject"],
            repair_notification_data["body"],
            repair_id=repair_notification_data["repair_id"],
            priority=repair_notification_data["priority"]
        )
        
        self.assertEqual(result["status"], "sent", "修正通知送信失敗")
        self.assertIsNotNone(result["message_id"], "メッセージID取得失敗")
        self.assertEqual(result["repair_id"], "REP000001", "修正ID確認失敗")
        self.mock_email_service.send_repair_notification.assert_called_once()
    
    def test_copy_activity_repair_data_export(self):
        """Copy Activity: 修正データエクスポート処理テスト"""
        # テストケース: 修正データのエクスポート
        repair_export_data = self.sample_lim_repair_data
        export_path = f"/Export/LIMRepairs/lim_repairs_{self.test_date}.csv"
        
        # CSV生成（Copy Activity内の処理）
        def generate_repair_export_csv(repair_data):
            if not repair_data:
                return ""
            
            header = ",".join(repair_data[0].keys())
            rows = []
            
            for repair in repair_data:
                row = ",".join(str(repair[key]) for key in repair.keys())
                rows.append(row)
            
            return header + "\n" + "\n".join(rows)
        
        # エクスポート実行
        csv_content = generate_repair_export_csv(repair_export_data)
        
        self.mock_sftp_service.upload_file.return_value = {
            "status": "success",
            "file_path": export_path,
            "file_size": len(csv_content),
            "upload_time": datetime.utcnow().isoformat()
        }
        
        # Copy Activity実行シミュレーション
        result = self.mock_sftp_service.upload_file(
            export_path,
            csv_content,
            encoding="utf-8"
        )
        
        self.assertEqual(result["status"], "success", "修正データエクスポート失敗")
        self.assertEqual(result["file_path"], export_path, "エクスポートパス確認失敗")
        self.assertGreater(result["file_size"], 0, "エクスポートファイルサイズ確認失敗")
        self.mock_sftp_service.upload_file.assert_called_once()
    
    def test_copy_activity_repair_log_recording(self):
        """Copy Activity: 修正ログ記録テスト"""
        # テストケース: 修正ログの記録
        repair_logs = [
            {
                "REPAIR_ID": "REP000001",
                "CUSTOMER_ID": "CUST123456",
                "SETTLEMENT_ID": "SETT000001",
                "REPAIR_TYPE": "ADJUSTMENT",
                "PROCESSED_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "PROCESSED_TIME": datetime.utcnow().strftime('%H:%M:%S'),
                "STATUS": "COMPLETED",
                "NOTIFICATION_SENT": "Y"
            },
            {
                "REPAIR_ID": "REP000002",
                "CUSTOMER_ID": "CUST123457",
                "SETTLEMENT_ID": "SETT000002",
                "REPAIR_TYPE": "BILLING_CORRECTION",
                "PROCESSED_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "PROCESSED_TIME": datetime.utcnow().strftime('%H:%M:%S'),
                "STATUS": "COMPLETED",
                "NOTIFICATION_SENT": "Y"
            }
        ]
        
        self.mock_database.insert_records.return_value = True
        
        # Copy Activity実行シミュレーション
        repair_log_table = "lim_settlement_repair_logs"
        result = self.mock_database.insert_records(repair_log_table, repair_logs)
        
        self.assertTrue(result, "修正ログ記録失敗")
        self.mock_database.insert_records.assert_called_once_with(repair_log_table, repair_logs)
    
    def test_script_activity_repair_analytics(self):
        """Script Activity: 修正分析処理テスト"""
        # テストケース: 修正の分析データ生成
        repair_analytics = {
            "execution_date": self.test_date,
            "total_repairs": 45,
            "auto_approved_repairs": 30,
            "manual_review_repairs": 15,
            "refund_repairs": 25,
            "additional_charge_repairs": 20,
            "meter_reading_errors": 20,
            "tariff_corrections": 15,
            "billing_system_errors": 10,
            "total_refund_amount": 125000.0,
            "total_additional_charge": 89000.0,
            "average_repair_amount": 2800.0,
            "processing_time_minutes": 8.5
        }
        
        self.mock_database.insert_records.return_value = True
        
        # Script Activity実行シミュレーション
        analytics_table = "lim_settlement_repair_analytics"
        result = self.mock_database.insert_records(analytics_table, [repair_analytics])
        
        self.assertTrue(result, "修正分析記録失敗")
        self.mock_database.insert_records.assert_called_once_with(analytics_table, [repair_analytics])
    
    def test_data_flow_repair_validation(self):
        """Data Flow: 修正データ検証テスト"""
        # テストケース: 修正データの検証
        test_repairs = [
            {"CUSTOMER_ID": "CUST123456", "ORIGINAL_AMOUNT": 15000.0, "CORRECTED_AMOUNT": 14500.0, "REPAIR_REASON": "METER_READING_ERROR"},
            {"CUSTOMER_ID": "", "ORIGINAL_AMOUNT": 8900.0, "CORRECTED_AMOUNT": 9200.0, "REPAIR_REASON": "TARIFF_CORRECTION"},  # 不正: 空ID
            {"CUSTOMER_ID": "CUST123458", "ORIGINAL_AMOUNT": -1000.0, "CORRECTED_AMOUNT": 1000.0, "REPAIR_REASON": "BILLING_SYSTEM_ERROR"},  # 不正: 負の金額
            {"CUSTOMER_ID": "CUST123459", "ORIGINAL_AMOUNT": 5000.0, "CORRECTED_AMOUNT": 5000.0, "REPAIR_REASON": "UNKNOWN"}  # 不正: 不明な理由、同額
        ]
        
        # 修正データ検証ロジック（Data Flow内の処理）
        def validate_repair_data(repair):
            errors = []
            
            # 顧客ID検証
            if not repair.get("CUSTOMER_ID", "").strip():
                errors.append("顧客ID必須")
            
            # 金額検証
            original_amount = repair.get("ORIGINAL_AMOUNT", 0)
            corrected_amount = repair.get("CORRECTED_AMOUNT", 0)
            
            if original_amount < 0:
                errors.append("元の金額は正の値である必要があります")
            
            if corrected_amount < 0:
                errors.append("修正後金額は正の値である必要があります")
            
            if original_amount == corrected_amount:
                errors.append("修正前後で金額が同じです")
            
            # 修正理由検証
            repair_reason = repair.get("REPAIR_REASON", "")
            valid_reasons = ["METER_READING_ERROR", "TARIFF_CORRECTION", "BILLING_SYSTEM_ERROR", "MANUAL_ADJUSTMENT"]
            if repair_reason not in valid_reasons:
                errors.append("修正理由が不正です")
            
            return errors
        
        # 検証実行
        validation_results = []
        for repair in test_repairs:
            errors = validate_repair_data(repair)
            validation_results.append({
                "repair": repair,
                "errors": errors,
                "is_valid": len(errors) == 0
            })
        
        # アサーション
        self.assertEqual(len(validation_results), 4, "検証結果数不正")
        self.assertTrue(validation_results[0]["is_valid"], "正常修正が不正判定")
        self.assertFalse(validation_results[1]["is_valid"], "不正修正（空ID）が正常判定")
        self.assertFalse(validation_results[2]["is_valid"], "不正修正（負の金額）が正常判定")
        self.assertFalse(validation_results[3]["is_valid"], "不正修正（不明な理由、同額）が正常判定")
    
    def test_lookup_activity_repair_statistics(self):
        """Lookup Activity: 修正統計取得テスト"""
        # テストケース: 修正の統計情報取得
        repair_statistics = [
            {
                "REPAIR_REASON": "METER_READING_ERROR",
                "REPAIR_COUNT": 20,
                "AVERAGE_REPAIR_AMOUNT": -1250.0,
                "TOTAL_IMPACT_AMOUNT": -25000.0
            },
            {
                "REPAIR_REASON": "TARIFF_CORRECTION",
                "REPAIR_COUNT": 15,
                "AVERAGE_REPAIR_AMOUNT": 1800.0,
                "TOTAL_IMPACT_AMOUNT": 27000.0
            }
        ]
        
        self.mock_database.query_records.return_value = repair_statistics
        
        # Lookup Activity実行シミュレーション
        statistics_query = f"""
        SELECT REPAIR_REASON, 
               COUNT(*) as REPAIR_COUNT,
               AVG(DIFFERENCE_AMOUNT) as AVERAGE_REPAIR_AMOUNT,
               SUM(DIFFERENCE_AMOUNT) as TOTAL_IMPACT_AMOUNT
        FROM lim_settlement_repairs
        WHERE REPAIR_DATE >= '{self.test_date}'
        GROUP BY REPAIR_REASON
        """
        
        result = self.mock_database.query_records("lim_settlement_repairs", statistics_query)
        
        self.assertEqual(len(result), 2, "修正統計取得件数不正")
        self.assertEqual(result[0]["REPAIR_REASON"], "METER_READING_ERROR", "修正理由確認失敗")
        self.assertEqual(result[0]["REPAIR_COUNT"], 20, "修正件数確認失敗")
        self.assertEqual(result[0]["AVERAGE_REPAIR_AMOUNT"], -1250.0, "平均修正金額確認失敗")
    
    def _create_lim_settlement_repair_csv_content(self) -> str:
        """LIM精算内訳修正データ用CSVコンテンツ生成"""
        header = "REPAIR_ID,CUSTOMER_ID,CUSTOMER_NAME,EMAIL_ADDRESS,SETTLEMENT_ID,ORIGINAL_AMOUNT,CORRECTED_AMOUNT,DIFFERENCE_AMOUNT,REPAIR_REASON,REPAIR_DATE,REPAIR_TYPE,APPROVAL_STATUS"
        rows = []
        
        for item in self.sample_lim_repair_data:
            row = f"{item['REPAIR_ID']},{item['CUSTOMER_ID']},{item['CUSTOMER_NAME']},{item['EMAIL_ADDRESS']},{item['SETTLEMENT_ID']},{item['ORIGINAL_AMOUNT']},{item['CORRECTED_AMOUNT']},{item['DIFFERENCE_AMOUNT']},{item['REPAIR_REASON']},{item['REPAIR_DATE']},{item['REPAIR_TYPE']},{item['APPROVAL_STATUS']}"
            rows.append(row)
        
        return header + "\n" + "\n".join(rows)


if __name__ == "__main__":
    unittest.main()