"""
pi_Send_Cpkiyk パイプラインのユニットテスト

Cpkiyk (顧客料金変更情報) 送信パイプラインの個別コンポーネントテスト
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any


class TestCpkiykUnit(unittest.TestCase):
    """Cpkiyk送信パイプラインユニットテスト"""
    
    def setUp(self):
        """テストセットアップ"""
        self.pipeline_name = "pi_Send_Cpkiyk"
        self.domain = "smc"
        
        # モックサービス
        self.mock_blob_storage = Mock()
        self.mock_database = Mock()
        self.mock_email_service = Mock()
        self.mock_sftp_service = Mock()
        
        # テストデータ
        self.test_date = datetime.utcnow().strftime('%Y%m%d')
        self.test_file_path = f"Cpkiyk/{self.test_date}/cpkiyk_data.csv"
        
        # 基本的なCpkiyk（顧客料金変更情報）データ
        self.sample_cpkiyk_data = [
            {
                "CPKIYK_ID": "CPKIYK000001",
                "CUSTOMER_ID": "CUST123456",
                "CUSTOMER_NAME": "テストユーザー1",
                "EMAIL_ADDRESS": "test1@example.com",
                "PHONE_NUMBER": "090-1234-5678",
                "TARIFF_CODE": "A123",
                "OLD_TARIFF_RATE": 150.0,
                "NEW_TARIFF_RATE": 155.0,
                "RATE_CHANGE_DATE": "20240301",
                "CHANGE_REASON": "REGULATORY_ADJUSTMENT",
                "ESTIMATED_IMPACT": 125.0,
                "CHANGE_TYPE": "INCREASE",
                "APPROVAL_STATUS": "APPROVED"
            },
            {
                "CPKIYK_ID": "CPKIYK000002",
                "CUSTOMER_ID": "CUST123457",
                "CUSTOMER_NAME": "テストユーザー2",
                "EMAIL_ADDRESS": "test2@example.com",
                "PHONE_NUMBER": "090-2345-6789",
                "TARIFF_CODE": "B456",
                "OLD_TARIFF_RATE": 140.0,
                "NEW_TARIFF_RATE": 138.0,
                "RATE_CHANGE_DATE": "20240301",
                "CHANGE_REASON": "COMPETITIVE_ADJUSTMENT",
                "ESTIMATED_IMPACT": -48.0,
                "CHANGE_TYPE": "DECREASE",
                "APPROVAL_STATUS": "APPROVED"
            }
        ]
    
    def test_lookup_activity_tariff_rate_changes(self):
        """Lookup Activity: 料金変更検出テスト"""
        # テストケース: 料金変更の検出
        mock_rate_changes = [
            {
                "CPKIYK_ID": "CPKIYK000001",
                "CUSTOMER_ID": "CUST123456",
                "TARIFF_CODE": "A123",
                "OLD_TARIFF_RATE": 150.0,
                "NEW_TARIFF_RATE": 155.0,
                "RATE_CHANGE_DATE": "20240301",
                "CHANGE_REASON": "REGULATORY_ADJUSTMENT",
                "NOTIFICATION_SENT": "N"
            },
            {
                "CPKIYK_ID": "CPKIYK000002",
                "CUSTOMER_ID": "CUST123457",
                "TARIFF_CODE": "B456",
                "OLD_TARIFF_RATE": 140.0,
                "NEW_TARIFF_RATE": 138.0,
                "RATE_CHANGE_DATE": "20240301",
                "CHANGE_REASON": "COMPETITIVE_ADJUSTMENT",
                "NOTIFICATION_SENT": "N"
            }
        ]
        
        self.mock_database.query_records.return_value = mock_rate_changes
        
        # Lookup Activity実行シミュレーション
        rate_changes_query = f"""
        SELECT CPKIYK_ID, CUSTOMER_ID, TARIFF_CODE, OLD_TARIFF_RATE, 
               NEW_TARIFF_RATE, RATE_CHANGE_DATE, CHANGE_REASON, NOTIFICATION_SENT
        FROM customer_tariff_rate_changes
        WHERE RATE_CHANGE_DATE >= '{self.test_date}' AND NOTIFICATION_SENT = 'N'
        """
        
        result = self.mock_database.query_records("customer_tariff_rate_changes", rate_changes_query)
        
        self.assertEqual(len(result), 2, "料金変更検出件数不正")
        self.assertEqual(result[0]["CPKIYK_ID"], "CPKIYK000001", "CPKIYK ID確認失敗")
        self.assertEqual(result[0]["CHANGE_REASON"], "REGULATORY_ADJUSTMENT", "変更理由確認失敗")
        self.assertEqual(result[0]["NOTIFICATION_SENT"], "N", "通知送信フラグ確認失敗")
    
    def test_lookup_activity_customer_tariff_profile(self):
        """Lookup Activity: 顧客料金プロファイル取得テスト"""
        # テストケース: 顧客の料金プロファイル取得
        mock_customer_profiles = [
            {
                "CUSTOMER_ID": "CUST123456",
                "TARIFF_CODE": "A123",
                "CONTRACT_TYPE": "RESIDENTIAL",
                "USAGE_TIER": "STANDARD",
                "MONTHLY_AVERAGE_USAGE": 35.5,
                "ANNUAL_BILLING_AMOUNT": 45000.0
            },
            {
                "CUSTOMER_ID": "CUST123457",
                "TARIFF_CODE": "B456",
                "CONTRACT_TYPE": "BUSINESS",
                "USAGE_TIER": "HIGH",
                "MONTHLY_AVERAGE_USAGE": 120.8,
                "ANNUAL_BILLING_AMOUNT": 185000.0
            }
        ]
        
        self.mock_database.query_records.return_value = mock_customer_profiles
        
        # Lookup Activity実行シミュレーション
        profile_query = """
        SELECT CUSTOMER_ID, TARIFF_CODE, CONTRACT_TYPE, USAGE_TIER,
               MONTHLY_AVERAGE_USAGE, ANNUAL_BILLING_AMOUNT
        FROM customer_tariff_profiles
        WHERE CUSTOMER_ID IN ('CUST123456', 'CUST123457')
        """
        
        result = self.mock_database.query_records("customer_tariff_profiles", profile_query)
        
        self.assertEqual(len(result), 2, "顧客料金プロファイル取得件数不正")
        self.assertEqual(result[0]["TARIFF_CODE"], "A123", "料金コード確認失敗")
        self.assertEqual(result[0]["CONTRACT_TYPE"], "RESIDENTIAL", "契約タイプ確認失敗")
    
    def test_data_flow_tariff_impact_calculation(self):
        """Data Flow: 料金影響計算テスト"""
        # テストケース: 料金変更の影響計算
        impact_scenarios = [
            {
                "OLD_TARIFF_RATE": 150.0,
                "NEW_TARIFF_RATE": 155.0,
                "MONTHLY_USAGE": 35.0,
                "EXPECTED_MONTHLY_IMPACT": 175.0,
                "EXPECTED_ANNUAL_IMPACT": 2100.0
            },
            {
                "OLD_TARIFF_RATE": 140.0,
                "NEW_TARIFF_RATE": 138.0,
                "MONTHLY_USAGE": 25.0,
                "EXPECTED_MONTHLY_IMPACT": -50.0,
                "EXPECTED_ANNUAL_IMPACT": -600.0
            }
        ]
        
        # 料金影響計算ロジック（Data Flow内の処理）
        def calculate_tariff_impact(old_rate, new_rate, monthly_usage):
            rate_difference = new_rate - old_rate
            monthly_impact = rate_difference * monthly_usage
            annual_impact = monthly_impact * 12
            
            return {
                "rate_difference": rate_difference,
                "monthly_impact": monthly_impact,
                "annual_impact": annual_impact,
                "percentage_change": (rate_difference / old_rate) * 100
            }
        
        # 各シナリオでの影響計算
        for scenario in impact_scenarios:
            result = calculate_tariff_impact(
                scenario["OLD_TARIFF_RATE"],
                scenario["NEW_TARIFF_RATE"],
                scenario["MONTHLY_USAGE"]
            )
            
            self.assertEqual(result["monthly_impact"], scenario["EXPECTED_MONTHLY_IMPACT"],
                           f"月間影響計算失敗: {scenario}")
            self.assertEqual(result["annual_impact"], scenario["EXPECTED_ANNUAL_IMPACT"],
                           f"年間影響計算失敗: {scenario}")
    
    def test_data_flow_cpkiyk_notification_message_generation(self):
        """Data Flow: Cpkiyk通知メッセージ生成テスト"""
        # テストケース: Cpkiyk通知メッセージの生成
        cpkiyk_data = {
            "CUSTOMER_NAME": "テストユーザー1",
            "TARIFF_CODE": "A123",
            "OLD_TARIFF_RATE": 150.0,
            "NEW_TARIFF_RATE": 155.0,
            "RATE_CHANGE_DATE": "20240301",
            "CHANGE_REASON": "REGULATORY_ADJUSTMENT",
            "ESTIMATED_IMPACT": 125.0,
            "CHANGE_TYPE": "INCREASE",
            "MONTHLY_USAGE": 35.0
        }
        
        # Cpkiyk通知メッセージ生成ロジック（Data Flow内の処理）
        def generate_cpkiyk_notification_message(cpkiyk_data):
            customer_name = cpkiyk_data["CUSTOMER_NAME"]
            tariff_code = cpkiyk_data["TARIFF_CODE"]
            old_rate = cpkiyk_data["OLD_TARIFF_RATE"]
            new_rate = cpkiyk_data["NEW_TARIFF_RATE"]
            change_date = cpkiyk_data["RATE_CHANGE_DATE"]
            change_reason = cpkiyk_data["CHANGE_REASON"]
            estimated_impact = cpkiyk_data["ESTIMATED_IMPACT"]
            change_type = cpkiyk_data["CHANGE_TYPE"]
            monthly_usage = cpkiyk_data["MONTHLY_USAGE"]
            
            # 変更日のフォーマット
            formatted_change_date = f"{change_date[:4]}年{change_date[4:6]}月{change_date[6:8]}日"
            
            # 変更理由の日本語化
            reason_translations = {
                "REGULATORY_ADJUSTMENT": "規制当局による調整",
                "COMPETITIVE_ADJUSTMENT": "競争力向上のための調整",
                "COST_INFLATION": "原価上昇による調整",
                "SYSTEM_UPGRADE": "システム改善による調整"
            }
            
            reason_text = reason_translations.get(change_reason, change_reason)
            
            # 変更タイプ別のメッセージ調整
            if change_type == "INCREASE":
                impact_msg = f"月額約{estimated_impact:,.0f}円の増加"
                trend_msg = "上昇"
            else:
                impact_msg = f"月額約{abs(estimated_impact):,.0f}円の減少"
                trend_msg = "下降"
            
            # 料金変更率の計算
            rate_change_percent = ((new_rate - old_rate) / old_rate) * 100
            
            notification_message = f"""
            {customer_name}様
            
            ガス料金単価変更のお知らせ
            
            {reason_text}により、ガス料金単価が変更されます。
            
            変更内容：
            - 料金コード：{tariff_code}
            - 変更前単価：{old_rate:,.2f}円/m³
            - 変更後単価：{new_rate:,.2f}円/m³
            - 変更率：{rate_change_percent:+.1f}%
            - 適用開始日：{formatted_change_date}
            
            お客様への影響（月間使用量{monthly_usage}m³の場合）：
            - {impact_msg}
            
            料金は{trend_msg}となりますが、引き続き安定したサービスを提供してまいります。
            
            ご不明な点がございましたら、お気軽にお問い合わせください。
            
            東京ガス株式会社
            """
            
            return notification_message.strip()
        
        # メッセージ生成実行
        message = generate_cpkiyk_notification_message(cpkiyk_data)
        
        # アサーション
        self.assertIn("テストユーザー1様", message, "顧客名挿入失敗")
        self.assertIn("ガス料金単価変更のお知らせ", message, "タイトル挿入失敗")
        self.assertIn("A123", message, "料金コード挿入失敗")
        self.assertIn("150.00円/m³", message, "変更前単価挿入失敗")
        self.assertIn("155.00円/m³", message, "変更後単価挿入失敗")
        self.assertIn("2024年03月01日", message, "変更日フォーマット失敗")
        self.assertIn("規制当局による調整", message, "変更理由挿入失敗")
        self.assertIn("125円の増加", message, "影響金額挿入失敗")
        self.assertIn("+3.3%", message, "変更率挿入失敗")
    
    def test_data_flow_tariff_change_categorization(self):
        """Data Flow: 料金変更分類処理テスト"""
        # テストケース: 料金変更の分類
        change_scenarios = [
            {
                "RATE_DIFFERENCE": 5.0,
                "RATE_CHANGE_PERCENT": 3.33,
                "CUSTOMER_TIER": "PREMIUM",
                "EXPECTED_CATEGORY": "MINOR_INCREASE",
                "EXPECTED_PRIORITY": "LOW"
            },
            {
                "RATE_DIFFERENCE": 15.0,
                "RATE_CHANGE_PERCENT": 10.0,
                "CUSTOMER_TIER": "STANDARD",
                "EXPECTED_CATEGORY": "MAJOR_INCREASE",
                "EXPECTED_PRIORITY": "HIGH"
            },
            {
                "RATE_DIFFERENCE": -2.0,
                "RATE_CHANGE_PERCENT": -1.43,
                "CUSTOMER_TIER": "BASIC",
                "EXPECTED_CATEGORY": "MINOR_DECREASE",
                "EXPECTED_PRIORITY": "LOW"
            }
        ]
        
        # 料金変更分類ロジック（Data Flow内の処理）
        def categorize_tariff_change(change_data):
            rate_difference = change_data["RATE_DIFFERENCE"]
            rate_change_percent = change_data["RATE_CHANGE_PERCENT"]
            customer_tier = change_data["CUSTOMER_TIER"]
            
            # 変更カテゴリの判定
            if rate_difference > 0:
                if rate_change_percent >= 5.0:
                    category = "MAJOR_INCREASE"
                else:
                    category = "MINOR_INCREASE"
            elif rate_difference < 0:
                if abs(rate_change_percent) >= 5.0:
                    category = "MAJOR_DECREASE"
                else:
                    category = "MINOR_DECREASE"
            else:
                category = "NO_CHANGE"
            
            # 優先度の判定
            if category in ["MAJOR_INCREASE", "MAJOR_DECREASE"]:
                priority = "HIGH"
            elif category in ["MINOR_INCREASE", "MINOR_DECREASE"] and customer_tier == "PREMIUM":
                priority = "MEDIUM"
            else:
                priority = "LOW"
            
            return {
                "category": category,
                "priority": priority,
                "notification_urgency": "IMMEDIATE" if priority == "HIGH" else "NORMAL"
            }
        
        # 各シナリオでの分類
        for scenario in change_scenarios:
            result = categorize_tariff_change(scenario)
            self.assertEqual(result["category"], scenario["EXPECTED_CATEGORY"],
                           f"料金変更分類失敗: {scenario}")
            self.assertEqual(result["priority"], scenario["EXPECTED_PRIORITY"],
                           f"優先度判定失敗: {scenario}")
    
    def test_data_flow_tariff_change_validation(self):
        """Data Flow: 料金変更データ検証テスト"""
        # テストケース: 料金変更データの検証
        test_changes = [
            {"CUSTOMER_ID": "CUST123456", "OLD_TARIFF_RATE": 150.0, "NEW_TARIFF_RATE": 155.0, "RATE_CHANGE_DATE": "20240301"},
            {"CUSTOMER_ID": "", "OLD_TARIFF_RATE": 140.0, "NEW_TARIFF_RATE": 138.0, "RATE_CHANGE_DATE": "20240301"},  # 不正: 空ID
            {"CUSTOMER_ID": "CUST123458", "OLD_TARIFF_RATE": -150.0, "NEW_TARIFF_RATE": 155.0, "RATE_CHANGE_DATE": "20240301"},  # 不正: 負の料金
            {"CUSTOMER_ID": "CUST123459", "OLD_TARIFF_RATE": 150.0, "NEW_TARIFF_RATE": 150.0, "RATE_CHANGE_DATE": "20240201"}  # 不正: 同じ料金、過去の日付
        ]
        
        # 料金変更データ検証ロジック（Data Flow内の処理）
        def validate_tariff_change_data(change):
            errors = []
            current_date = datetime.utcnow().strftime('%Y%m%d')
            
            # 顧客ID検証
            if not change.get("CUSTOMER_ID", "").strip():
                errors.append("顧客ID必須")
            
            # 料金検証
            old_rate = change.get("OLD_TARIFF_RATE", 0)
            new_rate = change.get("NEW_TARIFF_RATE", 0)
            
            if old_rate <= 0:
                errors.append("変更前料金は正の値である必要があります")
            
            if new_rate <= 0:
                errors.append("変更後料金は正の値である必要があります")
            
            if old_rate == new_rate:
                errors.append("変更前後で料金が同じです")
            
            # 変更日検証
            change_date = change.get("RATE_CHANGE_DATE", "")
            if change_date < current_date:
                errors.append("変更日は現在日付以降である必要があります")
            
            return errors
        
        # 検証実行
        validation_results = []
        for change in test_changes:
            errors = validate_tariff_change_data(change)
            validation_results.append({
                "change": change,
                "errors": errors,
                "is_valid": len(errors) == 0
            })
        
        # アサーション
        self.assertEqual(len(validation_results), 4, "検証結果数不正")
        self.assertTrue(validation_results[0]["is_valid"], "正常変更が不正判定")
        self.assertFalse(validation_results[1]["is_valid"], "不正変更（空ID）が正常判定")
        self.assertFalse(validation_results[2]["is_valid"], "不正変更（負の料金）が正常判定")
        self.assertFalse(validation_results[3]["is_valid"], "不正変更（同じ料金、過去の日付）が正常判定")
    
    def test_script_activity_cpkiyk_email_notification(self):
        """Script Activity: Cpkiykメール通知送信処理テスト"""
        # テストケース: Cpkiykメール通知の送信
        cpkiyk_email_data = {
            "to": "test@example.com",
            "subject": "【東京ガス】ガス料金単価変更のお知らせ",
            "body": "ガス料金単価が変更されます。",
            "cpkiyk_id": "CPKIYK000001",
            "priority": "normal"
        }
        
        self.mock_email_service.send_cpkiyk_notification.return_value = {
            "status": "sent",
            "message_id": "CPKIYK_EMAIL_123",
            "delivery_time": datetime.utcnow().isoformat(),
            "cpkiyk_id": "CPKIYK000001"
        }
        
        # Script Activity実行シミュレーション
        result = self.mock_email_service.send_cpkiyk_notification(
            cpkiyk_email_data["to"],
            cpkiyk_email_data["subject"],
            cpkiyk_email_data["body"],
            cpkiyk_id=cpkiyk_email_data["cpkiyk_id"],
            priority=cpkiyk_email_data["priority"]
        )
        
        self.assertEqual(result["status"], "sent", "Cpkiykメール通知送信失敗")
        self.assertIsNotNone(result["message_id"], "メッセージID取得失敗")
        self.assertEqual(result["cpkiyk_id"], "CPKIYK000001", "CPKIYK ID確認失敗")
        self.mock_email_service.send_cpkiyk_notification.assert_called_once()
    
    def test_copy_activity_cpkiyk_data_export(self):
        """Copy Activity: Cpkiykデータエクスポート処理テスト"""
        # テストケース: Cpkiykデータのエクスポート
        cpkiyk_export_data = self.sample_cpkiyk_data
        export_path = f"/Export/Cpkiyk/cpkiyk_changes_{self.test_date}.csv"
        
        # CSV生成（Copy Activity内の処理）
        def generate_cpkiyk_export_csv(cpkiyk_data):
            if not cpkiyk_data:
                return ""
            
            header = ",".join(cpkiyk_data[0].keys())
            rows = []
            
            for cpkiyk in cpkiyk_data:
                row = ",".join(str(cpkiyk[key]) for key in cpkiyk.keys())
                rows.append(row)
            
            return header + "\n" + "\n".join(rows)
        
        # エクスポート実行
        csv_content = generate_cpkiyk_export_csv(cpkiyk_export_data)
        
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
        
        self.assertEqual(result["status"], "success", "Cpkiykデータエクスポート失敗")
        self.assertEqual(result["file_path"], export_path, "エクスポートパス確認失敗")
        self.assertGreater(result["file_size"], 0, "エクスポートファイルサイズ確認失敗")
        self.mock_sftp_service.upload_file.assert_called_once()
    
    def test_copy_activity_cpkiyk_notification_log(self):
        """Copy Activity: Cpkiyk通知ログ記録テスト"""
        # テストケース: Cpkiyk通知ログの記録
        notification_logs = [
            {
                "CPKIYK_ID": "CPKIYK000001",
                "CUSTOMER_ID": "CUST123456",
                "NOTIFICATION_TYPE": "TARIFF_CHANGE",
                "SENT_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "SENT_TIME": datetime.utcnow().strftime('%H:%M:%S'),
                "DELIVERY_METHOD": "EMAIL",
                "STATUS": "SENT",
                "MESSAGE_ID": "CPKIYK_EMAIL_123"
            },
            {
                "CPKIYK_ID": "CPKIYK000002",
                "CUSTOMER_ID": "CUST123457",
                "NOTIFICATION_TYPE": "TARIFF_CHANGE",
                "SENT_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "SENT_TIME": datetime.utcnow().strftime('%H:%M:%S'),
                "DELIVERY_METHOD": "EMAIL",
                "STATUS": "SENT",
                "MESSAGE_ID": "CPKIYK_EMAIL_124"
            }
        ]
        
        self.mock_database.insert_records.return_value = True
        
        # Copy Activity実行シミュレーション
        notification_log_table = "cpkiyk_notification_logs"
        result = self.mock_database.insert_records(notification_log_table, notification_logs)
        
        self.assertTrue(result, "Cpkiyk通知ログ記録失敗")
        self.mock_database.insert_records.assert_called_once_with(notification_log_table, notification_logs)
    
    def test_script_activity_cpkiyk_analytics(self):
        """Script Activity: Cpkiyk分析処理テスト"""
        # テストケース: Cpkiyk分析データ生成
        cpkiyk_analytics = {
            "execution_date": self.test_date,
            "total_rate_changes": 85,
            "rate_increases": 60,
            "rate_decreases": 25,
            "major_changes": 20,
            "minor_changes": 65,
            "regulatory_adjustments": 50,
            "competitive_adjustments": 35,
            "average_rate_change_percent": 2.8,
            "total_customer_impact": 156000.0,
            "notifications_sent": 82,
            "notification_success_rate": 0.96,
            "processing_time_minutes": 5.2
        }
        
        self.mock_database.insert_records.return_value = True
        
        # Script Activity実行シミュレーション
        analytics_table = "cpkiyk_analytics"
        result = self.mock_database.insert_records(analytics_table, [cpkiyk_analytics])
        
        self.assertTrue(result, "Cpkiyk分析記録失敗")
        self.mock_database.insert_records.assert_called_once_with(analytics_table, [cpkiyk_analytics])
    
    def test_lookup_activity_cpkiyk_statistics(self):
        """Lookup Activity: Cpkiyk統計取得テスト"""
        # テストケース: Cpkiykの統計情報取得
        cpkiyk_statistics = [
            {
                "CHANGE_REASON": "REGULATORY_ADJUSTMENT",
                "CHANGE_COUNT": 50,
                "AVERAGE_RATE_CHANGE": 2.5,
                "TOTAL_CUSTOMER_IMPACT": 125000.0
            },
            {
                "CHANGE_REASON": "COMPETITIVE_ADJUSTMENT",
                "CHANGE_COUNT": 35,
                "AVERAGE_RATE_CHANGE": -1.8,
                "TOTAL_CUSTOMER_IMPACT": -63000.0
            }
        ]
        
        self.mock_database.query_records.return_value = cpkiyk_statistics
        
        # Lookup Activity実行シミュレーション
        statistics_query = f"""
        SELECT CHANGE_REASON, 
               COUNT(*) as CHANGE_COUNT,
               AVG((NEW_TARIFF_RATE - OLD_TARIFF_RATE) / OLD_TARIFF_RATE * 100) as AVERAGE_RATE_CHANGE,
               SUM(ESTIMATED_IMPACT) as TOTAL_CUSTOMER_IMPACT
        FROM customer_tariff_rate_changes
        WHERE RATE_CHANGE_DATE >= '{self.test_date}'
        GROUP BY CHANGE_REASON
        """
        
        result = self.mock_database.query_records("customer_tariff_rate_changes", statistics_query)
        
        self.assertEqual(len(result), 2, "Cpkiyk統計取得件数不正")
        self.assertEqual(result[0]["CHANGE_REASON"], "REGULATORY_ADJUSTMENT", "変更理由確認失敗")
        self.assertEqual(result[0]["CHANGE_COUNT"], 50, "変更件数確認失敗")
        self.assertEqual(result[0]["AVERAGE_RATE_CHANGE"], 2.5, "平均変更率確認失敗")
    
    def test_data_flow_batch_processing(self):
        """Data Flow: バッチ処理テスト"""
        # テストケース: 大量データのバッチ処理
        large_cpkiyk_dataset = []
        for i in range(500):
            large_cpkiyk_dataset.append({
                "CPKIYK_ID": f"CPKIYK{i:06d}",
                "CUSTOMER_ID": f"CUST{i:06d}",
                "OLD_TARIFF_RATE": 150.0 + (i % 10),
                "NEW_TARIFF_RATE": 155.0 + (i % 10),
                "CHANGE_REASON": ["REGULATORY_ADJUSTMENT", "COMPETITIVE_ADJUSTMENT"][i % 2],
                "CUSTOMER_TIER": ["BASIC", "STANDARD", "PREMIUM"][i % 3]
            })
        
        # バッチ処理ロジック（Data Flow内の処理）
        def process_cpkiyk_batch(cpkiyk_data_list, batch_size=50):
            processed_batches = []
            
            for i in range(0, len(cpkiyk_data_list), batch_size):
                batch = cpkiyk_data_list[i:i + batch_size]
                
                # バッチ処理の実行
                batch_result = {
                    "batch_id": f"BATCH_{i//batch_size + 1:03d}",
                    "cpkiyk_count": len(batch),
                    "regulatory_adjustments": sum(1 for cpkiyk in batch if cpkiyk["CHANGE_REASON"] == "REGULATORY_ADJUSTMENT"),
                    "competitive_adjustments": sum(1 for cpkiyk in batch if cpkiyk["CHANGE_REASON"] == "COMPETITIVE_ADJUSTMENT"),
                    "premium_customers": sum(1 for cpkiyk in batch if cpkiyk["CUSTOMER_TIER"] == "PREMIUM"),
                    "average_rate_change": sum((cpkiyk["NEW_TARIFF_RATE"] - cpkiyk["OLD_TARIFF_RATE"]) for cpkiyk in batch) / len(batch),
                    "processing_time": 1.2  # シミュレーション
                }
                
                processed_batches.append(batch_result)
            
            return processed_batches
        
        # バッチ処理実行
        batch_results = process_cpkiyk_batch(large_cpkiyk_dataset, batch_size=50)
        
        # アサーション
        self.assertEqual(len(batch_results), 10, "バッチ数不正")  # 500 / 50 = 10
        self.assertEqual(batch_results[0]["cpkiyk_count"], 50, "バッチサイズ不正")
        self.assertGreater(batch_results[0]["regulatory_adjustments"], 0, "規制調整数不正")
        self.assertGreater(batch_results[0]["competitive_adjustments"], 0, "競争調整数不正")
        
        # 全バッチの合計確認
        total_cpkiyk = sum(batch["cpkiyk_count"] for batch in batch_results)
        self.assertEqual(total_cpkiyk, 500, "全バッチ処理件数不正")
    
    def _create_cpkiyk_csv_content(self) -> str:
        """Cpkiykデータ用CSVコンテンツ生成"""
        header = "CPKIYK_ID,CUSTOMER_ID,CUSTOMER_NAME,EMAIL_ADDRESS,PHONE_NUMBER,TARIFF_CODE,OLD_TARIFF_RATE,NEW_TARIFF_RATE,RATE_CHANGE_DATE,CHANGE_REASON,ESTIMATED_IMPACT,CHANGE_TYPE,APPROVAL_STATUS"
        rows = []
        
        for item in self.sample_cpkiyk_data:
            row = f"{item['CPKIYK_ID']},{item['CUSTOMER_ID']},{item['CUSTOMER_NAME']},{item['EMAIL_ADDRESS']},{item['PHONE_NUMBER']},{item['TARIFF_CODE']},{item['OLD_TARIFF_RATE']},{item['NEW_TARIFF_RATE']},{item['RATE_CHANGE_DATE']},{item['CHANGE_REASON']},{item['ESTIMATED_IMPACT']},{item['CHANGE_TYPE']},{item['APPROVAL_STATUS']}"
            rows.append(row)
        
        return header + "\n" + "\n".join(rows)


if __name__ == "__main__":
    unittest.main()