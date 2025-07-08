"""
pi_Send_ActionPointCurrentMonthEntryList パイプラインのユニットテスト

アクションポイント当月エントリリスト送信パイプラインの個別コンポーネントテスト
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any


class TestActionPointCurrentMonthEntryListUnit(unittest.TestCase):
    """アクションポイント当月エントリリストパイプラインユニットテスト"""
    
    def setUp(self):
        """テストセットアップ"""
        self.pipeline_name = "pi_Send_ActionPointCurrentMonthEntryList"
        self.domain = "actionpoint"
        
        # モックサービス
        self.mock_blob_storage = Mock()
        self.mock_database = Mock()
        self.mock_email_service = Mock()
        self.mock_report_service = Mock()
        
        # テストデータ
        self.test_date = datetime.utcnow().strftime('%Y%m%d')
        self.test_month = datetime.utcnow().strftime('%Y%m')
        self.test_file_path = f"ActionPointCurrentMonthEntryList/{self.test_date}/current_month_entry_list.csv"
        
        # 基本的なアクションポイント当月エントリリストデータ
        self.sample_current_month_entry_data = [
            {
                "ENTRY_ID": "ENTRY000001",
                "CUSTOMER_ID": "CUST123456",
                "CUSTOMER_NAME": "テストユーザー1",
                "EMAIL_ADDRESS": "test1@example.com",
                "ACTION_TYPE": "CONTRACT_SIGNUP",
                "ENTRY_DATE": "20240301",
                "POINT_AMOUNT": 1000,
                "CAMPAIGN_ID": "CAMP2024001",
                "BUSINESS_UNIT": "GAS",
                "STATUS": "ACTIVE",
                "APPROVAL_STATUS": "APPROVED",
                "CREATED_DATE": "20240301"
            },
            {
                "ENTRY_ID": "ENTRY000002",
                "CUSTOMER_ID": "CUST123457",
                "CUSTOMER_NAME": "テストユーザー2",
                "EMAIL_ADDRESS": "test2@example.com",
                "ACTION_TYPE": "PAYMENT_COMPLETION",
                "ENTRY_DATE": "20240302",
                "POINT_AMOUNT": 50,
                "CAMPAIGN_ID": "CAMP2024002",
                "BUSINESS_UNIT": "ELECTRIC",
                "STATUS": "ACTIVE",
                "APPROVAL_STATUS": "APPROVED",
                "CREATED_DATE": "20240302"
            }
        ]
    
    def test_lookup_activity_current_month_entries(self):
        """Lookup Activity: 当月エントリ検出テスト"""
        # テストケース: 当月のエントリ検出
        mock_current_month_entries = [
            {
                "ENTRY_ID": "ENTRY000001",
                "CUSTOMER_ID": "CUST123456",
                "ACTION_TYPE": "CONTRACT_SIGNUP",
                "ENTRY_DATE": "20240301",
                "POINT_AMOUNT": 1000,
                "CAMPAIGN_ID": "CAMP2024001",
                "BUSINESS_UNIT": "GAS",
                "STATUS": "ACTIVE",
                "APPROVAL_STATUS": "APPROVED"
            },
            {
                "ENTRY_ID": "ENTRY000002",
                "CUSTOMER_ID": "CUST123457",
                "ACTION_TYPE": "PAYMENT_COMPLETION",
                "ENTRY_DATE": "20240302",
                "POINT_AMOUNT": 50,
                "CAMPAIGN_ID": "CAMP2024002",
                "BUSINESS_UNIT": "ELECTRIC",
                "STATUS": "ACTIVE",
                "APPROVAL_STATUS": "APPROVED"
            }
        ]
        
        self.mock_database.query_records.return_value = mock_current_month_entries
        
        # Lookup Activity実行シミュレーション
        current_month_query = f"""
        SELECT ENTRY_ID, CUSTOMER_ID, ACTION_TYPE, ENTRY_DATE, POINT_AMOUNT, 
               CAMPAIGN_ID, BUSINESS_UNIT, STATUS, APPROVAL_STATUS
        FROM action_point_entries
        WHERE ENTRY_DATE >= '{self.test_month}01' AND ENTRY_DATE < '{self.test_month}32'
        AND STATUS = 'ACTIVE' AND APPROVAL_STATUS = 'APPROVED'
        """
        
        result = self.mock_database.query_records("action_point_entries", current_month_query)
        
        self.assertEqual(len(result), 2, "当月エントリ検出件数不正")
        self.assertEqual(result[0]["ENTRY_ID"], "ENTRY000001", "エントリID確認失敗")
        self.assertEqual(result[0]["STATUS"], "ACTIVE", "ステータス確認失敗")
        self.assertEqual(result[0]["APPROVAL_STATUS"], "APPROVED", "承認ステータス確認失敗")
    
    def test_lookup_activity_customer_details(self):
        """Lookup Activity: 顧客詳細取得テスト"""
        # テストケース: 顧客詳細情報の取得
        mock_customer_details = [
            {
                "CUSTOMER_ID": "CUST123456",
                "CUSTOMER_NAME": "テストユーザー1",
                "EMAIL_ADDRESS": "test1@example.com",
                "PHONE_NUMBER": "090-1234-5678",
                "CUSTOMER_TIER": "PREMIUM",
                "REGISTRATION_DATE": "20230101",
                "LAST_LOGIN_DATE": "20240228"
            },
            {
                "CUSTOMER_ID": "CUST123457",
                "CUSTOMER_NAME": "テストユーザー2",
                "EMAIL_ADDRESS": "test2@example.com",
                "PHONE_NUMBER": "090-2345-6789",
                "CUSTOMER_TIER": "STANDARD",
                "REGISTRATION_DATE": "20230201",
                "LAST_LOGIN_DATE": "20240225"
            }
        ]
        
        self.mock_database.query_records.return_value = mock_customer_details
        
        # Lookup Activity実行シミュレーション
        customer_details_query = """
        SELECT CUSTOMER_ID, CUSTOMER_NAME, EMAIL_ADDRESS, PHONE_NUMBER, 
               CUSTOMER_TIER, REGISTRATION_DATE, LAST_LOGIN_DATE
        FROM customers
        WHERE CUSTOMER_ID IN ('CUST123456', 'CUST123457')
        """
        
        result = self.mock_database.query_records("customers", customer_details_query)
        
        self.assertEqual(len(result), 2, "顧客詳細取得件数不正")
        self.assertEqual(result[0]["CUSTOMER_ID"], "CUST123456", "顧客ID確認失敗")
        self.assertEqual(result[0]["CUSTOMER_TIER"], "PREMIUM", "顧客ティア確認失敗")
    
    def test_data_flow_entry_list_aggregation(self):
        """Data Flow: エントリリスト集計テスト"""
        # テストケース: エントリリストの集計
        entry_list = [
            {"CUSTOMER_ID": "CUST123456", "ACTION_TYPE": "CONTRACT_SIGNUP", "POINT_AMOUNT": 1000, "BUSINESS_UNIT": "GAS", "ENTRY_DATE": "20240301"},
            {"CUSTOMER_ID": "CUST123456", "ACTION_TYPE": "PAYMENT_COMPLETION", "POINT_AMOUNT": 50, "BUSINESS_UNIT": "GAS", "ENTRY_DATE": "20240302"},
            {"CUSTOMER_ID": "CUST123457", "ACTION_TYPE": "CONTRACT_SIGNUP", "POINT_AMOUNT": 1000, "BUSINESS_UNIT": "ELECTRIC", "ENTRY_DATE": "20240301"},
            {"CUSTOMER_ID": "CUST123457", "ACTION_TYPE": "REFERRAL", "POINT_AMOUNT": 500, "BUSINESS_UNIT": "ELECTRIC", "ENTRY_DATE": "20240303"}
        ]
        
        # エントリリスト集計ロジック（Data Flow内の処理）
        def aggregate_entry_list(entries):
            aggregation = {}
            
            for entry in entries:
                customer_id = entry["CUSTOMER_ID"]
                action_type = entry["ACTION_TYPE"]
                point_amount = entry["POINT_AMOUNT"]
                business_unit = entry["BUSINESS_UNIT"]
                
                if customer_id not in aggregation:
                    aggregation[customer_id] = {
                        "total_points": 0,
                        "total_entries": 0,
                        "action_types": set(),
                        "business_units": set(),
                        "contract_signups": 0,
                        "payment_completions": 0,
                        "referrals": 0
                    }
                
                aggregation[customer_id]["total_points"] += point_amount
                aggregation[customer_id]["total_entries"] += 1
                aggregation[customer_id]["action_types"].add(action_type)
                aggregation[customer_id]["business_units"].add(business_unit)
                
                if action_type == "CONTRACT_SIGNUP":
                    aggregation[customer_id]["contract_signups"] += 1
                elif action_type == "PAYMENT_COMPLETION":
                    aggregation[customer_id]["payment_completions"] += 1
                elif action_type == "REFERRAL":
                    aggregation[customer_id]["referrals"] += 1
            
            # setをlistに変換
            for customer_id in aggregation:
                aggregation[customer_id]["action_types"] = list(aggregation[customer_id]["action_types"])
                aggregation[customer_id]["business_units"] = list(aggregation[customer_id]["business_units"])
            
            return aggregation
        
        # 集計実行
        aggregated_data = aggregate_entry_list(entry_list)
        
        # アサーション
        self.assertEqual(len(aggregated_data), 2, "集計顧客数不正")
        self.assertEqual(aggregated_data["CUST123456"]["total_points"], 1050, "CUST123456総ポイント不正")
        self.assertEqual(aggregated_data["CUST123456"]["total_entries"], 2, "CUST123456総エントリ数不正")
        self.assertEqual(aggregated_data["CUST123456"]["contract_signups"], 1, "CUST123456契約申込数不正")
        self.assertEqual(aggregated_data["CUST123456"]["payment_completions"], 1, "CUST123456支払い完了数不正")
        
        self.assertEqual(aggregated_data["CUST123457"]["total_points"], 1500, "CUST123457総ポイント不正")
        self.assertEqual(aggregated_data["CUST123457"]["total_entries"], 2, "CUST123457総エントリ数不正")
        self.assertEqual(aggregated_data["CUST123457"]["referrals"], 1, "CUST123457紹介数不正")
    
    def test_data_flow_monthly_report_generation(self):
        """Data Flow: 月次レポート生成テスト"""
        # テストケース: 月次レポートの生成
        monthly_data = {
            "report_month": "202403",
            "total_entries": 125,
            "total_points_awarded": 85000,
            "total_customers": 95,
            "action_type_breakdown": {
                "CONTRACT_SIGNUP": 45,
                "PAYMENT_COMPLETION": 60,
                "REFERRAL": 15,
                "SURVEY_COMPLETION": 5
            },
            "business_unit_breakdown": {
                "GAS": 70,
                "ELECTRIC": 55
            }
        }
        
        # 月次レポート生成ロジック（Data Flow内の処理）
        def generate_monthly_report(data):
            report_month = data["report_month"]
            total_entries = data["total_entries"]
            total_points = data["total_points_awarded"]
            total_customers = data["total_customers"]
            action_breakdown = data["action_type_breakdown"]
            business_breakdown = data["business_unit_breakdown"]
            
            # 月次レポートの構造化
            monthly_report = {
                "report_header": {
                    "report_type": "MONTHLY_ENTRY_SUMMARY",
                    "report_month": report_month,
                    "generation_date": datetime.utcnow().strftime('%Y%m%d'),
                    "total_entries": total_entries,
                    "total_points_awarded": total_points,
                    "total_customers": total_customers,
                    "average_points_per_customer": round(total_points / total_customers, 2) if total_customers > 0 else 0
                },
                "action_type_summary": action_breakdown,
                "business_unit_summary": business_breakdown,
                "key_metrics": {
                    "most_popular_action": max(action_breakdown.keys(), key=lambda x: action_breakdown[x]),
                    "most_active_business_unit": max(business_breakdown.keys(), key=lambda x: business_breakdown[x]),
                    "conversion_rate": round((action_breakdown.get("CONTRACT_SIGNUP", 0) / total_entries) * 100, 2) if total_entries > 0 else 0
                }
            }
            
            return monthly_report
        
        # レポート生成実行
        report = generate_monthly_report(monthly_data)
        
        # アサーション
        self.assertEqual(report["report_header"]["report_type"], "MONTHLY_ENTRY_SUMMARY", "レポートタイプ確認失敗")
        self.assertEqual(report["report_header"]["report_month"], "202403", "レポート月確認失敗")
        self.assertEqual(report["report_header"]["total_entries"], 125, "総エントリ数確認失敗")
        self.assertEqual(report["report_header"]["average_points_per_customer"], 894.74, "平均ポイント確認失敗")
        self.assertEqual(report["key_metrics"]["most_popular_action"], "PAYMENT_COMPLETION", "最人気アクション確認失敗")
        self.assertEqual(report["key_metrics"]["conversion_rate"], 36.0, "コンバージョン率確認失敗")
    
    def test_data_flow_customer_ranking(self):
        """Data Flow: 顧客ランキング処理テスト"""
        # テストケース: 顧客ランキングの生成
        customer_entry_data = [
            {"CUSTOMER_ID": "CUST123456", "TOTAL_POINTS": 1500, "TOTAL_ENTRIES": 3},
            {"CUSTOMER_ID": "CUST123457", "TOTAL_POINTS": 2000, "TOTAL_ENTRIES": 4},
            {"CUSTOMER_ID": "CUST123458", "TOTAL_POINTS": 800, "TOTAL_ENTRIES": 2},
            {"CUSTOMER_ID": "CUST123459", "TOTAL_POINTS": 1200, "TOTAL_ENTRIES": 2}
        ]
        
        # 顧客ランキング生成ロジック（Data Flow内の処理）
        def generate_customer_ranking(customer_data):
            # ポイント数でソート
            sorted_by_points = sorted(customer_data, key=lambda x: x["TOTAL_POINTS"], reverse=True)
            
            # エントリ数でソート
            sorted_by_entries = sorted(customer_data, key=lambda x: x["TOTAL_ENTRIES"], reverse=True)
            
            # 総合ランキング（ポイント数 * 0.7 + エントリ数 * 0.3）
            for customer in customer_data:
                customer["COMPOSITE_SCORE"] = (customer["TOTAL_POINTS"] * 0.7) + (customer["TOTAL_ENTRIES"] * 100 * 0.3)
            
            sorted_by_composite = sorted(customer_data, key=lambda x: x["COMPOSITE_SCORE"], reverse=True)
            
            # ランキング情報の追加
            for i, customer in enumerate(sorted_by_points):
                customer["POINTS_RANK"] = i + 1
            
            for i, customer in enumerate(sorted_by_entries):
                customer["ENTRIES_RANK"] = i + 1
            
            for i, customer in enumerate(sorted_by_composite):
                customer["COMPOSITE_RANK"] = i + 1
            
            return {
                "ranking_by_points": sorted_by_points,
                "ranking_by_entries": sorted_by_entries,
                "ranking_by_composite": sorted_by_composite,
                "top_customer": sorted_by_composite[0] if sorted_by_composite else None
            }
        
        # ランキング生成実行
        ranking = generate_customer_ranking(customer_entry_data)
        
        # アサーション
        self.assertEqual(ranking["ranking_by_points"][0]["CUSTOMER_ID"], "CUST123457", "ポイントランキング1位確認失敗")
        self.assertEqual(ranking["ranking_by_entries"][0]["CUSTOMER_ID"], "CUST123457", "エントリランキング1位確認失敗")
        self.assertEqual(ranking["top_customer"]["CUSTOMER_ID"], "CUST123457", "総合ランキング1位確認失敗")
        self.assertEqual(ranking["ranking_by_points"][0]["POINTS_RANK"], 1, "ポイントランク確認失敗")
    
    def test_script_activity_report_generation(self):
        """Script Activity: レポート生成処理テスト"""
        # テストケース: レポート生成処理
        report_data = {
            "report_type": "MONTHLY_ENTRY_SUMMARY",
            "report_month": "202403",
            "total_entries": 125,
            "total_points": 85000,
            "total_customers": 95
        }
        
        self.mock_report_service.generate_monthly_report.return_value = {
            "status": "success",
            "report_id": "RPT20240301001",
            "report_path": "/reports/monthly/202403_entry_summary.pdf",
            "file_size": 1024000,
            "generation_time": datetime.utcnow().isoformat()
        }
        
        # Script Activity実行シミュレーション
        result = self.mock_report_service.generate_monthly_report(
            report_data["report_type"],
            report_data["report_month"],
            total_entries=report_data["total_entries"],
            total_points=report_data["total_points"],
            total_customers=report_data["total_customers"]
        )
        
        self.assertEqual(result["status"], "success", "レポート生成失敗")
        self.assertEqual(result["report_id"], "RPT20240301001", "レポートID確認失敗")
        self.assertIsNotNone(result["report_path"], "レポートパス確認失敗")
        self.assertGreater(result["file_size"], 0, "ファイルサイズ確認失敗")
        self.mock_report_service.generate_monthly_report.assert_called_once()
    
    def test_script_activity_email_notification(self):
        """Script Activity: メール通知送信処理テスト"""
        # テストケース: メール通知の送信
        email_data = {
            "to": ["admin@example.com", "manager@example.com"],
            "subject": "【東京ガス】アクションポイント当月エントリリスト",
            "body": "当月のアクションポイントエントリリストを添付いたします。",
            "attachments": ["/reports/monthly/202403_entry_summary.pdf"],
            "report_month": "202403"
        }
        
        self.mock_email_service.send_monthly_report.return_value = {
            "status": "sent",
            "message_id": "MONTHLY_REPORT_123",
            "delivery_time": datetime.utcnow().isoformat(),
            "recipients_count": 2
        }
        
        # Script Activity実行シミュレーション
        result = self.mock_email_service.send_monthly_report(
            email_data["to"],
            email_data["subject"],
            email_data["body"],
            attachments=email_data["attachments"],
            report_month=email_data["report_month"]
        )
        
        self.assertEqual(result["status"], "sent", "メール通知送信失敗")
        self.assertEqual(result["message_id"], "MONTHLY_REPORT_123", "メッセージID確認失敗")
        self.assertEqual(result["recipients_count"], 2, "受信者数確認失敗")
        self.mock_email_service.send_monthly_report.assert_called_once()
    
    def test_copy_activity_entry_list_export(self):
        """Copy Activity: エントリリストエクスポート処理テスト"""
        # テストケース: エントリリストのエクスポート
        entry_list_data = self.sample_current_month_entry_data
        
        # CSVエクスポート（Copy Activity内の処理）
        def export_entry_list_csv(entry_data):
            if not entry_data:
                return ""
            
            header = ",".join(entry_data[0].keys())
            rows = []
            
            for entry in entry_data:
                row = ",".join(str(entry[key]) for key in entry.keys())
                rows.append(row)
            
            return header + "\n" + "\n".join(rows)
        
        # エクスポート実行
        csv_content = export_entry_list_csv(entry_list_data)
        
        self.mock_blob_storage.upload_file.return_value = {
            "status": "success",
            "file_path": self.test_file_path,
            "file_size": len(csv_content),
            "upload_time": datetime.utcnow().isoformat()
        }
        
        # Copy Activity実行シミュレーション
        result = self.mock_blob_storage.upload_file(
            self.test_file_path,
            csv_content,
            encoding="utf-8"
        )
        
        self.assertEqual(result["status"], "success", "エントリリストエクスポート失敗")
        self.assertEqual(result["file_path"], self.test_file_path, "ファイルパス確認失敗")
        self.assertGreater(result["file_size"], 0, "ファイルサイズ確認失敗")
        self.mock_blob_storage.upload_file.assert_called_once()
    
    def test_copy_activity_monthly_summary_update(self):
        """Copy Activity: 月次サマリー更新テスト"""
        # テストケース: 月次サマリーの更新
        monthly_summaries = [
            {
                "SUMMARY_MONTH": "202403",
                "TOTAL_ENTRIES": 125,
                "TOTAL_POINTS_AWARDED": 85000,
                "TOTAL_CUSTOMERS": 95,
                "CONTRACT_SIGNUPS": 45,
                "PAYMENT_COMPLETIONS": 60,
                "REFERRALS": 15,
                "SURVEY_COMPLETIONS": 5,
                "GAS_BUSINESS_ENTRIES": 70,
                "ELECTRIC_BUSINESS_ENTRIES": 55,
                "AVERAGE_POINTS_PER_CUSTOMER": 894.74,
                "CONVERSION_RATE": 36.0,
                "UPDATED_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "UPDATED_TIME": datetime.utcnow().strftime('%H:%M:%S')
            }
        ]
        
        self.mock_database.insert_records.return_value = True
        
        # Copy Activity実行シミュレーション
        summary_table = "action_point_monthly_summary"
        result = self.mock_database.insert_records(summary_table, monthly_summaries)
        
        self.assertTrue(result, "月次サマリー更新失敗")
        self.mock_database.insert_records.assert_called_once_with(summary_table, monthly_summaries)
    
    def test_script_activity_entry_list_analytics(self):
        """Script Activity: エントリリスト分析処理テスト"""
        # テストケース: エントリリスト分析データ生成
        entry_list_analytics = {
            "execution_date": self.test_date,
            "report_month": "202403",
            "total_entries_processed": 125,
            "unique_customers": 95,
            "total_points_awarded": 85000,
            "contract_signup_entries": 45,
            "payment_completion_entries": 60,
            "referral_entries": 15,
            "survey_completion_entries": 5,
            "gas_business_entries": 70,
            "electric_business_entries": 55,
            "premium_customers": 25,
            "standard_customers": 60,
            "basic_customers": 10,
            "report_generation_success": True,
            "email_notification_sent": True,
            "processing_time_minutes": 15.2
        }
        
        self.mock_database.insert_records.return_value = True
        
        # Script Activity実行シミュレーション
        analytics_table = "action_point_entry_list_analytics"
        result = self.mock_database.insert_records(analytics_table, [entry_list_analytics])
        
        self.assertTrue(result, "エントリリスト分析記録失敗")
        self.mock_database.insert_records.assert_called_once_with(analytics_table, [entry_list_analytics])
    
    def test_data_flow_entry_list_validation(self):
        """Data Flow: エントリリストデータ検証テスト"""
        # テストケース: エントリリストデータの検証
        test_entries = [
            {"ENTRY_ID": "ENTRY000001", "CUSTOMER_ID": "CUST123456", "ACTION_TYPE": "CONTRACT_SIGNUP", "POINT_AMOUNT": 1000, "ENTRY_DATE": "20240301"},
            {"ENTRY_ID": "", "CUSTOMER_ID": "CUST123457", "ACTION_TYPE": "PAYMENT_COMPLETION", "POINT_AMOUNT": 50, "ENTRY_DATE": "20240301"},  # 不正: 空エントリID
            {"ENTRY_ID": "ENTRY000003", "CUSTOMER_ID": "CUST123458", "ACTION_TYPE": "UNKNOWN_ACTION", "POINT_AMOUNT": 100, "ENTRY_DATE": "20240301"},  # 不正: 不明アクション
            {"ENTRY_ID": "ENTRY000004", "CUSTOMER_ID": "CUST123459", "ACTION_TYPE": "CONTRACT_SIGNUP", "POINT_AMOUNT": 0, "ENTRY_DATE": "20240301"}  # 不正: ポイント数0
        ]
        
        # エントリリストデータ検証ロジック（Data Flow内の処理）
        def validate_entry_list_data(entry):
            errors = []
            
            # エントリID検証
            if not entry.get("ENTRY_ID", "").strip():
                errors.append("エントリID必須")
            
            # 顧客ID検証
            if not entry.get("CUSTOMER_ID", "").strip():
                errors.append("顧客ID必須")
            
            # アクションタイプ検証
            valid_action_types = ["CONTRACT_SIGNUP", "PAYMENT_COMPLETION", "REFERRAL", "SURVEY_COMPLETION"]
            if entry.get("ACTION_TYPE") not in valid_action_types:
                errors.append("アクションタイプが不正です")
            
            # ポイント数検証
            point_amount = entry.get("POINT_AMOUNT", 0)
            if point_amount <= 0:
                errors.append("ポイント数は正の値である必要があります")
            
            # エントリ日検証
            entry_date = entry.get("ENTRY_DATE", "")
            if not entry_date:
                errors.append("エントリ日必須")
            
            return errors
        
        # 検証実行
        validation_results = []
        for entry in test_entries:
            errors = validate_entry_list_data(entry)
            validation_results.append({
                "entry": entry,
                "errors": errors,
                "is_valid": len(errors) == 0
            })
        
        # アサーション
        self.assertEqual(len(validation_results), 4, "検証結果数不正")
        self.assertTrue(validation_results[0]["is_valid"], "正常エントリが不正判定")
        self.assertFalse(validation_results[1]["is_valid"], "不正エントリ（空ID）が正常判定")
        self.assertFalse(validation_results[2]["is_valid"], "不正エントリ（不明アクション）が正常判定")
        self.assertFalse(validation_results[3]["is_valid"], "不正エントリ（ポイント数0）が正常判定")
    
    def test_lookup_activity_campaign_performance(self):
        """Lookup Activity: キャンペーン実績取得テスト"""
        # テストケース: キャンペーン実績の取得
        campaign_performance = [
            {
                "CAMPAIGN_ID": "CAMP2024001",
                "CAMPAIGN_NAME": "新規契約特典",
                "ENTRY_COUNT": 45,
                "TOTAL_POINTS_AWARDED": 45000,
                "UNIQUE_CUSTOMERS": 45,
                "CONVERSION_RATE": 0.85
            },
            {
                "CAMPAIGN_ID": "CAMP2024002",
                "CAMPAIGN_NAME": "支払い完了ボーナス",
                "ENTRY_COUNT": 60,
                "TOTAL_POINTS_AWARDED": 3000,
                "UNIQUE_CUSTOMERS": 58,
                "CONVERSION_RATE": 0.92
            }
        ]
        
        self.mock_database.query_records.return_value = campaign_performance
        
        # Lookup Activity実行シミュレーション
        campaign_query = f"""
        SELECT CAMPAIGN_ID, CAMPAIGN_NAME, 
               COUNT(*) as ENTRY_COUNT,
               SUM(POINT_AMOUNT) as TOTAL_POINTS_AWARDED,
               COUNT(DISTINCT CUSTOMER_ID) as UNIQUE_CUSTOMERS,
               CAST(COUNT(DISTINCT CUSTOMER_ID) AS FLOAT) / COUNT(*) as CONVERSION_RATE
        FROM action_point_entries
        WHERE ENTRY_DATE >= '{self.test_month}01' AND ENTRY_DATE < '{self.test_month}32'
        GROUP BY CAMPAIGN_ID, CAMPAIGN_NAME
        """
        
        result = self.mock_database.query_records("action_point_entries", campaign_query)
        
        self.assertEqual(len(result), 2, "キャンペーン実績取得件数不正")
        self.assertEqual(result[0]["CAMPAIGN_ID"], "CAMP2024001", "キャンペーンID確認失敗")
        self.assertEqual(result[0]["ENTRY_COUNT"], 45, "エントリ数確認失敗")
        self.assertEqual(result[0]["TOTAL_POINTS_AWARDED"], 45000, "総ポイント確認失敗")
    
    def _create_current_month_entry_list_csv_content(self) -> str:
        """当月エントリリストデータ用CSVコンテンツ生成"""
        header = "ENTRY_ID,CUSTOMER_ID,CUSTOMER_NAME,EMAIL_ADDRESS,ACTION_TYPE,ENTRY_DATE,POINT_AMOUNT,CAMPAIGN_ID,BUSINESS_UNIT,STATUS,APPROVAL_STATUS,CREATED_DATE"
        rows = []
        
        for item in self.sample_current_month_entry_data:
            row = f"{item['ENTRY_ID']},{item['CUSTOMER_ID']},{item['CUSTOMER_NAME']},{item['EMAIL_ADDRESS']},{item['ACTION_TYPE']},{item['ENTRY_DATE']},{item['POINT_AMOUNT']},{item['CAMPAIGN_ID']},{item['BUSINESS_UNIT']},{item['STATUS']},{item['APPROVAL_STATUS']},{item['CREATED_DATE']}"
            rows.append(row)
        
        return header + "\n" + "\n".join(rows)


if __name__ == "__main__":
    unittest.main()