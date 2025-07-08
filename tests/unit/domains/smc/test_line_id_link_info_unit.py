"""
pi_Send_LINEIDLinkInfo パイプラインのユニットテスト

LINE ID連携情報送信パイプラインの個別コンポーネントテスト
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any


class TestLINEIDLinkInfoUnit(unittest.TestCase):
    """LINE ID連携情報パイプラインユニットテスト"""
    
    def setUp(self):
        """テストセットアップ"""
        self.pipeline_name = "pi_Send_LINEIDLinkInfo"
        self.domain = "smc"
        
        # モックサービス
        self.mock_blob_storage = Mock()
        self.mock_database = Mock()
        self.mock_line_api = Mock()
        self.mock_email_service = Mock()
        
        # テストデータ
        self.test_date = datetime.utcnow().strftime('%Y%m%d')
        self.test_file_path = f"LINEIDLinkInfo/{self.test_date}/line_id_link_info.csv"
        
        # 基本的なLINE ID連携情報データ
        self.sample_line_link_data = [
            {
                "LINK_ID": "LINK000001",
                "CUSTOMER_ID": "CUST123456",
                "CUSTOMER_NAME": "テストユーザー1",
                "EMAIL_ADDRESS": "test1@example.com",
                "PHONE_NUMBER": "090-1234-5678",
                "LINE_USER_ID": "U1234567890abcdef",
                "LINK_STATUS": "ACTIVE",
                "LINK_DATE": "20240201",
                "LINK_TYPE": "MANUAL",
                "VERIFICATION_STATUS": "VERIFIED",
                "NOTIFICATION_PREFERENCE": "LINE"
            },
            {
                "LINK_ID": "LINK000002",
                "CUSTOMER_ID": "CUST123457",
                "CUSTOMER_NAME": "テストユーザー2",
                "EMAIL_ADDRESS": "test2@example.com",
                "PHONE_NUMBER": "090-2345-6789",
                "LINE_USER_ID": "U2345678901bcdefg",
                "LINK_STATUS": "PENDING",
                "LINK_DATE": "20240202",
                "LINK_TYPE": "AUTO",
                "VERIFICATION_STATUS": "PENDING",
                "NOTIFICATION_PREFERENCE": "EMAIL"
            }
        ]
    
    def test_lookup_activity_line_link_status_detection(self):
        """Lookup Activity: LINE連携ステータス検出テスト"""
        # テストケース: LINE連携ステータスの検出
        mock_line_links = [
            {
                "LINK_ID": "LINK000001",
                "CUSTOMER_ID": "CUST123456",
                "LINE_USER_ID": "U1234567890abcdef",
                "LINK_STATUS": "ACTIVE",
                "VERIFICATION_STATUS": "VERIFIED",
                "NOTIFICATION_SENT": "N"
            },
            {
                "LINK_ID": "LINK000002",
                "CUSTOMER_ID": "CUST123457",
                "LINE_USER_ID": "U2345678901bcdefg",
                "LINK_STATUS": "PENDING",
                "VERIFICATION_STATUS": "PENDING",
                "NOTIFICATION_SENT": "N"
            }
        ]
        
        self.mock_database.query_records.return_value = mock_line_links
        
        # Lookup Activity実行シミュレーション
        link_query = f"""
        SELECT LINK_ID, CUSTOMER_ID, LINE_USER_ID, LINK_STATUS, 
               VERIFICATION_STATUS, NOTIFICATION_SENT
        FROM line_id_links
        WHERE LINK_DATE >= '{self.test_date}' AND NOTIFICATION_SENT = 'N'
        """
        
        result = self.mock_database.query_records("line_id_links", link_query)
        
        self.assertEqual(len(result), 2, "LINE連携ステータス検出件数不正")
        self.assertEqual(result[0]["LINK_ID"], "LINK000001", "連携ID確認失敗")
        self.assertEqual(result[0]["LINK_STATUS"], "ACTIVE", "連携ステータス確認失敗")
        self.assertEqual(result[0]["NOTIFICATION_SENT"], "N", "通知送信フラグ確認失敗")
    
    def test_lookup_activity_customer_line_preferences(self):
        """Lookup Activity: 顧客LINE設定取得テスト"""
        # テストケース: 顧客のLINE設定取得
        mock_line_preferences = [
            {
                "CUSTOMER_ID": "CUST123456",
                "LINE_NOTIFICATIONS": "Y",
                "LINE_RICH_MESSAGES": "Y",
                "LINE_PROMOTION": "N",
                "PREFERRED_LANGUAGE": "JA"
            },
            {
                "CUSTOMER_ID": "CUST123457",
                "LINE_NOTIFICATIONS": "Y",
                "LINE_RICH_MESSAGES": "N",
                "LINE_PROMOTION": "Y",
                "PREFERRED_LANGUAGE": "JA"
            }
        ]
        
        self.mock_database.query_records.return_value = mock_line_preferences
        
        # Lookup Activity実行シミュレーション
        preferences_query = """
        SELECT CUSTOMER_ID, LINE_NOTIFICATIONS, LINE_RICH_MESSAGES, 
               LINE_PROMOTION, PREFERRED_LANGUAGE
        FROM customer_line_preferences
        WHERE CUSTOMER_ID IN ('CUST123456', 'CUST123457')
        """
        
        result = self.mock_database.query_records("customer_line_preferences", preferences_query)
        
        self.assertEqual(len(result), 2, "顧客LINE設定取得件数不正")
        self.assertEqual(result[0]["LINE_NOTIFICATIONS"], "Y", "LINE通知設定確認失敗")
        self.assertEqual(result[0]["PREFERRED_LANGUAGE"], "JA", "優先言語確認失敗")
    
    def test_data_flow_line_message_generation(self):
        """Data Flow: LINEメッセージ生成テスト"""
        # テストケース: LINEメッセージの生成
        link_data = {
            "CUSTOMER_NAME": "テストユーザー1",
            "LINK_STATUS": "ACTIVE",
            "LINK_DATE": "20240201",
            "LINK_TYPE": "MANUAL",
            "VERIFICATION_STATUS": "VERIFIED",
            "SERVICES_AVAILABLE": ["請求書確認", "使用量確認", "お問い合わせ"]
        }
        
        # LINEメッセージ生成ロジック（Data Flow内の処理）
        def generate_line_message(link_data):
            customer_name = link_data["CUSTOMER_NAME"]
            link_status = link_data["LINK_STATUS"]
            link_date = link_data["LINK_DATE"]
            link_type = link_data["LINK_TYPE"]
            verification_status = link_data["VERIFICATION_STATUS"]
            services = link_data["SERVICES_AVAILABLE"]
            
            # 連携日のフォーマット
            formatted_link_date = f"{link_date[:4]}年{link_date[4:6]}月{link_date[6:8]}日"
            
            # 連携ステータス別メッセージ
            if link_status == "ACTIVE":
                status_msg = "LINE連携が完了しました"
                status_emoji = "✅"
            elif link_status == "PENDING":
                status_msg = "LINE連携の確認中です"
                status_emoji = "⏳"
            else:
                status_msg = "LINE連携でエラーが発生しました"
                status_emoji = "❌"
            
            # 連携タイプ別メッセージ
            if link_type == "MANUAL":
                type_msg = "手動連携"
            else:
                type_msg = "自動連携"
            
            # 利用可能サービス
            services_text = "\n".join([f"• {service}" for service in services])
            
            line_message = f"""
            {status_emoji} {customer_name}様
            
            {status_msg}
            
            連携情報：
            📅 連携日：{formatted_link_date}
            🔗 連携方法：{type_msg}
            ✅ 認証状況：{verification_status}
            
            ご利用いただけるサービス：
            {services_text}
            
            今後ともよろしくお願いいたします。
            
            東京ガス株式会社
            """
            
            return line_message.strip()
        
        # メッセージ生成実行
        message = generate_line_message(link_data)
        
        # アサーション
        self.assertIn("テストユーザー1様", message, "顧客名挿入失敗")
        self.assertIn("LINE連携が完了しました", message, "連携ステータス反映失敗")
        self.assertIn("2024年02月01日", message, "連携日フォーマット失敗")
        self.assertIn("手動連携", message, "連携タイプ挿入失敗")
        self.assertIn("請求書確認", message, "利用可能サービス挿入失敗")
        self.assertIn("✅", message, "ステータス絵文字挿入失敗")
    
    def test_data_flow_rich_message_content_generation(self):
        """Data Flow: リッチメッセージ内容生成テスト"""
        # テストケース: LINEリッチメッセージの生成
        rich_message_data = {
            "CUSTOMER_NAME": "テストユーザー1",
            "CUSTOMER_ID": "CUST123456",
            "CURRENT_BILL_AMOUNT": 8500.0,
            "USAGE_AMOUNT": 45.2,
            "LINK_STATUS": "ACTIVE",
            "QUICK_ACTIONS": ["請求書確認", "使用量確認", "お問い合わせ"]
        }
        
        # リッチメッセージ生成ロジック（Data Flow内の処理）
        def generate_rich_message_content(rich_data):
            customer_name = rich_data["CUSTOMER_NAME"]
            customer_id = rich_data["CUSTOMER_ID"]
            bill_amount = rich_data["CURRENT_BILL_AMOUNT"]
            usage_amount = rich_data["USAGE_AMOUNT"]
            link_status = rich_data["LINK_STATUS"]
            quick_actions = rich_data["QUICK_ACTIONS"]
            
            # リッチメッセージ構造
            rich_message = {
                "type": "bubble",
                "body": {
                    "type": "box",
                    "layout": "vertical",
                    "contents": [
                        {
                            "type": "text",
                            "text": f"🏠 {customer_name}様",
                            "weight": "bold",
                            "size": "xl"
                        },
                        {
                            "type": "text",
                            "text": "LINE連携情報",
                            "size": "md",
                            "color": "#666666"
                        },
                        {
                            "type": "separator",
                            "margin": "md"
                        },
                        {
                            "type": "box",
                            "layout": "vertical",
                            "margin": "md",
                            "contents": [
                                {
                                    "type": "box",
                                    "layout": "baseline",
                                    "contents": [
                                        {
                                            "type": "text",
                                            "text": "今月の請求額",
                                            "size": "sm",
                                            "color": "#AAAAAA",
                                            "flex": 1
                                        },
                                        {
                                            "type": "text",
                                            "text": f"¥{bill_amount:,.0f}",
                                            "size": "sm",
                                            "color": "#666666",
                                            "align": "end"
                                        }
                                    ]
                                },
                                {
                                    "type": "box",
                                    "layout": "baseline",
                                    "contents": [
                                        {
                                            "type": "text",
                                            "text": "ガス使用量",
                                            "size": "sm",
                                            "color": "#AAAAAA",
                                            "flex": 1
                                        },
                                        {
                                            "type": "text",
                                            "text": f"{usage_amount}m³",
                                            "size": "sm",
                                            "color": "#666666",
                                            "align": "end"
                                        }
                                    ]
                                },
                                {
                                    "type": "box",
                                    "layout": "baseline",
                                    "contents": [
                                        {
                                            "type": "text",
                                            "text": "連携状況",
                                            "size": "sm",
                                            "color": "#AAAAAA",
                                            "flex": 1
                                        },
                                        {
                                            "type": "text",
                                            "text": "✅ 連携済み" if link_status == "ACTIVE" else "⏳ 連携中",
                                            "size": "sm",
                                            "color": "#666666",
                                            "align": "end"
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                },
                "footer": {
                    "type": "box",
                    "layout": "vertical",
                    "spacing": "sm",
                    "contents": []
                }
            }
            
            # クイックアクションボタン追加
            for action in quick_actions:
                button = {
                    "type": "button",
                    "style": "link",
                    "height": "sm",
                    "action": {
                        "type": "message",
                        "label": action,
                        "text": action
                    }
                }
                rich_message["footer"]["contents"].append(button)
            
            return rich_message
        
        # リッチメッセージ生成実行
        rich_content = generate_rich_message_content(rich_message_data)
        
        # アサーション
        self.assertEqual(rich_content["type"], "bubble", "リッチメッセージタイプ確認失敗")
        self.assertIn("テストユーザー1様", rich_content["body"]["contents"][0]["text"], "顧客名挿入失敗")
        self.assertEqual(len(rich_content["footer"]["contents"]), 3, "クイックアクションボタン数不正")
        self.assertIn("¥8,500", str(rich_content), "請求額フォーマット失敗")
        self.assertIn("45.2m³", str(rich_content), "使用量フォーマット失敗")
    
    def test_data_flow_link_verification_process(self):
        """Data Flow: 連携認証処理テスト"""
        # テストケース: LINE連携の認証処理
        verification_scenarios = [
            {
                "CUSTOMER_ID": "CUST123456",
                "LINE_USER_ID": "U1234567890abcdef",
                "VERIFICATION_CODE": "123456",
                "CUSTOMER_PHONE": "090-1234-5678",
                "EXPECTED_STATUS": "VERIFIED"
            },
            {
                "CUSTOMER_ID": "CUST123457",
                "LINE_USER_ID": "U2345678901bcdefg",
                "VERIFICATION_CODE": "654321",
                "CUSTOMER_PHONE": "090-2345-6789",
                "EXPECTED_STATUS": "VERIFIED"
            },
            {
                "CUSTOMER_ID": "CUST123458",
                "LINE_USER_ID": "U3456789012cdefgh",
                "VERIFICATION_CODE": "INVALID",
                "CUSTOMER_PHONE": "090-3456-7890",
                "EXPECTED_STATUS": "FAILED"
            }
        ]
        
        # 認証処理ロジック（Data Flow内の処理）
        def verify_line_link(verification_data):
            customer_id = verification_data["CUSTOMER_ID"]
            line_user_id = verification_data["LINE_USER_ID"]
            verification_code = verification_data["VERIFICATION_CODE"]
            customer_phone = verification_data["CUSTOMER_PHONE"]
            
            # 認証コード検証
            if verification_code == "INVALID":
                return "FAILED"
            
            # 顧客ID形式確認
            if not customer_id.startswith("CUST"):
                return "FAILED"
            
            # LINE User ID形式確認
            if not line_user_id.startswith("U"):
                return "FAILED"
            
            # 電話番号形式確認
            if not customer_phone.startswith("090-"):
                return "FAILED"
            
            # 認証コード長さ確認
            if len(verification_code) != 6:
                return "FAILED"
            
            return "VERIFIED"
        
        # 各シナリオでの認証処理
        for scenario in verification_scenarios:
            verification_status = verify_line_link(scenario)
            self.assertEqual(verification_status, scenario["EXPECTED_STATUS"], 
                           f"連携認証処理失敗: {scenario}")
    
    def test_script_activity_line_message_sending(self):
        """Script Activity: LINEメッセージ送信処理テスト"""
        # テストケース: LINEメッセージの送信
        line_message_data = {
            "line_user_id": "U1234567890abcdef",
            "message_type": "text",
            "message_content": "LINE連携が完了しました。",
            "link_id": "LINK000001"
        }
        
        self.mock_line_api.send_message.return_value = {
            "status": "success",
            "message_id": "LINE_MSG_123",
            "delivery_time": datetime.utcnow().isoformat(),
            "quota_consumed": 1
        }
        
        # Script Activity実行シミュレーション
        result = self.mock_line_api.send_message(
            line_message_data["line_user_id"],
            line_message_data["message_content"],
            message_type=line_message_data["message_type"],
            link_id=line_message_data["link_id"]
        )
        
        self.assertEqual(result["status"], "success", "LINEメッセージ送信失敗")
        self.assertIsNotNone(result["message_id"], "メッセージID取得失敗")
        self.assertEqual(result["quota_consumed"], 1, "配信クォータ消費確認失敗")
        self.mock_line_api.send_message.assert_called_once()
    
    def test_script_activity_rich_message_sending(self):
        """Script Activity: リッチメッセージ送信処理テスト"""
        # テストケース: LINEリッチメッセージの送信
        rich_message_data = {
            "line_user_id": "U1234567890abcdef",
            "message_type": "flex",
            "flex_message": {
                "type": "bubble",
                "body": {"type": "box", "layout": "vertical", "contents": []}
            },
            "link_id": "LINK000001"
        }
        
        self.mock_line_api.send_flex_message.return_value = {
            "status": "success",
            "message_id": "LINE_FLEX_456",
            "delivery_time": datetime.utcnow().isoformat(),
            "quota_consumed": 1
        }
        
        # Script Activity実行シミュレーション
        result = self.mock_line_api.send_flex_message(
            rich_message_data["line_user_id"],
            rich_message_data["flex_message"],
            link_id=rich_message_data["link_id"]
        )
        
        self.assertEqual(result["status"], "success", "リッチメッセージ送信失敗")
        self.assertIsNotNone(result["message_id"], "リッチメッセージID取得失敗")
        self.assertEqual(result["quota_consumed"], 1, "配信クォータ消費確認失敗")
        self.mock_line_api.send_flex_message.assert_called_once()
    
    def test_copy_activity_line_message_log(self):
        """Copy Activity: LINEメッセージログ記録テスト"""
        # テストケース: LINEメッセージログの記録
        message_logs = [
            {
                "LINK_ID": "LINK000001",
                "CUSTOMER_ID": "CUST123456",
                "LINE_USER_ID": "U1234567890abcdef",
                "MESSAGE_TYPE": "TEXT",
                "SENT_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "SENT_TIME": datetime.utcnow().strftime('%H:%M:%S'),
                "STATUS": "SENT",
                "MESSAGE_ID": "LINE_MSG_123"
            },
            {
                "LINK_ID": "LINK000002",
                "CUSTOMER_ID": "CUST123457",
                "LINE_USER_ID": "U2345678901bcdefg",
                "MESSAGE_TYPE": "FLEX",
                "SENT_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "SENT_TIME": datetime.utcnow().strftime('%H:%M:%S'),
                "STATUS": "SENT",
                "MESSAGE_ID": "LINE_FLEX_456"
            }
        ]
        
        self.mock_database.insert_records.return_value = True
        
        # Copy Activity実行シミュレーション
        message_log_table = "line_message_logs"
        result = self.mock_database.insert_records(message_log_table, message_logs)
        
        self.assertTrue(result, "LINEメッセージログ記録失敗")
        self.mock_database.insert_records.assert_called_once_with(message_log_table, message_logs)
    
    def test_copy_activity_link_status_update(self):
        """Copy Activity: 連携ステータス更新テスト"""
        # テストケース: 連携ステータスの更新
        status_updates = [
            {
                "LINK_ID": "LINK000001",
                "NOTIFICATION_SENT": "Y",
                "NOTIFICATION_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "UPDATED_BY": "SYSTEM"
            },
            {
                "LINK_ID": "LINK000002",
                "NOTIFICATION_SENT": "Y",
                "NOTIFICATION_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "UPDATED_BY": "SYSTEM"
            }
        ]
        
        self.mock_database.update_records.return_value = True
        
        # Copy Activity実行シミュレーション
        for update in status_updates:
            result = self.mock_database.update_records(
                "line_id_links",
                update,
                where_clause=f"LINK_ID = '{update['LINK_ID']}'"
            )
            self.assertTrue(result, f"連携ステータス更新失敗: {update['LINK_ID']}")
        
        self.assertEqual(self.mock_database.update_records.call_count, 2, "更新処理回数不正")
    
    def test_script_activity_line_analytics(self):
        """Script Activity: LINE分析処理テスト"""
        # テストケース: LINE連携の分析データ生成
        line_analytics = {
            "execution_date": self.test_date,
            "total_active_links": 120,
            "total_pending_links": 15,
            "total_failed_links": 5,
            "messages_sent": 100,
            "rich_messages_sent": 80,
            "message_delivery_rate": 0.98,
            "link_verification_rate": 0.92,
            "average_response_time": 2.1,
            "processing_time_minutes": 1.8
        }
        
        self.mock_database.insert_records.return_value = True
        
        # Script Activity実行シミュレーション
        analytics_table = "line_id_link_analytics"
        result = self.mock_database.insert_records(analytics_table, [line_analytics])
        
        self.assertTrue(result, "LINE分析記録失敗")
        self.mock_database.insert_records.assert_called_once_with(analytics_table, [line_analytics])
    
    def test_data_flow_link_validation(self):
        """Data Flow: 連携データ検証テスト"""
        # テストケース: 連携データの検証
        test_links = [
            {"CUSTOMER_ID": "CUST123456", "LINE_USER_ID": "U1234567890abcdef", "VERIFICATION_STATUS": "VERIFIED"},
            {"CUSTOMER_ID": "", "LINE_USER_ID": "U1234567890abcdef", "VERIFICATION_STATUS": "VERIFIED"},  # 不正: 空ID
            {"CUSTOMER_ID": "CUST123458", "LINE_USER_ID": "INVALID", "VERIFICATION_STATUS": "VERIFIED"},  # 不正: 不正LINE ID
            {"CUSTOMER_ID": "CUST123459", "LINE_USER_ID": "U3456789012cdefgh", "VERIFICATION_STATUS": "UNKNOWN"}  # 不正: 不明認証状況
        ]
        
        # 連携データ検証ロジック（Data Flow内の処理）
        def validate_line_link_data(link):
            errors = []
            
            # 顧客ID検証
            if not link.get("CUSTOMER_ID", "").strip():
                errors.append("顧客ID必須")
            
            # LINE User ID検証
            line_user_id = link.get("LINE_USER_ID", "")
            if not line_user_id.startswith("U") or len(line_user_id) != 17:
                errors.append("LINE User ID形式不正")
            
            # 認証状況検証
            verification_status = link.get("VERIFICATION_STATUS", "")
            valid_statuses = ["VERIFIED", "PENDING", "FAILED"]
            if verification_status not in valid_statuses:
                errors.append("認証状況が不正です")
            
            return errors
        
        # 検証実行
        validation_results = []
        for link in test_links:
            errors = validate_line_link_data(link)
            validation_results.append({
                "link": link,
                "errors": errors,
                "is_valid": len(errors) == 0
            })
        
        # アサーション
        self.assertEqual(len(validation_results), 4, "検証結果数不正")
        self.assertTrue(validation_results[0]["is_valid"], "正常連携が不正判定")
        self.assertFalse(validation_results[1]["is_valid"], "不正連携（空ID）が正常判定")
        self.assertFalse(validation_results[2]["is_valid"], "不正連携（不正LINE ID）が正常判定")
        self.assertFalse(validation_results[3]["is_valid"], "不正連携（不明認証状況）が正常判定")
    
    def test_lookup_activity_line_usage_statistics(self):
        """Lookup Activity: LINE利用統計取得テスト"""
        # テストケース: LINE利用の統計情報取得
        line_usage_stats = [
            {
                "LINK_STATUS": "ACTIVE",
                "LINK_COUNT": 120,
                "AVERAGE_USAGE_DAYS": 45.2,
                "MESSAGE_OPEN_RATE": 0.85
            },
            {
                "LINK_STATUS": "PENDING",
                "LINK_COUNT": 15,
                "AVERAGE_USAGE_DAYS": 0.0,
                "MESSAGE_OPEN_RATE": 0.0
            }
        ]
        
        self.mock_database.query_records.return_value = line_usage_stats
        
        # Lookup Activity実行シミュレーション
        usage_query = f"""
        SELECT LINK_STATUS, 
               COUNT(*) as LINK_COUNT,
               AVG(DATEDIFF(day, LINK_DATE, '{self.test_date}')) as AVERAGE_USAGE_DAYS,
               AVG(MESSAGE_OPEN_RATE) as MESSAGE_OPEN_RATE
        FROM line_id_links
        WHERE LINK_DATE >= '{self.test_date}'
        GROUP BY LINK_STATUS
        """
        
        result = self.mock_database.query_records("line_id_links", usage_query)
        
        self.assertEqual(len(result), 2, "LINE利用統計取得件数不正")
        self.assertEqual(result[0]["LINK_STATUS"], "ACTIVE", "連携ステータス確認失敗")
        self.assertEqual(result[0]["LINK_COUNT"], 120, "連携件数確認失敗")
        self.assertEqual(result[0]["MESSAGE_OPEN_RATE"], 0.85, "メッセージ開封率確認失敗")
    
    def _create_line_id_link_info_csv_content(self) -> str:
        """LINE ID連携情報データ用CSVコンテンツ生成"""
        header = "LINK_ID,CUSTOMER_ID,CUSTOMER_NAME,EMAIL_ADDRESS,PHONE_NUMBER,LINE_USER_ID,LINK_STATUS,LINK_DATE,LINK_TYPE,VERIFICATION_STATUS,NOTIFICATION_PREFERENCE"
        rows = []
        
        for item in self.sample_line_link_data:
            row = f"{item['LINK_ID']},{item['CUSTOMER_ID']},{item['CUSTOMER_NAME']},{item['EMAIL_ADDRESS']},{item['PHONE_NUMBER']},{item['LINE_USER_ID']},{item['LINK_STATUS']},{item['LINK_DATE']},{item['LINK_TYPE']},{item['VERIFICATION_STATUS']},{item['NOTIFICATION_PREFERENCE']}"
            rows.append(row)
        
        return header + "\n" + "\n".join(rows)


if __name__ == "__main__":
    unittest.main()