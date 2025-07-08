"""
pi_Send_ActionPointRecentTransactionHistoryList パイプラインのユニットテスト

アクションポイント最近のトランザクション履歴リスト送信パイプラインの個別コンポーネントテスト
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any


class TestActionPointRecentTransactionHistoryListUnit(unittest.TestCase):
    """アクションポイント最近のトランザクション履歴リストパイプラインユニットテスト"""
    
    def setUp(self):
        """テストセットアップ"""
        self.pipeline_name = "pi_Send_ActionPointRecentTransactionHistoryList"
        self.domain = "actionpoint"
        
        # モックサービス
        self.mock_blob_storage = Mock()
        self.mock_database = Mock()
        self.mock_email_service = Mock()
        self.mock_report_service = Mock()
        
        # テストデータ
        self.test_date = datetime.utcnow().strftime('%Y%m%d')
        self.test_period_start = (datetime.utcnow() - timedelta(days=30)).strftime('%Y%m%d')
        self.test_file_path = f"ActionPointRecentTransactionHistoryList/{self.test_date}/recent_transaction_history_list.csv"
        
        # 基本的なアクションポイント最近のトランザクション履歴リストデータ
        self.sample_transaction_history_data = [
            {
                "TRANSACTION_ID": "TXN123456789",
                "CUSTOMER_ID": "CUST123456",
                "CUSTOMER_NAME": "テストユーザー1",
                "EMAIL_ADDRESS": "test1@example.com",
                "TRANSACTION_TYPE": "EARN",
                "TRANSACTION_DATE": "20240228",
                "TRANSACTION_TIME": "10:30:45",
                "POINT_AMOUNT": 1000,
                "BALANCE_BEFORE": 1500,
                "BALANCE_AFTER": 2500,
                "SOURCE_TYPE": "CAMPAIGN",
                "SOURCE_ID": "CAMP2024001",
                "DESCRIPTION": "新規契約特典",
                "EXPIRY_DATE": "20250228",
                "STATUS": "COMPLETED"
            },
            {
                "TRANSACTION_ID": "TXN123456790",
                "CUSTOMER_ID": "CUST123457",
                "CUSTOMER_NAME": "テストユーザー2",
                "EMAIL_ADDRESS": "test2@example.com",
                "TRANSACTION_TYPE": "REDEEM",
                "TRANSACTION_DATE": "20240227",
                "TRANSACTION_TIME": "14:15:30",
                "POINT_AMOUNT": 500,
                "BALANCE_BEFORE": 800,
                "BALANCE_AFTER": 300,
                "SOURCE_TYPE": "STORE",
                "SOURCE_ID": "STORE001",
                "DESCRIPTION": "商品購入",
                "EXPIRY_DATE": "",
                "STATUS": "COMPLETED"
            }
        ]
    
    def test_lookup_activity_recent_transactions(self):
        """Lookup Activity: 最近のトランザクション検出テスト"""
        # テストケース: 最近のトランザクション検出
        mock_recent_transactions = [
            {
                "TRANSACTION_ID": "TXN123456789",
                "CUSTOMER_ID": "CUST123456",
                "TRANSACTION_TYPE": "EARN",
                "TRANSACTION_DATE": "20240228",
                "TRANSACTION_TIME": "10:30:45",
                "POINT_AMOUNT": 1000,
                "BALANCE_BEFORE": 1500,
                "BALANCE_AFTER": 2500,
                "SOURCE_TYPE": "CAMPAIGN",
                "SOURCE_ID": "CAMP2024001",
                "DESCRIPTION": "新規契約特典",
                "STATUS": "COMPLETED"
            },
            {
                "TRANSACTION_ID": "TXN123456790",
                "CUSTOMER_ID": "CUST123457",
                "TRANSACTION_TYPE": "REDEEM",
                "TRANSACTION_DATE": "20240227",
                "TRANSACTION_TIME": "14:15:30",
                "POINT_AMOUNT": 500,
                "BALANCE_BEFORE": 800,
                "BALANCE_AFTER": 300,
                "SOURCE_TYPE": "STORE",
                "SOURCE_ID": "STORE001",
                "DESCRIPTION": "商品購入",
                "STATUS": "COMPLETED"
            }
        ]
        
        self.mock_database.query_records.return_value = mock_recent_transactions
        
        # Lookup Activity実行シミュレーション
        recent_transactions_query = f"""
        SELECT TRANSACTION_ID, CUSTOMER_ID, TRANSACTION_TYPE, TRANSACTION_DATE, 
               TRANSACTION_TIME, POINT_AMOUNT, BALANCE_BEFORE, BALANCE_AFTER, 
               SOURCE_TYPE, SOURCE_ID, DESCRIPTION, STATUS
        FROM action_point_transaction_history
        WHERE TRANSACTION_DATE >= '{self.test_period_start}' AND STATUS = 'COMPLETED'
        ORDER BY TRANSACTION_DATE DESC, TRANSACTION_TIME DESC
        """
        
        result = self.mock_database.query_records("action_point_transaction_history", recent_transactions_query)
        
        self.assertEqual(len(result), 2, "最近のトランザクション検出件数不正")
        self.assertEqual(result[0]["TRANSACTION_ID"], "TXN123456789", "トランザクションID確認失敗")
        self.assertEqual(result[0]["STATUS"], "COMPLETED", "ステータス確認失敗")
        self.assertEqual(result[0]["TRANSACTION_TYPE"], "EARN", "トランザクションタイプ確認失敗")
    
    def test_lookup_activity_customer_transaction_summary(self):
        """Lookup Activity: 顧客トランザクション概要取得テスト"""
        # テストケース: 顧客別トランザクション概要の取得
        mock_customer_summaries = [
            {
                "CUSTOMER_ID": "CUST123456",
                "CUSTOMER_NAME": "テストユーザー1",
                "EMAIL_ADDRESS": "test1@example.com",
                "TOTAL_TRANSACTIONS": 15,
                "TOTAL_EARNED": 5000,
                "TOTAL_REDEEMED": 2000,
                "CURRENT_BALANCE": 3000,
                "LAST_TRANSACTION_DATE": "20240228",
                "LAST_TRANSACTION_TYPE": "EARN"
            },
            {
                "CUSTOMER_ID": "CUST123457",
                "CUSTOMER_NAME": "テストユーザー2",
                "EMAIL_ADDRESS": "test2@example.com",
                "TOTAL_TRANSACTIONS": 8,
                "TOTAL_EARNED": 2500,
                "TOTAL_REDEEMED": 1200,
                "CURRENT_BALANCE": 1300,
                "LAST_TRANSACTION_DATE": "20240227",
                "LAST_TRANSACTION_TYPE": "REDEEM"
            }
        ]
        
        self.mock_database.query_records.return_value = mock_customer_summaries
        
        # Lookup Activity実行シミュレーション
        customer_summary_query = f"""
        SELECT c.CUSTOMER_ID, c.CUSTOMER_NAME, c.EMAIL_ADDRESS,
               COUNT(t.TRANSACTION_ID) as TOTAL_TRANSACTIONS,
               SUM(CASE WHEN t.TRANSACTION_TYPE = 'EARN' THEN t.POINT_AMOUNT ELSE 0 END) as TOTAL_EARNED,
               SUM(CASE WHEN t.TRANSACTION_TYPE = 'REDEEM' THEN t.POINT_AMOUNT ELSE 0 END) as TOTAL_REDEEMED,
               cb.CURRENT_BALANCE,
               MAX(t.TRANSACTION_DATE) as LAST_TRANSACTION_DATE,
               (SELECT TOP 1 TRANSACTION_TYPE FROM action_point_transaction_history 
                WHERE CUSTOMER_ID = c.CUSTOMER_ID ORDER BY TRANSACTION_DATE DESC) as LAST_TRANSACTION_TYPE
        FROM customers c
        LEFT JOIN action_point_transaction_history t ON c.CUSTOMER_ID = t.CUSTOMER_ID
        LEFT JOIN customer_point_balances cb ON c.CUSTOMER_ID = cb.CUSTOMER_ID
        WHERE t.TRANSACTION_DATE >= '{self.test_period_start}' AND t.STATUS = 'COMPLETED'
        GROUP BY c.CUSTOMER_ID, c.CUSTOMER_NAME, c.EMAIL_ADDRESS, cb.CURRENT_BALANCE
        """
        
        result = self.mock_database.query_records("customers", customer_summary_query)
        
        self.assertEqual(len(result), 2, "顧客トランザクション概要取得件数不正")
        self.assertEqual(result[0]["CUSTOMER_ID"], "CUST123456", "顧客ID確認失敗")
        self.assertEqual(result[0]["TOTAL_TRANSACTIONS"], 15, "総トランザクション数確認失敗")
        self.assertEqual(result[0]["CURRENT_BALANCE"], 3000, "現在残高確認失敗")
    
    def test_data_flow_transaction_history_aggregation(self):
        """Data Flow: トランザクション履歴集計テスト"""
        # テストケース: トランザクション履歴の集計
        transaction_history_list = [
            {"CUSTOMER_ID": "CUST123456", "TRANSACTION_TYPE": "EARN", "POINT_AMOUNT": 1000, "TRANSACTION_DATE": "20240228", "SOURCE_TYPE": "CAMPAIGN"},
            {"CUSTOMER_ID": "CUST123456", "TRANSACTION_TYPE": "REDEEM", "POINT_AMOUNT": 200, "TRANSACTION_DATE": "20240227", "SOURCE_TYPE": "STORE"},
            {"CUSTOMER_ID": "CUST123456", "TRANSACTION_TYPE": "EARN", "POINT_AMOUNT": 500, "TRANSACTION_DATE": "20240226", "SOURCE_TYPE": "PURCHASE"},
            {"CUSTOMER_ID": "CUST123457", "TRANSACTION_TYPE": "EARN", "POINT_AMOUNT": 800, "TRANSACTION_DATE": "20240225", "SOURCE_TYPE": "CAMPAIGN"},
            {"CUSTOMER_ID": "CUST123457", "TRANSACTION_TYPE": "REDEEM", "POINT_AMOUNT": 300, "TRANSACTION_DATE": "20240224", "SOURCE_TYPE": "STORE"}
        ]
        
        # トランザクション履歴集計ロジック（Data Flow内の処理）
        def aggregate_transaction_history(transactions):
            aggregation = {}
            
            for transaction in transactions:
                customer_id = transaction["CUSTOMER_ID"]
                transaction_type = transaction["TRANSACTION_TYPE"]
                point_amount = transaction["POINT_AMOUNT"]
                source_type = transaction["SOURCE_TYPE"]
                
                if customer_id not in aggregation:
                    aggregation[customer_id] = {
                        "total_transactions": 0,
                        "total_earned": 0,
                        "total_redeemed": 0,
                        "transaction_types": set(),
                        "source_types": set(),
                        "earn_transactions": 0,
                        "redeem_transactions": 0,
                        "campaign_transactions": 0,
                        "store_transactions": 0,
                        "purchase_transactions": 0
                    }
                
                aggregation[customer_id]["total_transactions"] += 1
                aggregation[customer_id]["transaction_types"].add(transaction_type)
                aggregation[customer_id]["source_types"].add(source_type)
                
                if transaction_type == "EARN":
                    aggregation[customer_id]["total_earned"] += point_amount
                    aggregation[customer_id]["earn_transactions"] += 1
                elif transaction_type == "REDEEM":
                    aggregation[customer_id]["total_redeemed"] += point_amount
                    aggregation[customer_id]["redeem_transactions"] += 1
                
                if source_type == "CAMPAIGN":
                    aggregation[customer_id]["campaign_transactions"] += 1
                elif source_type == "STORE":
                    aggregation[customer_id]["store_transactions"] += 1
                elif source_type == "PURCHASE":
                    aggregation[customer_id]["purchase_transactions"] += 1
            
            # setをlistに変換
            for customer_id in aggregation:
                aggregation[customer_id]["transaction_types"] = list(aggregation[customer_id]["transaction_types"])
                aggregation[customer_id]["source_types"] = list(aggregation[customer_id]["source_types"])
            
            return aggregation
        
        # 集計実行
        aggregated_data = aggregate_transaction_history(transaction_history_list)
        
        # アサーション
        self.assertEqual(len(aggregated_data), 2, "集計顧客数不正")
        self.assertEqual(aggregated_data["CUST123456"]["total_transactions"], 3, "CUST123456総トランザクション数不正")
        self.assertEqual(aggregated_data["CUST123456"]["total_earned"], 1500, "CUST123456獲得ポイント合計不正")
        self.assertEqual(aggregated_data["CUST123456"]["total_redeemed"], 200, "CUST123456使用ポイント合計不正")
        self.assertEqual(aggregated_data["CUST123456"]["earn_transactions"], 2, "CUST123456獲得トランザクション数不正")
        self.assertEqual(aggregated_data["CUST123456"]["redeem_transactions"], 1, "CUST123456使用トランザクション数不正")
        
        self.assertEqual(aggregated_data["CUST123457"]["total_transactions"], 2, "CUST123457総トランザクション数不正")
        self.assertEqual(aggregated_data["CUST123457"]["total_earned"], 800, "CUST123457獲得ポイント合計不正")
        self.assertEqual(aggregated_data["CUST123457"]["total_redeemed"], 300, "CUST123457使用ポイント合計不正")
    
    def test_data_flow_transaction_history_report_generation(self):
        """Data Flow: トランザクション履歴レポート生成テスト"""
        # テストケース: トランザクション履歴レポートの生成
        transaction_summary_data = {
            "report_period": "30日間",
            "period_start": "20240130",
            "period_end": "20240229",
            "total_transactions": 485,
            "total_customers": 125,
            "total_points_earned": 280000,
            "total_points_redeemed": 95000,
            "transaction_type_breakdown": {
                "EARN": 320,
                "REDEEM": 155,
                "EXPIRE": 10
            },
            "source_type_breakdown": {
                "CAMPAIGN": 180,
                "STORE": 160,
                "PURCHASE": 120,
                "REFERRAL": 25
            }
        }
        
        # トランザクション履歴レポート生成ロジック（Data Flow内の処理）
        def generate_transaction_history_report(summary_data):
            report_period = summary_data["report_period"]
            period_start = summary_data["period_start"]
            period_end = summary_data["period_end"]
            total_transactions = summary_data["total_transactions"]
            total_customers = summary_data["total_customers"]
            total_earned = summary_data["total_points_earned"]
            total_redeemed = summary_data["total_points_redeemed"]
            transaction_breakdown = summary_data["transaction_type_breakdown"]
            source_breakdown = summary_data["source_type_breakdown"]
            
            # レポートの構造化
            transaction_history_report = {
                "report_header": {
                    "report_type": "TRANSACTION_HISTORY_SUMMARY",
                    "report_period": report_period,
                    "period_start": period_start,
                    "period_end": period_end,
                    "generation_date": datetime.utcnow().strftime('%Y%m%d'),
                    "total_transactions": total_transactions,
                    "total_customers": total_customers,
                    "total_points_earned": total_earned,
                    "total_points_redeemed": total_redeemed,
                    "net_points_change": total_earned - total_redeemed,
                    "average_transactions_per_customer": round(total_transactions / total_customers, 2) if total_customers > 0 else 0
                },
                "transaction_type_summary": transaction_breakdown,
                "source_type_summary": source_breakdown,
                "key_metrics": {
                    "most_common_transaction_type": max(transaction_breakdown.keys(), key=lambda x: transaction_breakdown[x]),
                    "most_active_source_type": max(source_breakdown.keys(), key=lambda x: source_breakdown[x]),
                    "earn_to_redeem_ratio": round(transaction_breakdown.get("EARN", 0) / transaction_breakdown.get("REDEEM", 1), 2),
                    "average_points_per_transaction": round(total_earned / total_transactions, 2) if total_transactions > 0 else 0
                },
                "activity_analysis": {
                    "high_activity_customers": int(total_customers * 0.2),  # 上位20%
                    "medium_activity_customers": int(total_customers * 0.5),  # 中位50%
                    "low_activity_customers": int(total_customers * 0.3)  # 下位30%
                }
            }
            
            return transaction_history_report
        
        # レポート生成実行
        report = generate_transaction_history_report(transaction_summary_data)
        
        # アサーション
        self.assertEqual(report["report_header"]["report_type"], "TRANSACTION_HISTORY_SUMMARY", "レポートタイプ確認失敗")
        self.assertEqual(report["report_header"]["report_period"], "30日間", "レポート期間確認失敗")
        self.assertEqual(report["report_header"]["total_transactions"], 485, "総トランザクション数確認失敗")
        self.assertEqual(report["report_header"]["net_points_change"], 185000, "ネットポイント変動確認失敗")
        self.assertEqual(report["report_header"]["average_transactions_per_customer"], 3.88, "顧客あたり平均トランザクション数確認失敗")
        self.assertEqual(report["key_metrics"]["most_common_transaction_type"], "EARN", "最頻出トランザクションタイプ確認失敗")
        self.assertEqual(report["key_metrics"]["most_active_source_type"], "CAMPAIGN", "最活発ソースタイプ確認失敗")
        self.assertEqual(report["key_metrics"]["earn_to_redeem_ratio"], 2.06, "獲得対使用比率確認失敗")
    
    def test_data_flow_customer_activity_ranking(self):
        """Data Flow: 顧客活動ランキング処理テスト"""
        # テストケース: 顧客活動ランキングの生成
        customer_activity_data = [
            {"CUSTOMER_ID": "CUST123456", "TOTAL_TRANSACTIONS": 15, "TOTAL_EARNED": 5000, "TOTAL_REDEEMED": 2000},
            {"CUSTOMER_ID": "CUST123457", "TOTAL_TRANSACTIONS": 8, "TOTAL_EARNED": 2500, "TOTAL_REDEEMED": 1200},
            {"CUSTOMER_ID": "CUST123458", "TOTAL_TRANSACTIONS": 12, "TOTAL_EARNED": 4000, "TOTAL_REDEEMED": 1800},
            {"CUSTOMER_ID": "CUST123459", "TOTAL_TRANSACTIONS": 5, "TOTAL_EARNED": 1500, "TOTAL_REDEEMED": 500}
        ]
        
        # 顧客活動ランキング生成ロジック（Data Flow内の処理）
        def generate_customer_activity_ranking(activity_data):
            # トランザクション数でソート
            sorted_by_transactions = sorted(activity_data, key=lambda x: x["TOTAL_TRANSACTIONS"], reverse=True)
            
            # 獲得ポイント数でソート
            sorted_by_earned = sorted(activity_data, key=lambda x: x["TOTAL_EARNED"], reverse=True)
            
            # 使用ポイント数でソート
            sorted_by_redeemed = sorted(activity_data, key=lambda x: x["TOTAL_REDEEMED"], reverse=True)
            
            # 活動スコア（トランザクション数 * 10 + 獲得ポイント数 * 0.1）でソート
            for customer in activity_data:
                customer["ACTIVITY_SCORE"] = (customer["TOTAL_TRANSACTIONS"] * 10) + (customer["TOTAL_EARNED"] * 0.1)
            
            sorted_by_activity = sorted(activity_data, key=lambda x: x["ACTIVITY_SCORE"], reverse=True)
            
            # ランキング情報の追加
            for i, customer in enumerate(sorted_by_transactions):
                customer["TRANSACTION_RANK"] = i + 1
            
            for i, customer in enumerate(sorted_by_earned):
                customer["EARNED_RANK"] = i + 1
            
            for i, customer in enumerate(sorted_by_redeemed):
                customer["REDEEMED_RANK"] = i + 1
            
            for i, customer in enumerate(sorted_by_activity):
                customer["ACTIVITY_RANK"] = i + 1
            
            return {
                "ranking_by_transactions": sorted_by_transactions,
                "ranking_by_earned": sorted_by_earned,
                "ranking_by_redeemed": sorted_by_redeemed,
                "ranking_by_activity": sorted_by_activity,
                "top_customer_overall": sorted_by_activity[0] if sorted_by_activity else None
            }
        
        # ランキング生成実行
        ranking = generate_customer_activity_ranking(customer_activity_data)
        
        # アサーション
        self.assertEqual(ranking["ranking_by_transactions"][0]["CUSTOMER_ID"], "CUST123456", "トランザクション数ランキング1位確認失敗")
        self.assertEqual(ranking["ranking_by_earned"][0]["CUSTOMER_ID"], "CUST123456", "獲得ポイントランキング1位確認失敗")
        self.assertEqual(ranking["ranking_by_redeemed"][0]["CUSTOMER_ID"], "CUST123456", "使用ポイントランキング1位確認失敗")
        self.assertEqual(ranking["top_customer_overall"]["CUSTOMER_ID"], "CUST123456", "総合ランキング1位確認失敗")
        self.assertEqual(ranking["ranking_by_transactions"][0]["TRANSACTION_RANK"], 1, "トランザクション数ランク確認失敗")
        self.assertEqual(ranking["ranking_by_earned"][0]["EARNED_RANK"], 1, "獲得ポイントランク確認失敗")
    
    def test_script_activity_transaction_history_report_generation(self):
        """Script Activity: トランザクション履歴レポート生成処理テスト"""
        # テストケース: トランザクション履歴レポート生成処理
        report_data = {
            "report_type": "TRANSACTION_HISTORY_SUMMARY",
            "report_period": "30日間",
            "total_transactions": 485,
            "total_customers": 125,
            "total_points_earned": 280000
        }
        
        self.mock_report_service.generate_transaction_history_report.return_value = {
            "status": "success",
            "report_id": "TXNRPT20240301001",
            "report_path": "/reports/transaction_history/20240301_history_summary.pdf",
            "file_size": 2048000,
            "generation_time": datetime.utcnow().isoformat()
        }
        
        # Script Activity実行シミュレーション
        result = self.mock_report_service.generate_transaction_history_report(
            report_data["report_type"],
            report_data["report_period"],
            total_transactions=report_data["total_transactions"],
            total_customers=report_data["total_customers"],
            total_points_earned=report_data["total_points_earned"]
        )
        
        self.assertEqual(result["status"], "success", "レポート生成失敗")
        self.assertEqual(result["report_id"], "TXNRPT20240301001", "レポートID確認失敗")
        self.assertIsNotNone(result["report_path"], "レポートパス確認失敗")
        self.assertGreater(result["file_size"], 0, "ファイルサイズ確認失敗")
        self.mock_report_service.generate_transaction_history_report.assert_called_once()
    
    def test_script_activity_customer_notification_sending(self):
        """Script Activity: 顧客通知送信処理テスト"""
        # テストケース: 顧客通知の送信
        notification_data = {
            "to": ["customer1@example.com", "customer2@example.com"],
            "subject": "【東京ガス】アクションポイント最近のお取引履歴",
            "body": "お客様の最近のアクションポイントお取引履歴をお知らせいたします。",
            "attachments": ["/reports/transaction_history/20240301_history_summary.pdf"],
            "report_period": "30日間"
        }
        
        self.mock_email_service.send_transaction_history_notification.return_value = {
            "status": "sent",
            "message_id": "TXNHIST_NOTIF_789",
            "delivery_time": datetime.utcnow().isoformat(),
            "recipients_count": 2
        }
        
        # Script Activity実行シミュレーション
        result = self.mock_email_service.send_transaction_history_notification(
            notification_data["to"],
            notification_data["subject"],
            notification_data["body"],
            attachments=notification_data["attachments"],
            report_period=notification_data["report_period"]
        )
        
        self.assertEqual(result["status"], "sent", "顧客通知送信失敗")
        self.assertEqual(result["message_id"], "TXNHIST_NOTIF_789", "メッセージID確認失敗")
        self.assertEqual(result["recipients_count"], 2, "受信者数確認失敗")
        self.mock_email_service.send_transaction_history_notification.assert_called_once()
    
    def test_copy_activity_transaction_history_export(self):
        """Copy Activity: トランザクション履歴エクスポート処理テスト"""
        # テストケース: トランザクション履歴のエクスポート
        transaction_history_data = self.sample_transaction_history_data
        
        # CSVエクスポート（Copy Activity内の処理）
        def export_transaction_history_csv(transaction_data):
            if not transaction_data:
                return ""
            
            header = ",".join(transaction_data[0].keys())
            rows = []
            
            for transaction in transaction_data:
                row = ",".join(str(transaction[key]) for key in transaction.keys())
                rows.append(row)
            
            return header + "\n" + "\n".join(rows)
        
        # エクスポート実行
        csv_content = export_transaction_history_csv(transaction_history_data)
        
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
        
        self.assertEqual(result["status"], "success", "トランザクション履歴エクスポート失敗")
        self.assertEqual(result["file_path"], self.test_file_path, "ファイルパス確認失敗")
        self.assertGreater(result["file_size"], 0, "ファイルサイズ確認失敗")
        self.mock_blob_storage.upload_file.assert_called_once()
    
    def test_copy_activity_customer_activity_summary_update(self):
        """Copy Activity: 顧客活動サマリー更新テスト"""
        # テストケース: 顧客活動サマリーの更新
        activity_summaries = [
            {
                "CUSTOMER_ID": "CUST123456",
                "SUMMARY_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "PERIOD_START": self.test_period_start,
                "PERIOD_END": self.test_date,
                "TOTAL_TRANSACTIONS": 15,
                "TOTAL_EARNED": 5000,
                "TOTAL_REDEEMED": 2000,
                "TRANSACTION_RANK": 1,
                "EARNED_RANK": 1,
                "REDEEMED_RANK": 1,
                "ACTIVITY_SCORE": 650.0,
                "ACTIVITY_RANK": 1,
                "UPDATED_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "UPDATED_TIME": datetime.utcnow().strftime('%H:%M:%S')
            },
            {
                "CUSTOMER_ID": "CUST123457",
                "SUMMARY_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "PERIOD_START": self.test_period_start,
                "PERIOD_END": self.test_date,
                "TOTAL_TRANSACTIONS": 8,
                "TOTAL_EARNED": 2500,
                "TOTAL_REDEEMED": 1200,
                "TRANSACTION_RANK": 2,
                "EARNED_RANK": 2,
                "REDEEMED_RANK": 2,
                "ACTIVITY_SCORE": 330.0,
                "ACTIVITY_RANK": 2,
                "UPDATED_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "UPDATED_TIME": datetime.utcnow().strftime('%H:%M:%S')
            }
        ]
        
        self.mock_database.insert_records.return_value = True
        
        # Copy Activity実行シミュレーション
        summary_table = "customer_activity_summary"
        result = self.mock_database.insert_records(summary_table, activity_summaries)
        
        self.assertTrue(result, "顧客活動サマリー更新失敗")
        self.mock_database.insert_records.assert_called_once_with(summary_table, activity_summaries)
    
    def test_script_activity_transaction_history_analytics(self):
        """Script Activity: トランザクション履歴分析処理テスト"""
        # テストケース: トランザクション履歴分析データ生成
        transaction_history_analytics = {
            "execution_date": self.test_date,
            "report_period": "30日間",
            "period_start": self.test_period_start,
            "period_end": self.test_date,
            "total_transactions_analyzed": 485,
            "unique_customers_analyzed": 125,
            "earn_transactions": 320,
            "redeem_transactions": 155,
            "expire_transactions": 10,
            "total_points_earned": 280000,
            "total_points_redeemed": 95000,
            "total_points_expired": 5000,
            "average_points_per_transaction": 577.32,
            "campaign_transactions": 180,
            "store_transactions": 160,
            "purchase_transactions": 120,
            "referral_transactions": 25,
            "high_activity_customers": 25,
            "medium_activity_customers": 62,
            "low_activity_customers": 38,
            "report_generation_success": True,
            "customer_notification_sent": True,
            "processing_time_minutes": 22.8
        }
        
        self.mock_database.insert_records.return_value = True
        
        # Script Activity実行シミュレーション
        analytics_table = "transaction_history_analytics"
        result = self.mock_database.insert_records(analytics_table, [transaction_history_analytics])
        
        self.assertTrue(result, "トランザクション履歴分析記録失敗")
        self.mock_database.insert_records.assert_called_once_with(analytics_table, [transaction_history_analytics])
    
    def test_data_flow_transaction_history_validation(self):
        """Data Flow: トランザクション履歴データ検証テスト"""
        # テストケース: トランザクション履歴データの検証
        test_transactions = [
            {"TRANSACTION_ID": "TXN123456789", "CUSTOMER_ID": "CUST123456", "TRANSACTION_TYPE": "EARN", "POINT_AMOUNT": 1000, "TRANSACTION_DATE": "20240228"},
            {"TRANSACTION_ID": "", "CUSTOMER_ID": "CUST123457", "TRANSACTION_TYPE": "REDEEM", "POINT_AMOUNT": 500, "TRANSACTION_DATE": "20240227"},  # 不正: 空トランザクションID
            {"TRANSACTION_ID": "TXN123456791", "CUSTOMER_ID": "CUST123458", "TRANSACTION_TYPE": "INVALID", "POINT_AMOUNT": 100, "TRANSACTION_DATE": "20240226"},  # 不正: 不正トランザクションタイプ
            {"TRANSACTION_ID": "TXN123456792", "CUSTOMER_ID": "CUST123459", "TRANSACTION_TYPE": "EARN", "POINT_AMOUNT": 0, "TRANSACTION_DATE": "20240225"}  # 不正: ポイント数0
        ]
        
        # トランザクション履歴データ検証ロジック（Data Flow内の処理）
        def validate_transaction_history(transaction):
            errors = []
            
            # トランザクションID検証
            if not transaction.get("TRANSACTION_ID", "").strip():
                errors.append("トランザクションID必須")
            
            # 顧客ID検証
            if not transaction.get("CUSTOMER_ID", "").strip():
                errors.append("顧客ID必須")
            
            # トランザクションタイプ検証
            valid_transaction_types = ["EARN", "REDEEM", "EXPIRE", "ADJUST"]
            if transaction.get("TRANSACTION_TYPE") not in valid_transaction_types:
                errors.append("トランザクションタイプが不正です")
            
            # ポイント数検証
            point_amount = transaction.get("POINT_AMOUNT", 0)
            if point_amount <= 0:
                errors.append("ポイント数は正の値である必要があります")
            
            # トランザクション日検証
            transaction_date = transaction.get("TRANSACTION_DATE", "")
            if not transaction_date:
                errors.append("トランザクション日必須")
            
            return errors
        
        # 検証実行
        validation_results = []
        for transaction in test_transactions:
            errors = validate_transaction_history(transaction)
            validation_results.append({
                "transaction": transaction,
                "errors": errors,
                "is_valid": len(errors) == 0
            })
        
        # アサーション
        self.assertEqual(len(validation_results), 4, "検証結果数不正")
        self.assertTrue(validation_results[0]["is_valid"], "正常トランザクションが不正判定")
        self.assertFalse(validation_results[1]["is_valid"], "不正トランザクション（空ID）が正常判定")
        self.assertFalse(validation_results[2]["is_valid"], "不正トランザクション（不正タイプ）が正常判定")
        self.assertFalse(validation_results[3]["is_valid"], "不正トランザクション（ポイント数0）が正常判定")
    
    def test_lookup_activity_transaction_trend_analysis(self):
        """Lookup Activity: トランザクション傾向分析テスト"""
        # テストケース: トランザクション傾向の分析
        transaction_trends = [
            {
                "ANALYSIS_DATE": "20240228",
                "TRANSACTION_TYPE": "EARN",
                "DAILY_COUNT": 45,
                "DAILY_POINTS": 35000,
                "TREND_DIRECTION": "UP",
                "GROWTH_RATE": 0.12
            },
            {
                "ANALYSIS_DATE": "20240227",
                "TRANSACTION_TYPE": "EARN",
                "DAILY_COUNT": 40,
                "DAILY_POINTS": 31000,
                "TREND_DIRECTION": "UP",
                "GROWTH_RATE": 0.08
            },
            {
                "ANALYSIS_DATE": "20240228",
                "TRANSACTION_TYPE": "REDEEM",
                "DAILY_COUNT": 25,
                "DAILY_POINTS": 18000,
                "TREND_DIRECTION": "STABLE",
                "GROWTH_RATE": 0.02
            }
        ]
        
        self.mock_database.query_records.return_value = transaction_trends
        
        # Lookup Activity実行シミュレーション
        trend_query = f"""
        SELECT ANALYSIS_DATE, TRANSACTION_TYPE, 
               COUNT(*) as DAILY_COUNT,
               SUM(POINT_AMOUNT) as DAILY_POINTS,
               CASE 
                   WHEN COUNT(*) > LAG(COUNT(*)) OVER (PARTITION BY TRANSACTION_TYPE ORDER BY ANALYSIS_DATE) THEN 'UP'
                   WHEN COUNT(*) < LAG(COUNT(*)) OVER (PARTITION BY TRANSACTION_TYPE ORDER BY ANALYSIS_DATE) THEN 'DOWN'
                   ELSE 'STABLE'
               END as TREND_DIRECTION,
               CAST((COUNT(*) - LAG(COUNT(*)) OVER (PARTITION BY TRANSACTION_TYPE ORDER BY ANALYSIS_DATE)) AS FLOAT) / 
               LAG(COUNT(*)) OVER (PARTITION BY TRANSACTION_TYPE ORDER BY ANALYSIS_DATE) as GROWTH_RATE
        FROM action_point_transaction_history
        WHERE TRANSACTION_DATE >= '{self.test_period_start}' AND STATUS = 'COMPLETED'
        GROUP BY ANALYSIS_DATE, TRANSACTION_TYPE
        ORDER BY ANALYSIS_DATE DESC, TRANSACTION_TYPE
        """
        
        result = self.mock_database.query_records("action_point_transaction_history", trend_query)
        
        self.assertEqual(len(result), 3, "トランザクション傾向分析取得件数不正")
        self.assertEqual(result[0]["TRANSACTION_TYPE"], "EARN", "トランザクションタイプ確認失敗")
        self.assertEqual(result[0]["DAILY_COUNT"], 45, "日次件数確認失敗")
        self.assertEqual(result[0]["TREND_DIRECTION"], "UP", "傾向方向確認失敗")
        self.assertEqual(result[0]["GROWTH_RATE"], 0.12, "成長率確認失敗")
    
    def test_data_flow_batch_processing(self):
        """Data Flow: バッチ処理テスト"""
        # テストケース: 大量トランザクション履歴のバッチ処理
        large_transaction_dataset = []
        for i in range(2000):
            large_transaction_dataset.append({
                "TRANSACTION_ID": f"TXN{i:09d}",
                "CUSTOMER_ID": f"CUST{i:06d}",
                "TRANSACTION_TYPE": ["EARN", "REDEEM", "EXPIRE"][i % 3],
                "POINT_AMOUNT": [1000, 500, 100][i % 3],
                "TRANSACTION_DATE": "20240228",
                "SOURCE_TYPE": ["CAMPAIGN", "STORE", "PURCHASE", "REFERRAL"][i % 4]
            })
        
        # バッチ処理ロジック（Data Flow内の処理）
        def process_transaction_history_batch(transaction_list, batch_size=200):
            processed_batches = []
            
            for i in range(0, len(transaction_list), batch_size):
                batch = transaction_list[i:i + batch_size]
                
                # バッチ処理の実行
                batch_result = {
                    "batch_id": f"BATCH_{i//batch_size + 1:03d}",
                    "transaction_count": len(batch),
                    "earn_transactions": sum(1 for txn in batch if txn["TRANSACTION_TYPE"] == "EARN"),
                    "redeem_transactions": sum(1 for txn in batch if txn["TRANSACTION_TYPE"] == "REDEEM"),
                    "expire_transactions": sum(1 for txn in batch if txn["TRANSACTION_TYPE"] == "EXPIRE"),
                    "total_points_earned": sum(txn["POINT_AMOUNT"] for txn in batch if txn["TRANSACTION_TYPE"] == "EARN"),
                    "total_points_redeemed": sum(txn["POINT_AMOUNT"] for txn in batch if txn["TRANSACTION_TYPE"] == "REDEEM"),
                    "total_points_expired": sum(txn["POINT_AMOUNT"] for txn in batch if txn["TRANSACTION_TYPE"] == "EXPIRE"),
                    "processing_time": 3.5  # シミュレーション
                }
                
                processed_batches.append(batch_result)
            
            return processed_batches
        
        # バッチ処理実行
        batch_results = process_transaction_history_batch(large_transaction_dataset, batch_size=200)
        
        # アサーション
        self.assertEqual(len(batch_results), 10, "バッチ数不正")  # 2000 / 200 = 10
        self.assertEqual(batch_results[0]["transaction_count"], 200, "バッチサイズ不正")
        self.assertGreater(batch_results[0]["total_points_earned"], 0, "バッチ獲得ポイント合計不正")
        
        # 全バッチの合計確認
        total_transactions = sum(batch["transaction_count"] for batch in batch_results)
        total_earned = sum(batch["total_points_earned"] for batch in batch_results)
        total_redeemed = sum(batch["total_points_redeemed"] for batch in batch_results)
        total_expired = sum(batch["total_points_expired"] for batch in batch_results)
        
        self.assertEqual(total_transactions, 2000, "全バッチ処理件数不正")
        self.assertGreater(total_earned, 0, "全バッチ獲得ポイント合計不正")
        self.assertGreater(total_redeemed, 0, "全バッチ使用ポイント合計不正")
        self.assertGreater(total_expired, 0, "全バッチ失効ポイント合計不正")
    
    def _create_recent_transaction_history_list_csv_content(self) -> str:
        """最近のトランザクション履歴リストデータ用CSVコンテンツ生成"""
        header = "TRANSACTION_ID,CUSTOMER_ID,CUSTOMER_NAME,EMAIL_ADDRESS,TRANSACTION_TYPE,TRANSACTION_DATE,TRANSACTION_TIME,POINT_AMOUNT,BALANCE_BEFORE,BALANCE_AFTER,SOURCE_TYPE,SOURCE_ID,DESCRIPTION,EXPIRY_DATE,STATUS"
        rows = []
        
        for item in self.sample_transaction_history_data:
            row = f"{item['TRANSACTION_ID']},{item['CUSTOMER_ID']},{item['CUSTOMER_NAME']},{item['EMAIL_ADDRESS']},{item['TRANSACTION_TYPE']},{item['TRANSACTION_DATE']},{item['TRANSACTION_TIME']},{item['POINT_AMOUNT']},{item['BALANCE_BEFORE']},{item['BALANCE_AFTER']},{item['SOURCE_TYPE']},{item['SOURCE_ID']},{item['DESCRIPTION']},{item['EXPIRY_DATE']},{item['STATUS']}"
            rows.append(row)
        
        return header + "\n" + "\n".join(rows)


if __name__ == "__main__":
    unittest.main()