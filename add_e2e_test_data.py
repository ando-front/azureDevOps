#!/usr/bin/env python3
"""
E2Eテスト用の追加データを作成するスクリプト
"""

import pyodbc
import json
import random
from datetime import datetime, timedelta

def add_e2e_test_data():
    """E2Eテスト用の追加データを作成"""
    try:
        # 接続文字列
        conn_str = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            "SERVER=localhost,1433;"
            "DATABASE=TGMATestDB;"
            "UID=sa;"
            "PWD=YourStrong!Passw0rd123;"
            "TrustServerCertificate=yes;"
            "Encrypt=no;"
        )
        
        print("🔗 データベースに接続中...")
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # 1. productタイプのraw_data_sourceデータを追加
        print("🔧 productタイプのデータを追加...")
        product_data = []
        for i in range(1, 11):  # 10個の製品データ
            product_json = {
                "product_id": i,
                "name": f"Product {i}",
                "category": ["Electronics", "Clothing", "Books", "Sports", "Home"][i % 5],
                "price": round(random.uniform(10.0, 500.0), 2),
                "in_stock": random.choice([True, False]),
                "description": f"High quality product {i} description"
            }
            product_data.append(('product', json.dumps(product_json)))
        
        for source_type, data_json in product_data:
            cursor.execute("""
                INSERT INTO [dbo].[raw_data_source] (source_type, data_json)
                VALUES (?, ?)
            """, source_type, data_json)
        
        print(f"  ✅ {len(product_data)}個のproductレコードを追加しました")
        
        # 2. data_quality_metricsテーブルに初期データを追加
        print("🔧 data_quality_metricsに初期データを追加...")
        quality_metrics = [
            ('client_dm', 'completeness', 95.5),
            ('client_dm', 'accuracy', 98.2),
            ('ClientDmBx', 'completeness', 92.1),
            ('ClientDmBx', 'accuracy', 96.8),
            ('raw_data_source', 'completeness', 99.1),
            ('raw_data_source', 'accuracy', 97.5),
        ]
        
        for table_name, metric_type, metric_value in quality_metrics:
            cursor.execute("""
                INSERT INTO [dbo].[data_quality_metrics] (table_name, metric_type, metric_value)
                VALUES (?, ?, ?)
            """, table_name, metric_type, metric_value)
        
        print(f"  ✅ {len(quality_metrics)}個の品質メトリクスを追加しました")
        
        # 3. processing_logsテーブルに履歴データを追加
        print("🔧 processing_logsに履歴データを追加...")
        process_logs = []
        base_time = datetime.now() - timedelta(days=7)
        
        for i in range(10):
            start_time = base_time + timedelta(hours=i*6)
            end_time = start_time + timedelta(minutes=random.randint(5, 30))
            
            process_logs.append((
                f"ETL_Process_{i+1}",
                start_time,
                end_time,
                random.choice(['SUCCESS', 'FAILED', 'WARNING']),
                random.randint(100, 10000),
                None if random.random() > 0.3 else f"Test error message {i+1}"
            ))
        
        for process_name, start_time, end_time, status, records_processed, error_message in process_logs:
            cursor.execute("""
                INSERT INTO [dbo].[processing_logs] (process_name, start_time, end_time, status, records_processed, error_message)
                VALUES (?, ?, ?, ?, ?, ?)
            """, process_name, start_time, end_time, status, records_processed, error_message)
        
        print(f"  ✅ {len(process_logs)}個の処理ログを追加しました")
        
        # 4. raw_data_sourceの一部レコードにlast_processed_timestampを設定
        print("🔧 last_processed_timestampを更新...")
        cursor.execute("""
            UPDATE [dbo].[raw_data_source] 
            SET last_processed_timestamp = DATEADD(HOUR, -ABS(CHECKSUM(NEWID()) % 48), GETDATE())
            WHERE id % 3 = 0
        """)
        
        rows_updated = cursor.rowcount
        print(f"  ✅ {rows_updated}個のレコードのlast_processed_timestampを更新しました")
        
        # 5. より多くのクライアントデータを追加
        print("🔧 追加のクライアントデータを作成...")
        additional_clients = []
        for i in range(21, 101):  # ID 21-100の80個のクライアント
            client = {
                "client_id": f"C{i:05d}",
                "client_name": f"Client {i}",
                "email": f"client{i}@example.com",
                "phone": f"090-{i:04d}-{random.randint(1000, 9999)}",
                "address": f"Address {i}, Tokyo, Japan",
                "registration_date": (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y-%m-%d'),
                "status": random.choice(["ACTIVE", "INACTIVE", "PENDING"]),
                "created_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "updated_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            additional_clients.append(client)
        
        # IDENTITY_INSERTを無効にして安全に挿入
        for client in additional_clients:
            cursor.execute("""
                INSERT INTO [dbo].[client_dm] (client_id, client_name, email, phone, address, registration_date, status, created_date, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, 
            client["client_id"], client["client_name"], client["email"], client["phone"], 
            client["address"], client["registration_date"], client["status"], 
            client["created_date"], client["updated_at"])
        
        print(f"  ✅ {len(additional_clients)}個の追加クライアントを作成しました")
        
        conn.commit()
        conn.close()
        
        print("\n🎉 E2Eテスト用追加データの作成が完了しました！")
        
        # 統計情報を表示
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        print("\n📊 データベース統計:")
        tables = ['raw_data_source', 'client_dm', 'ClientDmBx', 'data_quality_metrics', 'processing_logs', 'pipeline_execution_log']
        
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM [dbo].[{table}]")
                count = cursor.fetchone()[0]
                print(f"  - {table}: {count}レコード")
            except Exception as e:
                print(f"  - {table}: エラー ({str(e)})")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ エラーが発生しました: {e}")

if __name__ == "__main__":
    add_e2e_test_data()
