#!/usr/bin/env python3
"""
データベーススキーマを修正してE2Eテストに対応させるスクリプト
"""

import pyodbc
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fix_database_schema():
    """データベーススキーマを修正"""
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
        
        # 1. data_quality_metricsテーブルを正しい構造で再作成
        print("🔧 data_quality_metricsテーブルを再作成...")
        try:
            cursor.execute("DROP TABLE IF EXISTS [dbo].[data_quality_metrics]")
            cursor.execute("""
                CREATE TABLE [dbo].[data_quality_metrics] (
                    [metric_id] INT IDENTITY(1,1) PRIMARY KEY,
                    [table_name] NVARCHAR(100) NOT NULL,
                    [metric_type] NVARCHAR(50) NOT NULL,
                    [metric_value] DECIMAL(18,4) NOT NULL,
                    [measured_at] DATETIME2 DEFAULT GETDATE(),
                    [created_at] DATETIME2 DEFAULT GETDATE()
                );
            """)
            print("  ✅ data_quality_metricsテーブルを再作成しました")
        except Exception as e:
            print(f"  ⚠️ テーブル再作成エラー: {e}")
        
        # 2. raw_data_sourceテーブルにproductタイプのデータを追加
        print("🔧 raw_data_sourceにproductデータを追加...")
        try:
            product_data = [
                ("product", '{"product_id": 1, "name": "Widget A", "price": 29.99, "category": "electronics"}'),
                ("product", '{"product_id": 2, "name": "Widget B", "price": 49.99, "category": "electronics"}'),
                ("product", '{"product_id": 3, "name": "Gadget C", "price": 19.99, "category": "accessories"}'),
                ("product", '{"product_id": 4, "name": "Tool D", "price": 79.99, "category": "tools"}'),
                ("product", '{"product_id": 5, "name": "Device E", "price": 99.99, "category": "electronics"}')
            ]
            
            for source_type, data_json in product_data:
                cursor.execute("""
                    INSERT INTO [dbo].[raw_data_source] (source_type, data_json)
                    VALUES (?, ?)
                """, source_type, data_json)
            
            print("  ✅ productデータを追加しました")
        except Exception as e:
            print(f"  ⚠️ productデータ追加エラー: {e}")
        
        # 3. performance_benchmarksテーブルを作成
        print("🔧 performance_benchmarksテーブルを作成...")
        try:
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='performance_benchmarks')
                BEGIN
                    CREATE TABLE [dbo].[performance_benchmarks] (
                        [benchmark_id] INT IDENTITY(1,1) PRIMARY KEY,
                        [operation_type] NVARCHAR(100) NOT NULL,
                        [operation_name] NVARCHAR(100) NOT NULL,
                        [records_processed] INT NOT NULL,
                        [execution_time_ms] INT NOT NULL,
                        [memory_usage_mb] DECIMAL(18,2) NULL,
                        [cpu_usage_percent] DECIMAL(5,2) NULL,
                        [throughput_rps] DECIMAL(18,2) NULL,
                        [created_at] DATETIME2 DEFAULT GETDATE()
                    );
                END
            """)
            print("  ✅ performance_benchmarksテーブルを作成しました")
        except Exception as e:
            print(f"  ⚠️ テーブル作成エラー: {e}")
        
        # 4. validation_resultsテーブルを作成
        print("🔧 validation_resultsテーブルを作成...")
        try:
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='validation_results')
                BEGIN
                    CREATE TABLE [dbo].[validation_results] (
                        [validation_id] INT IDENTITY(1,1) PRIMARY KEY,
                        [table_name] NVARCHAR(100) NOT NULL,
                        [validation_type] NVARCHAR(50) NOT NULL,
                        [validation_rule] NVARCHAR(500) NOT NULL,
                        [is_valid] BIT NOT NULL,
                        [error_count] INT DEFAULT 0,
                        [details] NVARCHAR(MAX) NULL,
                        [validated_at] DATETIME2 DEFAULT GETDATE()
                    );
                END
            """)
            print("  ✅ validation_resultsテーブルを作成しました")
        except Exception as e:
            print(f"  ⚠️ テーブル作成エラー: {e}")
        
        # 5. error_logsテーブルを作成
        print("🔧 error_logsテーブルを作成...")
        try:
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='error_logs')
                BEGIN
                    CREATE TABLE [dbo].[error_logs] (
                        [error_id] INT IDENTITY(1,1) PRIMARY KEY,
                        [error_type] NVARCHAR(50) NOT NULL,
                        [error_message] NVARCHAR(MAX) NOT NULL,
                        [source_table] NVARCHAR(100) NULL,
                        [record_id] NVARCHAR(50) NULL,
                        [pipeline_execution_id] NVARCHAR(100) NULL,
                        [stack_trace] NVARCHAR(MAX) NULL,
                        [occurred_at] DATETIME2 DEFAULT GETDATE()
                    );
                END
            """)
            print("  ✅ error_logsテーブルを作成しました")
        except Exception as e:
            print(f"  ⚠️ テーブル作成エラー: {e}")
        
        # 6. データ品質メトリクスのサンプルデータを挿入
        print("🔧 サンプルデータを挿入...")
        try:
            sample_metrics = [
                ("client_dm", "record_count", 100),
                ("client_dm", "completeness", 0.95),
                ("client_dm", "accuracy", 0.98),
                ("ClientDmBx", "record_count", 95),
                ("ClientDmBx", "completeness", 0.92),
                ("raw_data_source", "record_count", 150),
                ("raw_data_source", "validity", 0.97)
            ]
            
            for table_name, metric_type, metric_value in sample_metrics:
                cursor.execute("""
                    INSERT INTO [dbo].[data_quality_metrics] (table_name, metric_type, metric_value)
                    VALUES (?, ?, ?)
                """, table_name, metric_type, metric_value)
            
            print("  ✅ サンプルデータを挿入しました")
        except Exception as e:
            print(f"  ⚠️ サンプルデータ挿入エラー: {e}")
        
        conn.commit()
        conn.close()
        
        print("\n🎉 データベーススキーマの修正が完了しました！")
        
    except Exception as e:
        print(f"❌ エラーが発生しました: {e}")

if __name__ == "__main__":
    fix_database_schema()
