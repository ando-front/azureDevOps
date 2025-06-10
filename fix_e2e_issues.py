#!/usr/bin/env python3
"""
E2Eテストの問題を修正するためのスクリプト
"""

import pyodbc
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fix_e2e_test_issues():
    """E2Eテストの失敗原因を修正"""
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
        
        # 1. raw_data_sourceテーブルにlast_processed_timestampカラムを追加
        print("🔧 raw_data_sourceテーブルにlast_processed_timestampカラムを追加...")
        try:
            cursor.execute("""
                IF NOT EXISTS (
                    SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = 'raw_data_source' 
                    AND COLUMN_NAME = 'last_processed_timestamp'
                )
                BEGIN
                    ALTER TABLE [dbo].[raw_data_source] 
                    ADD [last_processed_timestamp] DATETIME2 NULL;
                END
            """)
            print("  ✅ last_processed_timestampカラムを追加しました")
        except Exception as e:
            print(f"  ⚠️ カラム追加エラー（既に存在する可能性）: {e}")
        
        # 2. data_quality_metricsテーブルを作成
        print("🔧 data_quality_metricsテーブルを作成...")
        try:
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='data_quality_metrics')
                BEGIN
                    CREATE TABLE [dbo].[data_quality_metrics] (
                        [metric_id] INT IDENTITY(1,1) PRIMARY KEY,
                        [table_name] NVARCHAR(100) NOT NULL,
                        [metric_type] NVARCHAR(50) NOT NULL,
                        [metric_value] DECIMAL(18,4) NOT NULL,
                        [measured_at] DATETIME2 DEFAULT GETDATE(),
                        [created_at] DATETIME2 DEFAULT GETDATE()
                    );
                END
            """)
            print("  ✅ data_quality_metricsテーブルを作成しました")
        except Exception as e:
            print(f"  ⚠️ テーブル作成エラー（既に存在する可能性）: {e}")
        
        # 3. processing_logsテーブルを作成
        print("🔧 processing_logsテーブルを作成...")
        try:
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='processing_logs')
                BEGIN
                    CREATE TABLE [dbo].[processing_logs] (
                        [log_id] INT IDENTITY(1,1) PRIMARY KEY,
                        [process_name] NVARCHAR(100) NOT NULL,
                        [start_time] DATETIME2 NOT NULL,
                        [end_time] DATETIME2 NULL,
                        [status] NVARCHAR(20) NOT NULL,
                        [records_processed] INT DEFAULT 0,
                        [error_message] NVARCHAR(MAX) NULL,
                        [created_at] DATETIME2 DEFAULT GETDATE()
                    );
                END
            """)
            print("  ✅ processing_logsテーブルを作成しました")
        except Exception as e:
            print(f"  ⚠️ テーブル作成エラー（既に存在する可能性）: {e}")
        
        # 4. etl_configurationテーブルを作成
        print("🔧 etl_configurationテーブルを作成...")
        try:
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='etl_configuration')
                BEGIN
                    CREATE TABLE [dbo].[etl_configuration] (
                        [config_id] INT IDENTITY(1,1) PRIMARY KEY,
                        [config_key] NVARCHAR(100) NOT NULL UNIQUE,
                        [config_value] NVARCHAR(MAX) NOT NULL,
                        [description] NVARCHAR(500) NULL,
                        [is_active] BIT DEFAULT 1,
                        [created_at] DATETIME2 DEFAULT GETDATE(),
                        [updated_at] DATETIME2 DEFAULT GETDATE()
                    );
                END
            """)
            print("  ✅ etl_configurationテーブルを作成しました")
        except Exception as e:
            print(f"  ⚠️ テーブル作成エラー（既に存在する可能性）: {e}")
        
        # 5. audit_trailテーブルを作成
        print("🔧 audit_trailテーブルを作成...")
        try:
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='audit_trail')
                BEGIN
                    CREATE TABLE [dbo].[audit_trail] (
                        [audit_id] INT IDENTITY(1,1) PRIMARY KEY,
                        [table_name] NVARCHAR(100) NOT NULL,
                        [operation] NVARCHAR(20) NOT NULL,
                        [record_id] NVARCHAR(50) NULL,
                        [old_values] NVARCHAR(MAX) NULL,
                        [new_values] NVARCHAR(MAX) NULL,
                        [user_id] NVARCHAR(100) DEFAULT 'system',
                        [timestamp] DATETIME2 DEFAULT GETDATE()
                    );
                END
            """)
            print("  ✅ audit_trailテーブルを作成しました")
        except Exception as e:
            print(f"  ⚠️ テーブル作成エラー（既に存在する可能性）: {e}")
        
        # 6. 既存テーブルにINDEX情報を追加
        print("🔧 インデックスを作成...")
        try:
            # client_dmテーブルのインデックス
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_client_dm_email')
                BEGIN
                    CREATE INDEX IX_client_dm_email ON [dbo].[client_dm] ([email]);
                END
            """)
            
            # ClientDmBxテーブルのインデックス
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_ClientDmBx_client_id')
                BEGIN
                    CREATE INDEX IX_ClientDmBx_client_id ON [dbo].[ClientDmBx] ([client_id]);
                END
            """)
            
            print("  ✅ インデックスを作成しました")
        except Exception as e:
            print(f"  ⚠️ インデックス作成エラー: {e}")
        
        # 7. テスト用のデフォルトデータを挿入
        print("🔧 テスト用データを挿入...")
        try:
            # ETL設定データ
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM [dbo].[etl_configuration] WHERE config_key = 'batch_size')
                BEGIN
                    INSERT INTO [dbo].[etl_configuration] (config_key, config_value, description)
                    VALUES ('batch_size', '1000', 'Default batch size for ETL operations');
                END
            """)
            
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM [dbo].[etl_configuration] WHERE config_key = 'max_retries')
                BEGIN
                    INSERT INTO [dbo].[etl_configuration] (config_key, config_value, description)
                    VALUES ('max_retries', '3', 'Maximum number of retries for failed operations');
                END
            """)
            
            print("  ✅ テスト用データを挿入しました")
        except Exception as e:
            print(f"  ⚠️ データ挿入エラー: {e}")
        
        conn.commit()
        conn.close()
        
        print("\n🎉 E2Eテスト環境の修正が完了しました！")
        
    except Exception as e:
        print(f"❌ エラーが発生しました: {e}")

if __name__ == "__main__":
    fix_e2e_test_issues()
