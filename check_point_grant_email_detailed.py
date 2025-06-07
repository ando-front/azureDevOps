#!/usr/bin/env python3
"""
point_grant_emailテーブルの詳細構造確認スクリプト
"""

import os
import pyodbc
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_point_grant_email_structure():
    """point_grant_emailテーブルの詳細構造確認"""
    try:
        # 接続設定
        sql_host = os.getenv("SQL_SERVER_HOST", "localhost")
        sql_port = os.getenv("SQL_SERVER_PORT", "1433")
        sql_user = os.getenv("SQL_SERVER_USER", "sa")
        sql_password = os.getenv("SQL_SERVER_PASSWORD", "YourStrong!Passw0rd123")
        sql_database = os.getenv("SQL_SERVER_DATABASE", "SynapseTestDB")
        
        connection_string = f"""
        DRIVER={{ODBC Driver 17 for SQL Server}};
        SERVER={sql_host},{sql_port};
        DATABASE={sql_database};
        UID={sql_user};
        PWD={sql_password};
        TrustServerCertificate=yes;
        """
        
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        # point_grant_emailテーブルの存在確認
        table_exists_query = """
        SELECT COUNT(*) as table_count
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME = 'point_grant_email' AND TABLE_SCHEMA = 'dbo'
        """
        
        cursor.execute(table_exists_query)
        result = cursor.fetchone()
        table_count = result[0] if result else 0
        
        logger.info(f"point_grant_email テーブル存在確認: {table_count}個")
        
        if table_count == 0:
            logger.info("point_grant_emailテーブルが存在しません。作成します...")
            create_table_query = """
            CREATE TABLE [dbo].[point_grant_email] (
                [id] INT IDENTITY(1,1) PRIMARY KEY,
                [client_id] NVARCHAR(50) NOT NULL,
                [email] NVARCHAR(100),
                [points_granted] INT DEFAULT 0,
                [campaign_id] NVARCHAR(50),
                [created_at] DATETIME2 DEFAULT GETDATE(),
                [processed_date] DATETIME2,
                [grant_date] NVARCHAR(20)
            )
            """
            cursor.execute(create_table_query)
            conn.commit()
            logger.info("point_grant_emailテーブルを作成しました")
        
        # テーブル構造確認
        structure_query = """
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'point_grant_email' AND TABLE_SCHEMA = 'dbo'
        ORDER BY ORDINAL_POSITION
        """
        
        cursor.execute(structure_query)
        columns = cursor.fetchall()
        
        logger.info("point_grant_email テーブルのカラム構造:")
        for col in columns:
            column_name, data_type, is_nullable, column_default, max_length = col
            logger.info(f"  - {column_name}: {data_type}({max_length if max_length else 'N/A'}) "
                       f"NULL={is_nullable} DEFAULT={column_default}")
        
        # サンプルデータ確認
        sample_query = "SELECT TOP 3 * FROM [dbo].[point_grant_email]"
        try:
            cursor.execute(sample_query)
            sample_data = cursor.fetchall()
            
            if sample_data:
                logger.info("point_grant_email サンプルデータ:")
                for row in sample_data:
                    logger.info(f"  - {row}")
            else:
                logger.info("point_grant_email テーブルにデータはありません")
        except Exception as e:
            logger.warning(f"サンプルデータ取得エラー: {str(e)}")
        
        conn.close()
        
    except Exception as e:
        logger.error(f"テーブル構造確認エラー: {str(e)}")

if __name__ == "__main__":
    check_point_grant_email_structure()
