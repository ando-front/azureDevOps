#!/usr/bin/env python3
import pyodbc
print(f"Available ODBC drivers: {pyodbc.drivers()}")
import os
import time
import sys

def check_db_connection():
    # 環境変数が設定されていない場合にデフォルト値を使用するように修正
    host = os.getenv('SQL_SERVER_HOST', 'sql-server')
    database = os.getenv('SQL_SERVER_DATABASE', 'master')
    user = os.getenv('SQL_SERVER_USER', 'sa')
    password = os.getenv('SQL_SERVER_PASSWORD', 'YourStrong!Passw0rd123')
    port = os.getenv('SQL_SERVER_PORT', '1433')
    timeout = int(os.getenv('E2E_DB_TIMEOUT', '60'))
    retry_interval = int(os.getenv('E2E_DB_RETRY_INTERVAL', '10'))

    # 接続文字列: 暗号化を無効化してSQL Serverに接続
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host},{port};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        "Encrypt=no;"
        "LoginTimeout=10;"
    )
    
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            print(f"Attempting to connect to SQL Server at {host}:{port}...")
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            print(f"Successfully connected to SQL Server and database {database}.")

            # Check for the existence of ClientDmBx table
            print(f"Checking for existence of ClientDmBx table in {database}...")
            cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'ClientDmBx'")
            if cursor.fetchone()[0] > 0:
                print("ClientDmBx table found. Database and required tables are fully ready.")
                conn.close()
                return 0
            else:
                print("ClientDmBx table not found. Retrying...")
                conn.close()
                time.sleep(retry_interval)
                continue
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            print(f"Connection failed. SQLSTATE: {sqlstate}. Retrying in {retry_interval} seconds...")
            time.sleep(retry_interval)
        except Exception as e:
            print(f"An unexpected error occurred: {e}. Retrying in {retry_interval} seconds...")
            time.sleep(retry_interval)
    
    print(f"SQL Server or SynapseTestDB did not become ready within the timeout period of {timeout} seconds.")
    return 1

if __name__ == "__main__":
    sys.exit(check_db_connection())
