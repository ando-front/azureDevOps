import pyodbc
import os
import time
import sys

def check_db_connection():
    host = os.getenv('SQL_SERVER_HOST')
    database = os.getenv('SQL_SERVER_DATABASE')
    user = os.getenv('SQL_SERVER_USER')
    password = os.getenv('SQL_SERVER_PASSWORD')
    port = os.getenv('SQL_SERVER_PORT', '1433')
    timeout = int(os.getenv('E2E_DB_TIMEOUT', '60'))
    retry_interval = int(os.getenv('E2E_DB_RETRY_INTERVAL', '10'))

    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={host},{port};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        f"TrustServerCertificate=yes;"
        f"Encrypt=no;"
        f"LoginTimeout=10;"
    )

    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            print(f"Attempting to connect to SQL Server at {host}:{port}...")
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            print("SQL Server and SynapseTestDB are fully ready.")
            conn.close()
            return 0
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
