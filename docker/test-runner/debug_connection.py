
import pyodbc
import os

print("Attempting to connect to SQL Server...")

server = os.environ.get("DB_SERVER", "localhost")
database = os.environ.get("DB_NAME", "master")
username = os.environ.get("DB_USER", "sa")
password = os.environ.get("DB_PASSWORD", "yourStrongPassword123")

connection_string = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={server},1433;"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password};"
    f"TrustServerCertificate=yes;"
)

try:
    cnxn = pyodbc.connect(connection_string)
    cursor = cnxn.cursor()
    cursor.execute("SELECT @@VERSION;")
    row = cursor.fetchone()
    print(f"Successfully connected! SQL Server Version: {row[0]}")
    cnxn.close()
except pyodbc.Error as ex:
    sqlstate = ex.args[0]
    print(f"Connection failed! SQLSTATE: {sqlstate}")
    print(f"Error details: {ex}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")

print("Debug script finished.")
