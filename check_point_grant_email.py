import pyodbc

conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost,1433;DATABASE=SynapseTestDB;UID=sa;PWD=YourStrong!Passw0rd123;TrustServerCertificate=yes;')
cursor = conn.cursor()

cursor.execute("SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'point_grant_email' AND TABLE_SCHEMA = 'dbo' ORDER BY ORDINAL_POSITION")
columns = cursor.fetchall()

print('point_grant_email テーブル構造:')
for col in columns:
    print(f'  {col[0]} ({col[1]}) - NULL許可: {col[2]}')

conn.close()
