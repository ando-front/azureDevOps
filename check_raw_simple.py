import pyodbc

# 接続
conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost,1433;DATABASE=TGMATestDB;UID=sa;PWD=YourStrong!Passw0rd123;TrustServerCertificate=yes;Encrypt=no;')
cursor = conn.cursor()

# テーブル構造を確認
cursor.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='raw_data_source' ORDER BY ORDINAL_POSITION")
print('raw_data_source columns:')
for row in cursor.fetchall():
    print(f'  - {row[0]} ({row[1]})')

# データサンプルを確認
cursor.execute('SELECT TOP 3 * FROM [dbo].[raw_data_source]')
print('\nSample data:')
for i, row in enumerate(cursor.fetchall(), 1):
    print(f'  Row {i}: {row}')

conn.close()
print("Done")
