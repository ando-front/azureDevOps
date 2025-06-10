#!/usr/bin/env python3
"""
パイプライン実行ログテーブルの内容を確認するスクリプト
"""

import pyodbc

def check_pipeline_logs():
    """パイプライン実行ログテーブルの内容を確認"""
    try:
        # 接続文字列（履歴展開エラー回避のため、パスワードを直接指定）
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
        
        print("📊 パイプライン実行ログテーブルの内容確認:")
        
        # レコード数をカウント
        cursor.execute("SELECT COUNT(*) as count FROM dbo.pipeline_execution_log")
        count = cursor.fetchone()[0]
        print(f"  レコード数: {count}")
        
        if count > 0:
            print("  最新の3件:")
            cursor.execute("SELECT TOP 3 * FROM dbo.pipeline_execution_log ORDER BY log_id DESC")
            results = cursor.fetchall()
            
            for row in results:
                print(f"    ID:{row[0]} Pipeline:{row[1]} Start:{row[2]} Status:{row[4]}")
        else:
            print("  ⚠️ パイプライン実行ログが記録されていません")
        
        conn.close()
        print("\n🔚 データベース接続を閉じました")
        
    except Exception as e:
        print(f"❌ エラーが発生しました: {e}")

if __name__ == "__main__":
    check_pipeline_logs()
