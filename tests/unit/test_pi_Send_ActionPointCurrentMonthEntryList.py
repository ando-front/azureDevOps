"""
ADFパイプライン: pi_Send_ActionPointCurrentMonthEntryList のユニットテスト。
パイプライン名・アクティビティ・Copyアクティビティ内容の検証を行う。
"""

from .normalize_column import normalize_column_name
import requests
from .helpers.sql_column_extractor import extract_normalized_columns
from .helpers.synapse_test_helper import SynapseTestConnection, verify_synapse_connection
import unittest
import json
import os
import re
import copy
import pytest

class TestPiSendActionPointCurrentMonthEntryList(unittest.TestCase):

    @classmethod
    def setup_class(cls):
        """Disable proxy settings for tests"""
        # Store and clear proxy environment variables
        for var in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY']:
            if var in os.environ:
                del os.environ[var]

    def _get_no_proxy_session(self):
        """Get a requests session with proxy disabled"""
        session = requests.Session()
        session.proxies = {'http': None, 'https': None}
        return session
    
    @classmethod
    def setUpClass(cls):
        # 詳細なデバッグ情報を出力
        print(f"[DEBUG] Current working directory: {os.getcwd()}")
        print(f"[DEBUG] PYTHONPATH: {os.environ.get('PYTHONPATH', 'Not set')}")
        print(f"[DEBUG] Checking for pipeline file...")
        
        # より包括的なパス候補
        base_paths = [
            # Docker環境での優先パス
            "/tests/src/dev/pipeline/pi_Send_ActionPointCurrentMonthEntryList.json",
            "/app/src/dev/pipeline/pi_Send_ActionPointCurrentMonthEntryList.json",
            # 相対パス候補（現在のディレクトリから）
            "src/dev/pipeline/pi_Send_ActionPointCurrentMonthEntryList.json",
            "../../src/dev/pipeline/pi_Send_ActionPointCurrentMonthEntryList.json",
            "../../../src/dev/pipeline/pi_Send_ActionPointCurrentMonthEntryList.json",
            # ワークスペースルートからの相対パス
            os.path.join(os.getcwd(), "src", "dev", "pipeline", "pi_Send_ActionPointCurrentMonthEntryList.json"),
            os.path.join(os.path.dirname(os.getcwd()), "src", "dev", "pipeline", "pi_Send_ActionPointCurrentMonthEntryList.json"),
            # 絶対パス候補
            os.path.join("/tests", "src", "dev", "pipeline", "pi_Send_ActionPointCurrentMonthEntryList.json"),
            os.path.join("/app", "src", "dev", "pipeline", "pi_Send_ActionPointCurrentMonthEntryList.json"),
        ]
        
        # パスの存在確認と詳細ログ
        for i, path in enumerate(base_paths):
            print(f"[DEBUG] Checking path {i+1}: {path}")
            if os.path.exists(path):
                print(f"[DEBUG] ✓ Found pipeline file at: {path}")
                cls.json_path = path
                with open(path, encoding="utf-8") as f:
                    cls.pipeline = json.load(f)
                return
            else:
                print(f"[DEBUG] ✗ Not found: {path}")
        
        # ファイルが見つからない場合の詳細ディレクトリ構造表示
        print(f"[ERROR] Pipeline file not found. Exploring directory structure...")
        print(f"[DEBUG] Current directory contents:")
        try:
            current_files = os.listdir(os.getcwd())
            for f in current_files[:10]:  # 最初の10個のみ表示
                print(f"  - {f}")
        except Exception as e:
            print(f"  Error listing current directory: {e}")
        
        # 重要なディレクトリの存在確認
        important_dirs = ["/tests", "/app", "/tests/src", "/app/src", "/tests/src/dev", "/app/src/dev"]
        for dir_path in important_dirs:
            if os.path.exists(dir_path):
                print(f"[DEBUG] Directory exists: {dir_path}")
                try:
                    contents = os.listdir(dir_path)
                    print(f"  Contents: {contents[:5]}...")  # 最初の5個のみ
                except Exception as e:
                    print(f"  Error listing {dir_path}: {e}")
            else:
                print(f"[DEBUG] Directory missing: {dir_path}")
        
        raise FileNotFoundError(f"Pipeline file not found in any of these paths: {base_paths}")

    def test_pipeline_name(self):
        print("[INFO] パイプライン名テスト")
        name = self.pipeline["name"]
        self.assertIn("pi_Send_ActionPointCurrentMonthEntryList", name)

@classmethod
def setup_class(cls):
    """Disable proxy settings for tests"""
    # Store and clear proxy environment variables
    for var in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY']:
        if var in os.environ:
            del os.environ[var]

def _get_no_proxy_session(self):
    """Get a requests session with proxy disabled"""
    session = requests.Session()
    session.proxies = {'http': None, 'https': None}
    return session

    def test_activities_exist(self):
        print("[INFO] activities数テスト")
        activities = self.pipeline["properties"]["activities"]
        self.assertGreaterEqual(len(activities), 2)

    def test_first_activity_copy(self):
        print("[INFO] 1つ目のCopyアクティビティ内容テスト")
        activities = self.pipeline["properties"]["activities"]
        first_activity = activities[0]
        self.assertEqual(first_activity["type"], "Copy")


class TestActionPointEntryDatabase(unittest.TestCase):
    """ActionPointEntryテーブルのデータベーステスト"""
    
    @classmethod
    def setUpClass(cls):
        cls.synapse_connection = SynapseTestConnection()
    
    def test_synapse_connection(self):
        """Synapse Analytics接続テスト"""
        self.assertTrue(verify_synapse_connection(), "Synapse Analytics (SQL Server)への接続に失敗しました")
    
    def test_action_point_entry_table_exists(self):
        """ActionPointEntryテーブルの存在確認"""
        query = """
        SELECT COUNT(*) as table_count 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME = 'ActionPointEntry' AND TABLE_SCHEMA = 'dbo'
        """
        result = self.synapse_connection.execute_query(query)
        self.assertEqual(result[0][0], 1, "ActionPointEntryテーブルが存在しません")
    
    def test_action_point_entry_insert(self):
        """ActionPointEntryテーブルへのデータ挿入テスト"""
        # テスト用のClientDmデータを準備
        client_insert_query = """
        INSERT INTO dbo.ClientDm (ClientName, ClientCode, Status) 
        VALUES (?, ?, ?)
        """
        self.synapse_connection.execute_query(client_insert_query, ('アクションポイントテスト', 'APE001', 'Active'))
        
        # ClientIdを取得
        client_select_query = "SELECT ClientId FROM dbo.ClientDm WHERE ClientCode = ?"
        client_result = self.synapse_connection.execute_query(client_select_query, ('APE001',))
        client_id = client_result[0][0]
        
        # ActionPointEntryにデータを挿入
        insert_query = """
        INSERT INTO dbo.ActionPointEntry (ClientId, ActionType, PointAmount, CurrentMonth) 
        VALUES (?, ?, ?, ?)
        """
        params = (client_id, 'TestAction', 750.00, 1)
        rows_affected = self.synapse_connection.execute_query(insert_query, params)
        self.assertEqual(rows_affected, 1, "ActionPointEntryデータの挿入に失敗しました")
        
        # 挿入されたデータの確認
        select_query = """
        SELECT ape.ActionType, ape.PointAmount, ape.CurrentMonth, cd.ClientName
        FROM dbo.ActionPointEntry ape
        INNER JOIN dbo.ClientDm cd ON ape.ClientId = cd.ClientId
        WHERE cd.ClientCode = ?
        """
        result = self.synapse_connection.execute_query(select_query, ('APE001',))
        self.assertGreaterEqual(len(result), 1, "挿入されたActionPointEntryデータが見つかりません")
        
        # 最後に挿入されたレコードを確認
        for row in result:
            if row[0] == 'TestAction':
                self.assertEqual(row[0], 'TestAction', "アクションタイプが一致しません")
                self.assertEqual(float(row[1]), 750.00, "ポイント額が一致しません")
                self.assertEqual(row[2], True, "当月フラグが一致しません")
                self.assertEqual(row[3], 'アクションポイントテスト', "関連するクライアント名が一致しません")
                break
        
        # テストデータのクリーンアップ
        cleanup_query = "DELETE FROM dbo.ActionPointEntry WHERE ClientId = ?"
        self.synapse_connection.execute_query(cleanup_query, (client_id,))
        cleanup_client_query = "DELETE FROM dbo.ClientDm WHERE ClientId = ?"
        self.synapse_connection.execute_query(cleanup_client_query, (client_id,))
    
    def test_current_month_filter(self):
        """当月データのフィルタリングテスト"""
        # テスト用のClientDmデータを準備
        client_insert_query = """
        INSERT INTO dbo.ClientDm (ClientName, ClientCode, Status) 
        VALUES (?, ?, ?)
        """
        self.synapse_connection.execute_query(client_insert_query, ('当月フィルタテスト', 'CMF001', 'Active'))
        
        client_select_query = "SELECT ClientId FROM dbo.ClientDm WHERE ClientCode = ?"
        client_result = self.synapse_connection.execute_query(client_select_query, ('CMF001',))
        client_id = client_result[0][0]
        
        # 当月データと前月データを挿入
        insert_query = """
        INSERT INTO dbo.ActionPointEntry (ClientId, ActionType, PointAmount, CurrentMonth) 
        VALUES (?, ?, ?, ?)
        """
        self.synapse_connection.execute_query(insert_query, (client_id, 'CurrentMonth', 500.00, 1))
        self.synapse_connection.execute_query(insert_query, (client_id, 'PreviousMonth', 300.00, 0))
        
        # 当月データのみを取得
        select_query = """
        SELECT ActionType, PointAmount 
        FROM dbo.ActionPointEntry 
        WHERE ClientId = ? AND CurrentMonth = 1
        """
        result = self.synapse_connection.execute_query(select_query, (client_id,))
        self.assertEqual(len(result), 1, "当月データのフィルタリングが正しく動作していません")
        self.assertEqual(result[0][0], 'CurrentMonth', "当月データのアクションタイプが一致しません")
        
        # テストデータのクリーンアップ
        cleanup_query = "DELETE FROM dbo.ActionPointEntry WHERE ClientId = ?"
        self.synapse_connection.execute_query(cleanup_query, (client_id,))
        cleanup_client_query = "DELETE FROM dbo.ClientDm WHERE ClientId = ?"
        self.synapse_connection.execute_query(cleanup_client_query, (client_id,))
