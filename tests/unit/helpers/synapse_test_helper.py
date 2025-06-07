"""
Synapse Analytics (SQL Server) テスト用のデータベース接続・クエリ実行のモック実装。
ユニットテストでDB依存を排除し、各種クエリパターンに応じた模擬応答を返す。
"""
import os
import pytest
from typing import Generator, Any


class MockSynapseConnection:
    """Synapse Analytics テスト用の軽量モッククラス"""
    
    def __init__(self):
        self.server = os.getenv('SQL_SERVER_HOST', 'localhost')
        self.port = os.getenv('SQL_SERVER_PORT', '1433')
        self.database = os.getenv('SQL_SERVER_DATABASE', 'SynapseTestDB')
        self.username = os.getenv('SQL_SERVER_USER', 'sa')
        self.password = os.getenv('SQL_SERVER_PASSWORD', 'YourStrong!Passw0rd')
        
        # モック用の接続文字列（実際の接続は行わない）
        self.connection_string = (        f"Mock connection to {self.server}:{self.port}/{self.database}"
        )
        
    def get_connection(self):
        """モック接続を返す"""
        return MockConnection()
    
    def execute_query(self, query: str, params: tuple = None):
        """モッククエリ実行"""
        print(f"Mock executing query: {query}")
        if params:
            print(f"With parameters: {params}")
        
        # SELECT文の場合は適切なモックデータを返す
        if query.strip().upper().startswith('SELECT'):
            # テーブル存在確認クエリ
            if 'INFORMATION_SCHEMA.TABLES' in query.upper():
                return [(1,)]  # テーブルが存在することを示す
            # COUNT関数の場合
            elif 'COUNT(*)' in query.upper():
                return [(1,)]  # 1件存在することを示す            # ClientId取得クエリ            elif 'SELECT ClientId FROM' in query and params:
                if any('PGE001' in str(p) for p in params):
                    return [(100,)]  # ClientId
                elif any('AUTO001' in str(p) for p in params):
                    return [(1,)]
                elif any('UPD001' in str(p) for p in params):
                    return [(2,)]
                elif any('CMF001' in str(p) for p in params):
                    return [(1,)]  # 当月フィルタテスト用
                else:
                    return [(1,)]
            # 当月データフィルタリング
            elif 'CurrentMonth = 1' in query and params:
                # 当月データのみを返す
                return [('CurrentMonth', 500.0)]
            # 結合クエリ（JOIN）の処理
            elif 'JOIN' in query.upper() and params:
                if any('test.pge@example.com' in str(p) for p in params):
                    return [('test.pge@example.com', 1500.0, 'Processing', 'ポイント付与テスト')]
                else:
                    return [('mock_email@example.com', 1000.0, 'Active', 'Mock Client')]
            # 特定の条件に基づくデータ選択
            elif params and any('AUTO001' in str(p) for p in params):
                return [('テストクライアント_自動', 'AUTO001', 'Active')]
            elif params and any('UPD001' in str(p) for p in params):
                # 更新後の状態を想定
                if 'Status' in query:
                    return [('Inactive',)]  # 更新されたステータス
                else:
                    return [('更新テスト', 'UPD001', 'Inactive')]
            else:
                return [('mock_data_1',), ('mock_data_2',)]
        else:
            return 1  # 影響を受けた行数のモック
    
    def execute_script(self, script_path: str):
        """モックスクリプト実行"""
        print(f"Mock executing script: {script_path}")
        return True


class MockConnection:
    """モック接続クラス"""
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
    
    def cursor(self):
        return MockCursor()
    
    def commit(self):
        pass


class MockCursor:
    """モックカーソルクラス"""
    
    def __init__(self):
        self._last_query = ""
        self._last_params = None
    
    def execute(self, query: str, params: tuple = None):
        print(f"Mock cursor executing: {query}")
        self._last_query = query
        self._last_params = params
        
    def fetchall(self):
        # クエリの内容に基づいて適切なデータを返す
        if 'INFORMATION_SCHEMA.TABLES' in self._last_query.upper():
            return [(1,)]  # テーブルが存在
        elif 'COUNT(*)' in self._last_query.upper():
            return [(1,)]  # 1件存在
        elif 'SELECT ClientId FROM' in self._last_query and self._last_params:
            if any('PGE001' in str(p) for p in self._last_params):
                return [(100,)]
            elif any('AUTO001' in str(p) for p in self._last_params):
                return [(1,)]
            elif any('UPD001' in str(p) for p in self._last_params):
                return [(2,)]
            else:
                return [(1,)]
        elif 'JOIN' in self._last_query.upper() and self._last_params:
            if any('test.pge@example.com' in str(p) for p in self._last_params):
                return [('test.pge@example.com', 1500.0, 'Processing', 'ポイント付与テスト')]
            else:
                return [('mock_email@example.com', 1000.0, 'Active', 'Mock Client')]
        elif self._last_params and any('AUTO001' in str(p) for p in self._last_params):
            return [('テストクライアント_自動', 'AUTO001', 'Active')]
        elif self._last_params and any('UPD001' in str(p) for p in self._last_params):
            if 'Status' in self._last_query:
                return [('Inactive',)]
            else:
                return [('更新テスト', 'UPD001', 'Inactive')]
        else:
            return [('mock_row_1',), ('mock_row_2',)]
    
    @property
    def rowcount(self):
        return 1


# 後方互換性のためのエイリアス
SynapseTestConnection = MockSynapseConnection


@pytest.fixture
def synapse_connection() -> Generator[MockSynapseConnection, None, None]:
    """Synapse Analytics (SQL Server) モック接続のフィクスチャ"""
    connection_helper = MockSynapseConnection()
    yield connection_helper


@pytest.fixture
def clean_test_data(synapse_connection: MockSynapseConnection):
    """テストデータをクリーンアップするモックフィクスチャ"""
    yield
    
    # モッククリーンアップ（実際の処理は行わない）
    print("Mock cleanup completed")


def verify_synapse_connection():
    """Synapse接続の確認（モック版）"""
    try:
        connection_helper = MockSynapseConnection()
        result = connection_helper.execute_query("SELECT 1 as test")
        return len(result) > 0
    except Exception as e:
        print(f"Mock Synapse接続エラー: {e}")
        return False
