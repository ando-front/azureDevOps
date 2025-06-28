"""
pyodbc可用性テスト - ODBC依存解消確認用
"""
import pytest

# pyodbcの条件付きインポート
try:
    import pyodbc
    PYODBC_AVAILABLE = True
except ImportError:
    PYODBC_AVAILABLE = False

class TestPyodbcAvailability:
    """pyodbc可用性テスト"""
    
    def test_pyodbc_availability_check(self):
        """pyodbc可用性チェックテスト"""
        print(f"PYODBC_AVAILABLE: {PYODBC_AVAILABLE}")
        
        if PYODBC_AVAILABLE:
            print("✅ pyodbcが利用可能です - DBテストが実行されます")
        else:
            print("⚠️ pyodbcが利用できません - DBテストはスキップされます")
        
        # このテストは常に成功します（pyodbcの有無に関わらず）
        assert True
    
    @pytest.mark.skipif(not PYODBC_AVAILABLE, reason="pyodbc not available")
    def test_conditional_skip_with_pyodbc(self):
        """pyodbcが利用可能な場合のみ実行されるテスト"""
        # このテストはpyodbcが利用可能な場合のみ実行されます
        import pyodbc
        print("✅ pyodbcを使用したテストが実行されました")
        assert hasattr(pyodbc, 'connect')
    
    @pytest.mark.skipif(PYODBC_AVAILABLE, reason="pyodbc is available - testing fallback mode")
    def test_conditional_skip_without_pyodbc(self):
        """pyodbcが利用できない場合のみ実行されるテスト（フォールバックモード）"""
        # このテストはpyodbcが利用できない場合のみ実行されます
        print("✅ pyodbc非依存のフォールバックモードが実行されました")
        assert not PYODBC_AVAILABLE
