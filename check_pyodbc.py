#!/usr/bin/env python3
"""Check pyodbc availability"""

try:
    import pyodbc
    print("✅ pyodbc available")
    print(f"   Version: {pyodbc.version}")
except ImportError as e:
    print("⚠️ pyodbc not available - tests will use MockPyodbc")
    print(f"   Error: {e}")

print("\n🔍 Testing MockPyodbc fallback...")
try:
    from tests.e2e.conftest import PYODBC_AVAILABLE
    print(f"   PYODBC_AVAILABLE flag: {PYODBC_AVAILABLE}")
except ImportError as e:
    print(f"   Could not import PYODBC_AVAILABLE: {e}")
