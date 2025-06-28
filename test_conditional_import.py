#!/usr/bin/env python3
"""Test pyodbc conditional import functionality"""

import sys
import os

print("🔍 Testing pyodbc conditional import functionality...\n")

# Test 1: Direct import from conftest
print("1️⃣ Testing conftest.py import...")
try:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'tests', 'e2e'))
    import conftest
    PYODBC_AVAILABLE = conftest.PYODBC_AVAILABLE
    print(f"   ✅ PYODBC_AVAILABLE: {PYODBC_AVAILABLE}")
    print(f"   ✅ MockPyodbc class available: {hasattr(conftest, 'MockPyodbc')}")
except ImportError as e:
    print(f"   ❌ Import failed: {e}")

# Test 2: Import from synapse_e2e_helper
print("\n2️⃣ Testing synapse_e2e_helper.py import...")
try:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'tests', 'e2e', 'helpers'))
    from synapse_e2e_helper import PYODBC_AVAILABLE as synapse_available
    print(f"   ✅ synapse_e2e_helper PYODBC_AVAILABLE: {synapse_available}")
except ImportError as e:
    print(f"   ❌ Import failed: {e}")

# Test 3: Import from reproducible_e2e_helper
print("\n3️⃣ Testing reproducible_e2e_helper.py import...")
try:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'tests', 'helpers'))
    from reproducible_e2e_helper import PYODBC_AVAILABLE as repro_available
    print(f"   ✅ reproducible_e2e_helper PYODBC_AVAILABLE: {repro_available}")
except ImportError as e:
    print(f"   ❌ Import failed: {e}")

# Test 4: Test MockPyodbc functionality
print("\n4️⃣ Testing MockPyodbc functionality...")
try:
    import conftest
    if hasattr(conftest, 'MockPyodbc'):
        MockPyodbc = conftest.MockPyodbc
        
        # Test connection attempt
        mock_conn = MockPyodbc.Connection()
        mock_cursor = mock_conn.cursor()
        mock_cursor.execute("SELECT 1")
        result = mock_cursor.fetchall()
        print(f"   ✅ MockPyodbc.Connection created: {mock_conn}")
        print(f"   ✅ MockPyodbc.Cursor created: {mock_cursor}")
        print(f"   ✅ MockPyodbc.fetchall() result: {result}")
        
        # Test connect method
        try:
            MockPyodbc.connect("dummy connection string")
            print("   ❌ Expected ImportError from MockPyodbc.connect()")
        except ImportError:
            print("   ✅ MockPyodbc.connect() correctly raises ImportError")
    else:
        print("   ❌ MockPyodbc not available in conftest")
        
except Exception as e:
    print(f"   ❌ MockPyodbc test failed: {e}")

print("\n✅ Conditional import testing completed!")
