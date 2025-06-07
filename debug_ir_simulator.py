#!/usr/bin/env python3
"""
IR Simulator Debug Script
403エラーの原因を調査するためのスクリプト
"""

import requests
import json

def test_ir_simulator():
    """IR Simulatorとの通信をテスト"""
    base_url = "http://localhost:8080"
    
    print("=== IR Simulator Debug Test ===")
    
    # 1. ルートエンドポイントのテスト
    print("\n1. Testing root endpoint...")
    try:
        response = requests.get(f"{base_url}/")
        print(f"Status: {response.status_code}")
        print(f"Response: {response.json()}")
    except Exception as e:
        print(f"Error: {e}")
    
    # 2. Pipeline executionエンドポイントのテスト
    print("\n2. Testing pipeline-execution endpoint...")
    
    payload = {
        "pipeline_name": "debug_test_pipeline",
        "parameters": {}
    }
    
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Python/requests"
    }
    
    try:
        print(f"Sending POST to {base_url}/pipeline-execution")
        print(f"Payload: {json.dumps(payload, indent=2)}")
        print(f"Headers: {headers}")
        
        response = requests.post(
            f"{base_url}/pipeline-execution",
            json=payload,
            headers=headers,
            timeout=30
        )
        
        print(f"Status Code: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")
        
        if response.status_code == 403:
            print("403 Forbidden - Response body:")
            print(response.text)
        else:
            print(f"Response: {response.json()}")
            
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
        print(f"Response: {e.response.text}")
    except Exception as e:
        print(f"Error: {e}")
    
    # 3. 利用可能エンドポイントの確認
    print("\n3. Testing other endpoints...")
    endpoints = ["/executions", "/test-database-status"]
    
    for endpoint in endpoints:
        try:
            response = requests.get(f"{base_url}{endpoint}")
            print(f"{endpoint}: {response.status_code}")
        except Exception as e:
            print(f"{endpoint}: Error - {e}")

if __name__ == "__main__":
    test_ir_simulator()
