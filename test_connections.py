#!/usr/bin/env python3
"""
Test script to verify the test connection functionality works
"""
import requests
import json
import time

def test_connection_endpoint():
    """Test the connection test endpoint directly"""
    base_url = "http://localhost:8080"
    
    # Test success connection
    print("🧪 Testing success connection...")
    success_response = requests.post(
        f"{base_url}/api/v2/connections/test",
        params={"connection_id": "test_connection_success"},
        headers={"Content-Type": "application/json"}
    )
    
    if success_response.status_code == 200:
        success_data = success_response.json()
        print(f"✅ Success connection: {success_data}")
    else:
        print(f"❌ Success connection failed: {success_response.status_code} - {success_response.text}")
    
    # Test failure connection
    print("\n🧪 Testing failure connection...")
    failure_response = requests.post(
        f"{base_url}/api/v2/connections/test",
        params={"connection_id": "test_connection_failure"},
        headers={"Content-Type": "application/json"}
    )
    
    if failure_response.status_code == 200:
        failure_data = failure_response.json()
        print(f"✅ Failure connection: {failure_data}")
    else:
        print(f"❌ Failure connection failed: {failure_response.status_code} - {failure_response.text}")

if __name__ == "__main__":
    print("🚀 Testing Airflow connection endpoints...")
    test_connection_endpoint()

