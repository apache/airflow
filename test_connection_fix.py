#!/usr/bin/env python3
"""
Test script to verify the test connection response display fix.

This script helps test the fix by:
1. Starting the Airflow development server
2. Opening the connections page
3. Testing various connection scenarios
4. Verifying that error/success messages are displayed

Usage:
    python test_connection_fix.py
"""

import subprocess
import time
import webbrowser
import os
import sys
from pathlib import Path

def run_command(cmd, cwd=None):
    """Run a command and return the result."""
    print(f"Running: {cmd}")
    try:
        result = subprocess.run(cmd, shell=True, cwd=cwd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
            return False
        print(f"Success: {result.stdout}")
        return True
    except Exception as e:
        print(f"Exception: {e}")
        return False

def check_dependencies():
    """Check if required dependencies are installed."""
    print("Checking dependencies...")
    
    # Check if we're in the right directory
    if not os.path.exists("airflow-core"):
        print("Error: Please run this script from the airflow root directory")
        return False
    
    # Check if uv is installed
    if not run_command("which uv"):
        print("Error: uv is not installed. Please install it first.")
        return False
    
    return True

def setup_environment():
    """Set up the development environment."""
    print("Setting up environment...")
    
    # Navigate to airflow-core directory
    os.chdir("airflow-core")
    
    # Install dependencies
    print("Installing dependencies...")
    if not run_command("uv sync"):
        print("Error: Failed to install dependencies")
        return False
    
    return True

def start_airflow():
    """Start the Airflow development server."""
    print("Starting Airflow...")
    
    # Set environment variables
    env = os.environ.copy()
    env["AIRFLOW__CORE__TEST_CONNECTION"] = "Enabled"
    env["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
    env["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = "sqlite:///airflow.db"
    
    # Start Airflow webserver
    print("Starting Airflow webserver...")
    webserver_process = subprocess.Popen(
        ["uv", "run", "airflow", "webserver", "--port", "8080"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    # Wait a bit for the server to start
    print("Waiting for server to start...")
    time.sleep(10)
    
    return webserver_process

def test_connections():
    """Test the connections functionality."""
    print("Testing connections...")
    
    # Open the connections page
    url = "http://localhost:8080/connections"
    print(f"Opening {url}")
    webbrowser.open(url)
    
    print("\n" + "="*50)
    print("TESTING INSTRUCTIONS:")
    print("="*50)
    print("1. The connections page should open in your browser")
    print("2. Look for the 'Test' button next to any connection")
    print("3. Click the 'Test' button and observe:")
    print("   - The button should show a loading state")
    print("   - After completion, hover over the button to see the tooltip")
    print("   - You should see a message overlay below the button")
    print("   - The button icon should change (green for success, red for failure)")
    print("4. Try testing different connections:")
    print("   - Valid connections should show success messages")
    print("   - Invalid connections should show error messages")
    print("5. Check the browser console for any error messages")
    print("\nExpected behavior:")
    print("- Success: Green wifi icon + success message in tooltip/overlay")
    print("- Failure: Red wifi-off icon + error message in tooltip/overlay")
    print("- Messages should be visible both in tooltip and as overlay")
    print("="*50)

def main():
    """Main function."""
    print("Test Connection Response Display Fix")
    print("===================================")
    
    if not check_dependencies():
        sys.exit(1)
    
    if not setup_environment():
        sys.exit(1)
    
    try:
        webserver_process = start_airflow()
        test_connections()
        
        print("\nPress Ctrl+C to stop the server and exit...")
        try:
            webserver_process.wait()
        except KeyboardInterrupt:
            print("\nStopping server...")
            webserver_process.terminate()
            webserver_process.wait()
            print("Server stopped.")
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
