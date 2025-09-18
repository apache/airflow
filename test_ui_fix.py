#!/usr/bin/env python3
"""
Simple test script to verify the test connection response display fix.

This script starts the UI development server so you can test the fix manually.
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

def main():
    """Main function."""
    print("Test Connection Response Display Fix - UI Testing")
    print("================================================")
    
    # Navigate to the UI directory
    ui_dir = Path("airflow-core/src/airflow/ui")
    if not ui_dir.exists():
        print("Error: UI directory not found")
        sys.exit(1)
    
    print(f"Changing to directory: {ui_dir}")
    os.chdir(ui_dir)
    
    # Check if node_modules exists
    if not Path("node_modules").exists():
        print("Installing dependencies...")
        if not run_command("pnpm install"):
            print("Error: Failed to install dependencies")
            sys.exit(1)
    
    # Start the development server
    print("Starting UI development server...")
    print("This will start the Vite development server on http://localhost:5173")
    print("You can then test the test connection functionality.")
    
    try:
        # Start the dev server
        dev_server = subprocess.Popen(
            ["pnpm", "dev"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Wait a bit for the server to start
        print("Waiting for server to start...")
        time.sleep(5)
        
        # Open the browser
        url = "http://localhost:5173"
        print(f"Opening {url}")
        webbrowser.open(url)
        
        print("\n" + "="*60)
        print("TESTING INSTRUCTIONS:")
        print("="*60)
        print("1. The UI should open in your browser")
        print("2. Navigate to the Connections page")
        print("3. Look for the 'Test' button next to any connection")
        print("4. Click the 'Test' button and observe:")
        print("   - The button should show a loading state")
        print("   - After completion, hover over the button to see the tooltip")
        print("   - You should see a message overlay below the button")
        print("   - The button icon should change (green for success, red for failure)")
        print("5. Try testing different connections:")
        print("   - Valid connections should show success messages")
        print("   - Invalid connections should show error messages")
        print("6. Check the browser console for any error messages")
        print("\nExpected behavior:")
        print("- Success: Green wifi icon + success message in tooltip/overlay")
        print("- Failure: Red wifi-off icon + error message in tooltip/overlay")
        print("- Messages should be visible both in tooltip and as overlay")
        print("="*60)
        
        print("\nPress Ctrl+C to stop the server and exit...")
        try:
            dev_server.wait()
        except KeyboardInterrupt:
            print("\nStopping server...")
            dev_server.terminate()
            dev_server.wait()
            print("Server stopped.")
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
