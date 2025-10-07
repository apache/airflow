#!/usr/bin/env python3
"""
Simple test script to verify DAG tag validation functionality.
This bypasses the complex test infrastructure and tests the core functionality directly.
"""

import sys
import os
from datetime import datetime

# Add the task-sdk source to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'task-sdk', 'src'))

try:
    from airflow.sdk.definitions.dag import DAG, TAG_MAX_LEN
    print(f"✓ Successfully imported DAG class")
    print(f"✓ TAG_MAX_LEN = {TAG_MAX_LEN}")
except ImportError as e:
    print(f"✗ Failed to import DAG: {e}")
    sys.exit(1)

def test_tag_validation():
    """Test the tag validation functionality."""
    print("\n=== Testing DAG Tag Validation ===")
    
    # Test 1: Tag with exactly 100 characters should be allowed
    print("\n1. Testing tag with exactly 100 characters...")
    try:
        dag = DAG(
            dag_id="test_dag_100",
            start_date=datetime(2021, 1, 1),
            tags=["a" * TAG_MAX_LEN]
        )
        print(f"✓ PASS: Tag with {TAG_MAX_LEN} characters was accepted")
        print(f"  Tag length: {len(list(dag.tags)[0])}")
    except Exception as e:
        print(f"✗ FAIL: Tag with {TAG_MAX_LEN} characters was rejected: {e}")
        return False
    
    # Test 2: Tag with 101 characters should raise ValueError
    print("\n2. Testing tag with 101 characters...")
    try:
        dag = DAG(
            dag_id="test_dag_101",
            start_date=datetime(2021, 1, 1),
            tags=["a" * (TAG_MAX_LEN + 1)]
        )
        print(f"✗ FAIL: Tag with {TAG_MAX_LEN + 1} characters was accepted (should have been rejected)")
        return False
    except ValueError as e:
        error_msg = str(e)
        print(f"✓ PASS: Tag with {TAG_MAX_LEN + 1} characters was rejected")
        print(f"  Error message: {error_msg}")
        
        # Check if error message contains expected content
        if f"{TAG_MAX_LEN + 1} characters long" in error_msg and f"maximum limit of {TAG_MAX_LEN} characters" in error_msg:
            print(f"✓ PASS: Error message contains expected content")
        else:
            print(f"✗ FAIL: Error message doesn't contain expected content")
            return False
    except Exception as e:
        print(f"✗ FAIL: Unexpected exception type: {type(e).__name__}: {e}")
        return False
    
    # Test 3: Multiple tags with one too long
    print("\n3. Testing multiple tags with one too long...")
    try:
        dag = DAG(
            dag_id="test_dag_multiple",
            start_date=datetime(2021, 1, 1),
            tags=["short", "a" * (TAG_MAX_LEN + 1), "another_short"]
        )
        print(f"✗ FAIL: Multiple tags with one too long was accepted (should have been rejected)")
        return False
    except ValueError as e:
        error_msg = str(e)
        print(f"✓ PASS: Multiple tags with one too long was rejected")
        print(f"  Error message: {error_msg}")
        
        # Check if error message contains expected content
        if f"{TAG_MAX_LEN + 1} characters long" in error_msg and f"maximum limit of {TAG_MAX_LEN} characters" in error_msg:
            print(f"✓ PASS: Error message contains expected content")
        else:
            print(f"✗ FAIL: Error message doesn't contain expected content")
            return False
    except Exception as e:
        print(f"✗ FAIL: Unexpected exception type: {type(e).__name__}: {e}")
        return False
    
    # Test 4: Very long tag preview trimming
    print("\n4. Testing very long tag preview trimming...")
    long_tag = "a" * 200
    try:
        dag = DAG(
            dag_id="test_dag_long",
            start_date=datetime(2021, 1, 1),
            tags=[long_tag]
        )
        print(f"✗ FAIL: Very long tag was accepted (should have been rejected)")
        return False
    except ValueError as e:
        error_msg = str(e)
        print(f"✓ PASS: Very long tag was rejected")
        print(f"  Error message: {error_msg}")
        
        # Check if error message contains trimmed preview
        if "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa..." in error_msg and "200 characters long" in error_msg:
            print(f"✓ PASS: Error message contains trimmed preview")
        else:
            print(f"✗ FAIL: Error message doesn't contain expected trimmed preview")
            return False
    except Exception as e:
        print(f"✗ FAIL: Unexpected exception type: {type(e).__name__}: {e}")
        return False
    
    print("\n=== All Tests Passed! ===")
    return True

if __name__ == "__main__":
    success = test_tag_validation()
    sys.exit(0 if success else 1)
