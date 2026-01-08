
import sys
import os
from unittest.mock import MagicMock

# Mock problematic modules BEFORE any airflow imports
sys.modules["git"] = MagicMock()
sys.modules["airflow.providers.git"] = MagicMock()
sys.modules["airflow.providers.git.bundles"] = MagicMock()
sys.modules["airflow.providers.git.bundles.git"] = MagicMock()

# Add paths to sys.path programmatically
base_path = "/Users/subhamsangwan/airflow"
sys.path.insert(0, os.path.join(base_path, "airflow-core/src"))
sys.path.insert(0, os.path.join(base_path, "task-sdk/src"))
sys.path.insert(0, os.path.join(base_path, "devel-common/src"))
sys.path.insert(0, os.path.join(base_path, "providers/standard/src"))

# Build providers path
for root, dirs, files in os.walk(os.path.join(base_path, "providers")):
    if "src" in dirs:
        sys.path.append(os.path.join(root, "src"))

import pytest

if __name__ == "__main__":
    # Ensure we are in the right directory
    os.chdir(os.path.join(base_path, "airflow-core"))
    
    # Run the test
    exit_code = pytest.main(["tests/unit/api_fastapi/execution_api/versions/head/test_access_control.py"])
    sys.exit(exit_code)
