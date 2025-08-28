# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Global pytest configuration for SeaTunnel provider tests.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

# Add the provider package to Python path
provider_root = Path(__file__).parent.parent
sys.path.insert(0, str(provider_root))

# Set up test environment
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS", "False")


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test requiring external services"
    )
    config.addinivalue_line("markers", "slow: mark test as slow running")
    config.addinivalue_line("markers", "requires_docker: mark test as requiring Docker")
    config.addinivalue_line("markers", "unit: mark test as unit test (default)")


def pytest_collection_modifyitems(config, items):
    """Automatically mark tests based on their location."""
    for item in items:
        # Mark integration tests
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
        else:
            item.add_marker(pytest.mark.unit)


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Set up the test environment."""
    # Ensure we're using test configuration
    os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"

    # Set up minimal Airflow configuration for testing
    os.environ.setdefault("AIRFLOW__CORE__EXECUTOR", "SequentialExecutor")
    os.environ.setdefault("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "sqlite:///:memory:")

    yield

    # Cleanup after all tests
    pass
