#
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
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

RESOURCE_CLEANUP_PATH = Path(__file__).resolve().parents[3] / "system" / "google" / "resources_cleanup"
sys.path.insert(0, str(RESOURCE_CLEANUP_PATH))

from airflow_google_provider_resource_cleanup import helpers  # noqa: E402


@pytest.fixture
def mock_args():
    """Default mock for argparse.Namespace."""
    return argparse.Namespace(
        project_id="test-proj",
        asset_type=None,
        sync=False,
        resources_file_path=None,
        config_path=None,
        min_age_days=None,
        skip_asset_type=[],
    )


@pytest.fixture
def config_mock():
    """Default mock for GCPProjectConfig."""
    return MagicMock()


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def patch_common():
    """Common patches for many CLI commands."""
    with patch.object(helpers, "init_directories") as mock_init:
        yield {
            "init": mock_init,
        }
