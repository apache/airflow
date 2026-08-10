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

from pathlib import Path
from unittest import mock
from unittest.mock import patch

import pytest

from airflow.sdk.api.datamodels._generated import BundleInfo
from airflow.sdk.exceptions import AirflowException
from airflow.sdk.execution_time.bundles import initialize_ti_bundle, verify_bundle_access


def test_verify_bundle_access_raises_when_not_accessible(tmp_path: Path):
    bundle_path = tmp_path / "test_bundle"
    bundle_path.mkdir()

    mock_bundle = mock.Mock()
    mock_bundle.path = bundle_path
    mock_bundle.name = "test-bundle"

    # Mock os.access to simulate permission denied (avoids root user issues in CI)
    with patch("airflow.sdk.execution_time.bundles.os.access", return_value=False):
        with pytest.raises(AirflowException) as exc_info:
            verify_bundle_access(mock_bundle)

        assert "not accessible" in str(exc_info.value)
        assert "test-bundle" in str(exc_info.value)


def test_verify_bundle_access_succeeds_when_readable(tmp_path: Path):
    bundle_path = tmp_path / "accessible_bundle"
    bundle_path.mkdir()

    mock_bundle = mock.Mock()
    mock_bundle.path = bundle_path
    mock_bundle.name = "test-bundle"

    verify_bundle_access(mock_bundle)


def test_verify_bundle_access_skips_nonexistent_path(tmp_path: Path):
    mock_bundle = mock.Mock()
    mock_bundle.path = tmp_path / "nonexistent"
    mock_bundle.name = "test-bundle"

    # Should not raise - nonexistent paths are handled by initialize()
    verify_bundle_access(mock_bundle)


def test_initialize_ti_bundle_resolves_initializes_and_verifies(tmp_path: Path):
    """initialize_ti_bundle resolves the bundle from BundleInfo, initializes it, and access-checks it."""
    bundle_path = tmp_path / "bundle"
    bundle_path.mkdir()
    mock_bundle = mock.Mock()
    mock_bundle.path = bundle_path
    mock_bundle.name = "my-bundle"

    bundle_info = BundleInfo(name="my-bundle", version="v2", version_data={"k": "v"})

    with patch("airflow.sdk.execution_time.bundles.DagBundlesManager") as mock_manager:
        mock_manager.return_value.get_bundle.return_value = mock_bundle
        result = initialize_ti_bundle(bundle_info)

    assert result is mock_bundle
    mock_manager.return_value.get_bundle.assert_called_once_with(
        name="my-bundle", version="v2", version_data={"k": "v"}
    )
    mock_bundle.initialize.assert_called_once_with()
