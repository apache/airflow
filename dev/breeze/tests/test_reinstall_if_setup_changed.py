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

from unittest.mock import MagicMock, patch

from airflow_breeze.utils.path_utils import reinstall_if_setup_changed


def test_reinstall_if_setup_changed_when_uv_not_installed():
    """Test that it returns False without error when the uv command is not found (FileNotFoundError)."""
    with patch("subprocess.run", side_effect=FileNotFoundError):
        # Should return False without any exception occurring during execution.
        result = reinstall_if_setup_changed()
        assert result is False


def test_reinstall_if_setup_changed_when_not_a_uv_tool():
    """Test when uv is present but 'apache-airflow-breeze' is not installed as a tool (exit 1)."""
    mock_res = MagicMock()
    mock_res.returncode = 1
    mock_res.stderr = "error: apache-airflow-breeze is not installed"

    with patch("subprocess.run", return_value=mock_res):
        # Should return False and not crash even if the subprocess fails (returncode 1).
        result = reinstall_if_setup_changed()
        assert result is False


def test_reinstall_if_setup_changed_success_and_modified():
    """Test that it returns True when successfully upgraded and content is modified."""
    mock_res = MagicMock()
    mock_res.returncode = 0
    mock_res.stderr = "Modified /Users/path/to/breeze"

    with patch("subprocess.run", return_value=mock_res):
        with patch("airflow_breeze.utils.path_utils.inform_about_self_upgrade") as mock_inform:
            result = reinstall_if_setup_changed()
            assert result is True
            mock_inform.assert_called_once()
