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

import pytest

from airflow.exceptions import AirflowException
from airflow.utils.setproctitle import setproctitle


class TestSetproctitle:
    """Test cases for the setproctitle utility function."""

    @pytest.mark.parametrize(
        "title",
        [
            pytest.param("test-title", id="test_title"),
            pytest.param("airflow: scheduler", id="airflow_scheduler"),
        ],
    )
    @pytest.mark.parametrize(
        "platform",
        [
            pytest.param("darwin", id="mac_os"),
            pytest.param("linux", id="linux"),
        ],
    )
    @pytest.mark.parametrize(
        "has_import_error",
        [
            pytest.param(False, id="no_import_error"),
            pytest.param(True, id="import_error"),
        ],
    )
    @patch("airflow.utils.setproctitle.log")
    def test_setproctitle_all_scenarios(self, mock_log, title, platform, has_import_error):
        """Test setproctitle with different titles, platforms, and import scenarios."""
        is_mac_os = platform == "darwin"
        mock_setproctitle_module = MagicMock()

        with patch("airflow.utils.setproctitle.sys.platform", platform):
            # Mock and act
            if not has_import_error:
                with patch.dict("sys.modules", {"setproctitle": mock_setproctitle_module}):
                    setproctitle(title)
            elif is_mac_os:
                # On darwin, will not raise ImportError, since it is skipped
                with patch("builtins.__import__", side_effect=ImportError("No module named 'setproctitle'")):
                    setproctitle(title)
            else:
                # On other platforms and has_import_error, should raise AirflowException if import fails
                with patch("builtins.__import__", side_effect=ImportError("No module named 'setproctitle'")):
                    with pytest.raises(
                        AirflowException,
                        match="The 'setproctitle' package is required to set process titles.",
                    ):
                        setproctitle(title)
                return

            # Assert
            if is_mac_os:
                # Verify debug message was logged and setproctitle was not called
                mock_log.debug.assert_called_once_with("Mac OS detected, skipping setproctitle")
                mock_setproctitle_module.setproctitle.assert_not_called()
            elif not has_import_error:
                # Should call setproctitle when import succeeds for non-mac platforms
                mock_setproctitle_module.setproctitle.assert_called_once_with(title)

                # Debug message should not be logged on non-darwin platforms
                mock_log.debug.assert_not_called()
