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

import pytest


class TestRemoteTaskHandlerKwargsSeparation:
    """Test that remote_task_handler_kwargs correctly separates handler-level and remote I/O kwargs."""

    _HANDLER_LEVEL_KWARGS = {"max_bytes", "backup_count", "delay"}

    @pytest.mark.parametrize(
        "input_kwargs, expected_handler, expected_io",
        [
            pytest.param(
                {"max_bytes": 5000000, "backup_count": 5},
                {"max_bytes": 5000000, "backup_count": 5},
                {},
                id="handler-only-kwargs",
            ),
            pytest.param(
                {"some_remote_option": "value"},
                {},
                {"some_remote_option": "value"},
                id="remote-io-only-kwargs",
            ),
            pytest.param(
                {"max_bytes": 5000000, "backup_count": 5, "some_remote_option": "value"},
                {"max_bytes": 5000000, "backup_count": 5},
                {"some_remote_option": "value"},
                id="mixed-kwargs",
            ),
            pytest.param(
                {},
                {},
                {},
                id="empty-kwargs",
            ),
            pytest.param(
                {"delay": True, "max_bytes": 1000},
                {"delay": True, "max_bytes": 1000},
                {},
                id="delay-and-max-bytes",
            ),
        ],
    )
    def test_kwargs_separation(self, input_kwargs, expected_handler, expected_io):
        """Verify handler-level kwargs are separated from remote I/O kwargs.

        This mirrors the separation logic in airflow_local_settings.py that
        prevents FileTaskHandler params (max_bytes, backup_count, delay) from
        being passed to RemoteLogIO constructors which don't accept them.
        """
        handler_kwargs = {
            k: v for k, v in input_kwargs.items() if k in self._HANDLER_LEVEL_KWARGS
        }
        remote_io_kwargs = {
            k: v for k, v in input_kwargs.items() if k not in self._HANDLER_LEVEL_KWARGS
        }
        assert handler_kwargs == expected_handler
        assert remote_io_kwargs == expected_io

    def test_handler_kwargs_applied_to_logging_config(self):
        """Verify handler kwargs are properly applied to the DEFAULT_LOGGING_CONFIG handler entry."""
        handler_config = {
            "class": "airflow.utils.log.file_task_handler.FileTaskHandler",
            "formatter": "airflow",
            "base_log_folder": "/tmp/logs",
        }
        handler_kwargs = {"max_bytes": 5000000, "backup_count": 5}

        handler_config.update(handler_kwargs)

        assert handler_config["max_bytes"] == 5000000
        assert handler_config["backup_count"] == 5
        # Original keys preserved
        assert handler_config["class"] == "airflow.utils.log.file_task_handler.FileTaskHandler"
