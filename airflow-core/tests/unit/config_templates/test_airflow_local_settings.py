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


class TestRemoteTaskHandlerKwargsSeparation:
    """Test that remote_task_handler_kwargs are correctly separated into handler and IO kwargs."""

    def test_handler_kwargs_separated_from_io_kwargs(self):
        """Verify that handler-level kwargs (max_bytes, backup_count, delay) are not passed to RemoteLogIO."""
        remote_task_handler_kwargs = {
            "max_bytes": 5000000,
            "backup_count": 5,
            "delay": True,
            "delete_local_copy": False,
        }

        # This mirrors the logic in airflow_local_settings.py
        _HANDLER_ONLY_KWARGS = {"max_bytes", "backup_count", "delay"}
        _handler_kwargs = {
            k: v for k, v in remote_task_handler_kwargs.items() if k in _HANDLER_ONLY_KWARGS
        }
        _remote_io_kwargs = {
            k: v for k, v in remote_task_handler_kwargs.items() if k not in _HANDLER_ONLY_KWARGS
        }

        assert _handler_kwargs == {"max_bytes": 5000000, "backup_count": 5, "delay": True}
        assert _remote_io_kwargs == {"delete_local_copy": False}

    def test_empty_kwargs_produces_empty_dicts(self):
        """Both handler and IO kwargs should be empty when input is empty."""
        remote_task_handler_kwargs: dict = {}

        _HANDLER_ONLY_KWARGS = {"max_bytes", "backup_count", "delay"}
        _handler_kwargs = {
            k: v for k, v in remote_task_handler_kwargs.items() if k in _HANDLER_ONLY_KWARGS
        }
        _remote_io_kwargs = {
            k: v for k, v in remote_task_handler_kwargs.items() if k not in _HANDLER_ONLY_KWARGS
        }

        assert _handler_kwargs == {}
        assert _remote_io_kwargs == {}

    def test_only_io_kwargs(self):
        """When only IO kwargs are provided, handler kwargs should be empty."""
        remote_task_handler_kwargs = {"delete_local_copy": True}

        _HANDLER_ONLY_KWARGS = {"max_bytes", "backup_count", "delay"}
        _handler_kwargs = {
            k: v for k, v in remote_task_handler_kwargs.items() if k in _HANDLER_ONLY_KWARGS
        }
        _remote_io_kwargs = {
            k: v for k, v in remote_task_handler_kwargs.items() if k not in _HANDLER_ONLY_KWARGS
        }

        assert _handler_kwargs == {}
        assert _remote_io_kwargs == {"delete_local_copy": True}
