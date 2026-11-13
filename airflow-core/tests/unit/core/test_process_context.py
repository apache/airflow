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

import sys
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from airflow.process_context import should_use_task_sdk_api_path

TASK_RUNNER = "airflow.sdk.execution_time.task_runner"


class TestShouldUseTaskSdkApiPath:
    """Whether models.Variable/Connection take the Execution API path, per thread."""

    @pytest.mark.parametrize(("comms", "expected"), [("comms", True), (None, False)])
    def test_follows_the_task_runners_answer_for_this_thread(self, monkeypatch, comms, expected):
        monkeypatch.setitem(sys.modules, TASK_RUNNER, SimpleNamespace(get_supervisor_comms=lambda: comms))
        assert should_use_task_sdk_api_path() is expected

    @pytest.mark.parametrize(
        ("module_attrs", "expected"), [({"SUPERVISOR_COMMS": "comms"}, True), ({}, False)]
    )
    def test_falls_back_to_the_attribute_for_an_older_task_sdk(self, monkeypatch, module_attrs, expected):
        monkeypatch.setitem(sys.modules, TASK_RUNNER, SimpleNamespace(**module_attrs))
        assert should_use_task_sdk_api_path() is expected

    @pytest.mark.parametrize("name", ["get_supervisor_comms", "hide_supervisor_comms"])
    def test_the_task_sdk_has_the_names_looked_up_with_getattr(self, name):
        """airflow-core falls back silently when a name is missing, so a rename must fail here."""
        from airflow.sdk.execution_time import task_runner

        assert callable(getattr(task_runner, name))

    def test_the_thread_serving_an_in_process_request_is_the_server_side(self):
        from airflow.sdk.execution_time import task_runner

        with patch.object(task_runner, "SUPERVISOR_COMMS", "comms", create=True):
            assert should_use_task_sdk_api_path() is True
            with task_runner.hide_supervisor_comms():
                assert should_use_task_sdk_api_path() is False
