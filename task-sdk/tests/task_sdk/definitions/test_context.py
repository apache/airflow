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

from airflow.sdk.definitions.context import get_current_context


class TestCurrentContext:
    def test_current_context_no_context_raise(self):
        with pytest.raises(RuntimeError):
            get_current_context()

    def test_get_current_context_with_context(self, monkeypatch):
        mock_context = {"ti": "task_instance", "key": "value"}
        monkeypatch.setattr(
            "airflow.sdk.definitions._internal.contextmanager._CURRENT_CONTEXT", [mock_context]
        )
        result = get_current_context()
        assert result == mock_context

    def test_get_current_context_without_context(self, monkeypatch):
        monkeypatch.setattr("airflow.sdk.definitions._internal.contextmanager._CURRENT_CONTEXT", [])
        with pytest.raises(RuntimeError, match="Current context was requested but no context was found!"):
            get_current_context()
