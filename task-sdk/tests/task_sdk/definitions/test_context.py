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

from airflow.sdk.definitions.context import context_merge, get_current_context


class TestContextMerge:
    def test_context_merge_empty_dict(self):
        context: dict[str, str] = {}
        context_merge(context, {"key": "value"})
        assert context == {"key": "value"}

    def test_context_merge_empty_dict_with_kwargs(self):
        context: dict[str, str] = {}
        context_merge(context, key="value", other="123")
        assert context == {"key": "value", "other": "123"}

    def test_context_merge_existing_dict(self):
        context: dict[str, int] = {"a": 1, "b": 2}
        context_merge(context, {"b": 3, "c": 4}, d=5)
        assert context == {"a": 1, "b": 3, "c": 4, "d": 5}

    def test_context_merge_iterable_key_value_pairs(self):
        context: dict[str, str] = {}
        context_merge(context, [("x", "1"), ("y", "2")])
        assert context == {"x": "1", "y": "2"}


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
