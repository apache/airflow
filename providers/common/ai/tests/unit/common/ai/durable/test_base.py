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

from typing import Any

from airflow.providers.common.ai.durable.base import (
    DURABLE_KEY_PREFIX,
    TOOL_RESULT_SENTINEL,
    DurableStorageProtocol,
)


class _CompleteBackend:
    """Implements the full ``DurableStorageProtocol`` surface."""

    def save_model_response(self, key, response, *, fingerprint):
        pass

    def load_model_response(self, key):
        return None, None

    def save_tool_result(self, key, result, *, fingerprint):
        pass

    def load_tool_result(self, key):
        return False, None, None

    def cleanup(self):
        pass


class _PartialBackend:
    """Missing ``cleanup`` -- must not satisfy the protocol."""

    def save_model_response(self, key, response, *, fingerprint):
        pass

    def load_model_response(self, key):
        return None, None

    def save_tool_result(self, key, result, *, fingerprint):
        pass

    def load_tool_result(self, key):
        return False, None, None


class TestDurableConstants:
    def test_key_prefix_is_a_single_path_segment(self):
        # Task state store keys are a single, un-encoded URL path segment, so the
        # reserved prefix must not contain a separator that would split the key.
        assert "/" not in DURABLE_KEY_PREFIX

    def test_sentinel_and_prefix_are_distinct(self):
        # Both are reserved markers written into the same store; if they ever
        # coincided a cached tool result could be mistaken for a key prefix.
        assert TOOL_RESULT_SENTINEL != DURABLE_KEY_PREFIX

    def test_constants_are_non_empty_strings(self):
        assert isinstance(TOOL_RESULT_SENTINEL, str)
        assert TOOL_RESULT_SENTINEL
        assert isinstance(DURABLE_KEY_PREFIX, str)
        assert DURABLE_KEY_PREFIX


class TestDurableStorageProtocol:
    def test_complete_backend_satisfies_protocol(self):
        assert isinstance(_CompleteBackend(), DurableStorageProtocol)

    def test_partial_backend_does_not_satisfy_protocol(self):
        assert not isinstance(_PartialBackend(), DurableStorageProtocol)

    def test_arbitrary_object_does_not_satisfy_protocol(self):
        assert not isinstance(object(), DurableStorageProtocol)

    def test_protocol_is_runtime_checkable(self):
        # ``runtime_checkable`` is what makes the ``isinstance`` checks above
        # legal; guard against the decorator being dropped.
        backend: Any = _CompleteBackend()
        assert isinstance(backend, DurableStorageProtocol)
