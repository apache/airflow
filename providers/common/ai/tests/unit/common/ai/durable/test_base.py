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

from airflow.providers.common.ai.durable.base import DURABLE_KEY_PREFIX, DurableStorageProtocol
from airflow.providers.common.ai.durable.storage import DurableStorage

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS


def test_key_prefix_is_a_single_path_segment():
    # Task state store keys are a single, un-encoded URL path segment, so the reserved
    # prefix must not contain a separator that would split the key.
    assert "/" not in DURABLE_KEY_PREFIX


class TestRealBackendsSatisfyProtocol:
    """The two shipped backends must stay in step with the protocol -- ``issubclass``
    on a methods-only ``runtime_checkable`` protocol catches a renamed or dropped method."""

    def test_object_storage_backend_satisfies_protocol(self):
        assert issubclass(DurableStorage, DurableStorageProtocol)

    @pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="task state store backend requires Airflow >= 3.3")
    def test_task_state_store_backend_satisfies_protocol(self):
        # Imported inside the test: this module runs on all cores, but
        # ``task_state_store`` pulls in ``NEVER_EXPIRE``, which only exists on 3.3+.
        from airflow.providers.common.ai.durable.task_state_store import TaskStateStoreDurableStorage

        assert issubclass(TaskStateStoreDurableStorage, DurableStorageProtocol)
