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

import json

import pytest

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS

if not AIRFLOW_V_3_3_PLUS:
    # ``airflow.sdk.execution_time.context`` exists on older Airflow versions, but ``NEVER_EXPIRE``
    # (imported transitively via ``task_state_store``) only lands in 3.3, so an
    # ``importorskip`` on the module is not enough -- gate on the version instead.
    pytest.skip("task state store needs Airflow >= 3.3", allow_module_level=True)

from pydantic import JsonValue, TypeAdapter

from airflow.providers.common.ai.durable.task_state_store import TaskStateStoreDurableStorage
from airflow.sdk.execution_time.context import NEVER_EXPIRE

# The real accessor validates the value against pydantic ``JsonValue`` before persisting.
_JSON_VALUE: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)


class FakeTaskStateStore:
    """In-memory stand-in for the ``context['task_state_store']`` accessor."""

    def __init__(self) -> None:
        self.store: dict = {}
        self.set_retentions: dict = {}
        self.deleted: list[str] = []

    def get(self, key, default=None):
        return self.store.get(key, default)

    def set(self, key, value, *, retention=None):
        # Mirror the real accessor: reject ``None`` and reject values that are not valid
        # ``JsonValue`` (tuples, non-string dict keys), then persist the JSON round-trip
        # -- so tests see the same rejections and Text-column round-trip as production.
        if value is None:
            raise ValueError("Cannot set value as None")
        _JSON_VALUE.validate_python(value)
        self.store[key] = json.loads(json.dumps(value))
        self.set_retentions[key] = retention

    def delete(self, key):
        self.deleted.append(key)
        self.store.pop(key, None)


@pytest.fixture
def accessor():
    return FakeTaskStateStore()


@pytest.fixture
def storage(accessor):
    return TaskStateStoreDurableStorage(accessor)


ENTRY = {"name": "agent__model.request", "kind": "model", "fingerprint": "fp", "payload": {"parts": []}}


class TestSaveLoad:
    def test_entry_roundtrips(self, storage):
        assert storage.save_step("k", ENTRY)

        assert storage.load_step("k") == ENTRY

    def test_stored_with_never_expire(self, storage, accessor):
        """A retry can replay the step however long the retry delay or the retention config."""
        storage.save_step("k", ENTRY)

        assert accessor.set_retentions["k"] is NEVER_EXPIRE

    def test_missing_key_is_none(self, storage):
        assert storage.load_step("missing") is None

    def test_a_value_that_is_not_a_journal_entry_is_a_miss(self, storage, accessor):
        accessor.store["k"] = "written by user code"

        assert storage.load_step("k") is None

    def test_a_write_the_store_rejects_is_skipped_not_raised(self, storage):
        """The step already succeeded; a rejected write only loses its replay."""
        assert not storage.save_step("k", {"payload": (1, 2)})
        assert storage.load_step("k") is None


class TestDeleteSteps:
    def test_deletes_only_the_given_keys(self, storage, accessor):
        storage.save_step("a", ENTRY)
        accessor.store["user_key"] = "kept"

        storage.delete_steps(["a"])

        assert accessor.store == {"user_key": "kept"}

    def test_a_failed_delete_does_not_raise(self, accessor):
        class FailingDelete(FakeTaskStateStore):
            def delete(self, key):
                raise RuntimeError("api down")

        storage = TaskStateStoreDurableStorage(FailingDelete())
        storage.save_step("a", ENTRY)

        storage.delete_steps(["a"])
