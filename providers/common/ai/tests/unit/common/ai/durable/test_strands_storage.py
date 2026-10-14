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

import asyncio
import time

import pytest

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS

if not AIRFLOW_V_3_3_PLUS:
    pytest.skip("task state store needs Airflow >= 3.3", allow_module_level=True)

pytest.importorskip("strands")

from strands import Agent
from strands.session import SnapshotSessionManager

from airflow.providers.common.ai.durable.base import DURABLE_KEY_PREFIX
from airflow.providers.common.ai.durable.strands_storage import TaskStateStoreStorage
from airflow.sdk.execution_time.context import NEVER_EXPIRE

from unit.common.ai.durable.test_task_state_store import FakeTaskStateStore
from unit.common.ai.tools.test_strands import _OneToolCallModel


@pytest.fixture
def accessor():
    return FakeTaskStateStore()


@pytest.fixture
def storage(accessor):
    return TaskStateStoreStorage(accessor)


class TestTaskStateStoreStorage:
    @pytest.mark.parametrize(
        "data",
        [
            pytest.param(b'{"messages": ["\xe2\x82\xac 42"]}', id="utf-8"),
            pytest.param(b"\x00\xff\xfe binary", id="binary"),
            pytest.param(b"", id="empty"),
        ],
    )
    def test_read_returns_the_bytes_written(self, storage, data):
        asyncio.run(storage.write("session/a/snapshot.json", data))

        assert asyncio.run(storage.read("session/a/snapshot.json")) == data

    def test_read_of_a_missing_key_returns_none(self, storage):
        assert asyncio.run(storage.read("session/missing.json")) is None

    def test_writes_under_the_reserved_prefix_without_expiry(self, storage, accessor):
        asyncio.run(storage.write("session/a/snapshot.json", b"{}"))

        assert set(accessor.store) == {
            f"{DURABLE_KEY_PREFIX}strands/session/a/snapshot.json",
            f"{DURABLE_KEY_PREFIX}strands_index",
        }
        assert set(accessor.set_retentions.values()) == {NEVER_EXPIRE}

    def test_leaves_user_keys_alone(self, storage, accessor):
        accessor.set("job_id", "abc")
        asyncio.run(storage.write("job_id", b"agent"))
        asyncio.run(storage.delete("job_id"))

        assert accessor.store == {"job_id": "abc"}

    def test_list_returns_matching_keys_sorted(self, storage):
        for key in ("session/b/x.json", "context/a/y.json", "session/a/z.json"):
            asyncio.run(storage.write(key, b"{}"))

        assert asyncio.run(storage.list("session/")) == ["session/a/z.json", "session/b/x.json"]
        assert asyncio.run(storage.list("")) == ["context/a/y.json", "session/a/z.json", "session/b/x.json"]

    def test_overwriting_a_key_lists_it_once(self, storage):
        asyncio.run(storage.write("session/a.json", b"1"))
        asyncio.run(storage.write("session/a.json", b"2"))

        assert asyncio.run(storage.list("")) == ["session/a.json"]
        assert asyncio.run(storage.read("session/a.json")) == b"2"

    def test_deleting_every_key_leaves_nothing_in_the_store(self, storage, accessor):
        asyncio.run(storage.write("session/a.json", b"1"))
        asyncio.run(storage.write("session/b.json", b"2"))

        asyncio.run(storage.delete("session/a.json"))
        assert asyncio.run(storage.list("")) == ["session/b.json"]
        asyncio.run(storage.delete("session/b.json"))

        assert accessor.store == {}

    def test_deleting_a_missing_key_is_a_no_op(self, storage, accessor):
        asyncio.run(storage.delete("session/missing.json"))

        assert accessor.store == {}

    def test_concurrent_writes_keep_every_key_listed(self):
        """Strands deletes a session's keys concurrently; index updates must not lose each other."""

        class _SlowStore(FakeTaskStateStore):
            def get(self, key, default=None):
                value = super().get(key, default)
                # Between this read of the index and the write that follows it, so that
                # unserialized updates would overwrite each other.
                time.sleep(0.01)
                return value

        storage = TaskStateStoreStorage(_SlowStore())
        keys = [f"session/{n}.json" for n in range(8)]

        async def write_all():
            await asyncio.gather(*(storage.write(key, b"{}") for key in keys))

        asyncio.run(write_all())

        assert asyncio.run(storage.list("")) == keys

    def test_instances_over_one_task_instance_share_their_keys(self, accessor):
        asyncio.run(TaskStateStoreStorage(accessor).write("session/a.json", b"1"))
        asyncio.run(TaskStateStoreStorage(accessor).write("session/b.json", b"2"))

        assert asyncio.run(TaskStateStoreStorage(accessor).list("")) == ["session/a.json", "session/b.json"]

    def test_backs_a_snapshot_session_manager(self, storage, accessor):
        session_manager = SnapshotSessionManager("run-1", storage=storage)
        agent = Agent(
            model=_OneToolCallModel("lookup", {}), session_manager=session_manager, callback_handler=None
        )
        agent.messages.append({"role": "user", "content": [{"text": "hello"}]})
        session_manager.sync_agent(agent)

        restored_manager = SnapshotSessionManager("run-1", storage=storage)
        restored = Agent(
            model=_OneToolCallModel("lookup", {}), session_manager=restored_manager, callback_handler=None
        )
        assert restored.messages == [{"role": "user", "content": [{"text": "hello"}]}]

        asyncio.run(restored_manager.delete_session())
        assert accessor.store == {}
