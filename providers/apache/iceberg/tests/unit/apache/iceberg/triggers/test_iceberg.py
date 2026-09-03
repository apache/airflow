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
from contextlib import aclosing, suppress
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError

from airflow.providers.apache.iceberg.triggers.iceberg import IcebergTableSnapshotTrigger

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

LOAD_TABLE = "airflow.providers.apache.iceberg.hooks.iceberg.IcebergHook.load_table"

TRIGGER_PATH = "airflow.providers.apache.iceberg.triggers.iceberg.IcebergTableSnapshotTrigger"


def _table_at(snapshot_id: int | None, branch: str = "main") -> MagicMock:
    """Build a table whose ``branch`` points at ``snapshot_id`` (None means no such ref)."""
    table = MagicMock()
    table.metadata.refs = {branch: MagicMock(snapshot_id=snapshot_id)} if snapshot_id is not None else {}
    return table


async def _collect(trigger: IcebergTableSnapshotTrigger, count: int, timeout: float = 1.0) -> list[dict]:
    """Pull up to ``count`` payloads off the trigger, giving up after ``timeout``."""
    payloads: list[dict] = []
    generator: AsyncGenerator = trigger.run()  # type: ignore[assignment]

    async with aclosing(generator):

        async def pump() -> None:
            async for event in generator:
                payloads.append(event.payload)
                if len(payloads) >= count:
                    return

        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(pump(), timeout=timeout)
    return payloads


def test_serialize_round_trip():
    trigger = IcebergTableSnapshotTrigger(
        table="db.tbl", iceberg_conn_id="my_conn", branch="audit", poll_interval=5
    )
    classpath, kwargs = trigger.serialize()

    assert classpath == TRIGGER_PATH
    assert kwargs == {
        "table": "db.tbl",
        "iceberg_conn_id": "my_conn",
        "branch": "audit",
        "poll_interval": 5,
        "last_seen_snapshot_id": None,
    }
    assert IcebergTableSnapshotTrigger(**kwargs).serialize() == (classpath, kwargs)


def test_rejects_table_without_namespace():
    with pytest.raises(ValueError, match="fully-qualified table name"):
        IcebergTableSnapshotTrigger(table="orders")


async def _never():
    """An empty shared stream, as the triggerer would hand to filter_shared_stream."""
    return
    yield  # pragma: no cover


def _shared_stream_key(trigger):
    return getattr(trigger, "shared_stream_key", lambda: None)()


@pytest.mark.parametrize(
    "kwargs",
    [
        pytest.param({"table": "db.tbl"}, id="defaults"),
        pytest.param({"table": "db.tbl", "branch": "audit"}, id="branch"),
        pytest.param({"table": "db.tbl", "poll_interval": 5}, id="interval"),
    ],
)
def test_the_triggerer_reaches_run(kwargs):
    """A non-None key sends the triggerer to filter_shared_stream instead of run().

    Declaring one without also implementing open_shared_stream and filter_shared_stream
    means the trigger raises NotImplementedError and never polls. getattr because
    shared streams postdate the Airflow versions this provider supports.
    """
    assert _shared_stream_key(IcebergTableSnapshotTrigger(**kwargs)) is None


@pytest.mark.asyncio
async def test_emits_current_head_on_first_poll():
    """With no watermark the current head is itself the first event."""
    with patch(LOAD_TABLE, return_value=_table_at(111)):
        payloads = await _collect(IcebergTableSnapshotTrigger(table="db.tbl", poll_interval=0.01), 1)

    assert payloads == [
        {"table": "db.tbl", "branch": "main", "snapshot_id": 111, "previous_snapshot_id": None}
    ]


@pytest.mark.asyncio
async def test_silent_while_head_is_unchanged():
    """A table that has not committed must not schedule anything."""
    with patch(LOAD_TABLE, return_value=_table_at(111)):
        trigger = IcebergTableSnapshotTrigger(table="db.tbl", poll_interval=0.01, last_seen_snapshot_id=111)
        payloads = await _collect(trigger, 1, timeout=0.2)

    assert payloads == []


@pytest.mark.asyncio
async def test_emits_once_per_new_snapshot():
    """Each commit produces exactly one event carrying the snapshot it replaced."""
    with patch(LOAD_TABLE, side_effect=[_table_at(111), _table_at(222), _table_at(222), _table_at(333)]):
        trigger = IcebergTableSnapshotTrigger(table="db.tbl", poll_interval=0.01, last_seen_snapshot_id=111)
        # Gathering 2 events takes 4 polling rounds, each with a real asyncio.to_thread call;
        # the default 1s budget is too tight under CI thread-pool scheduling latency.
        payloads = await _collect(trigger, 2, timeout=3.0)

    assert [(p["previous_snapshot_id"], p["snapshot_id"]) for p in payloads] == [(111, 222), (222, 333)]


@pytest.mark.asyncio
async def test_silent_while_branch_is_absent():
    """Watching a branch that does not exist yet waits for it rather than failing."""
    with patch(LOAD_TABLE, return_value=_table_at(None)):
        trigger = IcebergTableSnapshotTrigger(table="db.tbl", branch="audit", poll_interval=0.01)
        payloads = await _collect(trigger, 1, timeout=0.2)

    assert payloads == []


@pytest.mark.asyncio
async def test_watches_the_requested_branch():
    """The event reports the branch that was asked for, not main."""
    with patch(LOAD_TABLE, return_value=_table_at(999, branch="audit")):
        trigger = IcebergTableSnapshotTrigger(table="db.tbl", branch="audit", poll_interval=0.01)
        payloads = await _collect(trigger, 1)

    assert payloads[0]["branch"] == "audit"
    assert payloads[0]["snapshot_id"] == 999


@pytest.mark.asyncio
async def test_resumes_from_the_stored_watermark():
    """A restarted triggerer must not re-emit a snapshot it already reported.

    ``serialize()`` is captured once, so the kwarg still holds the value from when the trigger
    row was written; only the stored watermark reflects what was actually emitted.
    """
    store = MagicMock()
    store.aget = AsyncMock(return_value=222)

    trigger = IcebergTableSnapshotTrigger(table="db.tbl", poll_interval=0.01, last_seen_snapshot_id=111)
    trigger.asset_state_store = store

    with patch(LOAD_TABLE, return_value=_table_at(222)):
        payloads = await _collect(trigger, 1, timeout=0.2)

    assert payloads == []
    store.aget.assert_awaited_once_with("snapshot_id")


@pytest.mark.asyncio
async def test_persists_the_watermark_on_each_event():
    store = MagicMock()
    store.aget = AsyncMock(return_value=None)
    store.aset = AsyncMock()

    trigger = IcebergTableSnapshotTrigger(table="db.tbl", poll_interval=0.01)
    trigger.asset_state_store = store

    with patch(LOAD_TABLE, side_effect=[_table_at(111), _table_at(222), _table_at(222)]):
        # Multiple real thread-pool head lookups can exceed the default 1s budget under CI latency.
        payloads = await _collect(trigger, 2, timeout=3.0)

    assert [p["snapshot_id"] for p in payloads] == [111, 222]
    assert [c.args for c in store.aset.await_args_list] == [("snapshot_id", 111), ("snapshot_id", 222)]


@pytest.mark.asyncio
async def test_runs_without_a_watermark_when_several_assets_watch_it():
    """More than one watched asset leaves no single cursor, so it degrades instead of raising."""
    store = MagicMock()
    store.aget = AsyncMock(side_effect=ValueError("Task has 2 concrete inlets and outlets"))
    store.aset = AsyncMock()

    trigger = IcebergTableSnapshotTrigger(table="db.tbl", poll_interval=0.01)
    trigger.asset_state_store = store

    with patch(LOAD_TABLE, return_value=_table_at(111)):
        payloads = await _collect(trigger, 1)

    assert [p["snapshot_id"] for p in payloads] == [111]
    store.aset.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_state_store_failure_is_not_mistaken_for_several_assets():
    """A pluggable backend can raise ValueError too, and hiding it would disable the watermark."""
    store = MagicMock()
    store.aget = AsyncMock(side_effect=ValueError("could not decode the stored reference"))

    trigger = IcebergTableSnapshotTrigger(table="db.tbl", poll_interval=0.01)
    trigger.asset_state_store = store

    with patch(LOAD_TABLE, return_value=_table_at(111)):
        with pytest.raises(ValueError, match="could not decode"):
            await _collect(trigger, 1)


@pytest.mark.asyncio
async def test_runs_on_airflow_without_an_asset_state_store():
    """``asset_state_store`` postdates the oldest Airflow this provider supports."""
    trigger = IcebergTableSnapshotTrigger(table="db.tbl", poll_interval=0.01)
    if hasattr(trigger, "asset_state_store"):
        del trigger.asset_state_store
    assert not hasattr(trigger, "asset_state_store")

    with patch(LOAD_TABLE, return_value=_table_at(111)):
        payloads = await _collect(trigger, 1)

    assert [p["snapshot_id"] for p in payloads] == [111]


@pytest.mark.asyncio
async def test_the_triggerer_dispatch_polls_the_table():
    """Drive the branch triggerer_job_runner takes, rather than calling run() directly."""
    trigger = IcebergTableSnapshotTrigger(table="db.tbl", poll_interval=0.01)

    shared_key = _shared_stream_key(trigger)
    with patch(LOAD_TABLE, return_value=_table_at(111)):
        stream = trigger.filter_shared_stream(_never()) if shared_key is not None else trigger.run()
        async with aclosing(stream) as events:
            async for event in events:
                assert event.payload["snapshot_id"] == 111
                break


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [NoSuchTableError, NoSuchNamespaceError])
async def test_waits_for_a_table_that_does_not_exist_yet(error):
    """Raising kills the trigger, and the triggerer then restarts it once per second."""
    trigger = IcebergTableSnapshotTrigger(table="db.tbl", poll_interval=0.01)

    with patch(LOAD_TABLE, side_effect=error("Table does not exist: db.tbl")):
        assert await _collect(trigger, 1, timeout=0.15) == []


@pytest.mark.asyncio
async def test_fires_once_the_table_appears():
    trigger = IcebergTableSnapshotTrigger(table="db.tbl", poll_interval=0.01)
    absent = NoSuchTableError("Table does not exist: db.tbl")

    with patch(LOAD_TABLE, side_effect=[absent, absent, _table_at(111)]):
        payloads = await _collect(trigger, 1)

    assert [p["snapshot_id"] for p in payloads] == [111]
