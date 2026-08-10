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
from unittest.mock import MagicMock, patch

import pytest

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


@pytest.mark.parametrize(
    ("other", "shared"),
    [
        pytest.param({"table": "db.other"}, True, id="different table shares one poll"),
        pytest.param({"table": "db.tbl", "branch": "audit"}, False, id="different branch polls separately"),
        pytest.param({"table": "db.tbl", "iceberg_conn_id": "other"}, False, id="different catalog"),
        pytest.param({"table": "db.tbl", "poll_interval": 5}, False, id="different interval"),
    ],
)
def test_shared_stream_key_groups_by_catalog(other, shared):
    """Tables in one catalog share a poll; anything that changes the poll itself does not."""
    base = IcebergTableSnapshotTrigger(table="db.tbl")
    assert (base.shared_stream_key() == IcebergTableSnapshotTrigger(**other).shared_stream_key()) is shared


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
        payloads = await _collect(trigger, 2)

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
