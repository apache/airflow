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
from typing import TYPE_CHECKING, Any

from airflow.providers.apache.iceberg.hooks.iceberg import IcebergHook
from airflow.providers.apache.iceberg.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.triggers.base import BaseEventTrigger, TriggerEvent
else:
    from airflow.triggers.base import (  # type: ignore[assignment]
        BaseTrigger as BaseEventTrigger,
        TriggerEvent,
    )

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Hashable

DEFAULT_BRANCH = "main"
WATERMARK_KEY = "snapshot_id"


class IcebergTableSnapshotTrigger(BaseEventTrigger):
    """
    Fire an event whenever an Iceberg table gains a new snapshot.

    Polls the branch head through the catalog and emits an event when it points at a
    snapshot the trigger has not reported yet, which makes a table commit usable as a
    scheduling signal::

        from airflow.sdk import Asset, AssetWatcher

        orders = Asset(
            "orders",
            watchers=[
                AssetWatcher(
                    name="orders_commits",
                    trigger=IcebergTableSnapshotTrigger(table="sales.orders"),
                )
            ],
        )


        @dag(schedule=[orders])
        def downstream(): ...

    Triggers sharing a catalog connection, branch and poll interval share one poll loop,
    so watching many tables in a catalog does not open a connection per table.

    :param table: Fully-qualified table name, ``namespace.table``. Nested namespaces are
        written as ``a.b.table``.
    :param iceberg_conn_id: Connection holding the catalog URI and credentials.
    :param branch: Branch or tag to watch. Defaults to ``main``.
    :param poll_interval: Seconds between polls.
    :param last_seen_snapshot_id: Snapshot already reported. Leave unset to treat the
        current head as the first event.
    """

    def __init__(
        self,
        *,
        table: str,
        iceberg_conn_id: str = IcebergHook.default_conn_name,
        branch: str = DEFAULT_BRANCH,
        poll_interval: float = 60,
        last_seen_snapshot_id: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if "." not in table:
            raise ValueError(f"Expected a fully-qualified table name (namespace.table), got: {table!r}")
        self.table = table
        self.iceberg_conn_id = iceberg_conn_id
        self.branch = branch
        self.poll_interval = poll_interval
        self.last_seen_snapshot_id = last_seen_snapshot_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.apache.iceberg.triggers.iceberg.IcebergTableSnapshotTrigger",
            {
                "table": self.table,
                "iceberg_conn_id": self.iceberg_conn_id,
                "branch": self.branch,
                "poll_interval": self.poll_interval,
                "last_seen_snapshot_id": self.last_seen_snapshot_id,
            },
        )

    def shared_stream_key(self) -> Hashable | None:
        """Group triggers that can be served by a single poll of the same catalog."""
        return (self.iceberg_conn_id, self.branch, self.poll_interval)

    def _head_snapshot_id(self) -> int | None:
        """Return the snapshot the branch points at, or None if the branch does not exist."""
        table = IcebergHook(self.iceberg_conn_id).load_table(self.table)
        if ref := table.metadata.refs.get(self.branch):
            return ref.snapshot_id
        return None

    async def run(self) -> AsyncIterator[TriggerEvent]:
        # serialize() is captured once when the trigger row is created, so a value mutated on
        # self is lost when the triggerer restarts and the current head would be re-emitted as
        # a new commit. The watermark survives that; the kwarg only seeds the first run.
        # getattr because asset_state_store postdates the Airflow versions this provider supports.
        store = getattr(self, "asset_state_store", None)
        if store is not None:
            try:
                stored = await asyncio.to_thread(store.get, WATERMARK_KEY)
            except ValueError:
                # Several assets watch this trigger, so there is no single cursor to keep.
                self.log.warning(
                    "%s is watched by more than one asset; not persisting a snapshot watermark, "
                    "so a triggerer restart may re-emit the current head.",
                    self.table,
                )
                store = None
            else:
                if stored is not None:
                    self.last_seen_snapshot_id = int(stored)

        while True:
            # pyiceberg is synchronous, so keep the catalog call off the event loop.
            head = await asyncio.to_thread(self._head_snapshot_id)
            if head is not None and head != self.last_seen_snapshot_id:
                previous, self.last_seen_snapshot_id = self.last_seen_snapshot_id, head
                if store is not None:
                    await asyncio.to_thread(store.set, WATERMARK_KEY, head)
                yield TriggerEvent(
                    {
                        "table": self.table,
                        "branch": self.branch,
                        "snapshot_id": head,
                        "previous_snapshot_id": previous,
                    }
                )
            await asyncio.sleep(self.poll_interval)
