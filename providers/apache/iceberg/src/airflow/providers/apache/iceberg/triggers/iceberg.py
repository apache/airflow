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

from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError

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
    from collections.abc import AsyncIterator

DEFAULT_BRANCH = "main"
WATERMARK_KEY = "snapshot_id"
# Raised by AssetStateStoreAccessors when the trigger is watched by more than one asset.
_MULTI_ASSET_ERROR = "concrete inlets and outlets"


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

    The event carries ``table``, ``branch``, ``snapshot_id`` and ``previous_snapshot_id``, so a
    task can scan the delta rather than the whole table. ``previous_snapshot_id`` is ``None`` on
    the first event, which is also what a restart looks like where no watermark is kept, so a
    task that must not run twice for one snapshot keys on ``snapshot_id``.

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

    def _head_snapshot_id(self) -> int | None:
        """Return the snapshot the branch points at, or None if there is nothing to watch yet."""
        try:
            table = IcebergHook(self.iceberg_conn_id).load_table(self.table)
        except (NoSuchTableError, NoSuchNamespaceError):
            # A watcher outlives the table it watches, and raising here would kill the trigger
            # and have the triggerer restart it once per second until the table appears.
            self.log.debug("%s does not exist yet; waiting", self.table)
            return None
        if ref := table.metadata.refs.get(self.branch):
            return ref.snapshot_id
        return None

    async def run(self) -> AsyncIterator[TriggerEvent]:
        # serialize() is captured once when the trigger row is created, so a value mutated on
        # self is lost when the triggerer restarts and the current head would be re-emitted as
        # a new commit. The watermark survives that; the kwarg only seeds the first run.
        # It postdates the Airflow versions this provider supports and is absent when several
        # assets share the trigger, so polling carries on without it, losing only that cursor.
        store = getattr(self, "asset_state_store", None)
        if store is not None:
            try:
                stored = await store.aget(WATERMARK_KEY)
            except ValueError as err:
                # The accessor serves one asset at a time, so it refuses to guess when this
                # trigger is watched by several. That happens because triggers are deduplicated
                # by hash(classpath, kwargs) while asset_watcher is many-to-many, so two assets
                # watching this table with the same arguments share one trigger. There is then
                # no single cursor to keep. A state store backend can raise ValueError too, and
                # swallowing that would disable the watermark without saying so.
                if _MULTI_ASSET_ERROR not in str(err):
                    raise
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
                    await store.aset(WATERMARK_KEY, head)
                yield TriggerEvent(
                    {
                        "table": self.table,
                        "branch": self.branch,
                        "snapshot_id": head,
                        "previous_snapshot_id": previous,
                    }
                )
            await asyncio.sleep(self.poll_interval)
