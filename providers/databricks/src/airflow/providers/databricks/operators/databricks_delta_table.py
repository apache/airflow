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
"""Produce asset events for Delta table versions committed outside of Airflow."""

from __future__ import annotations

from collections.abc import Collection, Iterable, Sequence
from dataclasses import dataclass
from datetime import date
from functools import cached_property
from typing import TYPE_CHECKING, Any, Literal

from airflow.providers.common.compat.sdk import AirflowSkipException, BaseOperator
from airflow.providers.common.sql.hooks.handlers import fetch_all_handler
from airflow.providers.databricks.exceptions import DatabricksDeltaTableError
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from airflow.providers.databricks.utils.query_tags import build_query_tags

if TYPE_CHECKING:
    from airflow.providers.common.compat.assets import Asset
    from airflow.providers.common.compat.sdk import Context
    from airflow.providers.databricks.assets.databricks import UnityTableIdentity

CHECKPOINT_KEY_EXTRA = "databricks_delta_checkpoint_key"
"""Asset event ``extra`` key that marks an event as a checkpoint written by this producer."""

DEFAULT_IGNORED_OPERATIONS = ("OPTIMIZE", "VACUUM START", "VACUUM END")

ChangeKind = Literal["enrollment", "new_version", "recreated"]


@dataclass(frozen=True)
class DeltaCheckpoint:
    """Last Delta version this producer published for a table, read back from an asset event."""

    version: int
    table_id: str | None


@dataclass(frozen=True)
class _HistoryEntry:
    version: int
    operation: str | None
    timestamp: str | None


def _quote_identifier(part: str) -> str:
    return "`" + part.replace("`", "``") + "`"


def _row_to_dict(row: Any) -> dict[str, Any]:
    if hasattr(row, "_asdict"):
        return row._asdict()
    if hasattr(row, "asDict"):
        return row.asDict()
    return dict(row)


class DatabricksDeltaTableVersionProducerOperator(BaseOperator):
    """
    Publish an asset event when a Unity Catalog Delta table gets a newer committed version.

    Use this operator in a recurring producer Dag to turn Delta commits made by other jobs or
    systems into Airflow asset events. Downstream Dags schedule on ``unity_table.to_asset()``.

    Each run reads the newest Delta version, compares it with the checkpoint, and either publishes
    one asset event for the newest version or skips. Several commits between two runs are coalesced
    into one event for the newest version. Intermediate commits are not delivered one by one.

    The checkpoint is the newest asset event that this producer published for the asset, found by
    ``checkpoint_key`` in the event ``extra``. Airflow records the event when the task succeeds, so
    publishing the event and advancing the checkpoint are the same write. Delivery is at least
    once: concurrent runs or a cleared run can publish the same version again. Consumers that must
    not process a version twice deduplicate on ``extra["dedup_id"]``.

    An event for version N does not pin downstream reads to N. A consumer that needs a
    reproducible read queries ``VERSION AS OF`` ``extra["version"]``.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DatabricksDeltaTableVersionProducerOperator`

    :param unity_table: Static identity of the observed table. It is added to ``inlets`` (to read
        the checkpoint) and, when ``outlets`` is omitted, ``outlets`` is ``[unity_table.to_asset()]``.
    :param checkpoint_key: Scope of the checkpoint. Producers with different keys keep separate
        checkpoints for the same asset. Defaults to ``"<dag_id>.<task_id>"``. Changing it starts a
        new enrollment.
    :param baseline_version: Version treated as already delivered when no checkpoint exists. When
        ``None`` (the default), the first run publishes an ``enrollment`` event for the current version.
    :param ignored_operations: Delta history operations that do not count as a change. Defaults to
        ``OPTIMIZE``, ``VACUUM START`` and ``VACUUM END``. Pass an empty collection to publish for
        every commit.
    :param max_history_rows: Largest ``DESCRIBE HISTORY`` window read to prove that all commits after
        the checkpoint are ignored operations. When the window or the retained history does not
        reach the checkpoint, the operator publishes rather than risk missing a data change.
    :param on_recreation: What to do when the table id differs from the checkpoint, or the version
        moved backwards, which means the table was dropped and recreated or replaced by a rename.
        ``"publish"`` publishes a ``recreated`` event and checkpoints the new table. ``"fail"``
        raises and leaves the checkpoint untouched.
    :param databricks_conn_id: Reference to :ref:`Databricks connection id<howto/connection:databricks>`.
    :param http_path: HTTP path of a Databricks SQL warehouse or all-purpose cluster.
    :param sql_warehouse_name: Name of a Databricks SQL warehouse, used when ``http_path`` is not set.
    :param session_configuration: Spark session parameters.
    :param http_headers: (k, v) pairs set as HTTP headers on every request.
    :param client_parameters: Additional Databricks SQL connector parameters.
    :param query_tags: Query tags attached to the Databricks SQL queries. (templated)
    :param include_airflow_query_tags: If True, add Airflow Dag/task/run metadata as query tags.
    """

    template_fields: Sequence[str] = ("databricks_conn_id", "http_headers", "query_tags")

    def __init__(
        self,
        *,
        unity_table: UnityTableIdentity,
        checkpoint_key: str | None = None,
        baseline_version: int | None = None,
        ignored_operations: Collection[str] | None = None,
        max_history_rows: int = 1000,
        on_recreation: Literal["publish", "fail"] = "publish",
        databricks_conn_id: str = DatabricksSqlHook.default_conn_name,
        http_path: str | None = None,
        sql_warehouse_name: str | None = None,
        session_configuration: dict[str, str] | None = None,
        http_headers: list[tuple[str, str]] | None = None,
        client_parameters: dict[str, Any] | None = None,
        query_tags: dict[str, str | None] | None = None,
        include_airflow_query_tags: bool = True,
        **kwargs,
    ) -> None:
        if baseline_version is not None and (
            isinstance(baseline_version, bool)
            or not isinstance(baseline_version, int)
            or baseline_version < 0
        ):
            raise ValueError(f"baseline_version must be a non-negative int, got {baseline_version!r}.")
        if max_history_rows < 1:
            raise ValueError(f"max_history_rows must be at least 1, got {max_history_rows!r}.")
        if on_recreation not in ("publish", "fail"):
            raise ValueError(f"on_recreation must be 'publish' or 'fail', got {on_recreation!r}.")
        if checkpoint_key is not None and not checkpoint_key:
            raise ValueError("checkpoint_key must not be empty.")

        asset = unity_table.to_asset()
        if "outlets" not in kwargs:
            kwargs["outlets"] = [asset]
        inlets = list(kwargs.pop("inlets", None) or [])
        if not any(getattr(inlet, "uri", None) == asset.uri for inlet in inlets):
            inlets.append(asset)
        kwargs["inlets"] = inlets
        # Concurrent runs of one producer read the same checkpoint and publish duplicates.
        kwargs.setdefault("max_active_tis_per_dag", 1)
        super().__init__(**kwargs)

        self.unity_table = unity_table
        self._checkpoint_key = checkpoint_key
        self.baseline_version = baseline_version
        self.ignored_operations = tuple(
            DEFAULT_IGNORED_OPERATIONS if ignored_operations is None else ignored_operations
        )
        self._ignored = frozenset(op.casefold() for op in self.ignored_operations)
        self.max_history_rows = max_history_rows
        self.on_recreation = on_recreation
        self.databricks_conn_id = databricks_conn_id
        self.http_path = http_path
        self.sql_warehouse_name = sql_warehouse_name
        self.session_configuration = session_configuration
        self.http_headers = http_headers
        self.client_parameters = client_parameters or {}
        self.query_tags = query_tags or {}
        self.include_airflow_query_tags = include_airflow_query_tags

    @property
    def checkpoint_key(self) -> str:
        return self._checkpoint_key or f"{self.dag_id}.{self.task_id}"

    @cached_property
    def asset(self) -> Asset:
        return self.unity_table.to_asset()

    @cached_property
    def table_name(self) -> str:
        t = self.unity_table
        return ".".join(_quote_identifier(p) for p in (t.catalog, t.schema, t.table))

    @cached_property
    def hook(self) -> DatabricksSqlHook:
        return DatabricksSqlHook(
            self.databricks_conn_id,
            http_path=self.http_path,
            sql_endpoint_name=self.sql_warehouse_name,
            session_configuration=self.session_configuration,
            http_headers=self.http_headers,
            caller="DatabricksDeltaTableVersionProducerOperator",
            **self.client_parameters,
        )

    def execute(self, context: Context) -> dict[str, Any]:
        hook = self.hook
        if (hook.host or "").lower() != self.unity_table.host:
            raise ValueError(
                f"Databricks connection host {hook.host!r} does not match "
                f"unity_table host {self.unity_table.host!r}."
            )
        hook.query_tags = build_query_tags(context, self.query_tags, self.include_airflow_query_tags)

        checkpoint = self.read_checkpoint(context)
        table_id = self._describe_table_id()
        latest = self._read_history(limit=1)
        if not latest:
            raise DatabricksDeltaTableError(f"DESCRIBE HISTORY returned no rows for {self.table_name}.")
        head = latest[0]

        change: ChangeKind
        previous: int | None
        if checkpoint is None:
            if self.baseline_version is None:
                change, previous = "enrollment", None
            elif head.version < self.baseline_version:
                raise DatabricksDeltaTableError(
                    f"Delta table {self.table_name} is at version {head.version}, "
                    f"older than baseline_version {self.baseline_version}."
                )
            else:
                previous = self.baseline_version
                change = "new_version"
        elif (
            checkpoint.table_id is not None and table_id is not None and checkpoint.table_id != table_id
        ) or head.version < checkpoint.version:
            if self.on_recreation == "fail":
                raise DatabricksDeltaTableError(
                    f"Delta table {self.table_name} changed identity since the checkpoint "
                    f"(table id {checkpoint.table_id!r} version {checkpoint.version} -> "
                    f"table id {table_id!r} version {head.version}). Was it dropped and recreated?"
                )
            change, previous = "recreated", checkpoint.version
        else:
            change, previous = "new_version", checkpoint.version

        if change == "new_version" and previous is not None:
            if head.version == previous:
                raise AirflowSkipException(
                    f"Delta table {self.table_name} is still at version {previous}; no event published."
                )
            if not self._has_relevant_commit(head, previous):
                raise AirflowSkipException(
                    f"Delta table {self.table_name} only has {sorted(self.ignored_operations)} commits "
                    f"after version {previous}; no event published."
                )

        extra = self._build_extra(head=head, table_id=table_id, previous=previous, change=change)
        for outlet in self.outlets:
            context["outlet_events"][outlet].extra = extra
        self.log.info(
            "Publishing %s event for %s at Delta version %s (previous %s).",
            change,
            self.asset.uri,
            head.version,
            previous,
        )
        return extra

    def read_checkpoint(self, context: Context) -> DeltaCheckpoint | None:
        """Return the newest checkpoint for ``checkpoint_key`` from the asset's events, if any."""
        accessor = context["inlet_events"][self.asset]
        events: Iterable[Any]
        if all(hasattr(accessor, name) for name in ("extra", "ascending", "limit")):
            # Airflow versions that filter asset events on the API server.
            events = accessor.extra(CHECKPOINT_KEY_EXTRA, self.checkpoint_key).ascending(False).limit(1)
        else:
            events = reversed(accessor)
        for event in events:
            extra = event.extra or {}
            if extra.get(CHECKPOINT_KEY_EXTRA) != self.checkpoint_key:
                continue
            version = extra.get("version")
            if isinstance(version, bool) or not isinstance(version, int):
                raise DatabricksDeltaTableError(
                    f"Checkpoint event for {self.asset.uri} with key {self.checkpoint_key!r} "
                    f"has no integer version: {extra!r}."
                )
            table_id = extra.get("delta_table_id")
            return DeltaCheckpoint(version=version, table_id=str(table_id) if table_id is not None else None)
        return None

    def _describe_table_id(self) -> str | None:
        rows = self.hook.run(f"DESCRIBE DETAIL {self.table_name}", handler=fetch_all_handler)
        if not rows:
            raise DatabricksDeltaTableError(f"DESCRIBE DETAIL returned no rows for {self.table_name}.")
        detail = _row_to_dict(rows[0])
        table_format = detail.get("format")
        if table_format is not None and str(table_format).lower() != "delta":
            raise DatabricksDeltaTableError(
                f"{self.table_name} is a {table_format!r} table; only Delta tables have versions."
            )
        table_id = detail.get("id")
        return str(table_id) if table_id is not None else None

    def _read_history(self, *, limit: int) -> list[_HistoryEntry]:
        rows = self.hook.run(
            f"DESCRIBE HISTORY {self.table_name} LIMIT {int(limit)}", handler=fetch_all_handler
        )
        entries = []
        for row in rows or []:
            data = _row_to_dict(row)
            timestamp = data.get("timestamp")
            entries.append(
                _HistoryEntry(
                    version=int(data["version"]),
                    operation=data.get("operation"),
                    timestamp=timestamp.isoformat()
                    if isinstance(timestamp, date)
                    else (str(timestamp) if timestamp is not None else None),
                )
            )
        return sorted(entries, key=lambda e: e.version, reverse=True)

    def _has_relevant_commit(self, head: _HistoryEntry, previous: int) -> bool:
        """Whether any commit in ``(previous, head.version]`` is not an ignored operation."""
        if not self._ignored:
            return True
        if not self._is_ignored(head):
            return True
        needed = head.version - previous
        if needed > self.max_history_rows:
            self.log.warning(
                "%s commits since version %s exceed max_history_rows=%s; publishing without proof "
                "that they are all ignored operations.",
                needed,
                previous,
                self.max_history_rows,
            )
            return True
        window = [e for e in self._read_history(limit=needed) if e.version > previous]
        if len(window) < needed:
            self.log.warning(
                "Delta history after version %s is incomplete (%s of %s commits retained); "
                "publishing without proof that they are all ignored operations.",
                previous,
                len(window),
                needed,
            )
            return True
        return any(not self._is_ignored(e) for e in window)

    def _is_ignored(self, entry: _HistoryEntry) -> bool:
        return entry.operation is not None and entry.operation.casefold() in self._ignored

    def _build_extra(
        self, *, head: _HistoryEntry, table_id: str | None, previous: int | None, change: ChangeKind
    ) -> dict[str, Any]:
        t = self.unity_table
        return {
            "version": head.version,
            "observed_version": head.version,
            "previous_version": previous,
            "table_identity": {"host": t.host, "catalog": t.catalog, "schema": t.schema, "table": t.table},
            "operation": head.operation,
            "timestamp": head.timestamp,
            "change": change,
            "delta_table_id": table_id,
            "dedup_id": f"{self.asset.uri}@{table_id or '-'}:{head.version}",
            CHECKPOINT_KEY_EXTRA: self.checkpoint_key,
        }
