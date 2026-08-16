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

from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.module_loading import import_string
from airflow.providers.common.compat.sdk import (
    Asset,
    AssetAlias,
    BaseSensorOperator,
    PokeReturnValue,
)
from airflow.providers.standard.version_compat import AIRFLOW_V_3_4_PLUS

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from datetime import datetime

    from airflow.providers.common.compat.sdk import Context


def _count_satisfied(count: int, expected_count: int) -> bool:
    """
    Return whether the number of (processed) asset events satisfies the expectation.

    ``expected_count == -1`` means "at least one" (``count >= 1``); any other value
    requires an exact match (``count == expected_count``).
    """
    if expected_count == -1:
        return count >= 1
    return count == expected_count


def _fetch_asset_events(
    *,
    name: str | None,
    uri: str | None,
    alias_name: str | None,
    after: datetime | str | None,
    before: datetime | str | None,
    ascending: bool,
    limit: int | None,
    partition_key: str | None,
    partition_key_regexp_pattern: str | None,
    extra: dict[str, str] | None,
) -> list[Any]:
    """
    Fetch asset events matching the given filters.

    This uses the Task SDK :class:`InletEventsAccessor`, which lazily fetches events from
    the supervisor, so it can only run where the execution API is available (i.e. on a worker).
    """
    from airflow.sdk.execution_time.context import InletEventsAccessor

    accessor = InletEventsAccessor(asset_name=name, asset_uri=uri, alias_name=alias_name)
    if after is not None:
        accessor.after(after if isinstance(after, str) else after.isoformat())
    if before is not None:
        accessor.before(before if isinstance(before, str) else before.isoformat())
    accessor.ascending(ascending)
    if limit is not None:
        accessor.limit(limit)
    if partition_key is not None:
        accessor.partition_key(partition_key)
    if partition_key_regexp_pattern is not None:
        accessor.partition_key_regexp_pattern(partition_key_regexp_pattern)
    if extra:
        for key, value in extra.items():
            accessor.extra(key, value)
    return list(accessor)


def _serialize_events(events: list[Any]) -> list[Any]:
    """Serialize a list of (processed) asset events to JSON-safe values."""
    serialized: list[Any] = []
    for event in events:
        model_dump = getattr(event, "model_dump", None)
        if callable(model_dump):
            serialized.append(model_dump(mode="json"))
        else:
            serialized.append(event)
    return serialized


class AssetEventSensor(BaseSensorOperator):
    """
    Wait for asset events matching the given filters to reach an expected count.

    The sensor fetches asset events (by asset or asset alias) matching the supplied filters,
    optionally applies a ``process_result`` callable to transform, deduplicate or filter them,
    and succeeds once the resulting number of events satisfies ``expected_count``.

    This sensor requires Apache Airflow 3.4+ because the ``partition_key``,
    ``partition_key_regexp_pattern`` and ``extra`` asset-event filters are only available there.

    :param asset: The :class:`~airflow.sdk.Asset` or :class:`~airflow.sdk.AssetAlias` to wait on.
        As an alternative, pass ``name``/``uri``/``alias_name`` directly.
    :param name: The asset name to fetch events for.
    :param uri: The asset uri to fetch events for.
    :param alias_name: The asset alias name to fetch events for.
    :param after: Only include events at or after this timestamp.
    :param before: Only include events at or before this timestamp.
    :param ascending: Whether events are returned in ascending timestamp order.
    :param limit: Maximum number of events to fetch.
    :param partition_key: Filter by exact partition key match.
    :param partition_key_regexp_pattern: Filter by partition key regexp pattern.
    :param extra: Filter by key/value pairs contained in the event ``extra`` field.
    :param expected_count: The number of events required to succeed. ``-1`` (the default) means
        "at least one" (``count >= 1``); ``0`` means "exactly zero"; any other positive value
        requires an exact match. Note that if ``limit`` is set below an exact ``expected_count``
        the condition can never be satisfied (the sensor will wait until it times out).
    :param process_result: A callable (or a dotted import path to one) applied to the fetched
        events before the count check, to transform, deduplicate or filter them. It receives the
        list of asset events and must return a list. It runs on every poke, so it should be
        **idempotent / side-effect free**, and its return value must be JSON-serializable to be
        pushed to XCom.
    """

    template_fields: Sequence[str] = (
        "name",
        "uri",
        "alias_name",
        "partition_key",
        "partition_key_regexp_pattern",
        "extra",
        "after",
        "before",
    )

    def __init__(
        self,
        *,
        asset: Asset | AssetAlias | None = None,
        name: str | None = None,
        uri: str | None = None,
        alias_name: str | None = None,
        after: datetime | str | None = None,
        before: datetime | str | None = None,
        ascending: bool = True,
        limit: int | None = None,
        partition_key: str | None = None,
        partition_key_regexp_pattern: str | None = None,
        extra: dict[str, str] | None = None,
        expected_count: int = -1,
        process_result: Callable[[list[Any]], list[Any]] | str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if not AIRFLOW_V_3_4_PLUS:
            raise RuntimeError(
                "AssetEventSensor requires Apache Airflow 3.4+ because the asset event filters "
                "it relies on are only available from 3.4 onwards."
            )
        if asset is not None:
            if isinstance(asset, AssetAlias):
                alias_name = asset.name
            elif isinstance(asset, Asset):
                name = asset.name
                uri = asset.uri
            else:
                raise TypeError(f"`asset` must be an Asset or AssetAlias, got {type(asset).__name__}")
        if name is None and uri is None and alias_name is None:
            raise ValueError("One of `asset`, `name`, `uri`, or `alias_name` must be provided.")
        if expected_count < -1:
            raise ValueError(
                f"`expected_count` must be -1 (at least one) or a non-negative integer, got {expected_count}."
            )

        self.name = name
        self.uri = uri
        self.alias_name = alias_name
        self.after = after
        self.before = before
        self.ascending = ascending
        self.limit = limit
        self.partition_key = partition_key
        self.partition_key_regexp_pattern = partition_key_regexp_pattern
        self.extra = extra
        self.expected_count = expected_count
        self.process_result = process_result

    def _apply_process_result(self, events: list[Any]) -> list[Any]:
        if self.process_result is None:
            return events
        func = self.process_result if callable(self.process_result) else import_string(self.process_result)
        return func(events)

    def poke(self, context: Context) -> PokeReturnValue:
        events = _fetch_asset_events(
            name=self.name,
            uri=self.uri,
            alias_name=self.alias_name,
            after=self.after,
            before=self.before,
            ascending=self.ascending,
            limit=self.limit,
            partition_key=self.partition_key,
            partition_key_regexp_pattern=self.partition_key_regexp_pattern,
            extra=self.extra,
        )
        processed = self._apply_process_result(events)
        count = len(processed)
        done = _count_satisfied(count, self.expected_count)
        self.log.info(
            "Found %d matching asset events (expected %s): %s",
            count,
            self.expected_count,
            "condition met" if done else "still waiting",
        )
        return PokeReturnValue(is_done=done, xcom_value=_serialize_events(processed) if done else None)
