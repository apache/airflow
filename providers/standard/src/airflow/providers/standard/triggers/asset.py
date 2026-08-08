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

from asgiref.sync import sync_to_async

from airflow.providers.common.compat.module_loading import import_string
from airflow.triggers.base import BaseTrigger, TriggerEvent

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from datetime import datetime


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

    This uses the Task SDK :class:`InletEventsAccessor`, which lazily fetches events
    from the supervisor. It is a blocking call and must be wrapped with
    ``sync_to_async`` when used from within the triggerer.
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


class AssetEventTrigger(BaseTrigger):
    """
    A trigger that waits until asset events matching the given filters reach an expected count.

    The trigger periodically fetches asset events (by asset name/uri or alias) matching the
    supplied filters, optionally applies a ``process_result`` callable to transform, deduplicate
    or filter them, and fires once the resulting count satisfies ``expected_count``.

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
    :param expected_count: The number of events required to succeed. ``-1`` means at least one.
    :param process_result_path: Dotted import path to a callable applied to the fetched events
        before the count check. Its return value must be JSON-serializable.
    :param poke_interval: Number of seconds to wait between fetches.
    """

    def __init__(
        self,
        *,
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
        process_result_path: str | None = None,
        poke_interval: float = 60,
    ) -> None:
        super().__init__()
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
        self.process_result_path = process_result_path
        self.poke_interval = poke_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.standard.triggers.asset.AssetEventTrigger",
            {
                "name": self.name,
                "uri": self.uri,
                "alias_name": self.alias_name,
                "after": self.after,
                "before": self.before,
                "ascending": self.ascending,
                "limit": self.limit,
                "partition_key": self.partition_key,
                "partition_key_regexp_pattern": self.partition_key_regexp_pattern,
                "extra": self.extra,
                "expected_count": self.expected_count,
                "process_result_path": self.process_result_path,
                "poke_interval": self.poke_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        process_result = import_string(self.process_result_path) if self.process_result_path else None
        while True:
            events = await sync_to_async(_fetch_asset_events)(
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
            if process_result is not None:
                # ``process_result`` may be an ordinary (non-async) callable, so run it in a
                # separate thread to avoid blocking the triggerer event loop.
                events = await sync_to_async(process_result, thread_sensitive=False)(events)
            count = len(events)
            if _count_satisfied(count, self.expected_count):
                self.log.info("Found %d matching asset events; firing trigger.", count)
                yield TriggerEvent({"status": "success", "events": _serialize_events(events)})
                return
            self.log.info(
                "Found %d matching asset events, expected %s; sleeping %s seconds.",
                count,
                self.expected_count,
                self.poke_interval,
            )
            await asyncio.sleep(self.poke_interval)
