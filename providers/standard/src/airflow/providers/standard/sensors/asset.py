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

import contextlib
from typing import TYPE_CHECKING, Any, Literal

from airflow.providers.common.compat.module_loading import import_string
from airflow.providers.common.compat.sdk import BaseSensorOperator, PokeReturnValue
from airflow.providers.standard.version_compat import AIRFLOW_V_3_4_PLUS

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from datetime import datetime

    from airflow.providers.common.compat.sdk import Context
    from airflow.sdk.definitions.asset import Asset, AssetAlias, AssetRef
    from airflow.sdk.execution_time.context import InletEventsAccessor


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

    The sensor fetches events of an asset, asset reference, or asset alias
    matching the supplied filters, optionally applies a ``process_result``
    callable to transform, deduplicate or filter them, and succeeds once the
    resulting number of events satisfies ``expected_count``.

    This sensor requires Apache Airflow 3.4+ because the ``partition_key``,
    ``partition_key_regexp_pattern`` and ``extra`` asset-event filters are only
    available there.

    :param asset: The :class:`~airflow.sdk.Asset`, :meth:`~airflow.sdk.Asset.ref`,
        or :class:`~airflow.sdk.AssetAlias` to wait on. It is declared as an
        inlet. A reference must resolve to an existing asset when the task
        starts. The target is static; event filters can be templated.
    :param after: Only include events at or after this timestamp.
    :param before: Only include events at or before this timestamp.
    :param ascending: Whether events are returned in ascending timestamp order.
    :param limit: Positive maximum number of events to fetch, before applying
        ``process_result``.
    :param partition_key: Filter by exact partition key match.
    :param partition_key_regexp_pattern: Filter by partition key regexp pattern.
    :param extra: Filter by key/value pairs contained in the event ``extra``.
    :param expected_count: The non-negative number of processed events required
        to succeed. Defaults to 1. If ``process_result`` is omitted, ``limit``
        must not be smaller, or must be greater with the ``"exact"`` policy.
    :param count_policy: ``"minimum"`` (the default) waits for at least
        ``expected_count`` events; ``"exact"`` requires exactly that number. To
        check for no events, use ``"exact"`` and zero.
    :param process_result: A callable (or a dotted import path to one) applied
        to the fetched events before the count check, to transform, deduplicate
        or filter them. It receives the list of asset events and must return a
        list. It runs on every poke, so it should be idempotent and free of
        side-effects. Its return value must be JSON-serializable to be pushed
        into XCom.
    """

    template_fields: Sequence[str] = (
        "partition_key",
        "partition_key_regexp_pattern",
        "extra",
        "after",
        "before",
    )

    def __init__(
        self,
        *,
        asset: Asset | AssetAlias | AssetRef,
        after: datetime | str | None = None,
        before: datetime | str | None = None,
        ascending: bool = True,
        limit: int | None = None,
        partition_key: str | None = None,
        partition_key_regexp_pattern: str | None = None,
        extra: dict[str, str] | None = None,
        expected_count: int = 1,
        count_policy: Literal["minimum", "exact"] = "minimum",
        process_result: Callable[[list[Any]], list[Any]] | str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if not AIRFLOW_V_3_4_PLUS:
            raise RuntimeError(
                "AssetEventSensor requires Apache Airflow 3.4+ because the asset event filters "
                "it relies on are only available from 3.4 onwards."
            )
        if not isinstance(expected_count, int) or isinstance(expected_count, bool):
            raise TypeError("`expected_count` must be an integer.")
        if expected_count < 0:
            raise ValueError(f"`expected_count` must be a non-negative integer, got {expected_count}.")
        if limit is not None:
            if not isinstance(limit, int) or isinstance(limit, bool):
                raise TypeError("`limit` must be an integer.")
            if limit <= 0:
                raise ValueError("`limit` must be positive.")
        if count_policy not in {"minimum", "exact"}:
            raise ValueError(f"`count_policy` must be 'minimum' or 'exact', got {count_policy!r}.")
        if process_result is None and limit is not None:
            if count_policy == "exact" and limit <= expected_count:
                raise ValueError(
                    "`limit` must be greater than `expected_count` with the exact count policy, "
                    "otherwise extra events cannot be detected."
                )
            if limit < expected_count:
                raise ValueError(
                    "`limit` must be at least `expected_count` when `process_result` is not provided."
                )
        self.asset = asset
        if asset not in self.inlets:
            self.inlets.append(asset)

        self.after = after
        self.before = before
        self.ascending = ascending
        self.limit = limit
        self.partition_key = partition_key
        self.partition_key_regexp_pattern = partition_key_regexp_pattern
        self.extra = extra
        self.expected_count = expected_count
        self.count_policy = count_policy
        self.process_result = process_result

    def _apply_process_result(self, events: list[Any]) -> list[Any]:
        if self.process_result is None:
            return events
        func = self.process_result if callable(self.process_result) else import_string(self.process_result)
        return func(events)

    def _get_accessor(self, context: Context) -> InletEventsAccessor:
        # The mapped operator's inlets don't include the asset added in init.
        # TODO: Remove this once the min Airflow refreshes after unmapping.
        with contextlib.suppress(KeyError):
            return context["inlet_events"][self.asset]
        from airflow.sdk.execution_time.context import InletEventsAccessors

        return InletEventsAccessors([self.asset])[self.asset]

    def poke(self, context: Context) -> PokeReturnValue:
        accessor = self._get_accessor(context)
        if self.after is not None:
            accessor.after(self.after if isinstance(self.after, str) else self.after.isoformat())
        if self.before is not None:
            accessor.before(self.before if isinstance(self.before, str) else self.before.isoformat())
        accessor.ascending(self.ascending)
        if self.limit is not None:
            accessor.limit(self.limit)
        if self.partition_key is not None:
            accessor.partition_key(self.partition_key)
        if self.partition_key_regexp_pattern is not None:
            accessor.partition_key_regexp_pattern(self.partition_key_regexp_pattern)
        if self.extra:
            for key, value in self.extra.items():
                accessor.extra(key, value)
        processed = self._apply_process_result(list(accessor))
        count = len(processed)
        done = (
            count >= self.expected_count if self.count_policy == "minimum" else count == self.expected_count
        )
        self.log.info(
            "Found %d matching asset events (expected %s, policy %s): %s",
            count,
            self.expected_count,
            self.count_policy,
            "condition met" if done else "still waiting",
        )
        return PokeReturnValue(is_done=done, xcom_value=_serialize_events(processed) if done else None)
