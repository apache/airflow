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
from datetime import timedelta
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Protocol, cast

from pydantic import BaseModel

from airflow.providers.common.compat.sdk import (
    AirflowOptionalProviderFeatureException,
    BaseSensorOperator,
    PokeReturnValue,
)
from airflow.providers.standard.version_compat import AIRFLOW_V_3_2_PLUS, AIRFLOW_V_3_4_PLUS

if not AIRFLOW_V_3_2_PLUS:
    raise AirflowOptionalProviderFeatureException("AssetEventSensor needs Airflow 3.2+.")

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from datetime import datetime

    from airflow.providers.common.compat.sdk import Context
    from airflow.sdk.definitions.asset import Asset, AssetAlias, AssetRef
    from airflow.sdk.execution_time.comms import AssetEventResult
    from airflow.sdk.execution_time.context import InletEventsAccessor


def _serialize_events(events: list[Any]) -> list[Any]:
    """
    Convert asset events Pydantic models to JSON-compatible dicts.

    This keeps the XCom value plain JSON, rather than relying on how XCom
    serializes pydantic models.
    """
    return [e.model_dump(mode="json") if isinstance(e, BaseModel) else e for e in events]


class AssetEventTimeBound(Protocol):
    """
    A time bound that Airflow resolves during template rendering.

    Implement ``resolve(context)`` to return a datetime or ISO string. Custom
    objects do not need to inherit this protocol.
    The result must stay fixed across reschedules of the same Dag run.
    """

    def resolve(self, context: Context) -> datetime | str:
        """Return the event timestamp bound for this task's runtime context."""
        ...


class _ContextTimeBound(AssetEventTimeBound):
    _context_key: ClassVar[str]

    def __init__(self, *, lookback: timedelta = timedelta(0)) -> None:
        if lookback < timedelta(0):
            raise ValueError("`lookback` must be non-negative.")
        self.lookback = lookback

    def __repr__(self) -> str:
        return f"{type(self).__name__}(lookback={self.lookback!r})"

    def _get_timestamp(self, context: Context) -> datetime | None:
        return cast("datetime | None", context.get(self._context_key))

    def resolve(self, context: Context) -> datetime:
        """Resolve the bound during template rendering, before the sensor polls."""
        timestamp = self._get_timestamp(context)
        if timestamp is None:
            raise ValueError(f"{type(self).__name__} requires {self._context_key!r} in the task context.")
        return timestamp - self.lookback


class RunStart(_ContextTimeBound):
    """
    Bound events by the Dag run's actual start time, including for backfills.

    :param lookback: Non-negative duration to subtract from ``dag_run.start_date``.
    """

    _context_key = "dag_run.start_date"

    def _get_timestamp(self, context: Context) -> datetime | None:
        dag_run = context.get("dag_run")
        return dag_run.start_date if dag_run is not None else None


class LogicalDate(_ContextTimeBound):
    """
    Bound events by the Dag run's logical date.

    :param lookback: Non-negative duration to subtract from ``logical_date``.
    """

    _context_key = "logical_date"


class DataIntervalStart(_ContextTimeBound):
    """
    Bound events by the start of the Dag run's data interval.

    :param lookback: Non-negative duration to subtract from ``data_interval_start``.
    """

    _context_key = "data_interval_start"


class DataIntervalEnd(_ContextTimeBound):
    """
    Bound events by the end of the Dag run's data interval.

    :param lookback: Non-negative duration to subtract from ``data_interval_end``.
    """

    _context_key = "data_interval_end"


class AssetEventSensor(BaseSensorOperator):
    """
    Wait for asset events matching the given filters to reach an expected count.

    The sensor fetches events of an asset, asset reference, or asset alias
    matching the supplied filters, optionally applies a ``process_result``
    callable to transform, deduplicate or filter them, and succeeds once the
    resulting number of events satisfies ``expected_count``.

    This sensor requires Apache Airflow 3.2+. The ``partition_key``,
    ``partition_key_regexp_pattern`` and ``extra`` filters require Airflow 3.4+.

    :param asset: The :class:`~airflow.sdk.Asset`, :meth:`~airflow.sdk.Asset.ref`,
        or :class:`~airflow.sdk.AssetAlias` to wait on. It is declared as an
        inlet. A reference must resolve to an existing asset when the task
        starts. The target is static; event filters can be templated.
    :param after: Only include events at or after this timestamp.
    :param before: Only include events at or before this timestamp.
        Both bounds accept a datetime, ISO string, any object implementing
        :class:`AssetEventTimeBound`, or a template callable accepting
        ``context`` and ``jinja_env``. Airflow
        resolves them before polling. Custom callables must return stable
        bounds across reschedules.
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
    :param process_result: A callable applied to the fetched events before the
        count check, to transform, deduplicate or filter them. It receives the
        list of asset events and must return a list. It runs on every poke, so
        it should be idempotent and free of side-effects. Its return value must
        be JSON-serializable to be pushed into XCom.
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
        after: datetime | str | AssetEventTimeBound | Callable[..., datetime | str] | None = None,
        before: datetime | str | AssetEventTimeBound | Callable[..., datetime | str] | None = None,
        ascending: bool = True,
        limit: int | None = None,
        partition_key: str | None = None,
        partition_key_regexp_pattern: str | None = None,
        extra: dict[str, str] | None = None,
        expected_count: int = 1,
        count_policy: Literal["minimum", "exact"] = "minimum",
        process_result: Callable[[list[AssetEventResult]], list[Any]] | None = None,
        **kwargs,
    ) -> None:
        if not AIRFLOW_V_3_4_PLUS and (
            partition_key is not None or partition_key_regexp_pattern is not None or extra is not None
        ):
            raise AirflowOptionalProviderFeatureException(
                "AssetEventSensor filters partition_key, partition_key_regexp_pattern and extra "
                "need Airflow 3.4+."
            )
        super().__init__(**kwargs)
        if expected_count < 0:
            raise ValueError(f"`expected_count` must be a non-negative integer, got {expected_count}.")
        if limit is not None and limit <= 0:
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

    def _get_accessor(self, context: Context) -> InletEventsAccessor:
        # The mapped operator's inlets don't include the asset added in init.
        # TODO: Remove this once the min Airflow refreshes after unmapping.
        with contextlib.suppress(KeyError):
            return context["inlet_events"][self.asset]
        from airflow.sdk.execution_time.context import InletEventsAccessors

        return InletEventsAccessors([self.asset])[self.asset]

    def poke(self, context: Context) -> PokeReturnValue:
        # Template rendering replaces bound objects and callables before polling.
        after = cast("datetime | str | None", self.after)
        before = cast("datetime | str | None", self.before)
        accessor = self._get_accessor(context)
        if after is not None:
            accessor.after(after if isinstance(after, str) else after.isoformat())
        if before is not None:
            accessor.before(before if isinstance(before, str) else before.isoformat())
        accessor.ascending(self.ascending)
        if self.limit is not None:
            accessor.limit(self.limit)
        if self.partition_key is not None:
            accessor.partition_key(self.partition_key)
        if self.partition_key_regexp_pattern is not None:
            accessor.partition_key_regexp_pattern(self.partition_key_regexp_pattern)
        if self.extra is not None:
            for key, value in self.extra.items():
                accessor.extra(key, value)
        processed = list(accessor)
        if self.process_result is not None:
            processed = self.process_result(processed)
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
