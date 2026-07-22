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
    conf,
)
from airflow.providers.standard.exceptions import UnexpectedAssetEventTriggerEventError
from airflow.providers.standard.triggers.asset import (
    AssetEventTrigger,
    _count_satisfied,
    _fetch_asset_events,
    _serialize_events,
)
from airflow.providers.standard.version_compat import AIRFLOW_V_3_4_PLUS

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from datetime import datetime

    from airflow.providers.common.compat.sdk import Context


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
        list of asset events and must return a list. In ``deferrable`` mode it must be a top-level
        importable function (not a lambda, nested function, or bound method) that lives in a module
        installed on the triggerer -- a function defined in a DAG file will import on the worker but
        typically fail to import in the triggerer process. Its return value must be JSON-serializable,
        because it runs in the triggerer and its result is returned via the trigger event. It should
        be **idempotent / side-effect free**: in deferrable mode it runs once during the initial
        synchronous poke and again on every fetch in the triggerer.
    :param deferrable: Run the sensor in deferrable mode.
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
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
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
        if not (name or uri or alias_name):
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
        self.deferrable = deferrable

    def _apply_process_result(self, events: list[Any]) -> list[Any]:
        if self.process_result is None:
            return events
        func = self.process_result if callable(self.process_result) else import_string(self.process_result)
        return func(events)

    def _resolve_process_result_path(self) -> str | None:
        """Resolve ``process_result`` to a dotted import path for use in the trigger."""
        if self.process_result is None:
            return None
        if isinstance(self.process_result, str):
            path = self.process_result
        else:
            func = self.process_result
            module = getattr(func, "__module__", "")
            qualname = getattr(func, "__qualname__", "")
            if not module or not qualname or "<" in qualname:
                raise ValueError(
                    "In deferrable mode, `process_result` must be a top-level importable function "
                    "(or a dotted import path string), not a lambda or a nested/local function."
                )
            path = f"{module}.{qualname}"
        # Fail fast at defer time rather than in the triggerer: e.g. a bound method resolves to
        # ``module.Class.method`` which ``import_string`` cannot import. This validates importability
        # on the *worker*; the callable must also live in a module installed on the triggerer (a
        # function defined in a DAG file may import here yet fail there).
        try:
            import_string(path)
        except Exception as exc:
            raise ValueError(
                "In deferrable mode, `process_result` must be importable by a dotted path, but the "
                f"resolved path {path!r} is not importable ({exc}). Pass a top-level function "
                "(defined in an installed module, not a DAG file) or an explicit import-path string."
            ) from exc
        return path

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

    def execute(self, context: Context) -> Any:
        if not self.deferrable:
            return super().execute(context=context)

        # Deferrable path: do an initial synchronous check to avoid deferring needlessly.
        poke_result = self.poke(context)
        if poke_result:
            return poke_result.xcom_value if isinstance(poke_result, PokeReturnValue) else None

        self.defer(
            trigger=AssetEventTrigger(
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
                expected_count=self.expected_count,
                process_result_path=self._resolve_process_result_path(),
                poke_interval=self.poke_interval,
            ),
            method_name="execute_complete",
            timeout=self.timeout,
        )

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> Any:
        """Return the processed events once the trigger fires successfully."""
        if event and event.get("status") == "success":
            return event.get("events")
        raise UnexpectedAssetEventTriggerEventError(
            f"AssetEventSensor received an unexpected trigger event: {event}"
        )
