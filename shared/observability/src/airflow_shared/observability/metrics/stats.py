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

import logging
import os
import re
import socket
from collections.abc import Callable
from contextlib import AbstractContextManager, ExitStack
from typing import TYPE_CHECKING, Any, cast

from .base_stats_logger import NoStatsLogger
from .metrics_registry import MetricsRegistry

if TYPE_CHECKING:
    from .base_stats_logger import StatsLogger
    from .protocols import DeltaType

log = logging.getLogger(__name__)

_VALID_STAT_NAME_CHARS_RE = re.compile(r"^[a-zA-Z0-9_.-]+$")
_INVALID_STAT_NAME_CHARS_RE = re.compile(r"[^a-zA-Z0-9_.-]")

# Module-level singleton state.
_factory: Callable[[], StatsLogger | NoStatsLogger] | None = None
_backend: StatsLogger | NoStatsLogger | None = None
_instance_pid: int | None = None
_export_legacy_names: bool = True
_registry: MetricsRegistry = MetricsRegistry()


def normalize_name_for_stats(name: str, log_warning: bool = True) -> str:
    """
    Normalize a name for stats reporting by replacing invalid characters.

    Stats names must only contain ASCII alphabets, numbers, underscores, dots, and dashes.
    Invalid characters are replaced with underscores.

    :param name: The name to normalize
    :param log_warning: Whether to log a warning when normalization occurs
    :return: Normalized name safe for stats reporting
    """
    if _VALID_STAT_NAME_CHARS_RE.match(name):
        return name

    normalized = _INVALID_STAT_NAME_CHARS_RE.sub("_", name)

    if log_warning:
        log.warning(
            "Name '%s' contains invalid characters for stats reporting. "
            "Reporting stats with normalized name '%s'.",
            name,
            normalized,
        )

    return normalized


def initialize(
    *,
    factory: Callable[[], StatsLogger | NoStatsLogger],
    export_legacy_names: bool = True,
) -> None:
    """Initialize the stats module with a backend factory and legacy name configuration."""
    global _factory, _backend, _instance_pid, _export_legacy_names
    _factory = factory
    _backend = None
    _instance_pid = None
    _export_legacy_names = export_legacy_names


def _get_backend() -> StatsLogger | NoStatsLogger:
    """Return the current backend, re-initializing if the process has been forked."""
    global _backend, _instance_pid
    current_pid = os.getpid()

    if _backend is not None and _instance_pid != current_pid:
        log.info(
            "Stats backend was created in PID %s but accessed in PID %s. Re-initializing.",
            _instance_pid,
            current_pid,
        )
        _backend = None
        _instance_pid = None

    if _backend is None:
        factory = _factory if _factory is not None else NoStatsLogger
        try:
            _backend = factory()
            _instance_pid = current_pid
        except (socket.gaierror, ImportError) as e:
            log.error("Could not configure StatsClient: %s, using NoStatsLogger instead.", e)
            _backend = NoStatsLogger()
            _instance_pid = current_pid

    return _backend


def _get_legacy_stat(stat: str, variables: dict[str, Any]) -> str | None:
    """
    Look up and format the legacy name for a metric from the registry.

    Returns the formatted legacy name, or None if the metric has no legacy name.
    Raises ValueError if the metric is not in the registry or required variables are missing.
    """
    stat_from_registry = _registry.get(name=stat)

    if not stat_from_registry:
        raise ValueError(
            f"Metric '{stat}' not found in the registry. Add the metric to the YAML file before using it."
        )

    legacy_name = stat_from_registry.get("legacy_name", "-")

    if legacy_name == "-":
        return None

    required_vars = stat_from_registry.get("name_variables", [])
    missing_vars = set(required_vars) - set(variables.keys())
    if missing_vars:
        raise ValueError(
            f"Missing required variables for metric '{stat}': {sorted(missing_vars)}. "
            f"Required variables found in the registry: {required_vars}. "
            f"Provided variables: {sorted(variables.keys())}. "
            f"Provide all required variables."
        )

    legacy_vars = {k: variables[k] for k in required_vars if k in variables}
    return legacy_name.format(**legacy_vars)


def _defined(**kwargs: Any) -> dict[str, Any]:
    """Return only kwargs with a meaningful (non-None, non-empty) value."""
    result = {}
    for k, v in kwargs.items():
        if v is None:
            continue
        try:
            if len(v) == 0:
                continue
        except TypeError:
            pass  # Numerics and bools don't have len, always include
        result[k] = v
    return result


def _merged(
    tags: dict[str, Any] | None,
    legacy_name_tags: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Merge tags and legacy_name_tags into a single dict, returning None if both are empty."""
    if not tags and not legacy_name_tags:
        return None
    merged: dict[str, Any] = {}
    if tags:
        merged.update(tags)
    if legacy_name_tags:
        merged.update(legacy_name_tags)
    return merged or None


def _emit_legacy(stat: str, legacy_name_tags: dict[str, Any]) -> str:
    """Return the formatted legacy stat name, raising ValueError if it has none."""
    legacy_stat = _get_legacy_stat(stat, legacy_name_tags)
    if legacy_stat is None:
        raise ValueError(f"Stat '{stat}' doesn't have a legacy name registered in the YAML file.")
    return legacy_stat


def incr(
    stat: str,
    count: int | None = None,
    rate: int | float | None = None,
    *,
    tags: dict[str, Any] | None = None,
    legacy_name_tags: dict[str, Any] | None = None,
) -> None:
    """Increment a counter metric."""
    if _export_legacy_names and legacy_name_tags is not None:
        _get_backend().incr(
            _emit_legacy(stat, legacy_name_tags), **_defined(count=count, rate=rate, tags=tags)
        )
    _get_backend().incr(stat, **_defined(count=count, rate=rate, tags=_merged(tags, legacy_name_tags)))


def decr(
    stat: str,
    count: int | None = None,
    rate: int | float | None = None,
    *,
    tags: dict[str, Any] | None = None,
    legacy_name_tags: dict[str, Any] | None = None,
) -> None:
    """Decrement a counter metric."""
    if _export_legacy_names and legacy_name_tags is not None:
        _get_backend().decr(
            _emit_legacy(stat, legacy_name_tags), **_defined(count=count, rate=rate, tags=tags)
        )
    _get_backend().decr(stat, **_defined(count=count, rate=rate, tags=_merged(tags, legacy_name_tags)))


def gauge(
    stat: str,
    value: float,
    rate: int | float | None = None,
    delta: bool | None = None,
    *,
    tags: dict[str, Any] | None = None,
    legacy_name_tags: dict[str, Any] | None = None,
) -> None:
    """Set a gauge metric."""
    if _export_legacy_names and legacy_name_tags is not None:
        _get_backend().gauge(
            _emit_legacy(stat, legacy_name_tags), value, **_defined(rate=rate, delta=delta, tags=tags)
        )
    _get_backend().gauge(
        stat, value, **_defined(rate=rate, delta=delta, tags=_merged(tags, legacy_name_tags))
    )


def timing(
    stat: str,
    dt: DeltaType,
    *,
    tags: dict[str, Any] | None = None,
    legacy_name_tags: dict[str, Any] | None = None,
) -> None:
    """Record a timing metric."""
    if _export_legacy_names and legacy_name_tags is not None:
        _get_backend().timing(_emit_legacy(stat, legacy_name_tags), dt, **_defined(tags=tags))
    _get_backend().timing(stat, dt, **_defined(tags=_merged(tags, legacy_name_tags)))


def timer(
    stat: str | None = None,
    tags: dict[str, Any] | None = None,
    legacy_name_tags: dict[str, Any] | None = None,
    **kwargs,
) -> AbstractContextManager[Any]:
    """
    Context manager that times a block and emits a timer metric.

    When ``stat`` is None, returns the backend timer directly for stopwatch
    use (duration is available but no metric is emitted).

    When ``legacy_name_tags`` is provided and legacy name export is enabled, both the
    legacy metric name and the modern name are timed simultaneously via an
    ExitStack. Otherwise the backend timer is returned directly, preserving the
    ability to access ``timer.duration`` on the returned object.
    """
    if stat is None:
        return _get_backend().timer()

    modern_kw: dict[str, Any] = {**kwargs}
    merged = _merged(tags, legacy_name_tags)
    if merged:
        modern_kw["tags"] = merged

    if _export_legacy_names and legacy_name_tags is not None:
        legacy_kw: dict[str, Any] = {**kwargs}
        if tags is not None:
            legacy_kw["tags"] = tags

        stack = ExitStack()
        stack.enter_context(
            cast(
                "AbstractContextManager[Any]",
                _get_backend().timer(_emit_legacy(stat, legacy_name_tags), **legacy_kw),
            )
        )
        stack.enter_context(_get_backend().timer(stat, **modern_kw))
        return stack

    return _get_backend().timer(stat, **modern_kw)
