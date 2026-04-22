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
from .protocols import Timer

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
_registry: MetricsRegistry | None = None


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
    export_legacy_names: bool,
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


def _get_registry() -> MetricsRegistry:
    """Initialize the registry on first use to avoid import-time file I/O."""
    global _registry
    if _registry is None:
        _registry = MetricsRegistry()
    return _registry


def _get_legacy_stat_name_and_tags(
    stat: str, tags: dict[str, Any] | None
) -> tuple[str | None, dict[str, Any]]:
    """
    Look up and format the legacy name for a metric from the registry.

    Returns (formatted_name, extra_tags) where formatted_name is None when there
    is no legacy stat to emit, and extra_tags are the tags not consumed as name
    variables. Raises ValueError if required name variables are missing from tags.
    """
    _none: tuple[None, dict[str, Any]] = None, {}

    # If the config flag is enabled/disabled.
    if not _export_legacy_names:
        return _none

    stat_from_registry = _get_registry().get(name=stat)

    # If the provided stat exists in the registry.
    if not stat_from_registry:
        return _none

    legacy_name = stat_from_registry.get("legacy_name", "-")

    # If the registry stat has a legacy name.
    if legacy_name == "-":
        return _none

    required_vars = stat_from_registry.get("name_variables", [])
    provided_vars = set(tags.keys()) if tags else set()
    missing_vars = set(required_vars) - provided_vars
    # If there are specified variables in the YAML file that haven't been provided in the tags param.
    if missing_vars:
        raise ValueError(
            f"Missing required variables for metric '{stat}': {sorted(missing_vars)}. "
            f"Required variables found in the registry: {required_vars}. "
            f"Provided tags: {sorted(provided_vars)}. "
            f"Provide all required variables as tags."
        )

    # 'required_vars' are the ones found in the registry.
    # If there is a variable that exists in the tags and not in the registry,
    # then it's extra, and it will be set as a tag for the legacy stat.
    name_var_set = set(required_vars)
    formatted_name = legacy_name.format(**{k: tags[k] for k in required_vars})  # type: ignore[index]
    extra_tags = {k: v for k, v in (tags or {}).items() if k not in name_var_set}
    return formatted_name, extra_tags


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


def incr(
    stat: str,
    count: int | None = None,
    rate: int | float | None = None,
    *,
    tags: dict[str, Any] | None = None,
) -> None:
    """Increment a counter metric."""
    legacy_name, extra_tags = _get_legacy_stat_name_and_tags(stat, tags)
    if legacy_name is not None:
        _get_backend().incr(legacy_name, **_defined(count=count, rate=rate, tags=extra_tags))
    _get_backend().incr(stat, **_defined(count=count, rate=rate, tags=tags))


def decr(
    stat: str,
    count: int | None = None,
    rate: int | float | None = None,
    *,
    tags: dict[str, Any] | None = None,
) -> None:
    """Decrement a counter metric."""
    legacy_name, extra_tags = _get_legacy_stat_name_and_tags(stat, tags)
    if legacy_name is not None:
        _get_backend().decr(legacy_name, **_defined(count=count, rate=rate, tags=extra_tags))
    _get_backend().decr(stat, **_defined(count=count, rate=rate, tags=tags))


def gauge(
    stat: str,
    value: float,
    rate: int | float | None = None,
    delta: bool | None = None,
    *,
    tags: dict[str, Any] | None = None,
) -> None:
    """Set a gauge metric."""
    legacy_name, extra_tags = _get_legacy_stat_name_and_tags(stat, tags)
    if legacy_name is not None:
        _get_backend().gauge(legacy_name, value, **_defined(rate=rate, delta=delta, tags=extra_tags))
    _get_backend().gauge(stat, value, **_defined(rate=rate, delta=delta, tags=tags))


def timing(
    stat: str,
    dt: DeltaType,
    *,
    tags: dict[str, Any] | None = None,
) -> None:
    """Record a timing metric."""
    legacy_name, extra_tags = _get_legacy_stat_name_and_tags(stat, tags)
    if legacy_name is not None:
        _get_backend().timing(legacy_name, dt, **_defined(tags=extra_tags))
    _get_backend().timing(stat, dt, **_defined(tags=tags))


def timer(
    stat: str | None = None,
    tags: dict[str, Any] | None = None,
    **kwargs,
) -> Timer:
    """
    Context manager that times a block and emits a timer metric.

    When ``stat`` is None, returns the backend timer directly for stopwatch
    use (duration is available but no metric is emitted).

    When legacy name export is enabled and the metric has a legacy name in the
    registry, both the legacy metric name and the regular name are timed
    simultaneously via an ExitStack. Otherwise, the backend timer is returned
    directly, preserving the ability to access ``timer.duration`` on the returned object.
    """
    if stat is None:
        return _get_backend().timer()

    regular_kw: dict[str, Any] = {**kwargs}
    if tags:
        regular_kw["tags"] = tags

    legacy_name, extra_tags = _get_legacy_stat_name_and_tags(stat, tags)
    if legacy_name is not None:
        legacy_kw: dict[str, Any] = {**kwargs}
        if extra_tags:
            legacy_kw["tags"] = extra_tags

        stack = ExitStack()
        stack.enter_context(
            cast(
                "AbstractContextManager[Any]",
                _get_backend().timer(legacy_name, **legacy_kw),
            )
        )
        real_timer = stack.enter_context(_get_backend().timer(stat, **regular_kw))
        return _ExitStackTimer(stack, real_timer)

    return _get_backend().timer(stat, **regular_kw)


class _ExitStackTimer(Timer):
    """Timer wrapper that delegates to a real timer and closes an ExitStack on exit."""

    def __init__(self, stack: ExitStack, real_timer: Timer) -> None:
        super().__init__(real_timer)
        self._stack = stack

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.stop(send=False)
        self._stack.__exit__(exc_type, exc_value, traceback)


class Stats:
    """
    Class-based shim providing access to module-level stats functions.

    All attributes delegate to the corresponding module-level function so call-sites
    can use the old ``Stats.incr(…)`` syntax for backwards compatibility.
    """

    initialize = staticmethod(initialize)
    incr = staticmethod(incr)
    decr = staticmethod(decr)
    gauge = staticmethod(gauge)
    timing = staticmethod(timing)
    timer = staticmethod(timer)
