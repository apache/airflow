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
from collections.abc import Callable
from functools import wraps
from typing import TYPE_CHECKING, TypeVar, cast

from .protocols import Timer
from .validators import (
    PatternAllowListValidator,
    PatternBlockListValidator,
    get_validator,
    validate_stat,
)

if TYPE_CHECKING:
    from configparser import ConfigParser

    from statsd import StatsClient, UnixSocketStatsClient

    from .protocols import DeltaType
    from .validators import ListValidator

# Fallbacks for the plain StatsD client, which (unlike datadog's DogStatsd) cannot resolve a
# None host/port. The DataDog backend leaves these unset so the client reads its own env vars.
DEFAULT_STATSD_HOST = "localhost"
DEFAULT_STATSD_PORT = 8125


def resolve_statsd_connection(conf: ConfigParser) -> tuple[str | None, int | None, str | None]:
    """
    Resolve the (host, port, socket_path) a StatsD/DataDog logger should use from config.

    Only explicitly-configured values are returned; anything unset comes back as ``None``.
    ``statsd_host`` / ``statsd_port`` are nullable config keys, so ``conf.has_option`` tells
    an explicit value apart from "unset". Returning ``None`` for everything (when nothing is
    configured) lets the DataDog client fall back to its own environment variables
    (``DD_AGENT_HOST`` / ``DD_DOGSTATSD_URL`` / ``DD_DOGSTATSD_PORT``).

    ``statsd_socket_path`` (a Unix Domain Socket) is not fatal to combine with ``statsd_host`` /
    ``statsd_port`` — the socket takes precedence — but it is almost certainly a mistake, so a
    warning is logged. No defaults are applied here; each backend applies its own if needed.
    """
    socket_path = conf.get("metrics", "statsd_socket_path", fallback=None)
    host = (
        conf.get("metrics", "statsd_host", fallback=None)
        if conf.has_option("metrics", "statsd_host")
        else None
    )
    port = (
        conf.getint("metrics", "statsd_port", fallback=None)
        if conf.has_option("metrics", "statsd_port")
        else None
    )

    if socket_path and (host is not None or port is not None):
        log.warning(
            "[metrics] statsd_socket_path is set together with statsd_host / statsd_port; the socket "
            "path takes precedence and the host/port are ignored."
        )

    return host, port, socket_path


T = TypeVar("T", bound=Callable)

log = logging.getLogger(__name__)


def prepare_stat_with_tags(fn: T) -> T:
    """Add tags to stat with influxdb standard format if influxdb_tags_enabled is True."""

    @wraps(fn)
    def wrapper(
        self, stat: str | None = None, *args, tags: dict[str, str] | None = None, **kwargs
    ) -> Callable[[str], str]:
        if self.influxdb_tags_enabled:
            if stat is not None and tags is not None:
                for k, v in tags.items():
                    if self.metric_tags_validator.test(k):
                        v_str = "true" if v == "" else v
                        if all(c not in [",", "="] for c in f"{v_str}{k}"):
                            stat += f",{k}={v_str}"
                        else:
                            log.error("Dropping invalid tag: %s=%s.", k, v)
        return fn(self, stat, *args, tags=tags, **kwargs)

    return cast("T", wrapper)


class SafeStatsdLogger:
    """StatsD Logger."""

    def __init__(
        self,
        statsd_client: StatsClient,
        metrics_validator: ListValidator | None = None,
        influxdb_tags_enabled: bool = False,
        metric_tags_validator: ListValidator | None = None,
        stat_name_handler: Callable[[str], str] | None = None,
        statsd_influxdb_enabled: bool = False,
    ) -> None:
        self.statsd = statsd_client
        self.metrics_validator = metrics_validator or PatternAllowListValidator()
        self.influxdb_tags_enabled = influxdb_tags_enabled
        self.metric_tags_validator = metric_tags_validator or PatternAllowListValidator()
        self.stat_name_handler = stat_name_handler
        self.statsd_influxdb_enabled = statsd_influxdb_enabled

    @prepare_stat_with_tags
    @validate_stat
    def incr(
        self,
        stat: str,
        count: int = 1,
        rate: float = 1,
        *,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Increment stat."""
        if self.metrics_validator.test(stat):
            return self.statsd.incr(stat, count, rate)
        return None

    @prepare_stat_with_tags
    @validate_stat
    def decr(
        self,
        stat: str,
        count: int = 1,
        rate: float = 1,
        *,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Decrement stat."""
        if self.metrics_validator.test(stat):
            return self.statsd.decr(stat, count, rate)
        return None

    @prepare_stat_with_tags
    @validate_stat
    def gauge(
        self,
        stat: str,
        value: int | float,
        rate: float = 1,
        delta: bool = False,
        *,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Gauge stat."""
        if self.metrics_validator.test(stat):
            return self.statsd.gauge(stat, value, rate, delta)
        return None

    @prepare_stat_with_tags
    @validate_stat
    def timing(
        self,
        stat: str,
        dt: DeltaType,
        *,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Stats timing."""
        if self.metrics_validator.test(stat):
            return self.statsd.timing(stat, dt)
        return None

    @prepare_stat_with_tags
    @validate_stat
    def timer(
        self,
        stat: str | None = None,
        *args,
        tags: dict[str, str] | None = None,
        **kwargs,
    ) -> Timer:
        """Timer metric that can be cancelled."""
        if stat and self.metrics_validator.test(stat):
            return Timer(self.statsd.timer(stat, *args, **kwargs))
        return Timer()


def get_statsd_logger(
    *,
    stats_class: type[StatsClient] | type[UnixSocketStatsClient],
    host: str | None = None,
    port: int | None = None,
    socket_path: str | None = None,
    prefix: str | None = None,
    ipv6: bool = False,
    influxdb_tags_enabled: bool = False,
    statsd_disabled_tags: str | None = None,
    metrics_allow_list: str | None = None,
    metrics_block_list: str | None = None,
    stat_name_handler: Callable[[str], str] | None = None,
    statsd_influxdb_enabled: bool = False,
) -> SafeStatsdLogger:
    """Return logger for StatsD."""
    if socket_path is not None:
        # The socket path takes precedence; any host/port were already warned about upstream.
        statsd = stats_class(socket_path=socket_path, prefix=prefix)
    else:
        # StatsClient cannot resolve a None host/port, so apply the usual defaults here.
        statsd = stats_class(
            host=host if host is not None else DEFAULT_STATSD_HOST,
            port=port if port is not None else DEFAULT_STATSD_PORT,
            prefix=prefix,
            ipv6=ipv6,
        )

    metric_tags_validator = PatternBlockListValidator(statsd_disabled_tags)
    validator = get_validator(metrics_allow_list, metrics_block_list)
    return SafeStatsdLogger(
        statsd,
        validator,
        influxdb_tags_enabled,
        metric_tags_validator,
        stat_name_handler,
        statsd_influxdb_enabled,
    )
