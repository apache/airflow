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
from functools import wraps
from typing import TYPE_CHECKING, Callable, TypeVar, cast

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.metrics.protocols import Timer
from airflow.metrics.validators import (
    PatternAllowListValidator,
    PatternBlockListValidator,
    get_validator,
    validate_stat,
)

if TYPE_CHECKING:
    from statsd import StatsClient

    from airflow.metrics.protocols import DeltaType, TimerProtocol
    from airflow.metrics.validators import (
        ListValidator,
    )

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
                        if all(c not in [",", "="] for c in f"{v}{k}"):
                            stat += f",{k}={v}"
                        else:
                            log.error("Dropping invalid tag: %s=%s.", k, v)
        return fn(self, stat, *args, tags=tags, **kwargs)

    return cast(T, wrapper)


class SafeStatsdLogger:
    """StatsD Logger."""

    def __init__(
        self,
        statsd_client: StatsClient,
        metrics_validator: ListValidator = PatternAllowListValidator(),
        influxdb_tags_enabled: bool = False,
        metric_tags_validator: ListValidator = PatternAllowListValidator(),
    ) -> None:
        self.statsd = statsd_client
        self.metrics_validator = metrics_validator
        self.influxdb_tags_enabled = influxdb_tags_enabled
        self.metric_tags_validator = metric_tags_validator

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
    ) -> TimerProtocol:
        """Timer metric that can be cancelled."""
        if stat and self.metrics_validator.test(stat):
            return Timer(self.statsd.timer(stat, *args, **kwargs))
        return Timer()


def get_statsd_logger(cls) -> SafeStatsdLogger:
    """Return logger for StatsD."""
    # no need to check for the scheduler/statsd_on -> this method is only called when it is set
    # and previously it would crash with None is callable if it was called without it.
    from statsd import StatsClient

    stats_class = conf.getimport("metrics", "statsd_custom_client_path", fallback=None)
    if stats_class:
        if not issubclass(stats_class, StatsClient):
            raise AirflowConfigException(
                "Your custom StatsD client must extend the statsd.StatsClient in order to ensure "
                "backwards compatibility."
            )
        else:
            log.info("Successfully loaded custom StatsD client")

    else:
        stats_class = StatsClient

    statsd = stats_class(
        host=conf.get("metrics", "statsd_host"),
        port=conf.getint("metrics", "statsd_port"),
        prefix=conf.get("metrics", "statsd_prefix"),
        ipv6=conf.getboolean("metrics", "statsd_ipv6", fallback=False),
    )

    influxdb_tags_enabled = conf.getboolean("metrics", "statsd_influxdb_enabled", fallback=False)
    metric_tags_validator = PatternBlockListValidator(
        conf.get("metrics", "statsd_disabled_tags", fallback=None)
    )
    return SafeStatsdLogger(statsd, get_validator(), influxdb_tags_enabled, metric_tags_validator)
