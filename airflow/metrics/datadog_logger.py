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

import datetime
import logging
from functools import wraps
from typing import TYPE_CHECKING, Callable, TypeVar, cast

from airflow.configuration import conf
from airflow.metrics.protocols import Timer
from airflow.metrics.validators import (
    PatternAllowListValidator,
    PatternBlockListValidator,
    get_validator,
    validate_stat,
)

if TYPE_CHECKING:
    from datadog import DogStatsd

    from airflow.metrics.protocols import DeltaType, TimerProtocol
    from airflow.metrics.validators import (
        ListValidator,
    )

log = logging.getLogger(__name__)
T = TypeVar("T", bound=Callable)


def prepare_stat_with_tags(fn: T) -> T:
    """Prepare tags and stat."""

    @wraps(fn)
    def wrapper(self, stat: str | None = None, *args, tags: dict[str, str] | None = None, **kwargs):
        stat = stat or ""

        if tags and self.metrics_tags:
            valid_tags: dict[str, str] = {}
            for k, v in tags.items():
                if self.metric_tags_validator.test(k):
                    if all(c not in [",", "="] for c in f"{v}{k}"):
                        valid_tags[k] = v
                    else:
                        log.error("Dropping invalid tag: %s=%s.", k, v)
            tags_list = [f"{key}:{value}" for key, value in valid_tags.items()]
        else:
            tags_list = []

        kwargs["tags"] = tags_list

        return fn(self, stat, *args, **kwargs)

    return cast(T, wrapper)


class SafeDogStatsdLogger:
    """DogStatsd Logger."""

    def __init__(
        self,
        dogstatsd_client: DogStatsd,
        metrics_validator: ListValidator = PatternAllowListValidator(),
        metrics_tags: bool = False,
        metric_tags_validator: ListValidator = PatternAllowListValidator(),
    ) -> None:
        self.dogstatsd = dogstatsd_client
        self.metrics_validator = metrics_validator
        self.metrics_tags = metrics_tags
        self.metric_tags_validator = metric_tags_validator

    @prepare_stat_with_tags
    @validate_stat
    def incr(
        self,
        stat: str,
        count: int = 1,
        rate: float = 1,
        *,
        tags: list[str] | None = None,
    ) -> None:
        """Increment stat."""
        if self.metrics_validator.test(stat):
            return self.dogstatsd.increment(metric=stat, value=count, tags=tags, sample_rate=rate)
        return None

    @prepare_stat_with_tags
    @validate_stat
    def decr(
        self,
        stat: str,
        count: int = 1,
        rate: float = 1,
        *,
        tags: list[str] | None = None,
    ) -> None:
        """Decrement stat."""
        if self.metrics_validator.test(stat):
            return self.dogstatsd.decrement(metric=stat, value=count, tags=tags, sample_rate=rate)
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
        tags: list[str] | None = None,
    ) -> None:
        """Gauge stat."""
        if self.metrics_validator.test(stat):
            return self.dogstatsd.gauge(metric=stat, value=value, tags=tags, sample_rate=rate)
        return None

    @prepare_stat_with_tags
    @validate_stat
    def timing(
        self,
        stat: str,
        dt: DeltaType,
        *,
        tags: list[str] | None = None,
    ) -> None:
        """Stats timing."""
        if self.metrics_validator.test(stat):
            if isinstance(dt, datetime.timedelta):
                 dt = dt.total_seconds() * 1000.0
            return self.dogstatsd.timing(metric=stat, value=dt, tags=tags)
        return None

    @prepare_stat_with_tags
    @validate_stat
    def timer(
        self,
        stat: str | None = None,
        tags: list[str] | None = None,
        **kwargs,
    ) -> TimerProtocol:
        """Timer metric that can be cancelled."""
        if stat and self.metrics_validator.test(stat):
            return Timer(self.dogstatsd.timed(stat, tags=tags, **kwargs))
        return Timer()


def get_dogstatsd_logger(cls) -> SafeDogStatsdLogger:
    """Get DataDog StatsD logger."""
    from datadog import DogStatsd

    dogstatsd = DogStatsd(
        host=conf.get("metrics", "statsd_host"),
        port=conf.getint("metrics", "statsd_port"),
        namespace=conf.get("metrics", "statsd_prefix"),
        constant_tags=cls.get_constant_tags(),
    )
    datadog_metrics_tags = conf.getboolean("metrics", "statsd_datadog_metrics_tags", fallback=True)
    metric_tags_validator = PatternBlockListValidator(
        conf.get("metrics", "statsd_disabled_tags", fallback=None)
    )
    return SafeDogStatsdLogger(dogstatsd, get_validator(), datadog_metrics_tags, metric_tags_validator)
