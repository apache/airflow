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
from airflow.metrics.base_stats_logger import StatsLogger
from airflow.metrics.protocols import Timer
from airflow.metrics.validators import (
    PatternAllowListValidator,
    PatternBlockListValidator,
    get_validator,
    validate_stat,
)

if TYPE_CHECKING:
    from datadog import DogStatsd
    from opentelemetry.util.types import Attributes

    from airflow.metrics.protocols import DeltaType, TimerProtocol
    from airflow.metrics.validators import (
        ListValidator,
    )

log = logging.getLogger(__name__)
T = TypeVar("T", bound=Callable)


def prepare_metric_name_with_tags(fn: T) -> T:
    """Prepare tags and stat."""

    @wraps(fn)
    def wrapper(self, metric_name: str | None = None, *args, tags: dict[str, str] | None = None, **kwargs):
        if tags and self.metrics_tags:
            valid_tags: dict[str, str] = {}
            for k, v in tags.items():
                if self.metric_tags_validator.test(k):
                    if ":" not in f"{v}{k}":
                        valid_tags[k] = v
                    else:
                        log.error("Dropping invalid tag: %s:%s.", k, v)
            tags_list = [f"{key}:{value}" for key, value in valid_tags.items()]
        else:
            tags_list = []

        kwargs["tags"] = tags_list

        return fn(self, metric_name, *args, **kwargs)

    return cast(T, wrapper)


class SafeDogStatsdLogger(StatsLogger):
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

    @prepare_metric_name_with_tags
    @validate_stat
    def incr(
        self,
        metric_name: str,
        count: int = 1,
        rate: float = 1,
        *,
        tags: list[str] | None = None,
    ) -> None:
        """Increment stat."""
        full_metric_name = self.get_name(metric_name, None)
        if self.metrics_validator.test(full_metric_name):
            return self.dogstatsd.increment(metric=full_metric_name, value=count, tags=tags, sample_rate=rate)
        return None

    @prepare_metric_name_with_tags
    @validate_stat
    def decr(
        self,
        metric_name: str,
        count: int = 1,
        rate: float = 1,
        *,
        tags: list[str] | None = None,
    ) -> None:
        """Decrement stat."""
        full_metric_name = self.get_name(metric_name, None)
        if self.metrics_validator.test(full_metric_name):
            return self.dogstatsd.decrement(metric=full_metric_name, value=count, tags=tags, sample_rate=rate)
        return None

    @prepare_metric_name_with_tags
    @validate_stat
    def gauge(
        self,
        metric_name: str,
        value: int | float,
        rate: float = 1,
        delta: bool = False,
        *,
        tags: list[str] | None = None,
    ) -> None:
        """Gauge stat."""
        full_metric_name = self.get_name(metric_name, None)
        if self.metrics_validator.test(full_metric_name):
            return self.dogstatsd.gauge(metric=full_metric_name, value=value, tags=tags, sample_rate=rate)
        return None

    @prepare_metric_name_with_tags
    @validate_stat
    def timing(
        self,
        metric_name: str,
        dt: DeltaType,
        *,
        tags: list[str] | None = None,
    ) -> None:
        """Stats timing."""
        full_metric_name = self.get_name(metric_name, None)

        if self.metrics_validator.test(full_metric_name):
            if isinstance(dt, datetime.timedelta):
                dt = dt.total_seconds() * 1000.0
            return self.dogstatsd.timing(metric=full_metric_name, value=dt, tags=tags)

        return None

    @prepare_metric_name_with_tags
    @validate_stat
    def timer(
        self,
        metric_name: str,
        tags: list[str] | None = None,
        **kwargs,
    ) -> TimerProtocol:
        """Timer metric that can be cancelled."""
        full_metric_name = self.get_name(metric_name, None)

        if full_metric_name and self.metrics_validator.test(full_metric_name):
            return Timer(self.dogstatsd.timed(full_metric_name, tags=tags, **kwargs))
        return Timer()

    def get_name(self, metric_name: str, tags: Attributes | None = None) -> str:
        """Get metric name with tags, ensuring no invalid keys are included."""
        return metric_name


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
