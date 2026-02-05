#
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
import socket
from collections.abc import Callable
from typing import TYPE_CHECKING

from .base_stats_logger import NoStatsLogger

if TYPE_CHECKING:
    from .base_stats_logger import StatsLogger

log = logging.getLogger(__name__)


class _Stats(type):
    factory: Callable[[], StatsLogger | NoStatsLogger] | None = None
    instance: StatsLogger | NoStatsLogger | None = None

    def __getattr__(cls, name: str) -> str:
        factory = type.__getattribute__(cls, "factory")
        instance = type.__getattribute__(cls, "instance")

        if instance is None:
            if factory is None:
                factory = NoStatsLogger
                type.__setattr__(cls, "factory", factory)

            try:
                instance = factory()
            except (socket.gaierror, ImportError) as e:
                log.error("Could not configure StatsClient: %s, using NoStatsLogger instead.", e)
                instance = NoStatsLogger()

            type.__setattr__(cls, "instance", instance)

        return getattr(instance, name)

    def initialize(cls, *, is_statsd_datadog_enabled: bool, is_statsd_on: bool, is_otel_on: bool) -> None:
        type.__setattr__(cls, "factory", None)
        type.__setattr__(cls, "instance", None)
        factory: Callable

        if is_statsd_datadog_enabled:
            from airflow.observability.metrics import datadog_logger

            # Datadog needs the cls param, so wrap it into a 0-arg factory.
            factory = lambda: datadog_logger.get_dogstatsd_logger(cls)
        elif is_statsd_on:
            from airflow.observability.metrics import statsd_logger

            factory = statsd_logger.get_statsd_logger
        elif is_otel_on:
            from airflow.observability.metrics import otel_logger

            factory = otel_logger.get_otel_logger
        else:
            factory = NoStatsLogger

        type.__setattr__(cls, "factory", factory)

    @classmethod
    def get_constant_tags(cls, *, tags_in_string: str | None) -> list[str]:
        """Get constant DataDog tags to add to all stats."""
        if not tags_in_string:
            return []
        return tags_in_string.split(",")


if TYPE_CHECKING:
    Stats: StatsLogger
else:

    class Stats(metaclass=_Stats):
        """Empty class for Stats - we use metaclass to inject the right one."""
