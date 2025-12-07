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

from airflow._shared.observability.metrics.base_stats_logger import NoStatsLogger
from airflow.configuration import conf

if TYPE_CHECKING:
    from airflow._shared.observability.metrics.base_stats_logger import StatsLogger

log = logging.getLogger(__name__)


class _Stats(type):
    factory: Callable
    instance: StatsLogger | NoStatsLogger | None = None

    def __getattr__(cls, name: str) -> str:
        if not cls.instance:
            try:
                cls.instance = cls.factory()
            except (socket.gaierror, ImportError) as e:
                log.error("Could not configure StatsClient: %s, using NoStatsLogger instead.", e)
                cls.instance = NoStatsLogger()
        return getattr(cls.instance, name)

    def __init__(cls, *args, **kwargs) -> None:
        super().__init__(cls)
        if not hasattr(cls.__class__, "factory"):
            is_datadog_enabled_defined = conf.has_option("metrics", "statsd_datadog_enabled")
            if is_datadog_enabled_defined and conf.getboolean("metrics", "statsd_datadog_enabled"):
                from airflow.observability.metrics import datadog_logger

                cls.__class__.factory = datadog_logger.get_dogstatsd_logger
            elif conf.getboolean("metrics", "statsd_on"):
                from airflow.observability.metrics import statsd_logger

                cls.__class__.factory = statsd_logger.get_statsd_logger
            elif conf.getboolean("metrics", "otel_on"):
                from airflow.observability.metrics import otel_logger

                cls.__class__.factory = otel_logger.get_otel_logger
            else:
                cls.__class__.factory = NoStatsLogger

    @classmethod
    def get_constant_tags(cls) -> list[str]:
        """Get constant DataDog tags to add to all stats."""
        tags_in_string = conf.get("metrics", "statsd_datadog_tags", fallback=None)
        if not tags_in_string:
            return []
        return tags_in_string.split(",")


if TYPE_CHECKING:
    Stats: StatsLogger
else:

    class Stats(metaclass=_Stats):
        """Empty class for Stats - we use metaclass to inject the right one."""
