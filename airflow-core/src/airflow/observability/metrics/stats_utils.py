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

from collections.abc import Callable

from airflow._shared.observability.metrics.base_stats_logger import NoStatsLogger
from airflow.configuration import conf


def get_stats_factory(stats_cls) -> Callable:
    if conf.getboolean("metrics", "statsd_datadog_enabled"):
        from airflow.observability.metrics import datadog_logger

        # Datadog needs the 'stats_cls' param, so wrap it into a 0-arg factory.
        return lambda: datadog_logger.get_dogstatsd_logger(stats_cls)
    if conf.getboolean("metrics", "statsd_on"):
        from airflow.observability.metrics import statsd_logger

        return statsd_logger.get_statsd_logger
    if conf.getboolean("metrics", "otel_on"):
        from airflow.observability.metrics import otel_logger

        return otel_logger.get_otel_logger
    return NoStatsLogger
