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


def get_stats_factory() -> Callable:
    if conf.getboolean("metrics", "statsd_datadog_enabled"):
        from airflow.observability.metrics import datadog_logger

        return datadog_logger.get_dogstatsd_logger
    if conf.getboolean("metrics", "statsd_on"):
        from airflow.observability.metrics import statsd_logger

        return statsd_logger.get_statsd_logger
    if conf.getboolean("metrics", "otel_on"):
        from airflow.observability.metrics import otel_logger

        return otel_logger.get_otel_logger
    return NoStatsLogger


def initialize_sdk_stats_backend() -> None:
    """
    Initialize the task-sdk ``Stats`` singleton with this process's configured backend.

    Plugins and listener hooks commonly get ``Stats`` via ``airflow.sdk.observability.stats``
    (or the deprecated ``airflow.stats`` shim, which re-exports the same module). That module
    is a separate singleton from the one this process initializes for its own internal use, so
    it must be initialized here too, or plugin code silently falls back to ``NoStatsLogger`` in
    any long-running component other than the task runner.
    """
    from airflow.sdk.observability import stats as sdk_stats  # noqa: SDK001

    sdk_stats.initialize(
        factory=get_stats_factory(),
        export_legacy_names=conf.getboolean("metrics", "legacy_names_on"),
    )
