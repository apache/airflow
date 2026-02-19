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
import os
import re
import socket
from collections.abc import Callable
from typing import TYPE_CHECKING

from .base_stats_logger import NoStatsLogger

if TYPE_CHECKING:
    from .base_stats_logger import StatsLogger

log = logging.getLogger(__name__)

_VALID_STAT_NAME_CHARS_RE = re.compile(r"^[a-zA-Z0-9_.-]+$")
_INVALID_STAT_NAME_CHARS_RE = re.compile(r"[^a-zA-Z0-9_.-]")


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


class _Stats(type):
    factory: Callable[[], StatsLogger | NoStatsLogger] | None = None
    instance: StatsLogger | NoStatsLogger | None = None

    def __getattr__(cls, name: str) -> str:
        factory = type.__getattribute__(cls, "factory")
        instance = type.__getattribute__(cls, "instance")

        # When using OpenTelemetry, some subprocesses are short-lived and
        # often exit before flushing any metrics.
        #
        # The solution is to register a hook that performs a force flush at exit.
        # The atexit hook is registered when initializing the instance.
        #
        # The instance gets initialized once per process. In case a process is forked, then
        # the new subprocess, will inherit the already initialized instance of the parent process.
        #
        # Store the instance pid so that it can be compared with the current pid
        # to decide whether to initialize the instance again or not.
        #
        # So far, all forks are resetting their state to remove anything inherited by the parent.
        # But in the future that might not always be true.
        current_pid = os.getpid()
        if cls.instance and cls._instance_pid != current_pid:
            log.info(
                "Stats instance was created in PID %s but accessed in PID %s. Re-initializing.",
                cls._instance_pid,
                current_pid,
            )
            # Setting the instance to None, will force re-initialization.
            cls.instance = None
            cls._instance_pid = None

        if instance is None:
            if factory is None:
                factory = NoStatsLogger
                type.__setattr__(cls, "factory", factory)

            try:
                instance = factory()
                cls._instance_pid = current_pid
            except (socket.gaierror, ImportError) as e:
                log.error("Could not configure StatsClient: %s, using NoStatsLogger instead.", e)
                instance = NoStatsLogger()
                cls._instance_pid = current_pid

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
