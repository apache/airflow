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
from typing import TYPE_CHECKING

from airflow._shared.configuration import AirflowConfigException
from airflow._shared.observability.metrics import statsd_logger
from airflow.configuration import conf

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from airflow._shared.observability.metrics.statsd_logger import SafeStatsdLogger


def get_statsd_logger() -> SafeStatsdLogger:
    # Local import to avoid requiring statsd when other backends are used (e.g. Datadog)
    from statsd import StatsClient, UnixSocketStatsClient

    socket_path = conf.get("metrics", "statsd_socket_path", fallback=None) or None
    custom_class = conf.getimport("metrics", "statsd_custom_client_path", fallback=None)

    if custom_class is not None:
        if socket_path is not None:
            if not issubclass(custom_class, UnixSocketStatsClient):
                raise AirflowConfigException(
                    "Your custom StatsD client must extend the statsd.UnixSocketStatsClient "
                    "when using a socket path in order to ensure backwards compatibility."
                )
        elif not issubclass(custom_class, StatsClient):
            raise AirflowConfigException(
                "Your custom StatsD client must extend the statsd.StatsClient in order "
                "to ensure backwards compatibility."
            )
        log.info("Successfully loaded custom StatsD client")

    common_kwargs = {
        "stats_class": custom_class,
        "prefix": conf.get("metrics", "statsd_prefix"),
        "influxdb_tags_enabled": conf.getboolean("metrics", "statsd_influxdb_enabled", fallback=False),
        "statsd_disabled_tags": conf.get("metrics", "statsd_disabled_tags", fallback=None),
        "metrics_allow_list": conf.get("metrics", "metrics_allow_list", fallback=None),
        "metrics_block_list": conf.get("metrics", "metrics_block_list", fallback=None),
        "stat_name_handler": conf.getimport("metrics", "stat_name_handler"),
        "statsd_influxdb_enabled": conf.getboolean("metrics", "statsd_influxdb_enabled", fallback=False),
    }
    if socket_path is not None:
        return statsd_logger.get_socket_statsd_logger(socket_path=socket_path, **common_kwargs)
    return statsd_logger.get_udp_statsd_logger(
        host=conf.get("metrics", "statsd_host"),
        port=conf.getint("metrics", "statsd_port"),
        ipv6=conf.getboolean("metrics", "statsd_ipv6", fallback=False),
        **common_kwargs,
    )
