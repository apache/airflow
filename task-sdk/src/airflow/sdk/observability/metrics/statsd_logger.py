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

from airflow.sdk._shared.configuration import AirflowConfigException
from airflow.sdk._shared.observability.metrics import statsd_logger
from airflow.sdk.configuration import conf

if TYPE_CHECKING:
    from airflow.sdk._shared.observability.metrics.statsd_logger import SafeStatsdLogger

log = logging.getLogger(__name__)


def get_statsd_logger(cls) -> SafeStatsdLogger:
    stats_class = conf.getimport("metrics", "statsd_custom_client_path", fallback=None)

    # no need to check for the scheduler/statsd_on -> this method is only called when it is set
    # and previously it would crash with None is callable if it was called without it.
    from statsd import StatsClient

    if stats_class:
        if not issubclass(stats_class, StatsClient):
            raise AirflowConfigException(
                "Your custom StatsD client must extend the statsd.StatsClient in order to ensure "
                "backwards compatibility."
            )
        log.info("Successfully loaded custom StatsD client")

    else:
        stats_class = StatsClient

    return statsd_logger.get_statsd_logger(
        cls,
        stats_class=stats_class,
        host=conf.get("metrics", "statsd_host"),
        port=conf.getint("metrics", "statsd_port"),
        prefix=conf.get("metrics", "statsd_prefix"),
        ipv6=conf.getboolean("metrics", "statsd_ipv6", fallback=False),
        influxdb_tags_enabled=conf.getboolean("metrics", "statsd_influxdb_enabled", fallback=False),
        statsd_disabled_tags=conf.get("metrics", "statsd_disabled_tags", fallback=None),
        metrics_allow_list=conf.get("metrics", "metrics_allow_list", fallback=None),
        metrics_block_list=conf.get("metrics", "metrics_block_list", fallback=None),
        stat_name_handler=conf.getimport("metrics", "stat_name_handler"),
        statsd_influxdb_enabled=conf.getboolean("metrics", "statsd_influxdb_enabled", fallback=False),
    )
