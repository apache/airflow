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

from typing import TYPE_CHECKING

from airflow._shared.observability.metrics import datadog_logger
from airflow.configuration import conf

if TYPE_CHECKING:
    from airflow._shared.observability.metrics.datadog_logger import SafeDogStatsdLogger


def get_dogstatsd_logger(cls) -> SafeDogStatsdLogger:
    host = conf.get("metrics", "statsd_host")
    port = conf.getint("metrics", "statsd_port")
    namespace = conf.get("metrics", "statsd_prefix")

    datadog_metrics_tags = conf.getboolean("metrics", "statsd_datadog_metrics_tags", fallback=True)
    statsd_disabled_tags = conf.get("metrics", "statsd_disabled_tags", fallback=None)

    metrics_allow_list = conf.get("metrics", "metrics_allow_list", fallback=None)
    metrics_block_list = conf.get("metrics", "metrics_block_list", fallback=None)

    return datadog_logger.get_dogstatsd_logger(
        cls,
        host,
        port,
        namespace,
        datadog_metrics_tags,
        statsd_disabled_tags,
        metrics_allow_list,
        metrics_block_list,
    )
