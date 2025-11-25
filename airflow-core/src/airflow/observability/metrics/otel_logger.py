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

from airflow._shared.observability.metrics import otel_logger
from airflow.configuration import conf

if TYPE_CHECKING:
    from airflow._shared.observability.metrics.otel_logger import SafeOtelLogger


def get_otel_logger(cls) -> SafeOtelLogger:
    host = conf.get("metrics", "otel_host")  # ex: "breeze-otel-collector"
    port = conf.getint("metrics", "otel_port")  # ex: 4318
    prefix = conf.get("metrics", "otel_prefix")  # ex: "airflow"
    ssl_active = conf.getboolean("metrics", "otel_ssl_active")
    # PeriodicExportingMetricReader will default to an interval of 60000 millis.
    conf_interval = conf.getfloat("metrics", "otel_interval_milliseconds", fallback=None)  # ex: 30000
    debug = conf.getboolean("metrics", "otel_debugging_on")
    service_name = conf.get("metrics", "otel_service")

    metrics_allow_list = conf.get("metrics", "metrics_allow_list", fallback=None)
    metrics_block_list = conf.get("metrics", "metrics_block_list", fallback=None)

    return otel_logger.get_otel_logger(
        cls,
        host,
        port,
        prefix,
        ssl_active,
        conf_interval,
        debug,
        service_name,
        metrics_allow_list,
        metrics_block_list,
    )
