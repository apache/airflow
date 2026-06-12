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

from airflow.sdk._shared.observability.metrics import otel_logger
from airflow.sdk.configuration import conf

if TYPE_CHECKING:
    from airflow.sdk._shared.observability.metrics.otel_logger import SafeOtelLogger


def get_otel_logger() -> SafeOtelLogger:
    return otel_logger.configure_otel(
        host=conf.get("metrics", "otel_host", fallback=None),
        port=conf.get("metrics", "otel_port", fallback=None),
        ssl_active=conf.getboolean("metrics", "otel_ssl_active", fallback=False),
        service=conf.get("metrics", "otel_service", fallback=None),
        interval_ms=conf.get("metrics", "otel_interval_milliseconds", fallback=None),
        debug=conf.getboolean("metrics", "otel_debugging_on", fallback=False),
        prefix=conf.get("metrics", "otel_prefix", fallback="airflow"),
        allow_list=conf.get("metrics", "metrics_allow_list", fallback=None),
        block_list=conf.get("metrics", "metrics_block_list", fallback=None),
        stat_name_handler=conf.getimport("metrics", "stat_name_handler", fallback=None),
        statsd_influxdb_enabled=conf.getboolean("metrics", "statsd_influxdb_enabled", fallback=False),
    )
