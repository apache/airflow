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

import os
from typing import TYPE_CHECKING

from opentelemetry.sdk.resources import SERVICE_INSTANCE_ID

from airflow.sdk._shared.observability.metrics import otel_logger
from airflow.sdk._shared.observability.otel_env_config import load_metrics_env_config
from airflow.sdk.configuration import conf

if TYPE_CHECKING:
    from airflow.sdk._shared.observability.metrics.otel_logger import SafeOtelLogger


def get_otel_logger(short_lived: bool = False) -> SafeOtelLogger:
    service_instance_id = None
    temporality_preference_env = "OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE"

    if short_lived:
        # The temporality env variable defaults to `CUMULATIVE` if unset.
        # If the process is `short_lived`, then set the default fallback to `DELTA`.
        os.environ.setdefault(temporality_preference_env, "DELTA")

        # Check the value of the env variable because the user could have defined it as `CUMULATIVE`.
        # LOWMEMORY also exports counters and histograms as deltas.
        if (
            os.environ[temporality_preference_env].strip().upper() in ("DELTA", "LOWMEMORY")
            and SERVICE_INSTANCE_ID not in load_metrics_env_config().resource_attributes
        ):
            # If the values are delta, then the id should be shared.
            # For cumulative values this would result to an overwrite.
            service_instance_id = "task-sdk"

    # The config values have been deprecated and therefore,
    # if the user hasn't added them to the config, the default values won't be used.
    # A fallback is needed to avoid an exception.
    port = None
    if conf.has_option("metrics", "otel_port"):
        port = conf.getint("metrics", "otel_port")

    conf_interval = None
    if conf.has_option("metrics", "otel_interval_milliseconds"):
        conf_interval = conf.getfloat("metrics", "otel_interval_milliseconds")

    return otel_logger.get_otel_logger(
        host=conf.get("metrics", "otel_host", fallback=None),  # ex: "breeze-otel-collector"
        port=port,  # ex: 4318
        prefix=conf.get("metrics", "otel_prefix", fallback=None),  # ex: "airflow"
        ssl_active=conf.getboolean("metrics", "otel_ssl_active", fallback=False),
        # PeriodicExportingMetricReader will default to an interval of 60000 millis.
        conf_interval=conf_interval,  # ex: 30000
        debug=conf.getboolean("metrics", "otel_debugging_on", fallback=False),
        service_name=conf.get("metrics", "otel_service", fallback=None),
        metrics_allow_list=conf.get("metrics", "metrics_allow_list", fallback=None),
        metrics_block_list=conf.get("metrics", "metrics_block_list", fallback=None),
        stat_name_handler=conf.getimport("metrics", "stat_name_handler", fallback=None),
        statsd_influxdb_enabled=conf.getboolean("metrics", "statsd_influxdb_enabled", fallback=False),
        service_instance_id=service_instance_id,
    )
