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
from typing import TYPE_CHECKING

from airflow._shared.observability.metrics import validators
from airflow.configuration import conf

if TYPE_CHECKING:
    from airflow._shared.observability.metrics.validators import ListValidator


def get_validator() -> ListValidator:
    metric_allow_list = conf.get("metrics", "metrics_allow_list", fallback=None)
    metric_block_list = conf.get("metrics", "metrics_block_list", fallback=None)

    return validators.get_validator(metric_allow_list, metric_block_list)


def get_current_handler_stat_name_func() -> Callable[[str], str]:
    stat_name_handler = conf.getimport("metrics", "stat_name_handler")
    statsd_influxdb_enabled = conf.getboolean("metrics", "statsd_influxdb_enabled", fallback=False)

    return validators.get_current_handler_stat_name_func(stat_name_handler, statsd_influxdb_enabled)
