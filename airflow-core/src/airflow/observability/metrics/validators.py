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
from collections.abc import Callable
from functools import wraps
from typing import cast

from airflow._shared.observability.exceptions import InvalidStatsNameException
from airflow._shared.observability.metrics.validators import get_current_handler_stat_name_func
from airflow.configuration import conf

log = logging.getLogger(__name__)


def validate_stat(fn: Callable) -> Callable:
    """Check if stat name contains invalid characters; logs and does not emit stats if name is invalid."""

    @wraps(fn)
    def wrapper(self, stat: str | None = None, *args, **kwargs) -> Callable | None:
        stat_name_handler = conf.getimport("metrics", "stat_name_handler")
        statsd_influxdb_enabled = conf.getboolean("metrics", "statsd_influxdb_enabled", fallback=False)

        try:
            if stat is not None:
                handler_stat_name_func = get_current_handler_stat_name_func(
                    stat_name_handler, statsd_influxdb_enabled
                )
                stat = handler_stat_name_func(stat)
            return fn(self, stat, *args, **kwargs)
        except InvalidStatsNameException:
            log.exception("Invalid stat name: %s.", stat)
            return None

    return cast("Callable", wrapper)
