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
from datetime import datetime

import pendulum


class ISO8601Formatter(logging.Formatter):
    """Format log timestamps with milliseconds and the local UTC offset."""

    default_time_format = "%Y-%m-%dT%H:%M:%S"
    default_msec_format = "%s.%03d"
    default_tz_format = "%z"

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        # Shared code must not import Airflow, so this resolves the local zone through pendulum
        # rather than ``airflow.sdk.timezone.from_timestamp`` — which itself returns a pendulum
        # ``DateTime`` in the same zone, so the rendered timestamp is unchanged.
        dt = datetime.fromtimestamp(record.created, tz=pendulum.local_timezone())
        formatted = dt.strftime(datefmt or self.default_time_format)
        if self.default_msec_format:
            formatted = self.default_msec_format % (formatted, record.msecs)
        if self.default_tz_format:
            formatted += dt.strftime(self.default_tz_format)
        return formatted
