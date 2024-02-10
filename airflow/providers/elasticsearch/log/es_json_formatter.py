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

from datetime import datetime

import pendulum

from airflow.utils.log.json_formatter import JSONFormatter


class ElasticsearchJSONFormatter(JSONFormatter):
    """Convert a log record to JSON with ISO 8601 date and time format."""

    default_time_format = "%Y-%m-%dT%H:%M:%S"
    default_msec_format = "%s.%03d"
    default_tz_format = "%z"

    def formatTime(self, record, datefmt=None):
        """Return the creation time of the LogRecord in ISO 8601 date/time format in the local time zone."""
        # TODO: Use airflow.utils.timezone.from_timestamp(record.created, tz="local")
        #  as soon as min Airflow 2.9.0
        dt = datetime.fromtimestamp(record.created, tz=pendulum.local_timezone())
        s = dt.strftime(datefmt or self.default_time_format)
        if self.default_msec_format:
            s = self.default_msec_format % (s, record.msecs)
        if self.default_tz_format:
            s += dt.strftime(self.default_tz_format)
        return s
