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

from airflow.utils import timezone


class TimezoneAware(logging.Formatter):
    """
    Override time-formatting methods to include UTC offset.

    Since Airflow parses the logs to perform time conversion, UTC offset is
    critical information. This formatter ensures ``%(asctime)s`` is formatted
    containing the offset in ISO 8601, e.g. ``2022-06-12T13:00:00.123+0000``.
    """

    default_time_format = "%Y-%m-%dT%H:%M:%S"
    default_msec_format = "%s.%03d"
    default_tz_format = "%z"

    def formatTime(self, record, datefmt=None):
        """
        Format time in record.

        This returns the creation time of the specified LogRecord in ISO 8601
        date and time format in the local time zone.
        """
        dt = timezone.from_timestamp(record.created, tz="local")
        s = dt.strftime(datefmt or self.default_time_format)
        if self.default_msec_format:
            s = self.default_msec_format % (s, record.msecs)
        if self.default_tz_format:
            s += dt.strftime(self.default_tz_format)
        return s
