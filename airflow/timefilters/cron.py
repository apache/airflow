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

from croniter import croniter
from pendulum.tz.timezone import Timezone

from airflow.timefilters.base import TimeFilter
from airflow.utils.timezone import make_naive


class CronTimeFilter(TimeFilter):
    """Time filter that match on a cron expression."""

    def __init__(self, expression: str, timezone: Timezone):
        self.expression = expression
        self.timezone = timezone

    def match(self, date):
        naive = make_naive(date, self.timezone)
        return croniter.match(self.expression, naive)
