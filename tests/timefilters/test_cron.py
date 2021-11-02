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

from pendulum import DateTime

from airflow.settings import TIMEZONE
from airflow.timefilters.cron import CronTimeFilter

DATE = DateTime(1970, 1, 1, tzinfo=TIMEZONE)


def test_match():
    assert CronTimeFilter("* * * * THU", TIMEZONE).match(DATE)


def test_not_match():
    assert not CronTimeFilter("* * * * FRI", TIMEZONE).match(DATE)
