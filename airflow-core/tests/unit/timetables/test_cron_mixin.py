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

from airflow.timetables._cron import CronMixin

SAMPLE_TZ = "UTC"


def test_valid_cron_expression():
    cm = CronMixin("* * 1 * *", SAMPLE_TZ)  # every day at midnight
    assert isinstance(cm.description, str)
    assert "Every minute" in cm.description or "month" in cm.description


def test_invalid_cron_expression():
    cm = CronMixin("invalid cron", SAMPLE_TZ)
    assert cm.description == ""


def test_dom_and_dow_conflict():
    cm = CronMixin("* * 1 * 1", SAMPLE_TZ)  # 1st of month or Monday
    desc = cm.description

    assert "(or)" in desc
    assert "Every minute, on day 1 of the month" in desc
    assert "Every minute, only on Monday" in desc
