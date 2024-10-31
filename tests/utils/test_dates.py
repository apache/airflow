#
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

import pytest

from airflow.utils import dates, timezone


class TestDates:
    def test_parse_execution_date(self):
        execution_date_str_wo_ms = "2017-11-02 00:00:00"
        execution_date_str_w_ms = "2017-11-05 16:18:30.989729"
        bad_execution_date_str = "2017-11-06TXX:00:00Z"

        assert timezone.datetime(2017, 11, 2, 0, 0, 0) == dates.parse_execution_date(execution_date_str_wo_ms)
        assert timezone.datetime(2017, 11, 5, 16, 18, 30, 989729) == dates.parse_execution_date(
            execution_date_str_w_ms
        )
        with pytest.raises(ValueError):
            dates.parse_execution_date(bad_execution_date_str)
