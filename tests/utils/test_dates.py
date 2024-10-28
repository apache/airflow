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

from datetime import timedelta

import pytest
from dateutil.relativedelta import relativedelta

from airflow.utils import dates, timezone


class TestDates:
    def test_parse_execution_date(self):
        execution_date_str_wo_ms = "2017-11-02 00:00:00"
        execution_date_str_w_ms = "2017-11-05 16:18:30.989729"
        bad_execution_date_str = "2017-11-06TXX:00:00Z"

        assert timezone.datetime(2017, 11, 2, 0, 0, 0) == dates.parse_execution_date(
            execution_date_str_wo_ms
        )
        assert timezone.datetime(
            2017, 11, 5, 16, 18, 30, 989729
        ) == dates.parse_execution_date(execution_date_str_w_ms)
        with pytest.raises(ValueError):
            dates.parse_execution_date(bad_execution_date_str)

    def test_round_time(self):
        rt1 = dates.round_time(timezone.datetime(2015, 1, 1, 6), timedelta(days=1))
        assert timezone.datetime(2015, 1, 1, 0, 0) == rt1

        rt2 = dates.round_time(timezone.datetime(2015, 1, 2), relativedelta(months=1))
        assert timezone.datetime(2015, 1, 1, 0, 0) == rt2

        rt3 = dates.round_time(
            timezone.datetime(2015, 9, 16, 0, 0),
            timedelta(1),
            timezone.datetime(2015, 9, 14, 0, 0),
        )
        assert timezone.datetime(2015, 9, 16, 0, 0) == rt3

        rt4 = dates.round_time(
            timezone.datetime(2015, 9, 15, 0, 0),
            timedelta(1),
            timezone.datetime(2015, 9, 14, 0, 0),
        )
        assert timezone.datetime(2015, 9, 15, 0, 0) == rt4

        rt5 = dates.round_time(
            timezone.datetime(2015, 9, 14, 0, 0),
            timedelta(1),
            timezone.datetime(2015, 9, 14, 0, 0),
        )
        assert timezone.datetime(2015, 9, 14, 0, 0) == rt5

        rt6 = dates.round_time(
            timezone.datetime(2015, 9, 13, 0, 0),
            timedelta(1),
            timezone.datetime(2015, 9, 14, 0, 0),
        )
        assert timezone.datetime(2015, 9, 14, 0, 0) == rt6

    def test_infer_time_unit(self):
        assert dates.infer_time_unit([130, 5400, 10]) == "minutes"

        assert dates.infer_time_unit([110, 50, 10, 100]) == "seconds"

        assert dates.infer_time_unit([100000, 50000, 10000, 20000]) == "hours"

        assert dates.infer_time_unit([200000, 100000]) == "days"

    def test_scale_time_units(self):
        # floating point arrays
        arr1 = dates.scale_time_units([130, 5400, 10], "minutes")
        assert arr1 == pytest.approx([2.1667, 90.0, 0.1667], rel=1e-3)

        arr2 = dates.scale_time_units([110, 50, 10, 100], "seconds")
        assert arr2 == pytest.approx([110.0, 50.0, 10.0, 100.0])

        arr3 = dates.scale_time_units([100000, 50000, 10000, 20000], "hours")
        assert arr3 == pytest.approx([27.7778, 13.8889, 2.7778, 5.5556], rel=1e-3)

        arr4 = dates.scale_time_units([200000, 100000], "days")
        assert arr4 == pytest.approx([2.3147, 1.1574], rel=1e-3)
