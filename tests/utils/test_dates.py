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

import unittest
from datetime import datetime, timedelta

import pendulum
from dateutil.relativedelta import relativedelta
from pytest import approx

from airflow.utils import dates, timezone


class TestDates(unittest.TestCase):
    def test_days_ago(self):
        today = pendulum.today()
        today_midnight = pendulum.instance(datetime.fromordinal(today.date().toordinal()))

        self.assertEqual(dates.days_ago(0), today_midnight)
        self.assertEqual(dates.days_ago(100), today_midnight + timedelta(days=-100))

        self.assertEqual(dates.days_ago(0, hour=3), today_midnight + timedelta(hours=3))
        self.assertEqual(dates.days_ago(0, minute=3), today_midnight + timedelta(minutes=3))
        self.assertEqual(dates.days_ago(0, second=3), today_midnight + timedelta(seconds=3))
        self.assertEqual(dates.days_ago(0, microsecond=3), today_midnight + timedelta(microseconds=3))

    def test_parse_execution_date(self):
        execution_date_str_wo_ms = '2017-11-02 00:00:00'
        execution_date_str_w_ms = '2017-11-05 16:18:30.989729'
        bad_execution_date_str = '2017-11-06TXX:00:00Z'

        self.assertEqual(
            timezone.datetime(2017, 11, 2, 0, 0, 0), dates.parse_execution_date(execution_date_str_wo_ms)
        )
        self.assertEqual(
            timezone.datetime(2017, 11, 5, 16, 18, 30, 989729),
            dates.parse_execution_date(execution_date_str_w_ms),
        )
        self.assertRaises(ValueError, dates.parse_execution_date, bad_execution_date_str)

    def test_round_time(self):

        rt1 = dates.round_time(timezone.datetime(2015, 1, 1, 6), timedelta(days=1))
        assert timezone.datetime(2015, 1, 1, 0, 0) == rt1

        rt2 = dates.round_time(timezone.datetime(2015, 1, 2), relativedelta(months=1))
        assert timezone.datetime(2015, 1, 1, 0, 0) == rt2

        rt3 = dates.round_time(
            timezone.datetime(2015, 9, 16, 0, 0), timedelta(1), timezone.datetime(2015, 9, 14, 0, 0)
        )
        assert timezone.datetime(2015, 9, 16, 0, 0) == rt3

        rt4 = dates.round_time(
            timezone.datetime(2015, 9, 15, 0, 0), timedelta(1), timezone.datetime(2015, 9, 14, 0, 0)
        )
        assert timezone.datetime(2015, 9, 15, 0, 0) == rt4

        rt5 = dates.round_time(
            timezone.datetime(2015, 9, 14, 0, 0), timedelta(1), timezone.datetime(2015, 9, 14, 0, 0)
        )
        assert timezone.datetime(2015, 9, 14, 0, 0) == rt5

        rt6 = dates.round_time(
            timezone.datetime(2015, 9, 13, 0, 0), timedelta(1), timezone.datetime(2015, 9, 14, 0, 0)
        )
        assert timezone.datetime(2015, 9, 14, 0, 0) == rt6

    def test_infer_time_unit(self):
        assert dates.infer_time_unit([130, 5400, 10]) == 'minutes'

        assert dates.infer_time_unit([110, 50, 10, 100]) == 'seconds'

        assert dates.infer_time_unit([100000, 50000, 10000, 20000]) == 'hours'

        assert dates.infer_time_unit([200000, 100000]) == 'days'

    def test_scale_time_units(self):
        # floating point arrays
        arr1 = dates.scale_time_units([130, 5400, 10], 'minutes')
        assert arr1 == approx([2.1667, 90.0, 0.1667], rel=1e-3)

        arr2 = dates.scale_time_units([110, 50, 10, 100], 'seconds')
        assert arr2 == approx([110.0, 50.0, 10.0, 100.0])

        arr3 = dates.scale_time_units([100000, 50000, 10000, 20000], 'hours')
        assert arr3 == approx([27.7778, 13.8889, 2.7778, 5.5556], rel=1e-3)

        arr4 = dates.scale_time_units([200000, 100000], 'days')
        assert arr4 == approx([2.3147, 1.1574], rel=1e-3)


class TestUtilsDatesDateRange(unittest.TestCase):
    def test_no_delta(self):
        self.assertEqual(dates.date_range(datetime(2016, 1, 1), datetime(2016, 1, 3)), [])

    def test_end_date_before_start_date(self):
        with self.assertRaisesRegex(Exception, "Wait. start_date needs to be before end_date"):
            dates.date_range(datetime(2016, 2, 1), datetime(2016, 1, 1), delta=timedelta(seconds=1))

    def test_both_end_date_and_num_given(self):
        with self.assertRaisesRegex(Exception, "Wait. Either specify end_date OR num"):
            dates.date_range(datetime(2016, 1, 1), datetime(2016, 1, 3), num=2, delta=timedelta(seconds=1))

    def test_invalid_delta(self):
        exception_msg = "Wait. delta must be either datetime.timedelta or cron expression as str"
        with self.assertRaisesRegex(Exception, exception_msg):
            dates.date_range(datetime(2016, 1, 1), datetime(2016, 1, 3), delta=1)

    def test_positive_num_given(self):
        for num in range(1, 10):
            result = dates.date_range(datetime(2016, 1, 1), num=num, delta=timedelta(1))
            self.assertEqual(len(result), num)

            for i in range(num):
                self.assertTrue(timezone.is_localized(result[i]))

    def test_negative_num_given(self):
        for num in range(-1, -5, -10):
            result = dates.date_range(datetime(2016, 1, 1), num=num, delta=timedelta(1))
            self.assertEqual(len(result), -num)

            for i in range(num):
                self.assertTrue(timezone.is_localized(result[i]))

    def test_delta_cron_presets(self):
        preset_range = dates.date_range(datetime(2016, 1, 1), num=2, delta="@hourly")
        timedelta_range = dates.date_range(datetime(2016, 1, 1), num=2, delta=timedelta(hours=1))
        cron_range = dates.date_range(datetime(2016, 1, 1), num=2, delta="0 * * * *")

        self.assertEqual(preset_range, timedelta_range)
        self.assertEqual(preset_range, cron_range)
