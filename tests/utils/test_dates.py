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
from parameterized import parameterized

from airflow.utils import dates, timezone


class TestDates(unittest.TestCase):

    def test_days_ago(self):
        today = pendulum.today()
        today_midnight = pendulum.instance(datetime.fromordinal(today.date().toordinal()))

        self.assertTrue(dates.days_ago(0) == today_midnight)

        self.assertTrue(dates.days_ago(100) == today_midnight + timedelta(days=-100))

        self.assertTrue(dates.days_ago(0, hour=3) == today_midnight + timedelta(hours=3))
        self.assertTrue(dates.days_ago(0, minute=3) == today_midnight + timedelta(minutes=3))
        self.assertTrue(dates.days_ago(0, second=3) == today_midnight + timedelta(seconds=3))
        self.assertTrue(dates.days_ago(0, microsecond=3) == today_midnight + timedelta(microseconds=3))

    def test_parse_execution_date(self):
        execution_date_str_wo_ms = '2017-11-02 00:00:00'
        execution_date_str_w_ms = '2017-11-05 16:18:30.989729'
        bad_execution_date_str = '2017-11-06TXX:00:00Z'

        self.assertEqual(
            timezone.datetime(2017, 11, 2, 0, 0, 0),
            dates.parse_execution_date(execution_date_str_wo_ms))
        self.assertEqual(
            timezone.datetime(2017, 11, 5, 16, 18, 30, 989729),
            dates.parse_execution_date(execution_date_str_w_ms))
        self.assertRaises(ValueError, dates.parse_execution_date, bad_execution_date_str)


class TestUtilsDatesDateRange(unittest.TestCase):

    def test_no_delta(self):
        self.assertEqual(dates.date_range(datetime(2016, 1, 1), datetime(2016, 1, 3)),
                         [])

    def test_end_date_before_start_date(self):
        with self.assertRaisesRegex(Exception, "Wait. start_date needs to be before end_date"):
            dates.date_range(datetime(2016, 2, 1),
                             datetime(2016, 1, 1),
                             delta=timedelta(seconds=1))

    def test_both_end_date_and_num_given(self):
        with self.assertRaisesRegex(Exception, "Wait. Either specify end_date OR num"):
            dates.date_range(datetime(2016, 1, 1),
                             datetime(2016, 1, 3),
                             num=2,
                             delta=timedelta(seconds=1))

    def test_invalid_delta(self):
        exception_msg = "Wait. delta must be either datetime.timedelta or cron expression as str"
        with self.assertRaisesRegex(Exception, exception_msg):
            dates.date_range(datetime(2016, 1, 1),
                             datetime(2016, 1, 3),
                             delta=1)

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

    def test_delta_cron_multiple_expressions(self):
        timedelta_range = dates.date_range(datetime(2016, 1, 1), num=4, delta=timedelta(minutes=30))
        cron_range = dates.date_range(datetime(2016, 1, 1), num=4, delta=["0 * * * *", "30 * * * *"])

        self.assertEqual(cron_range, timedelta_range)

    def test_delta_cron_multiple_expressions_negative_num_given(self):
        timedelta_range = dates.date_range(datetime(2016, 1, 1), num=-4, delta=timedelta(minutes=30))
        cron_range = dates.date_range(datetime(2016, 1, 1), num=-4, delta=["0 * * * *", "30 * * * *"])

        self.assertEqual(cron_range, timedelta_range)


class TestUtilsDatesCron(unittest.TestCase):

    @parameterized.expand([
        ("@daily", ["0 0 * * *"]),
        ("@weekly", ["0 0 * * 0"]),
        ("@monthly", ["0 0 1 * *"]),
        ("@quarterly", ["0 0 1 */3 *"]),
        ("@yearly", ["0 0 1 1 *"]),
        (["@yearly", "1 * * * *"], ["0 0 1 1 *", "1 * * * *"]),
    ])
    def test_normalized_expressions(self, cron_exprs, expected_n_cron_exprs):
        cron = dates.Cron(cron_exprs)

        self.assertEqual(cron.normalized_exprs, expected_n_cron_exprs)

    @parameterized.expand([
        (None, ),
        ("@invalid", ),
        ("0 0 1 * * * * *", ),
    ])
    def test_invalid_expressions(self, cron_exprs):
        with self.assertRaises(dates.CronError):
            dates.Cron(cron_exprs)

    def test_next(self):
        cron = dates.Cron(["0 * * * *", "10 * * * *"], datetime(2016, 1, 1))

        self.assertEqual(cron.get_next(), datetime(2016, 1, 1, 0, 10))
        self.assertEqual(cron.get_next(), datetime(2016, 1, 1, 1, 0))
        self.assertEqual(cron.get_next(), datetime(2016, 1, 1, 1, 10))

    def test_prev(self):
        cron = dates.Cron(["0 * * * *", "10 * * * *"], datetime(2016, 1, 1, 1))

        self.assertEqual(cron.get_current(), datetime(2016, 1, 1, 1, 0))
        self.assertEqual(cron.get_prev(), datetime(2016, 1, 1, 0, 10))
        self.assertEqual(cron.get_prev(), datetime(2016, 1, 1, 0, 0))

    def test_overlapping_expressions(self):
        # Should not return duplicate dates if cron expressions overlap
        cron = dates.Cron(["0 * * * *", "0-10/5 * * * *"], datetime(2016, 1, 1))

        self.assertEqual(cron.get_next(), datetime(2016, 1, 1, 0, 5))
        self.assertEqual(cron.get_next(), datetime(2016, 1, 1, 0, 10))
        self.assertEqual(cron.get_next(), datetime(2016, 1, 1, 1, 0))
        self.assertEqual(cron.get_next(), datetime(2016, 1, 1, 1, 5))

    def test_direction_change_next_prev(self):
        cron = dates.Cron(["0 * * * *", "10 * * * *"], datetime(2016, 1, 1))

        self.assertEqual(cron.get_next(), datetime(2016, 1, 1, 0, 10))
        self.assertEqual(cron.get_next(), datetime(2016, 1, 1, 1, 0))
        self.assertEqual(cron.get_prev(), datetime(2016, 1, 1, 0, 10))
        self.assertEqual(cron.get_prev(), datetime(2016, 1, 1, 0, 0))

    def test_direction_change_prev_next(self):
        cron = dates.Cron(["0 * * * *", "10 * * * *"], datetime(2016, 1, 1, 1))

        self.assertEqual(cron.get_prev(), datetime(2016, 1, 1, 0, 10))
        self.assertEqual(cron.get_prev(), datetime(2016, 1, 1, 0, 0))
        self.assertEqual(cron.get_next(), datetime(2016, 1, 1, 0, 10))
        self.assertEqual(cron.get_next(), datetime(2016, 1, 1, 1, 0))

    def test_single_expression(self):
        cron = dates.Cron("0 * * * *", datetime(2016, 1, 1))

        self.assertEqual(cron.get_current(), datetime(2016, 1, 1, 0))
        self.assertEqual(cron.get_next(), datetime(2016, 1, 1, 1))
        self.assertEqual(cron.get_next(), datetime(2016, 1, 1, 2))

    @parameterized.expand([
        ("@daily", ),
        ("@weekly", ),
        ("0 0 1 * *", ),
        (["0 0 1 * *", "0 0 2 * *"], ),
    ])
    def test_is_fixed_interval(self, cron_exprs):
        self.assertTrue(dates.Cron(cron_exprs).is_fixed_time_schedule())

    @parameterized.expand([
        ("*/5 * * * *", ),
        (["*/5 0 1 * *", "0 0 1 * *"], ),
        (["0 0 1 * *", "1 0 1 * *"], ),
        (["0 0 1 * *", "0 1 1 * *"], ),
    ])
    def test_is_not_fixed_interval(self, cron_exprs):
        self.assertFalse(dates.Cron(cron_exprs).is_fixed_time_schedule())
