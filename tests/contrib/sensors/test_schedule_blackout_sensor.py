# -*- coding: utf-8 -*-
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

from airflow.contrib.sensors.schedule_blackout_sensor import ScheduleBlackoutSensor
from datetime import datetime

TEST_DT = datetime(2000, 1, 1, 1, 1)
DEFAULT_DATE = datetime(2015, 1, 1)


class TestScheduleBlackoutSensor(unittest.TestCase):
    def test_criteria_met(self):
        task = ScheduleBlackoutSensor(
            task_id='task',
            month_of_year=1,
            hour_of_day=[i + 1 for i in range(4)],
            day_of_week=[5],
            dt=TEST_DT
        )

        output = task.poke(None)
        self.assertFalse(output)

    def test_criteria_not_met(self):
        task = ScheduleBlackoutSensor(
            task_id='task',
            month_of_year=2,
            hour_of_day=[i for i in range(12, 24)],
            day_of_week=[1],
            dt=TEST_DT
        )

        output = task.poke(None)
        self.assertTrue(output)

    def test_type_check(self):
        task = ScheduleBlackoutSensor(
            task_id='task',
            month_of_year="str",
            dt=TEST_DT
        )

        with self.assertRaises(TypeError):
            task.poke(None)


if __name__ == '__main__':
    unittest.main()
