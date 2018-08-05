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
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime


class ScheduleBlackoutSensor(BaseSensorOperator):
    """
    Checks to see if a task is running for a specified date and time criteria
    Returns false if sensor is running within "blackout" criteria, true otherwise

    :param month_of_year: Integer representing month of year
        Not checked if left to default to None
    :type month_of_year: int
    :param day_of_month: Integer representing day of month
        Not checked if left to default to None
    :type day_of_month: int
    :param hour_of_day: Integer representing hour of day
        Not checked if left to default to None
    :type hour_of_day: int
    :param min_of_hour: Integer representing minute of hour
        Not checked if left to default to None
    :type min_of_hour: int
    :param day_of_week: Integer representing day of week
        Not checked if left to default to None
    :type day_of_week: int
    :param day_of_week: Datetime object to check criteria against
        Defaults to datetime.now() if set to none
    :type day_of_week: datetime
    """

    @apply_defaults
    def __init__(self,
                 month_of_year=None, day_of_month=None,
                 hour_of_day=None, min_of_hour=None,
                 day_of_week=None,
                 dt=None, *args, **kwargs):

        super(ScheduleBlackoutSensor, self).__init__(*args, **kwargs)

        self.dt = dt
        self.month_of_year = month_of_year
        self.day_of_month = day_of_month
        self.hour_of_day = hour_of_day
        self.min_of_hour = min_of_hour
        self.day_of_week = day_of_week

    def _check_criteria(self, crit, datepart):
        if crit is None:
            return None

        elif isinstance(crit, list):
            for i in crit:
                if i == datepart:
                    return True
            return False
        elif isinstance(crit, int):
            return True if datepart == crit else False
        else:
            raise TypeError(
                "Expected an interger or a list, received a {0}".format(type(crit)))

    def poke(self, context):
        self.dt = datetime.now() if self.dt is None else self.dt

        criteria = [
            # month of year
            self._check_criteria(self.month_of_year, self.dt.month),
            # day of month
            self._check_criteria(self.day_of_month, self.dt.day),
            # hour of day
            self._check_criteria(self.hour_of_day, self.dt.hour),
            # minute of hour
            self._check_criteria(self.min_of_hour, self.dt.minute),
            # day of week
            self._check_criteria(self.day_of_week, self.dt.weekday())
        ]

        # Removes criteria that are set to None and then checks that all
        # specified criteria are True. If all criteria are True - returns False
        # in order to trigger a sensor failure if blackout criteria are met
        return not all([crit for crit in criteria if crit is not None])
