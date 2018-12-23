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

import calendar
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils import timezone
from airflow.utils.decorators import apply_defaults


class DayOfWeekSensor(BaseSensorOperator):
    """
    Waits until the first specified day of the week. For example, if the execution
    day of the task is '2018-12-22' (Saturday) and you pass '5', the task will wait
    until next Friday (Week Day: 5)

    :param week_day_number: Day of the week as an integer, where Monday is 1 and
        Sunday is 7 (ISO Week Numbering)
    :type week_day_number: int
    :param use_task_execution_day: If ``True``, uses task's execution day to compare
        with week_day_number. Execution Date is Useful for backfilling.
        If ``False``, uses system's day of the week. Useful when you
        don't want to run anything on weekdays on the system.
    :type use_task_execution_day: bool
    """

    @apply_defaults
    def __init__(self, week_day_number,
                 use_task_execution_day=False,
                 *args, **kwargs):
        super(DayOfWeekSensor, self).__init__(*args, **kwargs)
        self.week_day_number = week_day_number
        self.use_task_execution_day = use_task_execution_day

    def poke(self, context):
        if self.week_day_number > 7:
            raise ValueError(
                'Invalid value ({}) for week_day_number! '
                'Valid value: 1 <= week_day_number <= 7'.format(self.week_day_number))
        self.log.info('Poking until weekday is %s, Today is %s',
                      calendar.day_name[self.week_day_number - 1],
                      calendar.day_name[timezone.utcnow().isoweekday() - 1])
        if self.use_task_execution_day:
            return context['execution_date'].isoweekday() == self.week_day_number
        else:
            return timezone.utcnow().isoweekday() == self.week_day_number


class WeekEndSensor(BaseSensorOperator):
    """
    Waits until this weekend. For example, if today is Monday, the task will wait
    until this weekend (Saturday or Sunday)

    :param use_task_execution_day: If ``True``, uses task's execution day to compare.
        Useful for backfilling.
        If ``False``, uses system's day of the week. Useful when you
        don't want to run anything on weekdays on the system.
    :type use_task_execution_day: bool
    """

    @apply_defaults
    def __init__(self, use_task_execution_day=False, *args, **kwargs):
        super(WeekEndSensor, self).__init__(*args, **kwargs)
        self.use_task_execution_day = use_task_execution_day

    def poke(self, context):
        self.log.info('Poking until weekend. Today is %s',
                      calendar.day_name[timezone.utcnow().isoweekday() - 1])
        if self.use_task_execution_day:
            return context['execution_date'].isoweekday() > 5
        else:
            return timezone.utcnow().isoweekday() > 5
