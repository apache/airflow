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
from airflow.utils import timezone
from airflow.utils.decorators import apply_defaults
from airflow.contrib.utils.weekday import WeekDay


class DayOfWeekSensor(BaseSensorOperator):
    """
    Waits until the first specified day of the week. For example, if the execution
    day of the task is '2018-12-22' (Saturday) and you pass 'FRIDAY', the task will wait
    until next Friday.

    :param week_day: Day of the week (full name). Example: "MONDAY"
    :type week_day_number: str
    :param use_task_execution_day: If ``True``, uses task's execution day to compare
        with week_day_number. Execution Date is Useful for backfilling.
        If ``False``, uses system's day of the week. Useful when you
        don't want to run anything on weekdays on the system.
    :type use_task_execution_day: bool
    """

    @apply_defaults
    def __init__(self, week_day,
                 use_task_execution_day=False,
                 *args, **kwargs):
        super(DayOfWeekSensor, self).__init__(*args, **kwargs)
        self.week_day = week_day
        self.use_task_execution_day = use_task_execution_day
        self._week_day_num = WeekDay.get_weekday_number(week_day_str=self.week_day)

    def poke(self, context):
        self.log.info('Poking until weekday is %s, Today is %s',
                      WeekDay(self._week_day_num).name,
                      WeekDay(timezone.utcnow().isoweekday()).name)
        if self.use_task_execution_day:
            return context['execution_date'].isoweekday() == self._week_day_num
        else:
            return timezone.utcnow().isoweekday() == self._week_day_num


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
                      WeekDay(timezone.utcnow().isoweekday()).name)
        if self.use_task_execution_day:
            return context['execution_date'].isoweekday() > 5
        else:
            return timezone.utcnow().isoweekday() > 5
