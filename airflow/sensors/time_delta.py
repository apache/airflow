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

from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.temporal import DateTimeTrigger
from airflow.utils import timezone
from airflow.utils.context import Context


class TimeDeltaSensor(BaseSensorOperator):
    """
    Waits for a timedelta after the run's data interval.

    :param delta: time length to wait after the data interval before succeeding.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/operator:TimeDeltaSensor`


    """

    def __init__(self, *, delta, **kwargs):
        super().__init__(**kwargs)
        self.delta = delta

    def poke(self, context: Context):
        target_dttm = context['data_interval_end']
        target_dttm += self.delta
        self.log.info('Checking if the time (%s) has come', target_dttm)
        return timezone.utcnow() > target_dttm


class TimeDeltaSensorAsync(TimeDeltaSensor):
    """
    A drop-in replacement for TimeDeltaSensor that defers itself to avoid
    taking up a worker slot while it is waiting.

    :param delta: time length to wait after the data interval before succeeding.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/operator:TimeDeltaSensorAsync`

    """

    def execute(self, context: Context):
        target_dttm = context['data_interval_end']
        target_dttm += self.delta
        self.defer(trigger=DateTimeTrigger(moment=target_dttm), method_name="execute_complete")

    def execute_complete(self, context, event=None):
        """Callback for when the trigger fires - returns immediately."""
        return None
