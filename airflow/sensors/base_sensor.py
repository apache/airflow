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

"""
Defines a Base Sensor.
"""
from abc import ABC
from time import sleep
from typing import Dict, Iterable, List, Optional, Union

from airflow.exceptions import (
    AirflowException, AirflowSensorTimeout, AirflowSkipException)
from airflow.models.base_reschedule_poke_operator import \
    BaseReschedulePokeOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils import timezone


class BaseSensor(BaseReschedulePokeOperator, ABC):
    """
    Sensor operators are derived from this class and inherit these attributes.

    Sensor operators keep executing at a time interval and succeed when
    a criteria is met and fail if and when they time out.

    :param soft_fail: Set to true to mark the task as SKIPPED on failure
    :type soft_fail: bool
    :param poke_interval: Time in seconds that the job should wait in
        between each tries
    :type poke_interval: int
    :param timeout: Time, in seconds before the task times out and fails.
    :type timeout: int
    :param mode: How the sensor operates.
        Options are: ``{ poke | reschedule }``, default is ``poke``.
        When set to ``poke`` the sensor is taking up a worker slot for its
        whole execution time and sleeps between pokes. Use this mode if the
        expected runtime of the sensor is short or if a short poke interval
        is required. Note that the sensor will hold onto a worker slot and
        a pool slot for the duration of the sensor's runtime in this mode.
        When set to ``reschedule`` the sensor task frees the worker slot when
        the criteria is not yet met and it's rescheduled at a later time. Use
        this mode if the time before the criteria is met is expected to be
        quite long. The poke interval should be more than one minute to
        prevent too much load on the scheduler.
    :type mode: str
    """
    ui_color = '#e6f1f2'  # type: str
    valid_modes = ['poke', 'reschedule']  # type: Iterable[str]

    @apply_defaults
    def __init__(self,
                 poke_interval: float = 60,
                 timeout: float = 60 * 60 * 24 * 7,
                 soft_fail: bool = False,
                 mode: str = 'poke',
                 *args,
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.poke_interval = poke_interval
        self.soft_fail = soft_fail
        self.timeout = timeout
        self.mode = mode
        self._validate_input_values()

    def _validate_input_values(self) -> None:
        if not isinstance(self.poke_interval, (int, float)) or self.poke_interval < 0:
            raise AirflowException(
                "The poke_interval must be a non-negative number")
        if not isinstance(self.timeout, (int, float)) or self.timeout < 0:
            raise AirflowException(
                "The timeout must be a non-negative number")
        # pylint: disable=too-many-nested-blocks
        if self.mode not in self.valid_modes:
            raise AirflowException(
                "The mode must be one of {valid_modes},"
                "'{d}.{t}'; received '{m}'."
                .format(valid_modes=self.valid_modes,
                        d=self.dag.dag_id if self.dag else "",
                        t=self.task_id, m=self.mode))

    def execute(self, context: Dict):
        started_at = timezone.utcnow()
        # pylint: disable=too-many-nested-blocks
        if self.mode == 'reschedule':
            super().execute(context)
        else:
            while not self.poke(context):
                if (timezone.utcnow() -
                   started_at).total_seconds() > self.timeout:
                    # If sensor is in soft fail mode but will be retried then
                    # give it a chance and fail with timeout.
                    # This gives the ability to set up non-blocking AND
                    # soft-fail sensors.
                    if self.soft_fail and \
                       not context['ti'].is_eligible_to_retry():
                        self._do_skip_downstream_tasks(context)
                        raise AirflowSkipException('Snap. Time is OUT.')
                    raise AirflowSensorTimeout('Snap. Time is OUT.')
                sleep(self.poke_interval)
        self.log.info("Success criteria met. Exiting.")

    def submit_request(self, context: Dict) -> Optional[Union[str, List, Dict]]:
        """ A sensor doesn't submit a request, all it's logic in
        `:py:meth:`poke`."""
        return None

    def process_result(self, context: Dict):
        return None
