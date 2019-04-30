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


from time import sleep
from datetime import timedelta

from airflow.exceptions import AirflowException, AirflowSensorTimeout, \
    AirflowSkipException, AirflowRescheduleException
from airflow.models import BaseOperator, SkipMixin, TaskReschedule
from airflow.utils import timezone
from airflow.utils.decorators import apply_defaults
from airflow.ti_deps.deps.ready_to_reschedule import ReadyToRescheduleDep


class BaseSensorOperator(BaseOperator, SkipMixin):
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
        is required.
        When set to ``reschedule`` the sensor task frees the worker slot when
        the criteria is not yet met and it's rescheduled at a later time. Use
        this mode if the expected time until the criteria is met is. The poke
        interval should be more than one minute to prevent too much load on
        the scheduler.
    :type mode: str
    """
    ui_color = '#e6f1f2'
    valid_modes = ['poke', 'reschedule']

    @apply_defaults
    def __init__(self,
                 poke_interval=60,
                 timeout=60 * 60 * 24 * 7,
                 soft_fail=False,
                 mode='poke',
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.poke_interval = poke_interval
        self.soft_fail = soft_fail
        self.timeout = timeout
        self.mode = mode
        self._validate_input_values()

    def _validate_input_values(self):
        if not isinstance(self.poke_interval, (int, float)) or self.poke_interval < 0:
            raise AirflowException(
                "The poke_interval must be a non-negative number")
        if not isinstance(self.timeout, (int, float)) or self.timeout < 0:
            raise AirflowException(
                "The timeout must be a non-negative number")
        if self.mode not in self.valid_modes:
            raise AirflowException(
                "The mode must be one of {valid_modes},"
                "'{d}.{t}'; received '{m}'."
                .format(valid_modes=self.valid_modes,
                        d=self.dag.dag_id if self.dag else "",
                        t=self.task_id, m=self.mode))

    def poke(self, context):
        """
        Function that the sensors defined while deriving this class should
        override.
        """
        raise AirflowException('Override me.')

    def execute(self, context):
        started_at = timezone.utcnow()
        if self.reschedule:
            # If reschedule, use first start date of current try
            task_reschedules = TaskReschedule.find_for_task_instance(context['ti'])
            if task_reschedules:
                started_at = task_reschedules[0].start_date
        while not self.poke(context):
            if (timezone.utcnow() - started_at).total_seconds() > self.timeout:
                # If sensor is in soft fail mode but will be retried then
                # give it a chance and fail with timeout.
                # This gives the ability to set up non-blocking AND soft-fail sensors.
                if self.soft_fail and not context['ti'].is_eligible_to_retry():
                    self._do_skip_downstream_tasks(context)
                    raise AirflowSkipException('Snap. Time is OUT.')
                else:
                    raise AirflowSensorTimeout('Snap. Time is OUT.')
            if self.reschedule:
                reschedule_date = timezone.utcnow() + timedelta(
                    seconds=self.poke_interval)
                raise AirflowRescheduleException(reschedule_date)
            else:
                sleep(self.poke_interval)
        self.log.info("Success criteria met. Exiting.")

    def _do_skip_downstream_tasks(self, context):
        downstream_tasks = context['task'].get_flat_relatives(upstream=False)
        self.log.debug("Downstream task_ids %s", downstream_tasks)
        if downstream_tasks:
            self.skip(context['dag_run'], context['ti'].execution_date, downstream_tasks)

    @property
    def reschedule(self):
        return self.mode == 'reschedule'

    @property
    def deps(self):
        """
        Adds one additional dependency for all sensor operators that
        checks if a sensor task instance can be rescheduled.
        """
        return BaseOperator.deps.fget(self) | {ReadyToRescheduleDep()}
