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

import hashlib
import os
from datetime import timedelta
from time import sleep
from typing import Any, Dict, Iterable

from airflow.exceptions import (
    AirflowException, AirflowRescheduleException, AirflowSensorTimeout, AirflowSkipException,
)
from airflow.models import BaseOperator, SkipMixin, TaskReschedule
from airflow.ti_deps.deps.ready_to_reschedule import ReadyToRescheduleDep
from airflow.utils import timezone
from airflow.utils.decorators import apply_defaults


class BaseSensorOperator(BaseOperator, SkipMixin):
    """
    Sensor operators are derived from this class and inherit these attributes.

    Sensor operators keep executing at a time interval and succeed when
    a criteria is met and fail if and when they time out.

    :param soft_fail: Set to true to mark the task as SKIPPED on failure
    :type soft_fail: bool
    :param poke_interval: Time in seconds that the job should wait in
        between each tries
    :type poke_interval: float
    :param timeout: Time, in seconds before the task times out and fails.
    :type timeout: float
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
    :param exponential_backoff: allow progressive longer waits between
        pokes by using exponential backoff algorithm
    :type exponential_backoff: bool
    """
    ui_color = '#e6f1f2'  # type: str
    valid_modes = ['poke', 'reschedule']  # type: Iterable[str]

    @apply_defaults
    def __init__(self,
                 poke_interval: float = 60,
                 timeout: float = 60 * 60 * 24 * 7,
                 soft_fail: bool = False,
                 mode: str = 'poke',
                 exponential_backoff: bool = False,
                 *args,
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.poke_interval = poke_interval
        self.soft_fail = soft_fail
        self.timeout = timeout
        self.mode = mode
        self.exponential_backoff = exponential_backoff
        self._validate_input_values()

    def _validate_input_values(self) -> None:
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

    def poke(self, context: Dict) -> bool:
        """
        Function that the sensors defined while deriving this class should
        override.
        """
        raise AirflowException('Override me.')

    def execute(self, context: Dict) -> Any:
        started_at = timezone.utcnow()
        try_number = 1
        if self.reschedule:
            # If reschedule, use first start date of current try
            task_reschedules = TaskReschedule.find_for_task_instance(context['ti'])
            if task_reschedules:
                started_at = task_reschedules[0].start_date
                try_number = len(task_reschedules) + 1
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
                    seconds=self._get_next_poke_interval(started_at, try_number))
                raise AirflowRescheduleException(reschedule_date)
            else:
                sleep(self._get_next_poke_interval(started_at, try_number))
                try_number += 1
        self.log.info("Success criteria met. Exiting.")

    def _do_skip_downstream_tasks(self, context: Dict) -> None:
        downstream_tasks = context['task'].get_flat_relatives(upstream=False)
        self.log.debug("Downstream task_ids %s", downstream_tasks)
        if downstream_tasks:
            self.skip(context['dag_run'], context['ti'].execution_date, downstream_tasks)

    def _get_next_poke_interval(self, started_at, try_number):
        """
        Using the similar logic which is used for exponential backoff retry delay for operators.
        """
        if self.exponential_backoff:
            min_backoff = int(self.poke_interval * (2 ** (try_number - 2)))
            current_time = timezone.utcnow()

            run_hash = int(hashlib.sha1("{}#{}#{}#{}".format(
                self.dag_id, self.task_id, started_at, try_number
            ).encode("utf-8")).hexdigest(), 16)
            modded_hash = min_backoff + run_hash % min_backoff

            delay_backoff_in_seconds = min(
                modded_hash,
                timedelta.max.total_seconds() - 1
            )
            new_interval = min(self.timeout - int((current_time - started_at).total_seconds()),
                               delay_backoff_in_seconds)
            self.log.info("new %s interval is %s", self.mode, new_interval)
            return new_interval
        else:
            return self.poke_interval

    @property
    def reschedule(self):
        """Define mode rescheduled sensors."""
        return self.mode == 'reschedule'

    # pylint: disable=no-member
    @property
    def deps(self):
        """
        Adds one additional dependency for all sensor operators that
        checks if a sensor task instance can be rescheduled.
        """
        if self.reschedule:
            return BaseOperator.deps.fget(self) | {ReadyToRescheduleDep()}
        return BaseOperator.deps.fget(self)


def poke_mode_only(cls):
    """
    Class Decorator for child classes of BaseSensorOperator to indicate
    that instances of this class are only safe to use poke mode.

    Will decorate all methods in the class to assert they did not change
    the mode from 'poke'.

    :param cls: BaseSensor class to enforce methods only use 'poke' mode.
    :type cls: type
    """
    def decorate(cls_type):
        def mode_getter(_):
            return 'poke'

        def mode_setter(_, value):
            if value != 'poke':
                raise ValueError(
                    f"cannot set mode to 'poke'.")

        if not issubclass(cls_type, BaseSensorOperator):
            raise ValueError(f"poke_mode_only decorator should only be "
                             f"applied to subclasses of BaseSensorOperator,"
                             f" got:{cls_type}.")

        cls_type.mode = property(mode_getter, mode_setter)

        return cls_type

    return decorate(cls)


if 'BUILDING_AIRFLOW_DOCS' in os.environ:
    # flake8: noqa: F811
    # Monkey patch hook to get good function headers while building docs
    apply_defaults = lambda x: x
