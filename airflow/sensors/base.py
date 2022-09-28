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

import datetime
import functools
import hashlib
import time
from datetime import timedelta
from typing import Any, Callable, Iterable

from airflow import settings
from airflow.configuration import conf
from airflow.exceptions import (
    AirflowException,
    AirflowRescheduleException,
    AirflowSensorTimeout,
    AirflowSkipException,
)
from airflow.models.baseoperator import BaseOperator
from airflow.models.skipmixin import SkipMixin
from airflow.models.taskreschedule import TaskReschedule
from airflow.ti_deps.deps.ready_to_reschedule import ReadyToRescheduleDep
from airflow.utils import timezone
from airflow.utils.context import Context

# We need to keep the import here because GCSToLocalFilesystemOperator released in
# Google Provider before 3.0.0 imported apply_defaults from here.
# See  https://github.com/apache/airflow/issues/16035
from airflow.utils.decorators import apply_defaults  # noqa: F401

# As documented in https://dev.mysql.com/doc/refman/5.7/en/datetime.html.
_MYSQL_TIMESTAMP_MAX = datetime.datetime(2038, 1, 19, 3, 14, 7, tzinfo=timezone.utc)


@functools.lru_cache(maxsize=None)
def _is_metadatabase_mysql() -> bool:
    if settings.engine is None:
        raise AirflowException("Must initialize ORM first")
    return settings.engine.url.get_backend_name() == "mysql"


class PokeReturnValue:
    """
    Sensors can optionally return an instance of the PokeReturnValue class in the poke method.
    If an XCom value is supplied when the sensor is done, then the XCom value will be
    pushed through the operator return value.
    :param is_done: Set to true to indicate the sensor can stop poking.
    :param xcom_value: An optional XCOM value to be returned by the operator.
    """

    def __init__(self, is_done: bool, xcom_value: Any | None = None) -> None:
        self.xcom_value = xcom_value
        self.is_done = is_done

    def __bool__(self) -> bool:
        return self.is_done


class BaseSensorOperator(BaseOperator, SkipMixin):
    """
    Sensor operators are derived from this class and inherit these attributes.

    Sensor operators keep executing at a time interval and succeed when
    a criteria is met and fail if and when they time out.

    :param soft_fail: Set to true to mark the task as SKIPPED on failure
    :param poke_interval: Time in seconds that the job should wait in
        between each try
    :param timeout: Time, in seconds before the task times out and fails.
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
    :param exponential_backoff: allow progressive longer waits between
        pokes by using exponential backoff algorithm
    """

    ui_color = '#e6f1f2'  # type: str
    valid_modes = ['poke', 'reschedule']  # type: Iterable[str]

    # Adds one additional dependency for all sensor operators that checks if a
    # sensor task instance can be rescheduled.
    deps = BaseOperator.deps | {ReadyToRescheduleDep()}

    def __init__(
        self,
        *,
        poke_interval: float = 60,
        timeout: float = conf.getfloat('sensors', 'default_timeout'),
        soft_fail: bool = False,
        mode: str = 'poke',
        exponential_backoff: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.poke_interval = poke_interval
        self.soft_fail = soft_fail
        self.timeout = timeout
        self.mode = mode
        self.exponential_backoff = exponential_backoff
        self._validate_input_values()

    def _validate_input_values(self) -> None:
        if not isinstance(self.poke_interval, (int, float)) or self.poke_interval < 0:
            raise AirflowException("The poke_interval must be a non-negative number")
        if not isinstance(self.timeout, (int, float)) or self.timeout < 0:
            raise AirflowException("The timeout must be a non-negative number")
        if self.mode not in self.valid_modes:
            raise AirflowException(
                f"The mode must be one of {self.valid_modes},'{self.dag.dag_id if self.has_dag() else ''} "
                f".{self.task_id}'; received '{self.mode}'."
            )

        # Quick check for poke_interval isn't immediately over MySQL's TIMESTAMP limit.
        # This check is only rudimentary to catch trivial user errors, e.g. mistakenly
        # set the value to milliseconds instead of seconds. There's another check when
        # we actually try to reschedule to ensure database coherence.
        if self.reschedule and _is_metadatabase_mysql():
            if timezone.utcnow() + datetime.timedelta(seconds=self.poke_interval) > _MYSQL_TIMESTAMP_MAX:
                raise AirflowException(
                    f"Cannot set poke_interval to {self.poke_interval} seconds in reschedule "
                    f"mode since it will take reschedule time over MySQL's TIMESTAMP limit."
                )

    def poke(self, context: Context) -> bool | PokeReturnValue:
        """
        Function that the sensors defined while deriving this class should
        override.
        """
        raise AirflowException('Override me.')

    def execute(self, context: Context) -> Any:
        started_at: datetime.datetime | float

        if self.reschedule:

            # If reschedule, use the start date of the first try (first try can be either the very
            # first execution of the task, or the first execution after the task was cleared.)
            first_try_number = context['ti'].max_tries - self.retries + 1
            task_reschedules = TaskReschedule.find_for_task_instance(
                context['ti'], try_number=first_try_number
            )
            if not task_reschedules:
                start_date = timezone.utcnow()
            else:
                start_date = task_reschedules[0].start_date
            started_at = start_date

            def run_duration() -> float:
                # If we are in reschedule mode, then we have to compute diff
                # based on the time in a DB, so can't use time.monotonic
                return (timezone.utcnow() - start_date).total_seconds()

        else:
            started_at = start_monotonic = time.monotonic()

            def run_duration() -> float:
                return time.monotonic() - start_monotonic

        try_number = 1
        log_dag_id = self.dag.dag_id if self.has_dag() else ""

        xcom_value = None
        while True:
            poke_return = self.poke(context)
            if poke_return:
                if isinstance(poke_return, PokeReturnValue):
                    xcom_value = poke_return.xcom_value
                break

            if run_duration() > self.timeout:
                # If sensor is in soft fail mode but times out raise AirflowSkipException.
                if self.soft_fail:
                    raise AirflowSkipException(f"Snap. Time is OUT. DAG id: {log_dag_id}")
                else:
                    raise AirflowSensorTimeout(f"Snap. Time is OUT. DAG id: {log_dag_id}")
            if self.reschedule:
                next_poke_interval = self._get_next_poke_interval(started_at, run_duration, try_number)
                reschedule_date = timezone.utcnow() + timedelta(seconds=next_poke_interval)
                if _is_metadatabase_mysql() and reschedule_date > _MYSQL_TIMESTAMP_MAX:
                    raise AirflowSensorTimeout(
                        f"Cannot reschedule DAG {log_dag_id} to {reschedule_date.isoformat()} "
                        f"since it is over MySQL's TIMESTAMP storage limit."
                    )
                raise AirflowRescheduleException(reschedule_date)
            else:
                time.sleep(self._get_next_poke_interval(started_at, run_duration, try_number))
                try_number += 1
        self.log.info("Success criteria met. Exiting.")
        return xcom_value

    def _get_next_poke_interval(
        self,
        started_at: datetime.datetime | float,
        run_duration: Callable[[], float],
        try_number: int,
    ) -> float:
        """Using the similar logic which is used for exponential backoff retry delay for operators."""
        if not self.exponential_backoff:
            return self.poke_interval

        min_backoff = int(self.poke_interval * (2 ** (try_number - 2)))

        run_hash = int(
            hashlib.sha1(f"{self.dag_id}#{self.task_id}#{started_at}#{try_number}".encode()).hexdigest(),
            16,
        )
        modded_hash = min_backoff + run_hash % min_backoff

        delay_backoff_in_seconds = min(modded_hash, timedelta.max.total_seconds() - 1)
        new_interval = min(self.timeout - int(run_duration()), delay_backoff_in_seconds)
        self.log.info("new %s interval is %s", self.mode, new_interval)
        return new_interval

    def prepare_for_execution(self) -> BaseOperator:
        task = super().prepare_for_execution()
        # Sensors in `poke` mode can block execution of DAGs when running
        # with single process executor, thus we change the mode to`reschedule`
        # to allow parallel task being scheduled and executed
        if conf.get('core', 'executor') == "DebugExecutor":
            self.log.warning("DebugExecutor changes sensor mode to 'reschedule'.")
            task.mode = 'reschedule'
        return task

    @property
    def reschedule(self):
        """Define mode rescheduled sensors."""
        return self.mode == 'reschedule'

    @classmethod
    def get_serialized_fields(cls):
        return super().get_serialized_fields() | {"reschedule"}


def poke_mode_only(cls):
    """
    Class Decorator for child classes of BaseSensorOperator to indicate
    that instances of this class are only safe to use poke mode.

    Will decorate all methods in the class to assert they did not change
    the mode from 'poke'.

    :param cls: BaseSensor class to enforce methods only use 'poke' mode.
    """

    def decorate(cls_type):
        def mode_getter(_):
            return 'poke'

        def mode_setter(_, value):
            if value != 'poke':
                raise ValueError("cannot set mode to 'poke'.")

        if not issubclass(cls_type, BaseSensorOperator):
            raise ValueError(
                f"poke_mode_only decorator should only be "
                f"applied to subclasses of BaseSensorOperator,"
                f" got:{cls_type}."
            )

        cls_type.mode = property(mode_getter, mode_setter)

        return cls_type

    return decorate(cls)
