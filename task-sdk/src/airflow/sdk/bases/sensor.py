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
import hashlib
import time
import traceback
from collections.abc import Iterable
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Callable

from airflow.configuration import conf
from airflow.exceptions import (
    AirflowException,
    AirflowFailException,
    AirflowRescheduleException,
    AirflowSensorTimeout,
    AirflowSkipException,
    AirflowTaskTimeout,
    TaskDeferralError,
    TaskDeferralTimeout,
)
from airflow.sdk.bases.operator import BaseOperator
from airflow.utils import timezone

if TYPE_CHECKING:
    from airflow.sdk.definitions.context import Context


class PokeReturnValue:
    """
    Optional return value for poke methods.

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


class BaseSensorOperator(BaseOperator):
    """
    Sensor operators are derived from this class and inherit these attributes.

    Sensor operators keep executing at a time interval and succeed when
    a criteria is met and fail if and when they time out.

    :param soft_fail: Set to true to mark the task as SKIPPED on failure.
           Mutually exclusive with never_fail.
    :param poke_interval: Time that the job should wait in between each try.
        Can be ``timedelta`` or ``float`` seconds.
    :param timeout: Time elapsed before the task times out and fails.
        Can be ``timedelta`` or ``float`` seconds.
        This should not be confused with ``execution_timeout`` of the
        ``BaseOperator`` class. ``timeout`` measures the time elapsed between the
        first poke and the current time (taking into account any
        reschedule delay between each poke), while ``execution_timeout``
        checks the **running** time of the task (leaving out any reschedule
        delay). In case that the ``mode`` is ``poke`` (see below), both of
        them are equivalent (as the sensor is never rescheduled), which is not
        the case in ``reschedule`` mode.
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
    :param max_wait: maximum wait interval between pokes, can be ``timedelta`` or ``float`` seconds
    :param silent_fail: If true, and poke method raises an exception different from
        AirflowSensorTimeout, AirflowTaskTimeout, AirflowSkipException
        and AirflowFailException, the sensor will log the error and continue
        its execution. Otherwise, the sensor task fails, and it can be retried
        based on the provided `retries` parameter.
    :param never_fail: If true, and poke method raises an exception, sensor will be skipped.
           Mutually exclusive with soft_fail.
    """

    ui_color: str = "#e6f1f2"
    valid_modes: Iterable[str] = ["poke", "reschedule"]

    _is_sensor: bool = True

    def __init__(
        self,
        *,
        poke_interval: timedelta | float = 60,
        timeout: timedelta | float = conf.getfloat("sensors", "default_timeout"),
        soft_fail: bool = False,
        mode: str = "poke",
        exponential_backoff: bool = False,
        max_wait: timedelta | float | None = None,
        silent_fail: bool = False,
        never_fail: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.poke_interval = self._coerce_poke_interval(poke_interval).total_seconds()
        self.soft_fail = soft_fail
        self.timeout: int | float = self._coerce_timeout(timeout).total_seconds()
        self.mode = mode
        self.exponential_backoff = exponential_backoff
        self.max_wait = self._coerce_max_wait(max_wait)
        if soft_fail is True and never_fail is True:
            raise ValueError("soft_fail and never_fail are mutually exclusive, you can not provide both.")

        self.silent_fail = silent_fail
        self.never_fail = never_fail
        self._validate_input_values()

    @staticmethod
    def _coerce_poke_interval(poke_interval: float | timedelta) -> timedelta:
        if isinstance(poke_interval, timedelta):
            return poke_interval
        if isinstance(poke_interval, (int, float)) and poke_interval >= 0:
            return timedelta(seconds=poke_interval)
        raise AirflowException(
            "Operator arg `poke_interval` must be timedelta object or a non-negative number"
        )

    @staticmethod
    def _coerce_timeout(timeout: float | timedelta) -> timedelta:
        if isinstance(timeout, timedelta):
            return timeout
        if isinstance(timeout, (int, float)) and timeout >= 0:
            return timedelta(seconds=timeout)
        raise AirflowException("Operator arg `timeout` must be timedelta object or a non-negative number")

    @staticmethod
    def _coerce_max_wait(max_wait: float | timedelta | None) -> timedelta | None:
        if max_wait is None or isinstance(max_wait, timedelta):
            return max_wait
        if isinstance(max_wait, (int, float)) and max_wait >= 0:
            return timedelta(seconds=max_wait)
        raise AirflowException("Operator arg `max_wait` must be timedelta object or a non-negative number")

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

    def poke(self, context: Context) -> bool | PokeReturnValue:
        """Override when deriving this class."""
        raise AirflowException("Override me.")

    def execute(self, context: Context) -> Any:
        started_at: datetime.datetime | float

        if self.reschedule:
            ti = context["ti"]
            first_reschedule_date = ti.get_first_reschedule_date(context)
            started_at = start_date = first_reschedule_date or timezone.utcnow()

            def run_duration() -> float:
                # If we are in reschedule mode, then we have to compute diff
                # based on the time in a DB, so can't use time.monotonic
                return (timezone.utcnow() - start_date).total_seconds()

        else:
            started_at = start_monotonic = time.monotonic()

            def run_duration() -> float:
                return time.monotonic() - start_monotonic

        poke_count = 1

        xcom_value = None
        while True:
            try:
                poke_return = self.poke(context)
            except (
                AirflowSensorTimeout,
                AirflowTaskTimeout,
                AirflowFailException,
            ) as e:
                if self.soft_fail:
                    raise AirflowSkipException("Skipping due to soft_fail is set to True.") from e
                if self.never_fail:
                    raise AirflowSkipException("Skipping due to never_fail is set to True.") from e
                raise e
            except AirflowSkipException as e:
                raise e
            except Exception as e:
                if self.silent_fail:
                    self.log.error("Sensor poke failed: \n %s", traceback.format_exc())
                    poke_return = False
                elif self.never_fail:
                    raise AirflowSkipException("Skipping due to never_fail is set to True.") from e
                else:
                    raise e

            if poke_return:
                if isinstance(poke_return, PokeReturnValue):
                    xcom_value = poke_return.xcom_value
                break

            if run_duration() > self.timeout:
                # If sensor is in soft fail mode but times out raise AirflowSkipException.
                message = (
                    f"Sensor has timed out; run duration of {run_duration()} seconds exceeds "
                    f"the specified timeout of {self.timeout}."
                )

                if self.soft_fail:
                    raise AirflowSkipException(message)
                raise AirflowSensorTimeout(message)
            if self.reschedule:
                next_poke_interval = self._get_next_poke_interval(started_at, run_duration, poke_count)
                reschedule_date = timezone.utcnow() + timedelta(seconds=next_poke_interval)
                raise AirflowRescheduleException(reschedule_date)
            time.sleep(self._get_next_poke_interval(started_at, run_duration, poke_count))
            poke_count += 1
        self.log.info("Success criteria met. Exiting.")
        return xcom_value

    def resume_execution(self, next_method: str, next_kwargs: dict[str, Any] | None, context: Context):
        try:
            return super().resume_execution(next_method, next_kwargs, context)
        except TaskDeferralTimeout as e:
            raise AirflowSensorTimeout(*e.args) from e
        except (AirflowException, TaskDeferralError) as e:
            if self.soft_fail:
                raise AirflowSkipException(str(e)) from e
            raise

    def _get_next_poke_interval(
        self,
        started_at: datetime.datetime | float,
        run_duration: Callable[[], float],
        poke_count: int,
    ) -> float:
        """Use similar logic which is used for exponential backoff retry delay for operators."""
        if not self.exponential_backoff:
            return self.poke_interval

        if self.reschedule:
            # Calculate elapsed time since the sensor started
            elapsed_time = run_duration()

            # Initialize variables for the simulation
            cumulative_time: float = 0.0
            estimated_poke_count: int = 0

            while cumulative_time <= elapsed_time:
                estimated_poke_count += 1
                # Calculate min_backoff for the current try number
                min_backoff = max(int(self.poke_interval * (2 ** (estimated_poke_count - 2))), 1)

                # Calculate the jitter
                run_hash = int(
                    hashlib.sha1(
                        f"{self.dag_id}#{self.task_id}#{started_at}#{estimated_poke_count}".encode(),
                        usedforsecurity=False,
                    ).hexdigest(),
                    16,
                )
                modded_hash = min_backoff + run_hash % min_backoff

                # Calculate the jitter, which is used to prevent multiple sensors simultaneously poking
                interval_with_jitter = min(modded_hash, timedelta.max.total_seconds() - 1)

                # Add the interval to the cumulative time
                cumulative_time += interval_with_jitter

            # Now we have an estimated_poke_count based on the elapsed time
            poke_count = estimated_poke_count or poke_count

        # The value of min_backoff should always be greater than or equal to 1.
        min_backoff = max(int(self.poke_interval * (2 ** (poke_count - 2))), 1)

        run_hash = int(
            hashlib.sha1(
                f"{self.dag_id}#{self.task_id}#{started_at}#{poke_count}".encode(), usedforsecurity=False
            ).hexdigest(),
            16,
        )
        modded_hash = min_backoff + run_hash % min_backoff

        delay_backoff_in_seconds = min(modded_hash, timedelta.max.total_seconds() - 1)
        new_interval = min(self.timeout - int(run_duration()), delay_backoff_in_seconds)

        if self.max_wait:
            new_interval = min(self.max_wait.total_seconds(), new_interval)

        self.log.info("new %s interval is %s", self.mode, new_interval)
        return new_interval

    @property
    def reschedule(self):
        """Define mode rescheduled sensors."""
        return self.mode == "reschedule"

    @classmethod
    def get_serialized_fields(cls):
        return super().get_serialized_fields() | {"reschedule", "_is_sensor"}


def poke_mode_only(cls):
    """
    Decorate a subclass of BaseSensorOperator with poke.

    Indicate that instances of this class are only safe to use poke mode.

    Will decorate all methods in the class to assert they did not change
    the mode from 'poke'.

    :param cls: BaseSensor class to enforce methods only use 'poke' mode.
    """

    def decorate(cls_type):
        def mode_getter(_):
            return "poke"

        def mode_setter(_, value):
            if value != "poke":
                raise ValueError(f"Cannot set mode to '{value}'. Only 'poke' is acceptable")

        if not issubclass(cls_type, BaseSensorOperator):
            raise ValueError(
                f"poke_mode_only decorator should only be "
                f"applied to subclasses of BaseSensorOperator,"
                f" got:{cls_type}."
            )

        cls_type.mode = property(mode_getter, mode_setter)

        return cls_type

    return decorate(cls)
