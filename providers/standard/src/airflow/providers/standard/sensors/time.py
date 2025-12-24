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
import warnings
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from airflow.configuration import conf
from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.common.compat.sdk import BaseSensorOperator, timezone
from airflow.providers.standard.triggers.temporal import DateTimeTrigger

try:
    from airflow.triggers.base import StartTriggerArgs  # type: ignore[no-redef]
except ImportError:  # TODO: Remove this when min airflow version is 2.10.0 for standard provider

    @dataclass
    class StartTriggerArgs:  # type: ignore[no-redef]
        """Arguments required for start task execution from triggerer."""

        trigger_cls: str
        next_method: str
        trigger_kwargs: dict[str, Any] | None = None
        next_kwargs: dict[str, Any] | None = None
        timeout: datetime.timedelta | None = None


if TYPE_CHECKING:
    from airflow.sdk import Context


class TimeSensor(BaseSensorOperator):
    """
    Waits until the specified time of the day.

    :param target_time: time after which the job succeeds
    :param deferrable: whether to defer execution

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/operator:TimeSensor`

    """

    start_trigger_args = StartTriggerArgs(
        trigger_cls="airflow.providers.standard.triggers.temporal.DateTimeTrigger",
        trigger_kwargs={"moment": "", "end_from_trigger": False},
        next_method="execute_complete",
        next_kwargs=None,
        timeout=None,
    )
    start_from_trigger = False

    def __init__(
        self,
        *,
        target_time: datetime.time,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        start_from_trigger: bool = False,
        end_from_trigger: bool = False,
        trigger_kwargs: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        # Create a "date-aware" timestamp that will be used as the "target_datetime". This is a requirement
        # of the DateTimeTrigger

        # Get date considering dag.timezone
        aware_time = timezone.coerce_datetime(
            datetime.datetime.combine(
                datetime.datetime.now(self.dag.timezone), target_time, self.dag.timezone
            )
        )

        # Now that the dag's timezone has made the datetime timezone aware, we need to convert to UTC
        self.target_datetime = timezone.convert_to_utc(aware_time)
        self.deferrable = deferrable
        self.start_from_trigger = start_from_trigger
        self.end_from_trigger = end_from_trigger

        if self.start_from_trigger:
            self.start_trigger_args.trigger_kwargs = dict(
                moment=self.target_datetime, end_from_trigger=self.end_from_trigger
            )

    def execute(self, context: Context) -> None:
        if self.deferrable:
            self.defer(
                trigger=DateTimeTrigger(
                    moment=self.target_datetime,  # This needs to be an aware timestamp
                    end_from_trigger=self.end_from_trigger,
                ),
                method_name="execute_complete",
            )
        else:
            super().execute(context)

    def execute_complete(self, context: Context, event: Any = None) -> None:
        return None

    def poke(self, context: Context) -> bool:
        self.log.info("Checking if the time (%s) has come", self.target_datetime)

        # self.target_date has been converted to UTC, so we do not need to convert timezone
        return timezone.utcnow() > self.target_datetime


class TimeSensorAsync(TimeSensor):
    """
    Deprecated. Use TimeSensor with deferrable=True instead.

    :sphinx-autoapi-skip:
    """

    def __init__(self, **kwargs) -> None:
        warnings.warn(
            "TimeSensorAsync is deprecated and will be removed in a future version. Use `TimeSensor` with deferrable=True instead.",
            AirflowProviderDeprecationWarning,
            stacklevel=2,
        )
        super().__init__(deferrable=True, **kwargs)
