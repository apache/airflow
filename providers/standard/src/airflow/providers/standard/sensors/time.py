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
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.common.compat.sdk import BaseSensorOperator, conf, timezone
from airflow.providers.standard.triggers.temporal import DateTimeTrigger
from airflow.triggers.base import StartTriggerArgs

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
        if start_from_trigger:
            raise ValueError(
                "TimeSensor does not support start_from_trigger=True. The target moment is "
                "computed fresh from the current wall-clock time on every Dag parse, so baking "
                "it into the serialized trigger arguments makes the serialized Dag hash change "
                "on every parse. Use deferrable=True instead, which computes the target moment "
                "at task execution time and does not have this problem."
            )
        super().__init__(**kwargs)

        self.target_time = target_time
        self.deferrable = deferrable
        self.start_from_trigger = False
        self.end_from_trigger = end_from_trigger

    @property
    def target_datetime(self) -> datetime.datetime:
        """Compute the target moment on demand, in the dag's timezone."""
        aware_time = timezone.coerce_datetime(
            datetime.datetime.combine(
                datetime.datetime.now(self.dag.timezone), self.target_time, self.dag.timezone
            )
        )
        return timezone.convert_to_utc(aware_time)

    def execute(self, context: Context) -> None:
        if self.deferrable:
            self.defer(
                trigger=DateTimeTrigger(
                    moment=self.target_datetime,
                    end_from_trigger=self.end_from_trigger,
                ),
                method_name="execute_complete",
            )
        else:
            super().execute(context)

    def execute_complete(self, context: Context, event: Any = None) -> None:
        return None

    def poke(self, context: Context) -> bool:
        target_datetime = self.target_datetime
        self.log.info("Checking if the time (%s) has come", target_datetime)
        return timezone.utcnow() > target_datetime


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
