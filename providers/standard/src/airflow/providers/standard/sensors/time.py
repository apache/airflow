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

if TYPE_CHECKING:
    from airflow.sdk import Context


class TimeSensor(BaseSensorOperator):
    """
    Waits until the specified time of the day.

    The time is evaluated against the current date in the Dag's timezone
    at execution time (poke or deferral), not at Dag-parse time. This avoids
    Dag version churn that previously occurred when target_datetime was
    baked into serialized start_trigger_args.

    :param target_time: time after which the job succeeds
    :param deferrable: whether to defer execution

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/operator:TimeSensor`

    """

    def __init__(
        self,
        *,
        target_time: datetime.time,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        end_from_trigger: bool = False,
        trigger_kwargs: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        start_from_trigger = kwargs.pop("start_from_trigger", None)
        if start_from_trigger is not None:
            warnings.warn(
                "start_from_trigger is deprecated and no longer supported for TimeSensor. "
                "It has been ignored. Target time is now always evaluated at execution time. "
                "Use deferrable=True to defer the sensor instead.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
        super().__init__(**kwargs)
        self.target_time = target_time
        self.deferrable = deferrable
        self.end_from_trigger = end_from_trigger
        # Accepted for compatibility only; storing it would reintroduce serialized Dag hash churn.
        del trigger_kwargs

    def _get_target_datetime(self) -> datetime.datetime:
        """Compute target datetime at execution time, not parse time."""
        dag_timezone = getattr(getattr(self, "dag", None), "timezone", None) or timezone.utc
        now_date = datetime.datetime.now(dag_timezone).date()
        aware_time = timezone.coerce_datetime(
            datetime.datetime.combine(now_date, self.target_time, dag_timezone)
        )
        return timezone.convert_to_utc(aware_time)

    @property
    def target_datetime(self) -> datetime.datetime:
        """Backward-compatible property, now computed at call time."""
        return self._get_target_datetime()

    def execute(self, context: Context) -> None:
        if self.deferrable:
            self.defer(
                trigger=DateTimeTrigger(
                    moment=self._get_target_datetime(),
                    end_from_trigger=self.end_from_trigger,
                ),
                method_name="execute_complete",
            )
        else:
            super().execute(context)

    def execute_complete(self, context: Context, event: Any = None) -> None:
        return None

    def poke(self, context: Context) -> bool:
        target_datetime = self._get_target_datetime()
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
