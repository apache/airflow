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

from collections.abc import Callable, Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from datadog import api

from airflow.providers.common.compat.sdk import (
    AirflowException,
    AirflowSensorTimeout,
    AirflowSkipException,
    BaseSensorOperator,
)
from airflow.providers.datadog.hooks.datadog import DatadogHook
from airflow.providers.datadog.triggers.datadog import DatadogMonitorTrigger

try:
    from airflow.sdk.exceptions import TaskDeferralError, TaskDeferralTimeout
except ImportError:  # Airflow 2.x
    from airflow.exceptions import TaskDeferralError

    try:
        from airflow.exceptions import TaskDeferralTimeout
    except ImportError:

        class TaskDeferralTimeout(TaskDeferralError):  # type: ignore[no-redef]
            """Not raised before Airflow 3; timeouts arrive as TaskDeferralError."""


if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class DatadogSensor(BaseSensorOperator):
    """
    A sensor to listen, with a filter, to datadog event streams and determine if some event was emitted.

    Depends on the datadog API, which has to be deployed on the same server where Airflow runs.

    :param datadog_conn_id: The connection to datadog, containing metadata for api keys.
    :param from_seconds_ago: POSIX timestamp start (default 3600).
    :param up_to_seconds_from_now: POSIX timestamp end (default 0).
    :param priority: Priority of your events, either low or normal.
    :param sources: A comma separated list indicating what tags, if any,
        should be used to filter the list of monitors by scope
    :param tags: Get datadog events from specific sources.
    :param response_check: A check against the 'requests' response object. The callable takes
        the response object as the first positional argument and optionally any number of
        keyword arguments available in the context dictionary. It should return True for
        'pass' and False otherwise.
    :param response_check: Callable[[dict[str, Any]], bool] | None
    """

    ui_color = "#66c3dd"

    def __init__(
        self,
        *,
        datadog_conn_id: str = "datadog_default",
        from_seconds_ago: int = 3600,
        up_to_seconds_from_now: int = 0,
        priority: str | None = None,
        sources: str | None = None,
        tags: list[str] | None = None,
        response_check: Callable[[dict[str, Any]], bool] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.datadog_conn_id = datadog_conn_id
        self.from_seconds_ago = from_seconds_ago
        self.up_to_seconds_from_now = up_to_seconds_from_now
        self.priority = priority
        self.sources = sources
        self.tags = tags
        self.response_check = response_check

    def poke(self, context: Context) -> bool:
        # This instantiates the hook, but doesn't need it further,
        # because the API authenticates globally (unfortunately),
        # but for airflow this shouldn't matter too much, because each
        # task instance runs in its own process anyway.
        DatadogHook(datadog_conn_id=self.datadog_conn_id)

        response = api.Event.query(
            start=self.from_seconds_ago,
            end=self.up_to_seconds_from_now,
            priority=self.priority,
            sources=self.sources,
            tags=self.tags,
        )

        if isinstance(response, dict) and response.get("status", "ok") != "ok":
            self.log.error("Unexpected Datadog result: %s", response)
            message = "Datadog returned unexpected result"
            raise AirflowException(message)

        if self.response_check:
            # run content check on response
            return self.response_check(response)

        # If no check was inserted, assume any event that matched yields true.
        return bool(response)


class DatadogMonitorSensorAsync(BaseSensorOperator):
    """
    Waits for a Datadog monitor to reach one of the target states.

    This sensor always runs deferred: it waits in the triggerer and never
    occupies a worker slot while waiting.

    :param monitor_id: The id of the Datadog monitor to watch.
    :param target_states: Monitor overall states that complete the wait
        (e.g. ``("OK",)`` or ``("Alert", "Warn")``).
    :param datadog_conn_id: The connection to datadog, containing metadata for api keys.
    """

    ui_color = "#66c3dd"
    deferrable = True

    def __init__(
        self,
        *,
        monitor_id: int,
        target_states: Sequence[str] = ("OK",),
        datadog_conn_id: str = "datadog_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.monitor_id = monitor_id
        self.target_states = target_states
        self.datadog_conn_id = datadog_conn_id

    def execute(self, context: Context) -> None:
        self.defer(
            trigger=DatadogMonitorTrigger(
                monitor_id=self.monitor_id,
                target_states=self.target_states,
                datadog_conn_id=self.datadog_conn_id,
                poke_interval=self.poke_interval,
            ),
            method_name="execute_complete",
            timeout=timedelta(seconds=self.timeout),
        )

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        if event.get("status") != "success":
            raise AirflowException(f"DatadogMonitorTrigger failed: {event}")
        self.log.info("Monitor %s reached state %s", self.monitor_id, event.get("state"))

    def resume_execution(self, next_method: str, next_kwargs: dict[str, Any] | None, context: Context):
        """
        Resume from deferral, applying ``soft_fail`` only to timeouts.

        ``BaseSensorOperator.resume_execution`` converts any deferral-path failure into a skip when
        ``soft_fail`` is set, including trigger crashes (e.g. a bad monitor id or auth failure),
        which silently skips the downstream branch. This sensor instead keeps parity with poke and
        reschedule modes, where only timeouts are skippable and other errors fail the task.
        """
        try:
            return super(BaseSensorOperator, self).resume_execution(next_method, next_kwargs, context)
        except TaskDeferralError as e:
            timed_out = isinstance(e, TaskDeferralTimeout) or str(e) == "Trigger/execution timeout"
            if timed_out and self.soft_fail:
                raise AirflowSkipException(str(e)) from e
            if getattr(self, "never_fail", False):
                raise AirflowSkipException(str(e)) from e
            if timed_out:
                raise AirflowSensorTimeout(*e.args) from e
            raise
