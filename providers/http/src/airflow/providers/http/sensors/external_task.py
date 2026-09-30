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

from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.http.triggers.external_task import HttpExternalTaskTrigger, _AirflowApiClient
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

if TYPE_CHECKING:
    import datetime

    from airflow.providers.common.compat.sdk import Context


class HttpExternalTaskSensor(ExternalTaskSensor):
    """
    Waits for a Dag, task group, or task of a remote Airflow deployment to complete for a logical date.

    Behaves like :class:`~airflow.providers.standard.sensors.external_task.ExternalTaskSensor`,
    but queries the REST API (v2) of the remote Airflow 3 deployment configured in the
    :ref:`http connection<howto/connection:http>` instead of the local Airflow deployment.

    Waiting for a task group requires the remote deployment to run Airflow 3.2 or later.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/operator:HttpExternalTaskSensor`

    :param http_conn_id: The :ref:`http connection<howto/connection:http>` of the remote
        Airflow deployment. (templated)

    All other parameters are the same as those of
    :class:`~airflow.providers.standard.sensors.external_task.ExternalTaskSensor`.
    """

    template_fields = [*ExternalTaskSensor.template_fields, "http_conn_id"]
    # ExternalDagLink would point to the local deployment.
    operator_extra_links = []

    def __init__(self, *, http_conn_id: str = HttpHook.default_conn_name, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.http_conn_id = http_conn_id

    @cached_property
    def _client(self) -> _AirflowApiClient:
        return _AirflowApiClient(self.http_conn_id)

    def _get_dr_count(
        self, context: Context, logical_dates: Sequence[datetime.datetime], states: list[str]
    ) -> int:
        return self._client.get_dr_count(self.external_dag_id, logical_dates, states)

    def _get_ti_count(
        self, context: Context, logical_dates: Sequence[datetime.datetime], states: list[str]
    ) -> int:
        if TYPE_CHECKING:
            assert self.external_task_ids
        return self._client.get_ti_count(self.external_dag_id, self.external_task_ids, logical_dates, states)

    def _get_task_group_states(
        self, context: Context, logical_dates: Sequence[datetime.datetime]
    ) -> dict[str, dict[str, Any]]:
        if TYPE_CHECKING:
            assert self.external_task_group_id
        return self._client.get_task_group_states(
            self.external_dag_id, self.external_task_group_id, logical_dates
        )

    def _get_trigger(
        self, context: Context, logical_dates: Sequence[datetime.datetime]
    ) -> HttpExternalTaskTrigger:
        return HttpExternalTaskTrigger(
            http_conn_id=self.http_conn_id,
            external_dag_id=self.external_dag_id,
            external_task_group_id=self.external_task_group_id,
            external_task_ids=self.external_task_ids,
            allowed_states=self.allowed_states,
            failed_states=self.failed_states,
            skipped_states=self.skipped_states,
            poke_interval=self.poke_interval,
            soft_fail=self.soft_fail,
            logical_dates=list(logical_dates),
        )
