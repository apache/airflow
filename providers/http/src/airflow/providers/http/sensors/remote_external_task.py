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
"""Sensor that waits for a DAG or tasks in another Airflow instance through its REST API."""

from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from requests.auth import AuthBase

from airflow.providers.common.compat.sdk import (
    AirflowException,
    AirflowFailException,
    AirflowOptionalProviderFeatureException,
    BaseSensorOperator,
)
from airflow.providers.http.hooks.http import HttpHook

try:
    from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
except ImportError as e:
    raise AirflowOptionalProviderFeatureException(e)

if TYPE_CHECKING:
    from requests import PreparedRequest, Response

    from airflow.sdk import Context

SUPPORTED_API_VERSIONS = ("v1", "v2")
# 429 Too Many Requests and 5xx can be temporary problems of the remote Airflow: let retries handle them.
RETRYABLE_STATUS_CODES = frozenset({429})
TASK_INSTANCES_PAGE_LIMIT = 100


class _BearerTokenAuth(AuthBase):
    """Send an Airflow 3 API access token in the ``Authorization`` header."""

    def __init__(self, token: str) -> None:
        self.token = token

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        request.headers["Authorization"] = f"Bearer {self.token}"
        return request


class RemoteExternalTaskSensor(ExternalTaskSensor):
    """
    Wait for a DAG, or tasks of a DAG, in another Airflow instance to reach a state.

    Works like :class:`~airflow.providers.standard.sensors.external_task.ExternalTaskSensor` and takes
    the same arguments, but reads the states from the REST API of a remote Airflow instead of the
    metadata database of the Airflow the sensor runs in.

    The remote Airflow is reached through an HTTP connection: host (and port) of its API server or
    webserver, and optionally a login and password.

    * ``api_version="v1"`` for a remote Airflow 2: the stable REST API ``/api/v1``. The login and
      password, if set, are sent with basic authentication.
    * ``api_version="v2"`` for a remote Airflow 3: the public REST API ``/api/v2``. The login and
      password, if set, are exchanged for an access token at ``/auth/token`` on every poke.

    The external DAG run is matched by its logical date (``execution_date`` in Airflow 2), computed
    from the logical date of the sensor's own run with ``execution_delta`` or ``execution_date_fn``
    exactly as in ``ExternalTaskSensor``.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/operator:RemoteExternalTaskSensor`

    :param http_conn_id: The :ref:`HTTP connection <howto/connection:http>` to the remote Airflow.
    :param api_version: ``"v1"`` for a remote Airflow 2, ``"v2"`` for a remote Airflow 3.

    Task groups (``external_task_group_id``) and deferrable mode are not supported yet.
    ``check_existence=True`` checks the DAG and tasks in the remote Airflow before the first poke.
    """

    # ExternalDagLink of the parent points to the UI of the Airflow the sensor runs in.
    operator_extra_links: list = []

    def __init__(
        self,
        *,
        http_conn_id: str,
        api_version: str = "v2",
        deferrable: bool = False,
        **kwargs: Any,
    ) -> None:
        if api_version not in SUPPORTED_API_VERSIONS:
            raise ValueError(
                f"api_version must be one of {SUPPORTED_API_VERSIONS}: "
                "'v1' for a remote Airflow 2, 'v2' for a remote Airflow 3."
            )
        if deferrable:
            raise ValueError("RemoteExternalTaskSensor does not support deferrable mode yet.")
        super().__init__(deferrable=False, **kwargs)
        if self.external_task_group_id:
            raise ValueError("RemoteExternalTaskSensor does not support external_task_group_id yet.")
        self.http_conn_id = http_conn_id
        self.api_version = api_version
        self._access_token: str | None = None

    # --- connecting to the remote Airflow ------------------------------------------------------------

    def _get_remote_api_hook(self, method: str) -> HttpHook:
        """HttpHook for the remote Airflow API, authenticated for the configured API version."""
        if self.api_version == "v2" and self._access_token:
            token = self._access_token
            return HttpHook(
                method=method,
                http_conn_id=self.http_conn_id,
                auth_type=lambda *_: _BearerTokenAuth(token),
            )
        # v1 with a login: HttpHook uses basic authentication. No login: no authentication.
        return HttpHook(method=method, http_conn_id=self.http_conn_id)

    def _fetch_access_token(self) -> str | None:
        """Exchange the connection login and password for an Airflow 3 API access token."""
        conn = HttpHook.get_connection(self.http_conn_id)
        if not conn.login:
            return None
        response = HttpHook(method="POST", http_conn_id=self.http_conn_id).run(
            "auth/token",
            json={"username": conn.login, "password": conn.password or ""},
            extra_options={"check_response": False},
        )
        self._raise_for_remote_error(response)
        return response.json()["access_token"]

    def _build_remote_api_endpoint(self, path: str) -> str:
        return f"api/{self.api_version}/{path.lstrip('/')}"

    @staticmethod
    def _raise_for_remote_error(response: Response) -> None:
        if response.status_code < 400:
            return
        message = (
            f"{response.request.method} {response.url} returned {response.status_code}: {response.text[:500]}"
        )
        if response.status_code >= 500 or response.status_code in RETRYABLE_STATUS_CODES:
            # The remote Airflow may be restarting or overloaded: fail this try, keep the task retries.
            raise AirflowException(message)
        # A wrong URL, filter or credentials give the same error on the next try: fail without retries.
        raise AirflowFailException(message)

    # --- asking the remote Airflow -------------------------------------------------------------------

    def _count_remote_records(self, path: str, filters: dict[str, Any]) -> int:
        """Return how many records the remote list endpoint finds for the filters."""
        # Only the total is needed: one record is enough, total_entries is still the full count.
        response = self._get_remote_api_hook("POST").run(
            self._build_remote_api_endpoint(path),
            json={**filters, "page_limit": 1},
            extra_options={"check_response": False},
        )
        self._raise_for_remote_error(response)
        return int(response.json()["total_entries"])

    def _build_exact_date_filter(self, date: datetime.datetime) -> dict[str, str]:
        """Filter that matches exactly this date: the list endpoints only have from/to date filters."""
        date_field = "execution_date" if self.api_version == "v1" else "logical_date"
        date_in_utc = date.astimezone(datetime.UTC).isoformat()
        return {f"{date_field}_gte": date_in_utc, f"{date_field}_lte": date_in_utc}

    def _count_remote_dag_runs_in_states(
        self, dttm_filter: Sequence[datetime.datetime], states: list[str]
    ) -> int:
        """
        Count remote DAG runs on exactly the dates of ``dttm_filter`` that are in these states.

        A DAG has at most one run per logical date (unique ``dag_id``, ``logical_date``), so the count is
        the number of dates whose run is in one of the states.
        """
        return sum(
            self._count_remote_records(
                "dags/~/dagRuns/list",
                {"dag_ids": [self.external_dag_id], "states": states, **self._build_exact_date_filter(date)},
            )
            for date in dttm_filter
        )

    def _get_remote_task_states(self, date: datetime.datetime) -> dict[str, list[str | None]]:
        """
        Return the states of all remote task instances of ``external_task_ids`` on exactly this date.

        Keyed by task id. A mapped task has one task instance per map index, so it can have several states.
        """
        states_by_task: dict[str, list[str | None]] = {}
        offset = 0
        while True:
            response = self._get_remote_api_hook("POST").run(
                self._build_remote_api_endpoint("dags/~/dagRuns/~/taskInstances/list"),
                json={
                    "dag_ids": [self.external_dag_id],
                    "task_ids": list(self.external_task_ids or []),
                    **self._build_exact_date_filter(date),
                    "page_offset": offset,
                    "page_limit": TASK_INSTANCES_PAGE_LIMIT,
                },
                extra_options={"check_response": False},
            )
            self._raise_for_remote_error(response)
            body = response.json()
            task_instances = body["task_instances"]
            for task_instance in task_instances:
                states_by_task.setdefault(task_instance["task_id"], []).append(task_instance["state"])
            offset += len(task_instances)
            if not task_instances or offset >= body["total_entries"]:
                return states_by_task

    def _count_dates_with_any_task_in_states(
        self, task_states_by_date: list[dict[str, list[str | None]]], states: list[str]
    ) -> int:
        """Count dates on which at least one task instance (any map index) is in these states."""
        return sum(
            any(state in states for task_states in states_by_task.values() for state in task_states)
            for states_by_task in task_states_by_date
        )

    def _count_dates_with_all_tasks_in_states(
        self, task_states_by_date: list[dict[str, list[str | None]]], states: list[str]
    ) -> int:
        """
        Count dates on which every external task is in these states.

        A task counts only if it has task instances on that date and all of them (every map index of a
        mapped task) are in the states, so extra map indexes of one task cannot stand in for another task.
        """
        return sum(
            all(
                states_by_task.get(task_id) and all(state in states for state in states_by_task[task_id])
                for task_id in self.external_task_ids or []
            )
            for states_by_task in task_states_by_date
        )

    def _check_remote_dag_and_tasks_exist(self) -> None:
        """Fail right away if the remote DAG or one of the external tasks does not exist."""
        hook = self._get_remote_api_hook("GET")
        paths = [f"dags/{self.external_dag_id}"] + [
            f"dags/{self.external_dag_id}/tasks/{task_id}" for task_id in self.external_task_ids or []
        ]
        for path in paths:
            response = hook.run(
                self._build_remote_api_endpoint(path), extra_options={"check_response": False}
            )
            if response.status_code == 404:
                raise AirflowFailException(f"{path} does not exist in the remote Airflow: {response.url}")
            self._raise_for_remote_error(response)

    # --- sensor --------------------------------------------------------------------------------------

    def execute(self, context: Context) -> None:
        # ExternalTaskSensor.execute defers to a trigger that reads the local metadata database.
        BaseSensorOperator.execute(self, context)

    def _validate_rendered_ids(self) -> None:
        """
        Fail on ids that are only known after templating: empty, blank or duplicated.

        ``ExternalTaskSensor`` validates the arguments in ``__init__``, before templates such as
        ``{{ params.task_id }}`` are rendered; an empty id would otherwise be sent to the remote API.
        """
        if not str(self.external_dag_id or "").strip():
            raise AirflowFailException("external_dag_id is empty after rendering the templates.")
        task_ids = list(self.external_task_ids or [])
        if any(not str(task_id).strip() for task_id in task_ids):
            raise AirflowFailException(
                f"external_task_ids contains an empty task id after rendering the templates: {task_ids}"
            )
        if len(task_ids) != len(set(task_ids)):
            raise AirflowFailException(f"Duplicate task ids in external_task_ids: {task_ids}")

    def poke(self, context: Context) -> bool:
        self._validate_rendered_ids()
        dttm_filter = self._get_dttm_filter(context)
        target = f"tasks {self.external_task_ids}" if self.external_task_ids else "DAG run"
        self.log.info(
            "Poking remote Airflow (connection %s, API %s) for %s of DAG %s on %s",
            self.http_conn_id,
            self.api_version,
            target,
            self.external_dag_id,
            ", ".join(date.isoformat() for date in dttm_filter),
        )

        if self.api_version == "v2":
            # A fresh token on every poke: a sensor can wait longer than a token lives.
            self._access_token = self._fetch_access_token()

        # ExternalTaskSensor checks existence in the local metadata database; check the remote one.
        if self.check_existence and not self._has_checked_existence:
            self._check_remote_dag_and_tasks_exist()
            self._has_checked_existence = True

        if self.external_task_ids:
            # One request per date gives the states of all external tasks, mapped ones included.
            task_states_by_date = [self._get_remote_task_states(date) for date in dttm_filter]
            if self.failed_states:
                self._handle_failed_states(
                    self._count_dates_with_any_task_in_states(task_states_by_date, self.failed_states)
                )
            if self.skipped_states:
                self._handle_skipped_states(
                    self._count_dates_with_any_task_in_states(task_states_by_date, self.skipped_states)
                )
            completed_dates = self._count_dates_with_all_tasks_in_states(
                task_states_by_date, self.allowed_states
            )
        else:
            if self.failed_states:
                self._handle_failed_states(
                    self._count_remote_dag_runs_in_states(dttm_filter, self.failed_states)
                )
            if self.skipped_states:
                self._handle_skipped_states(
                    self._count_remote_dag_runs_in_states(dttm_filter, self.skipped_states)
                )
            completed_dates = self._count_remote_dag_runs_in_states(dttm_filter, self.allowed_states)
        self.log.info(
            "Remote %s in states %s: %s of %s dates",
            target,
            self.allowed_states,
            completed_dates,
            len(dttm_filter),
        )
        return completed_dates == len(dttm_filter)
