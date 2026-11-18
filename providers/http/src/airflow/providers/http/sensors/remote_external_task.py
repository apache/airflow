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
        # Retrying will not help: a wrong URL, filter or credentials give the same error next time.
        if response.status_code >= 400:
            raise AirflowFailException(
                f"{response.request.method} {response.url} returned {response.status_code}: "
                f"{response.text[:500]}"
            )

    # --- asking the remote Airflow -------------------------------------------------------------------

    def _count_remote_records(self, path: str, filters: dict[str, Any]) -> int:
        """Return how many DAG runs or task instances the remote list endpoint finds for the filters."""
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

    def _count_remote_in_states(self, dttm_filter: Sequence[datetime.datetime], states: list[str]) -> int:
        """
        Count remote task instances, or remote DAG runs if no task ids are given, in these states.

        Only DAG runs or task instances on exactly the dates of ``dttm_filter`` are counted.
        """
        total = 0
        for date in dttm_filter:
            if self.external_task_ids:
                total += self._count_remote_records(
                    "dags/~/dagRuns/~/taskInstances/list",
                    {
                        "dag_ids": [self.external_dag_id],
                        "task_ids": list(self.external_task_ids),
                        "state": states,
                        **self._build_exact_date_filter(date),
                    },
                )
            else:
                total += self._count_remote_records(
                    "dags/~/dagRuns/list",
                    {
                        "dag_ids": [self.external_dag_id],
                        "states": states,
                        **self._build_exact_date_filter(date),
                    },
                )
        return total

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

    def poke(self, context: Context) -> bool:
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

        if self.failed_states:
            count = self._count_remote_in_states(dttm_filter, self.failed_states)
            self._handle_failed_states(self._calculate_count(count, dttm_filter))

        if self.skipped_states:
            count = self._count_remote_in_states(dttm_filter, self.skipped_states)
            self._handle_skipped_states(self._calculate_count(count, dttm_filter))

        count = self._count_remote_in_states(dttm_filter, self.allowed_states)
        completed_dates = self._calculate_count(count, dttm_filter)
        self.log.info(
            "Remote %s in states %s: %s of %s dates",
            target,
            self.allowed_states,
            completed_dates,
            len(dttm_filter),
        )
        return completed_dates == len(dttm_filter)
