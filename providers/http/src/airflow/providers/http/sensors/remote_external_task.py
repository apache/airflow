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
import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from requests.adapters import HTTPAdapter
from requests.auth import AuthBase
from urllib3.util.retry import Retry

from airflow.providers.common.compat.sdk import (
    AirflowFailException,
    AirflowOptionalProviderFeatureException,
    BaseSensorOperator,
    timezone,
)
from airflow.providers.http.exceptions import RemoteAirflowApiUnavailableException
from airflow.providers.http.hooks.http import HttpHook

try:
    from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
except ImportError as e:
    raise AirflowOptionalProviderFeatureException(e)

# ExternalTaskSensor helpers this sensor reuses. They are private to the standard provider, so check them
# at import time: a rename there must fail loudly here instead of breaking the sensor at run time.
_REUSED_EXTERNAL_TASK_SENSOR_HELPERS = ("_get_dttm_filter", "_handle_failed_states", "_handle_skipped_states")
_missing_helpers = [
    name for name in _REUSED_EXTERNAL_TASK_SENSOR_HELPERS if not hasattr(ExternalTaskSensor, name)
]
if _missing_helpers:
    raise AirflowOptionalProviderFeatureException(
        f"RemoteExternalTaskSensor needs ExternalTaskSensor.{', '.join(_missing_helpers)}, which the installed "
        "apache-airflow-providers-standard does not have. Install a compatible version with "
        "apache-airflow-providers-http[standard]."
    )

if TYPE_CHECKING:
    from requests import PreparedRequest, Response

    from airflow.sdk import Context

SUPPORTED_API_VERSIONS = ("v1", "v2")
# 429 Too Many Requests and 5xx can be temporary problems of the remote Airflow: let retries handle them.
RETRYABLE_STATUS_CODES = frozenset({429})
TASK_INSTANCES_PAGE_LIMIT = 100
# Used when the connection extra has no "timeout"; without one a stalled remote would hang the poke.
DEFAULT_REQUEST_TIMEOUT_SECONDS = 60


def _retrying_adapter() -> HTTPAdapter:
    """
    Retry a request a few times on connection errors, read timeouts, 429 and 5xx before giving up.

    A sensor polls across a network for hours, so one blip should not fail the try. ``allowed_methods=None``
    retries every method: the list endpoints of the v1 API are POSTs, which urllib3 does not retry by
    default, and all requests of this sensor are reads. ``Retry`` honours ``Retry-After`` on 429 and 503.
    """
    return HTTPAdapter(
        max_retries=Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=None,
            raise_on_status=False,
        )
    )


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

    Task groups (``external_task_group_id``) are not supported yet. Deferrable mode is not supported
    yet either: with ``deferrable=True`` the sensor warns and runs as a regular sensor.
    ``check_existence=True`` checks the DAG and tasks in the remote Airflow before the first poke.
    """

    template_fields = [*ExternalTaskSensor.template_fields, "http_conn_id"]
    # ExternalDagLink of the parent points to the UI of the Airflow the sensor runs in.
    operator_extra_links: list = []

    def __init__(
        self,
        *,
        http_conn_id: str,
        api_version: str = "v2",
        **kwargs: Any,
    ) -> None:
        if api_version not in SUPPORTED_API_VERSIONS:
            raise ValueError(
                f"api_version must be one of {SUPPORTED_API_VERSIONS}: "
                "'v1' for a remote Airflow 2, 'v2' for a remote Airflow 3."
            )
        super().__init__(**kwargs)
        if self.deferrable:
            # deferrable defaults to [operators] default_deferrable, so raising would break every
            # instance of this sensor where that option is on.
            warnings.warn(
                "RemoteExternalTaskSensor does not support deferrable mode yet; it runs as a regular "
                "sensor (poke or reschedule mode).",
                UserWarning,
                stacklevel=2,
            )
            self.deferrable = False
        if self.external_task_group_id:
            raise ValueError("RemoteExternalTaskSensor does not support external_task_group_id yet.")
        self.http_conn_id = http_conn_id
        self.api_version = api_version
        self._access_token: str | None = None
        self._request_timeout: float = DEFAULT_REQUEST_TIMEOUT_SECONDS

    # --- connecting to the remote Airflow ------------------------------------------------------------

    def _get_remote_api_hook(self, method: str, *, authenticated: bool = True) -> HttpHook:
        """HttpHook for the remote Airflow, authenticated for the configured API version, with retries."""
        if authenticated and self.api_version == "v2" and self._access_token:
            token = self._access_token
            return HttpHook(
                method=method,
                http_conn_id=self.http_conn_id,
                auth_type=lambda *_: _BearerTokenAuth(token),
                adapter=_retrying_adapter(),
            )
        # v1 with a login: HttpHook uses basic authentication. No login: no authentication.
        return HttpHook(method=method, http_conn_id=self.http_conn_id, adapter=_retrying_adapter())

    def _send(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        authenticated: bool = True,
        not_found_message: str | None = None,
    ) -> Response:
        """Send one request to the remote Airflow and fail the task on an error answer."""
        response = self._get_remote_api_hook(method, authenticated=authenticated).run(
            endpoint,
            data=params,
            json=json,
            extra_options={"check_response": False, "timeout": self._request_timeout},
        )
        if not_found_message and response.status_code == 404:
            raise AirflowFailException(f"{not_found_message}: {response.url}")
        self._raise_for_remote_error(response)
        return response

    def _fetch_access_token(self) -> str | None:
        """Exchange the connection login and password for an Airflow 3 API access token."""
        conn = HttpHook.get_connection(self.http_conn_id)
        if not conn.login:
            return None
        response = self._send(
            "POST",
            "auth/token",
            json={"username": conn.login, "password": conn.password or ""},
            authenticated=False,
        )
        return response.json()["access_token"]

    def _build_remote_api_endpoint(self, path: str) -> str:
        return f"api/{self.api_version}/{path.lstrip('/')}"

    def _build_remote_dag_endpoint(self, path: str = "") -> str:
        """Endpoint under the external DAG; its id is quoted, as DAG ids may contain URL characters."""
        dag_path = f"dags/{quote(str(self.external_dag_id), safe='')}"
        return self._build_remote_api_endpoint(f"{dag_path}/{path}" if path else dag_path)

    @staticmethod
    def _raise_for_remote_error(response: Response) -> None:
        if response.status_code < 400:
            return
        message = (
            f"{response.request.method} {response.url} returned {response.status_code}: {response.text[:500]}"
        )
        if response.status_code >= 500 or response.status_code in RETRYABLE_STATUS_CODES:
            # Still failing after the retries of the adapter: fail this try, keep the task retries.
            raise RemoteAirflowApiUnavailableException(message)
        # A wrong URL, filter or credentials give the same error on the next try: fail without retries.
        raise AirflowFailException(message)

    # --- asking the remote Airflow -------------------------------------------------------------------

    def _build_exact_date_filter(self, date: datetime.datetime) -> dict[str, str]:
        """Filter that matches exactly this date: the list endpoints only have from/to date filters."""
        date_field = "execution_date" if self.api_version == "v1" else "logical_date"
        # A naive date (e.g. from a user's execution_date_fn) is read in Airflow's default time zone,
        # as everywhere in Airflow, not in the time zone of the worker host.
        date_in_utc = timezone.coerce_datetime(date).astimezone(datetime.UTC).isoformat()
        return {f"{date_field}_gte": date_in_utc, f"{date_field}_lte": date_in_utc}

    def _count_remote_dag_runs_in_states(
        self, dttm_filter: Sequence[datetime.datetime], states: list[str]
    ) -> int:
        """
        Count remote DAG runs on exactly the dates of ``dttm_filter`` that are in these states.

        A DAG has at most one run per logical date (unique ``dag_id``, ``logical_date``), so the count is
        the number of dates whose run is in one of the states.
        """
        # GET under the DAG's own path, not POST dags/~/dagRuns/list: the batch endpoint authorizes "~" and
        # then drops the runs of DAGs the user cannot read, so missing access would look like "no run yet"
        # instead of a 403. Only the total is needed: total_entries is the full count even with limit=1.
        return sum(
            int(
                self._send(
                    "GET",
                    self._build_remote_dag_endpoint("dagRuns"),
                    params={**self._build_exact_date_filter(date), "state": states, "limit": 1},
                ).json()["total_entries"]
            )
            for date in dttm_filter
        )

    def _get_remote_task_states(self, date: datetime.datetime) -> dict[str, list[str | None]]:
        """
        Return the states of all remote task instances of ``external_task_ids`` on exactly this date.

        Keyed by task id. A mapped task has one task instance per map index, so it can have several states.
        """
        if self.api_version == "v1":
            # Airflow 2 has no task_id filter on GET .../taskInstances. Its batch endpoint answers 403 for a
            # DAG the user cannot read and writes no audit log, so it is used there.
            return self._collect_task_states(
                "POST",
                self._build_remote_api_endpoint("dags/~/dagRuns/~/taskInstances/list"),
                lambda offset: {
                    "json": {
                        "dag_ids": [self.external_dag_id],
                        "task_ids": list(self.external_task_ids or []),
                        **self._build_exact_date_filter(date),
                        "page_offset": offset,
                        "page_limit": TASK_INSTANCES_PAGE_LIMIT,
                    }
                },
            )
        # Airflow 3: GET under the DAG's own path, one call per task id. The batch endpoint
        # (POST dags/~/dagRuns/~/taskInstances/list) writes an audit log row on every poke and, authorizing
        # "~", returns nothing instead of a 403 for a DAG the user cannot read.
        states_by_task: dict[str, list[str | None]] = {}
        for task_id in self.external_task_ids or []:
            states_by_task.update(
                self._collect_task_states(
                    "GET",
                    self._build_remote_dag_endpoint("dagRuns/~/taskInstances"),
                    lambda offset, task_id=task_id: {
                        "params": {
                            "task_id": task_id,
                            **self._build_exact_date_filter(date),
                            "offset": offset,
                            "limit": TASK_INSTANCES_PAGE_LIMIT,
                        }
                    },
                )
            )
        return states_by_task

    def _collect_task_states(self, method: str, endpoint: str, page_request) -> dict[str, list[str | None]]:
        """Read every page of a task instances list and group the states by task id."""
        states_by_task: dict[str, list[str | None]] = {}
        offset = 0
        while True:
            body = self._send(method, endpoint, **page_request(offset)).json()
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
        self._send(
            "GET",
            self._build_remote_dag_endpoint(),
            not_found_message=f"DAG {self.external_dag_id} does not exist in the remote Airflow",
        )
        for task_id in self.external_task_ids or []:
            self._send(
                "GET",
                self._build_remote_dag_endpoint(f"tasks/{quote(str(task_id), safe='')}"),
                not_found_message=(
                    f"Task {task_id} does not exist in DAG {self.external_dag_id} in the remote Airflow"
                ),
            )

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

        extra = HttpHook.get_connection(self.http_conn_id).extra_dejson
        self._request_timeout = extra.get("timeout", DEFAULT_REQUEST_TIMEOUT_SECONDS)

        if self.api_version == "v2":
            # A fresh token on every poke: a sensor can wait longer than a token lives.
            self._access_token = self._fetch_access_token()

        # ExternalTaskSensor checks existence in the local metadata database; check the remote one.
        if self.check_existence and not self._has_checked_existence:
            self._check_remote_dag_and_tasks_exist()
            self._has_checked_existence = True

        if self.external_task_ids:
            # The states of all external tasks per date, mapped ones included.
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
