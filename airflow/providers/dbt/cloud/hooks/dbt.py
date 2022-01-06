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

import sys
import time
from enum import Enum
from functools import wraps
from inspect import signature
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Union

from requests import PreparedRequest, Session
from requests.auth import AuthBase

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.http.hooks.http import HttpHook
from airflow.typing_compat import TypedDict

if sys.version_info >= (3, 8):
    from functools import cached_property
else:
    from cached_property import cached_property


def fallback_to_default_account(func: Callable) -> Callable:
    """
    Decorator which provides a fallback value for ``account_id``. If the ``account_id`` is None or not passed
    to a function at all, the value will be taken from the configured dbt Cloud Airflow Connection.

    :param func: The function to decorate and apply fallback.
    :return: Result of the function call.
    """
    sig = signature(func)

    @wraps(func)
    def wrapper(*args, **kwargs) -> Callable:
        bound_args = sig.bind(*args, **kwargs)

        # Check if ``account_id`` was not included in the function signature or, if it is, the value is not
        # provided.
        if "account_id" not in bound_args.arguments or bound_args.arguments["account_id"] is None:
            self = args[0]
            default_account_id = self.conn.login
            if not default_account_id:
                raise AirflowException("Could not determine the dbt Cloud account.")

            bound_args.arguments["account_id"] = default_account_id

        return func(*bound_args.args, **bound_args.kwargs)

    return wrapper


class TokenAuth(AuthBase):
    def __init__(self, token: str) -> None:
        self.token = token

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        request.headers["Authorization"] = f"Token {self.token}"

        return request


class JobRunInfo(TypedDict):
    """Type class for the job_run_info dictionary."""

    account_id: int
    run_id: int


class RequestInfo(TypedDict):

    endpoint: str
    data: Mapping[str, Any]


class DbtCloudJobRunStatus(Enum):
    QUEUED = 1
    STARTING = 2
    RUNNING = 3
    SUCCESS = 10
    ERROR = 20
    CANCELLED = 30
    TERMINAL_STATUSES = (SUCCESS, ERROR, CANCELLED)

    @classmethod
    def is_valid(cls, statuses: Union[int, Sequence[int]]) -> bool:
        if isinstance(statuses, int):
            return bool(cls(statuses))
        else:
            return all(cls(status) for status in statuses)

    @classmethod
    def is_terminal(cls, status: int) -> bool:
        return status in cls.TERMINAL_STATUSES.value


class DbtCloudJobRunException(AirflowException):
    """An exception that indicates a job run failed to complete."""


class DbtCloudHook(HttpHook):
    conn_name_attr = "dbt_cloud_conn_id"
    default_conn_name = "dbt_cloud_default"
    conn_type = "dbt_cloud"
    hook_name = "dbt Cloud"

    @staticmethod
    def get_ui_field_behaviour() -> Dict[str, Any]:
        return {
            "hidden_fields": ["host", "port", "schema", "extra"],
            "relabeling": {"login": "Account ID", "password": "Access Token"},
        }

    def __init__(self, dbt_cloud_conn_id: str = default_conn_name, *args, **kwargs) -> None:
        super().__init__(auth_type=TokenAuth, method="GET")
        self.dbt_cloud_conn_id = dbt_cloud_conn_id
        self.base_url = "https://cloud.getdbt.com/api/v2/accounts/"

    @cached_property
    def conn(self) -> Connection:
        _conn = self.get_connection(self.dbt_cloud_conn_id)
        if not _conn.password:
            raise AirflowException("Access Token is required to connect to dbt Cloud.")

        return _conn

    def get_conn(self, *args, **kwargs) -> Session:
        session = Session()
        session.auth = self.auth_type(self.conn.password)

        return session

    def _run_and_get_response(self, request_info: Optional[RequestInfo] = None, method: str = "GET") -> Any:
        self.method = method

        if request_info:
            return self.run(**request_info)
        else:
            return self.run()

    def list_accounts(self) -> Dict[str, Any]:
        return self.run()

    @fallback_to_default_account
    def get_account(self, account_id: Optional[int] = None) -> Dict[str, Any]:
        return self._run_and_get_response(request_info=RequestInfo(endpoint=f"{account_id}/"))

    @fallback_to_default_account
    def list_projects(self, account_id: Optional[int] = None) -> Dict[str, Any]:
        return self._run_and_get_response(request_info=RequestInfo(endpoint=f"{account_id}/projects/"))

    @fallback_to_default_account
    def get_project(self, project_id: int, account_id: Optional[int] = None) -> Dict[str, Any]:
        return self._run_and_get_response(
            request_info=RequestInfo(endpoint=f"{account_id}/projects/{project_id}/")
        )

    @fallback_to_default_account
    def list_jobs(self, account_id: Optional[int] = None) -> Dict[str, Any]:
        return self._run_and_get_response(request_info=RequestInfo(endpoint=f"{account_id}/jobs/"))

    @fallback_to_default_account
    def get_job(self, job_id: int, account_id: Optional[int] = None) -> Dict[str, Any]:
        return self._run_and_get_response(request_info=RequestInfo(endpoint=f"{account_id}/jobs/{job_id}"))

    @fallback_to_default_account
    def trigger_job_run(self, job_id: int, cause: str, account_id: Optional[int] = None) -> int:
        return self._run_and_get_response(
            request_info=RequestInfo(endpoint=f"{account_id}/jobs/{job_id}/run/", data={"cause": cause}),
            method="POST",
        )

    @fallback_to_default_account
    def list_job_runs(
        self,
        account_id: Optional[int] = None,
        include_related: List[str] = ["trigger", "job", "repository", "environment"],
        job_definition_id: Optional[int] = None,
        order_by: Optional[str] = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        return self._run_and_get_response(
            request_info=RequestInfo(
                endpoint=f"{account_id}/runs/",
                data={
                    "include_related": include_related,
                    "job_definition_id": job_definition_id,
                    "order_by": order_by,
                    "offset": offset,
                    "limit": limit,
                },
            ),
        )

    @fallback_to_default_account
    def get_job_run(
        self, run_id: int, account_id: Optional[int] = None, include_related: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        return self._run_and_get_response(
            request_info=RequestInfo(
                endpoint=f"{account_id}/runs/{run_id}/",
                data={"include_related": include_related},
            )
        )

    def get_job_run_status(
        self, run_id: int, account_id: Optional[int] = None, include_related: Optional[List[str]] = None
    ) -> int:
        self.log.info(f"Getting the status of job run {run_id}.")

        job_run = self.get_job_run(account_id=account_id, run_id=run_id, include_related=include_related)
        job_run_status = job_run.json()["data"]["status"]

        self.log.info(f"Current status of job run {run_id}: {DbtCloudJobRunStatus(job_run_status).name}")

        return job_run_status

    def wait_for_job_run_status(
        self,
        run_id: int,
        account_id: Optional[int] = None,
        expected_statuses: Union[int, Sequence[int]] = DbtCloudJobRunStatus.SUCCESS.value,
        check_interval: int = 60,
        timeout: int = 60 * 60 * 24 * 7,
    ) -> bool:
        expected_statuses = (expected_statuses,) if isinstance(expected_statuses, int) else expected_statuses

        DbtCloudJobRunStatus.is_valid(expected_statuses)

        job_run_info = JobRunInfo(account_id=account_id, run_id=run_id)
        job_run_status = self.get_job_run_status(**job_run_info)

        start_time = time.monotonic()

        while (
            not DbtCloudJobRunStatus.is_terminal(job_run_status) and job_run_status not in expected_statuses
        ):
            # Check if the job-run duration has exceeded the ``timeout`` configured.
            if start_time + timeout < time.monotonic():
                raise AirflowException(
                    f"Job run {run_id} has not reached a terminal status after {timeout} seconds."
                )

            # Wait to check the status of the job run based on the ``check_interval`` configured.
            time.sleep(check_interval)

            job_run_status = self.get_job_run_status(**job_run_info)

        return job_run_status in expected_statuses

    @fallback_to_default_account
    def list_job_run_artifacts(
        self, run_id: int, account_id: Optional[int] = None, step: Optional[int] = None
    ) -> Dict[str, Any]:
        return self._run_and_get_response(
            request_info=RequestInfo(endpoint=f"{account_id}/runs/{run_id}/artifacts/", data={"step": step})
        )

    @fallback_to_default_account
    def get_job_run_artifact(
        self, run_id: int, path: str, account_id: Optional[int] = None, step: Optional[int] = None
    ) -> Any:  # need to update based on allowed artifacts to download
        return self._run_and_get_response(
            request_info=RequestInfo(
                endpoint=f"{account_id}/runs/{run_id}/artifacts/{path}", data={"step": step}
            )
        )

    @fallback_to_default_account
    def cancel_job_run(self, run_id: int, account_id: Optional[int] = None) -> Dict[str, Any]:
        return self._run_and_get_response(
            request_info=RequestInfo(endpoint=f"{account_id}/runs/{run_id}/cancel/"),
            method="POST",
        )
