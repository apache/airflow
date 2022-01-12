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
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple, Union

from requests import PreparedRequest, Session
from requests.auth import AuthBase
from requests.models import Response

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
    to the decorated function, the value will be taken from the configured dbt Cloud Airflow Connection.
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

            bound_args.arguments["account_id"] = int(default_account_id)

        return func(*bound_args.args, **bound_args.kwargs)

    return wrapper


class TokenAuth(AuthBase):
    """Helper class for Auth when executing requests."""

    def __init__(self, token: str) -> None:
        self.token = token

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        request.headers["Authorization"] = f"Token {self.token}"

        return request


class JobRunInfo(TypedDict):
    """Type class for the ``job_run_info`` dictionary."""

    account_id: int
    run_id: int


class DbtCloudJobRunStatus(Enum):
    """dbt Cloud Job statuses."""

    QUEUED = 1
    STARTING = 2
    RUNNING = 3
    SUCCESS = 10
    ERROR = 20
    CANCELLED = 30
    TERMINAL_STATUSES = (SUCCESS, ERROR, CANCELLED)

    @classmethod
    def is_valid(cls, statuses: Union[int, Sequence[int]]) -> bool:
        """Validates input statuses are a known value."""
        if isinstance(statuses, int):
            return bool(cls(statuses))
        else:
            return all(cls(status) for status in statuses)

    @classmethod
    def is_terminal(cls, status: int) -> bool:
        """Checks if the input status is that of a terminal type."""
        return status in cls.TERMINAL_STATUSES.value


class DbtCloudJobRunException(AirflowException):
    """An exception that indicates a job run failed to complete."""


class DbtCloudHook(HttpHook):
    """
    Interact with dbt Cloud using the V2 API.

    :param dbt_cloud_conn_id: The ID of the :ref:`dbt Cloud connection <howto/connection:dbt-cloud>`.
    :type dbt_cloud_conn_id: str
    """

    conn_name_attr = "dbt_cloud_conn_id"
    default_conn_name = "dbt_cloud_default"
    conn_type = "dbt_cloud"
    hook_name = "dbt Cloud"

    @staticmethod
    def get_ui_field_behaviour() -> Dict[str, Any]:
        """Builds custom field behavior for the dbt Cloud connection form in the Airflow UI."""
        return {
            "hidden_fields": ["host", "port", "schema", "extra"],
            "relabeling": {"login": "Account ID", "password": "Access Token"},
        }

    def __init__(self, dbt_cloud_conn_id: str = default_conn_name, *args, **kwargs) -> None:
        super().__init__(auth_type=TokenAuth)
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

    def _paginate(self, endpoint: str, data: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        results = []
        response = self.run(endpoint=endpoint, data=data)
        resp_json = response.json()
        limit = resp_json["extra"]["filters"]["limit"]
        num_total_results = resp_json["extra"]["pagination"]["total_count"]
        num_current_results = resp_json["extra"]["pagination"]["count"]
        results.append(resp_json)

        if not num_current_results == num_total_results:
            _paginate_data = data.copy()
            _paginate_data["offset"] = limit

            while True:
                if num_current_results < num_total_results:
                    response = self.run(endpoint=endpoint, data=_paginate_data)
                    resp_json = response.json()
                    if resp_json["data"]:
                        results.append(resp_json)
                        num_current_results += resp_json["extra"]["pagination"]["count"]
                        _paginate_data["offset"] += limit
                else:
                    break

        return results

    def _run_and_get_response(
        self,
        method: str = "GET",
        endpoint: Optional[str] = None,
        data: Optional[Dict[str, Any]] = None,
        paginate: bool = False,
    ) -> Any:
        self.method = method

        if paginate:
            return self._paginate(endpoint=endpoint, data=data)

        return self.run(endpoint=endpoint, data=data)

    def list_accounts(self) -> List[Dict[str, Any]]:
        """
        Retrieves all of the dbt Cloud the configured API token is authorized to access.

        :return: The request response.
        """
        return self._run_and_get_response(paginate=True)

    @fallback_to_default_account
    def get_account(self, account_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Retrieves metadata for a specific dbt Cloud account.

        :param account_id: Optional. The ID of a dbt Cloud account.
        :type account_id: int
        :return: The request response.
        """
        return self._run_and_get_response(endpoint=f"{account_id}/")

    @fallback_to_default_account
    def list_projects(self, account_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Retrieves metadata for all projects tied to a specified dbt Cloud account.

        :param account_id: Optional. The ID of a dbt Cloud account.
        :type account_id: int
        :return: The request response.
        """
        return self._run_and_get_response(endpoint=f"{account_id}/projects/", paginate=True)

    @fallback_to_default_account
    def get_project(self, project_id: int, account_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Retrieves metadata for a specific project.

        :param project_id: The ID of a dbt Cloud project.
        :type project_id: int
        :param account_id: Optional. The ID of a dbt Cloud account.
        :type account_id: int
        :return: The request response.
        """
        return self._run_and_get_response(endpoint=f"{account_id}/projects/{project_id}/")

    @fallback_to_default_account
    def list_jobs(
        self,
        account_id: Optional[int] = None,
        order_by: Optional[str] = None,
        project_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Retrieves metadata for all jobs tied to a specified dbt Cloud account. If a ``project_id`` is
        supplied, only jobs pertaining to this job will be retrieved.

        :param account_id: Optional. The ID of a dbt Cloud account.
        :type account_id: int
        :param order_by: Optional. Field to order the result by. Use '-' to indicate reverse order.
            For example, to use reverse order by the run ID use ``order_by=-id``.
        :type order_by: str
        :param project_id: The ID of a dbt Cloud project.
        :type project_id: int
        :return: The request response.
        """
        return self._run_and_get_response(
            endpoint=f"{account_id}/jobs/",
            data={"order_by": order_by, "project_id": project_id},
            paginate=True,
        )

    @fallback_to_default_account
    def get_job(self, job_id: int, account_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Retrieves metadata for a specific job.

        :param job_id: The ID of a dbt Cloud job.
        :type job_id: int
        :param account_id: Optional. The ID of a dbt Cloud account.
        :type account_id: int
        :return: The request response.
        """
        return self._run_and_get_response(endpoint=f"{account_id}/jobs/{job_id}")

    @fallback_to_default_account
    def create_job(
        self,
        project_id: int,
        environment_id: int,
        name: str,
        execute_steps: List[str],
        account_id: Optional[int] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Create a dbt Cloud job for a given project.

        :param job_id: The ID of a dbt Cloud job.
        :type job_id: int
        :param environment_id: The ID of a dbt environment.
        :type environment_id: int
        :param name: The name for the to-be-created dbt job.
        :type name: str
        :param execute_steps: List of dbt commands to execute when triggering the job.
        :type execute_steps: List[str]
        :param account_id: Optional. The ID of a dbt Cloud account.
        :type account_id: int
        :return: The request response.
        """
        return self._run_and_get_response(
            method="POST",
            endpoint=f"{account_id}/jobs/",
            data={
                "account_id": account_id,
                "project_id": project_id,
                "environment_id": environment_id,
                "name": name,
                "execute_steps": execute_steps,
                **kwargs,
            },
        )

    @fallback_to_default_account
    def update_job(
        self,
        job_id: int,
        project_id: int,
        environment_id: int,
        name: str,
        execute_steps: List[str],
        account_id: Optional[int] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Update the definition of an existing dbt Cloud job.

        :param job_id: The ID of a dbt Cloud job.
        :type job_id: int
        :param environment_id: The ID of a dbt environment.
        :type environment_id: int
        :param name: The name for the to-be-created dbt job.
        :type name: str
        :param execute_steps: List of dbt commands to execute when triggering the job.
        :type execute_steps: List[str]
        :param account_id: Optional. The ID of a dbt Cloud account.
        :type account_id: int
        :return: The request response.
        """
        return self._run_and_get_response(
            method="POST",
            endpoint=f"{account_id}/jobs/{job_id}/",
            data={
                "account_id": account_id,
                "project_id": project_id,
                "environment_id": environment_id,
                "name": name,
                "execute_steps": execute_steps,
                **kwargs,
            },
        )

    @fallback_to_default_account
    def trigger_job_run(
        self,
        job_id: int,
        cause: str,
        account_id: Optional[int] = None,
        steps_override: Optional[List[str]] = None,
        schema_override: Optional[str] = None,
        additional_run_config: Dict[str, Any] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Triggers a run of a dbt Cloud job.

        :param job_id: The ID of a dbt Cloud job.
        :type job_id: int
        :param cause: Description of the reason to trigger the job.
        :type cause: str
        :param account_id: Optional. The ID of a dbt Cloud account.
        :type account_id: int
        :param steps_override: Optional. List of dbt commands to execute when triggering the job
            instead of those configured in dbt Cloud.
        :type steps_override: List[str]
        :param schema_override: Optional. Override the destination schema in the configured target for this
            job.
        :type schema_override: str
        :param additional_run_config: Optional. Any additional parameters that should be included in the API
            request when triggering the job.
        :type additional_run_config: Dict[str, Any]
        :return: The request response.
        """
        if not additional_run_config:
            additional_run_config = {}

        return self._run_and_get_response(
            method="POST",
            endpoint=f"{account_id}/jobs/{job_id}/run/",
            data={
                "cause": cause,
                "steps_override": steps_override,
                "schema_override": schema_override,
                **additional_run_config,
            },
        )

    @fallback_to_default_account
    def list_job_runs(
        self,
        account_id: Optional[int] = None,
        include_related: List[str] = ["trigger", "job", "repository", "environment"],
        job_definition_id: Optional[int] = None,
        order_by: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Retrieves metadata for all of the dbt Cloud job runs for an account. If a ``job_definition_id`` is
        supplied, only metadata for runs of that specific job are pulled.

        :param account_id: Optional. The ID of a dbt Cloud account.
        :type account_id: int
        :param include_related: Optional. List of related fields to pull with the run.
            Valid values are "trigger", "job", "repository", and "environment".
        :type include_related: List[str]
        :param job_definition_id: Optional. The dbt Cloud job ID to retrieve run metadata.
        :type job_definition_id: int
        :param order_by: Optional. Field to order the result by. Use '-' to indicate reverse order.
            For example, to use reverse order by the run ID use ``order_by=-id``.
        :return: The request response.
        """
        return self._run_and_get_response(
            endpoint=f"{account_id}/runs/",
            data={
                "include_related": include_related,
                "job_definition_id": job_definition_id,
                "order_by": order_by,
            },
            paginate=True,
        )

    @fallback_to_default_account
    def get_job_run(
        self, run_id: int, account_id: Optional[int] = None, include_related: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Retrieves metadata for a specific run of a dbt Cloud job.

        :param run_id: The ID of a dbt Cloud job run.
        :type run_id: int
        :param account_id: Optional. The ID of a dbt Cloud account.
        :type account_id: int
        :param include_related: Optional. List of related fields to pull with the run.
            Valid values are "trigger", "job", "repository", and "environment".
        :type include_related: List[str]
        :return: The request response.
        """
        return self._run_and_get_response(
            endpoint=f"{account_id}/runs/{run_id}/",
            data={"include_related": include_related},
        )

    def get_job_run_status(
        self, run_id: int, account_id: Optional[int] = None, include_related: Optional[List[str]] = None
    ) -> int:
        """
        Retrieves the status for a specific run of a dbt Cloud job.

        :param run_id: The ID of a dbt Cloud job run.
        :type run_id: int
        :param account_id: Optional. The ID of a dbt Cloud account.
        :type account_id: int
        :param include_related: Optional. List of related fields to pull with the run.
            Valid values are "trigger", "job", "repository", and "environment".
        :type include_related: List[str]
        :return: The status of a dbt Cloud job run.
        """
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
        """
        Waits for a dbt Cloud job run to match an expected status.

        :param run_id: The ID of a dbt Cloud job run.
        :type run_id: int
        :param account_id: Optional. The ID of a dbt Cloud account.
        :type account_id: int
        :param expected_statuses: Optional. The desired status(es) to check against a job run's current
            status. Defaults to the success status value.
        :param check_interval: Time in seconds to check on a pipeline run's status.
        :type check_interval: int
        :param timeout: Time in seconds to wait for a pipeline to reach a terminal status or the expected
            status.
        :param timeout: int
        :return: Boolean indicating if the job run has reached the ``expected_status``.
        """
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
    def cancel_job_run(self, run_id: int, account_id: Optional[int] = None) -> None:
        """
        Cancel a specific dbt Cloud job run.

        :param run_id: The ID of a dbt Cloud job run.
        :type run_id: int
        :param account_id: Optional. The ID of a dbt Cloud account.
        :type account_id: int
        """
        self._run_and_get_response(method="POST", endpoint=f"{account_id}/runs/{run_id}/cancel/")

    @fallback_to_default_account
    def list_job_run_artifacts(
        self, run_id: int, account_id: Optional[int] = None, step: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieves a list of the available artifact files generated for a completed run of a dbt Cloud job. By
        default, this returns artifacts from the last step in the run. To list artifacts from other steps in
        the run, use the ``step`` parameter.

        :param run_id: The ID of a dbt Cloud job run.
        :type run_id: int
        :param account_id: Optional. The ID of a dbt Cloud account.
        :type account_id: int
        :param step: Optional. The index of the Step in the Run to query for artifacts. The first step in the
            run has the index 1. If the step parameter is omitted, artifacts for the last step in the run will
            be returned.
        :type step: int
        :return: List of artifact file names.
        """
        return self._run_and_get_response(
            endpoint=f"{account_id}/runs/{run_id}/artifacts/", data={"step": step}
        )

    @fallback_to_default_account
    def get_job_run_artifact(
        self, run_id: int, path: str, account_id: Optional[int] = None, step: Optional[int] = None
    ) -> Any:
        """
        Retrieves a list of the available artifact files generated for a completed run of a dbt Cloud job. By
        default, this returns artifacts from the last step in the run. To list artifacts from other steps in
        the run, use the ``step`` parameter.

        :param run_id: The ID of a dbt Cloud job run.
        :type run_id: int
        :param path: The file path related to the artifact file. Paths are rooted at the target/ directory.
            Use "manifest.json", "catalog.json", or "run_results.json" to download dbt-generated artifacts
            for the run.
        :type path: str
        :param account_id: Optional. The ID of a dbt Cloud account.
        :type account_id: int
        :param step: Optional. The index of the Step in the Run to query for artifacts. The first step in the
            run has the index 1. If the step parameter is omitted, artifacts for the last step in the run will
            be returned.
        :type step: int
        :return: List of artifact file names.
        """
        return self._run_and_get_response(
            endpoint=f"{account_id}/runs/{run_id}/artifacts/{path}", data={"step": step}
        )

    def test_connection(self) -> Tuple[bool, str]:
        """Test dbt Cloud connection."""
        try:
            self._run_and_get_response()
            return True, "Successfully connected to dbt Cloud."
        except Exception as e:
            return False, str(e)
