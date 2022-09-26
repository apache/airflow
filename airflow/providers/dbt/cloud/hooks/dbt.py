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

import json
import time
from enum import Enum
from functools import wraps
from inspect import signature
from typing import Any, Callable, Sequence, Set

from requests import PreparedRequest, Session
from requests.auth import AuthBase
from requests.models import Response

from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.http.hooks.http import HttpHook
from airflow.typing_compat import TypedDict


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
        if bound_args.arguments.get("account_id") is None:
            self = args[0]
            default_account_id = self.connection.login
            if not default_account_id:
                raise AirflowException("Could not determine the dbt Cloud account.")

            bound_args.arguments["account_id"] = int(default_account_id)

        return func(*bound_args.args, **bound_args.kwargs)

    return wrapper


def _get_provider_info() -> tuple[str, str]:
    from airflow.providers_manager import ProvidersManager

    manager = ProvidersManager()
    package_name = manager.hooks[DbtCloudHook.conn_type].package_name  # type: ignore[union-attr]
    provider = manager.providers[package_name]

    return package_name, provider.version


class TokenAuth(AuthBase):
    """Helper class for Auth when executing requests."""

    def __init__(self, token: str) -> None:
        self.token = token

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        package_name, provider_version = _get_provider_info()
        request.headers["User-Agent"] = f"{package_name}-v{provider_version}"
        request.headers["Content-Type"] = "application/json"
        request.headers["Authorization"] = f"Token {self.token}"

        return request


class JobRunInfo(TypedDict):
    """Type class for the ``job_run_info`` dictionary."""

    account_id: int | None
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
    def check_is_valid(cls, statuses: int | Sequence[int] | set[int]):
        """Validates input statuses are a known value."""
        if isinstance(statuses, (Sequence, Set)):
            for status in statuses:
                cls(status)
        else:
            cls(statuses)

    @classmethod
    def is_terminal(cls, status: int) -> bool:
        """Checks if the input status is that of a terminal type."""
        cls.check_is_valid(statuses=status)

        return status in cls.TERMINAL_STATUSES.value


class DbtCloudJobRunException(AirflowException):
    """An exception that indicates a job run failed to complete."""


class DbtCloudHook(HttpHook):
    """
    Interact with dbt Cloud using the V2 API.

    :param dbt_cloud_conn_id: The ID of the :ref:`dbt Cloud connection <howto/connection:dbt-cloud>`.
    """

    conn_name_attr = "dbt_cloud_conn_id"
    default_conn_name = "dbt_cloud_default"
    conn_type = "dbt_cloud"
    hook_name = "dbt Cloud"

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Builds custom field behavior for the dbt Cloud connection form in the Airflow UI."""
        return {
            "hidden_fields": ["host", "port", "extra"],
            "relabeling": {"login": "Account ID", "password": "API Token", "schema": "Tenant"},
            "placeholders": {"schema": "Defaults to 'cloud'."},
        }

    def __init__(self, dbt_cloud_conn_id: str = default_conn_name, *args, **kwargs) -> None:
        super().__init__(auth_type=TokenAuth)
        self.dbt_cloud_conn_id = dbt_cloud_conn_id
        tenant = self.connection.schema if self.connection.schema else 'cloud'

        self.base_url = f"https://{tenant}.getdbt.com/api/v2/accounts/"

    @cached_property
    def connection(self) -> Connection:
        _connection = self.get_connection(self.dbt_cloud_conn_id)
        if not _connection.password:
            raise AirflowException("An API token is required to connect to dbt Cloud.")

        return _connection

    def get_conn(self, *args, **kwargs) -> Session:
        session = Session()
        session.auth = self.auth_type(self.connection.password)

        return session

    def _paginate(self, endpoint: str, payload: dict[str, Any] | None = None) -> list[Response]:
        response = self.run(endpoint=endpoint, data=payload)
        resp_json = response.json()
        limit = resp_json["extra"]["filters"]["limit"]
        num_total_results = resp_json["extra"]["pagination"]["total_count"]
        num_current_results = resp_json["extra"]["pagination"]["count"]
        results = [response]
        if num_current_results != num_total_results:
            _paginate_payload = payload.copy() if payload else {}
            _paginate_payload["offset"] = limit

            while not num_current_results >= num_total_results:
                response = self.run(endpoint=endpoint, data=_paginate_payload)
                resp_json = response.json()
                results.append(response)
                num_current_results += resp_json["extra"]["pagination"]["count"]
                _paginate_payload["offset"] += limit
        return results

    def _run_and_get_response(
        self,
        method: str = "GET",
        endpoint: str | None = None,
        payload: str | dict[str, Any] | None = None,
        paginate: bool = False,
    ) -> Any:
        self.method = method

        if paginate:
            if isinstance(payload, str):
                raise ValueError("Payload cannot be a string to paginate a response.")

            if endpoint:
                return self._paginate(endpoint=endpoint, payload=payload)
            else:
                raise ValueError("An endpoint is needed to paginate a response.")

        return self.run(endpoint=endpoint, data=payload)

    def list_accounts(self) -> list[Response]:
        """
        Retrieves all of the dbt Cloud accounts the configured API token is authorized to access.

        :return: List of request responses.
        """
        return self._run_and_get_response()

    @fallback_to_default_account
    def get_account(self, account_id: int | None = None) -> Response:
        """
        Retrieves metadata for a specific dbt Cloud account.

        :param account_id: Optional. The ID of a dbt Cloud account.
        :return: The request response.
        """
        return self._run_and_get_response(endpoint=f"{account_id}/")

    @fallback_to_default_account
    def list_projects(self, account_id: int | None = None) -> list[Response]:
        """
        Retrieves metadata for all projects tied to a specified dbt Cloud account.

        :param account_id: Optional. The ID of a dbt Cloud account.
        :return: List of request responses.
        """
        return self._run_and_get_response(endpoint=f"{account_id}/projects/", paginate=True)

    @fallback_to_default_account
    def get_project(self, project_id: int, account_id: int | None = None) -> Response:
        """
        Retrieves metadata for a specific project.

        :param project_id: The ID of a dbt Cloud project.
        :param account_id: Optional. The ID of a dbt Cloud account.
        :return: The request response.
        """
        return self._run_and_get_response(endpoint=f"{account_id}/projects/{project_id}/")

    @fallback_to_default_account
    def list_jobs(
        self,
        account_id: int | None = None,
        order_by: str | None = None,
        project_id: int | None = None,
    ) -> list[Response]:
        """
        Retrieves metadata for all jobs tied to a specified dbt Cloud account. If a ``project_id`` is
        supplied, only jobs pertaining to this project will be retrieved.

        :param account_id: Optional. The ID of a dbt Cloud account.
        :param order_by: Optional. Field to order the result by. Use '-' to indicate reverse order.
            For example, to use reverse order by the run ID use ``order_by=-id``.
        :param project_id: The ID of a dbt Cloud project.
        :return: List of request responses.
        """
        return self._run_and_get_response(
            endpoint=f"{account_id}/jobs/",
            payload={"order_by": order_by, "project_id": project_id},
            paginate=True,
        )

    @fallback_to_default_account
    def get_job(self, job_id: int, account_id: int | None = None) -> Response:
        """
        Retrieves metadata for a specific job.

        :param job_id: The ID of a dbt Cloud job.
        :param account_id: Optional. The ID of a dbt Cloud account.
        :return: The request response.
        """
        return self._run_and_get_response(endpoint=f"{account_id}/jobs/{job_id}")

    @fallback_to_default_account
    def trigger_job_run(
        self,
        job_id: int,
        cause: str,
        account_id: int | None = None,
        steps_override: list[str] | None = None,
        schema_override: str | None = None,
        additional_run_config: dict[str, Any] | None = None,
    ) -> Response:
        """
        Triggers a run of a dbt Cloud job.

        :param job_id: The ID of a dbt Cloud job.
        :param cause: Description of the reason to trigger the job.
        :param account_id: Optional. The ID of a dbt Cloud account.
        :param steps_override: Optional. List of dbt commands to execute when triggering the job
            instead of those configured in dbt Cloud.
        :param schema_override: Optional. Override the destination schema in the configured target for this
            job.
        :param additional_run_config: Optional. Any additional parameters that should be included in the API
            request when triggering the job.
        :return: The request response.
        """
        if additional_run_config is None:
            additional_run_config = {}

        payload = {
            "cause": cause,
            "steps_override": steps_override,
            "schema_override": schema_override,
        }
        payload.update(additional_run_config)

        return self._run_and_get_response(
            method="POST",
            endpoint=f"{account_id}/jobs/{job_id}/run/",
            payload=json.dumps(payload),
        )

    @fallback_to_default_account
    def list_job_runs(
        self,
        account_id: int | None = None,
        include_related: list[str] | None = None,
        job_definition_id: int | None = None,
        order_by: str | None = None,
    ) -> list[Response]:
        """
        Retrieves metadata for all of the dbt Cloud job runs for an account. If a ``job_definition_id`` is
        supplied, only metadata for runs of that specific job are pulled.

        :param account_id: Optional. The ID of a dbt Cloud account.
        :param include_related: Optional. List of related fields to pull with the run.
            Valid values are "trigger", "job", "repository", and "environment".
        :param job_definition_id: Optional. The dbt Cloud job ID to retrieve run metadata.
        :param order_by: Optional. Field to order the result by. Use '-' to indicate reverse order.
            For example, to use reverse order by the run ID use ``order_by=-id``.
        :return: List of request responses.
        """
        return self._run_and_get_response(
            endpoint=f"{account_id}/runs/",
            payload={
                "include_related": include_related,
                "job_definition_id": job_definition_id,
                "order_by": order_by,
            },
            paginate=True,
        )

    @fallback_to_default_account
    def get_job_run(
        self, run_id: int, account_id: int | None = None, include_related: list[str] | None = None
    ) -> Response:
        """
        Retrieves metadata for a specific run of a dbt Cloud job.

        :param run_id: The ID of a dbt Cloud job run.
        :param account_id: Optional. The ID of a dbt Cloud account.
        :param include_related: Optional. List of related fields to pull with the run.
            Valid values are "trigger", "job", "repository", and "environment".
        :return: The request response.
        """
        return self._run_and_get_response(
            endpoint=f"{account_id}/runs/{run_id}/",
            payload={"include_related": include_related},
        )

    def get_job_run_status(self, run_id: int, account_id: int | None = None) -> int:
        """
        Retrieves the status for a specific run of a dbt Cloud job.

        :param run_id: The ID of a dbt Cloud job run.
        :param account_id: Optional. The ID of a dbt Cloud account.
        :return: The status of a dbt Cloud job run.
        """
        self.log.info("Getting the status of job run %s.", str(run_id))

        job_run = self.get_job_run(account_id=account_id, run_id=run_id)
        job_run_status = job_run.json()["data"]["status"]

        self.log.info(
            "Current status of job run %s: %s", str(run_id), DbtCloudJobRunStatus(job_run_status).name
        )

        return job_run_status

    def wait_for_job_run_status(
        self,
        run_id: int,
        account_id: int | None = None,
        expected_statuses: int | Sequence[int] | set[int] = DbtCloudJobRunStatus.SUCCESS.value,
        check_interval: int = 60,
        timeout: int = 60 * 60 * 24 * 7,
    ) -> bool:
        """
        Waits for a dbt Cloud job run to match an expected status.

        :param run_id: The ID of a dbt Cloud job run.
        :param account_id: Optional. The ID of a dbt Cloud account.
        :param expected_statuses: Optional. The desired status(es) to check against a job run's current
            status. Defaults to the success status value.
        :param check_interval: Time in seconds to check on a pipeline run's status.
        :param timeout: Time in seconds to wait for a pipeline to reach a terminal status or the expected
            status.
        :return: Boolean indicating if the job run has reached the ``expected_status``.
        """
        expected_statuses = (expected_statuses,) if isinstance(expected_statuses, int) else expected_statuses

        DbtCloudJobRunStatus.check_is_valid(expected_statuses)

        job_run_info = JobRunInfo(account_id=account_id, run_id=run_id)
        job_run_status = self.get_job_run_status(**job_run_info)

        start_time = time.monotonic()

        while (
            not DbtCloudJobRunStatus.is_terminal(job_run_status) and job_run_status not in expected_statuses
        ):
            # Check if the job-run duration has exceeded the ``timeout`` configured.
            if start_time + timeout < time.monotonic():
                raise DbtCloudJobRunException(
                    f"Job run {run_id} has not reached a terminal status after {timeout} seconds."
                )

            # Wait to check the status of the job run based on the ``check_interval`` configured.
            time.sleep(check_interval)

            job_run_status = self.get_job_run_status(**job_run_info)

        return job_run_status in expected_statuses

    @fallback_to_default_account
    def cancel_job_run(self, run_id: int, account_id: int | None = None) -> None:
        """
        Cancel a specific dbt Cloud job run.

        :param run_id: The ID of a dbt Cloud job run.
        :param account_id: Optional. The ID of a dbt Cloud account.
        """
        self._run_and_get_response(method="POST", endpoint=f"{account_id}/runs/{run_id}/cancel/")

    @fallback_to_default_account
    def list_job_run_artifacts(
        self, run_id: int, account_id: int | None = None, step: int | None = None
    ) -> list[Response]:
        """
        Retrieves a list of the available artifact files generated for a completed run of a dbt Cloud job. By
        default, this returns artifacts from the last step in the run. To list artifacts from other steps in
        the run, use the ``step`` parameter.

        :param run_id: The ID of a dbt Cloud job run.
        :param account_id: Optional. The ID of a dbt Cloud account.
        :param step: Optional. The index of the Step in the Run to query for artifacts. The first step in the
            run has the index 1. If the step parameter is omitted, artifacts for the last step in the run will
            be returned.
        :return: List of request responses.
        """
        return self._run_and_get_response(
            endpoint=f"{account_id}/runs/{run_id}/artifacts/", payload={"step": step}
        )

    @fallback_to_default_account
    def get_job_run_artifact(
        self, run_id: int, path: str, account_id: int | None = None, step: int | None = None
    ) -> Response:
        """
        Retrieves a list of the available artifact files generated for a completed run of a dbt Cloud job. By
        default, this returns artifacts from the last step in the run. To list artifacts from other steps in
        the run, use the ``step`` parameter.

        :param run_id: The ID of a dbt Cloud job run.
        :param path: The file path related to the artifact file. Paths are rooted at the target/ directory.
            Use "manifest.json", "catalog.json", or "run_results.json" to download dbt-generated artifacts
            for the run.
        :param account_id: Optional. The ID of a dbt Cloud account.
        :param step: Optional. The index of the Step in the Run to query for artifacts. The first step in the
            run has the index 1. If the step parameter is omitted, artifacts for the last step in the run will
            be returned.
        :return: The request response.
        """
        return self._run_and_get_response(
            endpoint=f"{account_id}/runs/{run_id}/artifacts/{path}", payload={"step": step}
        )

    def test_connection(self) -> tuple[bool, str]:
        """Test dbt Cloud connection."""
        try:
            self._run_and_get_response()
            return True, "Successfully connected to dbt Cloud."
        except Exception as e:
            return False, str(e)
