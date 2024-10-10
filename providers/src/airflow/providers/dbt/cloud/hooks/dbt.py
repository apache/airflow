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

import asyncio
import json
import time
import warnings
from collections.abc import Sequence
from enum import Enum
from functools import cached_property, wraps
from inspect import signature
from typing import TYPE_CHECKING, Any, Callable, TypeVar, cast

import aiohttp
from asgiref.sync import sync_to_async
from requests.auth import AuthBase
from requests.sessions import Session

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook
from airflow.typing_compat import TypedDict

if TYPE_CHECKING:
    from requests.models import PreparedRequest, Response

    from airflow.models import Connection

DBT_CAUSE_MAX_LENGTH = 255


def fallback_to_default_account(func: Callable) -> Callable:
    """
    Provide a fallback value for ``account_id``.

    If the ``account_id`` is None or not passed to the decorated function,
    the value will be taken from the configured dbt Cloud Airflow Connection.
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
    NON_TERMINAL_STATUSES = (QUEUED, STARTING, RUNNING)
    TERMINAL_STATUSES = (SUCCESS, ERROR, CANCELLED)

    @classmethod
    def check_is_valid(cls, statuses: int | Sequence[int] | set[int]):
        """Validate input statuses are a known value."""
        if isinstance(statuses, (Sequence, set)):
            for status in statuses:
                cls(status)
        else:
            cls(statuses)

    @classmethod
    def is_terminal(cls, status: int) -> bool:
        """Check if the input status is that of a terminal type."""
        cls.check_is_valid(statuses=status)

        return status in cls.TERMINAL_STATUSES.value


class DbtCloudJobRunException(AirflowException):
    """An exception that indicates a job run failed to complete."""


T = TypeVar("T", bound=Any)


def provide_account_id(func: T) -> T:
    """
    Provide a fallback value for ``account_id``.

    If the ``account_id`` is None or not passed to the decorated function,
    the value will be taken from the configured dbt Cloud Airflow Connection.
    """
    function_signature = signature(func)

    @wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        bound_args = function_signature.bind(*args, **kwargs)

        if bound_args.arguments.get("account_id") is None:
            self = args[0]
            if self.dbt_cloud_conn_id:
                connection = await sync_to_async(self.get_connection)(self.dbt_cloud_conn_id)
                default_account_id = connection.login
                if not default_account_id:
                    raise AirflowException("Could not determine the dbt Cloud account.")
                bound_args.arguments["account_id"] = int(default_account_id)

        return await func(*bound_args.args, **bound_args.kwargs)

    return cast(T, wrapper)


class DbtCloudHook(HttpHook):
    """
    Interact with dbt Cloud using the V2 (V3 if supported) API.

    :param dbt_cloud_conn_id: The ID of the :ref:`dbt Cloud connection <howto/connection:dbt-cloud>`.
    """

    conn_name_attr = "dbt_cloud_conn_id"
    default_conn_name = "dbt_cloud_default"
    conn_type = "dbt_cloud"
    hook_name = "dbt Cloud"

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Build custom field behavior for the dbt Cloud connection form in the Airflow UI."""
        return {
            "hidden_fields": ["schema", "port"],
            "relabeling": {"login": "Account ID", "password": "API Token", "host": "Tenant"},
            "placeholders": {
                "host": "Defaults to 'cloud.getdbt.com'.",
                "extra": "Optional JSON-formatted extra.",
            },
        }

    def __init__(self, dbt_cloud_conn_id: str = default_conn_name, *args, **kwargs) -> None:
        super().__init__(auth_type=TokenAuth)
        self.dbt_cloud_conn_id = dbt_cloud_conn_id

    @staticmethod
    def _get_tenant_domain(conn: Connection) -> str:
        return conn.host or "cloud.getdbt.com"

    @staticmethod
    def _get_proxies(conn: Connection) -> dict[str, str] | None:
        return conn.extra_dejson.get("proxies", None)

    @staticmethod
    def get_request_url_params(
        tenant: str, endpoint: str, include_related: list[str] | None = None, *, api_version: str = "v2"
    ) -> tuple[str, dict[str, Any]]:
        """
        Form URL from base url and endpoint url.

        :param tenant: The tenant domain name which is need to be replaced in base url.
        :param endpoint: Endpoint url to be requested.
        :param include_related: Optional. List of related fields to pull with the run.
            Valid values are "trigger", "job", "repository", and "environment".
        """
        data: dict[str, Any] = {}
        if include_related:
            data = {"include_related": include_related}
        url = f"https://{tenant}/api/{api_version}/accounts/{endpoint or ''}"
        return url, data

    async def get_headers_tenants_from_connection(self) -> tuple[dict[str, Any], str]:
        """Get Headers, tenants from the connection details."""
        headers: dict[str, Any] = {}
        tenant = self._get_tenant_domain(self.connection)
        package_name, provider_version = _get_provider_info()
        headers["User-Agent"] = f"{package_name}-v{provider_version}"
        headers["Content-Type"] = "application/json"
        headers["Authorization"] = f"Token {self.connection.password}"
        return headers, tenant

    @provide_account_id
    async def get_job_details(
        self, run_id: int, account_id: int | None = None, include_related: list[str] | None = None
    ) -> Any:
        """
        Use Http async call to retrieve metadata for a specific run of a dbt Cloud job.

        :param run_id: The ID of a dbt Cloud job run.
        :param account_id: Optional. The ID of a dbt Cloud account.
        :param include_related: Optional. List of related fields to pull with the run.
            Valid values are "trigger", "job", "repository", and "environment".
        """
        endpoint = f"{account_id}/runs/{run_id}/"
        headers, tenant = await self.get_headers_tenants_from_connection()
        url, params = self.get_request_url_params(tenant, endpoint, include_related)
        proxies = self._get_proxies(self.connection) or {}

        async with aiohttp.ClientSession(headers=headers) as session:
            proxy = proxies.get("https") if proxies and url.startswith("https") else proxies.get("http")
            extra_request_args = {}

            if proxy:
                extra_request_args["proxy"] = proxy

            async with session.get(url, params=params, **extra_request_args) as response:  # type: ignore[arg-type]
                response.raise_for_status()
                return await response.json()

    async def get_job_status(
        self, run_id: int, account_id: int | None = None, include_related: list[str] | None = None
    ) -> int:
        """
        Retrieve the status for a specific run of a dbt Cloud job.

        :param run_id: The ID of a dbt Cloud job run.
        :param account_id: Optional. The ID of a dbt Cloud account.
        :param include_related: Optional. List of related fields to pull with the run.
            Valid values are "trigger", "job", "repository", and "environment".
        """
        self.log.info("Getting the status of job run %s.", run_id)
        response = await self.get_job_details(run_id, account_id=account_id, include_related=include_related)
        job_run_status: int = response["data"]["status"]
        return job_run_status

    @cached_property
    def connection(self) -> Connection:
        _connection = self.get_connection(self.dbt_cloud_conn_id)
        if not _connection.password:
            raise AirflowException("An API token is required to connect to dbt Cloud.")

        return _connection

    def get_conn(self, *args, **kwargs) -> Session:
        tenant = self._get_tenant_domain(self.connection)
        self.base_url = f"https://{tenant}/"

        session = Session()
        session.auth = self.auth_type(self.connection.password)

        return session

    def _paginate(
        self, endpoint: str, payload: dict[str, Any] | None = None, proxies: dict[str, str] | None = None
    ) -> list[Response]:
        extra_options = {"proxies": proxies} if proxies is not None else None
        response = self.run(endpoint=endpoint, data=payload, extra_options=extra_options)
        resp_json = response.json()
        limit = resp_json["extra"]["filters"]["limit"]
        num_total_results = resp_json["extra"]["pagination"]["total_count"]
        num_current_results = resp_json["extra"]["pagination"]["count"]
        results = [response]
        if num_current_results != num_total_results:
            _paginate_payload = payload.copy() if payload else {}
            _paginate_payload["offset"] = limit

            while num_current_results < num_total_results:
                response = self.run(endpoint=endpoint, data=_paginate_payload, extra_options=extra_options)
                resp_json = response.json()
                results.append(response)
                num_current_results += resp_json["extra"]["pagination"]["count"]
                _paginate_payload["offset"] += limit
        return results

    def _run_and_get_response(
        self,
        *,
        method: str = "GET",
        endpoint: str | None = None,
        payload: str | dict[str, Any] | None = None,
        paginate: bool = False,
        api_version: str = "v2",
    ) -> Any:
        self.method = method
        full_endpoint = f"api/{api_version}/accounts/{endpoint}" if endpoint else None
        proxies = self._get_proxies(self.connection)
        extra_options = {"proxies": proxies} if proxies is not None else None

        if paginate:
            if isinstance(payload, str):
                raise ValueError("Payload cannot be a string to paginate a response.")

            if full_endpoint:
                return self._paginate(endpoint=full_endpoint, payload=payload, proxies=proxies)

            raise ValueError("An endpoint is needed to paginate a response.")

        # breakpoint()
        return self.run(endpoint=full_endpoint, data=payload, extra_options=extra_options)

    def list_accounts(self) -> list[Response]:
        """
        Retrieve all of the dbt Cloud accounts the configured API token is authorized to access.

        :return: List of request responses.
        """
        return self._run_and_get_response()

    @fallback_to_default_account
    def get_account(self, account_id: int | None = None) -> Response:
        """
        Retrieve metadata for a specific dbt Cloud account.

        :param account_id: Optional. The ID of a dbt Cloud account.
        :return: The request response.
        """
        return self._run_and_get_response(endpoint=f"{account_id}/")

    @fallback_to_default_account
    def list_projects(self, account_id: int | None = None) -> list[Response]:
        """
        Retrieve metadata for all projects tied to a specified dbt Cloud account.

        :param account_id: Optional. The ID of a dbt Cloud account.
        :return: List of request responses.
        """
        return self._run_and_get_response(endpoint=f"{account_id}/projects/", paginate=True, api_version="v3")

    @fallback_to_default_account
    def get_project(self, project_id: int, account_id: int | None = None) -> Response:
        """
        Retrieve metadata for a specific project.

        :param project_id: The ID of a dbt Cloud project.
        :param account_id: Optional. The ID of a dbt Cloud account.
        :return: The request response.
        """
        return self._run_and_get_response(endpoint=f"{account_id}/projects/{project_id}/", api_version="v3")

    @fallback_to_default_account
    def list_jobs(
        self,
        account_id: int | None = None,
        order_by: str | None = None,
        project_id: int | None = None,
    ) -> list[Response]:
        """
        Retrieve metadata for all jobs tied to a specified dbt Cloud account.

        If a ``project_id`` is supplied, only jobs pertaining to this project will be retrieved.

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
        Retrieve metadata for a specific job.

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
        retry_from_failure: bool = False,
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
        :param retry_from_failure: Optional. If set to True and the previous job run has failed, the job
            will be triggered using the "rerun" endpoint. This parameter cannot be used alongside
            steps_override, schema_override, or additional_run_config.
        :param additional_run_config: Optional. Any additional parameters that should be included in the API
            request when triggering the job.
        :return: The request response.
        """
        if additional_run_config is None:
            additional_run_config = {}

        if cause is not None and len(cause) > DBT_CAUSE_MAX_LENGTH:
            warnings.warn(
                f"Cause `{cause}` exceeds limit of {DBT_CAUSE_MAX_LENGTH}"
                f" characters and will be truncated.",
                UserWarning,
                stacklevel=2,
            )
            cause = cause[:DBT_CAUSE_MAX_LENGTH]

        payload = {
            "cause": cause,
            "steps_override": steps_override,
            "schema_override": schema_override,
        }
        payload.update(additional_run_config)

        if retry_from_failure:
            latest_run = self.get_job_runs(
                account_id=account_id,
                payload={
                    "job_definition_id": job_id,
                    "order_by": "-created_at",
                    "limit": 1,
                },
            ).json()["data"]
            if latest_run and latest_run[0]["status"] == DbtCloudJobRunStatus.ERROR.value:
                if steps_override is not None or schema_override is not None or additional_run_config != {}:
                    warnings.warn(
                        "steps_override, schema_override, or additional_run_config will be ignored when"
                        " retry_from_failure is True and previous job run has failed.",
                        UserWarning,
                        stacklevel=2,
                    )
                return self.retry_failed_job_run(job_id, account_id)
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
        Retrieve metadata for all dbt Cloud job runs for an account.

        If a ``job_definition_id`` is supplied, only metadata for runs of that specific job are pulled.

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
    def get_job_runs(self, account_id: int | None = None, payload: dict[str, Any] | None = None) -> Response:
        """
        Retrieve metadata for a specific run of a dbt Cloud job.

        :param account_id: Optional. The ID of a dbt Cloud account.
        :param paylod: Optional. Query Parameters
        :return: The request response.
        """
        return self._run_and_get_response(
            endpoint=f"{account_id}/runs/",
            payload=payload,
        )

    @fallback_to_default_account
    def get_job_run(
        self, run_id: int, account_id: int | None = None, include_related: list[str] | None = None
    ) -> Response:
        """
        Retrieve metadata for a specific run of a dbt Cloud job.

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
        Retrieve the status for a specific run of a dbt Cloud job.

        :param run_id: The ID of a dbt Cloud job run.
        :param account_id: Optional. The ID of a dbt Cloud account.
        :return: The status of a dbt Cloud job run.
        """
        self.log.info("Getting the status of job run %s.", run_id)

        job_run = self.get_job_run(account_id=account_id, run_id=run_id)
        job_run_status = job_run.json()["data"]["status"]

        self.log.info("Current status of job run %s: %s", run_id, DbtCloudJobRunStatus(job_run_status).name)

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
        Wait for a dbt Cloud job run to match an expected status.

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
        Retrieve a list of the available artifact files generated for a completed run of a dbt Cloud job.

        By default, this returns artifacts from the last step in the run. To
        list artifacts from other steps in the run, use the ``step`` parameter.

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
        Retrieve a list of the available artifact files generated for a completed run of a dbt Cloud job.

        By default, this returns artifacts from the last step in the run. To
        list artifacts from other steps in the run, use the ``step`` parameter.

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

    @fallback_to_default_account
    async def get_job_run_artifacts_concurrently(
        self,
        run_id: int,
        artifacts: list[str],
        account_id: int | None = None,
        step: int | None = None,
    ):
        """
        Retrieve a list of chosen artifact files generated for a step in completed run of a dbt Cloud job.

        By default, this returns artifacts from the last step in the run.
        This takes advantage of the asynchronous calls to speed up the retrieval.

        :param run_id: The ID of a dbt Cloud job run.
        :param step: The index of the Step in the Run to query for artifacts. The first step in the
            run has the index 1. If the step parameter is omitted, artifacts for the last step in the run will
            be returned.
        :param path: The file path related to the artifact file. Paths are rooted at the target/ directory.
            Use "manifest.json", "catalog.json", or "run_results.json" to download dbt-generated artifacts
            for the run.
        :param account_id: Optional. The ID of a dbt Cloud account.

        :return: The request response.
        """
        tasks = {
            artifact: sync_to_async(self.get_job_run_artifact)(
                run_id,
                path=artifact,
                account_id=account_id,
                step=step,
            )
            for artifact in artifacts
        }
        results = await asyncio.gather(*tasks.values())
        return {filename: result.json() for filename, result in zip(tasks.keys(), results)}

    @fallback_to_default_account
    def retry_failed_job_run(self, job_id: int, account_id: int | None = None) -> Response:
        """
        Retry a failed run for a job from the point of failure, if the run failed. Otherwise, trigger a new run.

        :param job_id: The ID of a dbt Cloud job.
        :param account_id: Optional. The ID of a dbt Cloud account.
        :return: The request response.
        """
        return self._run_and_get_response(method="POST", endpoint=f"{account_id}/jobs/{job_id}/rerun/")

    def test_connection(self) -> tuple[bool, str]:
        """Test dbt Cloud connection."""
        try:
            self._run_and_get_response()
            return True, "Successfully connected to dbt Cloud."
        except Exception as e:
            return False, str(e)
