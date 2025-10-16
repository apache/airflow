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
import copy
import json
import time
import warnings
from collections.abc import Callable, Sequence
from enum import Enum
from functools import cached_property, wraps
from inspect import signature
from typing import TYPE_CHECKING, Any, TypedDict, TypeVar, cast

import aiohttp
from asgiref.sync import sync_to_async
from requests import exceptions as requests_exceptions
from requests.auth import AuthBase
from requests.sessions import Session
from tenacity import AsyncRetrying, RetryCallState, retry_if_exception, stop_after_attempt, wait_exponential

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook

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


class DbtCloudResourceLookupError(AirflowException):
    """Exception raised when a dbt Cloud resource cannot be uniquely identified."""


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

    return cast("T", wrapper)


class DbtCloudHook(HttpHook):
    """
    Interact with dbt Cloud using the V2 (V3 if supported) API.

    :param dbt_cloud_conn_id: The ID of the :ref:`dbt Cloud connection <howto/connection:dbt-cloud>`.
    :param timeout_seconds: Optional. The timeout in seconds for HTTP requests. If not provided, no timeout is applied.
    :param retry_limit: The number of times to retry a request in case of failure.
    :param retry_delay: The delay in seconds between retries.
    :param retry_args: A dictionary of arguments to pass to the `tenacity.retry` decorator.
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

    def __init__(
        self,
        dbt_cloud_conn_id: str = default_conn_name,
        timeout_seconds: int | None = None,
        retry_limit: int = 1,
        retry_delay: float = 1.0,
        retry_args: dict[Any, Any] | None = None,
    ) -> None:
        super().__init__(auth_type=TokenAuth)
        self.dbt_cloud_conn_id = dbt_cloud_conn_id
        self.timeout_seconds = timeout_seconds
        if retry_limit < 1:
            raise ValueError("Retry limit must be greater than or equal to 1")
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay

        def retry_after_func(retry_state: RetryCallState) -> None:
            error_msg = str(retry_state.outcome.exception()) if retry_state.outcome else "Unknown error"
            self._log_request_error(retry_state.attempt_number, error_msg)

        if retry_args:
            self.retry_args = copy.copy(retry_args)
            self.retry_args["retry"] = retry_if_exception(self._retryable_error)
            self.retry_args["after"] = retry_after_func
            self.retry_args["reraise"] = True
        else:
            self.retry_args = {
                "stop": stop_after_attempt(self.retry_limit),
                "wait": wait_exponential(min=self.retry_delay, max=(2**retry_limit)),
                "retry": retry_if_exception(self._retryable_error),
                "after": retry_after_func,
                "reraise": True,
            }

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

    def _log_request_error(self, attempt_num: int, error: str) -> None:
        self.log.error("Attempt %s API Request to DBT failed with reason: %s", attempt_num, error)

    @staticmethod
    def _retryable_error(exception: BaseException) -> bool:
        if isinstance(exception, requests_exceptions.RequestException):
            if isinstance(exception, (requests_exceptions.ConnectionError, requests_exceptions.Timeout)) or (
                exception.response is not None
                and (exception.response.status_code >= 500 or exception.response.status_code == 429)
            ):
                return True

        if isinstance(exception, aiohttp.ClientResponseError):
            if exception.status >= 500 or exception.status == 429:
                return True

        if isinstance(exception, (aiohttp.ClientConnectorError, TimeoutError)):
            return True

        return False

    def _a_get_retry_object(self) -> AsyncRetrying:
        """
        Instantiate an async retry object.

        :return: instance of AsyncRetrying class
        """
        # for compatibility we use reraise to avoid handling request error
        return AsyncRetrying(**self.retry_args)

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
        proxy = proxies.get("https") if proxies and url.startswith("https") else proxies.get("http")
        extra_request_args = {}

        if proxy:
            extra_request_args["proxy"] = proxy

        timeout = (
            aiohttp.ClientTimeout(total=self.timeout_seconds) if self.timeout_seconds is not None else None
        )

        async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
            async for attempt in self._a_get_retry_object():
                with attempt:
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

        return _connection  # type: ignore[return-value]

    def get_conn(self, *args, **kwargs) -> Session:
        tenant = self._get_tenant_domain(self.connection)
        self.base_url = f"https://{tenant}/"

        session = Session()
        session.auth = self.auth_type(self.connection.password)

        return session

    def _paginate(
        self, endpoint: str, payload: dict[str, Any] | None = None, proxies: dict[str, str] | None = None
    ) -> list[Response]:
        extra_options: dict[str, Any] = {}
        if self.timeout_seconds is not None:
            extra_options["timeout"] = self.timeout_seconds
        if proxies is not None:
            extra_options["proxies"] = proxies
        response = self.run_with_advanced_retry(
            _retry_args=self.retry_args, endpoint=endpoint, data=payload, extra_options=extra_options or None
        )
        resp_json = response.json()
        limit = resp_json["extra"]["filters"]["limit"]
        num_total_results = resp_json["extra"]["pagination"]["total_count"]
        num_current_results = resp_json["extra"]["pagination"]["count"]
        results = [response]
        if num_current_results != num_total_results:
            _paginate_payload = payload.copy() if payload else {}
            _paginate_payload["offset"] = limit

            while num_current_results < num_total_results:
                response = self.run_with_advanced_retry(
                    _retry_args=self.retry_args,
                    endpoint=endpoint,
                    data=_paginate_payload,
                    extra_options=extra_options,
                )
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
        extra_options: dict[str, Any] = {}
        if self.timeout_seconds is not None:
            extra_options["timeout"] = self.timeout_seconds
        if proxies is not None:
            extra_options["proxies"] = proxies

        if paginate:
            if isinstance(payload, str):
                raise ValueError("Payload cannot be a string to paginate a response.")

            if full_endpoint:
                return self._paginate(endpoint=full_endpoint, payload=payload, proxies=proxies)

            raise ValueError("An endpoint is needed to paginate a response.")

        return self.run_with_advanced_retry(
            _retry_args=self.retry_args,
            endpoint=full_endpoint,
            data=payload,
            extra_options=extra_options or None,
        )

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
    def list_projects(
        self, account_id: int | None = None, name_contains: str | None = None
    ) -> list[Response]:
        """
        Retrieve metadata for all projects tied to a specified dbt Cloud account.

        :param account_id: Optional. The ID of a dbt Cloud account.
        :param name_contains: Optional. The case-insensitive substring of a dbt Cloud project name to filter by.
        :return: List of request responses.
        """
        payload = {"name__icontains": name_contains} if name_contains else None
        return self._run_and_get_response(
            endpoint=f"{account_id}/projects/",
            payload=payload,
            paginate=True,
            api_version="v3",
        )

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
    def list_environments(
        self, project_id: int, *, name_contains: str | None = None, account_id: int | None = None
    ) -> list[Response]:
        """
        Retrieve metadata for all environments tied to a specified dbt Cloud project.

        :param project_id: The ID of a dbt Cloud project.
        :param name_contains: Optional. The case-insensitive substring of a dbt Cloud environment name to filter by.
        :param account_id: Optional. The ID of a dbt Cloud account.
        :return: List of request responses.
        """
        payload = {"name__icontains": name_contains} if name_contains else None
        return self._run_and_get_response(
            endpoint=f"{account_id}/projects/{project_id}/environments/",
            payload=payload,
            paginate=True,
            api_version="v3",
        )

    @fallback_to_default_account
    def get_environment(
        self, project_id: int, environment_id: int, *, account_id: int | None = None
    ) -> Response:
        """
        Retrieve metadata for a specific project's environment.

        :param project_id: The ID of a dbt Cloud project.
        :param environment_id: The ID of a dbt Cloud environment.
        :param account_id: Optional. The ID of a dbt Cloud account.
        :return: The request response.
        """
        return self._run_and_get_response(
            endpoint=f"{account_id}/projects/{project_id}/environments/{environment_id}/", api_version="v3"
        )

    @fallback_to_default_account
    def list_jobs(
        self,
        account_id: int | None = None,
        order_by: str | None = None,
        project_id: int | None = None,
        environment_id: int | None = None,
        name_contains: str | None = None,
    ) -> list[Response]:
        """
        Retrieve metadata for all jobs tied to a specified dbt Cloud account.

        If a ``project_id`` is supplied, only jobs pertaining to this project will be retrieved.
        If an ``environment_id`` is supplied, only jobs pertaining to this environment will be retrieved.

        :param account_id: Optional. The ID of a dbt Cloud account.
        :param order_by: Optional. Field to order the result by. Use '-' to indicate reverse order.
            For example, to use reverse order by the run ID use ``order_by=-id``.
        :param project_id: Optional. The ID of a dbt Cloud project.
        :param environment_id: Optional. The ID of a dbt Cloud environment.
        :param name_contains: Optional. The case-insensitive substring of a dbt Cloud job name to filter by.
        :return: List of request responses.
        """
        payload = {"order_by": order_by, "project_id": project_id}
        if environment_id:
            payload["environment_id"] = environment_id
        if name_contains:
            payload["name__icontains"] = name_contains
        return self._run_and_get_response(
            endpoint=f"{account_id}/jobs/",
            payload=payload,
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
    def get_job_by_name(
        self, *, project_name: str, environment_name: str, job_name: str, account_id: int | None = None
    ) -> dict:
        """
        Retrieve metadata for a specific job by combination of project, environment, and job name.

        Raises DbtCloudResourceLookupError if the job is not found or cannot be uniquely identified by provided parameters.

        :param project_name: The name of a dbt Cloud project.
        :param environment_name: The name of a dbt Cloud environment.
        :param job_name: The name of a dbt Cloud job.
        :param account_id: Optional. The ID of a dbt Cloud account.
        :return: The details of a job.
        """
        # get project_id using project_name
        list_projects_responses = self.list_projects(name_contains=project_name, account_id=account_id)
        # flatten & filter the list of responses to find the exact match
        projects = [
            project
            for response in list_projects_responses
            for project in response.json()["data"]
            if project["name"] == project_name
        ]
        if len(projects) != 1:
            raise DbtCloudResourceLookupError(f"Found {len(projects)} projects with name `{project_name}`.")
        project_id = projects[0]["id"]

        # get environment_id using project_id and environment_name
        list_environments_responses = self.list_environments(
            project_id=project_id, name_contains=environment_name, account_id=account_id
        )
        # flatten & filter the list of responses to find the exact match
        environments = [
            env
            for response in list_environments_responses
            for env in response.json()["data"]
            if env["name"] == environment_name
        ]
        if len(environments) != 1:
            raise DbtCloudResourceLookupError(
                f"Found {len(environments)} environments with name `{environment_name}` in project `{project_name}`."
            )
        environment_id = environments[0]["id"]

        # get job using project_id, environment_id and job_name
        list_jobs_responses = self.list_jobs(
            project_id=project_id,
            environment_id=environment_id,
            name_contains=job_name,
            account_id=account_id,
        )
        # flatten & filter the list of responses to find the exact match
        jobs = [
            job
            for response in list_jobs_responses
            for job in response.json()["data"]
            if job["name"] == job_name
        ]
        if len(jobs) != 1:
            raise DbtCloudResourceLookupError(
                f"Found {len(jobs)} jobs with name `{job_name}` in environment `{environment_name}` in project `{project_name}`."
            )

        return jobs[0]

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
                f"Cause `{cause}` exceeds limit of {DBT_CAUSE_MAX_LENGTH} characters and will be truncated.",
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
