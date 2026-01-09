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
from copy import deepcopy
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
from requests import exceptions as requests_exceptions
from requests.models import Response

from airflow.models.connection import Connection
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.dbt.cloud.hooks.dbt import (
    DBT_CAUSE_MAX_LENGTH,
    DbtCloudHook,
    DbtCloudJobRunException,
    DbtCloudJobRunStatus,
    DbtCloudResourceLookupError,
    TokenAuth,
    fallback_to_default_account,
)

from tests_common.test_utils.compat import timezone

ACCOUNT_ID_CONN = "account_id_conn"
NO_ACCOUNT_ID_CONN = "no_account_id_conn"
SINGLE_TENANT_CONN = "single_tenant_conn"
PROXY_CONN = "proxy_conn"
DEFAULT_ACCOUNT_ID = 11111
ACCOUNT_ID = 22222
SINGLE_TENANT_DOMAIN = "single.tenant.getdbt.com"
EXTRA_PROXIES = {"proxies": {"https": "http://myproxy:1234"}}
TOKEN = "token"
PROJECT_ID = 33333
PROJECT_NAME = "project_name"
ENVIRONMENT_ID = 44444
ENVIRONMENT_NAME = "environment_name"
JOB_ID = 4444
JOB_NAME = "job_name"
RUN_ID = 5555

BASE_URL = "https://cloud.getdbt.com/"
SINGLE_TENANT_URL = "https://single.tenant.getdbt.com/"
NOT_VAILD_DBT_STATUS = "not a valid DbtCloudJobRunStatus"

DEFAULT_LIST_PROJECTS_RESPONSE = {
    "data": [
        {
            "id": PROJECT_ID,
            "name": PROJECT_NAME,
        }
    ]
}
DEFAULT_LIST_ENVIRONMENTS_RESPONSE = {
    "data": [
        {
            "id": ENVIRONMENT_ID,
            "name": ENVIRONMENT_NAME,
        }
    ]
}
DEFAULT_LIST_JOBS_RESPONSE = {
    "data": [
        {
            "id": JOB_ID,
            "name": JOB_NAME,
        }
    ]
}


def mock_response_json(response: dict):
    run_response = MagicMock(**response, spec=Response)
    run_response.json.return_value = response
    return run_response


def request_exception_with_status(status_code: int) -> requests_exceptions.HTTPError:
    response = Response()
    response.status_code = status_code
    return requests_exceptions.HTTPError(response=response)


class TestDbtCloudJobRunStatus:
    valid_job_run_statuses = [
        1,  # QUEUED
        2,  # STARTING
        3,  # RUNNING
        10,  # SUCCESS
        20,  # ERROR
        30,  # CANCELLED
        [1, 2, 3],  # QUEUED, STARTING, and RUNNING
        {10, 20, 30},  # SUCCESS, ERROR, and CANCELLED
    ]
    invalid_job_run_statuses = [
        123,  # Single invalid status
        [123, 23, 65],  # Multiple invalid statuses
        [1, 2, 65],  # Not all statuses are valid
        "1",  # String types are not valid
        "12",
        ["1", "2", "65"],
    ]

    def _get_ids(status_set: Any):
        return [f"checking_status_{argval}" for argval in status_set]

    @pytest.mark.parametrize(
        argnames="statuses",
        argvalues=valid_job_run_statuses,
        ids=_get_ids(valid_job_run_statuses),
    )
    def test_valid_job_run_status(self, statuses):
        DbtCloudJobRunStatus.check_is_valid(statuses)

    @pytest.mark.parametrize(
        argnames="statuses",
        argvalues=invalid_job_run_statuses,
        ids=_get_ids(invalid_job_run_statuses),
    )
    def test_invalid_job_run_status(self, statuses):
        with pytest.raises(ValueError, match=NOT_VAILD_DBT_STATUS):
            DbtCloudJobRunStatus.check_is_valid(statuses)

    @pytest.mark.parametrize(
        argnames="statuses",
        argvalues=valid_job_run_statuses,
        ids=_get_ids(valid_job_run_statuses),
    )
    def test_valid_terminal_job_run_status(self, statuses):
        DbtCloudJobRunStatus.check_is_valid(statuses)

    @pytest.mark.parametrize(
        argnames="statuses",
        argvalues=invalid_job_run_statuses,
        ids=_get_ids(invalid_job_run_statuses),
    )
    def test_invalid_terminal_job_run_status(self, statuses):
        with pytest.raises(ValueError, match=NOT_VAILD_DBT_STATUS):
            DbtCloudJobRunStatus.check_is_valid(statuses)


class TestDbtCloudHook:
    # TODO: Potential performance issue, converted setup_class to a setup_connections function level fixture
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        # Connection with ``account_id`` specified
        account_id_conn = Connection(
            conn_id=ACCOUNT_ID_CONN,
            conn_type=DbtCloudHook.conn_type,
            login=str(DEFAULT_ACCOUNT_ID),
            password=TOKEN,
        )

        # Connection with no ``account_id`` specified
        no_account_id_conn = Connection(
            conn_id=NO_ACCOUNT_ID_CONN,
            conn_type=DbtCloudHook.conn_type,
            password=TOKEN,
        )

        # Connection with `host` parameter set
        host_conn = Connection(
            conn_id=SINGLE_TENANT_CONN,
            conn_type=DbtCloudHook.conn_type,
            login=str(DEFAULT_ACCOUNT_ID),
            password=TOKEN,
            host=SINGLE_TENANT_DOMAIN,
        )

        # Connection with a proxy set in extra parameters
        proxy_conn = Connection(
            conn_id=PROXY_CONN,
            conn_type=DbtCloudHook.conn_type,
            login=str(DEFAULT_ACCOUNT_ID),
            password=TOKEN,
            host=SINGLE_TENANT_DOMAIN,
            extra=EXTRA_PROXIES,
        )

        create_connection_without_db(account_id_conn)
        create_connection_without_db(no_account_id_conn)
        create_connection_without_db(host_conn)
        create_connection_without_db(proxy_conn)

    @pytest.mark.parametrize(
        argnames=("conn_id", "url"),
        argvalues=[(ACCOUNT_ID_CONN, BASE_URL), (SINGLE_TENANT_CONN, SINGLE_TENANT_URL)],
        ids=["multi-tenant", "single-tenant"],
    )
    def test_init_hook(self, conn_id, url):
        hook = DbtCloudHook(conn_id)
        assert hook.auth_type == TokenAuth
        assert hook.method == "POST"
        assert hook.dbt_cloud_conn_id == conn_id

    @pytest.mark.parametrize(
        argnames=("conn_id", "url"),
        argvalues=[(ACCOUNT_ID_CONN, BASE_URL), (SINGLE_TENANT_CONN, SINGLE_TENANT_URL)],
        ids=["multi-tenant", "single-tenant"],
    )
    def test_tenant_base_url(self, conn_id, url):
        hook = DbtCloudHook(conn_id)
        hook.get_conn()
        assert hook.base_url == url

    @pytest.mark.parametrize(
        argnames=("conn_id", "account_id"),
        argvalues=[(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    def test_fallback_to_default_account(self, conn_id, account_id):
        hook = DbtCloudHook(conn_id)

        def dbt_cloud_func(_, account_id=None):
            return account_id

        _account_id = account_id or DEFAULT_ACCOUNT_ID

        if conn_id == ACCOUNT_ID_CONN:
            assert fallback_to_default_account(dbt_cloud_func)(hook, account_id=account_id) == _account_id
            assert fallback_to_default_account(dbt_cloud_func)(hook) == _account_id

        if conn_id == NO_ACCOUNT_ID_CONN:
            assert fallback_to_default_account(dbt_cloud_func)(hook, account_id=account_id) == _account_id

            with pytest.raises(AirflowException):
                fallback_to_default_account(dbt_cloud_func)(hook)

    @pytest.mark.parametrize(
        argnames=("conn_id", "account_id"),
        argvalues=[(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    @patch.object(DbtCloudHook, "run")
    @patch.object(DbtCloudHook, "_paginate")
    def test_list_accounts(self, mock_http_run, mock_paginate, conn_id, account_id):
        hook = DbtCloudHook(conn_id)
        hook.list_accounts()

        assert hook.method == "GET"
        hook.run.assert_called_once_with(endpoint=None, data=None, extra_options=None)
        hook._paginate.assert_not_called()

    @pytest.mark.parametrize(
        argnames=("conn_id", "account_id"),
        argvalues=[(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    @patch.object(DbtCloudHook, "run")
    @patch.object(DbtCloudHook, "_paginate")
    def test_get_account(self, mock_paginate, mock_http_run, conn_id, account_id):
        hook = DbtCloudHook(conn_id)
        hook.get_account(account_id=account_id)

        assert hook.method == "GET"

        _account_id = account_id or DEFAULT_ACCOUNT_ID
        hook.run.assert_called_once_with(
            endpoint=f"api/v2/accounts/{_account_id}/", data=None, extra_options=None
        )
        hook._paginate.assert_not_called()

    @pytest.mark.parametrize(
        argnames=("conn_id", "account_id"),
        argvalues=[(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    @patch.object(DbtCloudHook, "run")
    @patch.object(DbtCloudHook, "_paginate")
    def test_list_projects(self, mock_http_run, mock_paginate, conn_id, account_id):
        hook = DbtCloudHook(conn_id)
        hook.list_projects(account_id=account_id)

        assert hook.method == "GET"

        _account_id = account_id or DEFAULT_ACCOUNT_ID
        hook.run.assert_not_called()
        hook._paginate.assert_called_once_with(
            endpoint=f"api/v3/accounts/{_account_id}/projects/",
            payload=None,
            proxies=None,
        )

    @pytest.mark.parametrize(
        argnames=("conn_id", "account_id", "name_contains"),
        argvalues=[(ACCOUNT_ID_CONN, None, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID, PROJECT_NAME)],
        ids=["default_account", "explicit_account"],
    )
    @patch.object(DbtCloudHook, "run")
    @patch.object(DbtCloudHook, "_paginate")
    def test_list_projects_with_payload(
        self, mock_http_run, mock_paginate, conn_id, account_id, name_contains
    ):
        hook = DbtCloudHook(conn_id)
        hook.list_projects(account_id=account_id, name_contains=name_contains)

        assert hook.method == "GET"

        _account_id = account_id or DEFAULT_ACCOUNT_ID
        hook.run.assert_not_called()
        hook._paginate.assert_called_once_with(
            endpoint=f"api/v3/accounts/{_account_id}/projects/",
            payload={"name__icontains": name_contains} if name_contains else None,
            proxies=None,
        )

    @pytest.mark.parametrize(
        argnames=("conn_id", "account_id"),
        argvalues=[(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    @patch.object(DbtCloudHook, "run")
    @patch.object(DbtCloudHook, "_paginate")
    def test_get_project(self, mock_http_run, mock_paginate, conn_id, account_id):
        hook = DbtCloudHook(conn_id)
        hook.get_project(project_id=PROJECT_ID, account_id=account_id)

        assert hook.method == "GET"

        _account_id = account_id or DEFAULT_ACCOUNT_ID
        hook.run.assert_called_once_with(
            endpoint=f"api/v3/accounts/{_account_id}/projects/{PROJECT_ID}/", data=None, extra_options=None
        )
        hook._paginate.assert_not_called()

    @pytest.mark.parametrize(
        argnames=("conn_id", "account_id"),
        argvalues=[(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    @patch.object(DbtCloudHook, "run")
    @patch.object(DbtCloudHook, "_paginate")
    def test_list_environments(self, mock_http_run, mock_paginate, conn_id, account_id):
        hook = DbtCloudHook(conn_id)
        hook.list_environments(project_id=PROJECT_ID, account_id=account_id)

        assert hook.method == "GET"

        _account_id = account_id or DEFAULT_ACCOUNT_ID
        hook.run.assert_not_called()
        hook._paginate.assert_called_once_with(
            endpoint=f"api/v3/accounts/{_account_id}/projects/{PROJECT_ID}/environments/",
            payload=None,
            proxies=None,
        )

    @pytest.mark.parametrize(
        argnames=("conn_id", "account_id", "name_contains"),
        argvalues=[(ACCOUNT_ID_CONN, None, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID, ENVIRONMENT_NAME)],
        ids=["default_account", "explicit_account"],
    )
    @patch.object(DbtCloudHook, "run")
    @patch.object(DbtCloudHook, "_paginate")
    def test_list_environments_with_payload(
        self, mock_http_run, mock_paginate, conn_id, account_id, name_contains
    ):
        hook = DbtCloudHook(conn_id)
        hook.list_environments(project_id=PROJECT_ID, account_id=account_id, name_contains=name_contains)

        assert hook.method == "GET"

        _account_id = account_id or DEFAULT_ACCOUNT_ID
        hook.run.assert_not_called()
        hook._paginate.assert_called_once_with(
            endpoint=f"api/v3/accounts/{_account_id}/projects/{PROJECT_ID}/environments/",
            payload={"name__icontains": name_contains} if name_contains else None,
            proxies=None,
        )

    @pytest.mark.parametrize(
        argnames=("conn_id", "account_id"),
        argvalues=[(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    @patch.object(DbtCloudHook, "run")
    @patch.object(DbtCloudHook, "_paginate")
    def test_get_environment(self, mock_http_run, mock_paginate, conn_id, account_id):
        hook = DbtCloudHook(conn_id)
        hook.get_environment(project_id=PROJECT_ID, environment_id=ENVIRONMENT_ID, account_id=account_id)

        assert hook.method == "GET"

        _account_id = account_id or DEFAULT_ACCOUNT_ID
        hook.run.assert_called_once_with(
            endpoint=f"api/v3/accounts/{_account_id}/projects/{PROJECT_ID}/environments/{ENVIRONMENT_ID}/",
            data=None,
            extra_options=None,
        )
        hook._paginate.assert_not_called()

    @pytest.mark.parametrize(
        argnames=("conn_id", "account_id"),
        argvalues=[(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    @patch.object(DbtCloudHook, "run")
    @patch.object(DbtCloudHook, "_paginate")
    def test_list_jobs(self, mock_http_run, mock_paginate, conn_id, account_id):
        hook = DbtCloudHook(conn_id)
        hook.list_jobs(account_id=account_id)

        assert hook.method == "GET"

        _account_id = account_id or DEFAULT_ACCOUNT_ID
        hook._paginate.assert_called_once_with(
            endpoint=f"api/v2/accounts/{_account_id}/jobs/",
            payload={"order_by": None, "project_id": None},
            proxies=None,
        )
        hook.run.assert_not_called()

    @pytest.mark.parametrize(
        argnames=("conn_id", "account_id"),
        argvalues=[(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    @patch.object(DbtCloudHook, "run")
    @patch.object(DbtCloudHook, "_paginate")
    def test_list_jobs_with_payload(self, mock_http_run, mock_paginate, conn_id, account_id):
        hook = DbtCloudHook(conn_id)
        hook.list_jobs(
            project_id=PROJECT_ID,
            account_id=account_id,
            order_by="-id",
            environment_id=ENVIRONMENT_ID,
            name_contains=JOB_NAME,
        )

        assert hook.method == "GET"

        _account_id = account_id or DEFAULT_ACCOUNT_ID
        hook._paginate.assert_called_once_with(
            endpoint=f"api/v2/accounts/{_account_id}/jobs/",
            payload={
                "order_by": "-id",
                "project_id": PROJECT_ID,
                "environment_id": ENVIRONMENT_ID,
                "name__icontains": JOB_NAME,
            },
            proxies=None,
        )
        hook.run.assert_not_called()

    @pytest.mark.parametrize(
        argnames=("conn_id", "account_id"),
        argvalues=[(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    @patch.object(DbtCloudHook, "run")
    @patch.object(DbtCloudHook, "_paginate")
    def test_get_job(self, mock_http_run, mock_paginate, conn_id, account_id):
        hook = DbtCloudHook(conn_id)
        hook.get_job(job_id=JOB_ID, account_id=account_id)

        assert hook.method == "GET"

        _account_id = account_id or DEFAULT_ACCOUNT_ID
        hook.run.assert_called_once_with(
            endpoint=f"api/v2/accounts/{_account_id}/jobs/{JOB_ID}", data=None, extra_options=None
        )
        hook._paginate.assert_not_called()

    @patch.object(DbtCloudHook, "list_jobs", return_value=[mock_response_json(DEFAULT_LIST_JOBS_RESPONSE)])
    @patch.object(
        DbtCloudHook,
        "list_environments",
        return_value=[mock_response_json(DEFAULT_LIST_ENVIRONMENTS_RESPONSE)],
    )
    @patch.object(
        DbtCloudHook, "list_projects", return_value=[mock_response_json(DEFAULT_LIST_PROJECTS_RESPONSE)]
    )
    def test_get_job_by_name_returns_response(
        self, mock_list_projects, mock_list_environments, mock_list_jobs
    ):
        hook = DbtCloudHook(ACCOUNT_ID_CONN)
        job_details = hook.get_job_by_name(
            project_name=PROJECT_NAME,
            environment_name=ENVIRONMENT_NAME,
            job_name=JOB_NAME,
            account_id=None,
        )

        assert job_details == DEFAULT_LIST_JOBS_RESPONSE["data"][0]

    @pytest.mark.parametrize(
        argnames=("project_name", "environment_name", "job_name"),
        argvalues=[
            ("dummy_name", ENVIRONMENT_NAME, JOB_NAME),
            (PROJECT_NAME, "dummy_name", JOB_NAME),
            (PROJECT_NAME, ENVIRONMENT_NAME, JOB_NAME.upper()),
            (None, ENVIRONMENT_NAME, JOB_NAME),
            (PROJECT_NAME, "", JOB_NAME),
            ("", "", ""),
        ],
    )
    @patch.object(DbtCloudHook, "list_jobs", return_value=[mock_response_json(DEFAULT_LIST_JOBS_RESPONSE)])
    @patch.object(
        DbtCloudHook,
        "list_environments",
        return_value=[mock_response_json(DEFAULT_LIST_ENVIRONMENTS_RESPONSE)],
    )
    @patch.object(
        DbtCloudHook, "list_projects", return_value=[mock_response_json(DEFAULT_LIST_PROJECTS_RESPONSE)]
    )
    def test_get_job_by_incorrect_name_raises_exception(
        self,
        mock_list_projects,
        mock_list_environments,
        mock_list_jobs,
        project_name,
        environment_name,
        job_name,
    ):
        hook = DbtCloudHook(ACCOUNT_ID_CONN)
        with pytest.raises(DbtCloudResourceLookupError, match="Found 0"):
            hook.get_job_by_name(
                project_name=project_name,
                environment_name=environment_name,
                job_name=job_name,
                account_id=None,
            )

    @pytest.mark.parametrize("duplicated", ["projects", "environments", "jobs"])
    def test_get_job_by_duplicate_name_raises_exception(self, duplicated):
        hook = DbtCloudHook(ACCOUNT_ID_CONN)
        mock_list_jobs_response = deepcopy(DEFAULT_LIST_JOBS_RESPONSE)
        mock_list_environments_response = deepcopy(DEFAULT_LIST_ENVIRONMENTS_RESPONSE)
        mock_list_projects_response = deepcopy(DEFAULT_LIST_PROJECTS_RESPONSE)

        if duplicated == "projects":
            mock_list_projects_response["data"].append(
                {
                    "id": PROJECT_ID + 1,
                    "name": PROJECT_NAME,
                }
            )
        elif duplicated == "environments":
            mock_list_environments_response["data"].append(
                {
                    "id": ENVIRONMENT_ID + 1,
                    "name": ENVIRONMENT_NAME,
                }
            )
        elif duplicated == "jobs":
            mock_list_jobs_response["data"].append(
                {
                    "id": JOB_ID + 1,
                    "name": JOB_NAME,
                }
            )

        with (
            patch.object(
                DbtCloudHook, "list_jobs", return_value=[mock_response_json(mock_list_jobs_response)]
            ),
            patch.object(
                DbtCloudHook,
                "list_environments",
                return_value=[mock_response_json(mock_list_environments_response)],
            ),
            patch.object(
                DbtCloudHook,
                "list_projects",
                return_value=[mock_response_json(mock_list_projects_response)],
            ),
        ):
            with pytest.raises(DbtCloudResourceLookupError, match=f"Found 2 {duplicated}"):
                hook.get_job_by_name(
                    project_name=PROJECT_NAME,
                    environment_name=ENVIRONMENT_NAME,
                    job_name=JOB_NAME,
                    account_id=None,
                )

    @pytest.mark.parametrize(
        argnames=("conn_id", "account_id"),
        argvalues=[(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    @patch.object(DbtCloudHook, "run")
    @patch.object(DbtCloudHook, "_paginate")
    def test_trigger_job_run(self, mock_http_run, mock_paginate, conn_id, account_id):
        hook = DbtCloudHook(conn_id)
        cause = ""
        hook.trigger_job_run(job_id=JOB_ID, cause=cause, account_id=account_id)

        assert hook.method == "POST"

        _account_id = account_id or DEFAULT_ACCOUNT_ID
        hook.run.assert_called_once_with(
            endpoint=f"api/v2/accounts/{_account_id}/jobs/{JOB_ID}/run/",
            data=json.dumps({"cause": cause, "steps_override": None, "schema_override": None}),
            extra_options=None,
        )
        hook._paginate.assert_not_called()

    @pytest.mark.parametrize(
        argnames=("conn_id", "account_id"),
        argvalues=[(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    @patch.object(DbtCloudHook, "run")
    @patch.object(DbtCloudHook, "_paginate")
    def test_trigger_job_run_with_overrides(self, mock_http_run, mock_paginate, conn_id, account_id):
        hook = DbtCloudHook(conn_id)
        cause = ""
        steps_override = ["dbt test", "dbt run"]
        schema_override = ["other_schema"]
        hook.trigger_job_run(
            job_id=JOB_ID,
            cause=cause,
            account_id=account_id,
            steps_override=steps_override,
            schema_override=schema_override,
        )

        assert hook.method == "POST"

        _account_id = account_id or DEFAULT_ACCOUNT_ID
        hook.run.assert_called_once_with(
            endpoint=f"api/v2/accounts/{_account_id}/jobs/{JOB_ID}/run/",
            data=json.dumps(
                {"cause": cause, "steps_override": steps_override, "schema_override": schema_override}
            ),
            extra_options=None,
        )
        hook._paginate.assert_not_called()

    @pytest.mark.parametrize(
        argnames=("conn_id", "account_id"),
        argvalues=[(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    @patch.object(DbtCloudHook, "run")
    @patch.object(DbtCloudHook, "_paginate")
    def test_trigger_job_run_with_additional_run_configs(
        self, mock_http_run, mock_paginate, conn_id, account_id
    ):
        hook = DbtCloudHook(conn_id)
        cause = ""
        additional_run_config = {"threads_override": 8, "generate_docs_override": False}
        hook.trigger_job_run(
            job_id=JOB_ID, cause=cause, account_id=account_id, additional_run_config=additional_run_config
        )

        assert hook.method == "POST"

        _account_id = account_id or DEFAULT_ACCOUNT_ID
        hook.run.assert_called_once_with(
            endpoint=f"api/v2/accounts/{_account_id}/jobs/{JOB_ID}/run/",
            data=json.dumps(
                {
                    "cause": cause,
                    "steps_override": None,
                    "schema_override": None,
                    "threads_override": 8,
                    "generate_docs_override": False,
                }
            ),
            extra_options=None,
        )
        hook._paginate.assert_not_called()

    @pytest.mark.parametrize(
        argnames=("conn_id", "account_id"),
        argvalues=[(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    @patch.object(DbtCloudHook, "run")
    @patch.object(DbtCloudHook, "_paginate")
    def test_trigger_job_run_with_longer_cause(self, mock_http_run, mock_paginate, conn_id, account_id):
        hook = DbtCloudHook(conn_id)
        cause = "Some cause that is longer than limit. " * 15
        expected_cause = cause[:DBT_CAUSE_MAX_LENGTH]
        assert len(cause) > DBT_CAUSE_MAX_LENGTH

        with pytest.warns(
            UserWarning,
            match=f"Cause `{cause}` exceeds limit of {DBT_CAUSE_MAX_LENGTH}"
            f" characters and will be truncated.",
        ):
            hook.trigger_job_run(job_id=JOB_ID, cause=cause, account_id=account_id)

        assert hook.method == "POST"

        _account_id = account_id or DEFAULT_ACCOUNT_ID
        hook.run.assert_called_once_with(
            endpoint=f"api/v2/accounts/{_account_id}/jobs/{JOB_ID}/run/",
            data=json.dumps({"cause": expected_cause, "steps_override": None, "schema_override": None}),
            extra_options=None,
        )
        hook._paginate.assert_not_called()

    @pytest.mark.parametrize(
        argnames=("conn_id", "account_id"),
        argvalues=[(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    @pytest.mark.parametrize(
        argnames=("get_job_runs_data", "should_use_rerun"),
        argvalues=[
            ([], False),
            ([{"status": DbtCloudJobRunStatus.QUEUED.value}], False),
            ([{"status": DbtCloudJobRunStatus.STARTING.value}], False),
            ([{"status": DbtCloudJobRunStatus.RUNNING.value}], False),
            ([{"status": DbtCloudJobRunStatus.SUCCESS.value}], False),
            ([{"status": DbtCloudJobRunStatus.ERROR.value}], True),
            ([{"status": DbtCloudJobRunStatus.CANCELLED.value}], False),
        ],
    )
    @patch.object(DbtCloudHook, "run")
    @patch.object(DbtCloudHook, "_paginate")
    def test_trigger_job_run_with_retry_from_failure(
        self,
        mock_http_run,
        mock_paginate,
        get_job_runs_data,
        should_use_rerun,
        conn_id,
        account_id,
    ):
        hook = DbtCloudHook(conn_id)
        cause = ""
        retry_from_failure = True

        with patch.object(DbtCloudHook, "get_job_runs") as mock_get_job_run_status:
            mock_get_job_run_status.return_value.json.return_value = {"data": get_job_runs_data}
            hook.trigger_job_run(
                job_id=JOB_ID, cause=cause, account_id=account_id, retry_from_failure=retry_from_failure
            )
            assert hook.method == "POST"
            _account_id = account_id or DEFAULT_ACCOUNT_ID
            hook._paginate.assert_not_called()
            if should_use_rerun:
                hook.run.assert_called_once_with(
                    endpoint=f"api/v2/accounts/{_account_id}/jobs/{JOB_ID}/rerun/",
                    data=None,
                    extra_options=None,
                )
            else:
                hook.run.assert_called_once_with(
                    endpoint=f"api/v2/accounts/{_account_id}/jobs/{JOB_ID}/run/",
                    data=json.dumps(
                        {
                            "cause": cause,
                            "steps_override": None,
                            "schema_override": None,
                        }
                    ),
                    extra_options=None,
                )

    @pytest.mark.parametrize(
        argnames=("conn_id", "account_id"),
        argvalues=[(PROXY_CONN, ACCOUNT_ID)],
        ids=["proxy_connection"],
    )
    @patch.object(DbtCloudHook, "run")
    @patch.object(DbtCloudHook, "_paginate")
    def test_trigger_job_run_with_proxy(self, mock_http_run, mock_paginate, conn_id, account_id):
        hook = DbtCloudHook(conn_id)
        cause = ""
        hook.trigger_job_run(job_id=JOB_ID, cause=cause, account_id=account_id)

        assert hook.method == "POST"

        _account_id = account_id or DEFAULT_ACCOUNT_ID
        hook.run.assert_called_once_with(
            endpoint=f"api/v2/accounts/{_account_id}/jobs/{JOB_ID}/run/",
            data=json.dumps({"cause": cause, "steps_override": None, "schema_override": None}),
            extra_options={"proxies": {"https": "http://myproxy:1234"}},
        )
        hook._paginate.assert_not_called()

    @pytest.mark.parametrize(
        argnames=("conn_id", "account_id"),
        argvalues=[(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    @patch.object(DbtCloudHook, "run")
    @patch.object(DbtCloudHook, "_paginate")
    def test_list_job_runs(self, mock_http_run, mock_paginate, conn_id, account_id):
        hook = DbtCloudHook(conn_id)
        hook.list_job_runs(account_id=account_id)

        assert hook.method == "GET"

        _account_id = account_id or DEFAULT_ACCOUNT_ID
        hook.run.assert_not_called()
        hook._paginate.assert_called_once_with(
            endpoint=f"api/v2/accounts/{_account_id}/runs/",
            payload={
                "include_related": None,
                "job_definition_id": None,
                "order_by": None,
            },
            proxies=None,
        )

    @pytest.mark.parametrize(
        argnames=("conn_id", "account_id"),
        argvalues=[(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    @patch.object(DbtCloudHook, "run")
    @patch.object(DbtCloudHook, "_paginate")
    def test_list_job_runs_with_payload(self, mock_http_run, mock_paginate, conn_id, account_id):
        hook = DbtCloudHook(conn_id)
        hook.list_job_runs(
            account_id=account_id, include_related=["job"], job_definition_id=JOB_ID, order_by="id"
        )

        assert hook.method == "GET"

        _account_id = account_id or DEFAULT_ACCOUNT_ID
        hook.run.assert_not_called()
        hook._paginate.assert_called_once_with(
            endpoint=f"api/v2/accounts/{_account_id}/runs/",
            payload={
                "include_related": ["job"],
                "job_definition_id": JOB_ID,
                "order_by": "id",
            },
            proxies=None,
        )

    @pytest.mark.parametrize(
        argnames=("conn_id", "account_id"),
        argvalues=[(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    @patch.object(DbtCloudHook, "run")
    def test_get_job_runs(self, mock_http_run, conn_id, account_id):
        hook = DbtCloudHook(conn_id)
        hook.get_job_runs(account_id=account_id)

        assert hook.method == "GET"

        _account_id = account_id or DEFAULT_ACCOUNT_ID
        hook.run.assert_called_once_with(
            endpoint=f"api/v2/accounts/{_account_id}/runs/", data=None, extra_options=None
        )

    @pytest.mark.parametrize(
        argnames=("conn_id", "account_id"),
        argvalues=[(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    @patch.object(DbtCloudHook, "run")
    @patch.object(DbtCloudHook, "_paginate")
    def test_get_job_run(self, mock_http_run, mock_paginate, conn_id, account_id):
        hook = DbtCloudHook(conn_id)
        hook.get_job_run(run_id=RUN_ID, account_id=account_id)

        assert hook.method == "GET"

        _account_id = account_id or DEFAULT_ACCOUNT_ID
        hook.run.assert_called_once_with(
            endpoint=f"api/v2/accounts/{_account_id}/runs/{RUN_ID}/",
            data={"include_related": None},
            extra_options=None,
        )
        hook._paginate.assert_not_called()

    @pytest.mark.parametrize(
        argnames=("conn_id", "account_id"),
        argvalues=[(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    @patch.object(DbtCloudHook, "run")
    @patch.object(DbtCloudHook, "_paginate")
    def test_get_job_run_with_payload(self, mock_http_run, mock_paginate, conn_id, account_id):
        hook = DbtCloudHook(conn_id)
        hook.get_job_run(run_id=RUN_ID, account_id=account_id, include_related=["triggers"])

        assert hook.method == "GET"

        _account_id = account_id or DEFAULT_ACCOUNT_ID
        hook.run.assert_called_once_with(
            endpoint=f"api/v2/accounts/{_account_id}/runs/{RUN_ID}/",
            data={"include_related": ["triggers"]},
            extra_options=None,
        )
        hook._paginate.assert_not_called()

    wait_for_job_run_status_test_args = [
        (DbtCloudJobRunStatus.SUCCESS.value, DbtCloudJobRunStatus.SUCCESS.value, True),
        (DbtCloudJobRunStatus.ERROR.value, DbtCloudJobRunStatus.SUCCESS.value, False),
        (DbtCloudJobRunStatus.CANCELLED.value, DbtCloudJobRunStatus.SUCCESS.value, False),
        (DbtCloudJobRunStatus.RUNNING.value, DbtCloudJobRunStatus.SUCCESS.value, "timeout"),
        (DbtCloudJobRunStatus.QUEUED.value, DbtCloudJobRunStatus.SUCCESS.value, "timeout"),
        (DbtCloudJobRunStatus.STARTING.value, DbtCloudJobRunStatus.SUCCESS.value, "timeout"),
        (DbtCloudJobRunStatus.SUCCESS.value, DbtCloudJobRunStatus.TERMINAL_STATUSES.value, True),
        (DbtCloudJobRunStatus.ERROR.value, DbtCloudJobRunStatus.TERMINAL_STATUSES.value, True),
        (DbtCloudJobRunStatus.CANCELLED.value, DbtCloudJobRunStatus.TERMINAL_STATUSES.value, True),
    ]

    @pytest.mark.parametrize(
        argnames=("job_run_status", "expected_status", "expected_output"),
        argvalues=wait_for_job_run_status_test_args,
        ids=[
            (
                f"run_status_{argval[0]}_expected_{argval[1]}"
                if isinstance(argval[1], int)
                else f"run_status_{argval[0]}_expected_AnyTerminalStatus"
            )
            for argval in wait_for_job_run_status_test_args
        ],
    )
    def test_wait_for_job_run_status(self, job_run_status, expected_status, expected_output, time_machine):
        config = {"run_id": RUN_ID, "timeout": 3, "check_interval": 1, "expected_statuses": expected_status}
        hook = DbtCloudHook(ACCOUNT_ID_CONN)

        # Freeze time for avoid real clock side effects
        time_machine.move_to(timezone.datetime(1970, 1, 1), tick=False)

        def fake_sleep(seconds):
            # Shift frozen time every time we call a ``time.sleep`` during this test case.
            time_machine.shift(timedelta(seconds=seconds))

        with (
            patch.object(DbtCloudHook, "get_job_run_status") as mock_job_run_status,
            patch("airflow.providers.dbt.cloud.hooks.dbt.time.sleep", side_effect=fake_sleep),
        ):
            mock_job_run_status.return_value = job_run_status

            if expected_output != "timeout":
                assert hook.wait_for_job_run_status(**config) == expected_output
            else:
                with pytest.raises(DbtCloudJobRunException):
                    hook.wait_for_job_run_status(**config)

    @pytest.mark.parametrize(
        argnames=("conn_id", "account_id"),
        argvalues=[(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    @patch.object(DbtCloudHook, "run")
    @patch.object(DbtCloudHook, "_paginate")
    def test_cancel_job_run(self, mock_http_run, mock_paginate, conn_id, account_id):
        hook = DbtCloudHook(conn_id)
        hook.cancel_job_run(run_id=RUN_ID, account_id=account_id)

        assert hook.method == "POST"

        _account_id = account_id or DEFAULT_ACCOUNT_ID
        hook.run.assert_called_once_with(
            endpoint=f"api/v2/accounts/{_account_id}/runs/{RUN_ID}/cancel/", data=None, extra_options=None
        )
        hook._paginate.assert_not_called()

    @pytest.mark.parametrize(
        argnames=("conn_id", "account_id"),
        argvalues=[(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    @patch.object(DbtCloudHook, "run")
    @patch.object(DbtCloudHook, "_paginate")
    def test_list_job_run_artifacts(self, mock_http_run, mock_paginate, conn_id, account_id):
        hook = DbtCloudHook(conn_id)
        hook.list_job_run_artifacts(run_id=RUN_ID, account_id=account_id)

        assert hook.method == "GET"

        _account_id = account_id or DEFAULT_ACCOUNT_ID
        hook.run.assert_called_once_with(
            endpoint=f"api/v2/accounts/{_account_id}/runs/{RUN_ID}/artifacts/",
            data={"step": None},
            extra_options=None,
        )
        hook._paginate.assert_not_called()

    @pytest.mark.parametrize(
        argnames=("conn_id", "account_id"),
        argvalues=[(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    @patch.object(DbtCloudHook, "run")
    @patch.object(DbtCloudHook, "_paginate")
    def test_list_job_run_artifacts_with_payload(self, mock_http_run, mock_paginate, conn_id, account_id):
        hook = DbtCloudHook(conn_id)
        hook.list_job_run_artifacts(run_id=RUN_ID, account_id=account_id, step=2)

        assert hook.method == "GET"

        _account_id = account_id or DEFAULT_ACCOUNT_ID
        hook.run.assert_called_once_with(
            endpoint=f"api/v2/accounts/{_account_id}/runs/{RUN_ID}/artifacts/",
            data={"step": 2},
            extra_options=None,
        )
        hook._paginate.assert_not_called()

    @pytest.mark.parametrize(
        argnames=("conn_id", "account_id"),
        argvalues=[(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    @patch.object(DbtCloudHook, "run")
    @patch.object(DbtCloudHook, "_paginate")
    def test_get_job_run_artifact(self, mock_http_run, mock_paginate, conn_id, account_id):
        hook = DbtCloudHook(conn_id)
        path = "manifest.json"
        hook.get_job_run_artifact(run_id=RUN_ID, path=path, account_id=account_id)

        assert hook.method == "GET"

        _account_id = account_id or DEFAULT_ACCOUNT_ID
        hook.run.assert_called_once_with(
            endpoint=f"api/v2/accounts/{_account_id}/runs/{RUN_ID}/artifacts/{path}",
            data={"step": None},
            extra_options=None,
        )
        hook._paginate.assert_not_called()

    @pytest.mark.parametrize(
        argnames=("conn_id", "account_id"),
        argvalues=[(ACCOUNT_ID_CONN, None), (NO_ACCOUNT_ID_CONN, ACCOUNT_ID)],
        ids=["default_account", "explicit_account"],
    )
    @patch.object(DbtCloudHook, "run")
    @patch.object(DbtCloudHook, "_paginate")
    def test_get_job_run_artifact_with_payload(self, mock_http_run, mock_paginate, conn_id, account_id):
        hook = DbtCloudHook(conn_id)
        path = "manifest.json"
        hook.get_job_run_artifact(run_id=RUN_ID, path="manifest.json", account_id=account_id, step=2)

        assert hook.method == "GET"

        _account_id = account_id or DEFAULT_ACCOUNT_ID
        hook.run.assert_called_once_with(
            endpoint=f"api/v2/accounts/{_account_id}/runs/{RUN_ID}/artifacts/{path}",
            data={"step": 2},
            extra_options=None,
        )
        hook._paginate.assert_not_called()

    @pytest.mark.parametrize(
        argnames="conn_id",
        argvalues=[ACCOUNT_ID_CONN, NO_ACCOUNT_ID_CONN],
        ids=["default_account", "explicit_account"],
    )
    def test_connection_success(self, requests_mock, conn_id):
        requests_mock.get(BASE_URL, status_code=200)
        status, msg = DbtCloudHook(conn_id).test_connection()

        assert status is True
        assert msg == "Successfully connected to dbt Cloud."

    @pytest.mark.parametrize(
        argnames="conn_id",
        argvalues=[ACCOUNT_ID_CONN, NO_ACCOUNT_ID_CONN],
        ids=["default_account", "explicit_account"],
    )
    def test_connection_failure(self, requests_mock, conn_id):
        requests_mock.get(BASE_URL, status_code=403, reason="Authentication credentials were not provided")
        status, msg = DbtCloudHook(conn_id).test_connection()

        assert status is False
        assert msg == "403:Authentication credentials were not provided"

    @pytest.mark.parametrize(
        argnames="timeout_seconds",
        argvalues=[60, 180, 300],
        ids=["60s", "180s", "300s"],
    )
    @patch.object(DbtCloudHook, "run_with_advanced_retry")
    def test_timeout_passed_to_run_and_get_response(self, mock_run_with_retry, timeout_seconds):
        """Test that timeout is passed to extra_options in _run_and_get_response."""
        hook = DbtCloudHook(ACCOUNT_ID_CONN, timeout_seconds=timeout_seconds)
        mock_run_with_retry.return_value = mock_response_json({"data": {"id": JOB_ID}})

        hook.get_job(job_id=JOB_ID, account_id=DEFAULT_ACCOUNT_ID)

        call_args = mock_run_with_retry.call_args
        assert call_args is not None
        extra_options = call_args.kwargs.get("extra_options")
        assert extra_options is not None
        assert extra_options["timeout"] == timeout_seconds

    @pytest.mark.parametrize(
        argnames="timeout_seconds",
        argvalues=[60, 180, 300],
        ids=["60s", "180s", "300s"],
    )
    @patch.object(DbtCloudHook, "run_with_advanced_retry")
    def test_timeout_passed_to_paginate(self, mock_run_with_retry, timeout_seconds):
        """Test that timeout is passed to extra_options in _paginate."""
        hook = DbtCloudHook(ACCOUNT_ID_CONN, timeout_seconds=timeout_seconds)
        mock_response = mock_response_json(
            {
                "data": [{"id": JOB_ID}],
                "extra": {"filters": {"limit": 100}, "pagination": {"count": 1, "total_count": 1}},
            }
        )
        mock_run_with_retry.return_value = mock_response

        hook.list_jobs(account_id=DEFAULT_ACCOUNT_ID)

        call_args = mock_run_with_retry.call_args
        assert call_args is not None
        extra_options = call_args.kwargs.get("extra_options")
        assert extra_options is not None
        assert extra_options["timeout"] == timeout_seconds

    @pytest.mark.parametrize(
        argnames="timeout_seconds",
        argvalues=[60, 180, 300],
        ids=["60s", "180s", "300s"],
    )
    @patch.object(DbtCloudHook, "run_with_advanced_retry")
    def test_timeout_with_proxies(self, mock_run_with_retry, timeout_seconds):
        """Test that both timeout and proxies are passed to extra_options."""
        hook = DbtCloudHook(PROXY_CONN, timeout_seconds=timeout_seconds)
        mock_run_with_retry.return_value = mock_response_json({"data": {"id": JOB_ID}})

        hook.get_job(job_id=JOB_ID, account_id=DEFAULT_ACCOUNT_ID)

        call_args = mock_run_with_retry.call_args
        assert call_args is not None
        extra_options = call_args.kwargs.get("extra_options")
        assert extra_options is not None
        assert extra_options["timeout"] == timeout_seconds
        assert "proxies" in extra_options
        assert extra_options["proxies"] == EXTRA_PROXIES["proxies"]

    @pytest.mark.parametrize(
        argnames=("exception", "expected"),
        argvalues=[
            (requests_exceptions.ConnectionError(), True),
            (requests_exceptions.Timeout(), True),
            (request_exception_with_status(503), True),
            (request_exception_with_status(429), True),
            (request_exception_with_status(404), False),
            (aiohttp.ClientResponseError(MagicMock(), (), status=500, message=""), True),
            (aiohttp.ClientResponseError(MagicMock(), (), status=429, message=""), True),
            (aiohttp.ClientResponseError(MagicMock(), (), status=400, message=""), False),
            (aiohttp.ClientConnectorError(MagicMock(), OSError()), True),
            (TimeoutError(), True),
            (ValueError(), False),
        ],
        ids=[
            "requests_connection_error",
            "requests_timeout",
            "requests_status_503",
            "requests_status_429",
            "requests_status_404",
            "aiohttp_status_500",
            "aiohttp_status_429",
            "aiohttp_status_400",
            "aiohttp_connector_error",
            "timeout_error",
            "value_error",
        ],
    )
    def test_retryable_error(self, exception, expected):
        assert DbtCloudHook._retryable_error(exception) is expected

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("error_factory", "retry_qty", "retry_delay"),
        [
            (
                lambda: aiohttp.ClientResponseError(
                    request_info=AsyncMock(), history=(), status=500, message=""
                ),
                3,
                0.1,
            ),
            (
                lambda: aiohttp.ClientResponseError(
                    request_info=AsyncMock(), history=(), status=429, message=""
                ),
                5,
                0.1,
            ),
            (lambda: aiohttp.ClientConnectorError(AsyncMock(), OSError("boom")), 2, 0.1),
            (lambda: TimeoutError(), 2, 0.1),
        ],
        ids=["aiohttp_500", "aiohttp_429", "connector_error", "timeout"],
    )
    @patch("airflow.providers.dbt.cloud.hooks.dbt.aiohttp.ClientSession.get")
    async def test_get_job_details_retry_with_retryable_errors(
        self, get_mock, error_factory, retry_qty, retry_delay
    ):
        hook = DbtCloudHook(ACCOUNT_ID_CONN, retry_limit=retry_qty, retry_delay=retry_delay)

        def fail_cm():
            cm = AsyncMock()
            cm.__aenter__.side_effect = error_factory()
            return cm

        ok_resp = AsyncMock()
        ok_resp.raise_for_status = MagicMock(return_value=None)
        ok_resp.json = AsyncMock(return_value={"data": "Success"})
        ok_cm = AsyncMock()
        ok_cm.__aenter__.return_value = ok_resp
        ok_cm.__aexit__.return_value = AsyncMock()

        all_resp = [fail_cm() for _ in range(retry_qty - 1)]
        all_resp.append(ok_cm)
        get_mock.side_effect = all_resp

        result = await hook.get_job_details(run_id=RUN_ID, account_id=None)

        assert result == {"data": "Success"}
        assert get_mock.call_count == retry_qty

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("error_factory", "expected_exception"),
        [
            (
                lambda: aiohttp.ClientResponseError(
                    request_info=AsyncMock(), history=(), status=404, message="Not Found"
                ),
                aiohttp.ClientResponseError,
            ),
            (
                lambda: aiohttp.ClientResponseError(
                    request_info=AsyncMock(), history=(), status=400, message="Bad Request"
                ),
                aiohttp.ClientResponseError,
            ),
            (lambda: ValueError("Invalid parameter"), ValueError),
        ],
        ids=["aiohttp_404", "aiohttp_400", "value_error"],
    )
    @patch("airflow.providers.dbt.cloud.hooks.dbt.aiohttp.ClientSession.get")
    async def test_get_job_details_retry_with_non_retryable_errors(
        self, get_mock, error_factory, expected_exception
    ):
        hook = DbtCloudHook(ACCOUNT_ID_CONN, retry_limit=3, retry_delay=0.1)

        def fail_cm():
            cm = AsyncMock()
            cm.__aenter__.side_effect = error_factory()
            return cm

        get_mock.return_value = fail_cm()

        with pytest.raises(expected_exception):
            await hook.get_job_details(run_id=RUN_ID, account_id=None)

        assert get_mock.call_count == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        argnames=("error_factory", "expected_exception"),
        argvalues=[
            (
                lambda: aiohttp.ClientResponseError(
                    request_info=AsyncMock(), history=(), status=503, message="Service Unavailable"
                ),
                aiohttp.ClientResponseError,
            ),
            (
                lambda: aiohttp.ClientResponseError(
                    request_info=AsyncMock(), history=(), status=500, message="Internal Server Error"
                ),
                aiohttp.ClientResponseError,
            ),
            (
                lambda: aiohttp.ClientConnectorError(AsyncMock(), OSError("Connection refused")),
                aiohttp.ClientConnectorError,
            ),
            (lambda: TimeoutError("Request timeout"), TimeoutError),
        ],
        ids=[
            "aiohttp_503_exhausted",
            "aiohttp_500_exhausted",
            "connector_error_exhausted",
            "timeout_exhausted",
        ],
    )
    @patch("airflow.providers.dbt.cloud.hooks.dbt.aiohttp.ClientSession.get")
    async def test_get_job_details_retry_with_exhausted_retries(
        self, get_mock, error_factory, expected_exception
    ):
        hook = DbtCloudHook(ACCOUNT_ID_CONN, retry_limit=2, retry_delay=0.1)

        def fail_cm():
            cm = AsyncMock()
            cm.__aenter__.side_effect = error_factory()
            return cm

        get_mock.side_effect = [fail_cm() for _ in range(2)]

        with pytest.raises(expected_exception):
            await hook.get_job_details(run_id=RUN_ID, account_id=None)

        assert get_mock.call_count == 2
