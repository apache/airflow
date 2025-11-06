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

import base64
import json
import os
import platform
import tempfile
from unittest import mock
from unittest.mock import PropertyMock, call, mock_open
from urllib.parse import parse_qsl, unquote, urlsplit

import aiohttp
import httplib2
import pytest
from aiohttp.helpers import TimerNoop
from googleapiclient.errors import HttpError
from yarl import URL

from airflow.exceptions import AirflowException

from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS

if AIRFLOW_V_3_1_PLUS:
    from airflow.sdk import Connection
else:
    from airflow.models import Connection  # type: ignore[assignment,attr-defined,no-redef]
from airflow.providers.google.cloud.hooks.cloud_sql import (
    CloudSQLAsyncHook,
    CloudSQLDatabaseHook,
    CloudSQLHook,
    CloudSqlProxyRunner,
)

from unit.google.cloud.utils.base_gcp_mock import (
    mock_base_gcp_hook_default_project_id,
    mock_base_gcp_hook_no_default_project_id,
)

HOOK_STR = "airflow.providers.google.cloud.hooks.cloud_sql.{}"
PROJECT_ID = "test_project_id"
OPERATION_NAME = "test_operation_name"
OPERATION_URL = (
    f"https://sqladmin.googleapis.com/sql/v1beta4/projects/{PROJECT_ID}/operations/{OPERATION_NAME}"
)
SSL_CERT = "sslcert.pem"
SSL_KEY = "sslkey.pem"
SSL_ROOT_CERT = "sslrootcert.pem"
CONNECTION_ID = "test-conn-id"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]
SECRET_ID = "test-secret-id"


@pytest.fixture
def hook_async():
    with mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseAsyncHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    ):
        yield CloudSQLAsyncHook()


def session():
    return mock.Mock()


class TestGcpSqlHookDefaultProjectId:
    def setup_method(self):
        with mock.patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.cloudsql_hook = CloudSQLHook(api_version="v1", gcp_conn_id="test")

    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_credentials_and_project_id",
        return_value=(mock.MagicMock(), "example-project"),
    )
    def test_instance_import_exception(self, mock_get_credentials):
        self.cloudsql_hook.get_conn = mock.Mock(
            side_effect=HttpError(resp=httplib2.Response({"status": 400}), content=b"Error content")
        )
        with pytest.raises(AirflowException) as ctx:
            self.cloudsql_hook.import_instance(instance="instance", body={})
        err = ctx.value
        assert "Importing instance " in str(err)
        assert mock_get_credentials.call_count == 1

    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_credentials_and_project_id",
        return_value=(mock.MagicMock(), "example-project"),
    )
    def test_instance_export_exception(self, mock_get_credentials):
        self.cloudsql_hook.get_conn = mock.Mock(
            side_effect=HttpError(resp=httplib2.Response({"status": 400}), content=b"Error content")
        )
        with pytest.raises(HttpError) as ctx:
            self.cloudsql_hook.export_instance(instance="instance", body={})
        err = ctx.value
        assert err.resp.status == 400
        assert mock_get_credentials.call_count == 1

    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_credentials_and_project_id",
        return_value=(mock.MagicMock(), "example-project"),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook._wait_for_operation_to_complete")
    def test_instance_import(self, wait_for_operation_to_complete, get_conn, mock_get_credentials):
        import_method = get_conn.return_value.instances.return_value.import_
        execute_method = import_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        self.cloudsql_hook.import_instance(instance="instance", body={})

        import_method.assert_called_once_with(body={}, instance="instance", project="example-project")
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            project_id="example-project", operation_name="operation_id"
        )
        assert mock_get_credentials.call_count == 1

    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_credentials_and_project_id",
        return_value=(mock.MagicMock(), "example-project"),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook._wait_for_operation_to_complete")
    def test_instance_export(self, wait_for_operation_to_complete, get_conn, mock_get_credentials):
        export_method = get_conn.return_value.instances.return_value.export
        execute_method = export_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        self.cloudsql_hook.export_instance(instance="instance", body={})

        export_method.assert_called_once_with(body={}, instance="instance", project="example-project")
        execute_method.assert_called_once_with(num_retries=5)
        assert mock_get_credentials.call_count == 1

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook._wait_for_operation_to_complete")
    def test_instance_export_with_in_progress_retry(self, wait_for_operation_to_complete, get_conn):
        export_method = get_conn.return_value.instances.return_value.export
        execute_method = export_method.return_value.execute
        execute_method.side_effect = [
            HttpError(
                resp=httplib2.Response({"status": 429}),
                content=b"Internal Server Error",
            ),
            {"name": "operation_id"},
        ]
        with pytest.raises(HttpError):
            self.cloudsql_hook.export_instance(project_id="example-project", instance="instance", body={})
        wait_for_operation_to_complete.assert_not_called()

    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_credentials_and_project_id",
        return_value=(mock.MagicMock(), "example-project"),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook._wait_for_operation_to_complete")
    def test_get_instance(self, wait_for_operation_to_complete, get_conn, mock_get_credentials):
        get_method = get_conn.return_value.instances.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = {"name": "instance"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook.get_instance(instance="instance")
        assert res is not None
        assert res["name"] == "instance"
        get_method.assert_called_once_with(instance="instance", project="example-project")
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_not_called()
        assert mock_get_credentials.call_count == 1

    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_credentials_and_project_id",
        return_value=(mock.MagicMock(), "example-project"),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook._wait_for_operation_to_complete")
    def test_create_instance(self, wait_for_operation_to_complete, get_conn, mock_get_credentials):
        insert_method = get_conn.return_value.instances.return_value.insert
        execute_method = insert_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        self.cloudsql_hook.create_instance(body={})

        insert_method.assert_called_once_with(body={}, project="example-project")
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name="operation_id", project_id="example-project"
        )
        assert mock_get_credentials.call_count == 1

    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_credentials_and_project_id",
        return_value=(mock.MagicMock(), "example-project"),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook._wait_for_operation_to_complete")
    def test_create_instance_with_in_progress_retry(
        self, wait_for_operation_to_complete, get_conn, mock_get_credentials
    ):
        insert_method = get_conn.return_value.instances.return_value.insert
        execute_method = insert_method.return_value.execute
        execute_method.side_effect = [
            HttpError(
                resp=httplib2.Response({"status": 429}),
                content=b"Internal Server Error",
            ),
            {"name": "operation_id"},
        ]
        wait_for_operation_to_complete.return_value = None
        self.cloudsql_hook.create_instance(body={})

        assert mock_get_credentials.call_count == 1
        assert insert_method.call_count == 2
        assert execute_method.call_count == 2
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name="operation_id", project_id="example-project"
        )

    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_credentials_and_project_id",
        return_value=(mock.MagicMock(), "example-project"),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook._wait_for_operation_to_complete")
    def test_patch_instance_with_in_progress_retry(
        self, wait_for_operation_to_complete, get_conn, mock_get_credentials
    ):
        patch_method = get_conn.return_value.instances.return_value.patch
        execute_method = patch_method.return_value.execute
        execute_method.side_effect = [
            HttpError(
                resp=httplib2.Response({"status": 429}),
                content=b"Internal Server Error",
            ),
            {"name": "operation_id"},
        ]
        wait_for_operation_to_complete.return_value = None
        self.cloudsql_hook.patch_instance(instance="instance", body={})

        assert mock_get_credentials.call_count == 1
        assert patch_method.call_count == 2
        assert execute_method.call_count == 2
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name="operation_id", project_id="example-project"
        )

    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_credentials_and_project_id",
        return_value=(mock.MagicMock(), "example-project"),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook._wait_for_operation_to_complete")
    def test_patch_instance(self, wait_for_operation_to_complete, get_conn, mock_get_credentials):
        patch_method = get_conn.return_value.instances.return_value.patch
        execute_method = patch_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        self.cloudsql_hook.patch_instance(instance="instance", body={})

        patch_method.assert_called_once_with(body={}, instance="instance", project="example-project")
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name="operation_id", project_id="example-project"
        )
        assert mock_get_credentials.call_count == 1

    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_credentials_and_project_id",
        return_value=(mock.MagicMock(), "example-project"),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook._wait_for_operation_to_complete")
    def test_delete_instance(self, wait_for_operation_to_complete, get_conn, mock_get_credentials):
        delete_method = get_conn.return_value.instances.return_value.delete
        execute_method = delete_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        self.cloudsql_hook.delete_instance(instance="instance")

        delete_method.assert_called_once_with(instance="instance", project="example-project")
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name="operation_id", project_id="example-project", time_to_sleep=5
        )
        assert mock_get_credentials.call_count == 1

    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_credentials_and_project_id",
        return_value=(mock.MagicMock(), "example-project"),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook._wait_for_operation_to_complete")
    def test_delete_instance_with_in_progress_retry(
        self, wait_for_operation_to_complete, get_conn, mock_get_credentials
    ):
        delete_method = get_conn.return_value.instances.return_value.delete
        execute_method = delete_method.return_value.execute
        execute_method.side_effect = [
            HttpError(
                resp=httplib2.Response({"status": 429}),
                content=b"Internal Server Error",
            ),
            {"name": "operation_id"},
        ]
        wait_for_operation_to_complete.return_value = None
        self.cloudsql_hook.delete_instance(instance="instance")

        assert mock_get_credentials.call_count == 1
        assert delete_method.call_count == 2
        assert execute_method.call_count == 2
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name="operation_id", project_id="example-project", time_to_sleep=5
        )

    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_credentials_and_project_id",
        return_value=(mock.MagicMock(), "example-project"),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook._wait_for_operation_to_complete")
    def test_instance_clone(self, wait_for_operation_to_complete, get_conn, mock_get_credentials):
        clone_method = get_conn.return_value.instances.return_value.clone
        execute_method = clone_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        body = {
            "cloneContext": {
                "kind": "sql#cloneContext",
                "destinationInstanceName": "clonedInstance",
            }
        }
        self.cloudsql_hook.clone_instance(instance="instance", body=body)

        clone_method.assert_called_once_with(instance="instance", project="example-project", body=body)
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name="operation_id", project_id="example-project"
        )
        assert mock_get_credentials.call_count == 1

    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_credentials_and_project_id",
        return_value=(mock.MagicMock(), "example-project"),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook._wait_for_operation_to_complete")
    def test_get_database(self, wait_for_operation_to_complete, get_conn, mock_get_credentials):
        get_method = get_conn.return_value.databases.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = {"name": "database"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook.get_database(database="database", instance="instance")
        assert res is not None
        assert res["name"] == "database"
        get_method.assert_called_once_with(
            instance="instance", database="database", project="example-project"
        )
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_not_called()
        assert mock_get_credentials.call_count == 1

    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_credentials_and_project_id",
        return_value=(mock.MagicMock(), "example-project"),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook._wait_for_operation_to_complete")
    def test_create_database(self, wait_for_operation_to_complete, get_conn, mock_get_credentials):
        insert_method = get_conn.return_value.databases.return_value.insert
        execute_method = insert_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        self.cloudsql_hook.create_database(instance="instance", body={})

        insert_method.assert_called_once_with(body={}, instance="instance", project="example-project")
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name="operation_id", project_id="example-project"
        )
        assert mock_get_credentials.call_count == 1

    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_credentials_and_project_id",
        return_value=(mock.MagicMock(), "example-project"),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook._wait_for_operation_to_complete")
    def test_create_database_with_in_progress_retry(
        self, wait_for_operation_to_complete, get_conn, mock_get_credentials
    ):
        insert_method = get_conn.return_value.databases.return_value.insert
        execute_method = insert_method.return_value.execute
        execute_method.side_effect = [
            HttpError(
                resp=httplib2.Response({"status": 429}),
                content=b"Internal Server Error",
            ),
            {"name": "operation_id"},
        ]
        wait_for_operation_to_complete.return_value = None
        self.cloudsql_hook.create_database(instance="instance", body={})

        assert mock_get_credentials.call_count == 1
        assert insert_method.call_count == 2
        assert execute_method.call_count == 2
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name="operation_id", project_id="example-project"
        )

    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_credentials_and_project_id",
        return_value=(mock.MagicMock(), "example-project"),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook._wait_for_operation_to_complete")
    def test_patch_database(self, wait_for_operation_to_complete, get_conn, mock_get_credentials):
        patch_method = get_conn.return_value.databases.return_value.patch
        execute_method = patch_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        self.cloudsql_hook.patch_database(instance="instance", database="database", body={})

        patch_method.assert_called_once_with(
            body={}, database="database", instance="instance", project="example-project"
        )
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name="operation_id", project_id="example-project"
        )
        assert mock_get_credentials.call_count == 1

    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_credentials_and_project_id",
        return_value=(mock.MagicMock(), "example-project"),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook._wait_for_operation_to_complete")
    def test_patch_database_with_in_progress_retry(
        self, wait_for_operation_to_complete, get_conn, mock_get_credentials
    ):
        patch_method = get_conn.return_value.databases.return_value.patch
        execute_method = patch_method.return_value.execute
        execute_method.side_effect = [
            HttpError(
                resp=httplib2.Response({"status": 429}),
                content=b"Internal Server Error",
            ),
            {"name": "operation_id"},
        ]
        wait_for_operation_to_complete.return_value = None
        self.cloudsql_hook.patch_database(instance="instance", database="database", body={})

        assert mock_get_credentials.call_count == 1
        assert patch_method.call_count == 2
        assert execute_method.call_count == 2
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name="operation_id", project_id="example-project"
        )

    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_credentials_and_project_id",
        return_value=(mock.MagicMock(), "example-project"),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook._wait_for_operation_to_complete")
    def test_delete_database(self, wait_for_operation_to_complete, get_conn, mock_get_credentials):
        delete_method = get_conn.return_value.databases.return_value.delete
        execute_method = delete_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        self.cloudsql_hook.delete_database(instance="instance", database="database")

        delete_method.assert_called_once_with(
            database="database", instance="instance", project="example-project"
        )
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name="operation_id", project_id="example-project"
        )
        assert mock_get_credentials.call_count == 1

    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_credentials_and_project_id",
        return_value=(mock.MagicMock(), "example-project"),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook._wait_for_operation_to_complete")
    def test_delete_database_with_in_progress_retry(
        self, wait_for_operation_to_complete, get_conn, mock_get_credentials
    ):
        delete_method = get_conn.return_value.databases.return_value.delete
        execute_method = delete_method.return_value.execute
        execute_method.side_effect = [
            HttpError(
                resp=httplib2.Response({"status": 429}),
                content=b"Internal Server Error",
            ),
            {"name": "operation_id"},
        ]
        wait_for_operation_to_complete.return_value = None
        self.cloudsql_hook.delete_database(instance="instance", database="database")

        assert mock_get_credentials.call_count == 1
        assert delete_method.call_count == 2
        assert execute_method.call_count == 2
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name="operation_id", project_id="example-project"
        )


class TestGcpSqlHookNoDefaultProjectID:
    def setup_method(self):
        with mock.patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_no_default_project_id,
        ):
            self.cloudsql_hook_no_default_project_id = CloudSQLHook(api_version="v1", gcp_conn_id="test")

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook._wait_for_operation_to_complete")
    def test_instance_import_overridden_project_id(
        self, wait_for_operation_to_complete, get_conn, mock_project_id
    ):
        import_method = get_conn.return_value.instances.return_value.import_
        execute_method = import_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        self.cloudsql_hook_no_default_project_id.import_instance(
            project_id="example-project", instance="instance", body={}
        )
        import_method.assert_called_once_with(body={}, instance="instance", project="example-project")
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            project_id="example-project", operation_name="operation_id"
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook._wait_for_operation_to_complete")
    def test_instance_export_overridden_project_id(
        self, wait_for_operation_to_complete, get_conn, mock_project_id
    ):
        export_method = get_conn.return_value.instances.return_value.export
        execute_method = export_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        self.cloudsql_hook_no_default_project_id.export_instance(
            project_id="example-project", instance="instance", body={}
        )
        export_method.assert_called_once_with(body={}, instance="instance", project="example-project")
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook._wait_for_operation_to_complete")
    def test_get_instance_overridden_project_id(
        self, wait_for_operation_to_complete, get_conn, mock_project_id
    ):
        get_method = get_conn.return_value.instances.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = {"name": "instance"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook_no_default_project_id.get_instance(
            project_id="example-project", instance="instance"
        )
        assert res is not None
        assert res["name"] == "instance"
        get_method.assert_called_once_with(instance="instance", project="example-project")
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_not_called()

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook._wait_for_operation_to_complete")
    def test_create_instance_overridden_project_id(
        self, wait_for_operation_to_complete, get_conn, mock_project_id
    ):
        insert_method = get_conn.return_value.instances.return_value.insert
        execute_method = insert_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        self.cloudsql_hook_no_default_project_id.create_instance(project_id="example-project", body={})
        insert_method.assert_called_once_with(body={}, project="example-project")
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name="operation_id", project_id="example-project"
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook._wait_for_operation_to_complete")
    def test_patch_instance_overridden_project_id(
        self, wait_for_operation_to_complete, get_conn, mock_project_id
    ):
        patch_method = get_conn.return_value.instances.return_value.patch
        execute_method = patch_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        self.cloudsql_hook_no_default_project_id.patch_instance(
            project_id="example-project", instance="instance", body={}
        )
        patch_method.assert_called_once_with(body={}, instance="instance", project="example-project")
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name="operation_id", project_id="example-project"
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook._wait_for_operation_to_complete")
    def test_delete_instance_overridden_project_id(
        self, wait_for_operation_to_complete, get_conn, mock_project_id
    ):
        delete_method = get_conn.return_value.instances.return_value.delete
        execute_method = delete_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        self.cloudsql_hook_no_default_project_id.delete_instance(
            project_id="example-project", instance="instance"
        )
        delete_method.assert_called_once_with(instance="instance", project="example-project")
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name="operation_id", project_id="example-project", time_to_sleep=5
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook._wait_for_operation_to_complete")
    def test_get_database_overridden_project_id(
        self, wait_for_operation_to_complete, get_conn, mock_project_id
    ):
        get_method = get_conn.return_value.databases.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = {"name": "database"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook_no_default_project_id.get_database(
            project_id="example-project", database="database", instance="instance"
        )
        assert res is not None
        assert res["name"] == "database"
        get_method.assert_called_once_with(
            instance="instance", database="database", project="example-project"
        )
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_not_called()

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook._wait_for_operation_to_complete")
    def test_create_database_overridden_project_id(
        self, wait_for_operation_to_complete, get_conn, mock_project_id
    ):
        insert_method = get_conn.return_value.databases.return_value.insert
        execute_method = insert_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        self.cloudsql_hook_no_default_project_id.create_database(
            project_id="example-project", instance="instance", body={}
        )
        insert_method.assert_called_once_with(body={}, instance="instance", project="example-project")
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name="operation_id", project_id="example-project"
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook._wait_for_operation_to_complete")
    def test_patch_database_overridden_project_id(
        self, wait_for_operation_to_complete, get_conn, mock_project_id
    ):
        patch_method = get_conn.return_value.databases.return_value.patch
        execute_method = patch_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        self.cloudsql_hook_no_default_project_id.patch_database(
            project_id="example-project", instance="instance", database="database", body={}
        )
        patch_method.assert_called_once_with(
            body={}, database="database", instance="instance", project="example-project"
        )
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name="operation_id", project_id="example-project"
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook._wait_for_operation_to_complete")
    def test_delete_database_overridden_project_id(
        self, wait_for_operation_to_complete, get_conn, mock_project_id
    ):
        delete_method = get_conn.return_value.databases.return_value.delete
        execute_method = delete_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        self.cloudsql_hook_no_default_project_id.delete_database(
            project_id="example-project", instance="instance", database="database"
        )
        delete_method.assert_called_once_with(
            database="database", instance="instance", project="example-project"
        )
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name="operation_id", project_id="example-project"
        )


def _parse_from_uri(uri: str):
    connection_parameters = {}
    uri_parts = urlsplit(uri)
    connection_parameters["conn_type"] = uri_parts.scheme
    rest_of_the_url = uri.replace(f"{uri_parts.scheme}://", "//")
    uri_parts = urlsplit(rest_of_the_url)
    host = unquote(uri_parts.hostname or "")
    connection_parameters["host"] = host
    quoted_schema = uri_parts.path[1:]
    connection_parameters["schema"] = unquote(quoted_schema) if quoted_schema else ""
    connection_parameters["login"] = unquote(uri_parts.username) if uri_parts.username else ""
    connection_parameters["password"] = unquote(uri_parts.password) if uri_parts.password else ""
    connection_parameters["port"] = uri_parts.port  # type: ignore[assignment]
    if uri_parts.query:
        query = dict(parse_qsl(uri_parts.query, keep_blank_values=True))
        connection_parameters["extra"] = json.dumps(query)
    return connection_parameters


class TestCloudSqlDatabaseHook:
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_cloudsql_database_hook_validate_ssl_certs_no_ssl(self, get_connection):
        connection = Connection(
            conn_id="my_test_connection",
            conn_type="gcpcloudsqldb",
        )
        if AIRFLOW_V_3_1_PLUS:
            connection.extra = json.dumps(
                {"location": "test", "instance": "instance", "database_type": "postgres"}
            )
        else:
            connection.set_extra(
                json.dumps({"location": "test", "instance": "instance", "database_type": "postgres"})
            )
        get_connection.return_value = connection
        hook = CloudSQLDatabaseHook(
            gcp_cloudsql_conn_id="cloudsql_connection", default_gcp_project_id="google_connection"
        )
        hook.validate_ssl_certs()

    @pytest.mark.parametrize(
        "cert_dict",
        [
            {},
            {"sslcert": "cert_file.pem"},
            {"sslkey": "cert_key.pem"},
            {"sslrootcert": "root_cert_file.pem"},
            {"sslcert": "cert_file.pem", "sslkey": "cert_key.pem"},
            {"sslrootcert": "root_cert_file.pem", "sslkey": "cert_key.pem"},
            {"sslrootcert": "root_cert_file.pem", "sslcert": "cert_file.pem"},
        ],
    )
    @mock.patch("os.path.isfile")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook._set_temporary_ssl_file")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_cloudsql_database_hook_validate_ssl_certs_missing_cert_params(
        self, get_connection, mock_set_temporary_ssl_file, mock_is_file, cert_dict
    ):
        mock_is_file.side_effects = True
        mock_set_temporary_ssl_file.side_effect = cert_dict.values()
        connection = Connection(
            conn_id="my_test_connection",
            conn_type="gcpcloudsqldb",
        )
        extras = {"location": "test", "instance": "instance", "database_type": "postgres", "use_ssl": "True"}
        extras.update(cert_dict)
        if AIRFLOW_V_3_1_PLUS:
            connection.extra = json.dumps(extras)
        else:
            connection.set_extra(json.dumps(extras))

        get_connection.return_value = connection
        hook = CloudSQLDatabaseHook(
            gcp_cloudsql_conn_id="cloudsql_connection", default_gcp_project_id="google_connection"
        )
        with pytest.raises(AirflowException) as ctx:
            hook.validate_ssl_certs()
        err = ctx.value
        assert "SSL connections requires" in str(err)

    @mock.patch("os.path.isfile")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook._set_temporary_ssl_file")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_cloudsql_database_hook_validate_ssl_certs_with_ssl(
        self, get_connection, mock_set_temporary_ssl_file, mock_is_file
    ):
        connection = Connection(
            conn_id="my_test_connection",
            conn_type="gcpcloudsqldb",
        )
        mock_is_file.return_value = True
        mock_set_temporary_ssl_file.side_effect = [
            "/tmp/cert_file.pem",
            "/tmp/rootcert_file.pem",
            "/tmp/key_file.pem",
        ]
        extras = json.dumps(
            {
                "location": "test",
                "instance": "instance",
                "database_type": "postgres",
                "use_ssl": "True",
                "sslcert": "cert_file.pem",
                "sslrootcert": "rootcert_file.pem",
                "sslkey": "key_file.pem",
            }
        )
        if AIRFLOW_V_3_1_PLUS:
            connection.extra = extras
        else:
            connection.set_extra(extras)
        get_connection.return_value = connection
        hook = CloudSQLDatabaseHook(
            gcp_cloudsql_conn_id="cloudsql_connection", default_gcp_project_id="google_connection"
        )
        hook.validate_ssl_certs()

    @mock.patch("os.path.isfile")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook._set_temporary_ssl_file")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_cloudsql_database_hook_validate_ssl_certs_with_ssl_files_not_readable(
        self, get_connection, mock_set_temporary_ssl_file, mock_is_file
    ):
        connection = Connection(
            conn_id="my_test_connection",
            conn_type="gcpcloudsqldb",
        )
        mock_is_file.return_value = False
        mock_set_temporary_ssl_file.side_effect = [
            "/tmp/cert_file.pem",
            "/tmp/rootcert_file.pem",
            "/tmp/key_file.pem",
        ]
        extras = json.dumps(
            {
                "location": "test",
                "instance": "instance",
                "database_type": "postgres",
                "use_ssl": "True",
                "sslcert": "cert_file.pem",
                "sslrootcert": "rootcert_file.pem",
                "sslkey": "key_file.pem",
            }
        )
        if AIRFLOW_V_3_1_PLUS:
            connection.extra = extras
        else:
            connection.set_extra(extras)
        get_connection.return_value = connection
        hook = CloudSQLDatabaseHook(
            gcp_cloudsql_conn_id="cloudsql_connection", default_gcp_project_id="google_connection"
        )
        with pytest.raises(AirflowException) as ctx:
            hook.validate_ssl_certs()
        err = ctx.value
        assert "must be a readable file" in str(err)

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.gettempdir")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_cloudsql_database_hook_validate_socket_path_length_too_long(
        self, get_connection, gettempdir_mock
    ):
        gettempdir_mock.return_value = "/tmp"
        connection = Connection(
            conn_id="my_test_connection",
            conn_type="gcpcloudsqldb",
        )
        extras = json.dumps(
            {
                "location": "test",
                "instance": "very_long_instance_name_that_will_be_too_long_to_build_socket_length",
                "database_type": "postgres",
                "use_proxy": "True",
                "use_tcp": "False",
            }
        )
        if AIRFLOW_V_3_1_PLUS:
            connection.extra = extras
        else:
            connection.set_extra(extras)
        get_connection.return_value = connection
        hook = CloudSQLDatabaseHook(
            gcp_cloudsql_conn_id="cloudsql_connection", default_gcp_project_id="google_connection"
        )
        with pytest.raises(AirflowException) as ctx:
            hook.validate_socket_path_length()
        err = ctx.value
        assert "The UNIX socket path length cannot exceed" in str(err)

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.gettempdir")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_cloudsql_database_hook_validate_socket_path_length_not_too_long(
        self, get_connection, gettempdir_mock
    ):
        gettempdir_mock.return_value = "/tmp"
        connection = Connection(
            conn_id="my_test_connection",
            conn_type="gcpcloudsqldb",
        )
        extras = json.dumps(
            {
                "location": "test",
                "instance": "short_instance_name",
                "database_type": "postgres",
                "use_proxy": "True",
                "use_tcp": "False",
            }
        )
        if AIRFLOW_V_3_1_PLUS:
            connection.extra = extras
        else:
            connection.set_extra(extras)
        get_connection.return_value = connection
        hook = CloudSQLDatabaseHook(
            gcp_cloudsql_conn_id="cloudsql_connection", default_gcp_project_id="google_connection"
        )
        hook.validate_socket_path_length()

    @pytest.mark.parametrize(
        "uri",
        [
            "http://:password@host:80/database",
            "http://user:@host:80/database",
            "http://user:password@/database",
            "http://user:password@host:80/",
            "http://user:password@/",
            "http://host:80/database",
            "http://host:80/",
        ],
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_cloudsql_database_hook_create_connection_missing_fields(self, get_connection, uri):
        if AIRFLOW_V_3_1_PLUS:
            connection = Connection(conn_id="test_conn_id", **_parse_from_uri(uri))
        else:
            connection = Connection(uri=uri)
        params = {
            "location": "test",
            "instance": "instance",
            "database_type": "postgres",
            "use_proxy": "True",
            "use_tcp": "False",
        }
        extras = json.dumps(params)
        if AIRFLOW_V_3_1_PLUS:
            connection.extra = extras
        else:
            connection.set_extra(extras)
        get_connection.return_value = connection
        hook = CloudSQLDatabaseHook(
            gcp_cloudsql_conn_id="cloudsql_connection", default_gcp_project_id="google_connection"
        )
        with pytest.raises(AirflowException) as ctx:
            hook.create_connection()
        err = ctx.value
        assert "needs to be set in connection" in str(err)

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_cloudsql_database_hook_get_sqlproxy_runner_no_proxy(self, get_connection):
        if AIRFLOW_V_3_1_PLUS:
            connection = Connection(
                conn_id="test_conn_id", **_parse_from_uri("http://user:password@host:80/database")
            )
        else:
            connection = Connection(uri="http://user:password@host:80/database")
        extras = json.dumps(
            {
                "location": "test",
                "instance": "instance",
                "database_type": "postgres",
            }
        )
        if AIRFLOW_V_3_1_PLUS:
            connection.extra = extras
        else:
            connection.set_extra(extras)
        get_connection.return_value = connection
        hook = CloudSQLDatabaseHook(
            gcp_cloudsql_conn_id="cloudsql_connection", default_gcp_project_id="google_connection"
        )
        with pytest.raises(
            ValueError, match="Proxy runner can only be retrieved in case of use_proxy = True"
        ):
            hook.get_sqlproxy_runner()

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_cloudsql_database_hook_get_sqlproxy_runner(self, get_connection):
        if AIRFLOW_V_3_1_PLUS:
            connection = Connection(
                conn_id="test_conn_id", **_parse_from_uri("http://user:password@host:80/database")
            )
        else:
            connection = Connection(uri="http://user:password@host:80/database")
        extras = json.dumps(
            {
                "location": "test",
                "instance": "instance",
                "database_type": "postgres",
                "use_proxy": "True",
                "use_tcp": "False",
            }
        )
        if AIRFLOW_V_3_1_PLUS:
            connection.extra = extras
        else:
            connection.set_extra(extras)
        get_connection.return_value = connection
        hook = CloudSQLDatabaseHook(
            gcp_cloudsql_conn_id="cloudsql_connection", default_gcp_project_id="google_connection"
        )
        hook.create_connection()
        proxy_runner = hook.get_sqlproxy_runner()
        assert proxy_runner is not None

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_cloudsql_database_hook_get_database_hook(self, get_connection):
        if AIRFLOW_V_3_1_PLUS:
            connection = Connection(
                conn_id="test_conn_id", **_parse_from_uri("http://user:password@host:80/database")
            )
        else:
            connection = Connection(uri="http://user:password@host:80/database")
        extras = json.dumps(
            {
                "location": "test",
                "instance": "instance",
                "database_type": "postgres",
            }
        )
        if AIRFLOW_V_3_1_PLUS:
            connection.extra = extras
        else:
            connection.set_extra(extras)
        get_connection.return_value = connection
        hook = CloudSQLDatabaseHook(
            gcp_cloudsql_conn_id="cloudsql_connection", default_gcp_project_id="google_connection"
        )
        connection = hook.create_connection()
        db_hook = hook.get_database_hook(connection=connection)
        assert db_hook is not None

    @mock.patch(HOOK_STR.format("CloudSQLDatabaseHook._get_ssl_temporary_file_path"))
    @mock.patch(HOOK_STR.format("CloudSQLDatabaseHook.get_connection"))
    def test_ssl_cert_properties(self, mock_get_connection, mock_get_ssl_temporary_file_path):
        def side_effect_func(cert_name, cert_path):
            return f"/tmp/certs/{cert_name}"

        mock_get_ssl_temporary_file_path.side_effect = side_effect_func
        mock_get_connection.return_value = mock.MagicMock(
            extra_dejson=dict(database_type="postgres", location="test", instance="instance")
        )

        hook = CloudSQLDatabaseHook(
            gcp_cloudsql_conn_id="cloudsql_connection",
            default_gcp_project_id="google_connection",
            ssl_cert=SSL_CERT,
            ssl_key=SSL_KEY,
            ssl_root_cert=SSL_ROOT_CERT,
        )
        sslcert = hook.sslcert
        sslkey = hook.sslkey
        sslrootcert = hook.sslrootcert

        assert hook.ssl_cert == SSL_CERT
        assert hook.ssl_key == SSL_KEY
        assert hook.ssl_root_cert == SSL_ROOT_CERT
        assert sslcert == "/tmp/certs/sslcert"
        assert sslkey == "/tmp/certs/sslkey"
        assert sslrootcert == "/tmp/certs/sslrootcert"
        mock_get_ssl_temporary_file_path.assert_has_calls(
            [
                call(cert_name="sslcert", cert_path=SSL_CERT),
                call(cert_name="sslkey", cert_path=SSL_KEY),
                call(cert_name="sslrootcert", cert_path=SSL_ROOT_CERT),
            ]
        )

    @pytest.mark.parametrize("ssl_name", ["sslcert", "sslkey", "sslrootcert"])
    @pytest.mark.parametrize(
        ("cert_value", "cert_path", "extra_cert_path"),
        [
            (None, None, "/connection/path/to/cert.pem"),
            (None, "/path/to/cert.pem", None),
            (None, "/path/to/cert.pem", "/connection/path/to/cert.pem"),
            (mock.MagicMock(), None, None),
            (mock.MagicMock(), None, "/connection/path/to/cert.pem"),
            (mock.MagicMock(), "/path/to/cert.pem", None),
            (mock.MagicMock(), "/path/to/cert.pem", "/connection/path/to/cert.pem"),
        ],
    )
    @mock.patch(HOOK_STR.format("CloudSQLDatabaseHook._set_temporary_ssl_file"))
    @mock.patch(HOOK_STR.format("CloudSQLDatabaseHook._get_cert_from_secret"))
    @mock.patch(HOOK_STR.format("CloudSQLDatabaseHook.get_connection"))
    def test_get_ssl_temporary_file_path(
        self,
        mock_get_connection,
        mock_get_cert_from_secret,
        mock_set_temporary_ssl_file,
        cert_value,
        cert_path,
        extra_cert_path,
        ssl_name,
    ):
        expected_cert_file_path = cert_path
        mock_get_connection.return_value = mock.MagicMock(
            extra_dejson={
                "database_type": "postgres",
                "location": "test",
                "instance": "instance",
                ssl_name: extra_cert_path,
            }
        )
        mock_get_cert_from_secret.return_value = cert_value
        mock_set_temporary_ssl_file.return_value = expected_cert_file_path

        hook = CloudSQLDatabaseHook(
            gcp_cloudsql_conn_id="cloudsql_connection",
            default_gcp_project_id="google_connection",
            ssl_cert=SSL_CERT,
            ssl_key=SSL_KEY,
            ssl_root_cert=SSL_ROOT_CERT,
        )
        actual_cert_file_path = hook._get_ssl_temporary_file_path(cert_name=ssl_name, cert_path=cert_path)

        assert actual_cert_file_path == expected_cert_file_path
        assert hook.extras.get(ssl_name) == extra_cert_path
        mock_get_cert_from_secret.assert_called_once_with(ssl_name)
        mock_set_temporary_ssl_file.assert_called_once_with(
            cert_name=ssl_name, cert_path=cert_path or extra_cert_path, cert_value=cert_value
        )

    @pytest.mark.parametrize("ssl_name", ["sslcert", "sslkey", "sslrootcert"])
    @mock.patch(HOOK_STR.format("CloudSQLDatabaseHook._set_temporary_ssl_file"))
    @mock.patch(HOOK_STR.format("CloudSQLDatabaseHook._get_cert_from_secret"))
    @mock.patch(HOOK_STR.format("CloudSQLDatabaseHook.get_connection"))
    def test_get_ssl_temporary_file_path_none(
        self,
        mock_get_connection,
        mock_get_cert_from_secret,
        mock_set_temporary_ssl_file,
        ssl_name,
    ):
        expected_cert_file_path = None
        mock_get_connection.return_value = mock.MagicMock(
            extra_dejson={
                "database_type": "postgres",
                "location": "test",
                "instance": "instance",
            }
        )
        mock_get_cert_from_secret.return_value = None
        mock_set_temporary_ssl_file.return_value = expected_cert_file_path

        hook = CloudSQLDatabaseHook(
            gcp_cloudsql_conn_id="cloudsql_connection",
            default_gcp_project_id="google_connection",
            ssl_cert=SSL_CERT,
            ssl_key=SSL_KEY,
            ssl_root_cert=SSL_ROOT_CERT,
        )
        actual_cert_file_path = hook._get_ssl_temporary_file_path(cert_name=ssl_name, cert_path=None)

        assert actual_cert_file_path == expected_cert_file_path
        assert hook.extras.get(ssl_name) is None
        mock_get_cert_from_secret.assert_called_once_with(ssl_name)
        assert not mock_set_temporary_ssl_file.called

    @pytest.mark.parametrize(
        ("cert_name", "cert_value"),
        [
            ["sslcert", SSL_CERT],
            ["sslkey", SSL_KEY],
            ["sslrootcert", SSL_ROOT_CERT],
        ],
    )
    @mock.patch(HOOK_STR.format("GoogleCloudSecretManagerHook"))
    @mock.patch(HOOK_STR.format("CloudSQLDatabaseHook.get_connection"))
    def test_get_cert_from_secret(
        self,
        mock_get_connection,
        mock_secret_hook,
        cert_name,
        cert_value,
    ):
        mock_get_connection.return_value = mock.MagicMock(
            extra_dejson={
                "database_type": "postgres",
                "location": "test",
                "instance": "instance",
            }
        )
        mock_secret = mock_secret_hook.return_value.access_secret.return_value
        mock_secret.payload.data = base64.b64encode(json.dumps({cert_name: cert_value}).encode("ascii"))

        hook = CloudSQLDatabaseHook(
            gcp_conn_id=CONNECTION_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            default_gcp_project_id=PROJECT_ID,
            ssl_secret_id=SECRET_ID,
        )
        actual_cert_value = hook._get_cert_from_secret(cert_name=cert_name)

        assert actual_cert_value == cert_value
        mock_secret_hook.assert_called_once_with(
            gcp_conn_id=CONNECTION_ID, impersonation_chain=IMPERSONATION_CHAIN
        )
        mock_secret_hook.return_value.access_secret.assert_called_once_with(
            project_id=PROJECT_ID, secret_id=SECRET_ID
        )

    @pytest.mark.parametrize(
        ("cert_name", "cert_value"),
        [
            ["sslcert", SSL_CERT],
            ["sslkey", SSL_KEY],
            ["sslrootcert", SSL_ROOT_CERT],
        ],
    )
    @mock.patch(HOOK_STR.format("GoogleCloudSecretManagerHook"))
    @mock.patch(HOOK_STR.format("CloudSQLDatabaseHook.get_connection"))
    def test_get_cert_from_secret_exception(
        self,
        mock_get_connection,
        mock_secret_hook,
        cert_name,
        cert_value,
    ):
        mock_get_connection.return_value = mock.MagicMock(
            extra_dejson={
                "database_type": "postgres",
                "location": "test",
                "instance": "instance",
            }
        )
        mock_secret = mock_secret_hook.return_value.access_secret.return_value
        mock_secret.payload.data = base64.b64encode(json.dumps({"wrong_key": cert_value}).encode("ascii"))

        hook = CloudSQLDatabaseHook(
            gcp_conn_id=CONNECTION_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            default_gcp_project_id=PROJECT_ID,
            ssl_secret_id=SECRET_ID,
        )

        with pytest.raises(AirflowException):
            hook._get_cert_from_secret(cert_name=cert_name)

        mock_secret_hook.assert_called_once_with(
            gcp_conn_id=CONNECTION_ID, impersonation_chain=IMPERSONATION_CHAIN
        )
        mock_secret_hook.return_value.access_secret.assert_called_once_with(
            project_id=PROJECT_ID, secret_id=SECRET_ID
        )

    @pytest.mark.parametrize("cert_name", ["sslcert", "sslkey", "sslrootcert"])
    @mock.patch(HOOK_STR.format("GoogleCloudSecretManagerHook"))
    @mock.patch(HOOK_STR.format("CloudSQLDatabaseHook.get_connection"))
    def test_get_cert_from_secret_none(
        self,
        mock_get_connection,
        mock_secret_hook,
        cert_name,
    ):
        mock_get_connection.return_value = mock.MagicMock(
            extra_dejson={
                "database_type": "postgres",
                "location": "test",
                "instance": "instance",
            }
        )

        hook = CloudSQLDatabaseHook(
            gcp_conn_id=CONNECTION_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            default_gcp_project_id=PROJECT_ID,
        )
        actual_cert_value = hook._get_cert_from_secret(cert_name=cert_name)

        assert actual_cert_value is None
        assert not mock_secret_hook.called
        assert not mock_secret_hook.return_value.access_secret.called

    @pytest.mark.parametrize("cert_name", ["sslcert", "sslkey", "sslrootcert"])
    @mock.patch("builtins.open", new_callable=mock_open, read_data="test-data")
    @mock.patch(HOOK_STR.format("NamedTemporaryFile"))
    @mock.patch(HOOK_STR.format("CloudSQLDatabaseHook.get_connection"))
    def test_set_temporary_ssl_file_cert_path(
        self,
        mock_get_connection,
        mock_named_temporary_file,
        mock_open_file,
        cert_name,
    ):
        mock_get_connection.return_value = mock.MagicMock(
            extra_dejson={
                "database_type": "postgres",
                "location": "test",
                "instance": "instance",
            }
        )
        expected_file_name = "/test/path/to/file"
        mock_named_temporary_file.return_value.name = expected_file_name
        source_cert_path = "/source/cert/path/to/file"

        hook = CloudSQLDatabaseHook()
        actual_path = hook._set_temporary_ssl_file(cert_name=cert_name, cert_path=source_cert_path)

        assert actual_path == expected_file_name
        mock_named_temporary_file.assert_called_once_with(mode="w+b", prefix="/tmp/certs/")
        mock_open_file.assert_has_calls([call(source_cert_path, "rb")])
        mock_named_temporary_file.return_value.write.assert_called_once_with("test-data")
        mock_named_temporary_file.return_value.flush.assert_called_once()

    @pytest.mark.parametrize("cert_name", ["sslcert", "sslkey", "sslrootcert"])
    @mock.patch(HOOK_STR.format("NamedTemporaryFile"))
    @mock.patch(HOOK_STR.format("CloudSQLDatabaseHook.get_connection"))
    def test_set_temporary_ssl_file_cert_value(
        self,
        mock_get_connection,
        mock_named_temporary_file,
        cert_name,
    ):
        mock_get_connection.return_value = mock.MagicMock(
            extra_dejson={
                "database_type": "postgres",
                "location": "test",
                "instance": "instance",
            }
        )
        expected_file_name = "/test/path/to/file"
        mock_named_temporary_file.return_value.name = expected_file_name
        cert_value = "test-cert-value"

        hook = CloudSQLDatabaseHook()
        actual_path = hook._set_temporary_ssl_file(cert_name=cert_name, cert_value=cert_value)

        assert actual_path == expected_file_name
        mock_named_temporary_file.assert_called_once_with(mode="w+b", prefix="/tmp/certs/")
        mock_named_temporary_file.return_value.write.assert_called_once_with(cert_value.encode("ascii"))
        mock_named_temporary_file.return_value.flush.assert_called_once()

    @pytest.mark.parametrize("cert_name", ["sslcert", "sslkey", "sslrootcert"])
    @mock.patch(HOOK_STR.format("NamedTemporaryFile"))
    @mock.patch(HOOK_STR.format("CloudSQLDatabaseHook.get_connection"))
    def test_set_temporary_ssl_file_exception(
        self,
        mock_get_connection,
        mock_named_temporary_file,
        cert_name,
    ):
        mock_get_connection.return_value = mock.MagicMock(
            extra_dejson={
                "database_type": "postgres",
                "location": "test",
                "instance": "instance",
            }
        )
        expected_file_name = "/test/path/to/file"
        mock_named_temporary_file.return_value.name = expected_file_name
        cert_value = "test-cert-value"
        source_cert_path = "/source/cert/path/to/file"

        hook = CloudSQLDatabaseHook()

        with pytest.raises(AirflowException):
            hook._set_temporary_ssl_file(
                cert_name=cert_name, cert_value=cert_value, cert_path=source_cert_path
            )

        assert not mock_named_temporary_file.called
        assert not mock_named_temporary_file.return_value.write.called
        assert not mock_named_temporary_file.return_value.flush.called

    @pytest.mark.parametrize("cert_name", ["sslcert", "sslkey", "sslrootcert"])
    @mock.patch(HOOK_STR.format("NamedTemporaryFile"))
    @mock.patch(HOOK_STR.format("CloudSQLDatabaseHook.get_connection"))
    def test_set_temporary_ssl_file_none(
        self,
        mock_get_connection,
        mock_named_temporary_file,
        cert_name,
    ):
        mock_get_connection.return_value = mock.MagicMock(
            extra_dejson={
                "database_type": "postgres",
                "location": "test",
                "instance": "instance",
            }
        )
        expected_file_name = "/test/path/to/file"
        mock_named_temporary_file.return_value.name = expected_file_name

        hook = CloudSQLDatabaseHook()

        actual_path = hook._set_temporary_ssl_file(cert_name=cert_name)

        assert actual_path is None
        assert not mock_named_temporary_file.called
        assert not mock_named_temporary_file.return_value.write.called
        assert not mock_named_temporary_file.return_value.flush.called


class TestCloudSqlDatabaseQueryHook:
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def setup_method(self, method, mock_get_conn):
        self.sql_connection = Connection(
            conn_id="my_gcp_sql_connection",
            conn_type="gcpcloudsql",
            login="login",
            password="password",
            host="host",
            schema="schema",
            extra='{"database_type":"postgres", "location":"my_location", '
            '"instance":"my_instance", "use_proxy": true, '
            '"project_id":"my_project"}',
        )
        self.connection = Connection(
            conn_id="my_gcp_connection",
            conn_type="google_cloud_platform",
        )
        scopes = [
            "https://www.googleapis.com/auth/pubsub",
            "https://www.googleapis.com/auth/datastore",
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/devstorage.read_write",
            "https://www.googleapis.com/auth/logging.write",
            "https://www.googleapis.com/auth/cloud-platform",
        ]
        conn_extra = {
            "scope": ",".join(scopes),
            "project": "your-gcp-project",
            "key_path": "/var/local/google_cloud_default.json",
        }
        conn_extra_json = json.dumps(conn_extra)
        if AIRFLOW_V_3_1_PLUS:
            self.connection.extra = conn_extra_json
        else:
            self.connection.set_extra(conn_extra_json)

        mock_get_conn.side_effect = [self.sql_connection, self.connection]
        self.db_hook = CloudSQLDatabaseHook(
            gcp_cloudsql_conn_id="my_gcp_sql_connection", gcp_conn_id="my_gcp_connection"
        )

    def test_get_sqlproxy_runner(self):
        self.db_hook._generate_connection_uri()
        sqlproxy_runner = self.db_hook.get_sqlproxy_runner()
        assert sqlproxy_runner.gcp_conn_id == self.connection.conn_id
        project = self.sql_connection.extra_dejson["project_id"]
        location = self.sql_connection.extra_dejson["location"]
        instance = self.sql_connection.extra_dejson["instance"]
        instance_spec = f"{project}:{location}:{instance}"
        assert sqlproxy_runner.instance_specification == instance_spec

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_hook_with_not_too_long_unix_socket_path(self, get_connection):
        uri = (
            "gcpcloudsql://user:password@127.0.0.1:3200/testdb?database_type=postgres&"
            "project_id=example-project&location=europe-west1&"
            "instance="
            "test_db_with_longname_but_with_limit_of_UNIX_socket&"
            "use_proxy=True&sql_proxy_use_tcp=False"
        )
        if AIRFLOW_V_3_1_PLUS:
            get_connection.side_effect = [Connection(conn_id="test_conn_id", **_parse_from_uri(uri))]
        else:
            get_connection.side_effect = [Connection(uri=uri)]
        hook = CloudSQLDatabaseHook()
        connection = hook.create_connection()
        assert connection.conn_type == "postgres"
        assert connection.schema == "testdb"

    def _verify_postgres_connection(self, get_connection, uri):
        if AIRFLOW_V_3_1_PLUS:
            get_connection.side_effect = [Connection(conn_id="test_conn_id", **_parse_from_uri(uri))]
        else:
            get_connection.side_effect = [Connection(uri=uri)]
        hook = CloudSQLDatabaseHook()
        connection = hook.create_connection()
        assert connection.conn_type == "postgres"
        assert connection.host == "127.0.0.1"
        assert connection.port == 3200
        assert connection.schema == "testdb"
        return connection

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_hook_with_correct_parameters_postgres(self, get_connection):
        uri = (
            "gcpcloudsql://user:password@127.0.0.1:3200/testdb?database_type=postgres&"
            "project_id=example-project&location=europe-west1&instance=testdb&"
            "use_proxy=False&use_ssl=False"
        )
        self._verify_postgres_connection(get_connection, uri)

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook._set_temporary_ssl_file")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_hook_with_correct_parameters_postgres_ssl(self, get_connection, mock_set_temporary_ssl_file):
        def side_effect_func(cert_name, cert_path, cert_value):
            return f"/tmp/{cert_name}"

        mock_set_temporary_ssl_file.side_effect = side_effect_func
        uri = (
            "gcpcloudsql://user:password@127.0.0.1:3200/testdb?database_type=postgres&"
            "project_id=example-project&location=europe-west1&instance=testdb&"
            "use_proxy=False&use_ssl=True&sslcert=/bin/bash&"
            "sslkey=/bin/bash&sslrootcert=/bin/bash"
        )
        connection = self._verify_postgres_connection(get_connection, uri)
        assert connection.extra_dejson["sslkey"] == "/tmp/sslkey"
        assert connection.extra_dejson["sslcert"] == "/tmp/sslcert"
        assert connection.extra_dejson["sslrootcert"] == "/tmp/sslrootcert"

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_hook_with_correct_parameters_postgres_proxy_socket(self, get_connection):
        uri = (
            "gcpcloudsql://user:password@127.0.0.1:3200/testdb?database_type=postgres&"
            "project_id=example-project&location=europe-west1&instance=testdb&"
            "use_proxy=True&sql_proxy_use_tcp=False"
        )
        if AIRFLOW_V_3_1_PLUS:
            get_connection.side_effect = [Connection(conn_id="test_conn_id", **_parse_from_uri(uri))]
        else:
            get_connection.side_effect = [Connection(uri=uri)]
        hook = CloudSQLDatabaseHook()
        connection = hook.create_connection()
        assert connection.conn_type == "postgres"
        assert tempfile.gettempdir() in connection.host
        assert "example-project:europe-west1:testdb" in connection.host
        assert connection.port is None
        assert connection.schema == "testdb"

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_hook_with_correct_parameters_project_id_missing(self, get_connection):
        uri = (
            "gcpcloudsql://user:password@127.0.0.1:3200/testdb?database_type=mysql&"
            "location=europe-west1&instance=testdb&"
            "use_proxy=False&use_ssl=False"
        )
        self.verify_mysql_connection(get_connection, uri)

    def verify_mysql_connection(self, get_connection, uri):
        if AIRFLOW_V_3_1_PLUS:
            get_connection.side_effect = [Connection(conn_id="test_conn_id", **_parse_from_uri(uri))]
        else:
            get_connection.side_effect = [Connection(uri=uri)]
        hook = CloudSQLDatabaseHook()
        connection = hook.create_connection()
        assert connection.conn_type == "mysql"
        assert connection.host == "127.0.0.1"
        assert connection.port == 3200
        assert connection.schema == "testdb"
        return connection

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_hook_with_correct_parameters_postgres_proxy_tcp(self, get_connection):
        uri = (
            "gcpcloudsql://user:password@127.0.0.1:3200/testdb?database_type=postgres&"
            "project_id=example-project&location=europe-west1&instance=testdb&"
            "use_proxy=True&sql_proxy_use_tcp=True"
        )
        if AIRFLOW_V_3_1_PLUS:
            get_connection.side_effect = [Connection(conn_id="test_conn_id", **_parse_from_uri(uri))]
        else:
            get_connection.side_effect = [Connection(uri=uri)]
        hook = CloudSQLDatabaseHook()
        connection = hook.create_connection()
        assert connection.conn_type == "postgres"
        assert connection.host == "127.0.0.1"
        assert connection.port != 3200
        assert connection.schema == "testdb"

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_hook_with_correct_parameters_mysql(self, get_connection):
        uri = (
            "gcpcloudsql://user:password@127.0.0.1:3200/testdb?database_type=mysql&"
            "project_id=example-project&location=europe-west1&instance=testdb&"
            "use_proxy=False&use_ssl=False"
        )
        self.verify_mysql_connection(get_connection, uri)

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook._set_temporary_ssl_file")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_hook_with_correct_parameters_mysql_ssl(self, get_connection, mock_set_temporary_ssl_file):
        def side_effect_func(cert_name, cert_path, cert_value):
            return f"/tmp/{cert_name}"

        mock_set_temporary_ssl_file.side_effect = side_effect_func
        uri = (
            "gcpcloudsql://user:password@127.0.0.1:3200/testdb?database_type=mysql&"
            "project_id=example-project&location=europe-west1&instance=testdb&"
            "use_proxy=False&use_ssl=True&sslcert=/bin/bash&"
            "sslkey=/bin/bash&sslrootcert=/bin/bash"
        )
        connection = self.verify_mysql_connection(get_connection, uri)
        assert json.loads(connection.extra_dejson["ssl"])["cert"] == "/tmp/sslcert"
        assert json.loads(connection.extra_dejson["ssl"])["key"] == "/tmp/sslkey"
        assert json.loads(connection.extra_dejson["ssl"])["ca"] == "/tmp/sslrootcert"

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_hook_with_correct_parameters_mysql_proxy_socket(self, get_connection):
        uri = (
            "gcpcloudsql://user:password@127.0.0.1:3200/testdb?database_type=mysql&"
            "project_id=example-project&location=europe-west1&instance=testdb&"
            "use_proxy=True&sql_proxy_use_tcp=False"
        )
        if AIRFLOW_V_3_1_PLUS:
            get_connection.side_effect = [Connection(conn_id="test_conn_id", **_parse_from_uri(uri))]
        else:
            get_connection.side_effect = [Connection(uri=uri)]
        hook = CloudSQLDatabaseHook()
        connection = hook.create_connection()
        assert connection.conn_type == "mysql"
        assert connection.host == "localhost"
        assert tempfile.gettempdir() in connection.extra_dejson["unix_socket"]
        assert "example-project:europe-west1:testdb" in connection.extra_dejson["unix_socket"]
        assert connection.port is None
        assert connection.schema == "testdb"

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_hook_with_correct_parameters_mysql_tcp(self, get_connection):
        uri = (
            "gcpcloudsql://user:password@127.0.0.1:3200/testdb?database_type=mysql&"
            "project_id=example-project&location=europe-west1&instance=testdb&"
            "use_proxy=True&sql_proxy_use_tcp=True"
        )
        if AIRFLOW_V_3_1_PLUS:
            get_connection.side_effect = [Connection(conn_id="test_conn_id", **_parse_from_uri(uri))]
        else:
            get_connection.side_effect = [Connection(uri=uri)]
        hook = CloudSQLDatabaseHook()
        connection = hook.create_connection()
        assert connection.conn_type == "mysql"
        assert connection.host == "127.0.0.1"
        assert connection.port != 3200
        assert connection.schema == "testdb"


def get_processor():
    processor = os.uname().machine
    if processor == "x86_64":
        processor = "amd64"
    if processor == "aarch64":
        processor = "arm64"
    return processor


class TestCloudSqlProxyRunner:
    @pytest.mark.parametrize(
        ("version", "download_url"),
        [
            (
                "v1.23.0",
                "https://storage.googleapis.com/cloudsql-proxy/v1.23.0/cloud_sql_proxy."
                f"{platform.system().lower()}.{get_processor()}",
            ),
            (
                "v1.23.0-preview.1",
                "https://storage.googleapis.com/cloudsql-proxy/v1.23.0-preview.1/cloud_sql_proxy."
                f"{platform.system().lower()}.{get_processor()}",
            ),
        ],
    )
    def test_cloud_sql_proxy_runner_version_ok(self, version, download_url):
        runner = CloudSqlProxyRunner(
            path_prefix="12345678",
            instance_specification="project:us-east-1:instance",
            sql_proxy_version=version,
        )
        assert runner._get_sql_proxy_download_url() == download_url

    @pytest.mark.parametrize(
        "version",
        [
            "v1.23.",
            "v1.23.0..",
            "v1.23.0\\",
            "\\",
        ],
    )
    def test_cloud_sql_proxy_runner_version_nok(self, version):
        runner = CloudSqlProxyRunner(
            path_prefix="12345678",
            instance_specification="project:us-east-1:instance",
            sql_proxy_version=version,
        )
        with pytest.raises(ValueError, match="The sql_proxy_version should match the regular expression"):
            runner._get_sql_proxy_download_url()


class TestCloudSQLAsyncHook:
    @pytest.mark.asyncio
    @mock.patch(HOOK_STR.format("CloudSQLAsyncHook._get_conn"))
    async def test_async_get_operation_name_should_execute_successfully(self, mocked_conn, hook_async):
        await hook_async.get_operation_name(
            operation_name=OPERATION_NAME,
            project_id=PROJECT_ID,
            session=session,
        )

        mocked_conn.assert_awaited_once_with(url=OPERATION_URL, session=session)

    @pytest.mark.asyncio
    @mock.patch(HOOK_STR.format("CloudSQLAsyncHook.get_operation_name"))
    async def test_async_get_operation_completed_should_execute_successfully(self, mocked_get, hook_async):
        response = aiohttp.ClientResponse(
            "get",
            URL(OPERATION_URL),
            request_info=mock.Mock(),
            writer=mock.Mock(),
            continue100=None,
            timer=TimerNoop(),
            traces=[],
            loop=mock.Mock(),
            session=None,
        )
        response.status = 200
        mocked_get.return_value = response
        mocked_get.return_value._headers = {"Authorization": "test-token"}
        mocked_get.return_value._body = b'{"status": "DONE"}'

        operation = await hook_async.get_operation(operation_name=OPERATION_NAME, project_id=PROJECT_ID)
        mocked_get.assert_awaited_once()
        assert operation["status"] == "DONE"

    @pytest.mark.asyncio
    @mock.patch(HOOK_STR.format("CloudSQLAsyncHook.get_operation_name"))
    async def test_async_get_operation_running_should_execute_successfully(self, mocked_get, hook_async):
        response = aiohttp.ClientResponse(
            "get",
            URL(OPERATION_URL),
            request_info=mock.Mock(),
            writer=mock.Mock(),
            continue100=None,
            timer=TimerNoop(),
            traces=[],
            loop=mock.Mock(),
            session=None,
        )
        response.status = 200
        mocked_get.return_value = response
        mocked_get.return_value._headers = {"Authorization": "test-token"}
        mocked_get.return_value._body = b'{"status": "RUNNING"}'

        operation = await hook_async.get_operation(operation_name=OPERATION_NAME, project_id=PROJECT_ID)
        mocked_get.assert_awaited_once()
        assert operation["status"] == "RUNNING"

    @pytest.mark.asyncio
    @mock.patch(HOOK_STR.format("CloudSQLAsyncHook._get_conn"))
    async def test_async_get_operation_exception_should_execute_successfully(
        self, mocked_get_conn, hook_async
    ):
        """Assets that the logging is done correctly when CloudSQLAsyncHook raises HttpError"""
        mocked_get_conn.side_effect = HttpError(
            resp=mock.MagicMock(status=409), content=b"Operation already exists"
        )
        with pytest.raises(HttpError):
            await hook_async.get_operation(operation_name=OPERATION_NAME, project_id=PROJECT_ID)
