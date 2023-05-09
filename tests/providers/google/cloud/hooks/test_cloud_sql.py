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

import json
import os
import platform
import tempfile
from unittest import mock
from unittest.mock import PropertyMock

import httplib2
import pytest
from googleapiclient.errors import HttpError

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.google.cloud.hooks.cloud_sql import (
    CloudSQLDatabaseHook,
    CloudSQLHook,
    CloudSqlProxyRunner,
)
from tests.providers.google.cloud.utils.base_gcp_mock import (
    mock_base_gcp_hook_default_project_id,
    mock_base_gcp_hook_no_default_project_id,
)


class TestGcpSqlHookDefaultProjectId:
    def test_delegate_to_runtime_error(self):
        with pytest.raises(RuntimeError):
            CloudSQLHook(api_version="v1", gcp_conn_id="test", delegate_to="delegate_to")

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
        assert 1 == mock_get_credentials.call_count

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
        assert 400 == err.resp.status
        assert 1 == mock_get_credentials.call_count

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
        assert 1 == mock_get_credentials.call_count

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
        wait_for_operation_to_complete.assert_called_once_with(
            project_id="example-project", operation_name="operation_id"
        )
        assert 1 == mock_get_credentials.call_count

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook._wait_for_operation_to_complete")
    def test_instance_export_with_in_progress_retry(self, wait_for_operation_to_complete, get_conn):
        export_method = get_conn.return_value.instances.return_value.export
        execute_method = export_method.return_value.execute
        execute_method.side_effect = [
            HttpError(
                resp=type(
                    "",
                    (object,),
                    {
                        "status": 429,
                    },
                )(),
                content=b"Internal Server Error",
            ),
            {"name": "operation_id"},
        ]
        wait_for_operation_to_complete.return_value = None
        self.cloudsql_hook.export_instance(project_id="example-project", instance="instance", body={})

        assert 2 == export_method.call_count
        assert 2 == execute_method.call_count
        wait_for_operation_to_complete.assert_called_once_with(
            project_id="example-project", operation_name="operation_id"
        )

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
        assert "instance" == res["name"]
        get_method.assert_called_once_with(instance="instance", project="example-project")
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_not_called()
        assert 1 == mock_get_credentials.call_count

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
        assert 1 == mock_get_credentials.call_count

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
                resp=type(
                    "",
                    (object,),
                    {
                        "status": 429,
                    },
                )(),
                content=b"Internal Server Error",
            ),
            {"name": "operation_id"},
        ]
        wait_for_operation_to_complete.return_value = None
        self.cloudsql_hook.create_instance(body={})

        assert 1 == mock_get_credentials.call_count
        assert 2 == insert_method.call_count
        assert 2 == execute_method.call_count
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
                resp=type(
                    "",
                    (object,),
                    {
                        "status": 429,
                    },
                )(),
                content=b"Internal Server Error",
            ),
            {"name": "operation_id"},
        ]
        wait_for_operation_to_complete.return_value = None
        self.cloudsql_hook.patch_instance(instance="instance", body={})

        assert 1 == mock_get_credentials.call_count
        assert 2 == patch_method.call_count
        assert 2 == execute_method.call_count
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
        assert 1 == mock_get_credentials.call_count

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
            operation_name="operation_id", project_id="example-project"
        )
        assert 1 == mock_get_credentials.call_count

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
                resp=type(
                    "",
                    (object,),
                    {
                        "status": 429,
                    },
                )(),
                content=b"Internal Server Error",
            ),
            {"name": "operation_id"},
        ]
        wait_for_operation_to_complete.return_value = None
        self.cloudsql_hook.delete_instance(instance="instance")

        assert 1 == mock_get_credentials.call_count
        assert 2 == delete_method.call_count
        assert 2 == execute_method.call_count
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name="operation_id", project_id="example-project"
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
        assert 1 == mock_get_credentials.call_count

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
        assert "database" == res["name"]
        get_method.assert_called_once_with(
            instance="instance", database="database", project="example-project"
        )
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_not_called()
        assert 1 == mock_get_credentials.call_count

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
        assert 1 == mock_get_credentials.call_count

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
                resp=type(
                    "",
                    (object,),
                    {
                        "status": 429,
                    },
                )(),
                content=b"Internal Server Error",
            ),
            {"name": "operation_id"},
        ]
        wait_for_operation_to_complete.return_value = None
        self.cloudsql_hook.create_database(instance="instance", body={})

        assert 1 == mock_get_credentials.call_count
        assert 2 == insert_method.call_count
        assert 2 == execute_method.call_count
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
        assert 1 == mock_get_credentials.call_count

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
                resp=type(
                    "",
                    (object,),
                    {
                        "status": 429,
                    },
                )(),
                content=b"Internal Server Error",
            ),
            {"name": "operation_id"},
        ]
        wait_for_operation_to_complete.return_value = None
        self.cloudsql_hook.patch_database(instance="instance", database="database", body={})

        assert 1 == mock_get_credentials.call_count
        assert 2 == patch_method.call_count
        assert 2 == execute_method.call_count
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
        assert 1 == mock_get_credentials.call_count

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
                resp=type(
                    "",
                    (object,),
                    {
                        "status": 429,
                    },
                )(),
                content=b"Internal Server Error",
            ),
            {"name": "operation_id"},
        ]
        wait_for_operation_to_complete.return_value = None
        self.cloudsql_hook.delete_database(instance="instance", database="database")

        assert 1 == mock_get_credentials.call_count
        assert 2 == delete_method.call_count
        assert 2 == execute_method.call_count
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
        assert "instance" == res["name"]
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
            operation_name="operation_id", project_id="example-project"
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
        assert "database" == res["name"]
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


class TestCloudSqlDatabaseHook:
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_cloudsql_database_hook_validate_ssl_certs_no_ssl(self, get_connection):
        connection = Connection()
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
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_cloudsql_database_hook_validate_ssl_certs_missing_cert_params(
        self, get_connection, mock_is_file, cert_dict
    ):
        mock_is_file.side_effects = True
        connection = Connection()
        extras = {"location": "test", "instance": "instance", "database_type": "postgres", "use_ssl": "True"}
        extras.update(cert_dict)
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
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_cloudsql_database_hook_validate_ssl_certs_with_ssl(self, get_connection, mock_is_file):
        connection = Connection()
        mock_is_file.return_value = True
        connection.set_extra(
            json.dumps(
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
        )
        get_connection.return_value = connection
        hook = CloudSQLDatabaseHook(
            gcp_cloudsql_conn_id="cloudsql_connection", default_gcp_project_id="google_connection"
        )
        hook.validate_ssl_certs()

    @mock.patch("os.path.isfile")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_cloudsql_database_hook_validate_ssl_certs_with_ssl_files_not_readable(
        self, get_connection, mock_is_file
    ):
        connection = Connection()
        mock_is_file.return_value = False
        connection.set_extra(
            json.dumps(
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
        )
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
        connection = Connection()
        connection.set_extra(
            json.dumps(
                {
                    "location": "test",
                    "instance": "very_long_instance_name_that_will_be_too_long_to_build_socket_length",
                    "database_type": "postgres",
                    "use_proxy": "True",
                    "use_tcp": "False",
                }
            )
        )
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
        connection = Connection()
        connection.set_extra(
            json.dumps(
                {
                    "location": "test",
                    "instance": "short_instance_name",
                    "database_type": "postgres",
                    "use_proxy": "True",
                    "use_tcp": "False",
                }
            )
        )
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
        connection = Connection(uri=uri)
        params = {
            "location": "test",
            "instance": "instance",
            "database_type": "postgres",
            "use_proxy": "True",
            "use_tcp": "False",
        }
        connection.set_extra(json.dumps(params))
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
        connection = Connection(uri="http://user:password@host:80/database")
        connection.set_extra(
            json.dumps(
                {
                    "location": "test",
                    "instance": "instance",
                    "database_type": "postgres",
                }
            )
        )
        get_connection.return_value = connection
        hook = CloudSQLDatabaseHook(
            gcp_cloudsql_conn_id="cloudsql_connection", default_gcp_project_id="google_connection"
        )
        with pytest.raises(ValueError) as ctx:
            hook.get_sqlproxy_runner()
        err = ctx.value
        assert "Proxy runner can only be retrieved in case of use_proxy = True" in str(err)

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_cloudsql_database_hook_get_sqlproxy_runner(self, get_connection):
        connection = Connection(uri="http://user:password@host:80/database")
        connection.set_extra(
            json.dumps(
                {
                    "location": "test",
                    "instance": "instance",
                    "database_type": "postgres",
                    "use_proxy": "True",
                    "use_tcp": "False",
                }
            )
        )
        get_connection.return_value = connection
        hook = CloudSQLDatabaseHook(
            gcp_cloudsql_conn_id="cloudsql_connection", default_gcp_project_id="google_connection"
        )
        hook.create_connection()
        proxy_runner = hook.get_sqlproxy_runner()
        assert proxy_runner is not None

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_cloudsql_database_hook_get_database_hook(self, get_connection):
        connection = Connection(uri="http://user:password@host:80/database")
        connection.set_extra(
            json.dumps(
                {
                    "location": "test",
                    "instance": "instance",
                    "database_type": "postgres",
                }
            )
        )
        get_connection.return_value = connection
        hook = CloudSQLDatabaseHook(
            gcp_cloudsql_conn_id="cloudsql_connection", default_gcp_project_id="google_connection"
        )
        connection = hook.create_connection()
        db_hook = hook.get_database_hook(connection=connection)
        assert db_hook is not None


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
        get_connection.side_effect = [Connection(uri=uri)]
        hook = CloudSQLDatabaseHook()
        connection = hook.create_connection()
        assert "postgres" == connection.conn_type
        assert "testdb" == connection.schema

    def _verify_postgres_connection(self, get_connection, uri):
        get_connection.side_effect = [Connection(uri=uri)]
        hook = CloudSQLDatabaseHook()
        connection = hook.create_connection()
        assert "postgres" == connection.conn_type
        assert "127.0.0.1" == connection.host
        assert 3200 == connection.port
        assert "testdb" == connection.schema
        return connection

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_hook_with_correct_parameters_postgres(self, get_connection):
        uri = (
            "gcpcloudsql://user:password@127.0.0.1:3200/testdb?database_type=postgres&"
            "project_id=example-project&location=europe-west1&instance=testdb&"
            "use_proxy=False&use_ssl=False"
        )
        self._verify_postgres_connection(get_connection, uri)

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_hook_with_correct_parameters_postgres_ssl(self, get_connection):
        uri = (
            "gcpcloudsql://user:password@127.0.0.1:3200/testdb?database_type=postgres&"
            "project_id=example-project&location=europe-west1&instance=testdb&"
            "use_proxy=False&use_ssl=True&sslcert=/bin/bash&"
            "sslkey=/bin/bash&sslrootcert=/bin/bash"
        )
        connection = self._verify_postgres_connection(get_connection, uri)
        assert "/bin/bash" == connection.extra_dejson["sslkey"]
        assert "/bin/bash" == connection.extra_dejson["sslcert"]
        assert "/bin/bash" == connection.extra_dejson["sslrootcert"]

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_hook_with_correct_parameters_postgres_proxy_socket(self, get_connection):
        uri = (
            "gcpcloudsql://user:password@127.0.0.1:3200/testdb?database_type=postgres&"
            "project_id=example-project&location=europe-west1&instance=testdb&"
            "use_proxy=True&sql_proxy_use_tcp=False"
        )
        get_connection.side_effect = [Connection(uri=uri)]
        hook = CloudSQLDatabaseHook()
        connection = hook.create_connection()
        assert "postgres" == connection.conn_type
        assert tempfile.gettempdir() in connection.host
        assert "example-project:europe-west1:testdb" in connection.host
        assert connection.port is None
        assert "testdb" == connection.schema

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_hook_with_correct_parameters_project_id_missing(self, get_connection):
        uri = (
            "gcpcloudsql://user:password@127.0.0.1:3200/testdb?database_type=mysql&"
            "location=europe-west1&instance=testdb&"
            "use_proxy=False&use_ssl=False"
        )
        self.verify_mysql_connection(get_connection, uri)

    def verify_mysql_connection(self, get_connection, uri):
        get_connection.side_effect = [Connection(uri=uri)]
        hook = CloudSQLDatabaseHook()
        connection = hook.create_connection()
        assert "mysql" == connection.conn_type
        assert "127.0.0.1" == connection.host
        assert 3200 == connection.port
        assert "testdb" == connection.schema
        return connection

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_hook_with_correct_parameters_postgres_proxy_tcp(self, get_connection):
        uri = (
            "gcpcloudsql://user:password@127.0.0.1:3200/testdb?database_type=postgres&"
            "project_id=example-project&location=europe-west1&instance=testdb&"
            "use_proxy=True&sql_proxy_use_tcp=True"
        )
        get_connection.side_effect = [Connection(uri=uri)]
        hook = CloudSQLDatabaseHook()
        connection = hook.create_connection()
        assert "postgres" == connection.conn_type
        assert "127.0.0.1" == connection.host
        assert 3200 != connection.port
        assert "testdb" == connection.schema

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_hook_with_correct_parameters_mysql(self, get_connection):
        uri = (
            "gcpcloudsql://user:password@127.0.0.1:3200/testdb?database_type=mysql&"
            "project_id=example-project&location=europe-west1&instance=testdb&"
            "use_proxy=False&use_ssl=False"
        )
        self.verify_mysql_connection(get_connection, uri)

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_hook_with_correct_parameters_mysql_ssl(self, get_connection):
        uri = (
            "gcpcloudsql://user:password@127.0.0.1:3200/testdb?database_type=mysql&"
            "project_id=example-project&location=europe-west1&instance=testdb&"
            "use_proxy=False&use_ssl=True&sslcert=/bin/bash&"
            "sslkey=/bin/bash&sslrootcert=/bin/bash"
        )
        connection = self.verify_mysql_connection(get_connection, uri)
        assert "/bin/bash" == json.loads(connection.extra_dejson["ssl"])["cert"]
        assert "/bin/bash" == json.loads(connection.extra_dejson["ssl"])["key"]
        assert "/bin/bash" == json.loads(connection.extra_dejson["ssl"])["ca"]

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_hook_with_correct_parameters_mysql_proxy_socket(self, get_connection):
        uri = (
            "gcpcloudsql://user:password@127.0.0.1:3200/testdb?database_type=mysql&"
            "project_id=example-project&location=europe-west1&instance=testdb&"
            "use_proxy=True&sql_proxy_use_tcp=False"
        )
        get_connection.side_effect = [Connection(uri=uri)]
        hook = CloudSQLDatabaseHook()
        connection = hook.create_connection()
        assert "mysql" == connection.conn_type
        assert "localhost" == connection.host
        assert tempfile.gettempdir() in connection.extra_dejson["unix_socket"]
        assert "example-project:europe-west1:testdb" in connection.extra_dejson["unix_socket"]
        assert connection.port is None
        assert "testdb" == connection.schema

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook.get_connection")
    def test_hook_with_correct_parameters_mysql_tcp(self, get_connection):
        uri = (
            "gcpcloudsql://user:password@127.0.0.1:3200/testdb?database_type=mysql&"
            "project_id=example-project&location=europe-west1&instance=testdb&"
            "use_proxy=True&sql_proxy_use_tcp=True"
        )
        get_connection.side_effect = [Connection(uri=uri)]
        hook = CloudSQLDatabaseHook()
        connection = hook.create_connection()
        assert "mysql" == connection.conn_type
        assert "127.0.0.1" == connection.host
        assert 3200 != connection.port
        assert "testdb" == connection.schema


def get_processor():
    processor = os.uname().machine
    if processor == "x86_64":
        processor = "amd64"
    return processor


class TestCloudSqlProxyRunner:
    @pytest.mark.parametrize(
        ["version", "download_url"],
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
