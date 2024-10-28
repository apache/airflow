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

import os
from unittest import mock

import pytest

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.models import Connection
from airflow.providers.google.cloud.operators.cloud_sql import (
    CloudSQLCloneInstanceOperator,
    CloudSQLCreateInstanceDatabaseOperator,
    CloudSQLCreateInstanceOperator,
    CloudSQLDeleteInstanceDatabaseOperator,
    CloudSQLDeleteInstanceOperator,
    CloudSQLExecuteQueryOperator,
    CloudSQLExportInstanceOperator,
    CloudSQLImportInstanceOperator,
    CloudSQLInstancePatchOperator,
    CloudSQLPatchInstanceDatabaseOperator,
)
from airflow.providers.google.cloud.triggers.cloud_sql import CloudSQLExportTrigger
from airflow.providers.google.common.consts import GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME

PROJECT_ID = os.environ.get("PROJECT_ID", "project-id")
INSTANCE_NAME = os.environ.get("INSTANCE_NAME", "test-name")
DB_NAME = os.environ.get("DB_NAME", "db1")

CREATE_BODY = {
    "name": INSTANCE_NAME,
    "settings": {
        "tier": "db-n1-standard-1",
        "backupConfiguration": {
            "binaryLogEnabled": True,
            "enabled": True,
            "replicationLogArchivingEnabled": True,
            "startTime": "05:00",
        },
        "activationPolicy": "ALWAYS",
        "authorizedGaeApplications": [],
        "crashSafeReplicationEnabled": True,
        "dataDiskSizeGb": 30,
        "dataDiskType": "PD_SSD",
        "databaseFlags": [],
        "ipConfiguration": {
            "ipv4Enabled": True,
            "authorizedNetworks": [
                {
                    "value": "192.168.100.0/24",
                    "name": "network1",
                    "expirationTime": "2012-11-15T16:19:00.094Z",
                },
            ],
            "privateNetwork": "/vpc/resource/link",
            "requireSsl": True,
        },
        "locationPreference": {
            "zone": "europe-west4-a",
            "followGaeApplication": "/app/engine/application/to/follow",
        },
        "maintenanceWindow": {"hour": 5, "day": 7, "updateTrack": "canary"},
        "pricingPlan": "PER_USE",
        "replicationType": "ASYNCHRONOUS",
        "storageAutoResize": False,
        "storageAutoResizeLimit": 0,
        "userLabels": {"my-key": "my-value"},
    },
    "databaseVersion": "MYSQL_5_7",
    "failoverReplica": {"name": "replica-1"},
    "masterInstanceName": "master-instance-1",
    "onPremisesConfiguration": {},
    "region": "europe-west4",
    "replicaConfiguration": {
        "mysqlReplicaConfiguration": {
            "caCertificate": "cert-pem",
            "clientCertificate": "cert-pem",
            "clientKey": "cert-pem",
            "connectRetryInterval": 30,
            "dumpFilePath": "/path/to/dump",
            "masterHeartbeatPeriod": 100,
            "password": "secret_pass",
            "sslCipher": "list-of-ciphers",
            "username": "user",
            "verifyServerCertificate": True,
        },
    },
}
PATCH_BODY = {
    "name": INSTANCE_NAME,
    "settings": {"tier": "db-n1-standard-2", "dataDiskType": "PD_HDD"},
    "region": "europe-west4",
}
DATABASE_INSERT_BODY = {
    "name": DB_NAME,  # The name of the database in the Cloud SQL instance.
    # This does not include the project ID or instance name.
    "project": PROJECT_ID,  # The project ID of the project containing the Cloud SQL
    # database. The Google apps domain is prefixed if
    # applicable.
    "instance": INSTANCE_NAME,  # The name of the Cloud SQL instance.
    # This does not include the project ID.
}
DATABASE_PATCH_BODY = {"charset": "utf16", "collation": "utf16_general_ci"}
EXPORT_BODY = {
    "exportContext": {
        "fileType": "CSV",
        "uri": "gs://bucketName/fileName",
        "databases": [],
        "sqlExportOptions": {
            "tables": ["table1", "table2"],
            "schemaOnly": False,
            "mysqlExportOptions": {
                "masterData": 1,
            },
        },
        "csvExportOptions": {
            "selectQuery": "SELECT * FROM TABLE",
            "escapeCharacter": "e",
            "quoteCharacter": "q",
            "fieldsTerminatedBy": "f",
            "linesTerminatedBy": "l",
        },
        "offload": True,
    }
}
IMPORT_BODY = {
    "importContext": {
        "fileType": "CSV",
        "uri": "gs://bucketName/fileName",
        "database": "db1",
        "importUser": "",
        "csvImportOptions": {"table": "my_table", "columns": ["col1", "col2"]},
    }
}


class TestCloudSql:
    @mock.patch(
        "airflow.providers.google.cloud.operators.cloud_sql"
        ".CloudSQLCreateInstanceOperator._check_if_instance_exists"
    )
    @mock.patch("airflow.providers.google.cloud.operators.cloud_sql.CloudSQLHook")
    def test_instance_create(self, mock_hook, _check_if_instance_exists):
        _check_if_instance_exists.return_value = False
        mock_hook.return_value.create_instance.return_value = True
        op = CloudSQLCreateInstanceOperator(
            project_id=PROJECT_ID, instance=INSTANCE_NAME, body=CREATE_BODY, task_id="id"
        )
        op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version="v1beta4",
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.create_instance.assert_called_once_with(
            project_id=PROJECT_ID, body=CREATE_BODY
        )

    @mock.patch(
        "airflow.providers.google.cloud.operators.cloud_sql"
        ".CloudSQLCreateInstanceOperator._check_if_instance_exists"
    )
    @mock.patch("airflow.providers.google.cloud.operators.cloud_sql.CloudSQLHook")
    def test_instance_create_missing_project_id(
        self, mock_hook, _check_if_instance_exists
    ):
        _check_if_instance_exists.return_value = False
        mock_hook.return_value.create_instance.return_value = True
        op = CloudSQLCreateInstanceOperator(
            instance=INSTANCE_NAME, body=CREATE_BODY, task_id="id"
        )
        op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version="v1beta4",
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.create_instance.assert_called_once_with(
            project_id=None, body=CREATE_BODY
        )

    @mock.patch(
        "airflow.providers.google.cloud.operators.cloud_sql"
        ".CloudSQLCreateInstanceOperator._check_if_instance_exists"
    )
    @mock.patch("airflow.providers.google.cloud.operators.cloud_sql.CloudSQLHook")
    def test_instance_create_idempotent(self, mock_hook, _check_if_instance_exists):
        _check_if_instance_exists.return_value = True
        mock_hook.return_value.create_instance.return_value = True
        op = CloudSQLCreateInstanceOperator(
            project_id=PROJECT_ID, instance=INSTANCE_NAME, body=CREATE_BODY, task_id="id"
        )
        op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version="v1beta4",
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.create_instance.assert_not_called()

    @mock.patch("airflow.providers.google.cloud.operators.cloud_sql.CloudSQLHook")
    def test_create_should_throw_ex_when_empty_project_id(self, mock_hook):
        with pytest.raises(
            AirflowException, match="The required parameter 'project_id' is empty"
        ):
            CloudSQLCreateInstanceOperator(
                project_id="", body=CREATE_BODY, instance=INSTANCE_NAME, task_id="id"
            )

    @mock.patch("airflow.providers.google.cloud.operators.cloud_sql.CloudSQLHook")
    def test_create_should_throw_ex_when_empty_body(self, mock_hook):
        with pytest.raises(
            AirflowException, match="The required parameter 'body' is empty"
        ):
            CloudSQLCreateInstanceOperator(
                project_id=PROJECT_ID, body={}, instance=INSTANCE_NAME, task_id="id"
            )

    @mock.patch("airflow.providers.google.cloud.operators.cloud_sql.CloudSQLHook")
    def test_create_should_throw_ex_when_empty_instance(self, mock_hook):
        with pytest.raises(
            AirflowException, match="The required parameter 'instance' is empty"
        ):
            CloudSQLCreateInstanceOperator(
                project_id=PROJECT_ID, body=CREATE_BODY, instance="", task_id="id"
            )

    @mock.patch("airflow.providers.google.cloud.operators.cloud_sql.CloudSQLHook")
    def test_create_should_validate_list_type(self, mock_hook):
        wrong_list_type_body = {
            "name": INSTANCE_NAME,
            "settings": {
                "tier": "db-n1-standard-1",
                "ipConfiguration": {
                    "authorizedNetworks": {}  # Should be a list, not a dict.
                    # Testing if the validation catches this.
                },
            },
        }
        op = CloudSQLCreateInstanceOperator(
            project_id=PROJECT_ID,
            body=wrong_list_type_body,
            instance=INSTANCE_NAME,
            task_id="id",
        )
        with pytest.raises(AirflowException) as ctx:
            op.execute(None)
        err = ctx.value
        assert (
            "The field 'settings.ipConfiguration.authorizedNetworks' "
            "should be of list type according to the specification" in str(err)
        )
        mock_hook.assert_called_once_with(
            api_version="v1beta4",
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )

    @mock.patch("airflow.providers.google.cloud.operators.cloud_sql.CloudSQLHook")
    def test_create_should_validate_non_empty_fields(self, mock_hook):
        empty_tier_body = {
            "name": INSTANCE_NAME,
            "settings": {
                "tier": "",  # Field can't be empty (defined in CLOUD_SQL_VALIDATION).
                # Testing if the validation catches this.
            },
        }
        op = CloudSQLCreateInstanceOperator(
            project_id=PROJECT_ID,
            body=empty_tier_body,
            instance=INSTANCE_NAME,
            task_id="id",
        )
        with pytest.raises(AirflowException) as ctx:
            op.execute(None)
        err = ctx.value
        assert (
            "The body field 'settings.tier' can't be empty. Please provide a value."
            in str(err)
        )
        mock_hook.assert_called_once_with(
            api_version="v1beta4",
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )

    @mock.patch("airflow.providers.google.cloud.operators.cloud_sql.CloudSQLHook")
    def test_instance_patch(self, mock_hook):
        mock_hook.return_value.patch_instance.return_value = True
        op = CloudSQLInstancePatchOperator(
            project_id=PROJECT_ID, body=PATCH_BODY, instance=INSTANCE_NAME, task_id="id"
        )
        result = op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version="v1beta4",
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.patch_instance.assert_called_once_with(
            project_id=PROJECT_ID, body=PATCH_BODY, instance=INSTANCE_NAME
        )
        assert result

    @mock.patch("airflow.providers.google.cloud.operators.cloud_sql.CloudSQLHook")
    def test_instance_patch_missing_project_id(self, mock_hook):
        mock_hook.return_value.patch_instance.return_value = True
        op = CloudSQLInstancePatchOperator(
            body=PATCH_BODY, instance=INSTANCE_NAME, task_id="id"
        )
        result = op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version="v1beta4",
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.patch_instance.assert_called_once_with(
            project_id=None, body=PATCH_BODY, instance=INSTANCE_NAME
        )
        assert result

    @mock.patch(
        "airflow.providers.google.cloud.operators.cloud_sql"
        ".CloudSQLInstancePatchOperator._check_if_instance_exists"
    )
    @mock.patch("airflow.providers.google.cloud.operators.cloud_sql.CloudSQLHook")
    def test_instance_patch_should_bubble_up_ex_if_not_exists(
        self, mock_hook, _check_if_instance_exists
    ):
        _check_if_instance_exists.return_value = False
        op = CloudSQLInstancePatchOperator(
            project_id=PROJECT_ID, body=PATCH_BODY, instance=INSTANCE_NAME, task_id="id"
        )
        with pytest.raises(AirflowException) as ctx:
            op.execute(None)
        err = ctx.value
        assert "specify another instance to patch" in str(err)
        mock_hook.assert_called_once_with(
            api_version="v1beta4",
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.patch_instance.assert_not_called()

    @mock.patch(
        "airflow.providers.google.cloud.operators.cloud_sql"
        ".CloudSQLDeleteInstanceOperator._check_if_instance_exists"
    )
    @mock.patch("airflow.providers.google.cloud.operators.cloud_sql.CloudSQLHook")
    def test_instance_delete(self, mock_hook, _check_if_instance_exists):
        _check_if_instance_exists.return_value = True
        op = CloudSQLDeleteInstanceOperator(
            project_id=PROJECT_ID, instance=INSTANCE_NAME, task_id="id"
        )
        result = op.execute(None)
        assert result
        mock_hook.assert_called_once_with(
            api_version="v1beta4",
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.delete_instance.assert_called_once_with(
            project_id=PROJECT_ID, instance=INSTANCE_NAME
        )

    @mock.patch(
        "airflow.providers.google.cloud.operators.cloud_sql.CloudSQLCloneInstanceOperator._check_if_instance_exists"
    )
    @mock.patch("airflow.providers.google.cloud.operators.cloud_sql.CloudSQLHook")
    def test_instance_clone(self, mock_hook, _check_if_instance_exists):
        destination_instance_name = "clone-test-name"
        _check_if_instance_exists.return_value = True
        op = CloudSQLCloneInstanceOperator(
            project_id=PROJECT_ID,
            instance=INSTANCE_NAME,
            destination_instance_name=destination_instance_name,
            task_id="id",
        )
        result = op.execute(None)
        assert result
        mock_hook.assert_called_once_with(
            api_version="v1beta4",
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        body = {
            "cloneContext": {
                "kind": "sql#cloneContext",
                "destinationInstanceName": destination_instance_name,
            }
        }
        mock_hook.return_value.clone_instance.assert_called_once_with(
            project_id=PROJECT_ID, instance=INSTANCE_NAME, body=body
        )

    @mock.patch(
        "airflow.providers.google.cloud.operators.cloud_sql"
        ".CloudSQLDeleteInstanceOperator._check_if_instance_exists"
    )
    @mock.patch("airflow.providers.google.cloud.operators.cloud_sql.CloudSQLHook")
    def test_instance_delete_missing_project_id(
        self, mock_hook, _check_if_instance_exists
    ):
        _check_if_instance_exists.return_value = True
        op = CloudSQLDeleteInstanceOperator(instance=INSTANCE_NAME, task_id="id")
        result = op.execute(None)
        assert result
        mock_hook.assert_called_once_with(
            api_version="v1beta4",
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.delete_instance.assert_called_once_with(
            project_id=None, instance=INSTANCE_NAME
        )

    @mock.patch(
        "airflow.providers.google.cloud.operators.cloud_sql"
        ".CloudSQLDeleteInstanceOperator._check_if_instance_exists"
    )
    @mock.patch("airflow.providers.google.cloud.operators.cloud_sql.CloudSQLHook")
    def test_instance_delete_should_abort_and_succeed_if_not_exists(
        self, mock_hook, _check_if_instance_exists
    ):
        _check_if_instance_exists.return_value = False
        op = CloudSQLDeleteInstanceOperator(
            project_id=PROJECT_ID, instance=INSTANCE_NAME, task_id="id"
        )
        result = op.execute(None)
        assert result
        mock_hook.assert_called_once_with(
            api_version="v1beta4",
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.delete_instance.assert_not_called()

    @mock.patch(
        "airflow.providers.google.cloud.operators.cloud_sql"
        ".CloudSQLCreateInstanceDatabaseOperator._check_if_db_exists"
    )
    @mock.patch("airflow.providers.google.cloud.operators.cloud_sql.CloudSQLHook")
    def test_instance_db_create(self, mock_hook, _check_if_db_exists):
        _check_if_db_exists.return_value = False
        op = CloudSQLCreateInstanceDatabaseOperator(
            project_id=PROJECT_ID,
            instance=INSTANCE_NAME,
            body=DATABASE_INSERT_BODY,
            task_id="id",
        )
        result = op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version="v1beta4",
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.create_database.assert_called_once_with(
            project_id=PROJECT_ID, instance=INSTANCE_NAME, body=DATABASE_INSERT_BODY
        )
        assert result

    @mock.patch(
        "airflow.providers.google.cloud.operators.cloud_sql"
        ".CloudSQLCreateInstanceDatabaseOperator._check_if_db_exists"
    )
    @mock.patch("airflow.providers.google.cloud.operators.cloud_sql.CloudSQLHook")
    def test_instance_db_create_missing_project_id(self, mock_hook, _check_if_db_exists):
        _check_if_db_exists.return_value = False
        op = CloudSQLCreateInstanceDatabaseOperator(
            instance=INSTANCE_NAME, body=DATABASE_INSERT_BODY, task_id="id"
        )
        result = op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version="v1beta4",
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.create_database.assert_called_once_with(
            project_id=None, instance=INSTANCE_NAME, body=DATABASE_INSERT_BODY
        )
        assert result

    @mock.patch(
        "airflow.providers.google.cloud.operators.cloud_sql"
        ".CloudSQLCreateInstanceDatabaseOperator._check_if_db_exists"
    )
    @mock.patch("airflow.providers.google.cloud.operators.cloud_sql.CloudSQLHook")
    def test_instance_db_create_should_abort_and_succeed_if_exists(
        self, mock_hook, _check_if_db_exists
    ):
        _check_if_db_exists.return_value = True
        op = CloudSQLCreateInstanceDatabaseOperator(
            project_id=PROJECT_ID,
            instance=INSTANCE_NAME,
            body=DATABASE_INSERT_BODY,
            task_id="id",
        )
        result = op.execute(context=mock.MagicMock())
        assert result
        mock_hook.assert_called_once_with(
            api_version="v1beta4",
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.create_database.assert_not_called()

    @mock.patch(
        "airflow.providers.google.cloud.operators.cloud_sql"
        ".CloudSQLPatchInstanceDatabaseOperator._check_if_db_exists"
    )
    @mock.patch("airflow.providers.google.cloud.operators.cloud_sql.CloudSQLHook")
    def test_instance_db_patch(self, mock_hook, _check_if_db_exists):
        _check_if_db_exists.return_value = True
        op = CloudSQLPatchInstanceDatabaseOperator(
            project_id=PROJECT_ID,
            instance=INSTANCE_NAME,
            database=DB_NAME,
            body=DATABASE_PATCH_BODY,
            task_id="id",
        )
        result = op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version="v1beta4",
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.patch_database.assert_called_once_with(
            project_id=PROJECT_ID,
            instance=INSTANCE_NAME,
            database=DB_NAME,
            body=DATABASE_PATCH_BODY,
        )
        assert result

    @mock.patch(
        "airflow.providers.google.cloud.operators.cloud_sql"
        ".CloudSQLPatchInstanceDatabaseOperator._check_if_db_exists"
    )
    @mock.patch("airflow.providers.google.cloud.operators.cloud_sql.CloudSQLHook")
    def test_instance_db_patch_missing_project_id(self, mock_hook, _check_if_db_exists):
        _check_if_db_exists.return_value = True
        op = CloudSQLPatchInstanceDatabaseOperator(
            instance=INSTANCE_NAME,
            database=DB_NAME,
            body=DATABASE_PATCH_BODY,
            task_id="id",
        )
        result = op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version="v1beta4",
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.patch_database.assert_called_once_with(
            project_id=None,
            instance=INSTANCE_NAME,
            database=DB_NAME,
            body=DATABASE_PATCH_BODY,
        )
        assert result

    @mock.patch(
        "airflow.providers.google.cloud.operators.cloud_sql"
        ".CloudSQLPatchInstanceDatabaseOperator._check_if_db_exists"
    )
    @mock.patch("airflow.providers.google.cloud.operators.cloud_sql.CloudSQLHook")
    def test_instance_db_patch_should_throw_ex_if_not_exists(
        self, mock_hook, _check_if_db_exists
    ):
        _check_if_db_exists.return_value = False
        op = CloudSQLPatchInstanceDatabaseOperator(
            project_id=PROJECT_ID,
            instance=INSTANCE_NAME,
            database=DB_NAME,
            body=DATABASE_PATCH_BODY,
            task_id="id",
        )
        with pytest.raises(AirflowException) as ctx:
            op.execute(None)
        err = ctx.value
        assert "Cloud SQL instance with ID" in str(err)
        assert "does not contain database" in str(err)
        mock_hook.assert_called_once_with(
            api_version="v1beta4",
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.patch_database.assert_not_called()

    def test_instance_db_patch_should_throw_ex_when_empty_database(self):
        with pytest.raises(
            AirflowException, match="The required parameter 'database' is empty"
        ):
            CloudSQLPatchInstanceDatabaseOperator(
                project_id=PROJECT_ID,
                instance=INSTANCE_NAME,
                database="",
                body=DATABASE_INSERT_BODY,
                task_id="id",
            )

    @mock.patch(
        "airflow.providers.google.cloud.operators.cloud_sql"
        ".CloudSQLDeleteInstanceDatabaseOperator._check_if_db_exists"
    )
    @mock.patch("airflow.providers.google.cloud.operators.cloud_sql.CloudSQLHook")
    def test_instance_db_delete(self, mock_hook, _check_if_db_exists):
        _check_if_db_exists.return_value = True
        op = CloudSQLDeleteInstanceDatabaseOperator(
            project_id=PROJECT_ID, instance=INSTANCE_NAME, database=DB_NAME, task_id="id"
        )
        result = op.execute(None)
        assert result
        mock_hook.assert_called_once_with(
            api_version="v1beta4",
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.delete_database.assert_called_once_with(
            project_id=PROJECT_ID, instance=INSTANCE_NAME, database=DB_NAME
        )

    @mock.patch(
        "airflow.providers.google.cloud.operators.cloud_sql"
        ".CloudSQLDeleteInstanceDatabaseOperator._check_if_db_exists"
    )
    @mock.patch("airflow.providers.google.cloud.operators.cloud_sql.CloudSQLHook")
    def test_instance_db_delete_missing_project_id(self, mock_hook, _check_if_db_exists):
        _check_if_db_exists.return_value = True
        op = CloudSQLDeleteInstanceDatabaseOperator(
            instance=INSTANCE_NAME, database=DB_NAME, task_id="id"
        )
        result = op.execute(None)
        assert result
        mock_hook.assert_called_once_with(
            api_version="v1beta4",
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.delete_database.assert_called_once_with(
            project_id=None, instance=INSTANCE_NAME, database=DB_NAME
        )

    @mock.patch(
        "airflow.providers.google.cloud.operators.cloud_sql"
        ".CloudSQLDeleteInstanceDatabaseOperator._check_if_db_exists"
    )
    @mock.patch("airflow.providers.google.cloud.operators.cloud_sql.CloudSQLHook")
    def test_instance_db_delete_should_abort_and_succeed_if_not_exists(
        self, mock_hook, _check_if_db_exists
    ):
        _check_if_db_exists.return_value = False
        op = CloudSQLDeleteInstanceDatabaseOperator(
            project_id=PROJECT_ID, instance=INSTANCE_NAME, database=DB_NAME, task_id="id"
        )
        result = op.execute(None)
        assert result
        mock_hook.assert_called_once_with(
            api_version="v1beta4",
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.delete_database.assert_not_called()

    @mock.patch("airflow.providers.google.cloud.operators.cloud_sql.CloudSQLHook")
    def test_instance_export(self, mock_hook):
        mock_hook.return_value.export_instance.return_value = True
        op = CloudSQLExportInstanceOperator(
            project_id=PROJECT_ID, instance=INSTANCE_NAME, body=EXPORT_BODY, task_id="id"
        )
        result = op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version="v1beta4",
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.export_instance.assert_called_once_with(
            project_id=PROJECT_ID, instance=INSTANCE_NAME, body=EXPORT_BODY
        )
        assert result

    @mock.patch("airflow.providers.google.cloud.operators.cloud_sql.CloudSQLHook")
    def test_instance_export_missing_project_id(self, mock_hook):
        mock_hook.return_value.export_instance.return_value = True
        op = CloudSQLExportInstanceOperator(
            instance=INSTANCE_NAME, body=EXPORT_BODY, task_id="id"
        )
        result = op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version="v1beta4",
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.export_instance.assert_called_once_with(
            project_id=None, instance=INSTANCE_NAME, body=EXPORT_BODY
        )
        assert result

    @mock.patch("airflow.providers.google.cloud.operators.cloud_sql.CloudSQLHook")
    @mock.patch("airflow.providers.google.cloud.triggers.cloud_sql.CloudSQLAsyncHook")
    def test_execute_call_defer_method(self, mock_trigger_hook, mock_hook):
        operator = CloudSQLExportInstanceOperator(
            task_id="test_task",
            instance=INSTANCE_NAME,
            body=EXPORT_BODY,
            deferrable=True,
        )

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(mock.MagicMock())

        mock_hook.return_value.export_instance.assert_called_once()

        mock_hook.return_value.get_operation.assert_not_called()
        assert isinstance(exc.value.trigger, CloudSQLExportTrigger)
        assert exc.value.method_name == GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME

    def test_async_execute_should_should_throw_exception(self):
        """Tests that an AirflowException is raised in case of error event"""

        op = CloudSQLExportInstanceOperator(
            task_id="test_task",
            instance=INSTANCE_NAME,
            body=EXPORT_BODY,
            deferrable=True,
        )
        with pytest.raises(AirflowException):
            op.execute_complete(
                context=mock.MagicMock(),
                event={"status": "error", "message": "test failure message"},
            )

    @mock.patch("airflow.providers.google.cloud.operators.cloud_sql.CloudSQLHook")
    def test_instance_import(self, mock_hook):
        mock_hook.return_value.export_instance.return_value = True
        op = CloudSQLImportInstanceOperator(
            project_id=PROJECT_ID, instance=INSTANCE_NAME, body=IMPORT_BODY, task_id="id"
        )
        result = op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version="v1beta4",
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.import_instance.assert_called_once_with(
            project_id=PROJECT_ID, instance=INSTANCE_NAME, body=IMPORT_BODY
        )
        assert result

    @mock.patch("airflow.providers.google.cloud.operators.cloud_sql.CloudSQLHook")
    def test_instance_import_missing_project_id(self, mock_hook):
        mock_hook.return_value.export_instance.return_value = True
        op = CloudSQLImportInstanceOperator(
            instance=INSTANCE_NAME, body=IMPORT_BODY, task_id="id"
        )
        result = op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            api_version="v1beta4",
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.return_value.import_instance.assert_called_once_with(
            project_id=None, instance=INSTANCE_NAME, body=IMPORT_BODY
        )
        assert result


class TestCloudSqlQueryValidation:
    @staticmethod
    def _setup_connections(get_connection, uri):
        gcp_connection = mock.MagicMock()
        gcp_connection.extra_dejson = mock.MagicMock()
        gcp_connection.extra_dejson.get.return_value = "empty_project"
        cloudsql_connection = Connection(uri=uri)
        cloudsql_connection2 = Connection(uri=uri)
        get_connection.side_effect = [
            gcp_connection,
            cloudsql_connection,
            cloudsql_connection2,
        ]

    @pytest.mark.parametrize(
        "project_id, location, instance_name, database_type, use_proxy, use_ssl, sql, message",
        [
            (
                "project_id",
                "",
                "instance_name",
                "mysql",
                False,
                False,
                "SELECT * FROM TEST",
                "The required extra 'location' is empty",
            ),
            (
                "project_id",
                "location",
                "",
                "postgres",
                False,
                False,
                "SELECT * FROM TEST",
                "The required extra 'instance' is empty",
            ),
            (
                "project_id",
                "location",
                "instance_name",
                "wrong",
                False,
                False,
                "SELECT * FROM TEST",
                "Invalid database type 'wrong'. Must be one of ['postgres', 'mysql']",
            ),
            (
                "project_id",
                "location",
                "instance_name",
                "postgres",
                True,
                True,
                "SELECT * FROM TEST",
                "Cloud SQL Proxy does not support SSL connections. SSL is not needed as"
                " Cloud SQL Proxy provides encryption on its own",
            ),
            (
                "project_id",
                "location",
                "instance_name",
                "postgres",
                False,
                True,
                "SELECT * FROM TEST",
                "SSL connections requires sslcert to be set",
            ),
        ],
    )
    @mock.patch("airflow.hooks.base.BaseHook.get_connection")
    def test_create_operator_with_wrong_parameters(
        self,
        get_connection,
        project_id,
        location,
        instance_name,
        database_type,
        use_proxy,
        use_ssl,
        sql,
        message,
    ):
        uri = (
            f"gcpcloudsql://user:password@127.0.0.1:3200/testdb?"
            f"database_type={database_type}&project_id={project_id}&location={location}"
            f"&instance={instance_name}&use_proxy={use_proxy}&use_ssl={use_ssl}"
        )
        self._setup_connections(get_connection, uri)
        op = CloudSQLExecuteQueryOperator(sql=sql, task_id="task_id")
        with pytest.raises(AirflowException) as ctx:
            op.execute(None)
        err = ctx.value
        assert message in str(err)

    @mock.patch("airflow.hooks.base.BaseHook.get_connection")
    def test_create_operator_with_too_long_unix_socket_path(self, get_connection):
        uri = (
            "gcpcloudsql://user:password@127.0.0.1:3200/testdb?database_type=postgres&"
            "project_id=example-project&location=europe-west1&"
            "instance=test_db_with_long_name_a_bit_above_the_limit_of_UNIX_socket_asdadadasadasd&"
            "use_proxy=True&sql_proxy_use_tcp=False"
        )
        self._setup_connections(get_connection, uri)
        operator = CloudSQLExecuteQueryOperator(
            sql=["SELECT * FROM TABLE"], task_id="task_id"
        )
        with pytest.raises(AirflowException) as ctx:
            operator.execute(None)
        err = ctx.value
        assert "The UNIX socket path length cannot exceed" in str(err)
