# -*- coding: utf-8 -*-
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
import json
import os
import unittest

from parameterized import parameterized

from airflow import AirflowException
from airflow.contrib.operators.gcp_sql_operator import CloudSqlInstanceCreateOperator, \
    CloudSqlInstancePatchOperator, CloudSqlInstanceDeleteOperator, \
    CloudSqlInstanceDatabaseCreateOperator, CloudSqlInstanceDatabasePatchOperator, \
    CloudSqlInstanceExportOperator, CloudSqlInstanceImportOperator, \
    CloudSqlInstanceDatabaseDeleteOperator, CloudSqlQueryOperator
from airflow.models.connection import Connection

try:
    # noinspection PyProtectedMember
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

PROJECT_ID = os.environ.get('PROJECT_ID', 'project-id')
INSTANCE_NAME = os.environ.get('INSTANCE_NAME', 'test-name')
DB_NAME = os.environ.get('DB_NAME', 'db1')

CREATE_BODY = {
    "name": INSTANCE_NAME,
    "settings": {
        "tier": "db-n1-standard-1",
        "backupConfiguration": {
            "binaryLogEnabled": True,
            "enabled": True,
            "replicationLogArchivingEnabled": True,
            "startTime": "05:00"
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
                    "expirationTime": "2012-11-15T16:19:00.094Z"
                },
            ],
            "privateNetwork": "/vpc/resource/link",
            "requireSsl": True
        },
        "locationPreference": {
            "zone": "europe-west4-a",
            "followGaeApplication": "/app/engine/application/to/follow"
        },
        "maintenanceWindow": {
            "hour": 5,
            "day": 7,
            "updateTrack": "canary"
        },
        "pricingPlan": "PER_USE",
        "replicationType": "ASYNCHRONOUS",
        "storageAutoResize": False,
        "storageAutoResizeLimit": 0,
        "userLabels": {
            "my-key": "my-value"
        }
    },
    "databaseVersion": "MYSQL_5_7",
    "failoverReplica": {
        "name": "replica-1"
    },
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
            "verifyServerCertificate": True
        },
    }
}
PATCH_BODY = {
    "name": INSTANCE_NAME,
    "settings": {
        "tier": "db-n1-standard-2",
        "dataDiskType": "PD_HDD"
    },
    "region": "europe-west4"
}
DATABASE_INSERT_BODY = {
    "name": DB_NAME,            # The name of the database in the Cloud SQL instance.
                                # This does not include the project ID or instance name.

    "project": PROJECT_ID,      # The project ID of the project containing the Cloud SQL
                                # database. The Google apps domain is prefixed if
                                # applicable.

    "instance": INSTANCE_NAME,  # The name of the Cloud SQL instance.
                                # This does not include the project ID.
}
DATABASE_PATCH_BODY = {
    "charset": "utf16",
    "collation": "utf16_general_ci"
}
EXPORT_BODY = {
    "exportContext": {
        "fileType": "CSV",
        "uri": "gs://bucketName/fileName",
        "databases": [],
        "sqlExportOptions": {
            "tables": [
                "table1", "table2"
            ],
            "schemaOnly": False
        },
        "csvExportOptions": {
            "selectQuery": "SELECT * FROM TABLE"
        }
    }
}
IMPORT_BODY = {
    "importContext": {
        "fileType": "CSV",
        "uri": "gs://bucketName/fileName",
        "database": "db1",
        "importUser": "",
        "csvImportOptions": {
            "table": "my_table",
            "columns": [
                "col1", "col2"
            ]
        }
    }
}


class CloudSqlTest(unittest.TestCase):
    @mock.patch("airflow.contrib.operators.gcp_sql_operator"
                ".CloudSqlInstanceCreateOperator._check_if_instance_exists")
    @mock.patch("airflow.contrib.operators.gcp_sql_operator.CloudSqlHook")
    def test_instance_create(self, mock_hook, _check_if_instance_exists):
        _check_if_instance_exists.return_value = False
        mock_hook.return_value.create_instance.return_value = True
        op = CloudSqlInstanceCreateOperator(
            project_id=PROJECT_ID,
            instance=INSTANCE_NAME,
            body=CREATE_BODY,
            task_id="id"
        )
        result = op.execute(context={
            'task_instance': mock.Mock()
        })
        mock_hook.assert_called_once_with(api_version="v1beta4",
                                          gcp_conn_id="google_cloud_default")
        mock_hook.return_value.create_instance.assert_called_once_with(
            PROJECT_ID, CREATE_BODY
        )
        self.assertIsNone(result)

    @mock.patch("airflow.contrib.operators.gcp_sql_operator"
                ".CloudSqlInstanceCreateOperator._check_if_instance_exists")
    @mock.patch("airflow.contrib.operators.gcp_sql_operator.CloudSqlHook")
    def test_instance_create_idempotent(self, mock_hook, _check_if_instance_exists):
        _check_if_instance_exists.return_value = True
        mock_hook.return_value.create_instance.return_value = True
        op = CloudSqlInstanceCreateOperator(
            project_id=PROJECT_ID,
            instance=INSTANCE_NAME,
            body=CREATE_BODY,
            task_id="id"
        )
        result = op.execute(context={
            'task_instance': mock.Mock()
        })
        mock_hook.assert_called_once_with(api_version="v1beta4",
                                          gcp_conn_id="google_cloud_default")
        mock_hook.return_value.create_instance.assert_not_called()
        self.assertIsNone(result)

    @mock.patch("airflow.contrib.operators.gcp_sql_operator.CloudSqlHook")
    def test_create_should_throw_ex_when_empty_project_id(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            op = CloudSqlInstanceCreateOperator(
                project_id="",
                body=CREATE_BODY,
                instance=INSTANCE_NAME,
                task_id="id"
            )
            op.execute(None)
        err = cm.exception
        self.assertIn("The required parameter 'project_id' is empty", str(err))
        mock_hook.assert_not_called()

    @mock.patch("airflow.contrib.operators.gcp_sql_operator.CloudSqlHook")
    def test_create_should_throw_ex_when_empty_body(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            op = CloudSqlInstanceCreateOperator(
                project_id=PROJECT_ID,
                body={},
                instance=INSTANCE_NAME,
                task_id="id"
            )
            op.execute(None)
        err = cm.exception
        self.assertIn("The required parameter 'body' is empty", str(err))
        mock_hook.assert_not_called()

    @mock.patch("airflow.contrib.operators.gcp_sql_operator.CloudSqlHook")
    def test_create_should_throw_ex_when_empty_instance(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            op = CloudSqlInstanceCreateOperator(
                project_id=PROJECT_ID,
                body=CREATE_BODY,
                instance="",
                task_id="id"
            )
            op.execute(None)
        err = cm.exception
        self.assertIn("The required parameter 'instance' is empty", str(err))
        mock_hook.assert_not_called()

    @mock.patch("airflow.contrib.operators.gcp_sql_operator.CloudSqlHook")
    def test_create_should_validate_list_type(self, mock_hook):
        wrong_list_type_body = {
            "name": INSTANCE_NAME,
            "settings": {
                "tier": "db-n1-standard-1",
                "ipConfiguration": {
                    "authorizedNetworks": {}  # Should be a list, not a dict.
                    # Testing if the validation catches this.
                }
            }
        }
        with self.assertRaises(AirflowException) as cm:
            op = CloudSqlInstanceCreateOperator(
                project_id=PROJECT_ID,
                body=wrong_list_type_body,
                instance=INSTANCE_NAME,
                task_id="id"
            )
            op.execute(None)
        err = cm.exception
        self.assertIn("The field 'settings.ipConfiguration.authorizedNetworks' "
                      "should be of list type according to the specification", str(err))
        mock_hook.assert_called_once_with(api_version="v1beta4",
                                          gcp_conn_id="google_cloud_default")

    @mock.patch("airflow.contrib.operators.gcp_sql_operator.CloudSqlHook")
    def test_create_should_validate_non_empty_fields(self, mock_hook):
        empty_tier_body = {
            "name": INSTANCE_NAME,
            "settings": {
                "tier": "",  # Field can't be empty (defined in CLOUD_SQL_VALIDATION).
                # Testing if the validation catches this.
            }
        }
        with self.assertRaises(AirflowException) as cm:
            op = CloudSqlInstanceCreateOperator(
                project_id=PROJECT_ID,
                body=empty_tier_body,
                instance=INSTANCE_NAME,
                task_id="id"
            )
            op.execute(None)
        err = cm.exception
        self.assertIn("The body field 'settings.tier' can't be empty. "
                      "Please provide a value.", str(err))
        mock_hook.assert_called_once_with(api_version="v1beta4",
                                          gcp_conn_id="google_cloud_default")

    @mock.patch("airflow.contrib.operators.gcp_sql_operator.CloudSqlHook")
    def test_instance_patch(self, mock_hook):
        mock_hook.return_value.patch_instance.return_value = True
        op = CloudSqlInstancePatchOperator(
            project_id=PROJECT_ID,
            body=PATCH_BODY,
            instance=INSTANCE_NAME,
            task_id="id"
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version="v1beta4",
                                          gcp_conn_id="google_cloud_default")
        mock_hook.return_value.patch_instance.assert_called_once_with(
            PROJECT_ID, PATCH_BODY, INSTANCE_NAME
        )
        self.assertTrue(result)

    @mock.patch("airflow.contrib.operators.gcp_sql_operator"
                ".CloudSqlInstancePatchOperator._check_if_instance_exists")
    @mock.patch("airflow.contrib.operators.gcp_sql_operator.CloudSqlHook")
    def test_instance_patch_should_bubble_up_ex_if_not_exists(self, mock_hook,
                                                              _check_if_instance_exists):
        _check_if_instance_exists.return_value = False
        with self.assertRaises(AirflowException) as cm:
            op = CloudSqlInstancePatchOperator(
                project_id=PROJECT_ID,
                body=PATCH_BODY,
                instance=INSTANCE_NAME,
                task_id="id"
            )
            op.execute(None)
        err = cm.exception
        self.assertIn('specify another instance to patch', str(err))
        mock_hook.assert_called_once_with(api_version="v1beta4",
                                          gcp_conn_id="google_cloud_default")
        mock_hook.return_value.patch_instance.assert_not_called()

    @mock.patch("airflow.contrib.operators.gcp_sql_operator"
                ".CloudSqlInstanceDeleteOperator._check_if_instance_exists")
    @mock.patch("airflow.contrib.operators.gcp_sql_operator.CloudSqlHook")
    def test_instance_delete(self, mock_hook, _check_if_instance_exists):
        _check_if_instance_exists.return_value = True
        op = CloudSqlInstanceDeleteOperator(
            project_id=PROJECT_ID,
            instance=INSTANCE_NAME,
            task_id="id"
        )
        result = op.execute(None)
        self.assertTrue(result)
        mock_hook.assert_called_once_with(api_version="v1beta4",
                                          gcp_conn_id="google_cloud_default")
        mock_hook.return_value.delete_instance.assert_called_once_with(
            PROJECT_ID, INSTANCE_NAME
        )

    @mock.patch("airflow.contrib.operators.gcp_sql_operator"
                ".CloudSqlInstanceDeleteOperator._check_if_instance_exists")
    @mock.patch("airflow.contrib.operators.gcp_sql_operator.CloudSqlHook")
    def test_instance_delete_should_abort_and_succeed_if_not_exists(
            self,
            mock_hook,
            _check_if_instance_exists):
        _check_if_instance_exists.return_value = False
        op = CloudSqlInstanceDeleteOperator(
            project_id=PROJECT_ID,
            instance=INSTANCE_NAME,
            task_id="id"
        )
        result = op.execute(None)
        self.assertTrue(result)
        mock_hook.assert_called_once_with(api_version="v1beta4",
                                          gcp_conn_id="google_cloud_default")
        mock_hook.return_value.delete_instance.assert_not_called()

    @mock.patch("airflow.contrib.operators.gcp_sql_operator"
                ".CloudSqlInstanceDatabaseCreateOperator._check_if_db_exists")
    @mock.patch("airflow.contrib.operators.gcp_sql_operator.CloudSqlHook")
    def test_instance_db_create(self, mock_hook, _check_if_db_exists):
        _check_if_db_exists.return_value = False
        op = CloudSqlInstanceDatabaseCreateOperator(
            project_id=PROJECT_ID,
            instance=INSTANCE_NAME,
            body=DATABASE_INSERT_BODY,
            task_id="id"
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version="v1beta4",
                                          gcp_conn_id="google_cloud_default")
        mock_hook.return_value.create_database.assert_called_once_with(
            PROJECT_ID, INSTANCE_NAME, DATABASE_INSERT_BODY
        )
        self.assertTrue(result)

    @mock.patch("airflow.contrib.operators.gcp_sql_operator"
                ".CloudSqlInstanceDatabaseCreateOperator._check_if_db_exists")
    @mock.patch("airflow.contrib.operators.gcp_sql_operator.CloudSqlHook")
    def test_instance_db_create_should_abort_and_succeed_if_exists(
            self, mock_hook, _check_if_db_exists):
        _check_if_db_exists.return_value = True
        op = CloudSqlInstanceDatabaseCreateOperator(
            project_id=PROJECT_ID,
            instance=INSTANCE_NAME,
            body=DATABASE_INSERT_BODY,
            task_id="id"
        )
        result = op.execute(None)
        self.assertTrue(result)
        mock_hook.assert_called_once_with(api_version="v1beta4",
                                          gcp_conn_id="google_cloud_default")
        mock_hook.return_value.create_database.assert_not_called()

    @mock.patch("airflow.contrib.operators.gcp_sql_operator"
                ".CloudSqlInstanceDatabasePatchOperator._check_if_db_exists")
    @mock.patch("airflow.contrib.operators.gcp_sql_operator.CloudSqlHook")
    def test_instance_db_patch(self, mock_hook, _check_if_db_exists):
        _check_if_db_exists.return_value = True
        op = CloudSqlInstanceDatabasePatchOperator(
            project_id=PROJECT_ID,
            instance=INSTANCE_NAME,
            database=DB_NAME,
            body=DATABASE_PATCH_BODY,
            task_id="id"
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version="v1beta4",
                                          gcp_conn_id="google_cloud_default")
        mock_hook.return_value.patch_database.assert_called_once_with(
            PROJECT_ID, INSTANCE_NAME, DB_NAME, DATABASE_PATCH_BODY
        )
        self.assertTrue(result)

    @mock.patch("airflow.contrib.operators.gcp_sql_operator"
                ".CloudSqlInstanceDatabasePatchOperator._check_if_db_exists")
    @mock.patch("airflow.contrib.operators.gcp_sql_operator.CloudSqlHook")
    def test_instance_db_patch_should_throw_ex_if_not_exists(
            self, mock_hook, _check_if_db_exists):
        _check_if_db_exists.return_value = False
        with self.assertRaises(AirflowException) as cm:
            op = CloudSqlInstanceDatabasePatchOperator(
                project_id=PROJECT_ID,
                instance=INSTANCE_NAME,
                database=DB_NAME,
                body=DATABASE_PATCH_BODY,
                task_id="id"
            )
            op.execute(None)
        err = cm.exception
        self.assertIn("Cloud SQL instance with ID", str(err))
        self.assertIn("does not contain database", str(err))
        mock_hook.assert_called_once_with(api_version="v1beta4",
                                          gcp_conn_id="google_cloud_default")
        mock_hook.return_value.patch_database.assert_not_called()

    @mock.patch("airflow.contrib.operators.gcp_sql_operator.CloudSqlHook")
    def test_instance_db_patch_should_throw_ex_when_empty_database(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            op = CloudSqlInstanceDatabasePatchOperator(
                project_id=PROJECT_ID,
                instance=INSTANCE_NAME,
                database="",
                body=DATABASE_INSERT_BODY,
                task_id="id"
            )
            op.execute(None)
        err = cm.exception
        self.assertIn("The required parameter 'database' is empty", str(err))
        mock_hook.assert_not_called()
        mock_hook.return_value.patch_database.assert_not_called()

    @mock.patch("airflow.contrib.operators.gcp_sql_operator"
                ".CloudSqlInstanceDatabaseDeleteOperator._check_if_db_exists")
    @mock.patch("airflow.contrib.operators.gcp_sql_operator.CloudSqlHook")
    def test_instance_db_delete(self, mock_hook, _check_if_db_exists):
        _check_if_db_exists.return_value = True
        op = CloudSqlInstanceDatabaseDeleteOperator(
            project_id=PROJECT_ID,
            instance=INSTANCE_NAME,
            database=DB_NAME,
            task_id="id"
        )
        result = op.execute(None)
        self.assertTrue(result)
        mock_hook.assert_called_once_with(api_version="v1beta4",
                                          gcp_conn_id="google_cloud_default")
        mock_hook.return_value.delete_database.assert_called_once_with(
            PROJECT_ID, INSTANCE_NAME, DB_NAME
        )

    @mock.patch("airflow.contrib.operators.gcp_sql_operator"
                ".CloudSqlInstanceDatabaseDeleteOperator._check_if_db_exists")
    @mock.patch("airflow.contrib.operators.gcp_sql_operator.CloudSqlHook")
    def test_instance_db_delete_should_abort_and_succeed_if_not_exists(
            self, mock_hook, _check_if_db_exists):
        _check_if_db_exists.return_value = False
        op = CloudSqlInstanceDatabaseDeleteOperator(
            project_id=PROJECT_ID,
            instance=INSTANCE_NAME,
            database=DB_NAME,
            task_id="id"
        )
        result = op.execute(None)
        self.assertTrue(result)
        mock_hook.assert_called_once_with(api_version="v1beta4",
                                          gcp_conn_id="google_cloud_default")
        mock_hook.return_value.delete_database.assert_not_called()

    @mock.patch("airflow.contrib.operators.gcp_sql_operator.CloudSqlHook")
    def test_instance_export(self, mock_hook):
        mock_hook.return_value.export_instance.return_value = True
        op = CloudSqlInstanceExportOperator(
            project_id=PROJECT_ID,
            instance=INSTANCE_NAME,
            body=EXPORT_BODY,
            task_id="id"
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version="v1beta4",
                                          gcp_conn_id="google_cloud_default")
        mock_hook.return_value.export_instance.assert_called_once_with(
            PROJECT_ID, INSTANCE_NAME, EXPORT_BODY
        )
        self.assertTrue(result)

    @mock.patch("airflow.contrib.operators.gcp_sql_operator.CloudSqlHook")
    def test_instance_import(self, mock_hook):
        mock_hook.return_value.export_instance.return_value = True
        op = CloudSqlInstanceImportOperator(
            project_id=PROJECT_ID,
            instance=INSTANCE_NAME,
            body=IMPORT_BODY,
            task_id="id"
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version="v1beta4",
                                          gcp_conn_id="google_cloud_default")
        mock_hook.return_value.import_instance.assert_called_once_with(
            PROJECT_ID, INSTANCE_NAME, IMPORT_BODY
        )
        self.assertTrue(result)


class CloudSqlQueryValidationTest(unittest.TestCase):

    @staticmethod
    def _setup_connections(get_connections, uri):
        cloudsql_connection = Connection()
        cloudsql_connection.parse_from_uri(uri)
        get_connections.side_effect = [[cloudsql_connection]]

    @parameterized.expand([
        ('', 'location', 'instance_name', 'postgres', False, False,
         'SELECT * FROM TEST',
         "The required extra 'project_id' is empty"),
        ('project_id', '', 'instance_name', 'mysql', False, False,
         'SELECT * FROM TEST',
         "The required extra 'location' is empty"),
        ('project_id', 'location', '', 'postgres', False, False,
         'SELECT * FROM TEST',
         "The required extra 'instance' is empty"),
        ('project_id', 'location', 'instance_name', 'wrong', False, False,
         'SELECT * FROM TEST',
         "Invalid database type 'wrong'. Must be one of ['postgres', 'mysql']"),
        ('project_id', 'location', 'instance_name', 'postgres', True, True,
         'SELECT * FROM TEST',
         "Cloud SQL Proxy does not support SSL connections. SSL is not needed as"
         " Cloud SQL Proxy provides encryption on its own"),
        ('project_id', 'location', 'instance_name', 'postgres', False, True,
         'SELECT * FROM TEST',
         "SSL connections requires sslcert to be set"),
    ])
    @mock.patch("airflow.hooks.base_hook.BaseHook.get_connections")
    def test_create_operator_with_wrong_parameters(self,
                                                   project_id,
                                                   location,
                                                   instance_name,
                                                   database_type,
                                                   use_proxy,
                                                   use_ssl,
                                                   sql,
                                                   message,
                                                   get_connections):
        uri = \
            "gcpcloudsql://user:password@8.8.8.8:3200/testdb?" \
            "database_type={database_type}&" \
            "project_id={project_id}&location={location}&instance={instance_name}&" \
            "use_proxy={use_proxy}&use_ssl={use_ssl}".format(
                database_type=database_type,
                project_id=project_id,
                location=location,
                instance_name=instance_name,
                use_proxy=use_proxy,
                use_ssl=use_ssl)
        self._setup_connections(get_connections, uri)
        with self.assertRaises(AirflowException) as cm:
            op = CloudSqlQueryOperator(
                sql=sql,
                task_id='task_id'
            )
            op.execute(None)
        err = cm.exception
        self.assertIn(message, str(err))

    @mock.patch("airflow.hooks.base_hook.BaseHook.get_connections")
    def test_create_operator_with_correct_parameters_postgres(self, get_connections):
        uri = "gcpcloudsql://user:password@8.8.8.8:3200/testdb?database_type=postgres&" \
              "project_id=example-project&location=europe-west1&instance=testdb&" \
              "use_proxy=False&use_ssl=False"
        self._setup_connections(get_connections, uri)
        operator = CloudSqlQueryOperator(
            sql=['SELECT * FROM TABLE'],
            task_id='task_id'
        )
        operator.cloudsql_db_hook.create_connection()
        try:
            db_hook = operator.cloudsql_db_hook.get_database_hook()
            conn = db_hook._get_connections_from_db(db_hook.postgres_conn_id)[0]
        finally:
            operator.cloudsql_db_hook.delete_connection()
        self.assertEqual('postgres', conn.conn_type)
        self.assertEqual('8.8.8.8', conn.host)
        self.assertEqual(3200, conn.port)
        self.assertEqual('testdb', conn.schema)

    @mock.patch("airflow.hooks.base_hook.BaseHook.get_connections")
    def test_create_operator_with_correct_parameters_postgres_ssl(self, get_connections):
        uri = "gcpcloudsql://user:password@8.8.8.8:3200/testdb?database_type=postgres&" \
              "project_id=example-project&location=europe-west1&instance=testdb&" \
              "use_proxy=False&use_ssl=True&sslcert=/bin/bash&" \
              "sslkey=/bin/bash&sslrootcert=/bin/bash"
        self._setup_connections(get_connections, uri)
        operator = CloudSqlQueryOperator(
            sql=['SELECT * FROM TABLE'],
            task_id='task_id'
        )
        operator.cloudsql_db_hook.create_connection()
        try:
            db_hook = operator.cloudsql_db_hook.get_database_hook()
            conn = db_hook._get_connections_from_db(db_hook.postgres_conn_id)[0]
        finally:
            operator.cloudsql_db_hook.delete_connection()
        self.assertEqual('postgres', conn.conn_type)
        self.assertEqual('8.8.8.8', conn.host)
        self.assertEqual(3200, conn.port)
        self.assertEqual('testdb', conn.schema)
        self.assertEqual('/bin/bash', conn.extra_dejson['sslkey'])
        self.assertEqual('/bin/bash', conn.extra_dejson['sslcert'])
        self.assertEqual('/bin/bash', conn.extra_dejson['sslrootcert'])

    @mock.patch("airflow.hooks.base_hook.BaseHook.get_connections")
    def test_create_operator_with_correct_parameters_postgres_proxy_socket(
            self, get_connections):
        uri = "gcpcloudsql://user:password@8.8.8.8:3200/testdb?database_type=postgres&" \
              "project_id=example-project&location=europe-west1&instance=testdb&" \
              "use_proxy=True&sql_proxy_use_tcp=False"
        self._setup_connections(get_connections, uri)
        operator = CloudSqlQueryOperator(
            sql=['SELECT * FROM TABLE'],
            task_id='task_id'
        )
        operator.cloudsql_db_hook.create_connection()
        try:
            db_hook = operator.cloudsql_db_hook.get_database_hook()
            conn = db_hook._get_connections_from_db(db_hook.postgres_conn_id)[0]
        finally:
            operator.cloudsql_db_hook.delete_connection()
        self.assertEqual('postgres', conn.conn_type)
        self.assertIn('/tmp', conn.host)
        self.assertIn('example-project:europe-west1:testdb', conn.host)
        self.assertIsNone(conn.port)
        self.assertEqual('testdb', conn.schema)

    @mock.patch("airflow.hooks.base_hook.BaseHook.get_connections")
    def test_create_operator_with_correct_parameters_postgres_proxy_tcp(self,
                                                                        get_connections):
        uri = "gcpcloudsql://user:password@8.8.8.8:3200/testdb?database_type=postgres&" \
              "project_id=example-project&location=europe-west1&instance=testdb&" \
              "use_proxy=True&sql_proxy_use_tcp=True"
        self._setup_connections(get_connections, uri)
        operator = CloudSqlQueryOperator(
            sql=['SELECT * FROM TABLE'],
            task_id='task_id'
        )
        operator.cloudsql_db_hook.create_connection()
        try:
            db_hook = operator.cloudsql_db_hook.get_database_hook()
            conn = db_hook._get_connections_from_db(db_hook.postgres_conn_id)[0]
        finally:
            operator.cloudsql_db_hook.delete_connection()
        self.assertEqual('postgres', conn.conn_type)
        self.assertEqual('127.0.0.1', conn.host)
        self.assertNotEqual(3200, conn.port)
        self.assertEqual('testdb', conn.schema)

    @mock.patch("airflow.hooks.base_hook.BaseHook.get_connections")
    def test_create_operator_with_correct_parameters_mysql(self, get_connections):
        uri = "gcpcloudsql://user:password@8.8.8.8:3200/testdb?database_type=mysql&" \
            "project_id=example-project&location=europe-west1&instance=testdb&" \
            "use_proxy=False&use_ssl=False"
        self._setup_connections(get_connections, uri)
        operator = CloudSqlQueryOperator(
            sql=['SELECT * FROM TABLE'],
            task_id='task_id'
        )
        operator.cloudsql_db_hook.create_connection()
        try:
            db_hook = operator.cloudsql_db_hook.get_database_hook()
            conn = db_hook._get_connections_from_db(db_hook.mysql_conn_id)[0]
        finally:
            operator.cloudsql_db_hook.delete_connection()
        self.assertEqual('mysql', conn.conn_type)
        self.assertEqual('8.8.8.8', conn.host)
        self.assertEqual(3200, conn.port)
        self.assertEqual('testdb', conn.schema)

    @mock.patch("airflow.hooks.base_hook.BaseHook.get_connections")
    def test_create_operator_with_correct_parameters_mysql_ssl(self, get_connections):
        uri = "gcpcloudsql://user:password@8.8.8.8:3200/testdb?database_type=mysql&" \
              "project_id=example-project&location=europe-west1&instance=testdb&" \
              "use_proxy=False&use_ssl=True&sslcert=/bin/bash&" \
              "sslkey=/bin/bash&sslrootcert=/bin/bash"
        self._setup_connections(get_connections, uri)
        operator = CloudSqlQueryOperator(
            sql=['SELECT * FROM TABLE'],
            task_id='task_id'
        )
        operator.cloudsql_db_hook.create_connection()
        try:
            db_hook = operator.cloudsql_db_hook.get_database_hook()
            conn = db_hook._get_connections_from_db(db_hook.mysql_conn_id)[0]
        finally:
            operator.cloudsql_db_hook.delete_connection()
        self.assertEqual('mysql', conn.conn_type)
        self.assertEqual('8.8.8.8', conn.host)
        self.assertEqual(3200, conn.port)
        self.assertEqual('testdb', conn.schema)
        self.assertEqual('/bin/bash', json.loads(conn.extra_dejson['ssl'])['cert'])
        self.assertEqual('/bin/bash', json.loads(conn.extra_dejson['ssl'])['key'])
        self.assertEqual('/bin/bash', json.loads(conn.extra_dejson['ssl'])['ca'])

    @mock.patch("airflow.hooks.base_hook.BaseHook.get_connections")
    def test_create_operator_with_correct_parameters_mysql_proxy_socket(self,
                                                                        get_connections):
        uri = "gcpcloudsql://user:password@8.8.8.8:3200/testdb?database_type=mysql&" \
              "project_id=example-project&location=europe-west1&instance=testdb&" \
              "use_proxy=True&sql_proxy_use_tcp=False"
        self._setup_connections(get_connections, uri)
        operator = CloudSqlQueryOperator(
            sql=['SELECT * FROM TABLE'],
            task_id='task_id'
        )
        operator.cloudsql_db_hook.create_connection()
        try:
            db_hook = operator.cloudsql_db_hook.get_database_hook()
            conn = db_hook._get_connections_from_db(db_hook.mysql_conn_id)[0]
        finally:
            operator.cloudsql_db_hook.delete_connection()
        self.assertEqual('mysql', conn.conn_type)
        self.assertEqual('localhost', conn.host)
        self.assertIn('/tmp', conn.extra_dejson['unix_socket'])
        self.assertIn('example-project:europe-west1:testdb',
                      conn.extra_dejson['unix_socket'])
        self.assertIsNone(conn.port)
        self.assertEqual('testdb', conn.schema)

    @mock.patch("airflow.hooks.base_hook.BaseHook.get_connections")
    def test_create_operator_with_correct_parameters_mysql_tcp(self, get_connections):
        uri = "gcpcloudsql://user:password@8.8.8.8:3200/testdb?database_type=mysql&" \
              "project_id=example-project&location=europe-west1&instance=testdb&" \
              "use_proxy=True&sql_proxy_use_tcp=True"
        self._setup_connections(get_connections, uri)
        operator = CloudSqlQueryOperator(
            sql=['SELECT * FROM TABLE'],
            task_id='task_id'
        )
        operator.cloudsql_db_hook.create_connection()
        try:
            db_hook = operator.cloudsql_db_hook.get_database_hook()
            conn = db_hook._get_connections_from_db(db_hook.mysql_conn_id)[0]
        finally:
            operator.cloudsql_db_hook.delete_connection()
        self.assertEqual('mysql', conn.conn_type)
        self.assertEqual('127.0.0.1', conn.host)
        self.assertNotEqual(3200, conn.port)
        self.assertEqual('testdb', conn.schema)

    @mock.patch("airflow.hooks.base_hook.BaseHook.get_connections")
    def test_create_operator_with_too_long_unix_socket_path(self, get_connections):
        uri = "gcpcloudsql://user:password@8.8.8.8:3200/testdb?database_type=postgres&" \
              "project_id=example-project&location=europe-west1&" \
              "instance=" \
              "test_db_with_long_name_a_bit_above" \
              "_the_limit_of_UNIX_socket_asdadadasadasd&" \
              "use_proxy=True&sql_proxy_use_tcp=False"
        self._setup_connections(get_connections, uri)
        operator = CloudSqlQueryOperator(
            sql=['SELECT * FROM TABLE'],
            task_id='task_id'
        )
        with self.assertRaises(AirflowException) as cm:
            operator.execute(None)
        err = cm.exception
        self.assertIn("The UNIX socket path length cannot exceed", str(err))

    @mock.patch("airflow.hooks.base_hook.BaseHook.get_connections")
    def test_create_operator_with_not_too_long_unix_socket_path(self, get_connections):
        uri = "gcpcloudsql://user:password@8.8.8.8:3200/testdb?database_type=postgres&" \
              "project_id=example-project&location=europe-west1&" \
              "instance=" \
              "test_db_with_longname_but_with_limit_of_UNIX_socket&" \
              "use_proxy=True&sql_proxy_use_tcp=False"
        self._setup_connections(get_connections, uri)
        operator = CloudSqlQueryOperator(
            sql=['SELECT * FROM TABLE'],
            task_id='task_id'
        )
        operator.cloudsql_db_hook.create_connection()
        try:
            db_hook = operator.cloudsql_db_hook.get_database_hook()
            conn = db_hook._get_connections_from_db(db_hook.postgres_conn_id)[0]
        finally:
            operator.cloudsql_db_hook.delete_connection()
        self.assertEqual('postgres', conn.conn_type)
        self.assertEqual('testdb', conn.schema)

    @mock.patch("airflow.contrib.hooks.gcp_sql_hook.CloudSqlDatabaseHook."
                "delete_connection")
    @mock.patch("airflow.contrib.hooks.gcp_sql_hook.CloudSqlDatabaseHook."
                "get_connection")
    @mock.patch("airflow.hooks.mysql_hook.MySqlHook.run")
    @mock.patch("airflow.hooks.base_hook.BaseHook.get_connections")
    def test_cloudsql_hook_delete_connection_on_exception(
            self, get_connections, run, get_connection, delete_connection):
        connection = Connection()
        connection.parse_from_uri(
            "gcpcloudsql://user:password@8.8.8.8:3200/testdb?database_type=mysql&"
            "project_id=example-project&location=europe-west1&instance=testdb&"
            "use_proxy=False")
        get_connection.return_value = connection

        db_connection = Connection()
        db_connection.host = "8.8.8.8"
        db_connection.set_extra(json.dumps({"project_id": "example-project",
                                            "location": "europe-west1",
                                            "instance": "testdb",
                                            "database_type": "mysql"}))
        get_connections.return_value = [db_connection]
        run.side_effect = Exception("Exception when running a query")
        operator = CloudSqlQueryOperator(
            sql=['SELECT * FROM TABLE'],
            task_id='task_id'
        )
        with self.assertRaises(Exception) as cm:
            operator.execute(None)
        err = cm.exception
        self.assertEqual("Exception when running a query", str(err))
        delete_connection.assert_called_once_with()
