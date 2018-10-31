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

import unittest

from airflow import AirflowException
from airflow.contrib.operators.gcp_sql_operator import CloudSqlInstanceCreateOperator, \
    CloudSqlInstancePatchOperator, CloudSqlInstanceDeleteOperator

try:
    # noinspection PyProtectedMember
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

PROJECT_ID = "project-id"
INSTANCE_NAME = "test-name"
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
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version="v1beta4",
                                          gcp_conn_id="google_cloud_default")
        mock_hook.return_value.create_instance.assert_called_once_with(
            PROJECT_ID, CREATE_BODY
        )
        self.assertTrue(result)

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
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version="v1beta4",
                                          gcp_conn_id="google_cloud_default")
        mock_hook.return_value.create_instance.assert_not_called()
        self.assertTrue(result)

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
            self, mock_hook, _check_if_instance_exists):
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
