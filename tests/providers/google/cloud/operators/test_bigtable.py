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
from typing import Dict, List

import google.api_core.exceptions
import mock
from google.cloud.bigtable.column_family import MaxVersionsGCRule
from google.cloud.bigtable.instance import Instance
from google.cloud.bigtable_admin_v2 import enums
from parameterized import parameterized

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.operators.bigtable import (
    BigtableCreateInstanceOperator, BigtableCreateTableOperator, BigtableDeleteInstanceOperator,
    BigtableDeleteTableOperator, BigtableUpdateClusterOperator, BigtableUpdateInstanceOperator,
)

PROJECT_ID = 'test_project_id'
INSTANCE_ID = 'test-instance-id'
CLUSTER_ID = 'test-cluster-id'
CLUSTER_ZONE = 'us-central1-f'
REPLICATE_CLUSTERS = [
    {'id': 'replica-1', 'zone': 'us-west1-a'},
    {'id': 'replica-2', 'zone': 'us-central1-f'},
    {'id': 'replica-3', 'zone': 'us-east1-d'},
]
GCP_CONN_ID = 'test-gcp-conn-id'
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]
NODES = 5
INSTANCE_DISPLAY_NAME = "test instance"
INSTANCE_TYPE = enums.Instance.Type.PRODUCTION
INSTANCE_LABELS = {"env": "sit"}
TABLE_ID = 'test-table-id'
INITIAL_SPLIT_KEYS = []  # type: List
EMPTY_COLUMN_FAMILIES = {}  # type: Dict


class TestBigtableInstanceCreate(unittest.TestCase):
    @parameterized.expand([
        ('instance_id', PROJECT_ID, '', CLUSTER_ID, CLUSTER_ZONE),
        ('main_cluster_id', PROJECT_ID, INSTANCE_ID, '', CLUSTER_ZONE),
        ('main_cluster_zone', PROJECT_ID, INSTANCE_ID, CLUSTER_ID, ''),
    ], testcase_func_name=lambda f, n, p: 'test_empty_attribute.empty_' + p.args[0])
    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_empty_attribute(self, missing_attribute, project_id, instance_id,
                             main_cluster_id,
                             main_cluster_zone, mock_hook):
        with self.assertRaises(AirflowException) as e:
            BigtableCreateInstanceOperator(
                project_id=project_id,
                instance_id=instance_id,
                main_cluster_id=main_cluster_id,
                main_cluster_zone=main_cluster_zone,
                task_id="id",
                gcp_conn_id=GCP_CONN_ID
            )
        err = e.exception
        self.assertEqual(str(err), 'Empty parameter: {}'.format(missing_attribute))
        mock_hook.assert_not_called()

    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_create_instance_that_exists(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = mock.Mock(Instance)

        op = BigtableCreateInstanceOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            main_cluster_id=CLUSTER_ID,
            main_cluster_zone=CLUSTER_ZONE,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(None)

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.create_instance.assert_not_called()

    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_create_instance_that_exists_empty_project_id(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = mock.Mock(Instance)

        op = BigtableCreateInstanceOperator(
            instance_id=INSTANCE_ID,
            main_cluster_id=CLUSTER_ID,
            main_cluster_zone=CLUSTER_ZONE,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(None)

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.create_instance.assert_not_called()

    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_different_error_reraised(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = None
        op = BigtableCreateInstanceOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            main_cluster_id=CLUSTER_ID,
            main_cluster_zone=CLUSTER_ZONE,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_hook.return_value.create_instance.side_effect = mock.Mock(
            side_effect=google.api_core.exceptions.GoogleAPICallError('error'))

        with self.assertRaises(google.api_core.exceptions.GoogleAPICallError):
            op.execute(None)

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.create_instance.assert_called_once_with(
            cluster_nodes=None,
            cluster_storage_type=None,
            instance_display_name=None,
            instance_id=INSTANCE_ID,
            instance_labels=None,
            instance_type=None,
            main_cluster_id=CLUSTER_ID,
            main_cluster_zone=CLUSTER_ZONE,
            project_id=PROJECT_ID,
            replica_clusters=None,
            replica_cluster_id=None,
            replica_cluster_zone=None,
            timeout=None
        )

    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_create_instance_that_doesnt_exists(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = None
        op = BigtableCreateInstanceOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            main_cluster_id=CLUSTER_ID,
            main_cluster_zone=CLUSTER_ZONE,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID
        )
        op.execute(None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.create_instance.assert_called_once_with(
            cluster_nodes=None,
            cluster_storage_type=None,
            instance_display_name=None,
            instance_id=INSTANCE_ID,
            instance_labels=None,
            instance_type=None,
            main_cluster_id=CLUSTER_ID,
            main_cluster_zone=CLUSTER_ZONE,
            project_id=PROJECT_ID,
            replica_clusters=None,
            replica_cluster_id=None,
            replica_cluster_zone=None,
            timeout=None
        )

    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_create_instance_with_replicas_that_doesnt_exists(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = None
        op = BigtableCreateInstanceOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            main_cluster_id=CLUSTER_ID,
            main_cluster_zone=CLUSTER_ZONE,
            replica_clusters=REPLICATE_CLUSTERS,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID
        )
        op.execute(None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.create_instance.assert_called_once_with(
            cluster_nodes=None,
            cluster_storage_type=None,
            instance_display_name=None,
            instance_id=INSTANCE_ID,
            instance_labels=None,
            instance_type=None,
            main_cluster_id=CLUSTER_ID,
            main_cluster_zone=CLUSTER_ZONE,
            project_id=PROJECT_ID,
            replica_clusters=REPLICATE_CLUSTERS,
            replica_cluster_id=None,
            replica_cluster_zone=None,
            timeout=None
        )


class TestBigtableInstanceUpdate(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_delete_execute(self, mock_hook):
        op = BigtableUpdateInstanceOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            instance_display_name=INSTANCE_DISPLAY_NAME,
            instance_type=INSTANCE_TYPE,
            instance_labels=INSTANCE_LABELS,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.update_instance.assert_called_once_with(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            instance_display_name=INSTANCE_DISPLAY_NAME,
            instance_type=INSTANCE_TYPE,
            instance_labels=INSTANCE_LABELS,
            timeout=None
        )

    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_update_execute_empty_project_id(self, mock_hook):
        op = BigtableUpdateInstanceOperator(
            instance_id=INSTANCE_ID,
            instance_display_name=INSTANCE_DISPLAY_NAME,
            instance_type=INSTANCE_TYPE,
            instance_labels=INSTANCE_LABELS,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.update_instance.assert_called_once_with(
            project_id=None,
            instance_id=INSTANCE_ID,
            instance_display_name=INSTANCE_DISPLAY_NAME,
            instance_type=INSTANCE_TYPE,
            instance_labels=INSTANCE_LABELS,
            timeout=None
        )

    @parameterized.expand([
        ('instance_id', PROJECT_ID, ''),
    ], testcase_func_name=lambda f, n, p: 'test_empty_attribute.empty_' + p.args[0])
    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_empty_attribute(self, missing_attribute, project_id, instance_id, mock_hook):
        with self.assertRaises(AirflowException) as e:
            BigtableUpdateInstanceOperator(
                project_id=project_id,
                instance_id=instance_id,
                instance_display_name=INSTANCE_DISPLAY_NAME,
                instance_type=INSTANCE_TYPE,
                instance_labels=INSTANCE_LABELS,
                task_id="id"
            )
        err = e.exception
        self.assertEqual(str(err), 'Empty parameter: {}'.format(missing_attribute))
        mock_hook.assert_not_called()

    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_update_instance_that_doesnt_exists(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = None

        with self.assertRaises(AirflowException) as e:
            op = BigtableUpdateInstanceOperator(
                project_id=PROJECT_ID,
                instance_id=INSTANCE_ID,
                instance_display_name=INSTANCE_DISPLAY_NAME,
                instance_type=INSTANCE_TYPE,
                instance_labels=INSTANCE_LABELS,
                task_id="id",
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )
            op.execute(None)

        err = e.exception
        self.assertEqual(str(err), "Dependency: instance '{}' does not exist.".format(
            INSTANCE_ID))

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.update_instance.assert_not_called()

    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_update_instance_that_doesnt_exists_empty_project_id(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = None

        with self.assertRaises(AirflowException) as e:
            op = BigtableUpdateInstanceOperator(
                instance_id=INSTANCE_ID,
                instance_display_name=INSTANCE_DISPLAY_NAME,
                instance_type=INSTANCE_TYPE,
                instance_labels=INSTANCE_LABELS,
                task_id="id",
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )
            op.execute(None)

        err = e.exception
        self.assertEqual(str(err), "Dependency: instance '{}' does not exist.".format(
            INSTANCE_ID))

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.update_instance.assert_not_called()

    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_different_error_reraised(self, mock_hook):
        op = BigtableUpdateInstanceOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            instance_display_name=INSTANCE_DISPLAY_NAME,
            instance_type=INSTANCE_TYPE,
            instance_labels=INSTANCE_LABELS,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.update_instance.side_effect = mock.Mock(
            side_effect=google.api_core.exceptions.GoogleAPICallError('error'))

        with self.assertRaises(google.api_core.exceptions.GoogleAPICallError):
            op.execute(None)

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.update_instance.assert_called_once_with(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            instance_display_name=INSTANCE_DISPLAY_NAME,
            instance_type=INSTANCE_TYPE,
            instance_labels=INSTANCE_LABELS,
            timeout=None
        )


class TestBigtableClusterUpdate(unittest.TestCase):
    @parameterized.expand([
        ('instance_id', PROJECT_ID, '', CLUSTER_ID, NODES),
        ('cluster_id', PROJECT_ID, INSTANCE_ID, '', NODES),
        ('nodes', PROJECT_ID, INSTANCE_ID, CLUSTER_ID, ''),
    ], testcase_func_name=lambda f, n, p: 'test_empty_attribute.empty_' + p.args[0])
    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_empty_attribute(self, missing_attribute, project_id, instance_id,
                             cluster_id, nodes, mock_hook):
        with self.assertRaises(AirflowException) as e:
            BigtableUpdateClusterOperator(
                project_id=project_id,
                instance_id=instance_id,
                cluster_id=cluster_id,
                nodes=nodes,
                task_id="id",
                gcp_conn_id=GCP_CONN_ID
            )
        err = e.exception
        self.assertEqual(str(err), 'Empty parameter: {}'.format(missing_attribute))
        mock_hook.assert_not_called()

    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_updating_cluster_but_instance_does_not_exists(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = None

        with self.assertRaises(AirflowException) as e:
            op = BigtableUpdateClusterOperator(
                project_id=PROJECT_ID,
                instance_id=INSTANCE_ID,
                cluster_id=CLUSTER_ID,
                nodes=NODES,
                task_id="id",
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )
            op.execute(None)

        err = e.exception
        self.assertEqual(str(err), "Dependency: instance '{}' does not exist.".format(
            INSTANCE_ID))
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.update_cluster.assert_not_called()

    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_updating_cluster_but_instance_does_not_exists_empty_project_id(self,
                                                                            mock_hook):
        mock_hook.return_value.get_instance.return_value = None

        with self.assertRaises(AirflowException) as e:
            op = BigtableUpdateClusterOperator(
                instance_id=INSTANCE_ID,
                cluster_id=CLUSTER_ID,
                nodes=NODES,
                task_id="id",
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )
            op.execute(None)

        err = e.exception
        self.assertEqual(str(err), "Dependency: instance '{}' does not exist.".format(
            INSTANCE_ID))
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.update_cluster.assert_not_called()

    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_updating_cluster_that_does_not_exists(self, mock_hook):
        instance = mock_hook.return_value.get_instance.return_value = mock.Mock(Instance)
        mock_hook.return_value.update_cluster.side_effect = mock.Mock(
            side_effect=google.api_core.exceptions.NotFound("Cluster not found."))

        with self.assertRaises(AirflowException) as e:
            op = BigtableUpdateClusterOperator(
                project_id=PROJECT_ID,
                instance_id=INSTANCE_ID,
                cluster_id=CLUSTER_ID,
                nodes=NODES,
                task_id="id",
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )
            op.execute(None)

        err = e.exception
        self.assertEqual(
            str(err),
            "Dependency: cluster '{}' does not exist for instance '{}'.".format(
                CLUSTER_ID, INSTANCE_ID)
        )
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.update_cluster.assert_called_once_with(
            instance=instance, cluster_id=CLUSTER_ID, nodes=NODES)

    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_updating_cluster_that_does_not_exists_empty_project_id(self, mock_hook):
        instance = mock_hook.return_value.get_instance.return_value = mock.Mock(Instance)
        mock_hook.return_value.update_cluster.side_effect = mock.Mock(
            side_effect=google.api_core.exceptions.NotFound("Cluster not found."))

        with self.assertRaises(AirflowException) as e:
            op = BigtableUpdateClusterOperator(
                instance_id=INSTANCE_ID,
                cluster_id=CLUSTER_ID,
                nodes=NODES,
                task_id="id",
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )
            op.execute(None)

        err = e.exception
        self.assertEqual(
            str(err),
            "Dependency: cluster '{}' does not exist for instance '{}'.".format(
                CLUSTER_ID, INSTANCE_ID)
        )
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.update_cluster.assert_called_once_with(
            instance=instance, cluster_id=CLUSTER_ID, nodes=NODES)

    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_different_error_reraised(self, mock_hook):
        op = BigtableUpdateClusterOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            cluster_id=CLUSTER_ID,
            nodes=NODES,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        instance = mock_hook.return_value.get_instance.return_value = mock.Mock(Instance)
        mock_hook.return_value.update_cluster.side_effect = mock.Mock(
            side_effect=google.api_core.exceptions.GoogleAPICallError('error'))

        with self.assertRaises(google.api_core.exceptions.GoogleAPICallError):
            op.execute(None)

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.update_cluster.assert_called_once_with(
            instance=instance, cluster_id=CLUSTER_ID, nodes=NODES)


class TestBigtableInstanceDelete(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_delete_execute(self, mock_hook):
        op = BigtableDeleteInstanceOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.delete_instance.assert_called_once_with(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID)

    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_delete_execute_empty_project_id(self, mock_hook):
        op = BigtableDeleteInstanceOperator(
            instance_id=INSTANCE_ID,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.delete_instance.assert_called_once_with(
            project_id=None,
            instance_id=INSTANCE_ID)

    @parameterized.expand([
        ('instance_id', PROJECT_ID, ''),
    ], testcase_func_name=lambda f, n, p: 'test_empty_attribute.empty_' + p.args[0])
    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_empty_attribute(self, missing_attribute, project_id, instance_id, mock_hook):
        with self.assertRaises(AirflowException) as e:
            BigtableDeleteInstanceOperator(
                project_id=project_id,
                instance_id=instance_id,
                task_id="id"
            )
        err = e.exception
        self.assertEqual(str(err), 'Empty parameter: {}'.format(missing_attribute))
        mock_hook.assert_not_called()

    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_deleting_instance_that_doesnt_exists(self, mock_hook):
        op = BigtableDeleteInstanceOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.delete_instance.side_effect = mock.Mock(
            side_effect=google.api_core.exceptions.NotFound("Instance not found."))
        op.execute(None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.delete_instance.assert_called_once_with(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID)

    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_deleting_instance_that_doesnt_exists_empty_project_id(self, mock_hook):
        op = BigtableDeleteInstanceOperator(
            instance_id=INSTANCE_ID,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.delete_instance.side_effect = mock.Mock(
            side_effect=google.api_core.exceptions.NotFound("Instance not found."))
        op.execute(None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.delete_instance.assert_called_once_with(
            project_id=None,
            instance_id=INSTANCE_ID)

    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_different_error_reraised(self, mock_hook):
        op = BigtableDeleteInstanceOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.delete_instance.side_effect = mock.Mock(
            side_effect=google.api_core.exceptions.GoogleAPICallError('error'))

        with self.assertRaises(google.api_core.exceptions.GoogleAPICallError):
            op.execute(None)

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.delete_instance.assert_called_once_with(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID)


class TestBigtableTableDelete(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_delete_execute(self, mock_hook):
        op = BigtableDeleteTableOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.delete_table.assert_called_once_with(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID)

    @parameterized.expand([
        ('instance_id', PROJECT_ID, '', TABLE_ID),
        ('table_id', PROJECT_ID, INSTANCE_ID, ''),
    ], testcase_func_name=lambda f, n, p: 'test_empty_attribute.empty_' + p.args[0])
    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_empty_attribute(self, missing_attribute, project_id, instance_id, table_id,
                             mock_hook):
        with self.assertRaises(AirflowException) as e:
            BigtableDeleteTableOperator(
                project_id=project_id,
                instance_id=instance_id,
                table_id=table_id,
                task_id="id",
                gcp_conn_id=GCP_CONN_ID
            )
        err = e.exception
        self.assertEqual(str(err), 'Empty parameter: {}'.format(missing_attribute))
        mock_hook.assert_not_called()

    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_deleting_table_that_doesnt_exists(self, mock_hook):
        op = BigtableDeleteTableOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_hook.return_value.delete_table.side_effect = mock.Mock(
            side_effect=google.api_core.exceptions.NotFound("Table not found."))
        op.execute(None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.delete_table.assert_called_once_with(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID)

    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_deleting_table_that_doesnt_exists_empty_project_id(self, mock_hook):
        op = BigtableDeleteTableOperator(
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_hook.return_value.delete_table.side_effect = mock.Mock(
            side_effect=google.api_core.exceptions.NotFound("Table not found."))
        op.execute(None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.delete_table.assert_called_once_with(
            project_id=None,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID)

    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_deleting_table_when_instance_doesnt_exists(self, mock_hook):
        op = BigtableDeleteTableOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_hook.return_value.get_instance.return_value = None
        with self.assertRaises(AirflowException) as e:
            op.execute(None)
        err = e.exception
        self.assertEqual(str(err), "Dependency: instance '{}' does not exist.".format(
            INSTANCE_ID))
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.delete_table.assert_not_called()

    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_different_error_reraised(self, mock_hook):
        op = BigtableDeleteTableOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.delete_table.side_effect = mock.Mock(
            side_effect=google.api_core.exceptions.GoogleAPICallError('error'))

        with self.assertRaises(google.api_core.exceptions.GoogleAPICallError):
            op.execute(None)

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.delete_table.assert_called_once_with(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID)


class TestBigtableTableCreate(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_create_execute(self, mock_hook):
        op = BigtableCreateTableOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID,
            initial_split_keys=INITIAL_SPLIT_KEYS,
            column_families=EMPTY_COLUMN_FAMILIES,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        instance = mock_hook.return_value.get_instance.return_value = mock.Mock(Instance)
        op.execute(None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.create_table.assert_called_once_with(
            instance=instance,
            table_id=TABLE_ID,
            initial_split_keys=INITIAL_SPLIT_KEYS,
            column_families=EMPTY_COLUMN_FAMILIES)

    @parameterized.expand([
        ('instance_id', PROJECT_ID, '', TABLE_ID),
        ('table_id', PROJECT_ID, INSTANCE_ID, ''),
    ], testcase_func_name=lambda f, n, p: 'test_empty_attribute.empty_' + p.args[0])
    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_empty_attribute(self, missing_attribute, project_id, instance_id, table_id,
                             mock_hook):
        with self.assertRaises(AirflowException) as e:
            BigtableCreateTableOperator(
                project_id=project_id,
                instance_id=instance_id,
                table_id=table_id,
                task_id="id",
                gcp_conn_id=GCP_CONN_ID
            )
        err = e.exception
        self.assertEqual(str(err), 'Empty parameter: {}'.format(missing_attribute))
        mock_hook.assert_not_called()

    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_instance_not_exists(self, mock_hook):
        op = BigtableCreateTableOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID,
            initial_split_keys=INITIAL_SPLIT_KEYS,
            column_families=EMPTY_COLUMN_FAMILIES,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.get_instance.return_value = None
        with self.assertRaises(AirflowException) as e:
            op.execute(None)
        err = e.exception
        self.assertEqual(
            str(err),
            "Dependency: instance '{}' does not exist in project '{}'.".format(
                INSTANCE_ID, PROJECT_ID)
        )
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_creating_table_that_exists(self, mock_hook):
        op = BigtableCreateTableOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID,
            initial_split_keys=INITIAL_SPLIT_KEYS,
            column_families=EMPTY_COLUMN_FAMILIES,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_hook.return_value.get_column_families_for_table.return_value = \
            EMPTY_COLUMN_FAMILIES
        instance = mock_hook.return_value.get_instance.return_value = mock.Mock(Instance)
        mock_hook.return_value.create_table.side_effect = mock.Mock(
            side_effect=google.api_core.exceptions.AlreadyExists("Table already exists."))
        op.execute(None)

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.create_table.assert_called_once_with(
            instance=instance,
            table_id=TABLE_ID,
            initial_split_keys=INITIAL_SPLIT_KEYS,
            column_families=EMPTY_COLUMN_FAMILIES)

    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_creating_table_that_exists_empty_project_id(self, mock_hook):
        op = BigtableCreateTableOperator(
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID,
            initial_split_keys=INITIAL_SPLIT_KEYS,
            column_families=EMPTY_COLUMN_FAMILIES,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_hook.return_value.get_column_families_for_table.return_value = \
            EMPTY_COLUMN_FAMILIES
        instance = mock_hook.return_value.get_instance.return_value = mock.Mock(Instance)
        mock_hook.return_value.create_table.side_effect = mock.Mock(
            side_effect=google.api_core.exceptions.AlreadyExists("Table already exists."))
        op.execute(None)

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.create_table.assert_called_once_with(
            instance=instance,
            table_id=TABLE_ID,
            initial_split_keys=INITIAL_SPLIT_KEYS,
            column_families=EMPTY_COLUMN_FAMILIES)

    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_creating_table_that_exists_with_different_column_families_ids_in_the_table(
            self, mock_hook):
        op = BigtableCreateTableOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID,
            initial_split_keys=INITIAL_SPLIT_KEYS,
            column_families=EMPTY_COLUMN_FAMILIES,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_hook.return_value.get_column_families_for_table.return_value = {
            "existing_family": None}
        mock_hook.return_value.create_table.side_effect = mock.Mock(
            side_effect=google.api_core.exceptions.AlreadyExists("Table already exists."))

        with self.assertRaises(AirflowException) as e:
            op.execute(None)
        err = e.exception
        self.assertEqual(
            str(err),
            "Table '{}' already exists with different Column Families.".format(TABLE_ID)
        )
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

    @mock.patch('airflow.providers.google.cloud.operators.bigtable.BigtableHook')
    def test_creating_table_that_exists_with_different_column_families_gc_rule_in__table(
            self, mock_hook):
        op = BigtableCreateTableOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID,
            initial_split_keys=INITIAL_SPLIT_KEYS,
            column_families={"cf-id": MaxVersionsGCRule(1)},
            task_id="id",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        cf_mock = mock.Mock()
        cf_mock.gc_rule = mock.Mock(return_value=MaxVersionsGCRule(2))

        mock_hook.return_value.get_column_families_for_table.return_value = {
            "cf-id": cf_mock
        }
        mock_hook.return_value.create_table.side_effect = mock.Mock(
            side_effect=google.api_core.exceptions.AlreadyExists("Table already exists."))

        with self.assertRaises(AirflowException) as e:
            op.execute(None)
        err = e.exception
        self.assertEqual(
            str(err),
            "Table '{}' already exists with different Column Families.".format(TABLE_ID)
        )
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
