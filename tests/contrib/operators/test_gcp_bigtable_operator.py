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
from typing import List, Dict
import google.api_core.exceptions
from google.cloud.bigtable.column_family import MaxVersionsGCRule
from google.cloud.bigtable.instance import Instance
from google.cloud.bigtable.table import ClusterState
from parameterized import parameterized

from airflow import AirflowException
from airflow.contrib.operators.gcp_bigtable_operator import \
    BigtableInstanceDeleteOperator, \
    BigtableTableDeleteOperator, \
    BigtableTableCreateOperator, \
    BigtableTableWaitForReplicationSensor, \
    BigtableClusterUpdateOperator, \
    BigtableInstanceCreateOperator
from tests.compat import mock

PROJECT_ID = 'test_project_id'
INSTANCE_ID = 'test-instance-id'
CLUSTER_ID = 'test-cluster-id'
CLUSTER_ZONE = 'us-central1-f'
GCP_CONN_ID = 'test-gcp-conn-id'
NODES = 5
TABLE_ID = 'test-table-id'
INITIAL_SPLIT_KEYS = []  # type: List
EMPTY_COLUMN_FAMILIES = {}  # type: Dict


class BigtableInstanceCreateTest(unittest.TestCase):
    @parameterized.expand([
        ('instance_id', PROJECT_ID, '', CLUSTER_ID, CLUSTER_ZONE),
        ('main_cluster_id', PROJECT_ID, INSTANCE_ID, '', CLUSTER_ZONE),
        ('main_cluster_zone', PROJECT_ID, INSTANCE_ID, CLUSTER_ID, ''),
    ], testcase_func_name=lambda f, n, p: 'test_empty_attribute.empty_' + p.args[0])
    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_empty_attribute(self, missing_attribute, project_id, instance_id,
                             main_cluster_id,
                             main_cluster_zone, mock_hook):
        with self.assertRaises(AirflowException) as e:
            BigtableInstanceCreateOperator(
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

    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_create_instance_that_exists(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = mock.Mock(Instance)

        op = BigtableInstanceCreateOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            main_cluster_id=CLUSTER_ID,
            main_cluster_zone=CLUSTER_ZONE,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID
        )
        op.execute(None)

        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.create_instance.assert_not_called()

    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_create_instance_that_exists_empty_project_id(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = mock.Mock(Instance)

        op = BigtableInstanceCreateOperator(
            instance_id=INSTANCE_ID,
            main_cluster_id=CLUSTER_ID,
            main_cluster_zone=CLUSTER_ZONE,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID
        )
        op.execute(None)

        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.create_instance.assert_not_called()

    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_different_error_reraised(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = None
        op = BigtableInstanceCreateOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            main_cluster_id=CLUSTER_ID,
            main_cluster_zone=CLUSTER_ZONE,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID
        )

        mock_hook.return_value.create_instance.side_effect = mock.Mock(
            side_effect=google.api_core.exceptions.GoogleAPICallError('error'))

        with self.assertRaises(google.api_core.exceptions.GoogleAPICallError):
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
            replica_cluster_id=None,
            replica_cluster_zone=None,
            timeout=None
        )


class BigtableClusterUpdateTest(unittest.TestCase):
    @parameterized.expand([
        ('instance_id', PROJECT_ID, '', CLUSTER_ID, NODES),
        ('cluster_id', PROJECT_ID, INSTANCE_ID, '', NODES),
        ('nodes', PROJECT_ID, INSTANCE_ID, CLUSTER_ID, ''),
    ], testcase_func_name=lambda f, n, p: 'test_empty_attribute.empty_' + p.args[0])
    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_empty_attribute(self, missing_attribute, project_id, instance_id,
                             cluster_id, nodes, mock_hook):
        with self.assertRaises(AirflowException) as e:
            BigtableClusterUpdateOperator(
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

    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_updating_cluster_but_instance_does_not_exists(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = None

        with self.assertRaises(AirflowException) as e:
            op = BigtableClusterUpdateOperator(
                project_id=PROJECT_ID,
                instance_id=INSTANCE_ID,
                cluster_id=CLUSTER_ID,
                nodes=NODES,
                task_id="id",
                gcp_conn_id=GCP_CONN_ID
            )
            op.execute(None)

        err = e.exception
        self.assertEqual(str(err), "Dependency: instance '{}' does not exist.".format(
            INSTANCE_ID))
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.update_cluster.assert_not_called()

    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_updating_cluster_but_instance_does_not_exists_empty_project_id(self,
                                                                            mock_hook):
        mock_hook.return_value.get_instance.return_value = None

        with self.assertRaises(AirflowException) as e:
            op = BigtableClusterUpdateOperator(
                instance_id=INSTANCE_ID,
                cluster_id=CLUSTER_ID,
                nodes=NODES,
                task_id="id",
                gcp_conn_id=GCP_CONN_ID
            )
            op.execute(None)

        err = e.exception
        self.assertEqual(str(err), "Dependency: instance '{}' does not exist.".format(
            INSTANCE_ID))
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.update_cluster.assert_not_called()

    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_updating_cluster_that_does_not_exists(self, mock_hook):
        instance = mock_hook.return_value.get_instance.return_value = mock.Mock(Instance)
        mock_hook.return_value.update_cluster.side_effect = mock.Mock(
            side_effect=google.api_core.exceptions.NotFound("Cluster not found."))

        with self.assertRaises(AirflowException) as e:
            op = BigtableClusterUpdateOperator(
                project_id=PROJECT_ID,
                instance_id=INSTANCE_ID,
                cluster_id=CLUSTER_ID,
                nodes=NODES,
                task_id="id",
                gcp_conn_id=GCP_CONN_ID
            )
            op.execute(None)

        err = e.exception
        self.assertEqual(
            str(err),
            "Dependency: cluster '{}' does not exist for instance '{}'.".format(
                CLUSTER_ID, INSTANCE_ID)
        )
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.update_cluster.assert_called_once_with(
            instance=instance, cluster_id=CLUSTER_ID, nodes=NODES)

    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_updating_cluster_that_does_not_exists_empty_project_id(self, mock_hook):
        instance = mock_hook.return_value.get_instance.return_value = mock.Mock(Instance)
        mock_hook.return_value.update_cluster.side_effect = mock.Mock(
            side_effect=google.api_core.exceptions.NotFound("Cluster not found."))

        with self.assertRaises(AirflowException) as e:
            op = BigtableClusterUpdateOperator(
                instance_id=INSTANCE_ID,
                cluster_id=CLUSTER_ID,
                nodes=NODES,
                task_id="id",
                gcp_conn_id=GCP_CONN_ID
            )
            op.execute(None)

        err = e.exception
        self.assertEqual(
            str(err),
            "Dependency: cluster '{}' does not exist for instance '{}'.".format(
                CLUSTER_ID, INSTANCE_ID)
        )
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.update_cluster.assert_called_once_with(
            instance=instance, cluster_id=CLUSTER_ID, nodes=NODES)

    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_different_error_reraised(self, mock_hook):
        op = BigtableClusterUpdateOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            cluster_id=CLUSTER_ID,
            nodes=NODES,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID
        )
        instance = mock_hook.return_value.get_instance.return_value = mock.Mock(Instance)
        mock_hook.return_value.update_cluster.side_effect = mock.Mock(
            side_effect=google.api_core.exceptions.GoogleAPICallError('error'))

        with self.assertRaises(google.api_core.exceptions.GoogleAPICallError):
            op.execute(None)

        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.update_cluster.assert_called_once_with(
            instance=instance, cluster_id=CLUSTER_ID, nodes=NODES)


class BigtableInstanceDeleteTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_delete_execute(self, mock_hook):
        op = BigtableInstanceDeleteOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID
        )
        op.execute(None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.delete_instance.assert_called_once_with(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID)

    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_delete_execute_empty_project_id(self, mock_hook):
        op = BigtableInstanceDeleteOperator(
            instance_id=INSTANCE_ID,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID
        )
        op.execute(None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.delete_instance.assert_called_once_with(
            project_id=None,
            instance_id=INSTANCE_ID)

    @parameterized.expand([
        ('instance_id', PROJECT_ID, ''),
    ], testcase_func_name=lambda f, n, p: 'test_empty_attribute.empty_' + p.args[0])
    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_empty_attribute(self, missing_attribute, project_id, instance_id, mock_hook):
        with self.assertRaises(AirflowException) as e:
            BigtableInstanceDeleteOperator(
                project_id=project_id,
                instance_id=instance_id,
                task_id="id"
            )
        err = e.exception
        self.assertEqual(str(err), 'Empty parameter: {}'.format(missing_attribute))
        mock_hook.assert_not_called()

    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_deleting_instance_that_doesnt_exists(self, mock_hook):
        op = BigtableInstanceDeleteOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID
        )
        mock_hook.return_value.delete_instance.side_effect = mock.Mock(
            side_effect=google.api_core.exceptions.NotFound("Instance not found."))
        op.execute(None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.delete_instance.assert_called_once_with(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID)

    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_deleting_instance_that_doesnt_exists_empty_project_id(self, mock_hook):
        op = BigtableInstanceDeleteOperator(
            instance_id=INSTANCE_ID,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID
        )
        mock_hook.return_value.delete_instance.side_effect = mock.Mock(
            side_effect=google.api_core.exceptions.NotFound("Instance not found."))
        op.execute(None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.delete_instance.assert_called_once_with(
            project_id=None,
            instance_id=INSTANCE_ID)

    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_different_error_reraised(self, mock_hook):
        op = BigtableInstanceDeleteOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID
        )
        mock_hook.return_value.delete_instance.side_effect = mock.Mock(
            side_effect=google.api_core.exceptions.GoogleAPICallError('error'))

        with self.assertRaises(google.api_core.exceptions.GoogleAPICallError):
            op.execute(None)

        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.delete_instance.assert_called_once_with(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID)


class BigtableTableDeleteTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_delete_execute(self, mock_hook):
        op = BigtableTableDeleteOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID
        )
        op.execute(None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.delete_table.assert_called_once_with(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID)

    @parameterized.expand([
        ('instance_id', PROJECT_ID, '', TABLE_ID),
        ('table_id', PROJECT_ID, INSTANCE_ID, ''),
    ], testcase_func_name=lambda f, n, p: 'test_empty_attribute.empty_' + p.args[0])
    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_empty_attribute(self, missing_attribute, project_id, instance_id, table_id,
                             mock_hook):
        with self.assertRaises(AirflowException) as e:
            BigtableTableDeleteOperator(
                project_id=project_id,
                instance_id=instance_id,
                table_id=table_id,
                task_id="id",
                gcp_conn_id=GCP_CONN_ID
            )
        err = e.exception
        self.assertEqual(str(err), 'Empty parameter: {}'.format(missing_attribute))
        mock_hook.assert_not_called()

    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_deleting_table_that_doesnt_exists(self, mock_hook):
        op = BigtableTableDeleteOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID
        )

        mock_hook.return_value.delete_table.side_effect = mock.Mock(
            side_effect=google.api_core.exceptions.NotFound("Table not found."))
        op.execute(None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.delete_table.assert_called_once_with(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID)

    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_deleting_table_that_doesnt_exists_empty_project_id(self, mock_hook):
        op = BigtableTableDeleteOperator(
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID
        )

        mock_hook.return_value.delete_table.side_effect = mock.Mock(
            side_effect=google.api_core.exceptions.NotFound("Table not found."))
        op.execute(None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.delete_table.assert_called_once_with(
            project_id=None,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID)

    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_deleting_table_when_instance_doesnt_exists(self, mock_hook):
        op = BigtableTableDeleteOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID
        )

        mock_hook.return_value.get_instance.return_value = None
        with self.assertRaises(AirflowException) as e:
            op.execute(None)
        err = e.exception
        self.assertEqual(str(err), "Dependency: instance '{}' does not exist.".format(
            INSTANCE_ID))
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.delete_table.assert_not_called()

    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_different_error_reraised(self, mock_hook):
        op = BigtableTableDeleteOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID
        )
        mock_hook.return_value.delete_table.side_effect = mock.Mock(
            side_effect=google.api_core.exceptions.GoogleAPICallError('error'))

        with self.assertRaises(google.api_core.exceptions.GoogleAPICallError):
            op.execute(None)

        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.delete_table.assert_called_once_with(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID)


class BigtableTableCreateTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_create_execute(self, mock_hook):
        op = BigtableTableCreateOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID,
            initial_split_keys=INITIAL_SPLIT_KEYS,
            column_families=EMPTY_COLUMN_FAMILIES,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID
        )
        instance = mock_hook.return_value.get_instance.return_value = mock.Mock(Instance)
        op.execute(None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.create_table.assert_called_once_with(
            instance=instance,
            table_id=TABLE_ID,
            initial_split_keys=INITIAL_SPLIT_KEYS,
            column_families=EMPTY_COLUMN_FAMILIES)

    @parameterized.expand([
        ('instance_id', PROJECT_ID, '', TABLE_ID),
        ('table_id', PROJECT_ID, INSTANCE_ID, ''),
    ], testcase_func_name=lambda f, n, p: 'test_empty_attribute.empty_' + p.args[0])
    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_empty_attribute(self, missing_attribute, project_id, instance_id, table_id,
                             mock_hook):
        with self.assertRaises(AirflowException) as e:
            BigtableTableCreateOperator(
                project_id=project_id,
                instance_id=instance_id,
                table_id=table_id,
                task_id="id",
                gcp_conn_id=GCP_CONN_ID
            )
        err = e.exception
        self.assertEqual(str(err), 'Empty parameter: {}'.format(missing_attribute))
        mock_hook.assert_not_called()

    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_instance_not_exists(self, mock_hook):
        op = BigtableTableCreateOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID,
            initial_split_keys=INITIAL_SPLIT_KEYS,
            column_families=EMPTY_COLUMN_FAMILIES,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID
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
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)

    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_creating_table_that_exists(self, mock_hook):
        op = BigtableTableCreateOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID,
            initial_split_keys=INITIAL_SPLIT_KEYS,
            column_families=EMPTY_COLUMN_FAMILIES,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID
        )

        mock_hook.return_value.get_column_families_for_table.return_value = \
            EMPTY_COLUMN_FAMILIES
        instance = mock_hook.return_value.get_instance.return_value = mock.Mock(Instance)
        mock_hook.return_value.create_table.side_effect = mock.Mock(
            side_effect=google.api_core.exceptions.AlreadyExists("Table already exists."))
        op.execute(None)

        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.create_table.assert_called_once_with(
            instance=instance,
            table_id=TABLE_ID,
            initial_split_keys=INITIAL_SPLIT_KEYS,
            column_families=EMPTY_COLUMN_FAMILIES)

    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_creating_table_that_exists_empty_project_id(self, mock_hook):
        op = BigtableTableCreateOperator(
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID,
            initial_split_keys=INITIAL_SPLIT_KEYS,
            column_families=EMPTY_COLUMN_FAMILIES,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID
        )

        mock_hook.return_value.get_column_families_for_table.return_value = \
            EMPTY_COLUMN_FAMILIES
        instance = mock_hook.return_value.get_instance.return_value = mock.Mock(Instance)
        mock_hook.return_value.create_table.side_effect = mock.Mock(
            side_effect=google.api_core.exceptions.AlreadyExists("Table already exists."))
        op.execute(None)

        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.create_table.assert_called_once_with(
            instance=instance,
            table_id=TABLE_ID,
            initial_split_keys=INITIAL_SPLIT_KEYS,
            column_families=EMPTY_COLUMN_FAMILIES)

    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_creating_table_that_exists_with_different_column_families_ids_in_the_table(
            self, mock_hook):
        op = BigtableTableCreateOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID,
            initial_split_keys=INITIAL_SPLIT_KEYS,
            column_families=EMPTY_COLUMN_FAMILIES,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID
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
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)

    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_creating_table_that_exists_with_different_column_families_gc_rule_in__table(
            self, mock_hook):
        op = BigtableTableCreateOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID,
            initial_split_keys=INITIAL_SPLIT_KEYS,
            column_families={"cf-id": MaxVersionsGCRule(1)},
            task_id="id",
            gcp_conn_id=GCP_CONN_ID
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
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)


class BigtableWaitForTableReplicationTest(unittest.TestCase):
    @parameterized.expand([
        ('instance_id', PROJECT_ID, '', TABLE_ID),
        ('table_id', PROJECT_ID, INSTANCE_ID, ''),
    ], testcase_func_name=lambda f, n, p: 'test_empty_attribute.empty_' + p.args[0])
    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_empty_attribute(self, missing_attribute, project_id, instance_id, table_id,
                             mock_hook):
        with self.assertRaises(AirflowException) as e:
            BigtableTableWaitForReplicationSensor(
                project_id=project_id,
                instance_id=instance_id,
                table_id=table_id,
                task_id="id",
                gcp_conn_id=GCP_CONN_ID
            )
        err = e.exception
        self.assertEqual(str(err), 'Empty parameter: {}'.format(missing_attribute))
        mock_hook.assert_not_called()

    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_wait_no_instance(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = None

        op = BigtableTableWaitForReplicationSensor(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID
        )
        self.assertFalse(op.poke(None))
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)

    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_wait_no_table(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = mock.Mock(Instance)
        mock_hook.return_value.get_cluster_states_for_table.side_effect = mock.Mock(
            side_effect=google.api_core.exceptions.NotFound("Table not found."))

        op = BigtableTableWaitForReplicationSensor(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID
        )
        self.assertFalse(op.poke(None))
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)

    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_wait_not_ready(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = mock.Mock(Instance)
        mock_hook.return_value.get_cluster_states_for_table.return_value = {
            "cl-id": ClusterState(0)
        }
        op = BigtableTableWaitForReplicationSensor(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID
        )
        self.assertFalse(op.poke(None))
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)

    @mock.patch('airflow.contrib.operators.gcp_bigtable_operator.BigtableHook')
    def test_wait_ready(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = mock.Mock(Instance)
        mock_hook.return_value.get_cluster_states_for_table.return_value = {
            "cl-id": ClusterState(4)
        }
        op = BigtableTableWaitForReplicationSensor(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            table_id=TABLE_ID,
            task_id="id",
            gcp_conn_id=GCP_CONN_ID
        )
        self.assertTrue(op.poke(None))
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
