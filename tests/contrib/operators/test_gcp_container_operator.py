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
from airflow.contrib.operators.gcp_container_operator import GKEClusterCreateOperator, \
    GKEClusterDeleteOperator

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

PROJECT_ID = 'test-id'
PROJECT_LOCATION = 'test-location'
PROJECT_TASK_ID = 'test-task-id'
CLUSTER_NAME = 'test-cluster-name'

PROJECT_BODY = {'name': 'test-name'}
PROJECT_BODY_CREATE = {'name': 'test-name', 'initial_node_count': 1}


class GoogleCloudPlatformContainerOperatorTest(unittest.TestCase):

    @mock.patch('airflow.contrib.operators.gcp_container_operator.GKEClusterHook')
    def test_create_execute(self, mock_hook):
        operator = GKEClusterCreateOperator(project_id=PROJECT_ID,
                                            location=PROJECT_LOCATION,
                                            body=PROJECT_BODY_CREATE,
                                            task_id=PROJECT_TASK_ID)

        operator.execute(None)
        mock_hook.return_value.create_cluster.assert_called_once_with(
            cluster=PROJECT_BODY_CREATE)

    @mock.patch('airflow.contrib.operators.gcp_container_operator.GKEClusterHook')
    def test_create_execute_error_body(self, mock_hook):
        with self.assertRaises(AirflowException):
            operator = GKEClusterCreateOperator(project_id=PROJECT_ID,
                                                location=PROJECT_LOCATION,
                                                body=None,
                                                task_id=PROJECT_TASK_ID)

            operator.execute(None)
            mock_hook.return_value.create_cluster.assert_not_called()

    @mock.patch('airflow.contrib.operators.gcp_container_operator.GKEClusterHook')
    def test_create_execute_error_project_id(self, mock_hook):
        with self.assertRaises(AirflowException):
            operator = GKEClusterCreateOperator(location=PROJECT_LOCATION,
                                                body=PROJECT_BODY,
                                                task_id=PROJECT_TASK_ID)

            operator.execute(None)
            mock_hook.return_value.create_cluster.assert_not_called()

    @mock.patch('airflow.contrib.operators.gcp_container_operator.GKEClusterHook')
    def test_create_execute_error_location(self, mock_hook):
        with self.assertRaises(AirflowException):
            operator = GKEClusterCreateOperator(project_id=PROJECT_ID,
                                                body=PROJECT_BODY,
                                                task_id=PROJECT_TASK_ID)

            operator.execute(None)
            mock_hook.return_value.create_cluster.assert_not_called()

    @mock.patch('airflow.contrib.operators.gcp_container_operator.GKEClusterHook')
    def test_delete_execute(self, mock_hook):
        operator = GKEClusterDeleteOperator(project_id=PROJECT_ID,
                                            name=CLUSTER_NAME,
                                            location=PROJECT_LOCATION,
                                            task_id=PROJECT_TASK_ID)

        operator.execute(None)
        mock_hook.return_value.delete_cluster.assert_called_once_with(
            name=CLUSTER_NAME)

    @mock.patch('airflow.contrib.operators.gcp_container_operator.GKEClusterHook')
    def test_delete_execute_error_project_id(self, mock_hook):
        with self.assertRaises(AirflowException):
            operator = GKEClusterDeleteOperator(location=PROJECT_LOCATION,
                                                name=CLUSTER_NAME,
                                                task_id=PROJECT_TASK_ID)
            operator.execute(None)
            mock_hook.return_value.delete_cluster.assert_not_called()

    @mock.patch('airflow.contrib.operators.gcp_container_operator.GKEClusterHook')
    def test_delete_execute_error_cluster_name(self, mock_hook):
        with self.assertRaises(AirflowException):
            operator = GKEClusterDeleteOperator(project_id=PROJECT_ID,
                                                location=PROJECT_LOCATION,
                                                task_id=PROJECT_TASK_ID)

            operator.execute(None)
            mock_hook.return_value.delete_cluster.assert_not_called()

    @mock.patch('airflow.contrib.operators.gcp_container_operator.GKEClusterHook')
    def test_delete_execute_error_location(self, mock_hook):
        with self.assertRaises(AirflowException):
            operator = GKEClusterDeleteOperator(project_id=PROJECT_ID,
                                                name=CLUSTER_NAME,
                                                task_id=PROJECT_TASK_ID)

            operator.execute(None)
            mock_hook.return_value.delete_cluster.assert_not_called()
