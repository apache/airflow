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

import mock
from google.auth.environment_vars import CREDENTIALS
from mock import PropertyMock
from parameterized import parameterized

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKECreateClusterOperator, GKEDeleteClusterOperator, GKEStartPodOperator,
)

TEST_GCP_PROJECT_ID = 'test-id'
PROJECT_LOCATION = 'test-location'
PROJECT_TASK_ID = 'test-task-id'
CLUSTER_NAME = 'test-cluster-name'

PROJECT_BODY = {'name': 'test-name'}
PROJECT_BODY_CREATE_DICT = {'name': 'test-name', 'initial_node_count': 1}
PROJECT_BODY_CREATE_CLUSTER = type(
    "Cluster", (object,), {"name": "test-name", "initial_node_count": 1}
)()

TASK_NAME = 'test-task-name'
NAMESPACE = ('default',)
IMAGE = 'bash'

GCLOUD_COMMAND = "gcloud container clusters get-credentials {} --zone {} --project {}"
KUBE_ENV_VAR = 'KUBECONFIG'
FILE_NAME = '/tmp/mock_name'


class TestGoogleCloudPlatformContainerOperator(unittest.TestCase):

    @parameterized.expand(
        (body,) for body in [PROJECT_BODY_CREATE_DICT, PROJECT_BODY_CREATE_CLUSTER]
    )
    @mock.patch('airflow.providers.google.cloud.operators.kubernetes_engine.GKEHook')
    def test_create_execute(self, body, mock_hook):
        operator = GKECreateClusterOperator(project_id=TEST_GCP_PROJECT_ID,
                                            location=PROJECT_LOCATION,
                                            body=body,
                                            task_id=PROJECT_TASK_ID)

        operator.execute(None)
        mock_hook.return_value.create_cluster.assert_called_once_with(
            cluster=body, project_id=TEST_GCP_PROJECT_ID)

    @parameterized.expand(
        (body,) for body in [
            None,
            {'missing_name': 'test-name', 'initial_node_count': 1},
            {'name': 'test-name', 'missing_initial_node_count': 1},
            type('Cluster', (object,), {'missing_name': 'test-name', 'initial_node_count': 1})(),
            type('Cluster', (object,), {'name': 'test-name', 'missing_initial_node_count': 1})(),
        ]
    )
    @mock.patch('airflow.providers.google.cloud.operators.kubernetes_engine.GKEHook')
    def test_create_execute_error_body(self, body, mock_hook):
        with self.assertRaises(AirflowException):
            GKECreateClusterOperator(project_id=TEST_GCP_PROJECT_ID,
                                     location=PROJECT_LOCATION,
                                     body=body,
                                     task_id=PROJECT_TASK_ID)

    # pylint: disable=no-value-for-parameter
    @mock.patch('airflow.providers.google.cloud.operators.kubernetes_engine.GKEHook')
    def test_create_execute_error_project_id(self, mock_hook):
        with self.assertRaises(AirflowException):
            GKECreateClusterOperator(location=PROJECT_LOCATION,
                                     body=PROJECT_BODY,
                                     task_id=PROJECT_TASK_ID)

    # pylint: disable=no-value-for-parameter
    @mock.patch('airflow.providers.google.cloud.operators.kubernetes_engine.GKEHook')
    def test_create_execute_error_location(self, mock_hook):
        with self.assertRaises(AirflowException):
            GKECreateClusterOperator(project_id=TEST_GCP_PROJECT_ID,
                                     body=PROJECT_BODY,
                                     task_id=PROJECT_TASK_ID)

    @mock.patch('airflow.providers.google.cloud.operators.kubernetes_engine.GKEHook')
    def test_delete_execute(self, mock_hook):
        operator = GKEDeleteClusterOperator(project_id=TEST_GCP_PROJECT_ID,
                                            name=CLUSTER_NAME,
                                            location=PROJECT_LOCATION,
                                            task_id=PROJECT_TASK_ID)

        operator.execute(None)
        mock_hook.return_value.delete_cluster.assert_called_once_with(
            name=CLUSTER_NAME, project_id=TEST_GCP_PROJECT_ID)

    # pylint: disable=no-value-for-parameter
    @mock.patch('airflow.providers.google.cloud.operators.kubernetes_engine.GKEHook')
    def test_delete_execute_error_project_id(self, mock_hook):
        with self.assertRaises(AirflowException):
            GKEDeleteClusterOperator(location=PROJECT_LOCATION,
                                     name=CLUSTER_NAME,
                                     task_id=PROJECT_TASK_ID)

    # pylint: disable=no-value-for-parameter
    @mock.patch('airflow.providers.google.cloud.operators.kubernetes_engine.GKEHook')
    def test_delete_execute_error_cluster_name(self, mock_hook):
        with self.assertRaises(AirflowException):
            GKEDeleteClusterOperator(project_id=TEST_GCP_PROJECT_ID,
                                     location=PROJECT_LOCATION,
                                     task_id=PROJECT_TASK_ID)

    # pylint: disable=no-value-for-parameter
    @mock.patch('airflow.providers.google.cloud.operators.kubernetes_engine.GKEHook')
    def test_delete_execute_error_location(self, mock_hook):
        with self.assertRaises(AirflowException):
            GKEDeleteClusterOperator(project_id=TEST_GCP_PROJECT_ID,
                                     name=CLUSTER_NAME,
                                     task_id=PROJECT_TASK_ID)


class TestGKEPodOperator(unittest.TestCase):
    def setUp(self):
        self.gke_op = GKEStartPodOperator(project_id=TEST_GCP_PROJECT_ID,
                                          location=PROJECT_LOCATION,
                                          cluster_name=CLUSTER_NAME,
                                          task_id=PROJECT_TASK_ID,
                                          name=TASK_NAME,
                                          namespace=NAMESPACE,
                                          image=IMAGE)

    def test_template_fields(self):
        self.assertTrue(set(KubernetesPodOperator.template_fields).issubset(
            GKEStartPodOperator.template_fields))

    # pylint: disable=unused-argument
    @mock.patch(
        "airflow.hooks.base_hook.BaseHook.get_connections",
        return_value=[Connection(
            extra=json.dumps({})
        )]
    )
    @mock.patch(
        'airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator.execute')
    @mock.patch('tempfile.NamedTemporaryFile')
    @mock.patch("subprocess.check_call")
    @mock.patch.dict(os.environ, {CREDENTIALS: '/tmp/local-creds'})
    def test_execute_conn_id_none(self, proc_mock, file_mock, exec_mock, get_conn):
        type(file_mock.return_value.__enter__.return_value).name = PropertyMock(side_effect=[
            FILE_NAME
        ])

        def assert_credentials(*args, **kwargs):
            # since we passed in keyfile_path we should get a file
            self.assertIn(CREDENTIALS, os.environ)
            self.assertEqual(os.environ[CREDENTIALS], '/tmp/local-creds')

        proc_mock.side_effect = assert_credentials

        self.gke_op.execute(None)

        # Assert Environment Variable is being set correctly
        self.assertIn(KUBE_ENV_VAR, os.environ)
        self.assertEqual(os.environ[KUBE_ENV_VAR], FILE_NAME)

        # Assert the gcloud command being called correctly
        proc_mock.assert_called_once_with(
            GCLOUD_COMMAND.format(CLUSTER_NAME, PROJECT_LOCATION, TEST_GCP_PROJECT_ID).split())

        self.assertEqual(self.gke_op.config_file, FILE_NAME)

    # pylint: disable=unused-argument
    @mock.patch(
        "airflow.hooks.base_hook.BaseHook.get_connections",
        return_value=[Connection(
            extra=json.dumps({
                'extra__google_cloud_platform__key_path': '/path/to/file'
            })
        )]
    )
    @mock.patch(
        'airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator.execute')
    @mock.patch('tempfile.NamedTemporaryFile')
    @mock.patch("subprocess.check_call")
    @mock.patch.dict(os.environ, {})
    def test_execute_conn_id_path(self, proc_mock, file_mock, exec_mock, get_con_mock):
        type(file_mock.return_value.__enter__.return_value).name = PropertyMock(side_effect=[
            FILE_NAME
        ])

        def assert_credentials(*args, **kwargs):
            # since we passed in keyfile_path we should get a file
            self.assertIn(CREDENTIALS, os.environ)
            self.assertEqual(os.environ[CREDENTIALS], '/path/to/file')

        proc_mock.side_effect = assert_credentials
        self.gke_op.execute(None)

        # Assert Environment Variable is being set correctly
        self.assertIn(KUBE_ENV_VAR, os.environ)
        self.assertEqual(os.environ[KUBE_ENV_VAR], FILE_NAME)

        # Assert the gcloud command being called correctly
        proc_mock.assert_called_once_with(
            GCLOUD_COMMAND.format(CLUSTER_NAME, PROJECT_LOCATION, TEST_GCP_PROJECT_ID).split())

        self.assertEqual(self.gke_op.config_file, FILE_NAME)

    # pylint: disable=unused-argument
    @mock.patch.dict(os.environ, {})
    @mock.patch(
        "airflow.hooks.base_hook.BaseHook.get_connections",
        return_value=[Connection(
            extra=json.dumps({
                "extra__google_cloud_platform__keyfile_dict": '{"private_key": "r4nd0m_k3y"}'
            })
        )]
    )
    @mock.patch(
        'airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator.execute')
    @mock.patch('tempfile.NamedTemporaryFile')
    @mock.patch("subprocess.check_call")
    def test_execute_conn_id_dict(self, proc_mock, file_mock, exec_mock, get_con_mock):
        type(file_mock.return_value.__enter__.return_value).name = PropertyMock(side_effect=[
            FILE_NAME, '/path/to/new-file'
        ])

        def assert_credentials(*args, **kwargs):
            # since we passed in keyfile_dict we should get a new file
            self.assertIn(CREDENTIALS, os.environ)
            self.assertEqual(os.environ[CREDENTIALS], '/path/to/new-file')

        proc_mock.side_effect = assert_credentials

        self.gke_op.execute(None)

        # Assert Environment Variable is being set correctly
        self.assertIn(KUBE_ENV_VAR, os.environ)
        self.assertEqual(os.environ[KUBE_ENV_VAR], FILE_NAME)

        # Assert the gcloud command being called correctly
        proc_mock.assert_called_once_with(
            GCLOUD_COMMAND.format(CLUSTER_NAME, PROJECT_LOCATION, TEST_GCP_PROJECT_ID).split())

        self.assertEqual(self.gke_op.config_file, FILE_NAME)
