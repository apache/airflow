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

import os
import unittest

from parameterized import parameterized
from google.auth.environment_vars import CREDENTIALS

from airflow import AirflowException
from airflow.gcp.operators.kubernetes_engine import GKEClusterCreateOperator, \
    GKEClusterDeleteOperator, GKEPodOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from tests.compat import mock

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
    @mock.patch('airflow.gcp.operators.kubernetes_engine.GKEClusterHook')
    def test_create_execute(self, body, mock_hook):
        operator = GKEClusterCreateOperator(project_id=TEST_GCP_PROJECT_ID,
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
    @mock.patch('airflow.gcp.operators.kubernetes_engine.GKEClusterHook')
    def test_create_execute_error_body(self, body, mock_hook):
        with self.assertRaises(AirflowException):
            GKEClusterCreateOperator(project_id=TEST_GCP_PROJECT_ID,
                                     location=PROJECT_LOCATION,
                                     body=body,
                                     task_id=PROJECT_TASK_ID)

    # pylint:disable=no-value-for-parameter
    @mock.patch('airflow.gcp.operators.kubernetes_engine.GKEClusterHook')
    def test_create_execute_error_project_id(self, mock_hook):
        with self.assertRaises(AirflowException):
            GKEClusterCreateOperator(location=PROJECT_LOCATION,
                                     body=PROJECT_BODY,
                                     task_id=PROJECT_TASK_ID)

    # pylint:disable=no-value-for-parameter
    @mock.patch('airflow.gcp.operators.kubernetes_engine.GKEClusterHook')
    def test_create_execute_error_location(self, mock_hook):
        with self.assertRaises(AirflowException):
            GKEClusterCreateOperator(project_id=TEST_GCP_PROJECT_ID,
                                     body=PROJECT_BODY,
                                     task_id=PROJECT_TASK_ID)

    @mock.patch('airflow.gcp.operators.kubernetes_engine.GKEClusterHook')
    def test_delete_execute(self, mock_hook):
        operator = GKEClusterDeleteOperator(project_id=TEST_GCP_PROJECT_ID,
                                            name=CLUSTER_NAME,
                                            location=PROJECT_LOCATION,
                                            task_id=PROJECT_TASK_ID)

        operator.execute(None)
        mock_hook.return_value.delete_cluster.assert_called_once_with(
            name=CLUSTER_NAME, project_id=TEST_GCP_PROJECT_ID)

    # pylint:disable=no-value-for-parameter
    @mock.patch('airflow.gcp.operators.kubernetes_engine.GKEClusterHook')
    def test_delete_execute_error_project_id(self, mock_hook):
        with self.assertRaises(AirflowException):
            GKEClusterDeleteOperator(location=PROJECT_LOCATION,
                                     name=CLUSTER_NAME,
                                     task_id=PROJECT_TASK_ID)

    # pylint:disable=no-value-for-parameter
    @mock.patch('airflow.gcp.operators.kubernetes_engine.GKEClusterHook')
    def test_delete_execute_error_cluster_name(self, mock_hook):
        with self.assertRaises(AirflowException):
            GKEClusterDeleteOperator(project_id=TEST_GCP_PROJECT_ID,
                                     location=PROJECT_LOCATION,
                                     task_id=PROJECT_TASK_ID)

    # pylint:disable=no-value-for-parameter
    @mock.patch('airflow.gcp.operators.kubernetes_engine.GKEClusterHook')
    def test_delete_execute_error_location(self, mock_hook):
        with self.assertRaises(AirflowException):
            GKEClusterDeleteOperator(project_id=TEST_GCP_PROJECT_ID,
                                     name=CLUSTER_NAME,
                                     task_id=PROJECT_TASK_ID)


class TestGKEPodOperator(unittest.TestCase):
    def setUp(self):
        self.gke_op = GKEPodOperator(project_id=TEST_GCP_PROJECT_ID,
                                     location=PROJECT_LOCATION,
                                     cluster_name=CLUSTER_NAME,
                                     task_id=PROJECT_TASK_ID,
                                     name=TASK_NAME,
                                     namespace=NAMESPACE,
                                     image=IMAGE)
        if CREDENTIALS in os.environ:
            del os.environ[CREDENTIALS]

    def test_template_fields(self):
        self.assertTrue(set(KubernetesPodOperator.template_fields).issubset(
            GKEPodOperator.template_fields))

    # pylint:disable=unused-argument
    @mock.patch(
        'airflow.contrib.operators.kubernetes_pod_operator.KubernetesPodOperator.execute')
    @mock.patch('tempfile.NamedTemporaryFile')
    @mock.patch("subprocess.check_call")
    def test_execute_conn_id_none(self, proc_mock, file_mock, exec_mock):
        self.gke_op.gcp_conn_id = None

        file_mock.return_value.__enter__.return_value.name = FILE_NAME

        self.gke_op.execute(None)

        # Assert Environment Variable is being set correctly
        self.assertIn(KUBE_ENV_VAR, os.environ)
        self.assertEqual(os.environ[KUBE_ENV_VAR], FILE_NAME)

        # Assert the gcloud command being called correctly
        proc_mock.assert_called_once_with(
            GCLOUD_COMMAND.format(CLUSTER_NAME, PROJECT_LOCATION, TEST_GCP_PROJECT_ID).split())

        self.assertEqual(self.gke_op.config_file, FILE_NAME)

    # pylint:disable=unused-argument
    @mock.patch('airflow.hooks.base_hook.BaseHook.get_connection')
    @mock.patch(
        'airflow.contrib.operators.kubernetes_pod_operator.KubernetesPodOperator.execute')
    @mock.patch('tempfile.NamedTemporaryFile')
    @mock.patch("subprocess.check_call")
    @mock.patch.dict(os.environ, {})
    def test_execute_conn_id_path(self, proc_mock, file_mock, exec_mock, get_con_mock):
        # gcp_conn_id is defaulted to `google_cloud_default`

        file_path = '/path/to/file'
        kaeyfile_dict = {"extra__google_cloud_platform__key_path": file_path}
        get_con_mock.return_value.extra_dejson = kaeyfile_dict
        file_mock.return_value.__enter__.return_value.name = FILE_NAME

        self.gke_op.execute(None)

        # Assert Environment Variable is being set correctly
        self.assertIn(KUBE_ENV_VAR, os.environ)
        self.assertEqual(os.environ[KUBE_ENV_VAR], FILE_NAME)

        self.assertIn(CREDENTIALS, os.environ)
        # since we passed in keyfile_path we should get a file
        self.assertEqual(os.environ[CREDENTIALS], file_path)

        # Assert the gcloud command being called correctly
        proc_mock.assert_called_once_with(
            GCLOUD_COMMAND.format(CLUSTER_NAME, PROJECT_LOCATION, TEST_GCP_PROJECT_ID).split())

        self.assertEqual(self.gke_op.config_file, FILE_NAME)

    # pylint:disable=unused-argument
    @mock.patch.dict(os.environ, {})
    @mock.patch('airflow.hooks.base_hook.BaseHook.get_connection')
    @mock.patch(
        'airflow.contrib.operators.kubernetes_pod_operator.KubernetesPodOperator.execute')
    @mock.patch('tempfile.NamedTemporaryFile')
    @mock.patch("subprocess.check_call")
    def test_execute_conn_id_dict(self, proc_mock, file_mock, exec_mock, get_con_mock):
        # gcp_conn_id is defaulted to `google_cloud_default`
        file_path = '/path/to/file'

        # This is used in the _set_env_from_extras method
        file_mock.return_value.name = file_path
        # This is used in the execute method
        file_mock.return_value.__enter__.return_value.name = FILE_NAME

        keyfile_dict = {"extra__google_cloud_platform__keyfile_dict":
                        '{"private_key": "r4nd0m_k3y"}'}
        get_con_mock.return_value.extra_dejson = keyfile_dict

        self.gke_op.execute(None)

        # Assert Environment Variable is being set correctly
        self.assertIn(KUBE_ENV_VAR, os.environ)
        self.assertEqual(os.environ[KUBE_ENV_VAR], FILE_NAME)

        self.assertIn(CREDENTIALS, os.environ)
        # since we passed in keyfile_path we should get a file
        self.assertEqual(os.environ[CREDENTIALS], file_path)

        # Assert the gcloud command being called correctly
        proc_mock.assert_called_once_with(
            GCLOUD_COMMAND.format(CLUSTER_NAME, PROJECT_LOCATION, TEST_GCP_PROJECT_ID).split())

        self.assertEqual(self.gke_op.config_file, FILE_NAME)

    @mock.patch.dict(os.environ, {})
    def test_set_env_from_extras_none(self):
        extras = {}
        self.gke_op._set_env_from_extras(extras)
        # _set_env_from_extras should not edit os.environ if extras does not specify
        self.assertNotIn(CREDENTIALS, os.environ)

    @mock.patch.dict(os.environ, {})
    @mock.patch('tempfile.NamedTemporaryFile')
    def test_set_env_from_extras_dict(self, file_mock):
        keyfile_dict_str = '{ \"test\": \"cluster\" }'
        extras = {
            'extra__google_cloud_platform__keyfile_dict': keyfile_dict_str,
        }

        def mock_temp_write(content):
            if not isinstance(content, bytes):
                raise TypeError("a bytes-like object is required, not {}".format(type(content).__name__))

        file_mock.return_value.write = mock_temp_write
        file_mock.return_value.name = FILE_NAME

        key_file = self.gke_op._set_env_from_extras(extras)
        self.assertEqual(os.environ[CREDENTIALS], FILE_NAME)
        self.assertIsInstance(key_file, mock.MagicMock)

    @mock.patch.dict(os.environ, {})
    def test_set_env_from_extras_path(self):
        test_path = '/test/path'

        extras = {
            'extra__google_cloud_platform__key_path': test_path,
        }

        self.gke_op._set_env_from_extras(extras)
        self.assertEqual(os.environ[CREDENTIALS], test_path)

    def test_get_field(self):
        field_name = 'test_field'
        field_value = 'test_field_value'
        extras = {
            'extra__google_cloud_platform__{}'.format(field_name):
                field_value
        }

        ret_val = self.gke_op._get_field(extras, field_name)
        self.assertEqual(field_value, ret_val)

    @mock.patch('airflow.gcp.operators.kubernetes_engine.GKEPodOperator.log')
    def test_get_field_fail(self, log_mock):
        log_mock.info = mock.Mock()
        log_str = 'Field %s not found in extras.'
        field_name = 'test_field'
        field_value = 'test_field_value'

        extras = {}

        ret_val = self.gke_op._get_field(extras, field_name, default=field_value)
        # Assert default is returned upon failure
        self.assertEqual(field_value, ret_val)
        log_mock.info.assert_called_once_with(log_str, field_name)
