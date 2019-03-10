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

from airflow import AirflowException
from airflow.contrib.operators.gcp_container_operator import GKEClusterCreateOperator, \
    GKEClusterDeleteOperator, GKEPodOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

TEST_GCP_PROJECT_ID = 'test-id'
PROJECT_LOCATION = 'test-location'
PROJECT_TASK_ID = 'test-task-id'
CLUSTER_NAME = 'test-cluster-name'

PROJECT_BODY = {'name': 'test-name'}
PROJECT_BODY_CREATE = {'name': 'test-name', 'initial_node_count': 1}

TASK_NAME = 'test-task-name'
NAMESPACE = 'default',
IMAGE = 'bash'

GCLOUD_COMMAND = "gcloud container clusters get-credentials {} --zone {} --project {}"
KUBE_ENV_VAR = 'KUBECONFIG'
GAC_ENV_VAR = 'GOOGLE_APPLICATION_CREDENTIALS'
FILE_NAME = '/tmp/mock_name'


class GoogleCloudPlatformContainerOperatorTest(unittest.TestCase):

    @mock.patch('airflow.contrib.operators.gcp_container_operator.GKEClusterHook')
    def test_create_execute(self, mock_hook):
        operator = GKEClusterCreateOperator(project_id=TEST_GCP_PROJECT_ID,
                                            location=PROJECT_LOCATION,
                                            body=PROJECT_BODY_CREATE,
                                            task_id=PROJECT_TASK_ID)

        operator.execute(None)
        mock_hook.return_value.create_cluster.assert_called_once_with(
            cluster=PROJECT_BODY_CREATE)

    @mock.patch('airflow.contrib.operators.gcp_container_operator.GKEClusterHook')
    def test_create_execute_error_body(self, mock_hook):
        with self.assertRaises(AirflowException):
            operator = GKEClusterCreateOperator(project_id=TEST_GCP_PROJECT_ID,
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
            operator = GKEClusterCreateOperator(project_id=TEST_GCP_PROJECT_ID,
                                                body=PROJECT_BODY,
                                                task_id=PROJECT_TASK_ID)

            operator.execute(None)
            mock_hook.return_value.create_cluster.assert_not_called()

    @mock.patch('airflow.contrib.operators.gcp_container_operator.GKEClusterHook')
    def test_delete_execute(self, mock_hook):
        operator = GKEClusterDeleteOperator(project_id=TEST_GCP_PROJECT_ID,
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
            operator = GKEClusterDeleteOperator(project_id=TEST_GCP_PROJECT_ID,
                                                location=PROJECT_LOCATION,
                                                task_id=PROJECT_TASK_ID)

            operator.execute(None)
            mock_hook.return_value.delete_cluster.assert_not_called()

    @mock.patch('airflow.contrib.operators.gcp_container_operator.GKEClusterHook')
    def test_delete_execute_error_location(self, mock_hook):
        with self.assertRaises(AirflowException):
            operator = GKEClusterDeleteOperator(project_id=TEST_GCP_PROJECT_ID,
                                                name=CLUSTER_NAME,
                                                task_id=PROJECT_TASK_ID)

            operator.execute(None)
            mock_hook.return_value.delete_cluster.assert_not_called()


class GKEPodOperatorTest(unittest.TestCase):
    def setUp(self):
        self.gke_op = GKEPodOperator(project_id=TEST_GCP_PROJECT_ID,
                                     location=PROJECT_LOCATION,
                                     cluster_name=CLUSTER_NAME,
                                     task_id=PROJECT_TASK_ID,
                                     name=TASK_NAME,
                                     namespace=NAMESPACE,
                                     image=IMAGE)
        if GAC_ENV_VAR in os.environ:
            del os.environ[GAC_ENV_VAR]

    def test_template_fields(self):
        self.assertTrue(set(KubernetesPodOperator.template_fields).issubset(
            GKEPodOperator.template_fields))

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
        proc_mock.assert_called_with(
            GCLOUD_COMMAND.format(CLUSTER_NAME, PROJECT_LOCATION, TEST_GCP_PROJECT_ID).split())

        self.assertEqual(self.gke_op.config_file, FILE_NAME)

    @mock.patch('airflow.hooks.base_hook.BaseHook.get_connection')
    @mock.patch(
        'airflow.contrib.operators.kubernetes_pod_operator.KubernetesPodOperator.execute')
    @mock.patch('tempfile.NamedTemporaryFile')
    @mock.patch("subprocess.check_call")
    @mock.patch.dict(os.environ, {})
    def test_execute_conn_id_path(self, proc_mock, file_mock, exec_mock, get_con_mock):
        # gcp_conn_id is defaulted to `google_cloud_default`

        FILE_PATH = '/path/to/file'
        KEYFILE_DICT = {"extra__google_cloud_platform__key_path": FILE_PATH}
        get_con_mock.return_value.extra_dejson = KEYFILE_DICT
        file_mock.return_value.__enter__.return_value.name = FILE_NAME

        self.gke_op.execute(None)

        # Assert Environment Variable is being set correctly
        self.assertIn(KUBE_ENV_VAR, os.environ)
        self.assertEqual(os.environ[KUBE_ENV_VAR], FILE_NAME)

        self.assertIn(GAC_ENV_VAR, os.environ)
        # since we passed in keyfile_path we should get a file
        self.assertEqual(os.environ[GAC_ENV_VAR], FILE_PATH)

        # Assert the gcloud command being called correctly
        proc_mock.assert_called_with(
            GCLOUD_COMMAND.format(CLUSTER_NAME, PROJECT_LOCATION, TEST_GCP_PROJECT_ID).split())

        self.assertEqual(self.gke_op.config_file, FILE_NAME)

    @mock.patch.dict(os.environ, {})
    @mock.patch('airflow.hooks.base_hook.BaseHook.get_connection')
    @mock.patch(
        'airflow.contrib.operators.kubernetes_pod_operator.KubernetesPodOperator.execute')
    @mock.patch('tempfile.NamedTemporaryFile')
    @mock.patch("subprocess.check_call")
    def test_execute_conn_id_dict(self, proc_mock, file_mock, exec_mock, get_con_mock):
        # gcp_conn_id is defaulted to `google_cloud_default`
        FILE_PATH = '/path/to/file'

        # This is used in the _set_env_from_extras method
        file_mock.return_value.name = FILE_PATH
        # This is used in the execute method
        file_mock.return_value.__enter__.return_value.name = FILE_NAME

        KEYFILE_DICT = {"extra__google_cloud_platform__keyfile_dict":
                        '{"private_key": "r4nd0m_k3y"}'}
        get_con_mock.return_value.extra_dejson = KEYFILE_DICT

        self.gke_op.execute(None)

        # Assert Environment Variable is being set correctly
        self.assertIn(KUBE_ENV_VAR, os.environ)
        self.assertEqual(os.environ[KUBE_ENV_VAR], FILE_NAME)

        self.assertIn(GAC_ENV_VAR, os.environ)
        # since we passed in keyfile_path we should get a file
        self.assertEqual(os.environ[GAC_ENV_VAR], FILE_PATH)

        # Assert the gcloud command being called correctly
        proc_mock.assert_called_with(
            GCLOUD_COMMAND.format(CLUSTER_NAME, PROJECT_LOCATION, TEST_GCP_PROJECT_ID).split())

        self.assertEqual(self.gke_op.config_file, FILE_NAME)

    @mock.patch.dict(os.environ, {})
    def test_set_env_from_extras_none(self):
        extras = {}
        self.gke_op._set_env_from_extras(extras)
        # _set_env_from_extras should not edit os.environ if extras does not specify
        self.assertNotIn(GAC_ENV_VAR, os.environ)

    @mock.patch.dict(os.environ, {})
    @mock.patch('tempfile.NamedTemporaryFile')
    def test_set_env_from_extras_dict(self, file_mock):
        file_mock.return_value.name = FILE_NAME

        KEYFILE_DICT_STR = '{ \"test\": \"cluster\" }'
        extras = {
            'extra__google_cloud_platform__keyfile_dict': KEYFILE_DICT_STR,
        }

        self.gke_op._set_env_from_extras(extras)
        self.assertEqual(os.environ[GAC_ENV_VAR], FILE_NAME)

        file_mock.return_value.write.assert_called_once_with(KEYFILE_DICT_STR)

    @mock.patch.dict(os.environ, {})
    def test_set_env_from_extras_path(self):
        TEST_PATH = '/test/path'

        extras = {
            'extra__google_cloud_platform__key_path': TEST_PATH,
        }

        self.gke_op._set_env_from_extras(extras)
        self.assertEqual(os.environ[GAC_ENV_VAR], TEST_PATH)

    def test_get_field(self):
        FIELD_NAME = 'test_field'
        FIELD_VALUE = 'test_field_value'
        extras = {
            'extra__google_cloud_platform__{}'.format(FIELD_NAME):
                FIELD_VALUE
        }

        ret_val = self.gke_op._get_field(extras, FIELD_NAME)
        self.assertEqual(FIELD_VALUE, ret_val)

    @mock.patch('airflow.contrib.operators.gcp_container_operator.GKEPodOperator.log')
    def test_get_field_fail(self, log_mock):
        log_mock.info = mock.Mock()
        LOG_STR = 'Field %s not found in extras.'
        FIELD_NAME = 'test_field'
        FIELD_VALUE = 'test_field_value'

        extras = {}

        ret_val = self.gke_op._get_field(extras, FIELD_NAME, default=FIELD_VALUE)
        # Assert default is returned upon failure
        self.assertEqual(FIELD_VALUE, ret_val)
        log_mock.info.assert_called_with(LOG_STR, FIELD_NAME)
