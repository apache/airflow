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
#
import unittest

import mock
from google.cloud.container_v1.types import Cluster
from mock import PropertyMock

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.kubernetes_engine import GKEHook

TASK_ID = 'test-gke-cluster-operator'
CLUSTER_NAME = 'test-cluster'
TEST_GCP_PROJECT_ID = 'test-project'
GKE_ZONE = 'test-zone'


class TestGKEHookClient(unittest.TestCase):
    def setUp(self):
        self.gke_hook = GKEHook(location=GKE_ZONE)

    @mock.patch(
        "airflow.providers.google.cloud.hooks.kubernetes_engine.GKEHook.client_info",
        new_callable=mock.PropertyMock
    )
    @mock.patch("airflow.providers.google.cloud.hooks.kubernetes_engine.GKEHook._get_credentials")
    @mock.patch("airflow.providers.google.cloud.hooks.kubernetes_engine.container_v1.ClusterManagerClient")
    def test_gke_cluster_client_creation(self, mock_client, mock_get_creds, mock_client_info):

        result = self.gke_hook.get_conn()
        mock_client.assert_called_once_with(
            credentials=mock_get_creds.return_value,
            client_info=mock_client_info.return_value
        )
        self.assertEqual(mock_client.return_value, result)
        self.assertEqual(self.gke_hook._client, result)


class TestGKEHookDelete(unittest.TestCase):
    def setUp(self):
        self.gke_hook = GKEHook(location=GKE_ZONE)
        self.gke_hook._client = mock.Mock()

    @mock.patch(
        'airflow.providers.google.cloud.hooks.base.CloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch("airflow.providers.google.cloud.hooks.kubernetes_engine.ParseDict")
    @mock.patch(
        "airflow.providers.google.cloud.hooks.kubernetes_engine.GKEHook.wait_for_operation")
    def test_delete_cluster(self, wait_mock, convert_mock, mock_project_id):
        retry_mock, timeout_mock = mock.Mock(), mock.Mock()

        client_delete = self.gke_hook._client.delete_cluster = mock.Mock()

        self.gke_hook.delete_cluster(name=CLUSTER_NAME, project_id=TEST_GCP_PROJECT_ID,
                                     retry=retry_mock,
                                     timeout=timeout_mock)

        client_delete.assert_called_once_with(project_id=TEST_GCP_PROJECT_ID,
                                              zone=GKE_ZONE,
                                              cluster_id=CLUSTER_NAME,
                                              retry=retry_mock,
                                              timeout=timeout_mock)
        wait_mock.assert_called_once_with(client_delete.return_value)
        convert_mock.assert_not_called()

    @mock.patch(
        'airflow.providers.google.cloud.hooks.base.CloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch(
        "airflow.providers.google.cloud.hooks.kubernetes_engine.GKEHook.log")
    @mock.patch("airflow.providers.google.cloud.hooks.kubernetes_engine.ParseDict")
    @mock.patch(
        "airflow.providers.google.cloud.hooks.kubernetes_engine.GKEHook.wait_for_operation")
    def test_delete_cluster_not_found(self, wait_mock, convert_mock, log_mock, mock_project_id):
        from google.api_core.exceptions import NotFound
        # To force an error
        message = 'Not Found'
        self.gke_hook._client.delete_cluster.side_effect = NotFound(message=message)

        self.gke_hook.delete_cluster(name='not-existing', project_id=TEST_GCP_PROJECT_ID)
        wait_mock.assert_not_called()
        convert_mock.assert_not_called()
        log_mock.info.assert_any_call("Assuming Success: %s", message)

    @mock.patch(
        'airflow.providers.google.cloud.hooks.base.CloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch("airflow.providers.google.cloud.hooks.kubernetes_engine.ParseDict")
    @mock.patch(
        "airflow.providers.google.cloud.hooks.kubernetes_engine.GKEHook.wait_for_operation")
    def test_delete_cluster_error(self, wait_mock, convert_mock, mock_project_id):
        # To force an error
        self.gke_hook._client.delete_cluster.side_effect = AirflowException('400')

        with self.assertRaises(AirflowException):
            self.gke_hook.delete_cluster(name='a-cluster')
            wait_mock.assert_not_called()
            convert_mock.assert_not_called()


class TestGKEHookCreate(unittest.TestCase):
    def setUp(self):
        self.gke_hook = GKEHook(location=GKE_ZONE)
        self.gke_hook._client = mock.Mock()

    @mock.patch(
        'airflow.providers.google.cloud.hooks.base.CloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch("airflow.providers.google.cloud.hooks.kubernetes_engine.ParseDict")
    @mock.patch(
        "airflow.providers.google.cloud.hooks.kubernetes_engine.GKEHook.wait_for_operation")
    def test_create_cluster_proto(self, wait_mock, convert_mock, mock_project_id):
        mock_cluster_proto = Cluster()
        mock_cluster_proto.name = CLUSTER_NAME

        retry_mock, timeout_mock = mock.Mock(), mock.Mock()

        client_create = self.gke_hook._client.create_cluster = mock.Mock()

        self.gke_hook.create_cluster(cluster=mock_cluster_proto,
                                     project_id=TEST_GCP_PROJECT_ID,
                                     retry=retry_mock,
                                     timeout=timeout_mock)

        client_create.assert_called_once_with(project_id=TEST_GCP_PROJECT_ID,
                                              zone=GKE_ZONE,
                                              cluster=mock_cluster_proto,
                                              retry=retry_mock, timeout=timeout_mock)
        wait_mock.assert_called_once_with(client_create.return_value)
        convert_mock.assert_not_called()

    @mock.patch(
        'airflow.providers.google.cloud.hooks.base.CloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch("airflow.providers.google.cloud.hooks.kubernetes_engine.ParseDict")
    @mock.patch(
        "airflow.providers.google.cloud.hooks.kubernetes_engine.GKEHook.wait_for_operation")
    def test_create_cluster_dict(self, wait_mock, convert_mock, mock_project_id):
        mock_cluster_dict = {'name': CLUSTER_NAME}
        retry_mock, timeout_mock = mock.Mock(), mock.Mock()

        client_create = self.gke_hook._client.create_cluster = mock.Mock()
        proto_mock = convert_mock.return_value = mock.Mock()

        self.gke_hook.create_cluster(cluster=mock_cluster_dict,
                                     project_id=TEST_GCP_PROJECT_ID,
                                     retry=retry_mock,
                                     timeout=timeout_mock)

        client_create.assert_called_once_with(project_id=TEST_GCP_PROJECT_ID,
                                              zone=GKE_ZONE,
                                              cluster=proto_mock,
                                              retry=retry_mock, timeout=timeout_mock)
        wait_mock.assert_called_once_with(client_create.return_value)
        convert_mock.assert_called_once_with(
            {'name': 'test-cluster'},
            Cluster()
        )

    @mock.patch("airflow.providers.google.cloud.hooks.kubernetes_engine.ParseDict")
    @mock.patch(
        "airflow.providers.google.cloud.hooks.kubernetes_engine.GKEHook.wait_for_operation")
    def test_create_cluster_error(self, wait_mock, convert_mock):
        # to force an error
        mock_cluster_proto = None

        with self.assertRaises(AirflowException):
            self.gke_hook.create_cluster(mock_cluster_proto)
            wait_mock.assert_not_called()
            convert_mock.assert_not_called()

    @mock.patch(
        'airflow.providers.google.cloud.hooks.kubernetes_engine.GKEHook._get_credentials_and_project_id',
        return_value=(mock.MagicMock(), TEST_GCP_PROJECT_ID)
    )
    @mock.patch(
        "airflow.providers.google.cloud.hooks.kubernetes_engine.GKEHook.log")
    @mock.patch("airflow.providers.google.cloud.hooks.kubernetes_engine.ParseDict")
    @mock.patch(
        "airflow.providers.google.cloud.hooks.kubernetes_engine.GKEHook.wait_for_operation")
    def test_create_cluster_already_exists(self, wait_mock, convert_mock, log_mock, mock_get_credentials):
        from google.api_core.exceptions import AlreadyExists
        # To force an error
        message = 'Already Exists'
        self.gke_hook._client.create_cluster.side_effect = AlreadyExists(message=message)

        self.gke_hook.create_cluster(cluster={}, project_id=TEST_GCP_PROJECT_ID)
        wait_mock.assert_not_called()
        self.assertEqual(convert_mock.call_count, 1)
        log_mock.info.assert_any_call("Assuming Success: %s", message)


class TestGKEHookGet(unittest.TestCase):
    def setUp(self):
        self.gke_hook = GKEHook(location=GKE_ZONE)
        self.gke_hook._client = mock.Mock()

    def test_get_cluster(self):
        retry_mock, timeout_mock = mock.Mock(), mock.Mock()

        client_get = self.gke_hook._client.get_cluster = mock.Mock()

        self.gke_hook.get_cluster(name=CLUSTER_NAME,
                                  project_id=TEST_GCP_PROJECT_ID,
                                  retry=retry_mock,
                                  timeout=timeout_mock)

        client_get.assert_called_once_with(project_id=TEST_GCP_PROJECT_ID,
                                           zone=GKE_ZONE,
                                           cluster_id=CLUSTER_NAME,
                                           retry=retry_mock, timeout=timeout_mock)


class TestGKEHook(unittest.TestCase):

    def setUp(self):
        self.gke_hook = GKEHook(location=GKE_ZONE)
        self.gke_hook._client = mock.Mock()

    @mock.patch('airflow.providers.google.cloud.hooks.kubernetes_engine.container_v1.'
                'ClusterManagerClient')
    @mock.patch('airflow.providers.google.cloud.hooks.base.ClientInfo')
    @mock.patch('airflow.providers.google.cloud.hooks.kubernetes_engine.GKEHook._get_credentials')
    def test_get_client(self, mock_get_credentials, mock_client_info, mock_client):
        self.gke_hook._client = None
        self.gke_hook.get_conn()
        assert mock_get_credentials.called
        mock_client.assert_called_once_with(
            credentials=mock_get_credentials.return_value,
            client_info=mock_client_info.return_value)

    def test_get_operation(self):
        self.gke_hook._client.get_operation = mock.Mock()
        self.gke_hook.get_operation('TEST_OP', project_id=TEST_GCP_PROJECT_ID)
        self.gke_hook._client.get_operation.assert_called_once_with(
            project_id=TEST_GCP_PROJECT_ID, zone=GKE_ZONE, operation_id='TEST_OP')

    def test_append_label(self):
        key = 'test-key'
        val = 'test-val'
        mock_proto = mock.Mock()
        self.gke_hook._append_label(mock_proto, key, val)
        mock_proto.resource_labels.update.assert_called_once_with({key: val})

    def test_append_label_replace(self):
        key = 'test-key'
        val = 'test.val+this'
        mock_proto = mock.Mock()
        self.gke_hook._append_label(mock_proto, key, val)
        mock_proto.resource_labels.update.assert_called_once_with({key: 'test-val-this'})

    @mock.patch("airflow.providers.google.cloud.hooks.kubernetes_engine.time.sleep")
    def test_wait_for_response_done(self, time_mock):
        from google.cloud.container_v1.gapic.enums import Operation
        mock_op = mock.Mock()
        mock_op.status = Operation.Status.DONE
        self.gke_hook.wait_for_operation(mock_op)
        self.assertEqual(time_mock.call_count, 1)

    @mock.patch("airflow.providers.google.cloud.hooks.kubernetes_engine.time.sleep")
    def test_wait_for_response_exception(self, time_mock):
        from google.cloud.container_v1.gapic.enums import Operation
        from google.cloud.exceptions import GoogleCloudError

        mock_op = mock.Mock()
        mock_op.status = Operation.Status.ABORTING

        with self.assertRaises(GoogleCloudError):
            self.gke_hook.wait_for_operation(mock_op)
            self.assertEqual(time_mock.call_count, 1)

    @mock.patch("airflow.providers.google.cloud.hooks.kubernetes_engine.GKEHook.get_operation")
    @mock.patch("airflow.providers.google.cloud.hooks.kubernetes_engine.time.sleep")
    def test_wait_for_response_running(self, time_mock, operation_mock):
        from google.cloud.container_v1.gapic.enums import Operation

        running_op, done_op, pending_op = mock.Mock(), mock.Mock(), mock.Mock()
        running_op.status = Operation.Status.RUNNING
        done_op.status = Operation.Status.DONE
        pending_op.status = Operation.Status.PENDING

        # Status goes from Running -> Pending -> Done
        operation_mock.side_effect = [pending_op, done_op]
        self.gke_hook.wait_for_operation(running_op, project_id=TEST_GCP_PROJECT_ID)

        self.assertEqual(time_mock.call_count, 3)
        operation_mock.assert_any_call(running_op.name, project_id=TEST_GCP_PROJECT_ID)
        operation_mock.assert_any_call(pending_op.name, project_id=TEST_GCP_PROJECT_ID)
        self.assertEqual(operation_mock.call_count, 2)
