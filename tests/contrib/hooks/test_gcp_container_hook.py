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
#
import unittest

from airflow import AirflowException
from airflow.contrib.hooks.gcp_container_hook import GKEClusterHook
from tests.compat import mock


TASK_ID = 'test-gke-cluster-operator'
CLUSTER_NAME = 'test-cluster'
TEST_GCP_PROJECT_ID = 'test-project'
GKE_ZONE = 'test-zone'


class GKEClusterHookDeleteTest(unittest.TestCase):
    def setUp(self):
        self.gke_hook = GKEClusterHook(location=GKE_ZONE)
        self.gke_hook._client = mock.Mock()

    @mock.patch("airflow.contrib.hooks.gcp_container_hook.GKEClusterHook._dict_to_proto")
    @mock.patch(
        "airflow.contrib.hooks.gcp_container_hook.GKEClusterHook.wait_for_operation")
    def test_delete_cluster(self, wait_mock, convert_mock):
        retry_mock, timeout_mock = mock.Mock(), mock.Mock()

        client_delete = self.gke_hook._client.delete_cluster = mock.Mock()

        self.gke_hook.delete_cluster(name=CLUSTER_NAME, project_id=TEST_GCP_PROJECT_ID,
                                     retry=retry_mock,
                                     timeout=timeout_mock)

        client_delete.assert_called_with(project_id=TEST_GCP_PROJECT_ID,
                                         zone=GKE_ZONE,
                                         cluster_id=CLUSTER_NAME,
                                         retry=retry_mock,
                                         timeout=timeout_mock)
        wait_mock.assert_called_with(client_delete.return_value)
        convert_mock.assert_not_called()

    @mock.patch(
        "airflow.contrib.hooks.gcp_container_hook.GKEClusterHook.log")
    @mock.patch("airflow.contrib.hooks.gcp_container_hook.GKEClusterHook._dict_to_proto")
    @mock.patch(
        "airflow.contrib.hooks.gcp_container_hook.GKEClusterHook.wait_for_operation")
    def test_delete_cluster_not_found(self, wait_mock, convert_mock, log_mock):
        from google.api_core.exceptions import NotFound
        # To force an error
        message = 'Not Found'
        self.gke_hook._client.delete_cluster.side_effect = NotFound(message=message)

        self.gke_hook.delete_cluster('not-existing')
        wait_mock.assert_not_called()
        convert_mock.assert_not_called()
        log_mock.info.assert_any_call("Assuming Success: %s", message)

    @mock.patch("airflow.contrib.hooks.gcp_container_hook.GKEClusterHook._dict_to_proto")
    @mock.patch(
        "airflow.contrib.hooks.gcp_container_hook.GKEClusterHook.wait_for_operation")
    def test_delete_cluster_error(self, wait_mock, convert_mock):
        # To force an error
        self.gke_hook._client.delete_cluster.side_effect = AirflowException('400')

        with self.assertRaises(AirflowException):
            self.gke_hook.delete_cluster('a-cluster')
            wait_mock.assert_not_called()
            convert_mock.assert_not_called()


class GKEClusterHookCreateTest(unittest.TestCase):
    def setUp(self):
        self.gke_hook = GKEClusterHook(location=GKE_ZONE)
        self.gke_hook._client = mock.Mock()

    @mock.patch("airflow.contrib.hooks.gcp_container_hook.GKEClusterHook._dict_to_proto")
    @mock.patch(
        "airflow.contrib.hooks.gcp_container_hook.GKEClusterHook.wait_for_operation")
    def test_create_cluster_proto(self, wait_mock, convert_mock):
        from google.cloud.container_v1.proto.cluster_service_pb2 import Cluster

        mock_cluster_proto = Cluster()
        mock_cluster_proto.name = CLUSTER_NAME

        retry_mock, timeout_mock = mock.Mock(), mock.Mock()

        client_create = self.gke_hook._client.create_cluster = mock.Mock()

        self.gke_hook.create_cluster(mock_cluster_proto,
                                     project_id=TEST_GCP_PROJECT_ID,
                                     retry=retry_mock,
                                     timeout=timeout_mock)

        client_create.assert_called_with(project_id=TEST_GCP_PROJECT_ID,
                                         zone=GKE_ZONE,
                                         cluster=mock_cluster_proto,
                                         retry=retry_mock, timeout=timeout_mock)
        wait_mock.assert_called_with(client_create.return_value)
        convert_mock.assert_not_called()

    @mock.patch("airflow.contrib.hooks.gcp_container_hook.GKEClusterHook._dict_to_proto")
    @mock.patch(
        "airflow.contrib.hooks.gcp_container_hook.GKEClusterHook.wait_for_operation")
    def test_create_cluster_dict(self, wait_mock, convert_mock):
        mock_cluster_dict = {'name': CLUSTER_NAME}
        retry_mock, timeout_mock = mock.Mock(), mock.Mock()

        client_create = self.gke_hook._client.create_cluster = mock.Mock()
        proto_mock = convert_mock.return_value = mock.Mock()

        self.gke_hook.create_cluster(mock_cluster_dict,
                                     project_id=TEST_GCP_PROJECT_ID,
                                     retry=retry_mock,
                                     timeout=timeout_mock)

        client_create.assert_called_with(project_id=TEST_GCP_PROJECT_ID,
                                         zone=GKE_ZONE,
                                         cluster=proto_mock,
                                         retry=retry_mock, timeout=timeout_mock)
        wait_mock.assert_called_with(client_create.return_value)
        self.assertEqual(convert_mock.call_args[1]['py_dict'], mock_cluster_dict)

    @mock.patch("airflow.contrib.hooks.gcp_container_hook.GKEClusterHook._dict_to_proto")
    @mock.patch(
        "airflow.contrib.hooks.gcp_container_hook.GKEClusterHook.wait_for_operation")
    def test_create_cluster_error(self, wait_mock, convert_mock):
        # to force an error
        mock_cluster_proto = None

        with self.assertRaises(AirflowException):
            self.gke_hook.create_cluster(mock_cluster_proto)
            wait_mock.assert_not_called()
            convert_mock.assert_not_called()

    @mock.patch(
        "airflow.contrib.hooks.gcp_container_hook.GKEClusterHook.log")
    @mock.patch("airflow.contrib.hooks.gcp_container_hook.GKEClusterHook._dict_to_proto")
    @mock.patch(
        "airflow.contrib.hooks.gcp_container_hook.GKEClusterHook.wait_for_operation")
    def test_create_cluster_already_exists(self, wait_mock, convert_mock, log_mock):
        from google.api_core.exceptions import AlreadyExists
        # To force an error
        message = 'Already Exists'
        self.gke_hook._client.create_cluster.side_effect = AlreadyExists(message=message)

        self.gke_hook.create_cluster({})
        wait_mock.assert_not_called()
        self.assertEqual(convert_mock.call_count, 1)
        log_mock.info.assert_any_call("Assuming Success: %s", message)


class GKEClusterHookGetTest(unittest.TestCase):
    def setUp(self):
        self.gke_hook = GKEClusterHook(location=GKE_ZONE)
        self.gke_hook._client = mock.Mock()

    def test_get_cluster(self):
        retry_mock, timeout_mock = mock.Mock(), mock.Mock()

        client_get = self.gke_hook._client.get_cluster = mock.Mock()

        self.gke_hook.get_cluster(name=CLUSTER_NAME,
                                  project_id=TEST_GCP_PROJECT_ID,
                                  retry=retry_mock,
                                  timeout=timeout_mock)

        client_get.assert_called_with(project_id=TEST_GCP_PROJECT_ID,
                                      zone=GKE_ZONE,
                                      cluster_id=CLUSTER_NAME,
                                      retry=retry_mock, timeout=timeout_mock)


class GKEClusterHookTest(unittest.TestCase):

    def setUp(self):
        self.gke_hook = GKEClusterHook(location=GKE_ZONE)
        self.gke_hook._client = mock.Mock()

    @mock.patch('airflow.contrib.hooks.gcp_container_hook.container_v1.'
                'ClusterManagerClient')
    @mock.patch('airflow.contrib.hooks.gcp_api_base_hook.ClientInfo')
    @mock.patch('airflow.contrib.hooks.gcp_container_hook.GKEClusterHook._get_credentials')
    def test_get_client(self, mock_get_credentials, mock_client_info, mock_client):
        self.gke_hook._client = None
        self.gke_hook.get_client()
        assert mock_get_credentials.called
        mock_client.assert_called_with(
            credentials=mock_get_credentials.return_value,
            client_info=mock_client_info.return_value)

    def test_get_operation(self):
        self.gke_hook._client.get_operation = mock.Mock()
        self.gke_hook.get_operation('TEST_OP', project_id=TEST_GCP_PROJECT_ID)
        self.gke_hook._client.get_operation.assert_called_with(
            project_id=TEST_GCP_PROJECT_ID, zone=GKE_ZONE, operation_id='TEST_OP')

    def test_append_label(self):
        key = 'test-key'
        val = 'test-val'
        mock_proto = mock.Mock()
        self.gke_hook._append_label(mock_proto, key, val)
        mock_proto.resource_labels.update.assert_called_with({key: val})

    def test_append_label_replace(self):
        key = 'test-key'
        val = 'test.val+this'
        mock_proto = mock.Mock()
        self.gke_hook._append_label(mock_proto, key, val)
        mock_proto.resource_labels.update.assert_called_with({key: 'test-val-this'})

    @mock.patch("time.sleep")
    def test_wait_for_response_done(self, time_mock):
        from google.cloud.container_v1.gapic.enums import Operation
        mock_op = mock.Mock()
        mock_op.status = Operation.Status.DONE
        self.gke_hook.wait_for_operation(mock_op)
        self.assertEqual(time_mock.call_count, 1)

    @mock.patch("time.sleep")
    def test_wait_for_response_exception(self, time_mock):
        from google.cloud.container_v1.gapic.enums import Operation
        from google.cloud.exceptions import GoogleCloudError

        mock_op = mock.Mock()
        mock_op.status = Operation.Status.ABORTING

        with self.assertRaises(GoogleCloudError):
            self.gke_hook.wait_for_operation(mock_op)
            self.assertEqual(time_mock.call_count, 1)

    @mock.patch("airflow.contrib.hooks.gcp_container_hook.GKEClusterHook.get_operation")
    @mock.patch("time.sleep")
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

    @mock.patch("google.protobuf.json_format.Parse")
    @mock.patch("json.dumps")
    def test_dict_to_proto(self, dumps_mock, parse_mock):
        mock_dict = {'name': 'test'}
        mock_proto = mock.Mock()

        dumps_mock.return_value = mock.Mock()

        self.gke_hook._dict_to_proto(mock_dict, mock_proto)

        dumps_mock.assert_called_with(mock_dict)
        parse_mock.assert_called_with(dumps_mock(), mock_proto)
