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
from unittest import mock

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.gdm import GoogleDeploymentManagerHook


def mock_init(self, gcp_conn_id, delegate_to=None):  # pylint: disable=unused-argument
    pass


TEST_PROJECT = 'my-project'
TEST_DEPLOYMENT = 'my-deployment'


class TestDeploymentManagerHook(unittest.TestCase):

    def setUp(self):
        with mock.patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_init,
        ):
            self.gdm_hook = GoogleDeploymentManagerHook(gcp_conn_id="test")

    @mock.patch("airflow.providers.google.cloud.hooks.gdm.GoogleDeploymentManagerHook.get_conn")
    def test_list_deployments(self, mock_get_conn):

        response1 = {'deployments': [{'id': 'deployment1', 'name': 'test-deploy1'}], 'pageToken': None}
        response2 = {'deployments': [{'id': 'deployment2', 'name': 'test-deploy2'}], 'pageToken': None}

        mock_get_conn.return_value.deployments.return_value.list.return_value.execute.return_value = response1

        request_mock = mock.MagicMock()
        request_mock.execute.return_value = response2
        mock_get_conn.return_value.deployments.return_value.list_next.side_effect = [
            request_mock,
            None,
        ]

        deployments = self.gdm_hook.list_deployments(project_id=TEST_PROJECT,
                                                     deployment_filter='filter',
                                                     order_by='name')

        mock_get_conn.assert_called_once_with()

        mock_get_conn.return_value.deployments.return_value.list.assert_called_once_with(
            project=TEST_PROJECT,
            filter='filter',
            orderBy='name',
        )

        self.assertEqual(mock_get_conn.return_value.deployments.return_value.list_next.call_count, 2)

        self.assertEqual(deployments, [{'id': 'deployment1', 'name': 'test-deploy1'},
                                       {'id': 'deployment2', 'name': 'test-deploy2'}])

    @mock.patch("airflow.providers.google.cloud.hooks.gdm.GoogleDeploymentManagerHook.get_conn")
    def test_delete_deployment(self, mock_get_conn):
        self.gdm_hook.delete_deployment(project_id=TEST_PROJECT, deployment=TEST_DEPLOYMENT)
        mock_get_conn.assert_called_once_with()
        mock_get_conn.return_value.deployments().delete.assert_called_once_with(
            project=TEST_PROJECT,
            deployment=TEST_DEPLOYMENT,
            deletePolicy=None
        )

    @mock.patch("airflow.providers.google.cloud.hooks.gdm.GoogleDeploymentManagerHook.get_conn")
    def test_delete_deployment_delete_fails(self, mock_get_conn):

        resp = {'error': {'errors': [{'message': 'error deleting things.', 'domain': 'global'}]}}

        mock_get_conn.return_value.deployments.return_value.delete.return_value.execute.return_value = resp

        with self.assertRaises(AirflowException):
            self.gdm_hook.delete_deployment(project_id=TEST_PROJECT, deployment=TEST_DEPLOYMENT)

        mock_get_conn.assert_called_once_with()
        mock_get_conn.return_value.deployments().delete.assert_called_once_with(
            project=TEST_PROJECT,
            deployment=TEST_DEPLOYMENT,
            deletePolicy=None
        )
