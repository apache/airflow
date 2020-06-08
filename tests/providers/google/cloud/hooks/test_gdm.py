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

from airflow.providers.google.cloud.hooks.gdm import GoogleDeploymentManagerHook


def mock_init(self, gcp_conn_id, delegate_to=None):  # pylint: disable=unused-argument
    pass


TEST_PROJECT = 'my-project'
TEST_DEPLOYMENT = 'my-deployment'
FILTER = 'staging-'
MAX_RESULTS = 10
ORDER_BY = 'name'
TEST_LIST_RESPONSE = {'deployments': [{'id': '38763452', 'name': 'test-deploy'}]}


class TestDeploymentManagerHook(unittest.TestCase):

    def setUp(self):
        with mock.patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_init,
        ):
            self.gdm_hook = GoogleDeploymentManagerHook(gcp_conn_id="test")

    @mock.patch("airflow.providers.google.cloud.hooks.gdm.GoogleDeploymentManagerHook.get_conn")
    def test_list_deployments(self, mock_get_conn):
        mock_get_conn.return_value.deployments().list.return_value.execute.return_value = TEST_LIST_RESPONSE
        deployments = self.gdm_hook.list_deployments(project_id=TEST_PROJECT)
        mock_get_conn.assert_called_once_with()
        mock_get_conn.return_value.deployments().list.assert_called_once_with(
            project=TEST_PROJECT,
            filter=None,
            maxResults=None,
            orderBy=None,
            pageToken=None,
        )
        self.assertEqual(deployments, [{'id': '38763452', 'name': 'test-deploy'}])

    @mock.patch("airflow.providers.google.cloud.hooks.gdm.GoogleDeploymentManagerHook.get_conn")
    def test_list_deployments_with_params(self, mock_get_conn):
        mock_get_conn.return_value.deployments().list.return_value.execute.return_value = TEST_LIST_RESPONSE
        deployments = self.gdm_hook.list_deployments(project_id=TEST_PROJECT, deployment_filter=FILTER,
                                                     max_results=MAX_RESULTS, order_by=ORDER_BY)
        mock_get_conn.assert_called_once_with()
        mock_get_conn.return_value.deployments().list.assert_called_once_with(
            project=TEST_PROJECT,
            filter=FILTER,
            maxResults=MAX_RESULTS,
            orderBy=ORDER_BY,
            pageToken=None,
        )
        self.assertEqual(deployments, [{'id': '38763452', 'name': 'test-deploy'}])

    @mock.patch("airflow.providers.google.cloud.hooks.gdm.GoogleDeploymentManagerHook.get_conn")
    def test_delete_deployment(self, mock_get_conn):
        self.gdm_hook.delete_deployment(project_id=TEST_PROJECT, deployment=TEST_DEPLOYMENT)
        mock_get_conn.assert_called_once_with()
        mock_get_conn.return_value.deployments().delete.assert_called_once_with(
            project=TEST_PROJECT,
            deployment=TEST_DEPLOYMENT,
            deletePolicy="DELETE"
        )
