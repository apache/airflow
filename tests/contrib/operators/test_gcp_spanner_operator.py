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

from parameterized import parameterized

from airflow import AirflowException
from airflow.contrib.operators.gcp_spanner_operator import \
    CloudSpannerInstanceDeployOperator, CloudSpannerInstanceDeleteOperator
from tests.contrib.operators.test_gcp_base import BaseGcpIntegrationTestCase, \
    SKIP_TEST_WARNING, GCP_SPANNER_KEY

try:
    # noinspection PyProtectedMember
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

PROJECT_ID = 'project-id'
INSTANCE_ID = 'instance-id'
DB_NAME = 'db1'
CONFIG_NAME = 'projects/project-id/instanceConfigs/eur3'
NODE_COUNT = '1'
DISPLAY_NAME = 'Test Instance'


class CloudSpannerTest(unittest.TestCase):
    @mock.patch("airflow.contrib.operators.gcp_spanner_operator.CloudSpannerHook")
    def test_instance_create(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = None
        op = CloudSpannerInstanceDeployOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            configuration_name=CONFIG_NAME,
            node_count=int(NODE_COUNT),
            display_name=DISPLAY_NAME,
            task_id="id"
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(gcp_conn_id="google_cloud_default")
        mock_hook.return_value.create_instance.assert_called_once_with(
            PROJECT_ID, INSTANCE_ID, CONFIG_NAME, int(NODE_COUNT), DISPLAY_NAME
        )
        mock_hook.return_value.update_instance.assert_not_called()
        self.assertTrue(result)

    @mock.patch("airflow.contrib.operators.gcp_spanner_operator.CloudSpannerHook")
    def test_instance_update(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = {"name": INSTANCE_ID}
        op = CloudSpannerInstanceDeployOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            configuration_name=CONFIG_NAME,
            node_count=int(NODE_COUNT),
            display_name=DISPLAY_NAME,
            task_id="id"
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(gcp_conn_id="google_cloud_default")
        mock_hook.return_value.update_instance.assert_called_once_with(
            PROJECT_ID, INSTANCE_ID, CONFIG_NAME, int(NODE_COUNT), DISPLAY_NAME
        )
        mock_hook.return_value.create_instance.assert_not_called()
        self.assertTrue(result)

    @mock.patch("airflow.contrib.operators.gcp_spanner_operator.CloudSpannerHook")
    def test_instance_create_aborts_and_succeeds_if_instance_exists(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = {"name": INSTANCE_ID}
        op = CloudSpannerInstanceDeployOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            configuration_name=CONFIG_NAME,
            node_count=int(NODE_COUNT),
            display_name=DISPLAY_NAME,
            task_id="id"
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(gcp_conn_id="google_cloud_default")
        mock_hook.return_value.create_instance.assert_not_called()
        self.assertTrue(result)

    @parameterized.expand([
        ("", INSTANCE_ID, "project_id"),
        (PROJECT_ID, "", "instance_id"),
    ])
    @mock.patch("airflow.contrib.operators.gcp_spanner_operator.CloudSpannerHook")
    def test_instance_create_ex_if_param_missing(self, project_id, instance_id,
                                                 exp_msg, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            CloudSpannerInstanceDeployOperator(
                project_id=project_id,
                instance_id=instance_id,
                configuration_name=CONFIG_NAME,
                node_count=int(NODE_COUNT),
                display_name=DISPLAY_NAME,
                task_id="id"
            )
        err = cm.exception
        self.assertIn("The required parameter '{}' is empty".format(exp_msg), str(err))
        mock_hook.assert_not_called()

    @mock.patch("airflow.contrib.operators.gcp_spanner_operator.CloudSpannerHook")
    def test_instance_delete(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = {"name": INSTANCE_ID}
        op = CloudSpannerInstanceDeleteOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            task_id="id"
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(gcp_conn_id="google_cloud_default")
        mock_hook.return_value.delete_instance.assert_called_once_with(
            PROJECT_ID, INSTANCE_ID
        )
        self.assertTrue(result)

    @mock.patch("airflow.contrib.operators.gcp_spanner_operator.CloudSpannerHook")
    def test_instance_delete_aborts_and_succeeds_if_instance_does_not_exist(self,
                                                                            mock_hook):
        mock_hook.return_value.get_instance.return_value = None
        op = CloudSpannerInstanceDeleteOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            task_id="id"
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(gcp_conn_id="google_cloud_default")
        mock_hook.return_value.delete_instance.assert_not_called()
        self.assertTrue(result)

    @parameterized.expand([
        ("", INSTANCE_ID, "project_id"),
        (PROJECT_ID, "", "instance_id"),
    ])
    @mock.patch("airflow.contrib.operators.gcp_spanner_operator.CloudSpannerHook")
    def test_instance_delete_ex_if_param_missing(self, project_id, instance_id, exp_msg,
                                                 mock_hook):
        with self.assertRaises(AirflowException) as cm:
            CloudSpannerInstanceDeleteOperator(
                project_id=project_id,
                instance_id=instance_id,
                task_id="id"
            )
        err = cm.exception
        self.assertIn("The required parameter '{}' is empty".format(exp_msg), str(err))
        mock_hook.assert_not_called()


@unittest.skipIf(
    BaseGcpIntegrationTestCase.skip_check(GCP_SPANNER_KEY), SKIP_TEST_WARNING)
class CloudSpannerExampleDagsTest(BaseGcpIntegrationTestCase):
    def __init__(self, method_name='runTest'):
        super(CloudSpannerExampleDagsTest, self).__init__(
            method_name,
            dag_id='example_gcp_spanner',
            gcp_key=GCP_SPANNER_KEY)

    def test_run_example_dag_cloudsql_query(self):
        self._run_dag()
