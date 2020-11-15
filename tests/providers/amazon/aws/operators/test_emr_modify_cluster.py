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
from unittest.mock import MagicMock, patch

from airflow.exceptions import AirflowException
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.emr_modify_cluster import EmrModifyClusterOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2017, 1, 1)

MODIFY_CLUSTER_SUCCESS_RETURN = {'ResponseMetadata': {'HTTPStatusCode': 200}, 'StepConcurrencyLevel': 1}

MODIFY_CLUSTER_ERROR_RETURN = {'ResponseMetadata': {'HTTPStatusCode': 400}}


class TestEmrModifyClusterOperator(unittest.TestCase):
    def setUp(self):
        self.args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}

        # Mock out the emr_client (moto has incorrect response)
        self.emr_client_mock = MagicMock()

        # Mock out the emr_client creator
        emr_session_mock = MagicMock()
        emr_session_mock.client.return_value = self.emr_client_mock
        self.boto3_session_mock = MagicMock(return_value=emr_session_mock)

        self.mock_context = MagicMock()

        self.operator = EmrModifyClusterOperator(
            task_id='test_task',
            cluster_id='j-8989898989',
            step_concurrency_level=1,
            aws_conn_id='aws_default',
            dag=DAG('test_dag_id', default_args=self.args),
        )

    def test_init(self):
        self.assertEqual(self.operator.cluster_id, 'j-8989898989')
        self.assertEqual(self.operator.step_concurrency_level, 1)
        self.assertEqual(self.operator.aws_conn_id, 'aws_default')

    def test_execute_returns_step_concurrency(self):
        self.emr_client_mock.modify_cluster.return_value = MODIFY_CLUSTER_SUCCESS_RETURN

        with patch('boto3.session.Session', self.boto3_session_mock):
            self.assertEqual(self.operator.execute(self.mock_context), 1)

    def test_execute_returns_error(self):
        self.emr_client_mock.modify_cluster.return_value = MODIFY_CLUSTER_ERROR_RETURN

        with patch('boto3.session.Session', self.boto3_session_mock):
            self.assertRaises(AirflowException, self.operator.execute, self.mock_context)
