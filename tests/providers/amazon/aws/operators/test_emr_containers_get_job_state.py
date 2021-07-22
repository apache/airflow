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

import pytest
from botocore.exceptions import ClientError

from airflow.exceptions import AirflowException
from airflow.models import TaskInstance
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.emr_containers_get_job_state import (
    EmrContainersGetJobStateOperator,
)
from airflow.utils import timezone


DEFAULT_DATE = timezone.datetime(2017, 1, 1)
CLUSTER_ID = "foo_12321"
EXECUTION_ROLE_ARN = "arn:aws:iam:region:account-id:role/test"
MOCK_RESPONSE = {'jobRun': {'id': 'foo-123', 'virtualClusterId': CLUSTER_ID, 'state': 'PENDING'}}


class TestEmrContainersGetJobStateOperator(unittest.TestCase):
    def setUp(self):
        self.args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        self.execution_date = timezone.utcnow()

        # Mock out the emr containers client
        self.emr_container_client_mock = MagicMock()

        # Mock out the emr containers creator
        emr_session_mock = MagicMock()
        emr_session_mock.client.return_value = self.emr_container_client_mock
        self.boto3_session_mock = MagicMock(return_value=emr_session_mock)

        self.mock_context = MagicMock()

        self.operator = EmrContainersGetJobStateOperator(
            task_id="get_job_state",
            job_id=MOCK_RESPONSE['jobRun']['id'],
            cluster_id=CLUSTER_ID,
            dag=DAG('test_dag_emr_container_get_job_state', default_args=self.args),
        )

    def test_get_job_state_init(self):
        assert self.operator.cluster_id == CLUSTER_ID
        assert self.operator.job_id == MOCK_RESPONSE['jobRun']['id']

    def test_get_job_state_render_template(self):
        ti = TaskInstance(self.operator, DEFAULT_DATE)
        ti.render_templates()

    def test_emr_container_get_job_state(self):
        self.emr_container_client_mock.describe_job_run.return_value = MOCK_RESPONSE

        with patch('boto3.session.Session', self.boto3_session_mock):
            ti = TaskInstance(task=self.operator, execution_date=self.execution_date)
            ti.run()

        self.emr_container_client_mock.describe_job_run.assert_called_once_with(
            **{'id': MOCK_RESPONSE['jobRun']["id"], 'virtualClusterId': CLUSTER_ID}
        )

    def test_get_job_state_return_value(self):
        self.emr_container_client_mock.describe_job_run.return_value = MOCK_RESPONSE
        with patch('boto3.session.Session', self.boto3_session_mock):
            assert self.operator.execute(self.mock_context) == MOCK_RESPONSE['jobRun']['state']

    def test_get_job_state_with_non_existent_job_id(self):
        error_response = {"Error": {"Code": "ValidationException", "Message": "Job not found"}}
        self.emr_container_client_mock.describe_job_run.side_effect = ClientError(error_response, "test")

        with pytest.raises(AirflowException) as ctx:
            with patch('boto3.session.Session', self.boto3_session_mock):
                ti = TaskInstance(task=self.operator, execution_date=self.execution_date)
                ti.run()
        assert str(ctx.value) == error_response['Error']['Message']
