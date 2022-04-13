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
from unittest.mock import MagicMock, patch

import pytest

from airflow.exceptions import AirflowException
from airflow.models import DAG
from airflow.providers.amazon.aws.operators.emr import EmrChangePolicyOperator
from airflow.utils import timezone
from tests.test_utils import AIRFLOW_MAIN_FOLDER

DEFAULT_DATE = timezone.datetime(2017, 1, 1)

PUT_AUTO_TERMINATION_POLICY_SUCCESS_RETURN = {'ResponseMetadata': {'HTTPStatusCode': 200}}

PUT_AUTO_TERMINATION_POLICY_ERROR_RETURN = {'ResponseMetadata': {'HTTPStatusCode': 400}}

TEMPLATE_SEARCHPATH = os.path.join(
    AIRFLOW_MAIN_FOLDER, 'tests', 'providers', 'amazon', 'aws', 'config_templates'
)


class TestEmrChangePolicyOperator(unittest.TestCase):

    def setUp(self):
        self.args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}

        # Mock out the emr_client (moto has incorrect response)
        self.emr_client_mock = MagicMock()

        # Mock out the emr_client creator
        emr_session_mock = MagicMock()
        emr_session_mock.client.return_value = self.emr_client_mock
        self.boto3_session_mock = MagicMock(return_value=emr_session_mock)

        self.mock_context = MagicMock()

        self.operator = EmrChangePolicyOperator(
            idle_timeout=600,
            task_id='test_task',
            job_flow_id='j-8989898989',
            aws_conn_id='aws_default',
            dag=DAG('test_dag_id', default_args=self.args),
        )

    def test_init(self):
        assert self.operator.job_flow_id == 'j-8989898989'
        assert self.operator.aws_conn_id == 'aws_default'
        assert self.operator.idle_timeout == 600

    def test_init_fails_with_no_job_flow_arguments(self):
        with pytest.raises(AirflowException):
            EmrChangePolicyOperator(
                idle_timeout=600,
                task_id='test_task',
                dag=DAG('test_dag_id', default_args=self.args),
            )

    def test_render_template_from_file(self):
        dag = DAG(
            dag_id='test',
            default_args=self.args
        )

        self.emr_client_mock.put_auto_termination_policy.return_value = \
            PUT_AUTO_TERMINATION_POLICY_SUCCESS_RETURN

        test_task = EmrChangePolicyOperator(
            task_id='test_task',
            job_flow_id='j-8989898989',
            aws_conn_id='aws_default',
            idle_timeout=600,
            dag=dag,
            do_xcom_push=False,
        )

        with patch('boto3.session.Session', self.boto3_session_mock):
            test_task.execute(None)

        self.emr_client_mock.put_auto_termination_policy.assert_called_once_with(
            ClusterId='j-8989898989',
            AutoTerminationPolicy={
                'IdleTimeout': 600
            }
        )

    def test_init_with_cluster_name(self):
        expected_job_flow_id = 'j-1231231234'

        self.emr_client_mock.add_job_flow_steps.return_value = PUT_AUTO_TERMINATION_POLICY_SUCCESS_RETURN

        with patch('boto3.session.Session', self.boto3_session_mock):
            with patch(
                'airflow.providers.amazon.aws.hooks.emr.EmrHook.get_cluster_id_by_name'
            ) as mock_get_cluster_id_by_name:
                mock_get_cluster_id_by_name.return_value = expected_job_flow_id

                operator = EmrChangePolicyOperator(
                    idle_timeout=600,
                    task_id='test_task',
                    job_flow_name='test_cluster',
                    cluster_states=['RUNNING', 'WAITING'],
                    aws_conn_id='aws_default',
                    dag=DAG('test_dag_id', default_args=self.args),
                )
                with pytest.raises(AirflowException):
                    operator.execute(self.mock_context)
                    ti = self.mock_context['ti']
                    ti.xcom_push.assert_called_once_with(key='job_flow_id', value=expected_job_flow_id)

    def test_init_with_non_existent_cluster_name(self):
        cluster_name = 'test_cluster'

        with patch(
            'airflow.providers.amazon.aws.hooks.emr.EmrHook.get_cluster_id_by_name'
        ) as mock_get_cluster_id_by_name:
            mock_get_cluster_id_by_name.return_value = None

            operator = EmrChangePolicyOperator(
                idle_timeout=600,
                task_id='test_task',
                job_flow_name=cluster_name,
                cluster_states=['RUNNING', 'WAITING'],
                aws_conn_id='aws_default',
                dag=DAG('test_dag_id', default_args=self.args),
            )

            with pytest.raises(AirflowException) as ctx:
                operator.execute(self.mock_context)
            assert str(ctx.value) == f'No cluster found for name: {cluster_name}'

    def test_execute_returns_error(self):
        self.emr_client_mock.put_auto_termination_policy.return_value = \
            PUT_AUTO_TERMINATION_POLICY_ERROR_RETURN

        with patch('boto3.session.Session', self.boto3_session_mock):
            with pytest.raises(AirflowException):
                self.operator.execute(self.mock_context)
