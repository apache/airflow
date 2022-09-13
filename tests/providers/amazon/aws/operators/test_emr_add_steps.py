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
from __future__ import annotations

import json
import os
import unittest
from datetime import timedelta
from unittest.mock import MagicMock, call, patch

import pytest
from jinja2 import StrictUndefined

from airflow.exceptions import AirflowException
from airflow.models import DAG, DagRun, TaskInstance
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.utils import timezone
from tests.test_utils import AIRFLOW_MAIN_FOLDER

DEFAULT_DATE = timezone.datetime(2017, 1, 1)

ADD_STEPS_SUCCESS_RETURN = {'ResponseMetadata': {'HTTPStatusCode': 200}, 'StepIds': ['s-2LH3R5GW3A53T']}

TEMPLATE_SEARCHPATH = os.path.join(
    AIRFLOW_MAIN_FOLDER, 'tests', 'providers', 'amazon', 'aws', 'config_templates'
)


class TestEmrAddStepsOperator(unittest.TestCase):
    # When
    _config = [
        {
            'Name': 'test_step',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['/usr/lib/spark/bin/run-example', '{{ macros.ds_add(ds, -1) }}', '{{ ds }}'],
            },
        }
    ]

    def setUp(self):
        self.args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}

        # Mock out the emr_client (moto has incorrect response)
        self.emr_client_mock = MagicMock()

        # Mock out the emr_client creator
        emr_session_mock = MagicMock()
        emr_session_mock.client.return_value = self.emr_client_mock
        self.boto3_session_mock = MagicMock(return_value=emr_session_mock)

        self.mock_context = MagicMock()

        self.operator = EmrAddStepsOperator(
            task_id='test_task',
            job_flow_id='j-8989898989',
            aws_conn_id='aws_default',
            steps=self._config,
            dag=DAG('test_dag_id', default_args=self.args),
        )

    def test_init(self):
        assert self.operator.job_flow_id == 'j-8989898989'
        assert self.operator.aws_conn_id == 'aws_default'

    def test_render_template(self):
        dag_run = DagRun(dag_id=self.operator.dag.dag_id, execution_date=DEFAULT_DATE, run_id="test")
        ti = TaskInstance(task=self.operator)
        ti.dag_run = dag_run
        ti.render_templates()

        expected_args = [
            {
                'Name': 'test_step',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        '/usr/lib/spark/bin/run-example',
                        (DEFAULT_DATE - timedelta(days=1)).strftime("%Y-%m-%d"),
                        DEFAULT_DATE.strftime("%Y-%m-%d"),
                    ],
                },
            }
        ]

        assert self.operator.steps == expected_args

    def test_render_template_from_file(self):
        dag = DAG(
            dag_id='test_file',
            default_args=self.args,
            template_searchpath=TEMPLATE_SEARCHPATH,
            template_undefined=StrictUndefined,
        )

        file_steps = [
            {
                'Name': 'test_step1',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {'Jar': 'command-runner.jar', 'Args': ['/usr/lib/spark/bin/run-example1']},
            }
        ]

        self.emr_client_mock.add_job_flow_steps.return_value = ADD_STEPS_SUCCESS_RETURN

        test_task = EmrAddStepsOperator(
            task_id='test_task',
            job_flow_id='j-8989898989',
            aws_conn_id='aws_default',
            steps='steps.j2.json',
            dag=dag,
            do_xcom_push=False,
        )
        dag_run = DagRun(dag_id=dag.dag_id, execution_date=timezone.utcnow(), run_id="test")
        ti = TaskInstance(task=test_task)
        ti.dag_run = dag_run
        ti.render_templates()

        assert json.loads(test_task.steps) == file_steps

        # String in job_flow_overrides (i.e. from loaded as a file) is not "parsed" until inside execute()
        with patch('boto3.session.Session', self.boto3_session_mock):
            test_task.execute(None)

        self.emr_client_mock.add_job_flow_steps.assert_called_once_with(
            JobFlowId='j-8989898989', Steps=file_steps
        )

    def test_execute_returns_step_id(self):
        self.emr_client_mock.add_job_flow_steps.return_value = ADD_STEPS_SUCCESS_RETURN

        with patch('boto3.session.Session', self.boto3_session_mock):
            assert self.operator.execute(self.mock_context) == ['s-2LH3R5GW3A53T']

    def test_init_with_cluster_name(self):
        expected_job_flow_id = 'j-1231231234'

        self.emr_client_mock.add_job_flow_steps.return_value = ADD_STEPS_SUCCESS_RETURN

        with patch('boto3.session.Session', self.boto3_session_mock):
            with patch(
                'airflow.providers.amazon.aws.hooks.emr.EmrHook.get_cluster_id_by_name'
            ) as mock_get_cluster_id_by_name:
                mock_get_cluster_id_by_name.return_value = expected_job_flow_id

                operator = EmrAddStepsOperator(
                    task_id='test_task',
                    job_flow_name='test_cluster',
                    cluster_states=['RUNNING', 'WAITING'],
                    aws_conn_id='aws_default',
                    dag=DAG('test_dag_id', default_args=self.args),
                )

                operator.execute(self.mock_context)

        ti = self.mock_context['ti']
        ti.assert_has_calls(calls=[call.xcom_push(key='job_flow_id', value=expected_job_flow_id)])

    def test_init_with_nonexistent_cluster_name(self):
        cluster_name = 'test_cluster'

        with patch(
            'airflow.providers.amazon.aws.hooks.emr.EmrHook.get_cluster_id_by_name'
        ) as mock_get_cluster_id_by_name:
            mock_get_cluster_id_by_name.return_value = None

            operator = EmrAddStepsOperator(
                task_id='test_task',
                job_flow_name=cluster_name,
                cluster_states=['RUNNING', 'WAITING'],
                aws_conn_id='aws_default',
                dag=DAG('test_dag_id', default_args=self.args),
            )

            with pytest.raises(AirflowException) as ctx:
                operator.execute(self.mock_context)
            assert str(ctx.value) == f'No cluster found for name: {cluster_name}'
