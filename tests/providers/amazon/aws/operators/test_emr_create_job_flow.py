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

import os
import unittest
from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest
from jinja2 import StrictUndefined

from airflow.models import DAG, DagRun, TaskInstance
from airflow.models.xcom import XCOM_RETURN_KEY
from airflow.providers.amazon.aws.operators.emr import EmrClusterLink, EmrCreateJobFlowOperator
from airflow.serialization.serialized_objects import SerializedDAG
from airflow.utils import timezone
from tests.test_utils import AIRFLOW_MAIN_FOLDER

TASK_ID = 'test_task'

TEST_DAG_ID = 'test_dag_id'

DEFAULT_DATE = timezone.datetime(2017, 1, 1)

RUN_JOB_FLOW_SUCCESS_RETURN = {'ResponseMetadata': {'HTTPStatusCode': 200}, 'JobFlowId': 'j-8989898989'}

TEMPLATE_SEARCHPATH = os.path.join(
    AIRFLOW_MAIN_FOLDER, 'tests', 'providers', 'amazon', 'aws', 'config_templates'
)


class TestEmrCreateJobFlowOperator(unittest.TestCase):
    # When
    _config = {
        'Name': 'test_job_flow',
        'ReleaseLabel': '5.11.0',
        'Steps': [
            {
                'Name': 'test_step',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['/usr/lib/spark/bin/run-example', '{{ macros.ds_add(ds, -1) }}', '{{ ds }}'],
                },
            }
        ],
    }

    def setUp(self):
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}

        # Mock out the emr_client (moto has incorrect response)
        self.emr_client_mock = MagicMock()
        self.operator = EmrCreateJobFlowOperator(
            task_id=TASK_ID,
            aws_conn_id='aws_default',
            emr_conn_id='emr_default',
            region_name='ap-southeast-2',
            dag=DAG(
                TEST_DAG_ID,
                default_args=args,
                template_searchpath=TEMPLATE_SEARCHPATH,
                template_undefined=StrictUndefined,
            ),
        )

    def test_init(self):
        assert self.operator.aws_conn_id == 'aws_default'
        assert self.operator.emr_conn_id == 'emr_default'
        assert self.operator.region_name == 'ap-southeast-2'

    def test_render_template(self):
        self.operator.job_flow_overrides = self._config
        dag_run = DagRun(dag_id=self.operator.dag_id, execution_date=DEFAULT_DATE, run_id="test")
        ti = TaskInstance(task=self.operator)
        ti.dag_run = dag_run
        ti.render_templates()

        expected_args = {
            'Name': 'test_job_flow',
            'ReleaseLabel': '5.11.0',
            'Steps': [
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
            ],
        }

        assert self.operator.job_flow_overrides == expected_args

    def test_render_template_from_file(self):
        self.operator.job_flow_overrides = 'job.j2.json'
        self.operator.params = {'releaseLabel': '5.11.0'}

        dag_run = DagRun(dag_id=self.operator.dag_id, execution_date=DEFAULT_DATE, run_id="test")
        ti = TaskInstance(task=self.operator)
        ti.dag_run = dag_run
        ti.render_templates()

        self.emr_client_mock.run_job_flow.return_value = RUN_JOB_FLOW_SUCCESS_RETURN
        emr_session_mock = MagicMock()
        emr_session_mock.client.return_value = self.emr_client_mock
        boto3_session_mock = MagicMock(return_value=emr_session_mock)

        # String in job_flow_overrides (i.e. from loaded as a file) is not "parsed" until inside execute()
        with patch('boto3.session.Session', boto3_session_mock):
            self.operator.execute(None)

        expected_args = {
            'Name': 'test_job_flow',
            'ReleaseLabel': '5.11.0',
            'Steps': [
                {
                    'Name': 'test_step',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': [
                            '/usr/lib/spark/bin/run-example',
                            '2016-12-31',
                            '2017-01-01',
                        ],
                    },
                }
            ],
        }

        assert self.operator.job_flow_overrides == expected_args

    def test_execute_returns_job_id(self):
        self.emr_client_mock.run_job_flow.return_value = RUN_JOB_FLOW_SUCCESS_RETURN

        # Mock out the emr_client creator
        emr_session_mock = MagicMock()
        emr_session_mock.client.return_value = self.emr_client_mock
        boto3_session_mock = MagicMock(return_value=emr_session_mock)

        with patch('boto3.session.Session', boto3_session_mock):
            assert self.operator.execute(None) == 'j-8989898989'


@pytest.mark.need_serialized_dag
def test_operator_extra_links(dag_maker, create_task_instance_of_operator):
    ti = create_task_instance_of_operator(
        EmrCreateJobFlowOperator, dag_id=TEST_DAG_ID, execution_date=DEFAULT_DATE, task_id=TASK_ID
    )

    serialized_dag = dag_maker.get_serialized_data()
    deserialized_dag = SerializedDAG.from_dict(serialized_dag)
    deserialized_task = deserialized_dag.task_dict[TASK_ID]

    assert serialized_dag["dag"]["tasks"][0]["_operator_extra_links"] == [
        {"airflow.providers.amazon.aws.operators.emr.EmrClusterLink": {}}
    ], "Operator links should exist for serialized DAG"

    assert isinstance(
        deserialized_task.operator_extra_links[0], EmrClusterLink
    ), "Operator link type should be preserved during deserialization"

    assert (
        ti.task.get_extra_links(ti, EmrClusterLink.name) == ""
    ), "Operator link should only be added if job id is available in XCom"

    assert (
        deserialized_task.get_extra_links(ti, EmrClusterLink.name) == ""
    ), "Operator link should be empty for deserialized task with no XCom push"

    ti.xcom_push(key=XCOM_RETURN_KEY, value='j-SomeClusterId')

    expected = "https://console.aws.amazon.com/elasticmapreduce/home#cluster-details:j-SomeClusterId"
    assert (
        deserialized_task.get_extra_links(ti, EmrClusterLink.name) == expected
    ), "Operator link should be preserved in deserialized tasks after execution"

    assert (
        ti.task.get_extra_links(ti, EmrClusterLink.name) == expected
    ), "Operator link should be preserved after execution"
