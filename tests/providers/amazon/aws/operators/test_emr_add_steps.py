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
from datetime import timedelta
from unittest.mock import MagicMock, call, patch

import pytest
from jinja2 import StrictUndefined

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.models import DAG, DagRun, TaskInstance
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.triggers.emr import EmrAddStepsTrigger
from airflow.utils import timezone
from tests.test_utils import AIRFLOW_MAIN_FOLDER

DEFAULT_DATE = timezone.datetime(2017, 1, 1)

ADD_STEPS_SUCCESS_RETURN = {"ResponseMetadata": {"HTTPStatusCode": 200}, "StepIds": ["s-2LH3R5GW3A53T"]}

TEMPLATE_SEARCHPATH = os.path.join(
    AIRFLOW_MAIN_FOLDER, "tests", "providers", "amazon", "aws", "config_templates"
)


@pytest.fixture
def mocked_hook_client():
    with patch("airflow.providers.amazon.aws.hooks.emr.EmrHook.conn") as m:
        yield m


class TestEmrAddStepsOperator:
    # When
    _config = [
        {
            "Name": "test_step",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": ["/usr/lib/spark/bin/run-example", "{{ macros.ds_add(ds, -1) }}", "{{ ds }}"],
            },
        }
    ]

    def setup_method(self):
        self.args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.operator = EmrAddStepsOperator(
            task_id="test_task",
            job_flow_id="j-8989898989",
            aws_conn_id="aws_default",
            steps=self._config,
            dag=DAG("test_dag_id", default_args=self.args),
        )

    def test_init(self):
        op = EmrAddStepsOperator(
            task_id="test_task",
            job_flow_id="j-8989898989",
            aws_conn_id="aws_default",
            steps=self._config,
        )
        assert op.job_flow_id == "j-8989898989"
        assert op.aws_conn_id == "aws_default"

    @pytest.mark.parametrize(
        "job_flow_id, job_flow_name",
        [
            pytest.param("j-8989898989", "test_cluster", id="both-specified"),
            pytest.param(None, None, id="both-none"),
        ],
    )
    def test_validate_mutually_exclusive_args(self, job_flow_id, job_flow_name):
        error_message = r"Exactly one of job_flow_id or job_flow_name must be specified\."
        with pytest.raises(AirflowException, match=error_message):
            EmrAddStepsOperator(
                task_id="test_validate_mutually_exclusive_args",
                job_flow_id=job_flow_id,
                job_flow_name=job_flow_name,
            )

    @pytest.mark.db_test
    def test_render_template(self):
        dag_run = DagRun(dag_id=self.operator.dag.dag_id, execution_date=DEFAULT_DATE, run_id="test")
        ti = TaskInstance(task=self.operator)
        ti.dag_run = dag_run
        ti.render_templates()

        expected_args = [
            {
                "Name": "test_step",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "/usr/lib/spark/bin/run-example",
                        (DEFAULT_DATE - timedelta(days=1)).strftime("%Y-%m-%d"),
                        DEFAULT_DATE.strftime("%Y-%m-%d"),
                    ],
                },
            }
        ]

        assert self.operator.steps == expected_args

    @pytest.mark.db_test
    def test_render_template_from_file(self, mocked_hook_client):
        dag = DAG(
            dag_id="test_file",
            default_args=self.args,
            template_searchpath=TEMPLATE_SEARCHPATH,
            template_undefined=StrictUndefined,
        )

        file_steps = [
            {
                "Name": "test_step1",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {"Jar": "command-runner.jar", "Args": ["/usr/lib/spark/bin/run-example1"]},
            }
        ]

        mocked_hook_client.add_job_flow_steps.return_value = ADD_STEPS_SUCCESS_RETURN

        test_task = EmrAddStepsOperator(
            task_id="test_task",
            job_flow_id="j-8989898989",
            aws_conn_id="aws_default",
            steps="steps.j2.json",
            dag=dag,
            do_xcom_push=False,
        )
        dag_run = DagRun(dag_id=dag.dag_id, execution_date=timezone.utcnow(), run_id="test")
        ti = TaskInstance(task=test_task)
        ti.dag_run = dag_run
        ti.render_templates()

        assert json.loads(test_task.steps) == file_steps

        # String in job_flow_overrides (i.e. from loaded as a file) is not "parsed" until inside execute()
        test_task.execute(MagicMock())

        mocked_hook_client.add_job_flow_steps.assert_called_once_with(
            JobFlowId="j-8989898989", Steps=file_steps
        )

    def test_execute_returns_step_id(self, mocked_hook_client):
        mocked_hook_client.add_job_flow_steps.return_value = ADD_STEPS_SUCCESS_RETURN

        assert self.operator.execute(MagicMock()) == ["s-2LH3R5GW3A53T"]

    def test_init_with_cluster_name(self, mocked_hook_client):
        mocked_hook_client.add_job_flow_steps.return_value = ADD_STEPS_SUCCESS_RETURN
        mock_context = MagicMock()
        expected_job_flow_id = "j-1231231234"

        operator = EmrAddStepsOperator(
            task_id="test_task",
            job_flow_name="test_cluster",
            cluster_states=["RUNNING", "WAITING"],
            aws_conn_id="aws_default",
            dag=DAG("test_dag_id", default_args=self.args),
        )

        with patch(
            "airflow.providers.amazon.aws.hooks.emr.EmrHook.get_cluster_id_by_name",
            return_value=expected_job_flow_id,
        ):
            operator.execute(mock_context)

        mocked_ti = mock_context["ti"]
        mocked_ti.assert_has_calls(calls=[call.xcom_push(key="job_flow_id", value=expected_job_flow_id)])

    def test_init_with_nonexistent_cluster_name(self):
        cluster_name = "test_cluster"
        operator = EmrAddStepsOperator(
            task_id="test_task",
            job_flow_name=cluster_name,
            cluster_states=["RUNNING", "WAITING"],
            aws_conn_id="aws_default",
            dag=DAG("test_dag_id", default_args=self.args),
        )

        with patch(
            "airflow.providers.amazon.aws.hooks.emr.EmrHook.get_cluster_id_by_name", return_value=None
        ):
            error_match = rf"No cluster found for name: {cluster_name}"
            with pytest.raises(AirflowException, match=error_match):
                operator.execute(MagicMock())

    def test_wait_for_completion(self, mocked_hook_client):
        job_flow_id = "j-8989898989"
        operator = EmrAddStepsOperator(
            task_id="test_task",
            job_flow_id=job_flow_id,
            aws_conn_id="aws_default",
            dag=DAG("test_dag_id", default_args=self.args),
            wait_for_completion=False,
        )

        with patch(
            "airflow.providers.amazon.aws.hooks.emr.EmrHook.add_job_flow_steps"
        ) as mock_add_job_flow_steps:
            operator.execute(MagicMock())

        mock_add_job_flow_steps.assert_called_once_with(
            job_flow_id=job_flow_id,
            steps=[],
            wait_for_completion=False,
            waiter_delay=30,
            waiter_max_attempts=60,
            execution_role_arn=None,
        )

    def test_wait_for_completion_false_with_deferrable(self):
        job_flow_id = "j-8989898989"
        operator = EmrAddStepsOperator(
            task_id="test_task",
            job_flow_id=job_flow_id,
            aws_conn_id="aws_default",
            dag=DAG("test_dag_id", default_args=self.args),
            wait_for_completion=True,
            deferrable=True,
        )

        assert operator.wait_for_completion is False

    @patch("airflow.providers.amazon.aws.operators.emr.get_log_uri")
    @patch("airflow.providers.amazon.aws.hooks.emr.EmrHook.add_job_flow_steps")
    def test_emr_add_steps_deferrable(self, mock_add_job_flow_steps, mock_get_log_uri):
        mock_add_job_flow_steps.return_value = "test_step_id"
        mock_get_log_uri.return_value = "test/log/uri"
        job_flow_id = "j-8989898989"
        operator = EmrAddStepsOperator(
            task_id="test_task",
            job_flow_id=job_flow_id,
            aws_conn_id="aws_default",
            dag=DAG("test_dag_id", default_args=self.args),
            wait_for_completion=True,
            deferrable=True,
        )

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(MagicMock())

        assert isinstance(exc.value.trigger, EmrAddStepsTrigger), "Trigger is not a EmrAddStepsTrigger"
