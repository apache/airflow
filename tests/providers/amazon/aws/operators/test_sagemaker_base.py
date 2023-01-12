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

from typing import Any
from unittest import mock
from unittest.mock import patch

from airflow import DAG
from airflow.models import DagRun, TaskInstance
from airflow.providers.amazon.aws.operators.sagemaker import (
    SageMakerBaseOperator,
    SageMakerCreateExperimentOperator,
)
from airflow.utils import timezone

CONFIG: dict = {"key1": "1", "key2": {"key3": "3", "key4": "4"}, "key5": [{"key6": "6"}, {"key6": "7"}]}
PARSED_CONFIG: dict = {"key1": 1, "key2": {"key3": 3, "key4": 4}, "key5": [{"key6": 6}, {"key6": 7}]}

EXPECTED_INTEGER_FIELDS: list[list[Any]] = []


class TestSageMakerBaseOperator:
    def setup_method(self):
        self.sagemaker = SageMakerBaseOperator(task_id="test_sagemaker_operator", config=CONFIG)
        self.sagemaker.aws_conn_id = "aws_default"

    def test_parse_integer(self):
        self.sagemaker.integer_fields = [["key1"], ["key2", "key3"], ["key2", "key4"], ["key5", "key6"]]
        self.sagemaker.parse_config_integers()
        assert self.sagemaker.config == PARSED_CONFIG

    def test_default_integer_fields(self):
        self.sagemaker.preprocess_config()

        assert self.sagemaker.integer_fields == EXPECTED_INTEGER_FIELDS


class TestSageMakerExperimentOperator:
    @patch("airflow.providers.amazon.aws.hooks.sagemaker.SageMakerHook.conn", new_callable=mock.PropertyMock)
    def test_create_experiment(self, conn_mock):
        conn_mock().create_experiment.return_value = {"ExperimentArn": "abcdef"}

        # putting a DAG around the operator so that jinja template gets rendered
        execution_date = timezone.datetime(2020, 1, 1)
        dag = DAG("test_experiment", start_date=execution_date)
        op = SageMakerCreateExperimentOperator(
            name="the name",
            description="the desc",
            tags={"jinja": "{{ task.task_id }}"},
            task_id="tid",
            dag=dag,
        )
        dag_run = DagRun(dag_id=dag.dag_id, execution_date=execution_date, run_id="test")
        ti = TaskInstance(task=op)
        ti.dag_run = dag_run
        context = ti.get_template_context()
        ti.render_templates(context)

        ret = op.execute(None)

        assert ret == "abcdef"
        conn_mock().create_experiment.assert_called_once_with(
            ExperimentName="the name", Description="the desc", Tags=[{"Key": "jinja", "Value": "tid"}]
        )
