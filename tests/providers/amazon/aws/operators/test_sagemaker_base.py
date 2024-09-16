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

import re
from typing import Any
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from airflow.exceptions import AirflowException
from airflow.models import DAG, DagRun, TaskInstance
from airflow.providers.amazon.aws.operators.sagemaker import (
    SageMakerBaseOperator,
    SageMakerCreateExperimentOperator,
)
from airflow.utils import timezone
from airflow.utils.types import DagRunType
from tests.providers.amazon.aws.utils.test_template_fields import validate_template_fields

CONFIG: dict = {
    "key1": "1",
    "key2": {"key3": "3", "key4": "4"},
    "key5": [{"key6": "6"}, {"key6": "7"}],
}
PARSED_CONFIG: dict = {
    "key1": 1,
    "key2": {"key3": 3, "key4": 4},
    "key5": [{"key6": 6}, {"key6": 7}],
}

EXPECTED_INTEGER_FIELDS: list[list[Any]] = []


class TestSageMakerBaseOperator:
    ERROR_WHEN_RESOURCE_NOT_FOUND = ClientError({"Error": {"Code": "ValidationException"}}, "op")

    def setup_method(self):
        self.sagemaker = SageMakerBaseOperator(task_id="test_sagemaker_operator", config=CONFIG)
        self.sagemaker.aws_conn_id = "aws_default"

    def test_parse_integer(self):
        self.sagemaker.integer_fields = [
            ["key1"],
            ["key2", "key3"],
            ["key2", "key4"],
            ["key5", "key6"],
        ]
        self.sagemaker.parse_config_integers()
        assert self.sagemaker.config == PARSED_CONFIG

    def test_default_integer_fields(self):
        self.sagemaker.preprocess_config()
        assert self.sagemaker.integer_fields == EXPECTED_INTEGER_FIELDS

    def test_job_exists(self):
        exists = self.sagemaker._check_if_job_exists("the name", lambda _: {})
        assert exists

    def test_job_does_not_exists(self):
        def raiser(_):
            raise self.ERROR_WHEN_RESOURCE_NOT_FOUND

        exists = self.sagemaker._check_if_job_exists("the name", raiser)
        assert not exists

    def test_job_renamed(self):
        describe_mock = MagicMock()
        # scenario : name exists, new proposed name exists as well, second proposal is ok
        describe_mock.side_effect = [None, None, self.ERROR_WHEN_RESOURCE_NOT_FOUND]

        name = self.sagemaker._get_unique_job_name("test", False, describe_mock)

        assert describe_mock.call_count == 3
        assert re.match("test-[0-9]+$", name)

    def test_job_not_unique_with_fail(self):
        with pytest.raises(AirflowException):
            self.sagemaker._get_unique_job_name("test", True, lambda _: None)

    def test_check_resource_type_raises_exception_when_resource_type_is_invalid(self):
        with pytest.raises(AirflowException) as context:
            self.sagemaker._check_resource_type("invalid_resource")

        assert str(context.value) == (
            "Argument resource_type accepts only 'model' and 'job'. Provided value: 'invalid_resource'."
        )

    def test_get_unique_name_raises_exception_if_name_exists_when_fail_is_true(self):
        with pytest.raises(AirflowException) as context:
            self.sagemaker._get_unique_name(
                "existing_name",
                fail_if_exists=True,
                describe_func=None,
                check_exists_func=lambda name, describe_func: True,
                resource_type="model",
            )

        assert str(context.value) == "A SageMaker model with name existing_name already exists."

    @patch("airflow.providers.amazon.aws.operators.sagemaker.time.time_ns", return_value=3000000)
    def test_get_unique_name_avoids_name_collision(self, time_mock):
        new_name = self.sagemaker._get_unique_name(
            "existing_name",
            fail_if_exists=False,
            describe_func=None,
            check_exists_func=MagicMock(side_effect=[True, False]),
            resource_type="model",
        )

        assert new_name == "existing_name-3"

    def test_get_unique_name_checks_only_once_when_resource_does_not_exist(self):
        describe_func = MagicMock(side_effect=ClientError({"Error": {"Code": "ValidationException"}}, "op"))
        new_name = "new_name"

        name = self.sagemaker._get_unique_name(
            new_name,
            fail_if_exists=False,
            describe_func=describe_func,
            check_exists_func=self.sagemaker._check_if_job_exists,
            resource_type="job",
        )

        describe_func.assert_called_once_with(new_name)
        assert name == new_name

    def test_check_if_resource_exists_returns_true_when_it_finds_existing_resource(self):
        exists = self.sagemaker._check_if_resource_exists("job_123", "job", lambda name: None)
        assert exists

    def test_check_if_resource_exists_returns_false_when_validation_exception_is_raised(self):
        describe_func = MagicMock(side_effect=ClientError({"Error": {"Code": "ValidationException"}}, "op"))
        exists = self.sagemaker._check_if_resource_exists("job_123", "job", describe_func)
        assert not exists

    def test_check_if_resource_exists_raises_when_it_is_not_validation_exception(self):
        describe_func = MagicMock(side_effect=ValueError("different exception"))

        with pytest.raises(ValueError) as context:
            self.sagemaker._check_if_resource_exists("job_123", "job", describe_func)

        assert str(context.value) == "different exception"


@pytest.mark.db_test
class TestSageMakerExperimentOperator:
    @patch(
        "airflow.providers.amazon.aws.hooks.sagemaker.SageMakerHook.conn",
        new_callable=mock.PropertyMock,
    )
    def test_create_experiment(self, conn_mock, session, clean_dags_and_dagruns):
        conn_mock().create_experiment.return_value = {"ExperimentArn": "abcdef"}

        # putting a DAG around the operator so that jinja template gets rendered
        execution_date = timezone.datetime(2020, 1, 1)
        dag = DAG("test_experiment", schedule=None, start_date=execution_date)
        op = SageMakerCreateExperimentOperator(
            name="the name",
            description="the desc",
            tags={"jinja": "{{ task.task_id }}"},
            task_id="tid",
            dag=dag,
        )
        dag_run = DagRun(
            dag_id=dag.dag_id, execution_date=execution_date, run_id="test", run_type=DagRunType.MANUAL
        )
        ti = TaskInstance(task=op)
        ti.dag_run = dag_run
        session.add(ti)
        session.commit()
        context = ti.get_template_context()
        ti.render_templates(context)

        ret = op.execute(None)

        assert ret == "abcdef"
        conn_mock().create_experiment.assert_called_once_with(
            ExperimentName="the name",
            Description="the desc",
            Tags=[{"Key": "jinja", "Value": "tid"}],
        )

    def test_template_fields(self):
        operator = SageMakerCreateExperimentOperator(
            name="the name",
            description="the desc",
            task_id="tid",
        )
        validate_template_fields(operator)
