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

from abc import abstractmethod
from typing import TYPE_CHECKING, NamedTuple
from unittest import mock

import pytest

from airflow.models.xcom import XCom
from airflow.providers.amazon.aws.links.base_aws import BaseAwsLink
from airflow.serialization.serialized_objects import SerializedDAG

from tests_common.test_utils.compat import AIRFLOW_V_3_0_PLUS
from tests_common.test_utils.mock_operators import MockOperator

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance

XCOM_KEY = "test_xcom_key"
CUSTOM_KEYS = {
    "foo": "bar",
    "spam": "egg",
}
TEST_REGION_NAME = "eu-west-1"
TEST_AWS_PARTITION = "aws"


class SimpleBaseAwsLink(BaseAwsLink):
    key = XCOM_KEY


class TestBaseAwsLink:
    @pytest.mark.parametrize(
        "region_name, aws_partition,keywords,expected_value",
        [
            (
                "eu-central-1",
                "aws",
                {},
                {"region_name": "eu-central-1", "aws_domain": "aws.amazon.com"},
            ),
            (
                "cn-north-1",
                "aws-cn",
                {},
                {"region_name": "cn-north-1", "aws_domain": "amazonaws.cn"},
            ),
            (
                "us-gov-east-1",
                "aws-us-gov",
                {},
                {"region_name": "us-gov-east-1", "aws_domain": "amazonaws-us-gov.com"},
            ),
            (
                "eu-west-1",
                "aws",
                CUSTOM_KEYS,
                {
                    "region_name": "eu-west-1",
                    "aws_domain": "aws.amazon.com",
                    **CUSTOM_KEYS,
                },
            ),
        ],
    )
    def test_persist(self, region_name, aws_partition, keywords, expected_value):
        mock_context = mock.MagicMock()

        SimpleBaseAwsLink.persist(
            context=mock_context,
            operator=MockOperator(task_id="test_task_id"),
            region_name=region_name,
            aws_partition=aws_partition,
            **keywords,
        )

        ti = mock_context["ti"]
        if AIRFLOW_V_3_0_PLUS:
            ti.xcom_push.assert_called_once_with(
                key=XCOM_KEY,
                value=expected_value,
            )
        else:
            ti.xcom_push.assert_called_once_with(
                key=XCOM_KEY, value=expected_value, execution_date=None
            )

    def test_disable_xcom_push(self):
        mock_context = mock.MagicMock()
        SimpleBaseAwsLink.persist(
            context=mock_context,
            operator=MockOperator(task_id="test_task_id", do_xcom_push=False),
            region_name="eu-east-1",
            aws_partition="aws",
        )
        ti = mock_context["ti"]
        ti.xcom_push.assert_not_called()

    def test_suppress_error_on_xcom_push(self):
        mock_context = mock.MagicMock()
        with mock.patch.object(
            MockOperator, "xcom_push", side_effect=PermissionError("FakeError")
        ) as m:
            SimpleBaseAwsLink.persist(
                context=mock_context,
                operator=MockOperator(task_id="test_task_id"),
                region_name="eu-east-1",
                aws_partition="aws",
            )
            m.assert_called_once_with(
                mock_context,
                key="test_xcom_key",
                value={"region_name": "eu-east-1", "aws_domain": "aws.amazon.com"},
            )


def link_test_operator(*links):
    """Helper for create mock operator class with extra links"""

    class LinkTestOperator(MockOperator):
        operator_extra_links = tuple(c() for c in links)

    return LinkTestOperator


class OperatorAndTi(NamedTuple):
    """Helper container for store task and generated task instance."""

    task: MockOperator
    task_instance: TaskInstance


@pytest.mark.db_test
@pytest.mark.need_serialized_dag
class BaseAwsLinksTestCase:
    """Base class for AWS Provider links tests."""

    link_class: type[BaseAwsLink]

    @pytest.fixture(autouse=True)
    def setup_base_test_case(self, dag_maker, create_task_instance_of_operator):
        self.dag_maker = dag_maker
        self.ti_maker = create_task_instance_of_operator

    @property
    def full_qualname(self) -> str:
        return f"{self.link_class.__module__}.{self.link_class.__qualname__}"

    @property
    def task_id(self) -> str:
        return f"test-{self.link_class.__name__}"

    def create_op_and_ti(
        self,
        extra_link_class: type[BaseAwsLink],
        *,
        dag_id,
        task_id,
        execution_date=None,
        session=None,
        **operator_kwargs,
    ):
        """Helper method for generate operator and task instance"""
        op = link_test_operator(extra_link_class)
        return OperatorAndTi(
            task=op(task_id=task_id),
            task_instance=self.ti_maker(
                op,
                dag_id=dag_id,
                task_id=task_id,
                execution_date=execution_date,
                session=session,
                **operator_kwargs,
            ),
        )

    def assert_extra_link_url(
        self,
        expected_url: str,
        region_name=TEST_REGION_NAME,
        aws_partition=TEST_AWS_PARTITION,
        **extra_link_kwargs,
    ):
        """Helper method for create extra link URL from the parameters."""
        task, ti = self.create_op_and_ti(
            self.link_class, dag_id="test_extra_link", task_id=self.task_id
        )

        mock_context = mock.MagicMock()
        mock_context.__getitem__.side_effect = {"ti": ti}.__getitem__

        self.link_class.persist(
            context=mock_context,
            operator=task,
            region_name=region_name,
            aws_partition=aws_partition,
            **extra_link_kwargs,
        )

        error_msg = f"{self.full_qualname!r} should be preserved after execution"
        assert (
            ti.task.get_extra_links(ti, self.link_class.name) == expected_url
        ), error_msg

        serialized_dag = self.dag_maker.get_serialized_data()
        deserialized_dag = SerializedDAG.from_dict(serialized_dag)
        deserialized_task = deserialized_dag.task_dict[self.task_id]

        error_msg = f"{self.full_qualname!r} should be preserved in deserialized tasks after execution"
        assert (
            deserialized_task.get_extra_links(ti, self.link_class.name) == expected_url
        ), error_msg

    def test_link_serialize(self):
        """Test: Operator links should exist for serialized DAG."""
        self.create_op_and_ti(
            self.link_class, dag_id="test_link_serialize", task_id=self.task_id
        )
        serialized_dag = self.dag_maker.get_serialized_data()
        deserialized_dag = SerializedDAG.deserialize_dag(serialized_dag["dag"])
        operator_extra_link = deserialized_dag.tasks[0].operator_extra_links[0]
        error_message = "Operator links should exist for serialized DAG"
        assert operator_extra_link.name == self.link_class.name, error_message

    def test_empty_xcom(self):
        """Test: Operator links should return empty string if no XCom value."""
        ti = self.create_op_and_ti(
            self.link_class, dag_id="test_empty_xcom", task_id=self.task_id
        ).task_instance

        serialized_dag = self.dag_maker.get_serialized_data()
        deserialized_dag = SerializedDAG.from_dict(serialized_dag)
        deserialized_task = deserialized_dag.task_dict[self.task_id]

        assert (
            ti.task.get_extra_links(ti, self.link_class.name) == ""
        ), "Operator link should only be added if job id is available in XCom"

        assert (
            deserialized_task.get_extra_links(ti, self.link_class.name) == ""
        ), "Operator link should be empty for deserialized task with no XCom push"

    def test_suppress_error_on_xcom_pull(self):
        """Test ignore any error on XCom pull"""
        with mock.patch.object(XCom, "get_value", side_effect=OSError("FakeError")) as m:
            op, ti = self.create_op_and_ti(
                self.link_class, dag_id="test_error_on_xcom_pull", task_id=self.task_id
            )
            self.link_class().get_link(op, ti_key=ti.key)
            m.assert_called_once()

    @abstractmethod
    def test_extra_link(self, **kwargs):
        """Test: Expected URL Link."""
        raise NotImplementedError(
            f"{type(self).__name__!r} should implement `test_extra_link` test"
        )
