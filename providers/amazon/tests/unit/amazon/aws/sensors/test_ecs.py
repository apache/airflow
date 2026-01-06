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

from typing import TypeVar
from unittest import mock

import boto3
import pytest
from slugify import slugify

from airflow.providers.amazon.aws.sensors.ecs import (
    EcsBaseSensor,
    EcsClusterStates,
    EcsClusterStateSensor,
    EcsHook,
    EcsTaskDefinitionStates,
    EcsTaskDefinitionStateSensor,
    EcsTaskStates,
    EcsTaskStateSensor,
)
from airflow.providers.amazon.version_compat import NOTSET
from airflow.providers.common.compat.sdk import AirflowException

from tests_common.test_utils.compat import timezone

_Operator = TypeVar("_Operator")
TEST_CLUSTER_NAME = "fake-cluster"
TEST_TASK_ARN = "arn:aws:ecs:us-east-1:012345678910:task/spam-egg"
TEST_TASK_DEFINITION_ARN = "arn:aws:ecs:us-east-1:012345678910:task-definition/foo-bar:42"

pytestmark = pytest.mark.db_test


class EcsBaseTestCase:
    @pytest.fixture(autouse=True)
    def _setup_test_cases(self, monkeypatch, request, create_task_instance_of_operator, session):
        self.dag_id = f"dag-{slugify(request.cls.__name__)}"
        self.task_id = f"task-{slugify(request.node.name, max_length=40)}"
        self.fake_client = boto3.client("ecs", region_name="eu-west-3")
        monkeypatch.setattr(EcsHook, "conn", self.fake_client)
        self.ti_maker = create_task_instance_of_operator
        self.session = session

    def create_rendered_task(self, operator_class: type[_Operator], **kwargs) -> _Operator:
        """
        Create operator from given class and render fields.

        This might help to prevent of unexpected behaviour in Jinja/task field serialisation
        """
        ti = self.ti_maker(
            operator_class,
            dag_id=self.dag_id,
            task_id=self.task_id,
            logical_date=timezone.datetime(2021, 12, 21),
            **kwargs,
        )
        self.session.add(ti)
        self.session.commit()
        return ti.render_templates()


class TestEcsBaseSensor(EcsBaseTestCase):
    @pytest.mark.parametrize("aws_conn_id", [None, NOTSET, "aws_test_conn"])
    @pytest.mark.parametrize("region_name", [None, NOTSET, "ca-central-1"])
    def test_initialise_operator(self, aws_conn_id, region_name):
        """Test sensor initialize."""
        op_kw = {"aws_conn_id": aws_conn_id, "region_name": region_name}
        op_kw = {k: v for k, v in op_kw.items() if v is not NOTSET}
        op = EcsBaseSensor(task_id="test_ecs_base", **op_kw)

        assert op.aws_conn_id == (aws_conn_id if aws_conn_id is not NOTSET else "aws_default")
        assert op.region_name == (region_name if region_name is not NOTSET else None)

    @pytest.mark.parametrize("aws_conn_id", [None, NOTSET, "aws_test_conn"])
    @pytest.mark.parametrize("region_name", [None, NOTSET, "ca-central-1"])
    def test_hook_and_client(self, aws_conn_id, region_name):
        """Test initialize ``EcsHook`` and ``boto3.client``."""
        op_kw = {"aws_conn_id": aws_conn_id, "region_name": region_name}
        op_kw = {k: v for k, v in op_kw.items() if v is not NOTSET}
        op = EcsBaseSensor(task_id="test_ecs_base_hook_client", **op_kw)

        hook = op.hook
        assert op.hook is hook, "'hook' property should be cached."
        assert isinstance(op.hook, EcsHook)

        client = op.client
        assert op.client is client, "'client' property should be cached."
        assert client is self.fake_client


class TestEcsClusterStateSensor(EcsBaseTestCase):
    @pytest.mark.parametrize(
        ("return_state", "expected"), [("ACTIVE", True), ("PROVISIONING", False), ("DEPROVISIONING", False)]
    )
    def test_default_values_poke(self, return_state, expected):
        task = self.create_rendered_task(EcsClusterStateSensor, cluster_name=TEST_CLUSTER_NAME)
        with mock.patch.object(task.hook, "get_cluster_state") as m:
            m.return_value = return_state
            assert task.poke({}) == expected
            m.assert_called_once_with(cluster_name=TEST_CLUSTER_NAME)

    @pytest.mark.parametrize("return_state", ["FAILED", "INACTIVE"])
    def test_default_values_terminal_state(self, return_state):
        task = self.create_rendered_task(EcsClusterStateSensor, cluster_name=TEST_CLUSTER_NAME)
        with mock.patch.object(task.hook, "get_cluster_state") as m:
            m.return_value = return_state
            with pytest.raises(AirflowException, match="Terminal state reached"):
                task.poke({})
            m.assert_called_once_with(cluster_name=TEST_CLUSTER_NAME)

    @pytest.mark.parametrize(
        ("target_state", "return_state", "expected"),
        [
            (EcsClusterStates.ACTIVE, "ACTIVE", True),
            (EcsClusterStates.ACTIVE, "DEPROVISIONING", False),
            (EcsClusterStates.DEPROVISIONING, "ACTIVE", False),
            (EcsClusterStates.DEPROVISIONING, "DEPROVISIONING", True),
        ],
    )
    def test_custom_values_poke(self, target_state, return_state, expected):
        task = self.create_rendered_task(
            EcsClusterStateSensor, cluster_name=TEST_CLUSTER_NAME, target_state=target_state
        )
        with mock.patch.object(task.hook, "get_cluster_state") as m:
            m.return_value = return_state
            assert task.poke({}) == expected
            m.assert_called_once_with(cluster_name=TEST_CLUSTER_NAME)

    @pytest.mark.parametrize(
        ("failure_states", "return_state"),
        [
            ({EcsClusterStates.ACTIVE}, "ACTIVE"),
            ({EcsClusterStates.PROVISIONING, EcsClusterStates.DEPROVISIONING}, "DEPROVISIONING"),
            ({EcsClusterStates.PROVISIONING, EcsClusterStates.DEPROVISIONING}, "PROVISIONING"),
        ],
    )
    def test_custom_values_terminal_state(self, failure_states, return_state):
        task = self.create_rendered_task(
            EcsClusterStateSensor,
            cluster_name=TEST_CLUSTER_NAME,
            target_state=EcsClusterStates.FAILED,
            failure_states=failure_states,
        )
        with mock.patch.object(task.hook, "get_cluster_state") as m:
            m.return_value = return_state
            with pytest.raises(AirflowException, match="Terminal state reached"):
                task.poke({})
            m.assert_called_once_with(cluster_name=TEST_CLUSTER_NAME)


class TestEcsTaskDefinitionStateSensor(EcsBaseTestCase):
    @pytest.mark.parametrize(
        ("return_state", "expected"), [("ACTIVE", True), ("INACTIVE", False), ("DELETE_IN_PROGRESS", False)]
    )
    def test_default_values_poke(self, return_state, expected):
        task = self.create_rendered_task(
            EcsTaskDefinitionStateSensor, task_definition=TEST_TASK_DEFINITION_ARN
        )
        with mock.patch.object(task.hook, "get_task_definition_state") as m:
            m.return_value = return_state
            assert task.poke({}) == expected
            m.assert_called_once_with(task_definition=TEST_TASK_DEFINITION_ARN)

    @pytest.mark.parametrize(
        ("target_state", "return_state", "expected"),
        [
            (EcsTaskDefinitionStates.INACTIVE, "ACTIVE", False),
            (EcsTaskDefinitionStates.INACTIVE, "INACTIVE", True),
            (EcsTaskDefinitionStates.ACTIVE, "INACTIVE", False),
            (EcsTaskDefinitionStates.ACTIVE, "ACTIVE", True),
        ],
    )
    def test_custom_values_poke(self, create_task_of_operator, target_state, return_state, expected):
        task = self.create_rendered_task(
            EcsTaskDefinitionStateSensor, task_definition=TEST_TASK_DEFINITION_ARN, target_state=target_state
        )
        with mock.patch.object(task.hook, "get_task_definition_state") as m:
            m.return_value = return_state
            assert task.poke({}) == expected
            m.assert_called_once_with(task_definition=TEST_TASK_DEFINITION_ARN)


class TestEcsTaskStateSensor(EcsBaseTestCase):
    @pytest.mark.parametrize(
        ("return_state", "expected"),
        [
            ("PROVISIONING", False),
            ("PENDING", False),
            ("ACTIVATING", False),
            ("RUNNING", True),
            ("DEACTIVATING", False),
            ("STOPPING", False),
            ("DEPROVISIONING", False),
            ("NONE", False),
        ],
    )
    def test_default_values_poke(self, return_state, expected):
        task = self.create_rendered_task(EcsTaskStateSensor, cluster=TEST_CLUSTER_NAME, task=TEST_TASK_ARN)
        with mock.patch.object(task.hook, "get_task_state") as m:
            m.return_value = return_state
            assert task.poke({}) == expected
            m.assert_called_once_with(cluster=TEST_CLUSTER_NAME, task=TEST_TASK_ARN)

    @pytest.mark.parametrize("return_state", ["STOPPED"])
    def test_default_values_terminal_state(self, return_state):
        task = self.create_rendered_task(EcsTaskStateSensor, cluster=TEST_CLUSTER_NAME, task=TEST_TASK_ARN)
        with mock.patch.object(task.hook, "get_task_state") as m:
            m.return_value = return_state
            with pytest.raises(AirflowException, match="Terminal state reached"):
                task.poke({})
            m.assert_called_once_with(cluster=TEST_CLUSTER_NAME, task=TEST_TASK_ARN)

    @pytest.mark.parametrize(
        ("target_state", "return_state", "expected"),
        [
            (EcsTaskStates.RUNNING, "RUNNING", True),
            (EcsTaskStates.DEACTIVATING, "DEACTIVATING", True),
            (EcsTaskStates.NONE, "PENDING", False),
            (EcsTaskStates.STOPPING, "NONE", False),
        ],
    )
    def test_custom_values_poke(self, target_state, return_state, expected):
        task = self.create_rendered_task(
            EcsTaskStateSensor, cluster=TEST_CLUSTER_NAME, task=TEST_TASK_ARN, target_state=target_state
        )
        with mock.patch.object(task.hook, "get_task_state") as m:
            m.return_value = return_state
            assert task.poke({}) == expected
            m.assert_called_once_with(cluster=TEST_CLUSTER_NAME, task=TEST_TASK_ARN)

    @pytest.mark.parametrize(
        ("failure_states", "return_state"),
        [
            ({EcsTaskStates.RUNNING}, "RUNNING"),
            ({EcsTaskStates.RUNNING, EcsTaskStates.DEACTIVATING}, "DEACTIVATING"),
            ({EcsTaskStates.RUNNING, EcsTaskStates.DEACTIVATING}, "RUNNING"),
        ],
    )
    def test_custom_values_terminal_state(self, failure_states, return_state):
        task = self.create_rendered_task(
            EcsTaskStateSensor,
            cluster=TEST_CLUSTER_NAME,
            task=TEST_TASK_ARN,
            target_state=EcsTaskStates.NONE,
            failure_states=failure_states,
        )
        with mock.patch.object(task.hook, "get_task_state") as m:
            m.return_value = return_state
            with pytest.raises(AirflowException, match="Terminal state reached"):
                task.poke({})
            m.assert_called_once_with(cluster=TEST_CLUSTER_NAME, task=TEST_TASK_ARN)
