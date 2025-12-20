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

from unittest import mock

import attrs
import pytest

from airflow_shared.dagnode.node import GenericDAGNode


class Task:
    """Task type for tests."""


@attrs.define
class TaskGroup:
    """Task group type for tests."""

    node_id: str = attrs.field(init=False, default="test_group_id")
    prefix_group_id: str


class Dag:
    """Dag type for tests."""

    dag_id = "test_dag_id"


class ConcreteDAGNode(GenericDAGNode[Dag, Task, TaskGroup]):
    """Concrete DAGNode variant for tests."""

    dag = None
    task_group = None

    @property
    def node_id(self) -> str:
        return "test_group_id.test_node_id"


class TestDAGNode:
    @pytest.fixture
    def node(self):
        return ConcreteDAGNode()

    def test_log(self, node: ConcreteDAGNode) -> None:
        assert node._cached_logger is None
        with mock.patch("structlog.get_logger") as mock_get_logger:
            log = node.log
        assert log is node._cached_logger
        assert mock_get_logger.mock_calls == [mock.call("tests.dagnode.test_node.ConcreteDAGNode")]

    def test_dag_id(self, node: ConcreteDAGNode) -> None:
        assert node.dag is None
        assert node.dag_id == "_in_memory_dag_"
        node.dag = Dag()
        assert node.dag_id == "test_dag_id"

    @pytest.mark.parametrize(
        ("prefix_group_id", "expected_label"),
        [(True, "test_node_id"), (False, "test_group_id.test_node_id")],
    )
    def test_label(self, node: ConcreteDAGNode, prefix_group_id: bool, expected_label: str) -> None:
        assert node.task_group is None
        assert node.label == "test_group_id.test_node_id"
        node.task_group = TaskGroup(prefix_group_id)
        assert node.label == expected_label
