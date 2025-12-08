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

from typing import TYPE_CHECKING

from airflow._shared.definitions.node import GenericDAGNode

if TYPE_CHECKING:
    from typing import TypeAlias

    from airflow._shared.logging.types import Logger  # noqa: F401
    from airflow.models.mappedoperator import MappedOperator
    from airflow.serialization.definitions.taskgroup import SerializedTaskGroup  # noqa: F401
    from airflow.serialization.serialized_objects import SerializedBaseOperator, SerializedDAG  # noqa: F401

    Operator: TypeAlias = SerializedBaseOperator | MappedOperator


class DAGNode(GenericDAGNode["SerializedDAG", "Operator", "SerializedTaskGroup", "Logger"]):
    """
    Base class for a node in the graph of a workflow.

    A node may be an operator or task group, either mapped or unmapped.
    """
