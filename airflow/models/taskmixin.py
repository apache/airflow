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

from typing import TYPE_CHECKING, Any, Iterable

if TYPE_CHECKING:
    from airflow.models.operator import Operator
    from airflow.serialization.enums import DagAttributeTypes
    from airflow.typing_compat import TypeAlias

import airflow.sdk.definitions.mixins
import airflow.sdk.definitions.node

DependencyMixin: TypeAlias = airflow.sdk.definitions.mixins.DependencyMixin


class DAGNode(airflow.sdk.definitions.node.DAGNode):
    """
    A base class for a node in the graph of a workflow.

    A node may be an Operator or a Task Group, either mapped or unmapped.
    """

    def get_direct_relatives(self, upstream: bool = False) -> Iterable[Operator]:
        """Get list of the direct relatives to the current task, upstream or downstream."""
        if upstream:
            return self.upstream_list
        else:
            return self.downstream_list

    def serialize_for_task_group(self) -> tuple[DagAttributeTypes, Any]:
        """Serialize a task group's content; used by TaskGroupSerialization."""
        raise NotImplementedError()
