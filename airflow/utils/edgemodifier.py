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

from typing import Sequence

from airflow.models.taskmixin import DAGNode, DependencyMixin
from airflow.utils.task_group import TaskGroup


class EdgeModifier(DependencyMixin):
    """
    Class that represents edge information to be added between two tasks/operators.

    Has shorthand factory functions, like Label("hooray").

    Current implementation supports
        t1 >> Label("Success route") >> t2
        t2 << Label("Success route") << t2

    Note that due to the potential for use in either direction, this waits
    to make the actual connection between both sides until both are declared,
    and will do so progressively if multiple ups/downs are added.

    This and EdgeInfo are related - an EdgeModifier is the Python object you
    use to add information to (potentially multiple) edges, and EdgeInfo
    is the representation of the information for one specific edge.
    """

    def __init__(self, label: str | None = None):
        self.label = label
        self._upstream: list[DependencyMixin] = []
        self._downstream: list[DependencyMixin] = []

    @property
    def roots(self):
        return self._downstream

    @property
    def leaves(self):
        return self._upstream

    @staticmethod
    def _make_list(
        item_or_list: DependencyMixin | Sequence[DependencyMixin],
    ) -> Sequence[DependencyMixin]:
        if not isinstance(item_or_list, Sequence):
            return [item_or_list]
        return item_or_list

    def _save_nodes(
        self,
        nodes: DependencyMixin | Sequence[DependencyMixin],
        stream: list[DependencyMixin],
    ):
        from airflow.models.xcom_arg import XComArg

        for node in self._make_list(nodes):
            if isinstance(node, (TaskGroup, XComArg, DAGNode)):
                stream.append(node)
            else:
                raise TypeError(
                    f"Cannot use edge labels with {type(node).__name__}, "
                    f"only tasks, XComArg or TaskGroups"
                )

    def _convert_streams_to_task_groups(self):
        """
        Convert a node to a TaskGroup or leave it as a DAGNode.

        Requires both self._upstream and self._downstream.

        To do this, we keep a set of group_ids seen among the streams. If we find that
        the nodes are from the same TaskGroup, we will leave them as DAGNodes and not
        convert them to TaskGroups
        """
        from airflow.models.xcom_arg import XComArg

        group_ids = set()
        for node in [*self._upstream, *self._downstream]:
            if isinstance(node, DAGNode) and node.task_group:
                if node.task_group.is_root:
                    group_ids.add("root")
                else:
                    group_ids.add(node.task_group.group_id)
            elif isinstance(node, TaskGroup):
                group_ids.add(node.group_id)
            elif isinstance(node, XComArg):
                if isinstance(node.operator, DAGNode) and node.operator.task_group:
                    if node.operator.task_group.is_root:
                        group_ids.add("root")
                    else:
                        group_ids.add(node.operator.task_group.group_id)

        # If all nodes originate from the same TaskGroup, we will not convert them
        if len(group_ids) != 1:
            self._upstream = self._convert_stream_to_task_groups(self._upstream)
            self._downstream = self._convert_stream_to_task_groups(self._downstream)

    def _convert_stream_to_task_groups(
        self, stream: Sequence[DependencyMixin]
    ) -> Sequence[DependencyMixin]:
        return [
            node.task_group
            if isinstance(node, DAGNode)
            and node.task_group
            and not node.task_group.is_root
            else node
            for node in stream
        ]

    def set_upstream(
        self,
        other: DependencyMixin | Sequence[DependencyMixin],
        edge_modifier: EdgeModifier | None = None,
    ):
        """
        Set the given task/list onto the upstream attribute, then attempt to resolve the relationship.

        Providing this also provides << via DependencyMixin.
        """
        self._save_nodes(other, self._upstream)
        if self._upstream and self._downstream:
            # Convert _upstream and _downstream to task_groups only after both are set
            self._convert_streams_to_task_groups()
        for node in self._downstream:
            node.set_upstream(other, edge_modifier=self)

    def set_downstream(
        self,
        other: DependencyMixin | Sequence[DependencyMixin],
        edge_modifier: EdgeModifier | None = None,
    ):
        """
        Set the given task/list onto the downstream attribute, then attempt to resolve the relationship.

        Providing this also provides >> via DependencyMixin.
        """
        self._save_nodes(other, self._downstream)
        if self._upstream and self._downstream:
            # Convert _upstream and _downstream to task_groups only after both are set
            self._convert_streams_to_task_groups()
        for node in self._upstream:
            node.set_downstream(other, edge_modifier=self)

    def update_relative(
        self,
        other: DependencyMixin,
        upstream: bool = True,
        edge_modifier: EdgeModifier | None = None,
    ) -> None:
        """Update relative if we're not the "main" side of a relationship; still run the same logic."""
        if upstream:
            self.set_upstream(other)
        else:
            self.set_downstream(other)

    def add_edge_info(self, dag, upstream_id: str, downstream_id: str):
        """
        Add or update task info on the DAG for this specific pair of tasks.

        Called either from our relationship trigger methods above, or directly
        by set_upstream/set_downstream in operators.
        """
        dag.set_edge_info(upstream_id, downstream_id, {"label": self.label})


# Factory functions
def Label(label: str):
    """Create an EdgeModifier that sets a human-readable label on the edge."""
    return EdgeModifier(label=label)
