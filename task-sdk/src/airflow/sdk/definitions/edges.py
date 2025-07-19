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

from collections.abc import Sequence
from typing import TYPE_CHECKING, TypedDict

from airflow.sdk.definitions._internal.mixins import DependencyMixin

if TYPE_CHECKING:
    from airflow.sdk.definitions.dag import DAG
    from airflow.sdk.types import Operator


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
        from airflow.sdk.definitions._internal.node import DAGNode
        from airflow.sdk.definitions.taskgroup import TaskGroup
        from airflow.sdk.definitions.xcom_arg import XComArg

        for node in self._make_list(nodes):
            if isinstance(node, (TaskGroup, XComArg, DAGNode)):
                stream.append(node)
            else:
                raise TypeError(
                    f"Cannot use edge labels with {type(node).__name__}, only tasks, XComArg or TaskGroups"
                )

    def _convert_streams_to_task_groups(self):
        """
        Convert a node to a TaskGroup or leave it as a DAGNode.

        Requires both self._upstream and self._downstream.

        To do this, we keep a set of group_ids seen among the streams. If we find that
        the nodes are from the same TaskGroup, we will leave them as DAGNodes and not
        convert them to TaskGroups
        """
        from airflow.sdk.definitions._internal.node import DAGNode
        from airflow.sdk.definitions.taskgroup import TaskGroup
        from airflow.sdk.definitions.xcom_arg import XComArg

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

    def _convert_stream_to_task_groups(self, stream: Sequence[DependencyMixin]) -> Sequence[DependencyMixin]:
        from airflow.sdk.definitions._internal.node import DAGNode

        return [
            node.task_group
            if isinstance(node, DAGNode) and node.task_group and not node.task_group.is_root
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

    def add_edge_info(self, dag: DAG, upstream_id: str, downstream_id: str):
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


class EdgeInfoType(TypedDict):
    """Extra metadata that the DAG can store about an edge, usually generated from an EdgeModifier."""

    label: str | None


def dag_edges(dag: DAG):
    """
    Create the list of edges needed to construct the Graph view.

    A special case is made if a TaskGroup is immediately upstream/downstream of another
    TaskGroup or task. Two proxy nodes named upstream_join_id and downstream_join_id are
    created for the TaskGroup. Instead of drawing an edge onto every task in the TaskGroup,
    all edges are directed onto the proxy nodes. This is to cut down the number of edges on
    the graph.

    For example: A DAG with TaskGroups group1 and group2:
        group1: task1, task2, task3
        group2: task4, task5, task6

    group2 is downstream of group1:
        group1 >> group2

    Edges to add (This avoids having to create edges between every task in group1 and group2):
        task1 >> downstream_join_id
        task2 >> downstream_join_id
        task3 >> downstream_join_id
        downstream_join_id >> upstream_join_id
        upstream_join_id >> task4
        upstream_join_id >> task5
        upstream_join_id >> task6
    """
    from airflow.sdk.definitions._internal.abstractoperator import AbstractOperator
    from airflow.serialization.serialized_objects import SerializedBaseOperator

    # Edges to add between TaskGroup
    edges_to_add = set()
    # Edges to remove between individual tasks that are replaced by edges_to_add.
    edges_to_skip = set()

    task_group_map = dag.task_group.get_task_group_dict()

    def collect_edges(task_group):
        """Update edges_to_add and edges_to_skip according to TaskGroups."""
        if isinstance(task_group, (AbstractOperator, SerializedBaseOperator)):
            return

        for target_id in task_group.downstream_group_ids:
            # For every TaskGroup immediately downstream, add edges between downstream_join_id
            # and upstream_join_id. Skip edges between individual tasks of the TaskGroups.
            target_group = task_group_map[target_id]
            edges_to_add.add((task_group.downstream_join_id, target_group.upstream_join_id))

            for child in task_group.get_leaves():
                edges_to_add.add((child.task_id, task_group.downstream_join_id))
                for target in target_group.get_roots():
                    edges_to_skip.add((child.task_id, target.task_id))
                edges_to_skip.add((child.task_id, target_group.upstream_join_id))

            for child in target_group.get_roots():
                edges_to_add.add((target_group.upstream_join_id, child.task_id))
                edges_to_skip.add((task_group.downstream_join_id, child.task_id))

        # For every individual task immediately downstream, add edges between downstream_join_id and
        # the downstream task. Skip edges between individual tasks of the TaskGroup and the
        # downstream task.
        for target_id in task_group.downstream_task_ids:
            edges_to_add.add((task_group.downstream_join_id, target_id))

            for child in task_group.get_leaves():
                edges_to_add.add((child.task_id, task_group.downstream_join_id))
                edges_to_skip.add((child.task_id, target_id))

        # For every individual task immediately upstream, add edges between the upstream task
        # and upstream_join_id. Skip edges between the upstream task and individual tasks
        # of the TaskGroup.
        for source_id in task_group.upstream_task_ids:
            edges_to_add.add((source_id, task_group.upstream_join_id))
            for child in task_group.get_roots():
                edges_to_add.add((task_group.upstream_join_id, child.task_id))
                edges_to_skip.add((source_id, child.task_id))

        for child in task_group.children.values():
            collect_edges(child)

    collect_edges(dag.task_group)

    # Collect all the edges between individual tasks
    edges = set()
    setup_teardown_edges = set()

    tasks_to_trace: list[Operator] = dag.roots
    while tasks_to_trace:
        tasks_to_trace_next: list[Operator] = []
        for task in tasks_to_trace:
            for child in task.downstream_list:
                edge = (task.task_id, child.task_id)
                if task.is_setup and child.is_teardown:
                    setup_teardown_edges.add(edge)
                if edge not in edges:
                    edges.add(edge)
                    tasks_to_trace_next.append(child)
        tasks_to_trace = tasks_to_trace_next

    result = []
    # Build result dicts with the two ends of the edge, plus any extra metadata
    # if we have it.
    for source_id, target_id in sorted(edges.union(edges_to_add) - edges_to_skip):
        record = {"source_id": source_id, "target_id": target_id}
        label = dag.get_edge_info(source_id, target_id).get("label")
        if (source_id, target_id) in setup_teardown_edges:
            record["is_setup_teardown"] = True
        if label:
            record["label"] = label
        result.append(record)
    return result
