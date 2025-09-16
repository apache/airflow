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

from typing import TYPE_CHECKING, TypeAlias, cast

from airflow.models.mappedoperator import MappedOperator
from airflow.sdk.definitions._internal.abstractoperator import AbstractOperator
from airflow.serialization.serialized_objects import SerializedBaseOperator, SerializedDAG

if TYPE_CHECKING:
    from collections.abc import Iterable

    from airflow.sdk import DAG

    Operator: TypeAlias = MappedOperator | SerializedBaseOperator


def dag_edges(dag: DAG | SerializedDAG):
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
    # Edges to add between TaskGroup
    edges_to_add = set()
    # Edges to remove between individual tasks that are replaced by edges_to_add.
    edges_to_skip = set()

    task_group_map = dag.task_group.get_task_group_dict()

    def collect_edges(task_group):
        """Update edges_to_add and edges_to_skip according to TaskGroups."""
        if isinstance(task_group, (AbstractOperator, SerializedBaseOperator, MappedOperator)):
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

    # TODO (GH-52141): 'roots' in scheduler needs to return scheduler types
    # instead, but currently it inherits SDK's DAG.
    tasks_to_trace = cast("list[Operator]", dag.roots)
    while tasks_to_trace:
        tasks_to_trace_next: list[Operator] = []
        for task in tasks_to_trace:
            # TODO (GH-52141): downstream_list on DAGNode needs to be able to
            # return scheduler types when used in scheduler, but SDK types when
            # used at runtime. This means DAGNode needs to be rewritten as a
            # generic class.
            for child in cast("Iterable[Operator]", task.downstream_list):
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
