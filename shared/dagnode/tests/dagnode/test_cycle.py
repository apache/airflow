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

import sys

import pytest

from airflow_shared.dagnode.cycle import detect_cycle


def edges_of(graph: dict[str, list[str]]):
    return lambda node_id: graph[node_id]


@pytest.mark.parametrize(
    "graph",
    [
        pytest.param({}, id="empty"),
        pytest.param({"a": []}, id="single-node"),
        pytest.param({"a": ["b"], "b": ["c"], "c": []}, id="chain"),
        pytest.param({"a": ["b", "c"], "b": ["d"], "c": ["d"], "d": []}, id="diamond"),
        pytest.param({"a": ["b"], "b": [], "c": ["d"], "d": []}, id="disconnected-components"),
    ],
)
def test_returns_none_when_acyclic(graph):
    assert detect_cycle(graph, edges_of(graph)) is None


@pytest.mark.parametrize(
    ("graph", "expected"),
    [
        pytest.param({"a": ["a"]}, "a", id="self-loop"),
        pytest.param({"a": ["b"], "b": ["a"]}, "b", id="two-node-cycle"),
        pytest.param({"a": ["b"], "b": ["c"], "c": ["a"]}, "c", id="three-node-cycle"),
        pytest.param({"a": ["b"], "b": ["c"], "c": ["b"]}, "c", id="cycle-below-an-acyclic-root"),
        pytest.param({"a": [], "b": ["c"], "c": ["b"]}, "c", id="cycle-outside-the-first-component"),
    ],
)
def test_returns_the_node_whose_edge_closes_the_cycle(graph, expected):
    assert detect_cycle(graph, edges_of(graph)) == expected


def test_follows_downstream_edges_only():
    """An edge is only traversed in the direction the callback reports."""
    downstream = {"a": ["b"], "b": []}
    upstream = {"a": [], "b": ["a"]}

    assert detect_cycle(downstream, lambda node_id: downstream[node_id]) is None
    assert detect_cycle(upstream, lambda node_id: upstream[node_id]) is None


def test_reads_the_graph_only_through_the_callback():
    """Callers may answer from any shape, such as raw serialized Dag data."""
    serialized = {
        "tasks": [
            {"task_id": "a", "downstream_task_ids": ["b"]},
            {"task_id": "b", "downstream_task_ids": ["a"]},
        ]
    }
    by_id = {task["task_id"]: task for task in serialized["tasks"]}

    assert detect_cycle(by_id, lambda node_id: by_id[node_id]["downstream_task_ids"]) == "b"


@pytest.mark.parametrize("cyclic", [False, True])
def test_handles_a_graph_deeper_than_the_recursion_limit(cyclic):
    depth = sys.getrecursionlimit() * 2
    graph = {str(i): [str(i + 1)] for i in range(depth)}
    graph[str(depth)] = ["0"] if cyclic else []

    assert detect_cycle(graph, edges_of(graph)) == (str(depth) if cyclic else None)
