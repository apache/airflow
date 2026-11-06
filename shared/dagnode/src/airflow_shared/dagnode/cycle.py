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

from collections import defaultdict, deque
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

# default of int is 0 which corresponds to _NEW
_NEW = 0
_IN_PROGRESS = 1
_DONE = 2

__all__ = ["detect_cycle"]


def detect_cycle(node_ids: Iterable[str], downstream_of: Callable[[str], Iterable[str]]) -> str | None:
    """
    Search the graph for a cycle, following downstream edges only.

    The graph is described by callbacks rather than by node objects so that callers
    holding a serialized Dag can answer from raw data without hydrating it.

    :param node_ids: Every node in the graph, so that disconnected components are covered too.
    :param downstream_of: Returns the ids directly downstream of the given node id.
    :return: The id of the node whose downstream edge closes a cycle, or None when acyclic.
    """
    visited: dict[str, int] = defaultdict(int)
    path_stack: deque[str] = deque()

    for node_id in node_ids:
        if visited[node_id] == _DONE:
            continue
        path_stack.append(node_id)
        # Iterative rather than recursive: Python's stack depth limit is easily
        # reached by a graph with long chains.
        while path_stack:
            current_id = path_stack[-1]
            if visited[current_id] == _NEW:
                visited[current_id] = _IN_PROGRESS

            child_to_check = None
            for downstream_id in downstream_of(current_id):
                if visited[downstream_id] == _IN_PROGRESS:
                    return current_id
                if visited[downstream_id] == _NEW:
                    child_to_check = downstream_id
                    break

            if child_to_check is None:
                visited[current_id] = _DONE
                path_stack.pop()
            else:
                path_stack.append(child_to_check)

    return None
