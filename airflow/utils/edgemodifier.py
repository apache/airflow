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

from typing import TYPE_CHECKING, Sequence

from airflow.models.taskmixin import DependencyMixin

if TYPE_CHECKING:
    from airflow.models.baseoperator import BaseOperator


class EdgeModifier(DependencyMixin):
    """
    Class that represents edge information to be added between two
    tasks/operators. Has shorthand factory functions, like Label("hooray").

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
        self._upstream: list[BaseOperator] = []
        self._downstream: list[BaseOperator] = []

    @property
    def roots(self):
        return self._downstream

    @property
    def leaves(self):
        return self._upstream

    def set_upstream(
        self, task_or_task_list: DependencyMixin | Sequence[DependencyMixin], chain: bool = True
    ):
        """
        Sets the given task/list onto the upstream attribute, and then checks if
        we have both sides so we can resolve the relationship.

        Providing this also provides << via DependencyMixin.
        """
        from airflow.models.baseoperator import BaseOperator

        # Ensure we have a list, even if it's just one item
        if isinstance(task_or_task_list, DependencyMixin):
            task_or_task_list = [task_or_task_list]
        # Unfurl it into actual operators
        operators: list[BaseOperator] = []
        for task in task_or_task_list:
            for root in task.roots:
                if not isinstance(root, BaseOperator):
                    raise TypeError(f"Cannot use edge labels with {type(root).__name__}, only operators")
                operators.append(root)
        # For each already-declared downstream, pair off with each new upstream
        # item and store the edge info.
        for operator in operators:
            for downstream in self._downstream:
                self.add_edge_info(operator.dag, operator.task_id, downstream.task_id)
                if chain:
                    operator.set_downstream(downstream)
        # Add the new tasks to our list of ones we've seen
        self._upstream.extend(operators)

    def set_downstream(
        self, task_or_task_list: DependencyMixin | Sequence[DependencyMixin], chain: bool = True
    ):
        """
        Sets the given task/list onto the downstream attribute, and then checks if
        we have both sides so we can resolve the relationship.

        Providing this also provides >> via DependencyMixin.
        """
        from airflow.models.baseoperator import BaseOperator

        # Ensure we have a list, even if it's just one item
        if isinstance(task_or_task_list, DependencyMixin):
            task_or_task_list = [task_or_task_list]
        # Unfurl it into actual operators
        operators: list[BaseOperator] = []
        for task in task_or_task_list:
            for leaf in task.leaves:
                if not isinstance(leaf, BaseOperator):
                    raise TypeError(f"Cannot use edge labels with {type(leaf).__name__}, only operators")
                operators.append(leaf)
        # Pair them off with existing
        for operator in operators:
            for upstream in self._upstream:
                self.add_edge_info(upstream.dag, upstream.task_id, operator.task_id)
                if chain:
                    upstream.set_downstream(operator)
        # Add the new tasks to our list of ones we've seen
        self._downstream.extend(operators)

    def update_relative(self, other: DependencyMixin, upstream: bool = True) -> None:
        """
        Called if we're not the "main" side of a relationship; we still run the
        same logic, though.
        """
        if upstream:
            self.set_upstream(other, chain=False)
        else:
            self.set_downstream(other, chain=False)

    def add_edge_info(self, dag, upstream_id: str, downstream_id: str):
        """
        Adds or updates task info on the DAG for this specific pair of tasks.

        Called either from our relationship trigger methods above, or directly
        by set_upstream/set_downstream in operators.
        """
        dag.set_edge_info(upstream_id, downstream_id, {"label": self.label})


# Factory functions
def Label(label: str):
    """Creates an EdgeModifier that sets a human-readable label on the edge."""
    return EdgeModifier(label=label)
