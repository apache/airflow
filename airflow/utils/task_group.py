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
"""
A TaskGroup is a collection of closely related tasks on the same DAG that should be grouped
together when the DAG is displayed graphically.
"""
import copy
import re
import weakref
from typing import TYPE_CHECKING, Any, Dict, Generator, List, Optional, Sequence, Set, Tuple, Union

from airflow.exceptions import (
    AirflowDagCycleException,
    AirflowException,
    DuplicateTaskIdFound,
    TaskAlreadyInTaskGroup,
)
from airflow.models.taskmixin import DAGNode, DependencyMixin
from airflow.serialization.enums import DagAttributeTypes
from airflow.utils.helpers import validate_group_key

if TYPE_CHECKING:
    from airflow.models.baseoperator import BaseOperator
    from airflow.models.dag import DAG
    from airflow.utils.edgemodifier import EdgeModifier


class TaskGroup(DAGNode):
    """
    A collection of tasks. When set_downstream() or set_upstream() are called on the
    TaskGroup, it is applied across all tasks within the group if necessary.

    :param group_id: a unique, meaningful id for the TaskGroup. group_id must not conflict
        with group_id of TaskGroup or task_id of tasks in the DAG. Root TaskGroup has group_id
        set to None.
    :param prefix_group_id: If set to True, child task_id and group_id will be prefixed with
        this TaskGroup's group_id. If set to False, child task_id and group_id are not prefixed.
        Default is True.
    :param parent_group: The parent TaskGroup of this TaskGroup. parent_group is set to None
        for the root TaskGroup.
    :param dag: The DAG that this TaskGroup belongs to.
    :param default_args: A dictionary of default parameters to be used
        as constructor keyword parameters when initialising operators,
        will override default_args defined in the DAG level.
        Note that operators have the same hook, and precede those defined
        here, meaning that if your dict contains `'depends_on_past': True`
        here and `'depends_on_past': False` in the operator's call
        `default_args`, the actual value will be `False`.
    :param tooltip: The tooltip of the TaskGroup node when displayed in the UI
    :param ui_color: The fill color of the TaskGroup node when displayed in the UI
    :param ui_fgcolor: The label color of the TaskGroup node when displayed in the UI
    :param add_suffix_on_collision: If this task group name already exists,
        automatically add `__1` etc suffixes
    """

    used_group_ids: Set[Optional[str]]

    def __init__(
        self,
        group_id: Optional[str],
        prefix_group_id: bool = True,
        parent_group: Optional["TaskGroup"] = None,
        dag: Optional["DAG"] = None,
        default_args: Optional[Dict] = None,
        tooltip: str = "",
        ui_color: str = "CornflowerBlue",
        ui_fgcolor: str = "#000",
        add_suffix_on_collision: bool = False,
    ):
        from airflow.models.dag import DagContext

        self.prefix_group_id = prefix_group_id
        self.default_args = copy.deepcopy(default_args or {})

        dag = dag or DagContext.get_current_dag()

        if group_id is None:
            # This creates a root TaskGroup.
            if parent_group:
                raise AirflowException("Root TaskGroup cannot have parent_group")
            # used_group_ids is shared across all TaskGroups in the same DAG to keep track
            # of used group_id to avoid duplication.
            self.used_group_ids = set()
            self.dag = dag
        else:
            if prefix_group_id:
                # If group id is used as prefix, it should not contain spaces nor dots
                # because it is used as prefix in the task_id
                validate_group_key(group_id)
            else:
                if not isinstance(group_id, str):
                    raise ValueError("group_id must be str")
                if not group_id:
                    raise ValueError("group_id must not be empty")

            if not parent_group and not dag:
                raise AirflowException("TaskGroup can only be used inside a dag")

            parent_group = parent_group or TaskGroupContext.get_current_task_group(dag)
            if not parent_group:
                raise AirflowException("TaskGroup must have a parent_group except for the root TaskGroup")
            if dag is not parent_group.dag:
                raise RuntimeError(
                    "Cannot mix TaskGroups from different DAGs: %s and %s", dag, parent_group.dag
                )

            self.used_group_ids = parent_group.used_group_ids

        # if given group_id already used assign suffix by incrementing largest used suffix integer
        # Example : task_group ==> task_group__1 -> task_group__2 -> task_group__3
        self._group_id = group_id
        self._check_for_group_id_collisions(add_suffix_on_collision)

        self.children: Dict[str, DAGNode] = {}
        if parent_group:
            parent_group.add(self)

        self.used_group_ids.add(self.group_id)
        if self.group_id:
            self.used_group_ids.add(self.downstream_join_id)
            self.used_group_ids.add(self.upstream_join_id)

        self.tooltip = tooltip
        self.ui_color = ui_color
        self.ui_fgcolor = ui_fgcolor

        # Keep track of TaskGroups or tasks that depend on this entire TaskGroup separately
        # so that we can optimize the number of edges when entire TaskGroups depend on each other.
        self.upstream_group_ids: Set[Optional[str]] = set()
        self.downstream_group_ids: Set[Optional[str]] = set()
        self.upstream_task_ids = set()
        self.downstream_task_ids = set()

    def _check_for_group_id_collisions(self, add_suffix_on_collision: bool):
        if self._group_id is None:
            return
        # if given group_id already used assign suffix by incrementing largest used suffix integer
        # Example : task_group ==> task_group__1 -> task_group__2 -> task_group__3
        if self._group_id in self.used_group_ids:
            if not add_suffix_on_collision:
                raise DuplicateTaskIdFound(f"group_id '{self._group_id}' has already been added to the DAG")
            base = re.split(r'__\d+$', self._group_id)[0]
            suffixes = sorted(
                int(re.split(r'^.+__', used_group_id)[1])
                for used_group_id in self.used_group_ids
                if used_group_id is not None and re.match(rf'^{base}__\d+$', used_group_id)
            )
            if not suffixes:
                self._group_id += '__1'
            else:
                self._group_id = f'{base}__{suffixes[-1] + 1}'

    @classmethod
    def create_root(cls, dag: "DAG") -> "TaskGroup":
        """Create a root TaskGroup with no group_id or parent."""
        return cls(group_id=None, dag=dag)

    @property
    def node_id(self):
        return self.group_id

    @property
    def is_root(self) -> bool:
        """Returns True if this TaskGroup is the root TaskGroup. Otherwise False"""
        return not self.group_id

    @property
    def parent_group(self) -> Optional["TaskGroup"]:
        return self.task_group

    def __iter__(self):
        for child in self.children.values():
            if isinstance(child, TaskGroup):
                yield from child
            else:
                yield child

    def add(self, task: DAGNode) -> None:
        """Add a task to this TaskGroup.

        :meta private:
        """
        from airflow.models.abstractoperator import AbstractOperator

        existing_tg = task.task_group
        if isinstance(task, AbstractOperator) and existing_tg is not None and existing_tg != self:
            raise TaskAlreadyInTaskGroup(task.node_id, existing_tg.node_id, self.node_id)

        # Set the TG first, as setting it might change the return value of node_id!
        task.task_group = weakref.proxy(self)
        key = task.node_id

        if key in self.children:
            node_type = "Task" if hasattr(task, 'task_id') else "Task Group"
            raise DuplicateTaskIdFound(f"{node_type} id '{key}' has already been added to the DAG")

        if isinstance(task, TaskGroup):
            if self.dag:
                if task.dag is not None and self.dag is not task.dag:
                    raise RuntimeError(
                        "Cannot mix TaskGroups from different DAGs: %s and %s", self.dag, task.dag
                    )
                task.dag = self.dag
            if task.children:
                raise AirflowException("Cannot add a non-empty TaskGroup")

        self.children[key] = task

    def _remove(self, task: DAGNode) -> None:
        key = task.node_id

        if key not in self.children:
            raise KeyError(f"Node id {key!r} not part of this task group")

        self.used_group_ids.remove(key)
        del self.children[key]

    @property
    def group_id(self) -> Optional[str]:
        """group_id of this TaskGroup."""
        if self.task_group and self.task_group.prefix_group_id and self.task_group.group_id:
            return self.task_group.child_id(self._group_id)

        return self._group_id

    @property
    def label(self) -> Optional[str]:
        """group_id excluding parent's group_id used as the node label in UI."""
        return self._group_id

    def update_relative(self, other: DependencyMixin, upstream=True) -> None:
        """
        Overrides TaskMixin.update_relative.

        Update upstream_group_ids/downstream_group_ids/upstream_task_ids/downstream_task_ids
        accordingly so that we can reduce the number of edges when displaying Graph view.
        """
        if isinstance(other, TaskGroup):
            # Handles setting relationship between a TaskGroup and another TaskGroup
            if upstream:
                parent, child = (self, other)
            else:
                parent, child = (other, self)

            parent.upstream_group_ids.add(child.group_id)
            child.downstream_group_ids.add(parent.group_id)
        else:
            # Handles setting relationship between a TaskGroup and a task
            for task in other.roots:
                if not isinstance(task, DAGNode):
                    raise AirflowException(
                        "Relationships can only be set between TaskGroup "
                        f"or operators; received {task.__class__.__name__}"
                    )

                if upstream:
                    self.upstream_task_ids.add(task.node_id)
                else:
                    self.downstream_task_ids.add(task.node_id)

    def _set_relatives(
        self,
        task_or_task_list: Union["DependencyMixin", Sequence["DependencyMixin"]],
        upstream: bool = False,
        edge_modifier: Optional["EdgeModifier"] = None,
    ) -> None:
        """
        Call set_upstream/set_downstream for all root/leaf tasks within this TaskGroup.
        Update upstream_group_ids/downstream_group_ids/upstream_task_ids/downstream_task_ids.
        """
        if not isinstance(task_or_task_list, Sequence):
            task_or_task_list = [task_or_task_list]

        for task_like in task_or_task_list:
            self.update_relative(task_like, upstream)

        if upstream:
            for task in self.get_roots():
                task.set_upstream(task_or_task_list)
        else:
            for task in self.get_leaves():
                task.set_downstream(task_or_task_list)

    def __enter__(self) -> "TaskGroup":
        TaskGroupContext.push_context_managed_task_group(self)
        return self

    def __exit__(self, _type, _value, _tb):
        TaskGroupContext.pop_context_managed_task_group()

    def has_task(self, task: "BaseOperator") -> bool:
        """Returns True if this TaskGroup or its children TaskGroups contains the given task."""
        if task.task_id in self.children:
            return True

        return any(child.has_task(task) for child in self.children.values() if isinstance(child, TaskGroup))

    @property
    def roots(self) -> List["BaseOperator"]:
        """Required by TaskMixin"""
        return list(self.get_roots())

    @property
    def leaves(self) -> List["BaseOperator"]:
        """Required by TaskMixin"""
        return list(self.get_leaves())

    def get_roots(self) -> Generator["BaseOperator", None, None]:
        """
        Returns a generator of tasks that are root tasks, i.e. those with no upstream
        dependencies within the TaskGroup.
        """
        for task in self:
            if not any(self.has_task(parent) for parent in task.get_direct_relatives(upstream=True)):
                yield task

    def get_leaves(self) -> Generator["BaseOperator", None, None]:
        """
        Returns a generator of tasks that are leaf tasks, i.e. those with no downstream
        dependencies within the TaskGroup
        """
        for task in self:
            if not any(self.has_task(child) for child in task.get_direct_relatives(upstream=False)):
                yield task

    def child_id(self, label):
        """
        Prefix label with group_id if prefix_group_id is True. Otherwise return the label
        as-is.
        """
        if self.prefix_group_id and self.group_id:
            return f"{self.group_id}.{label}"

        return label

    @property
    def upstream_join_id(self) -> str:
        """
        If this TaskGroup has immediate upstream TaskGroups or tasks, a dummy node called
        upstream_join_id will be created in Graph view to join the outgoing edges from this
        TaskGroup to reduce the total number of edges needed to be displayed.
        """
        return f"{self.group_id}.upstream_join_id"

    @property
    def downstream_join_id(self) -> str:
        """
        If this TaskGroup has immediate downstream TaskGroups or tasks, a dummy node called
        downstream_join_id will be created in Graph view to join the outgoing edges from this
        TaskGroup to reduce the total number of edges needed to be displayed.
        """
        return f"{self.group_id}.downstream_join_id"

    def get_task_group_dict(self) -> Dict[str, "TaskGroup"]:
        """Returns a flat dictionary of group_id: TaskGroup"""
        task_group_map = {}

        def build_map(task_group):
            if not isinstance(task_group, TaskGroup):
                return

            task_group_map[task_group.group_id] = task_group

            for child in task_group.children.values():
                build_map(child)

        build_map(self)
        return task_group_map

    def get_child_by_label(self, label: str) -> DAGNode:
        """Get a child task/TaskGroup by its label (i.e. task_id/group_id without the group_id prefix)"""
        return self.children[self.child_id(label)]

    def serialize_for_task_group(self) -> Tuple[DagAttributeTypes, Any]:
        """Required by DAGNode."""
        from airflow.serialization.serialized_objects import SerializedTaskGroup

        return DagAttributeTypes.TASK_GROUP, SerializedTaskGroup.serialize_task_group(self)

    def topological_sort(self, _include_subdag_tasks: bool = False):
        """
        Sorts children in topographical order, such that a task comes after any of its
        upstream dependencies.

        :return: list of tasks in topological order
        """
        # This uses a modified version of Kahn's Topological Sort algorithm to
        # not have to pre-compute the "in-degree" of the nodes.
        from airflow.operators.subdag import SubDagOperator  # Avoid circular import

        graph_unsorted = copy.copy(self.children)

        graph_sorted: List[DAGNode] = []

        # special case
        if len(self.children) == 0:
            return graph_sorted

        # Run until the unsorted graph is empty.
        while graph_unsorted:
            # Go through each of the node/edges pairs in the unsorted graph. If a set of edges doesn't contain
            # any nodes that haven't been resolved, that is, that are still in the unsorted graph, remove the
            # pair from the unsorted graph, and append it to the sorted graph. Note here that by using using
            # the values() method for iterating, a copy of the unsorted graph is used, allowing us to modify
            # the unsorted graph as we move through it.
            #
            # We also keep a flag for checking that graph is acyclic, which is true if any nodes are resolved
            # during each pass through the graph. If not, we need to exit as the graph therefore can't be
            # sorted.
            acyclic = False
            for node in list(graph_unsorted.values()):
                for edge in node.upstream_list:
                    if edge.node_id in graph_unsorted:
                        break
                    # Check for task's group is a child (or grand child) of this TG,
                    tg = edge.task_group
                    while tg:
                        if tg.node_id in graph_unsorted:
                            break
                        tg = tg.task_group

                    if tg:
                        # We are already going to visit that TG
                        break
                else:
                    acyclic = True
                    del graph_unsorted[node.node_id]
                    graph_sorted.append(node)
                    if _include_subdag_tasks and isinstance(node, SubDagOperator):
                        graph_sorted.extend(
                            node.subdag.task_group.topological_sort(_include_subdag_tasks=True)
                        )

            if not acyclic:
                raise AirflowDagCycleException(f"A cyclic dependency occurred in dag: {self.dag_id}")

        return graph_sorted


class TaskGroupContext:
    """TaskGroup context is used to keep the current TaskGroup when TaskGroup is used as ContextManager."""

    _context_managed_task_group: Optional[TaskGroup] = None
    _previous_context_managed_task_groups: List[TaskGroup] = []

    @classmethod
    def push_context_managed_task_group(cls, task_group: TaskGroup):
        """Push a TaskGroup into the list of managed TaskGroups."""
        if cls._context_managed_task_group:
            cls._previous_context_managed_task_groups.append(cls._context_managed_task_group)
        cls._context_managed_task_group = task_group

    @classmethod
    def pop_context_managed_task_group(cls) -> Optional[TaskGroup]:
        """Pops the last TaskGroup from the list of manged TaskGroups and update the current TaskGroup."""
        old_task_group = cls._context_managed_task_group
        if cls._previous_context_managed_task_groups:
            cls._context_managed_task_group = cls._previous_context_managed_task_groups.pop()
        else:
            cls._context_managed_task_group = None
        return old_task_group

    @classmethod
    def get_current_task_group(cls, dag: Optional["DAG"]) -> Optional[TaskGroup]:
        """Get the current TaskGroup."""
        from airflow.models.dag import DagContext

        if not cls._context_managed_task_group:
            dag = dag or DagContext.get_current_dag()
            if dag:
                # If there's currently a DAG but no TaskGroup, return the root TaskGroup of the dag.
                return dag.task_group

        return cls._context_managed_task_group
