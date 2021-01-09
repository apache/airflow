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

from typing import TYPE_CHECKING, Dict, Generator, List, Optional, Sequence, Set, Union

from airflow.exceptions import AirflowException, DuplicateTaskIdFound
from airflow.models.taskmixin import TaskMixin

if TYPE_CHECKING:
    from airflow.models.baseoperator import BaseOperator
    from airflow.models.dag import DAG


class TaskGroup(TaskMixin):
    """
    A collection of tasks. When set_downstream() or set_upstream() are called on the
    TaskGroup, it is applied across all tasks within the group if necessary.

    :param group_id: a unique, meaningful id for the TaskGroup. group_id must not conflict
        with group_id of TaskGroup or task_id of tasks in the DAG. Root TaskGroup has group_id
        set to None.
    :type group_id: str
    :param prefix_group_id: If set to True, child task_id and group_id will be prefixed with
        this TaskGroup's group_id. If set to False, child task_id and group_id are not prefixed.
        Default is True.
    :type prefix_group_id: bool
    :param parent_group: The parent TaskGroup of this TaskGroup. parent_group is set to None
        for the root TaskGroup.
    :type parent_group: TaskGroup
    :param dag: The DAG that this TaskGroup belongs to.
    :type dag: airflow.models.DAG
    :param tooltip: The tooltip of the TaskGroup node when displayed in the UI
    :type tooltip: str
    :param ui_color: The fill color of the TaskGroup node when displayed in the UI
    :type ui_color: str
    :param ui_fgcolor: The label color of the TaskGroup node when displayed in the UI
    :type ui_fgcolor: str
    """

    def __init__(
        self,
        group_id: Optional[str],
        prefix_group_id: bool = True,
        parent_group: Optional["TaskGroup"] = None,
        dag: Optional["DAG"] = None,
        tooltip: str = "",
        ui_color: str = "CornflowerBlue",
        ui_fgcolor: str = "#000",
    ):
        from airflow.models.dag import DagContext

        self.prefix_group_id = prefix_group_id

        if group_id is None:
            # This creates a root TaskGroup.
            if parent_group:
                raise AirflowException("Root TaskGroup cannot have parent_group")
            # used_group_ids is shared across all TaskGroups in the same DAG to keep track
            # of used group_id to avoid duplication.
            self.used_group_ids: Set[Optional[str]] = set()
            self._parent_group = None
        else:
            if not isinstance(group_id, str):
                raise ValueError("group_id must be str")
            if not group_id:
                raise ValueError("group_id must not be empty")

            dag = dag or DagContext.get_current_dag()

            if not parent_group and not dag:
                raise AirflowException("TaskGroup can only be used inside a dag")

            self._parent_group = parent_group or TaskGroupContext.get_current_task_group(dag)
            if not self._parent_group:
                raise AirflowException("TaskGroup must have a parent_group except for the root TaskGroup")
            self.used_group_ids = self._parent_group.used_group_ids

        self._group_id = group_id
        if self.group_id in self.used_group_ids:
            raise DuplicateTaskIdFound(f"group_id '{self.group_id}' has already been added to the DAG")
        self.used_group_ids.add(self.group_id)
        self.used_group_ids.add(self.downstream_join_id)
        self.used_group_ids.add(self.upstream_join_id)
        self.children: Dict[str, Union["BaseOperator", "TaskGroup"]] = {}
        if self._parent_group:
            self._parent_group.add(self)

        self.tooltip = tooltip
        self.ui_color = ui_color
        self.ui_fgcolor = ui_fgcolor

        # Keep track of TaskGroups or tasks that depend on this entire TaskGroup separately
        # so that we can optimize the number of edges when entire TaskGroups depend on each other.
        self.upstream_group_ids: Set[Optional[str]] = set()
        self.downstream_group_ids: Set[Optional[str]] = set()
        self.upstream_task_ids: Set[Optional[str]] = set()
        self.downstream_task_ids: Set[Optional[str]] = set()

    @classmethod
    def create_root(cls, dag: "DAG") -> "TaskGroup":
        """Create a root TaskGroup with no group_id or parent."""
        return cls(group_id=None, dag=dag)

    @property
    def is_root(self) -> bool:
        """Returns True if this TaskGroup is the root TaskGroup. Otherwise False"""
        return not self.group_id

    def __iter__(self):
        for child in self.children.values():
            if isinstance(child, TaskGroup):
                yield from child
            else:
                yield child

    def add(self, task: Union["BaseOperator", "TaskGroup"]) -> None:
        """Add a task to this TaskGroup."""
        key = task.group_id if isinstance(task, TaskGroup) else task.task_id

        if key in self.children:
            raise DuplicateTaskIdFound(f"Task id '{key}' has already been added to the DAG")

        if isinstance(task, TaskGroup):
            if task.children:
                raise AirflowException("Cannot add a non-empty TaskGroup")

        self.children[key] = task  # type: ignore

    @property
    def group_id(self) -> Optional[str]:
        """group_id of this TaskGroup."""
        if self._parent_group and self._parent_group.prefix_group_id and self._parent_group.group_id:
            return self._parent_group.child_id(self._group_id)

        return self._group_id

    @property
    def label(self) -> Optional[str]:
        """group_id excluding parent's group_id used as the node label in UI."""
        return self._group_id

    def update_relative(self, other: "TaskMixin", upstream=True) -> None:
        """
        Overrides TaskMixin.update_relative.

        Update upstream_group_ids/downstream_group_ids/upstream_task_ids/downstream_task_ids
        accordingly so that we can reduce the number of edges when displaying Graph View.
        """
        from airflow.models.baseoperator import BaseOperator

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
                if not isinstance(task, BaseOperator):
                    raise AirflowException(
                        "Relationships can only be set between TaskGroup "
                        f"or operators; received {task.__class__.__name__}"
                    )

                if upstream:
                    self.upstream_task_ids.add(task.task_id)
                else:
                    self.downstream_task_ids.add(task.task_id)

    def _set_relative(
        self, task_or_task_list: Union[TaskMixin, Sequence[TaskMixin]], upstream: bool = False
    ) -> None:
        """
        Call set_upstream/set_downstream for all root/leaf tasks within this TaskGroup.
        Update upstream_group_ids/downstream_group_ids/upstream_task_ids/downstream_task_ids.
        """
        if upstream:
            for task in self.get_roots():
                task.set_upstream(task_or_task_list)
        else:
            for task in self.get_leaves():
                task.set_downstream(task_or_task_list)

        if not isinstance(task_or_task_list, Sequence):
            task_or_task_list = [task_or_task_list]

        for task_like in task_or_task_list:
            self.update_relative(task_like, upstream)

    def set_downstream(self, task_or_task_list: Union[TaskMixin, Sequence[TaskMixin]]) -> None:
        """Set a TaskGroup/task/list of task downstream of this TaskGroup."""
        self._set_relative(task_or_task_list, upstream=False)

    def set_upstream(self, task_or_task_list: Union[TaskMixin, Sequence[TaskMixin]]) -> None:
        """Set a TaskGroup/task/list of task upstream of this TaskGroup."""
        self._set_relative(task_or_task_list, upstream=True)

    def __enter__(self):
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
        upstream_join_id will be created in Graph View to join the outgoing edges from this
        TaskGroup to reduce the total number of edges needed to be displayed.
        """
        return f"{self.group_id}.upstream_join_id"

    @property
    def downstream_join_id(self) -> str:
        """
        If this TaskGroup has immediate downstream TaskGroups or tasks, a dummy node called
        downstream_join_id will be created in Graph View to join the outgoing edges from this
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

    def get_child_by_label(self, label: str) -> Union["BaseOperator", "TaskGroup"]:
        """Get a child task/TaskGroup by its label (i.e. task_id/group_id without the group_id prefix)"""
        return self.children[self.child_id(label)]


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
