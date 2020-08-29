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

if TYPE_CHECKING:
    from airflow.models.baseoperator import BaseOperator
    from airflow.models.dag import DAG


class TaskGroup:
    """
    A collection of tasks. When set_downstream() or set_upstream() are called on the
    TaskGroup, it is applied across all tasks within the group if necessary.
    """

    def __init__(
        self,
        group_id: Optional[str],
        parent_group: Optional["TaskGroup"] = None,
        dag: Optional["DAG"] = None,
        tooltip: str = "",
        ui_color: str = "CornflowerBlue",
        ui_fgcolor: str = "#000",
    ):
        from airflow.models.dag import DagContext

        if group_id is None:
            # This creates a root TaskGroup.
            if parent_group:
                raise AirflowException("Root TaskGroup cannot have parent_group")
            # used_group_ids is shared across all TaskGroups in the same DAG to keep track
            # of used group_id to avoid duplication.
            self.used_group_ids: Set[Optional[str]] = set()
        else:
            if not isinstance(group_id, str):
                raise ValueError("group_id must be str")
            if not group_id:
                raise ValueError("group_id must not be empty")

            dag = dag or DagContext.get_current_dag()

            if not parent_group and not dag:
                raise AirflowException("TaskGroup can only be used inside a dag")

            parent_group = parent_group or TaskGroupContext.get_current_task_group(dag)
            self.used_group_ids = parent_group.used_group_ids  # type: ignore

        if group_id in self.used_group_ids:
            raise DuplicateTaskIdFound(f"group_id '{group_id}' has already been added to the DAG")
        self._group_id = group_id
        self.used_group_ids.add(self._group_id)
        self.used_group_ids.add(self.downstream_join_id)
        self.used_group_ids.add(self.upstream_join_id)
        self.children: Dict[str, Union["BaseOperator", "TaskGroup"]] = {}
        if parent_group:
            parent_group.add(self)

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
    def create_root(cls, dag: "DAG"):
        """
        Create a root TaskGroup with no group_id or parent.
        """
        return cls(group_id=None, dag=dag)

    @property
    def is_root(self):
        """
        Returns True if this TaskGroup is the root TaskGroup. Otherwise False
        """
        return not self.group_id

    def __iter__(self):
        for child in self.children.values():
            if isinstance(child, TaskGroup):
                for inner_task in child:
                    yield inner_task
            else:
                yield child

    def add(self, task: Union["BaseOperator", "TaskGroup"]) -> None:
        """
        Add a task to this TaskGroup.
        """
        key = task.group_id if isinstance(task, TaskGroup) else task.task_id

        if key in self.children:
            raise DuplicateTaskIdFound(f"Task id '{key}' has already been added to the DAG")

        if isinstance(task, TaskGroup):
            if task.children:
                raise AirflowException("Cannot add a non-empty TaskGroup")

        self.children[key] = task  # type: ignore

    @property
    def group_id(self) -> Optional[str]:
        """
        group_id of this TaskGroup.
        """
        return self._group_id

    def _set_relative(
            self,
            task_or_task_list: Union['BaseOperator', Sequence['BaseOperator'], "TaskGroup"],
            upstream: bool = False
    ) -> None:
        """
        Call set_upstream/set_downstream for all root/leaf tasks within this TaskGroup.
        Update upstream_group_ids/downstream_group_ids/upstream_task_ids/downstream_task_ids.
        """
        from airflow.models.baseoperator import BaseOperator

        if upstream:
            for task in self.get_roots():
                task.set_upstream(task_or_task_list)
        else:
            for task in self.get_leaves():
                task.set_downstream(task_or_task_list)

        # Update upstream_group_ids/downstream_group_ids/upstream_task_ids/downstream_task_ids
        # accordingly so that we can reduce the number of edges when displaying Graph View.
        if isinstance(task_or_task_list, TaskGroup):
            # Handles TaskGroup and TaskGroup
            if upstream:
                parent, child = (self, task_or_task_list)
            else:
                parent, child = (task_or_task_list, self)

            parent.upstream_group_ids.add(child.group_id)
            child.downstream_group_ids.add(parent.group_id)
        else:
            # Handles TaskGroup and task or list of tasks
            try:
                task_list = list(task_or_task_list)  # type: ignore
            except TypeError:
                task_list = [task_or_task_list]  # type: ignore

            for task in task_list:
                if not isinstance(task, BaseOperator):
                    raise AirflowException("Relationships can only be set between TaskGroup or operators; "
                                           f"received {task.__class__.__name__}")

                if upstream:
                    self.upstream_task_ids.add(task.task_id)
                else:
                    self.downstream_task_ids.add(task.task_id)

    def set_downstream(
        self, task_or_task_list: Union['BaseOperator', Sequence['BaseOperator'], "TaskGroup"]
    ) -> None:
        """
        Set a TaskGroup/task/list of task downstream of this TaskGroup.
        """
        self._set_relative(task_or_task_list, upstream=False)

    def set_upstream(
        self, task_or_task_list: Union['BaseOperator', Sequence['BaseOperator'], "TaskGroup"]
    ) -> None:
        """
        Set a TaskGroup/task/list of task upstream of this TaskGroup.
        """
        self._set_relative(task_or_task_list, upstream=True)

    def __enter__(self):
        TaskGroupContext.push_context_managed_task_group(self)
        return self

    def __exit__(self, _type, _value, _tb):
        TaskGroupContext.pop_context_managed_task_group()

    def has_task(self, task: "BaseOperator") -> bool:
        """
        Returns True if this TaskGroup or its children TaskGroups contains the given task.
        """
        if task.task_id in self.children:
            return True

        return any(child.has_task(task) for child in self.children.values() if isinstance(child, TaskGroup))

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

    def __rshift__(self, other):
        """
        Implements Self >> Other == self.set_downstream(other)
        """
        self.set_downstream(other)
        return other

    def __lshift__(self, other):
        """
        Implements Self << Other == self.set_upstream(other)
        """
        self.set_upstream(other)
        return other

    def __rrshift__(self, other):
        """
        Called for Operator >> [Operator] because list don't have
        __rshift__ operators.
        """
        self.__lshift__(other)
        return self

    def __rlshift__(self, other):
        """
        Called for Operator << [Operator] because list don't have
        __lshift__ operators.
        """
        self.__rshift__(other)
        return self

    @property
    def upstream_join_id(self):
        """
        If this TaskGroup has immediate upstream TaskGroups or tasks, a dummy node called
        upstream_join_id will be created in Graph View to join the outgoing edges from this
        TaskGroup to reduce the total number of edges needed to be displayed.
        """
        return f"{self.group_id}_upstream_join_id"

    @property
    def downstream_join_id(self):
        """
        If this TaskGroup has immediate downstream TaskGroups or tasks, a dummy node called
        downstream_join_id will be created in Graph View to join the outgoing edges from this
        TaskGroup to reduce the total number of edges needed to be displayed.
        """
        return f"{self.group_id}_downstream_join_id"

    def get_task_group_dict(self) -> Dict[str, "TaskGroup"]:
        """
        Returns a flat dictionary of group_id: TaskGroup
        """
        task_group_map = {}

        def build_map(task_group):
            if not isinstance(task_group, TaskGroup):
                return

            task_group_map[task_group.group_id] = task_group

            for child in task_group.children.values():
                build_map(child)

        build_map(self)
        return task_group_map


class TaskGroupContext:
    """
    TaskGroup context is used to keep the current TaskGroup when TaskGroup is used as ContextManager.
    """

    _context_managed_task_group: Optional[TaskGroup] = None
    _previous_context_managed_task_groups: List[TaskGroup] = []

    @classmethod
    def push_context_managed_task_group(cls, task_group: TaskGroup):
        """
        Push a TaskGroup into the list of managed TaskGroups.
        """
        if cls._context_managed_task_group:
            cls._previous_context_managed_task_groups.append(cls._context_managed_task_group)
        cls._context_managed_task_group = task_group

    @classmethod
    def pop_context_managed_task_group(cls) -> Optional[TaskGroup]:
        """
        Pops the last TaskGroup from the list of manged TaskGroups and update the current TaskGroup.
        """
        old_task_group = cls._context_managed_task_group
        if cls._previous_context_managed_task_groups:
            cls._context_managed_task_group = cls._previous_context_managed_task_groups.pop()
        else:
            cls._context_managed_task_group = None
        return old_task_group

    @classmethod
    def get_current_task_group(cls, dag) -> Optional[TaskGroup]:
        """
        Get the current TaskGroup.
        """
        from airflow.models.dag import DagContext

        if not cls._context_managed_task_group:
            dag = dag or DagContext.get_current_dag()
            if dag:
                # If there's currently a DAG but no TaskGroup, return the root TaskGroup of the dag.
                return dag.task_group

        return cls._context_managed_task_group
