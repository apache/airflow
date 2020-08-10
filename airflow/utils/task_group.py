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
TaskGroup
"""

from typing import List, Optional


class TaskGroup:
    """
    A collection of tasks. Tasks within the TaskGroup have their task_id prefixed with the
    group_id of the TaskGroup. When set_downstream() or set_upstream() are called on the
    TaskGroup, it is applied across all tasks within the group if necessary.
    """
    def __init__(self, group_id, parent_group=None):
        if group_id is None:
            # This creates a root TaskGroup.
            self.parent_group = None
        else:
            if not isinstance(group_id, str):
                raise ValueError("group_id must be str")
            if not group_id:
                raise ValueError("group_id must not be empty")
            self.parent_group = parent_group or TaskGroupContext.get_current_task_group()

        self._group_id = group_id
        if self.parent_group:
            self.parent_group.add(self)
        self.children = {}

    @classmethod
    def create_root(cls):
        """
        Create a root TaskGroup with no group_id or parent.
        """
        return cls(group_id=None)

    @property
    def is_root(self):
        """
        Returns True if this TaskGroup is the root TaskGroup. Otherwise False
        """
        return not self.parent_group

    def __iter__(self):
        for child in self.children.values():
            if isinstance(child, TaskGroup):
                for inner_task in child:
                    yield inner_task
            else:
                yield child

    def add(self, task):
        """
        Add a task to this TaskGroup.
        """
        if task.label in self.children:
            raise ValueError(f"Duplicate label {task.label} in {self.group_id}")
        self.children[task.label] = task

    @property
    def label(self):
        """
        group_id excluding parent's group_id.
        """
        return self._group_id

    @property
    def group_id(self):
        """
        group_id is prefixed with parent group_id if applicable.
        """
        return ".".join(self.group_ids)

    @property
    def group_ids(self):
        """
        Returns all the group_id of nested TaskGroups as a list, starting from the top.
        """
        return list(self._group_ids())[1:]

    def _group_ids(self):
        if self.parent_group:
            for group_id in self.parent_group._group_ids():  # pylint: disable=protected-access
                yield group_id

        yield self._group_id

    def set_downstream(self, task_or_task_list) -> None:
        """
        Call set_downstream for all leaf tasks within this TaskGroup.
        """
        for task in self.get_leaves():
            task.set_downstream(task_or_task_list)

    def set_upstream(self, task_or_task_list) -> None:
        """
        Call set_upstream for all root tasks within this TaskGroup.
        """
        for task in self.get_roots():
            task.set_upstream(task_or_task_list)

    def __enter__(self):
        TaskGroupContext.push_context_managed_task_group(self)
        return self

    def __exit__(self, _type, _value, _tb):
        TaskGroupContext.pop_context_managed_task_group()

    def get_roots(self):
        """
        Returns a generator of tasks that are root tasks, i.e. those with no upstream
        dependencies within the TaskGroup.
        """
        for task in self:
            if not any(parent.is_in_task_group(self.group_ids)
                       for parent in task.get_direct_relatives(upstream=True)):
                yield task

    def get_leaves(self):
        """
        Returns a generator of tasks that are leaf tasks, i.e. those with no downstream
        dependencies within the TaskGroup
        """
        for task in self:
            if not any(child.is_in_task_group(self.group_ids)
                       for child in task.get_direct_relatives(upstream=False)):
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

    @classmethod
    def build_task_group(cls, tasks):
        """
        Put tasks into TaskGroup.
        """
        root = TaskGroup.create_root()
        for task in tasks:
            current = root
            for label in task.task_group_ids:
                if label not in current.children:
                    child_group = TaskGroup(group_id=label, parent_group=current)
                else:
                    child_group = current.children[label]
                current = child_group
            current.add(task)
        return root


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
    def get_current_task_group(cls) -> Optional[TaskGroup]:
        """
        Get the current TaskGroup.
        """
        if not cls._context_managed_task_group:
            return TaskGroup.create_root()
        return cls._context_managed_task_group
