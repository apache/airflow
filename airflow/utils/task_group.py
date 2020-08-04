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
        self.parent_group = parent_group or TaskGroupContext.get_current_task_group()
        self.group_id = group_id
        if self.parent_group:
            self.parent_group.add(self)
        self.tasks = set()

    def __iter__(self):
        for task in self.tasks:
            if isinstance(task, TaskGroup):
                for inner_task in task:
                    yield inner_task
            else:
                yield task

    def add(self, task):
        """
        Add a task to this TaskGroup.
        """
        self.tasks.add(task)

    @property
    def group_id(self):
        """
        group_id is prefixed with parent group_id if applicable.
        """
        if self.parent_group:
            return f"{self.parent_group.group_id}.{self._group_id}"

        return self._group_id

    @group_id.setter
    def group_id(self, val):
        """
        Setter for group_id.
        """
        self._group_id = val

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
            if not any(parent.is_in_task_group(self) for parent in task.get_direct_relatives(upstream=True)):
                yield task

    def get_leaves(self):
        """
        Returns a generator of tasks that are leaf tasks, i.e. those with no downstream
        dependencies within the TaskGroup
        """
        for task in self:
            if not any(child.is_in_task_group(self) for child in task.get_direct_relatives(upstream=False)):
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
        return cls._context_managed_task_group
