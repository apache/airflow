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
"""Branching operators."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

from airflow.sdk.bases.operator import BaseOperator
from airflow.sdk.bases.skipmixin import SkipMixin

if TYPE_CHECKING:
    from airflow.sdk.definitions.context import Context
    from airflow.sdk.types import RuntimeTaskInstanceProtocol


class BranchMixIn(SkipMixin):
    """Utility helper which handles the branching as one-liner."""

    def do_branch(
        self, context: Context, branches_to_execute: str | Iterable[str] | None
    ) -> str | Iterable[str] | None:
        """Implement the handling of branching including logging."""
        self.log.info("Branch into %s", branches_to_execute)
        if branches_to_execute is None:
            self.skip_all_except(context["ti"], None)
        else:
            branch_task_ids = self._expand_task_group_roots(context["ti"], branches_to_execute)
            self.skip_all_except(context["ti"], branch_task_ids)
        return branches_to_execute

    def _expand_task_group_roots(
        self, ti: RuntimeTaskInstanceProtocol, branches_to_execute: str | Iterable[str]
    ) -> Iterable[str]:
        """Expand any task group into its root task ids."""
        if TYPE_CHECKING:
            assert ti.task

        task = ti.task
        dag = task.dag
        if TYPE_CHECKING:
            assert dag

        if isinstance(branches_to_execute, str) or not isinstance(branches_to_execute, Iterable):
            branches_to_execute = [branches_to_execute]

        for branch in branches_to_execute:
            resolved = self._resolve_branch_id(task, dag, branch)
            if resolved in dag.task_group_dict:
                tg = dag.task_group_dict[resolved]
                root_ids = [root.task_id for root in tg.roots]
                self.log.info("Expanding task group %s into %s", tg.group_id, root_ids)
                yield from root_ids
            else:
                yield resolved

    def _resolve_branch_id(self, task, dag, branch: str) -> str:
        """
        Resolve a branch id, allowing relative ids when called from inside a task group.

        If ``branch`` matches a task or task group id as-is, return it unchanged.
        Otherwise, if the caller is inside a prefixed task group, try resolving
        ``branch`` as ``"{group_id}.{branch}"`` so a branch inside group ``g``
        can return ``"path_a"`` instead of ``"g.path_a"``.
        """
        if branch in dag.task_group_dict or branch in dag.task_dict:
            return branch
        tg = getattr(task, "task_group", None)
        if tg is not None and tg.group_id and tg.prefix_group_id:
            candidate = f"{tg.group_id}.{branch}"
            if candidate in dag.task_group_dict or candidate in dag.task_dict:
                self.log.info("Resolving relative branch %r as %r", branch, candidate)
                return candidate
        return branch


class BaseBranchOperator(BaseOperator, BranchMixIn):
    """
    A base class for creating operators with branching functionality, like to BranchPythonOperator.

    Users should create a subclass from this operator and implement the function
    `choose_branch(self, context)`. This should run whatever business logic
    is needed to determine the branch, and return one of the following:
    - A single task_id (as a str)
    - A single task_group_id (as a str)
    - A list containing a combination of task_ids and task_group_ids

    The operator will continue with the returned task_id(s) and/or task_group_id(s), and all other
    tasks directly downstream of this operator will be skipped.
    """

    inherits_from_skipmixin = True

    def choose_branch(self, context: Context) -> str | Iterable[str] | None:
        """
        Abstract method to choose which branch to run.

        Subclasses should implement this, running whatever logic is
        necessary to choose a branch and returning a task_id or list of
        task_ids. If None is returned, all downstream tasks will be skipped.

        :param context: Context dictionary as passed to execute()
        """
        raise NotImplementedError

    def execute(self, context: Context):
        return self.do_branch(context, self.choose_branch(context))
