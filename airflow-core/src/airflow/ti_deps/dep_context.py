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
from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING
from uuid import UUID

import attr
from sqlalchemy import select

from airflow.exceptions import TaskNotFound
from airflow.utils.state import State

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session

    from airflow.models.dagrun import DagRun
    from airflow.models.task_coordinates import TaskCoordinateResolver
    from airflow.models.taskinstance import TaskInstance


@attr.define
class DepContext:
    """
    A base class for dependency contexts.

    Specifies which dependencies should be evaluated in the context for a task
    instance to satisfy the requirements of the context. Also stores state
    related to the context that can be used by dependency classes.

    For example there could be a SomeRunContext that subclasses this class which has
    dependencies for:

    - Making sure there are slots available on the infrastructure to run the task instance
    - A task-instance's task-specific dependencies are met (e.g. the previous task
      instance completed successfully)
    - ...

    :param deps: The context-specific dependencies that need to be evaluated for a
        task instance to run in this execution context.
    :param flag_upstream_failed: This is a hack to generate the upstream_failed state
        creation while checking to see whether the task instance is runnable. It was the
        shortest path to add the feature. This is bad since this class should be pure (no
        side effects).
    :param ignore_all_deps: Whether or not the context should ignore all ignorable
        dependencies. Overrides the other ignore_* parameters
    :param ignore_depends_on_past: Ignore depends_on_past parameter of DAGs (e.g. for
        Backfills)
    :param wait_for_past_depends_before_skipping: Wait for past depends before marking the ti as skipped
    :param ignore_in_retry_period: Ignore the retry period for task instances
    :param ignore_in_reschedule_period: Ignore the reschedule period for task instances
    :param ignore_unmapped_tasks: Ignore errors about mapped tasks not yet being expanded
    :param ignore_task_deps: Ignore task-specific dependencies such as depends_on_past and
        trigger rule
    :param ignore_ti_state: Ignore the task instance's previous failure/success
    :param finished_tis: A list of all the finished task instances of this run
    """

    deps: set = attr.ib(factory=set)
    flag_upstream_failed: bool = False
    ignore_all_deps: bool = False
    ignore_depends_on_past: bool = False
    wait_for_past_depends_before_skipping: bool = False
    ignore_in_retry_period: bool = False
    ignore_in_reschedule_period: bool = False
    ignore_task_deps: bool = False
    ignore_ti_state: bool = False
    ignore_unmapped_tasks: bool = False
    finished_tis: list[TaskInstance] | None = None
    description: str | None = None

    have_changed_ti_states: bool = False
    """Have any of the TIs state's been changed as a result of evaluating dependencies"""

    upstream_task_id_counts: dict[tuple[str, str, frozenset[str | UUID]], list[tuple[str, int]]] = attr.ib(
        factory=dict, repr=False
    )
    """
    Per-pass memo of the trigger-rule upstream task-instance counts, keyed by
    ``(dag_id, run_id, frozenset of direct-upstream task ids or region ids)``.

    Only populated for the "simple" case where the count-query predicate is exactly
    ``task_id IN (upstream_ids)`` and is therefore identical for every downstream sharing the same
    direct upstreams; the mapped-task-group case uses per-ti map-index predicates and is not cached.

    Lifetime is one scheduling pass, like ``finished_tis``, but note it is not a snapshot handed in
    by the caller: it is filled in as dependencies are evaluated, and invalidated via
    :meth:`invalidate_upstream_task_id_counts` when a mapped task's instance count changes mid-pass.

    This is deliberately an ``init=True`` field even though callers never pass it. ``attrs.evolve``
    only carries over fields that ``__init__`` accepts, and
    :meth:`~airflow.models.taskinstance.TaskInstance.are_dependencies_met` evolves the context for
    every ``UP_FOR_RESCHEDULE`` task instance. With ``init=False`` those instances would each get a
    fresh empty dict, so they would neither read the memo nor warm it for anything else.
    """
    regional_runs: dict[tuple[str, str], bool] = attr.ib(factory=dict, repr=False)
    producer_tis: dict[tuple[str, str, UUID | None, UUID, int, str], tuple[TaskInstance, ...]] = attr.ib(
        factory=dict, repr=False
    )
    coordinate_resolvers: dict[Session, TaskCoordinateResolver] = attr.ib(factory=dict, repr=False)
    dynamic_dags: dict[int, bool] = attr.ib(factory=dict, repr=False)

    def has_regions(self, ti: TaskInstance, *, session: Session) -> bool:
        from airflow.models.dynamic_region import DynamicRegion

        key = ti.dag_id, ti.run_id
        if key not in self.regional_runs:
            self.regional_runs[key] = self._dag_can_have_regions(ti) and bool(
                session.scalar(
                    select(
                        select(DynamicRegion.id)
                        .where(
                            DynamicRegion.dag_id == ti.dag_id,
                            DynamicRegion.run_id == ti.run_id,
                        )
                        .exists()
                    )
                )
            )
        return self.regional_runs[key]

    def _dag_can_have_regions(self, ti: TaskInstance) -> bool:
        from airflow.models.task_coordinates import enclosing_loop

        dag = getattr(ti.task, "dag", None)
        if dag is None:
            return True
        if id(dag) not in self.dynamic_dags:
            self.dynamic_dags[id(dag)] = any(
                task.get_needs_expansion() or enclosing_loop(task) is not None
                for task in dag.task_dict.values()
            )
        return self.dynamic_dags[id(dag)]

    def coordinate_resolver(self, ti: TaskInstance, *, session: Session) -> TaskCoordinateResolver:
        from airflow.models.task_coordinates import TaskCoordinateResolver

        if session not in self.coordinate_resolvers:
            self.coordinate_resolvers[session] = TaskCoordinateResolver.for_dag(None, session)
        resolver = self.coordinate_resolvers[session]
        resolver.adopt_dag(getattr(ti.task, "dag", None))
        return resolver

    def upstream_tis(self, ti: TaskInstance, task_id: str, *, session: Session) -> tuple[TaskInstance, ...]:
        resolver = self.coordinate_resolver(ti, session=session)
        task = resolver.get_task(ti.dag_id, ti.run_id, ti.task_id, dag_version_id=ti.dag_version_id)
        key = (
            ti.dag_id,
            ti.run_id,
            ti.dag_version_id,
            ti.region_id,
            -1 if task.get_needs_expansion() else ti.region_index,
            task_id,
        )
        if key not in self.producer_tis:
            self.producer_tis[key] = resolver.resolve_dependency(ti, task_id)
        return self.producer_tis[key]

    def ensure_finished_tis(self, dag_run: DagRun, session: Session) -> list[TaskInstance]:
        """
        Ensure finished_tis is populated if it's currently None, which allows running tasks without dag_run.

         :param dag_run: The DagRun for which to find finished tasks
         :return: A list of all the finished tasks of this DAG and logical_date
        """
        if self.finished_tis is None:
            finished_tis = dag_run.get_task_instances(state=State.finished, session=session)
            for ti in finished_tis:
                if getattr(ti, "task", None) is not None or (dag := dag_run.dag) is None:
                    continue
                with contextlib.suppress(TaskNotFound):
                    ti.task = dag.get_task(ti.task_id)
            self.finished_tis = finished_tis
        else:
            finished_tis = self.finished_tis
        return finished_tis

    def invalidate_upstream_task_id_counts(self) -> None:
        """
        Drop the memoized trigger-rule upstream counts.

        Call this whenever a mapped task's instance count changes mid-pass, so a downstream evaluated
        later in the same pass recomputes the count instead of reading a stale one.
        """
        self.upstream_task_id_counts.clear()
        self.producer_tis.clear()
        self.regional_runs.clear()
