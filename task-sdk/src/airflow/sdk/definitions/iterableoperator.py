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

import asyncio
import copy
import os
from collections import deque
from collections.abc import Iterable, Mapping, Sequence
from functools import cached_property
from itertools import repeat
from typing import TYPE_CHECKING, Any

try:
    # Python 3.11+
    BaseExceptionGroup
except NameError:
    from exceptiongroup import BaseExceptionGroup

from airflow.sdk import BaseXCom, TaskInstanceState, timezone
from airflow.sdk.bases.operator import BaseOperator, DecoratedDeferredAsyncOperator, event_loop
from airflow.sdk.definitions._internal.expandinput import PartitionedExpandInput
from airflow.sdk.definitions.context import clone_context
from airflow.sdk.definitions.mappedoperator import MappedOperator
from airflow.sdk.definitions.xcom_arg import MapXComArg, XComArg  # noqa: F401
from airflow.sdk.exceptions import (
    AirflowFailException,
    AirflowRescheduleTaskInstanceException,
    AirflowTaskTimeout,
    TaskDeferred,
)
from airflow.sdk.execution_time.executor import AsyncAwareExecutor, TaskExecutor
from airflow.sdk.execution_time.task_runner import IndexedTaskInstance

if TYPE_CHECKING:
    import jinja2

    from airflow.providers.standard.triggers.temporal import DateTimeTrigger
    from airflow.sdk.definitions._internal.expandinput import ExpandInput
    from airflow.sdk.definitions.context import Context
    from airflow.sdk.execution_time.lazy_sequence import XComIterable


ExternalDateTimeTrigger: type[DateTimeTrigger] | None

try:
    from airflow.providers.standard.triggers.temporal import DateTimeTrigger as ExternalDateTimeTrigger
except ModuleNotFoundError:
    # If the providers package with DateTimeTrigger is not available (e.g. in
    # minimal installs or tests), set the symbol to None so callers can
    # explicitly check for availability. Using hasattr(self, DateTimeTrigger)
    # is incorrect because hasattr expects a string attribute name.
    ExternalDateTimeTrigger = None


class IterableOperator(BaseOperator):
    """
    Operator used for Dynamic Task Iteration (DTI) that runs a mapped operator over an iterable input.

    The IterableOperator wraps a :class:`MappedOperator` together with an
    :class:`ExpandInput` and is responsible for creating and running the
    per-index runtime task instances. The IterableOperator itself is a
    lightweight, non-retrying wrapper — retries, timeouts and deferred
    execution are handled by the individual indexed task instances that the
    IterableOperator creates for each element produced by the
    ``expand_input``.

    The IterableOperator executes the mapped operator instances using a
    concurrent executor with a configurable number of workers. By default
    the worker count is taken from the mapped operator's ``partial_kwargs``
    (``task_concurrency``) if present, otherwise falls back to
    ``os.cpu_count()`` and finally to ``1``.

    :param operator: The :class:`MappedOperator` to unmap and execute for
        each element of ``expand_input``. Each indexed runtime receives a
        deep copy/unmapped instance of this operator.

    :param expand_input: Provider of the values (or partitions) to iterate
        over. Its ``iter_values(context)`` method is used to produce the
        per-index ``mapped_kwargs`` used to unmap the operator.

    :param kwargs: Additional keyword arguments forwarded to
        :class:`BaseOperator` when instantiating the IterableOperator
        (e.g. ``dag``, ``start_date``). Note that the IterableOperator
        overrides retry-related parameters because retries are managed by
        the per-index tasks.

    :returns: An :class:`XComIterable` if the mapped operator pushes XComs, otherwise ``None``.
    """

    _operator: MappedOperator
    expand_input: ExpandInput
    partial_kwargs: dict[str, Any]
    shallow_copy_attrs: Sequence[str] = (
        "_operator",
        "expand_input",
        "partial_kwargs",
        "_log",
    )

    def __init__(
        self,
        *,
        operator: MappedOperator,
        expand_input: ExpandInput,
        **kwargs,
    ):
        super().__init__(
            **{
                **kwargs,
                "task_id": operator.task_id,
                "owner": operator.owner,
                "email": operator.email,
                "email_on_retry": operator.email_on_retry,
                "email_on_failure": operator.email_on_failure,
                "retries": 0,  # We should not retry the IterableOperator, only the indexed runtime ti's should be retried
                "retry_delay": operator.retry_delay,
                "retry_exponential_backoff": operator.retry_exponential_backoff,
                "max_retry_delay": operator.max_retry_delay,
                "start_date": operator.start_date,
                "end_date": operator.end_date,
                "depends_on_past": operator.depends_on_past,
                "ignore_first_depends_on_past": operator.ignore_first_depends_on_past,
                "wait_for_past_depends_before_skipping": operator.wait_for_past_depends_before_skipping,
                "wait_for_downstream": operator.wait_for_downstream,
                "dag": operator.dag,
                "priority_weight": operator.priority_weight,
                "queue": operator.queue,
                "pool": operator.pool,
                "pool_slots": operator.pool_slots,
                "execution_timeout": None,
                "trigger_rule": operator.trigger_rule,
                "resources": operator.resources,
                "run_as_user": operator.run_as_user,
                "map_index_template": operator.map_index_template,
                "max_active_tis_per_dag": operator.max_active_tis_per_dag,
                "max_active_tis_per_dagrun": operator.max_active_tis_per_dagrun,
                "executor": operator.executor,
                "executor_config": operator.executor_config,
                "inlets": operator.inlets,
                "outlets": operator.outlets,
                "task_group": operator.task_group,
                "doc": operator.doc,
                "doc_md": operator.doc_md,
                "doc_json": operator.doc_json,
                "doc_yaml": operator.doc_yaml,
                "doc_rst": operator.doc_rst,
                "task_display_name": operator.task_display_name,
                "allow_nested_operators": operator.allow_nested_operators,
            }
        )
        self._operator = operator
        self.expand_input = expand_input
        self.partial_kwargs = operator.partial_kwargs or {}
        self.max_workers = self.partial_kwargs.pop("task_concurrency", None) or os.cpu_count() or 1
        self._number_of_tasks: int = 0
        XComArg.apply_upstream_relationship(self, self.expand_input.value)

    @property
    def task_type(self) -> str:
        """@property: type of the task."""
        return self._operator.__class__.__name__

    @cached_property
    def timeout(self) -> float | None:
        if self._operator.execution_timeout:
            return self._operator.execution_timeout.total_seconds()
        return None

    def _do_render_template_fields(
        self,
        parent: Any,
        template_fields: Iterable[str],
        context: Context,
        jinja_env: jinja2.Environment,
        seen_oids: set[int],
    ) -> None:
        # IterableOperator doesn't need to render template fields as the actual operator's template fields
        # will be rendered in the TaskExecutor when running each mapped task instance.
        pass

    def _get_specified_expand_input(self) -> ExpandInput:
        return self.expand_input

    def _unmap_operator(
        self, context: Context, mapped_kwargs: Context, jinja_env: jinja2.Environment
    ) -> BaseOperator:
        from airflow.sdk.execution_time.context import context_update_for_unmapped

        self._number_of_tasks += 1
        unmapped_task = self._operator.unmap(mapped_kwargs)
        # Make sure deferred operators will always raise a DeferredTask exception when executed
        unmapped_task.start_from_trigger = False
        context_update_for_unmapped(context, unmapped_task)

        unmapped_task._do_render_template_fields(
            parent=unmapped_task,
            template_fields=self._operator.template_fields,
            context=context,
            jinja_env=jinja_env,
            seen_oids=set(),
        )
        return unmapped_task

    async def _xcom_push(self, task: IndexedTaskInstance, value: Any) -> None:
        if task.xcom_pushed:
            self.log.debug(
                "XCom already pushed for task_id %s with index %s",
                task.task_id,
                task.index,
            )
        else:
            self.log.debug(
                "Pushing XCom for task_id %s with index %s",
                task.task_id,
                task.index,
            )

            await task.axcom_push(key=BaseXCom.XCOM_RETURN_KEY, value=value)

    def _run_tasks(
        self,
        context: Context,
        tasks: Iterable[IndexedTaskInstance],
    ) -> XComIterable | None:
        exceptions: list[BaseException] = []
        reschedule_date = timezone.utcnow()
        deferred_tasks: deque[IndexedTaskInstance] = deque()
        failed_tasks: deque[IndexedTaskInstance] = deque()
        do_xcom_push = True

        self.log.info("Running tasks with %d workers", self.max_workers)

        while True:
            with event_loop() as loop:
                with AsyncAwareExecutor(loop=loop, max_workers=self.max_workers) as executor:
                    for task, result, raised in executor.map(
                        self._run_task,
                        repeat(executor),
                        repeat(context),
                        tasks,
                        timeout=self.timeout,
                    ):
                        do_xcom_push = task.do_xcom_push

                        if raised is None:
                            self.log.debug("result: %s", result)
                            continue

                        if isinstance(raised, TaskDeferred):
                            operator = DecoratedDeferredAsyncOperator(
                                operator=task.task, task_deferred=raised
                            )
                            deferred_tasks.append(
                                self._create_mapped_task(
                                    run_id=task.run_id,
                                    index=task.index,
                                    map_index=task.map_index,  # type: ignore[arg-type]
                                    try_number=task.try_number,
                                    operator=operator,
                                )
                            )
                            continue

                        if isinstance(raised, asyncio.TimeoutError):
                            self.log.warning("A timeout occurred for task_id %s", task.task_id)
                            if task.next_try_number > (self.retries or 0):
                                exceptions.append(AirflowTaskTimeout(raised))
                            else:
                                reschedule_date = min(reschedule_date, task.next_retry_datetime())
                                failed_tasks.append(task)
                            continue

                        if isinstance(raised, AirflowRescheduleTaskInstanceException):
                            reschedule_date = min(reschedule_date, raised.reschedule_date)
                            self.log.exception(
                                "An exception occurred for task_id %s with index %s, it has been rescheduled at %s",
                                task.task_id,
                                task.index,
                                reschedule_date,
                            )
                            failed_tasks.append(raised.task)
                            continue

                        self.log.exception(
                            "An exception occurred for task_id %s with index %s",
                            task.task_id,
                            task.index,
                            exc_info=raised,
                        )
                        exceptions.append(raised)

                    # Deferred tasks are re-fed as a new pass once the current batch completes,
                    # because the event loop is restarted at the top of the outer while loop.
                    if deferred_tasks:
                        tasks = list(deferred_tasks)
                        deferred_tasks.clear()
                        continue

            if not failed_tasks:
                if exceptions:
                    # If this IterableOperator is backed by a partitioned expand input
                    # (created from a MappedIterableOperator), the parent mapped
                    # task should never be retried; retries are handled by the
                    # individual indexed runtime tasks. In that case raise
                    # AirflowFailException to mark failure without retrying the
                    # parent TaskInstance. For regular (non-partitioned) IterableOperator
                    # behavior, preserve the previous behavior and raise the
                    # BaseExceptionGroup so callers/tests that expect it keep working.
                    if isinstance(self.expand_input, PartitionedExpandInput):
                        raise AirflowFailException(f"Multiple sub-task failures: {exceptions}")
                    raise BaseExceptionGroup("Multiple sub-task failures", exceptions)
                if do_xcom_push:
                    from airflow.sdk.execution_time.lazy_sequence import XComIterable

                    return XComIterable(
                        task_id=self.task_id,
                        dag_id=self.dag_id,
                        run_id=context["run_id"],
                        length=self._number_of_tasks,
                        map_index=context["ti"].map_index,
                    )
                return None

            # If the retry time is still in the future we defer the operator so the worker
            # slot is released. If the retry time has already passed we immediately re-run
            # the failed tasks without deferring.
            if reschedule_date > timezone.utcnow():
                if ExternalDateTimeTrigger is not None:
                    self.defer(
                        trigger=ExternalDateTimeTrigger(reschedule_date),
                        method_name=self.execute_failed_tasks.__name__,
                        kwargs={
                            "failed_tasks": {failed_task.index for failed_task in failed_tasks},
                            "try_number": next(iter(failed_tasks)).try_number,
                        },
                    )
                else:
                    self.log.warning(
                        "DateTimeTrigger is not available; failed tasks cannot be rescheduled at %s and will be retried immediately",
                        reschedule_date,
                    )

            tasks = list(failed_tasks)
            failed_tasks.clear()
            exceptions.clear()
            reschedule_date = timezone.utcnow()

    async def _run_task(
        self,
        executor: AsyncAwareExecutor,
        context: Context,
        task: IndexedTaskInstance,
    ) -> tuple[IndexedTaskInstance, Any | None, BaseException | None]:
        try:
            if task.is_async:
                result = await self._run_async_operator(context, task)
            else:
                result = await executor.run_sync(self._run_operator, context, task)

            # Push XCom asynchronously (non-blocking)
            if result is not None and task.do_xcom_push:
                await self._xcom_push(task, result)

            return task, result, None
        except BaseException as e:
            return task, None, e

    def _run_operator(self, context: Context, task_instance: IndexedTaskInstance):
        with TaskExecutor(task_instance=task_instance) as executor:
            return executor.run(
                context={
                    **clone_context(context),
                    **{
                        "ti": task_instance,
                        "task_instance": task_instance,
                    },
                }
            )

    async def _run_async_operator(self, context: Context, task_instance: IndexedTaskInstance):
        async with TaskExecutor(task_instance=task_instance) as executor:
            return await executor.arun(
                context={
                    **clone_context(context),
                    **{
                        "ti": task_instance,
                        "task_instance": task_instance,
                    },
                }
            )

    def _create_task(
        self,
        context: Context,
        index: int,
        mapped_kwargs: Context,
        jinja_env: jinja2.Environment,
        try_number: int = 0,
    ) -> IndexedTaskInstance:
        run_id = context["ti"].run_id
        map_index = context["ti"].map_index
        operator = self._unmap_operator(context.copy(), mapped_kwargs, jinja_env)
        return self._create_mapped_task(
            run_id=run_id, map_index=map_index, index=index, try_number=try_number, operator=operator
        )

    def _create_mapped_task(
        self, run_id: str, map_index: int | None, index: int, try_number: int, operator: BaseOperator
    ) -> IndexedTaskInstance:
        return IndexedTaskInstance.model_construct(
            task_id=operator.task_id,
            dag_id=operator.dag_id,
            run_id=run_id,
            map_index=map_index,
            index=index,
            max_tries=operator.retries,
            start_date=self.start_date,
            state=TaskInstanceState.SCHEDULED.value,
            is_mapped=True,
            task=operator,
            try_number=try_number,
            xcom_pushed=False,
        )

    def execute(self, context: Context):
        jinja_env = self.get_template_env(dag=self.dag)
        tasks = (
            self._create_task(
                context=context,
                index=index,
                mapped_kwargs=value,
                jinja_env=jinja_env,
            )
            for index, value in enumerate(self.expand_input.iter_values(context=context))
        )
        return self._run_tasks(context=context, tasks=tasks)

    def execute_failed_tasks(
        self,
        context: Context,
        try_number: int,
        failed_tasks: set[int],
        event: dict[Any, Any],
    ):
        jinja_env = self.get_template_env(dag=self.dag)
        tasks = (
            self._create_task(
                context=context,
                index=index,
                try_number=try_number,
                jinja_env=jinja_env,
                mapped_kwargs=value,
            )
            for index, value in enumerate(self.expand_input.iter_values(context=context))
            if index in failed_tasks
        )
        return self._run_tasks(context=context, tasks=tasks)


class MappedIterableOperator(MappedOperator):
    """A thin wrapper around an existing MappedOperator that unmaps an MappedOperator within an IterableOperator."""

    def __init__(
        self,
        mapped_operator: MappedOperator,
        expand_input: ExpandInput,
        partition_size: int,
    ):
        self.delegate = mapped_operator
        self.delegate.partial_kwargs["partition_size"] = partition_size
        self.expand_input = expand_input
        self._register_with_dag = True
        self.__attrs_post_init__()

    def __getattr__(self, name):
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(f"{type(self).__name__!r} object has no attribute {name!r}")
        return getattr(self.delegate, name)

    def prepare_for_execution(self) -> MappedOperator:
        return self

    @property
    def partition_size(self) -> int:
        return self.delegate.partial_kwargs.get("partition_size", 0)

    @property
    def retries(self) -> int:
        return 0  # We should not retry the IterableOperator, only the indexed runtime ti's should be retried

    @retries.setter
    def retries(self, value: int) -> None:
        if value != 0:
            raise ValueError(
                "MappedIterableOperator always has retries=0; retries are handled by indexed tasks."
            )

    def __repr__(self):
        return f"<MappedIterable({self.task_type}): {self.task_id}>"

    def unmap(self, resolve: Mapping[str, Any]) -> BaseOperator:
        return IterableOperator(
            operator=copy.deepcopy(self.delegate),
            expand_input=PartitionedExpandInput(self.expand_input, self.partition_size),
            _airflow_from_mapped=True,
        )
