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
import hashlib
import json
import os
import threading
import warnings
from collections.abc import AsyncIterable, AsyncIterator, Iterable, Mapping, Sequence
from functools import partial
from typing import TYPE_CHECKING, Any

try:
    # Python 3.11+
    BaseExceptionGroup
except NameError:
    from exceptiongroup import BaseExceptionGroup

from airflow.sdk import BaseXCom, TaskInstanceState, TriggerRule
from airflow.sdk.bases.operator import BaseAsyncOperator, BaseOperator, event_loop
from airflow.sdk.bases.skipmixin import SkipMixin
from airflow.sdk.bases.xcom import XComIterable
from airflow.sdk.definitions.asset import Asset, AssetAlias, AssetAliasEvent, AssetUniqueKey
from airflow.sdk.definitions.retry_policy import RetryAction, RetryDecision
from airflow.sdk.definitions.xcom_arg import XComArg
from airflow.sdk.exceptions import (
    AirflowFailException,
    AirflowRescheduleException,
    AirflowSensorTimeout,
    AirflowSkipException,
    AirflowTaskTerminated,
    AirflowTaskTimeout,
    DagRunTriggerException,
    DownstreamTasksSkipped,
    TaskDeferred,
)
from airflow.sdk.execution_time.comms import DeadlockImminentError
from airflow.sdk.execution_time.context import OutletEventAccessors, context_update_for_unmapped
from airflow.sdk.execution_time.executor import AsyncAwareExecutor
from airflow.sdk.execution_time.task_runner import (
    IndexedTaskInstance,
    IndexedTaskRunner,
    IndexedTaskState,
    _push_xcom_if_needed,
)
from airflow.sdk.serde import serialize

if TYPE_CHECKING:
    import jinja2

    from airflow.sdk.definitions._internal.expandinput import ExpandInput, Resolved
    from airflow.sdk.definitions.context import Context
    from airflow.sdk.definitions.mappedoperator import MappedOperator
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
    from airflow.sdk.types import OutletEventAccessorsProtocol


# The trigger rules under which one skipped upstream task instance skips a task, whatever the other
# upstream task instances did (see TriggerRuleDep). A skipped iteration has the same effect on a
# downstream task with one of these rules as a skipped mapped task instance would.
SKIPPED_WITH_A_SKIPPED_UPSTREAM = frozenset(
    {TriggerRule.ALL_SUCCESS, TriggerRule.NONE_SKIPPED, TriggerRule.ALL_DONE_MIN_ONE_SUCCESS}
)


# Raised by an item, these fail the task without a retry, as the runner does for a task that raises
# them itself (see _run_task_and_map_outcome and _handle_handler_failure).
FAIL_WITHOUT_RETRY = (AirflowFailException, AirflowSensorTimeout, AirflowTaskTerminated)

# How strongly a retry policy decision speaks for the task when several items failed: one item the
# policy says must not be retried fails the task, one it says to retry makes it retry on its terms.
_DECISION_WEIGHT = {RetryAction.FAIL: 2, RetryAction.RETRY: 1, RetryAction.DEFAULT: 0}


def refuse_operators_that_skip_downstream(operator: MappedOperator) -> None:
    """
    Refuse to iterate an operator that can skip downstream tasks.

    An iteration has no downstream tasks of its own, so ``ShortCircuitOperator``, the branch
    operators and any other ``SkipMixin`` would skip nothing and let every downstream task run.
    Checked on the class: ``MappedOperator._can_skip_downstream`` is only derived from ``SkipMixin``
    on the classic path, while the ``@task`` path copies a class default that is ``False`` even for
    ``@task.short_circuit`` and ``@task.branch``.
    """
    if issubclass(operator.operator_class, SkipMixin):
        raise TypeError(
            f"{operator.operator_name} can skip downstream tasks and cannot be iterated: an iteration "
            f"of {operator.task_id!r} has no downstream tasks of its own, so it would skip nothing and "
            "every downstream task would run. Use .expand() for it instead."
        )


def _unprefixed_task_id(operator: MappedOperator) -> str:
    """
    Return the wrapped operator's task id without its task group's prefix.

    ``partial()`` already gave the wrapped operator the prefixed id, and ``BaseOperator.__init__``
    prefixes the id it gets once more, since an IterableOperator is not built from a mapped
    operator. Handing it the bare id keeps the two equal. The same rule as ``label``, which cannot
    be used here because it returns the display name when there is one.
    """
    task_group = operator.task_group
    if task_group and task_group.node_id and task_group.prefix_group_id:
        return operator.task_id[len(task_group.node_id) + 1 :]
    return operator.task_id


def _fingerprint(mapped_kwargs: Mapping[str, Any]) -> str | None:
    """
    Digest one sub-task's input, stored on its checkpoint to tell whether the checkpoint still applies.

    A retry may run on another input than the attempt that wrote the checkpoints: the upstream was
    cleared together with this task and produced other items. An index then no longer means the
    same work, and replaying its result would hand downstream a value computed from the old item.
    An input serde cannot serialize has no digest, and its checkpoint is honoured by index alone.
    """
    try:
        serialized = json.dumps(serialize(mapped_kwargs), sort_keys=True)
    except (TypeError, ValueError, AttributeError, RecursionError):
        return None
    return hashlib.sha256(serialized.encode()).hexdigest()


def _partial_inputs_from_upstream(
    partial_kwargs: Mapping[str, Any], unmapped_task: BaseOperator
) -> dict[str, Any]:
    """
    Collect the rendered values of the partial kwargs an upstream task provides.

    They belong in the fingerprint next to the iterated kwargs: clearing the upstream together with
    this task can change them while the items stay the same, and a checkpoint written with the old
    value must not be replayed. Only XComArg values count, read back from the unmapped operator
    once rendered, at the top level or inside a mapping such as a ``@task``'s ``op_kwargs``. Other
    templated values are left out on purpose: one like ``{{ ti.try_number }}`` changes with every
    attempt and would make every checkpoint look stale.
    """
    inputs: dict[str, Any] = {}
    for key, value in partial_kwargs.items():
        if isinstance(value, XComArg):
            inputs[key] = getattr(unmapped_task, key, None)
        elif isinstance(value, Mapping):
            rendered = getattr(unmapped_task, key, None)
            for name, nested in value.items():
                if isinstance(nested, XComArg):
                    inputs[f"{key}.{name}"] = rendered.get(name) if isinstance(rendered, Mapping) else None
    return inputs


def _serialize_outlet_events(accessors: OutletEventAccessors) -> list[dict[str, Any]]:
    """
    Snapshot the outlet asset events one sub-task recorded into a JSON-safe list.

    Persisted on the sub-task's checkpoint so a later attempt can replay them via
    ``_replay_outlet_events`` when the sub-task is skipped because it already succeeded.
    """
    events: list[dict[str, Any]] = []
    for _asset_or_alias, accessor in accessors.items():
        if isinstance(accessor.key, AssetUniqueKey):
            events.append(
                {
                    "kind": "asset",
                    "name": accessor.key.name,
                    "uri": accessor.key.uri,
                    "extra": accessor.extra,
                    "partition_keys": sorted(accessor.partition_keys),
                }
            )
        for alias_event in accessor.asset_alias_events:
            events.append(
                {
                    "kind": "asset_alias",
                    "source_alias_name": alias_event.source_alias_name,
                    "dest_asset_key": {
                        "name": alias_event.dest_asset_key.name,
                        "uri": alias_event.dest_asset_key.uri,
                    },
                    "dest_asset_extra": alias_event.dest_asset_extra,
                    "extra": alias_event.extra,
                }
            )
    return events


def _merge_outlet_events(target: OutletEventAccessorsProtocol, source: OutletEventAccessors) -> None:
    """
    Merge every outlet asset event recorded in ``source`` into ``target``.

    Used both to fold a sub-task's isolated accessor into the IterableOperator's shared
    ``context["outlet_events"]`` right after it succeeds, and to replay a checkpointed
    snapshot (via ``_replay_outlet_events``) for a sub-task skipped on retry.

    A task instance sends one event per asset, so items that emit to the same asset end up in
    that one event: ``extra`` keeps what the last item to finish wrote, while partition keys and
    alias events accumulate. ``.expand()`` sends one event per mapped task instance instead; the
    docs page says so in its comparison table.
    """
    for asset_or_alias, accessor in source.items():
        target_accessor = target[asset_or_alias]
        target_accessor.extra.update(accessor.extra)
        target_accessor.asset_alias_events.extend(accessor.asset_alias_events)
        target_accessor.partition_keys.update(accessor.partition_keys)


def _replay_outlet_events(target: OutletEventAccessorsProtocol, events: list[dict[str, Any]]) -> None:
    """
    Re-populate ``target`` with events a sub-task recorded on a previous attempt.

    A sub-task skipped on retry (because it already succeeded) never re-executes, so it never
    re-emits into the fresh ``OutletEventAccessors`` created for the new attempt. The failed
    attempt sent nothing to the server either (outlet events travel only on the success payload),
    so replaying cannot emit an event twice; without it the events would be lost.
    """
    replayed = OutletEventAccessors()
    for event in events:
        if event["kind"] == "asset":
            accessor = replayed[Asset(name=event["name"], uri=event["uri"])]
            accessor.extra.update(event["extra"])
            if event["partition_keys"]:
                accessor.add_partitions(event["partition_keys"])
        else:
            accessor = replayed[AssetAlias(name=event["source_alias_name"])]
            accessor.asset_alias_events.append(
                AssetAliasEvent(
                    source_alias_name=event["source_alias_name"],
                    dest_asset_key=AssetUniqueKey(**event["dest_asset_key"]),
                    dest_asset_extra=event["dest_asset_extra"],
                    extra=event["extra"],
                )
            )
    _merge_outlet_events(target, replayed)


class Checkpoints:
    """
    Decide whether one attempt of an IterableOperator may resume from its per-index checkpoints.

    Checkpoints are only consulted from the second attempt onwards. A completion marker left by a
    previous fully successful run means this attempt follows a manual clear (which raises
    ``max_tries`` but does not reset ``try_number``), so every index must run again: the stale
    ``SUCCESS`` checkpoints are ignored and overwritten. On entry the marker is replaced by the
    attempt the rerun starts at, so that a crash during the rerun resumes from the checkpoints
    written since, and only from those: an index the crashed rerun did not reach still holds its
    checkpoint from before the clear, which must not be replayed. The marker is written again when
    the block exits without an exception, and when every iteration skipped: the task is then
    ``SKIPPED``, a final state, and a clear of it must run every iteration again rather than
    replay the skips, as a cleared mapped task instance would. An attempt that fails, even with
    some iterations skipped, writes no marker, so a retry or a clear after it resumes: iterations
    that succeeded or skipped keep that outcome and only the others run again.

    The checkpoints themselves are never deleted: one marker write costs the same whatever the item
    count, and the store is scoped to the parent task instance, so state a sub-task stored for itself
    is never touched. They expire with the store's default retention (``[state_store]
    default_retention_days``, 30 days unless configured, 0 disables expiry), which also bounds how
    long a task that exhausted its retries keeps them. Keeping them until then is intended: a manual
    clear of such a task resumes from the checkpoints instead of re-running every index, and the
    marker is what tells a clear-after-success apart from that.
    """

    # Same namespace as IndexedTaskState.build_key, for the same reason.
    COMPLETION_KEY = "_iterable_completed"

    def __init__(self, context: Context) -> None:
        self._store = context["task_state_store"]
        self._try_number = context["ti"].try_number
        self.trust_checkpoints = False
        # The attempt from which checkpoints may be resumed; older ones predate a manual clear.
        self.since = 0

    def __enter__(self) -> Checkpoints:
        if self._try_number > 1:
            marker = self._store.get(self.COMPLETION_KEY)
            if isinstance(marker, Mapping) and marker.get("completed"):
                self._store.set(self.COMPLETION_KEY, {"completed": False, "since": self._try_number})
            else:
                self.trust_checkpoints = True
                since = marker.get("since") if isinstance(marker, Mapping) else None
                if isinstance(since, int):
                    self.since = since
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if exc_type is None or issubclass(exc_type, AirflowSkipException):
            self._store.set(self.COMPLETION_KEY, {"completed": True, "try_number": self._try_number})


class IterationState:
    """
    The state of one run of an iterated task, kept apart from the operator's configuration.

    It holds what the run needs to remember while it is going: the sub-operators in flight and
    those already killed, so that :meth:`IterableOperator.on_kill` reaches each one once; the
    stop flag a kill sets, which the executor consults before starting the next item; the
    runners of the items that failed, whose callbacks wait for the task's fate; the resolved
    input; and the retry policy's decision for the exception handed to the runner.

    A deep copy of the operator (``dag.partial_subset``, ``prepare_for_execution``) is another
    task with nothing in flight and no kill pending, so copying the state gives a fresh one. The
    operator starts every run with a fresh one as well.
    """

    def __init__(self) -> None:
        # Keyed by identity: BaseOperator equality compares fields such as task_id, which every
        # sub-operator of one iterated task shares, so a set would hold one of them at most.
        self._in_flight: dict[int, BaseOperator] = {}
        self._lock = threading.Lock()
        # The runner calls on_kill() again after the execution timeout that made _run_tasks call
        # it first, and a sub-operator is killed once.
        self._killed: set[int] = set()
        self._stop_requested = threading.Event()
        self._failed_runners: list[IndexedTaskRunner] = []
        self._decision: tuple[BaseException, RetryDecision] | None = None
        #: The input resolved for this task instance, once ``aresolve`` returned.
        self.resolved: Resolved | None = None

    def __deepcopy__(self, memo: dict[int, Any]) -> IterationState:
        return IterationState()

    def register(self, operator: BaseOperator) -> None:
        """Note that ``operator`` is executing, so a kill reaches it."""
        with self._lock:
            self._in_flight[id(operator)] = operator

    def unregister(self, operator: BaseOperator) -> None:
        """Note that ``operator`` is done, one way or another."""
        with self._lock:
            self._in_flight.pop(id(operator), None)

    def __contains__(self, operator: object) -> bool:
        with self._lock:
            return id(operator) in self._in_flight

    def take_in_flight(self) -> list[BaseOperator]:
        """Return the sub-operators in flight that were not handed out before, and mark them killed."""
        with self._lock:
            operators = [op for key, op in self._in_flight.items() if key not in self._killed]
            self._killed.update(map(id, operators))
        return operators

    def request_stop(self) -> None:
        """Ask the iteration to start nothing else; see :meth:`stop_requested`."""
        self._stop_requested.set()

    def stop_requested(self) -> bool:
        """Whether :meth:`request_stop` was called; passed to the executor as its ``stop``."""
        return self._stop_requested.is_set()

    @property
    def length(self) -> int | None:
        """How many items the resolved input has, or None while it is still being resolved."""
        return self.resolved.length if self.resolved is not None else None

    def note_failed(self, runner: IndexedTaskRunner) -> None:
        """Remember a failed item's runner: its callback waits for the task's fate."""
        self._failed_runners.append(runner)

    @property
    def failed_runners(self) -> tuple[IndexedTaskRunner, ...]:
        """The runners of the items that failed in this run, in the order they failed."""
        return tuple(self._failed_runners)

    def keep_decision(self, exception: BaseException, decision: RetryDecision) -> None:
        """Keep the retry policy's decision for the exception handed to the runner."""
        self._decision = (exception, decision)

    def decision_for(self, exception: BaseException) -> RetryDecision | None:
        """Return the decision kept for ``exception``, if it is the one handed to the runner."""
        if self._decision is not None and self._decision[0] is exception:
            return self._decision[1]
        return None


class IterableOperator(BaseOperator):
    """
    Operator used for Iterable Tasks (IT) that runs a mapped operator over an iterable input.

    The IterableOperator wraps a :class:`MappedOperator` together with an
    :class:`ExpandInput` and is responsible for creating and running the
    per-index runtime task instances. The IterableOperator itself participates
    in Airflow's native retry mechanism — its ``retries`` and ``retry_delay``
    are inherited from the wrapped operator so that when any sub-task needs
    a retry the whole IterableOperator is retried by Airflow. Already-succeeded
    sub-tasks are skipped on each retry attempt because their state is
    checkpointed in the ``task_state_store``.

    The IterableOperator executes the mapped operator instances using a
    concurrent executor with a configurable number of workers. By default
    the worker count is taken from the mapped operator's ``partial_kwargs``
    (``task_concurrency``) if present, otherwise falls back to
    ``os.cpu_count()`` and finally to ``1``. ``os.cpu_count()`` counts the CPUs of
    the machine: a worker in a container with a CPU limit still sees every CPU of
    its node, so set ``task_concurrency`` there.

    **Crash recovery:** When the worker crashes mid-iteration and the task is re-run (e.g. via a
    manual clear), already-succeeded sub-tasks are skipped and only the pending/failed ones are
    executed again. Every sub-task inherits its ``try_number`` from the IterableOperator's own task
    instance, so the attempt count reported to a sub-task matches the attempt Airflow is currently
    running. The checkpoint is only consulted from the second attempt onwards, and solely to decide
    whether an index already succeeded. Once every index has succeeded, a completion marker is written
    so that a *subsequent* manual clear (which does not reset ``try_number``) re-runs every index from
    scratch instead of replaying the previous run's stale results (see :class:`Checkpoints`). A
    checkpoint carries the item's full result (plus its extra XComs and outlet events) and lives in
    the ``task_state_store`` table for the store's retention, unless a ``[state_store]
    state_store_backend`` is configured and only a reference is stored; a custom XCom backend alone
    does not keep large item results out of the metadata database.

    :param operator: The :class:`MappedOperator` to unmap and execute for
        each element of ``expand_input``. Each indexed runtime receives a
        deep copy/unmapped instance of this operator.

    :param expand_input: Provider of the values to iterate
        over. Its ``aresolve(context)`` method gives the item count and the
        per-index ``mapped_kwargs`` used to unmap the operator.

    :param kwargs: Additional keyword arguments forwarded to
        :class:`BaseOperator` when instantiating the IterableOperator
        (e.g. ``dag``, ``start_date``).

    :returns: An :class:`XComIterable` if the mapped operator pushes XComs, otherwise ``None``.

    .. note::
        ``multiple_outputs`` is ignored for iterated tasks. Each sub-task's return value is pushed
        whole as ``return_value_<index>`` and the task's own return value is the ``XComIterable``
        over them, so a ``Mapping`` return annotation on the wrapped ``@task`` does not fan its
        keys out into separate XComs the way it does for ``.expand()``.

    .. note::
        Deferred operators (those that raise :class:`~airflow.sdk.exceptions.TaskDeferred`) are not
        supported yet inside IterableOperator. A ``TaskDeferred`` exception raised by an indexed task
        instance will propagate as an error rather than pausing and resuming the task.

        Reschedule-mode sensors (those that raise :class:`~airflow.sdk.exceptions.AirflowRescheduleException`)
        are also not supported. A reschedule raised by an indexed task instance will fail the whole
        IterableOperator immediately with a clear error rather than being silently mishandled.

        Triggering DAG runs (:class:`~airflow.sdk.exceptions.DagRunTriggerException`, raised by
        ``TriggerDagRunOperator``) and skipping downstream tasks are not supported either: a sub-task
        index has no DAG run or downstream tasks of its own for the trigger/skip to apply to.
        Operators that can skip downstream tasks (``ShortCircuitOperator``, the branch operators,
        ``@task.short_circuit``, ``@task.branch`` and any other ``SkipMixin``) are rejected by
        ``.iterate()`` itself, since inside an iteration they would find nothing to skip and let
        every downstream task run. A trigger, or a
        :class:`~airflow.sdk.exceptions.DownstreamTasksSkipped` raised anyway, fails the whole
        IterableOperator immediately with a clear error rather than silently doing nothing.

        Sub-task outcomes are classified before being aggregated: if any sub-task raises
        :class:`~airflow.sdk.exceptions.AirflowFailException`, that exception is re-raised directly so
        the IterableOperator fails without retrying. A sub-task that raises
        :class:`~airflow.sdk.exceptions.AirflowSkipException` is skipped, as a mapped task instance
        would be: it pushes no XCom, does not fail the task and is not run again on a retry. It is
        left out of the task's :class:`~airflow.sdk.bases.xcom.XComIterable`, so downstream tasks
        only see the values that exist. A direct downstream task whose trigger rule skips it when an
        upstream task instance is skipped (``all_success``, ``none_skipped``,
        ``all_done_min_one_success``) is skipped, as after a mapped upstream; one with a rule such
        as ``none_failed`` runs over the remaining values. If *every* sub-task is skipped, a single
        ``AirflowSkipException`` is re-raised so the IterableOperator itself is marked ``SKIPPED``, and
        so is it over an empty input, as a mapped task over nothing is. All other sub-task exceptions are aggregated
        into a :class:`BaseExceptionGroup` and treated as a regular retryable failure.

    .. warning::
        **Inputs are shared between iterations.**

        All iterations run in one process, so a value handed to several of them is the same object
        in each of them: every value passed through ``.partial()``, and with
        ``.iterate(a=..., b=...)`` every element of ``a`` and of ``b``, which the cross product
        combines more than once. A mapped task instance gets its own copy, because it runs in its
        own process; an iteration does not. Treat inputs as read-only, or copy what the task
        changes in place.

    .. note::
        **Callbacks run per item, and a failed item's wait for the task's fate.**

        ``on_success_callback`` and ``on_skipped_callback`` run as soon as an item succeeds or
        skips, where the item ran: in its worker thread for a sync operator, on the event loop
        for an async one. A failed item's ``on_failure_callback`` or ``on_retry_callback`` runs
        once every item has run, on the thread that ran the iteration, and says what happens to
        the task: retried or failed for good (see :meth:`_report_failed_items`). A failure no item
        owns, such as an error resolving the input, fires no callback: the iterated task has none
        of its own.

    .. note::
        **Pools count the task instance, not its iterations.**

        The scheduler reserves ``pool_slots`` once for the iterated task, while up to
        ``task_concurrency`` iterations run inside it. A pool sized to cap the load on a shared
        resource (database connections, the rate limit of an API) therefore sees one reservation
        for that many concurrent uses. Choose ``task_concurrency`` with the pool in mind; reserving
        slots per iteration needs support in the scheduler, which does not exist yet.

    .. warning::
        **Async sub-tasks must only make async SDK calls.**

        IterableOperator runs multiple async sub-tasks concurrently on the same event loop, each
        making async SDK calls of its own (checkpointing, XCom push). If an async sub-task's
        ``aexecute()`` — or a hook/callback it calls — issues a *synchronous* SDK call instead (e.g.
        ``Variable.get``, ``BaseHook.get_connection``/``get_hook``, ``ti.xcom_pull``, or a sync
        ``on_success_callback``/``pre_execute``), it can collide with another sub-task's async SDK
        call that is concurrently holding the communication lock, which is detected and raised
        eagerly as a non-retryable failure rather than silently deadlocking. Use the async-safe
        equivalents inside async operators: :meth:`~airflow.sdk.bases.hook.BaseHook.aget_connection`/
        ``aget_hook``, ``ti.axcom_pull`` and ``Variable.aget``/``aset``. Sync sub-tasks are not
        concerned: they run in worker threads, their ``execute``, hooks and callbacks included,
        where a synchronous SDK call waits for the lock; so does ``on_kill`` of the sub-operators,
        which :meth:`on_kill` runs off the loop thread.

    .. warning::
        **``execution_timeout`` caps the whole iteration; per-sub-task enforcement is async-only.**

        The IterableOperator keeps the wrapped operator's ``execution_timeout`` as a wall-clock limit
        on the entire task instance. The runner enforces it on the main thread exactly as for any
        other task, so an iteration that overruns fails with ``AirflowTaskTimeout`` and
        :meth:`on_kill` is propagated to every sub-task still in flight. Since ``.iterate()`` runs
        all items in one task instance, this is the per-instance limit of ``.expand()`` applied to
        the whole iteration rather than to each item.

        Per item, only async sub-tasks (instances of :class:`~airflow.sdk.bases.operator.BaseAsyncOperator`)
        are additionally limited, via ``asyncio.wait_for``. Sync sub-tasks run in worker threads and rely
        on :class:`~airflow.sdk.execution_time.timeout.TimeoutPosix`, which requires ``signal.SIGALRM`` and
        only works in the main thread, so no per-item limit applies to them. Use
        :class:`~airflow.sdk.bases.operator.BaseAsyncOperator` if per-sub-task time limits are required.
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
        if operator.get_closest_mapped_task_group() is not None:
            raise NotImplementedError("operator expansion in an expanded task group is not yet supported")
        refuse_operators_that_skip_downstream(operator)

        super().__init__(
            **{
                **kwargs,
                "task_id": _unprefixed_task_id(operator),
                "owner": operator.owner,
                "email": operator.email,
                "email_on_retry": operator.email_on_retry,
                "email_on_failure": operator.email_on_failure,
                "retries": operator.retries,
                "retry_delay": operator.retry_delay,
                "retry_exponential_backoff": operator.retry_exponential_backoff,
                "max_retry_delay": operator.max_retry_delay,
                "retry_policy": operator.retry_policy,
                "start_date": operator.start_date,
                "end_date": operator.end_date,
                "depends_on_past": operator.depends_on_past,
                "ignore_first_depends_on_past": operator.ignore_first_depends_on_past,
                "wait_for_past_depends_before_skipping": operator.wait_for_past_depends_before_skipping,
                "wait_for_downstream": operator.wait_for_downstream,
                "dag": operator.dag,
                "params": operator.params,
                "priority_weight": operator.priority_weight,
                "weight_rule": operator.weight_rule,
                "queue": operator.queue,
                "pool": operator.pool,
                "pool_slots": operator.pool_slots,
                # Kept as the wall-clock cap on the whole iteration, enforced by the runner (see the
                # class docstring); per-item enforcement stays with the sub-tasks.
                "execution_timeout": operator.execution_timeout,
                "trigger_rule": operator.trigger_rule,
                "resources": operator.resources,
                "run_as_user": operator.run_as_user,
                "map_index_template": operator.map_index_template,
                "max_active_tis_per_dag": operator.max_active_tis_per_dag,
                "max_active_tis_per_dagrun": operator.max_active_tis_per_dagrun,
                "executor": operator.executor,
                "executor_config": operator.executor_config,
                "do_xcom_push": operator.partial_kwargs.get("do_xcom_push", True),
                # Ignored for iterated tasks (also when passed explicitly): the return value pushed by the
                # runner is the XComIterable aggregate, not a dict, and every sub-task result is pushed
                # whole under return_value_<index>. The wrapped @task may still infer True from a
                # Mapping return annotation, which would make the runner reject the aggregate.
                "multiple_outputs": False,
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
                # The iterated task has no callbacks and no execute hooks of its own: they run per
                # item, from the wrapped operator's partial kwargs. Passed explicitly, since
                # _apply_defaults would otherwise fill them from the DAG's default_args and the
                # runner would run them for the task on top of the items' own.
                "on_execute_callback": None,
                "on_success_callback": None,
                "on_failure_callback": None,
                "on_retry_callback": None,
                "on_skipped_callback": None,
                "pre_execute": None,
                "post_execute": None,
            }
        )
        self._operator = operator
        self.expand_input = expand_input
        self.partial_kwargs = dict(operator.partial_kwargs) if operator.partial_kwargs else {}
        task_concurrency = self.partial_kwargs.pop("task_concurrency", None)
        if task_concurrency is not None and task_concurrency < 1:
            raise ValueError(f"task_concurrency must be at least 1, got {task_concurrency}")
        # pool_slots is reserved once for the task instance, not per iteration: see the class docstring.
        self.max_workers = task_concurrency if task_concurrency is not None else (os.cpu_count() or 1)
        if operator.execution_timeout and not issubclass(operator.operator_class, BaseAsyncOperator):
            warnings.warn(
                f"Operator {operator.task_id!r} has execution_timeout set, but sync operators run in "
                "worker threads where TimeoutPosix (SIGALRM) cannot be delivered. "
                "It caps the whole iteration but is not enforced per sync sub-task inside IterableOperator. "
                "Use BaseAsyncOperator if per-sub-task time limits are required.",
                UserWarning,
                stacklevel=2,
            )
        # unmap() would normally apply these three flags to each generated sub-operator, and
        # __attrs_post_init__ would apply them (plus the upstream-relationship wiring below) to the
        # MappedOperator itself; since IterableOperator skips __attrs_post_init__ entirely (it isn't a
        # MappedOperator), it must reproduce that part of the contract for its own single DAG node.
        self.is_setup = bool(self.partial_kwargs.get("is_setup", False))
        self.is_teardown = bool(self.partial_kwargs.get("is_teardown", False))
        on_failure_fail_dagrun = self.partial_kwargs.get("on_failure_fail_dagrun", False)
        if on_failure_fail_dagrun:
            self.on_failure_fail_dagrun = on_failure_fail_dagrun
        XComArg.apply_upstream_relationship(self, self.expand_input.value)
        # Mirrors MappedOperator.__attrs_post_init__: partial kwargs corresponding to the wrapped
        # operator's own template fields may themselves be XComArgs (e.g. `.partial(some_field=xcom)`),
        # and those upstream edges must be recorded too, not just the ones from expand_input.
        for key, value in self.partial_kwargs.items():
            if key in self._operator.template_fields:
                XComArg.apply_upstream_relationship(self, value)
        # What one run remembers while it is going (see IterationState); fresh for every run and
        # for every copy of the operator.
        self._state = IterationState()

    def on_kill(self) -> None:
        # The default BaseOperator.on_kill() is a no-op, which would otherwise leave every
        # currently in-flight sub-task unaware that the IterableOperator itself was killed
        # (SIGTERM) or hit its execution_timeout: propagate to each active sub-operator instead.
        # First stop the iteration from starting anything else: the killed items come back as
        # failures and free their slots, which would otherwise be filled with the next items.
        self._state.request_stop()
        active_operators = self._state.take_in_flight()
        if not active_operators:
            return
        # Always in a thread of its own. The runner's SIGTERM handler calls this on the main thread,
        # where the event loop either runs, and a synchronous SDK call in a sub-operator's on_kill
        # would raise DeadlockImminentError, or is paused between two run_until_complete calls
        # while a result is handed to the consumer, and the same call would wait for a lock a
        # parked asend holds, which only the paused loop can release. In its own thread the call
        # waits its turn in both cases, and the loop goes on serving the sub-tasks. _run_tasks
        # kills what is in flight through _kill directly, from a thread the loop drives.
        threading.Thread(
            target=self._kill, args=(active_operators,), name="iterable-operator-on-kill", daemon=True
        ).start()

    def _kill(self, operators: list[BaseOperator]) -> None:
        # One sub-operator's on_kill must not keep the kill from the others: DeadlockImminentError
        # is a BaseException, so it is caught here as a plain error is.
        for operator in operators:
            try:
                operator.on_kill()
            except BaseException:
                self.log.exception("Error calling on_kill() for sub-task operator %s", operator.task_id)

    @property
    def returns_dag_result(self) -> bool:
        return self._operator.returns_dag_result

    @returns_dag_result.setter
    def returns_dag_result(self, value: bool) -> None:
        self._operator.returns_dag_result = value

    @property
    def operator_name(self) -> str:
        # Shown as the wrapped operator (its class name, or a @task callable's custom_operator_name).
        # task_type is not forwarded: it names the class that runs, and what resolves a class from it
        # (the task's class reference, OpenLineage's extractors) would otherwise get the wrapped
        # operator's, whose attributes this one does not have.
        return self._operator.operator_name

    @property
    def task_retries(self) -> int:
        return self._operator.retries or 0

    def _do_render_template_fields(
        self,
        parent: Any,
        template_fields: Iterable[str],
        context: Context,
        jinja_env: jinja2.Environment,
        seen_oids: set[int],
    ) -> None:
        # IterableOperator doesn't need to render template fields as the actual operator's template fields
        # will be rendered in the IndexedTaskRunner when running each mapped task instance.
        pass

    def _get_specified_expand_input(self) -> ExpandInput:
        return self.expand_input

    def _render_unmapped_operator(
        self, context: Context, unmapped_task: BaseOperator, jinja_env: jinja2.Environment
    ) -> None:
        context_update_for_unmapped(context, unmapped_task)

        unmapped_task._do_render_template_fields(
            parent=unmapped_task,
            template_fields=self._operator.template_fields,
            context=context,
            jinja_env=jinja_env,
            seen_oids=set(),
        )

    async def axcom_push(self, task: IndexedTaskInstance, value: Any) -> None:
        await task.axcom_push(key=BaseXCom.XCOM_RETURN_KEY, value=value)

    def _run_tasks(
        self,
        context: Context,
        tasks: AsyncIterable[IndexedTaskInstance],
    ) -> tuple[bool, list[int]]:
        """Run ``tasks`` to completion; return whether they pushed results, and the skipped indices."""
        exceptions: list[Exception] = []
        skipped: dict[int, AirflowSkipException] = {}
        total = 0
        do_xcom_push = True

        self._state = IterationState()
        try:
            self.log.info("Running tasks with %d workers", self.max_workers)

            with Checkpoints(context) as checkpoints:
                with event_loop() as loop:
                    with AsyncAwareExecutor(loop=loop, max_workers=self.max_workers) as executor:
                        try:
                            for task, _result, raised in executor.imap_unordered(
                                partial(
                                    self._run_task,
                                    executor,
                                    context,
                                    trust_checkpoints=checkpoints.trust_checkpoints,
                                    since=checkpoints.since,
                                ),
                                tasks,
                                stop=self._state.stop_requested,
                            ):
                                total += 1
                                do_xcom_push = task.do_xcom_push

                                if raised is None:
                                    continue

                                if isinstance(raised, AirflowSkipException):
                                    skipped[task.index] = raised
                                    continue

                                if isinstance(raised, TaskDeferred):
                                    raise AirflowFailException(
                                        f"Sub-task {task.task_id}[{task.index}] attempted to defer. "
                                        "Deferrable operators are not supported inside IterableOperator."
                                    )

                                if isinstance(raised, (DagRunTriggerException, DownstreamTasksSkipped)):
                                    raise AirflowFailException(
                                        f"Sub-task {task.task_id}[{task.index}] raised "
                                        f"{type(raised).__name__}. Triggering DAG runs "
                                        "(TriggerDagRunOperator) and skipping downstream tasks "
                                        "(ShortCircuitOperator and similar) are not supported inside "
                                        "IterableOperator: the sub-task's index has no downstream "
                                        "tasks or DAG run of its own for the effect to apply to."
                                    ) from raised

                                if isinstance(raised, AirflowRescheduleException):
                                    raise AirflowFailException(
                                        f"Sub-task {task.task_id}[{task.index}] attempted to reschedule "
                                        "(raised AirflowRescheduleException). Reschedule-mode sensors are not "
                                        "supported inside IterableOperator: the sub-task's index has no task "
                                        "instance of its own to reschedule."
                                    ) from raised

                                # Non-Exception BaseExceptions (e.g. DeadlockImminentError,
                                # KeyboardInterrupt, SystemExit) must never be swallowed: they
                                # signal conditions where continuing iteration is meaningless
                                # because every subsequent task would fail for the same reason.
                                # Re-raise immediately to stop iterating over the remaining sub-tasks.
                                if isinstance(raised, DeadlockImminentError):
                                    raise AirflowFailException(
                                        f"Sub-task {task.task_id}[{task.index}] made a synchronous SDK call "
                                        "(e.g. Variable.get, BaseHook.get_connection/get_hook, ti.xcom_pull) on "
                                        "the event loop thread while another sub-task's async SDK call was in "
                                        "flight, which would deadlock the loop, so it is detected and raised "
                                        "eagerly instead. Inside IterableOperator only async sub-tasks run on "
                                        "that thread: an async operator's aexecute(), its pre_execute/"
                                        "post_execute and its callbacks. Use the async-safe equivalents there "
                                        "(e.g. Variable.aget/aset, Hook.aget_connection/aget_hook, "
                                        "ti.axcom_pull); sync sub-tasks and their callbacks run in worker "
                                        "threads, where the same calls wait their turn."
                                    ) from raised
                                if not isinstance(raised, Exception):
                                    raise AirflowFailException(
                                        f"Sub-task {task.task_id}[{task.index}] raised a non-Exception BaseException: "
                                        f"{type(raised).__name__}: {raised}"
                                    ) from raised

                                self.log.exception(
                                    "An exception occurred for task_id %s with index %s",
                                    task.task_id,
                                    task.index,
                                    exc_info=raised,
                                )
                                exceptions.append(raised)
                        except BaseException:
                            # Whatever ends the loop early (the parent's execution_timeout, a failure that
                            # stops the task) is followed by the executor cancelling the coroutines, which
                            # would leave nothing registered for on_kill(); kill what is in flight first,
                            # off the loop thread, so that a sub-operator's synchronous SDK call in on_kill
                            # waits for the sub-tasks' calls in flight instead of raising. Awaited here,
                            # unlike on_kill()'s own thread: the loop keeps running meanwhile.
                            self._state.request_stop()
                            in_flight = self._state.take_in_flight()
                            try:
                                loop.run_until_complete(asyncio.to_thread(self._kill, in_flight))
                            except RuntimeError:
                                self._kill(in_flight)
                            raise

                if self._state.stop_requested():
                    # Killed: nothing started once on_kill() ran, and what was in flight has
                    # finished one way or another. The task fails without a retry, as the runner
                    # treats a terminated task; no completion marker is written, so a later clear
                    # resumes from the checkpoints of the items that did finish. Checked before the
                    # other outcomes: with every killed item returning normally there would be no
                    # failure to raise, and a kill while the input resolves is not an empty input.
                    length = self._state.length
                    raise AirflowTaskTerminated(
                        f"The iterated task was killed: {total} of {length} items ran, the rest never started."
                        if length is not None
                        else "The iterated task was killed while its input was being resolved."
                    ) from (BaseExceptionGroup("Sub-task failures", exceptions) if exceptions else None)
                if exceptions:
                    raise self._failure_for_the_runner(context, exceptions)
                # Nothing to iterate over is skipped, as a mapped task over an empty input is.
                if total == 0:
                    raise AirflowSkipException("The input to iterate over is empty.")
                # If every sub-task was skipped, propagate a single AirflowSkipException so the runner
                # marks the whole IterableOperator SKIPPED.
                if skipped and len(skipped) == total:
                    raise next(iter(skipped.values()))
        except BaseException as raised:
            # Whatever the task ends with decides every failed sub-task's callback, so they agree.
            self._report_failed_items(context, raised)
            raise
        return do_xcom_push, sorted(skipped)

    def _report_failed_items(self, context: Context, raised: BaseException) -> None:
        """
        Report each failed sub-task as what happens to the task: retried, or failed for good.

        Their callbacks were held back until every sub-task had run, so the retry callback of one
        item no longer announces a retry a sibling's ``AirflowFailException`` then rules out. They
        run one after another, on the thread that ran the iteration.
        """
        if not self._state.failed_runners:
            return
        task_will_retry = self._task_will_retry(context, raised)
        for runner in self._state.failed_runners:
            runner.report_failure(task_will_retry=task_will_retry)

    def _task_will_retry(self, context: Context, raised: BaseException) -> bool:
        """
        Whether the runner retries the task for ``raised``, by the runner's own rules.

        No retry for the fail-fast exceptions nor for what is not an ``Exception`` (other than the
        parent's timeout), none when the retry policy decides FAIL, and otherwise a retry while the
        parent has attempts left (``IndexedTaskInstance.is_eligible_to_retry``). The policy's
        decision is the one ``_failure_for_the_runner`` took for ``raised`` when it chose it, so
        the policy is not evaluated again for the callbacks: a policy that calls a model may answer
        differently each time, and the callbacks must say what the exception handed over says.
        """
        if isinstance(raised, FAIL_WITHOUT_RETRY) or not isinstance(raised, (Exception, AirflowTaskTimeout)):
            return False
        if not self._state.failed_runners[0].task_instance.is_eligible_to_retry:
            return False
        if (policy := self.retry_policy) is not None:
            if (kept := self._state.decision_for(raised)) is not None:
                decision = kept
            else:
                ti = context["ti"]
                from_server = getattr(ti, "_ti_context_from_server", None)
                max_tries = from_server.max_tries if from_server else ti.max_tries
                try:
                    decision = policy.evaluate(
                        exception=raised, try_number=ti.try_number, max_tries=max_tries, context=context
                    )
                except Exception:
                    return True
            if decision.action == RetryAction.FAIL:
                return False
        return True

    def _failure_for_the_runner(self, context: Context, exceptions: list[Exception]) -> BaseException:
        """
        Pick the exception the runner decides the task's outcome on.

        The runner classifies by exception type: a fail-fast exception fails without a retry, and a
        ``retry_policy`` matches rules against the type. A ``BaseExceptionGroup`` defeats both, so
        an item's own exception is handed over whenever one decides: the first fail-fast one, the
        only one, or the one whose policy decision weighs most. With several failures the others
        stay attached as its cause, so every traceback reaches the log. Several failures no policy
        decides between are raised as a group, which the task's own retries then apply to. The
        policy is evaluated once per failure, and the decision for the chosen one is kept for the
        callbacks (see ``_task_will_retry``).
        """
        group = BaseExceptionGroup("Multiple sub-task failures", exceptions)
        chosen: BaseException | None = next(
            (exc for exc in exceptions if isinstance(exc, FAIL_WITHOUT_RETRY)), None
        )
        if chosen is None and len(exceptions) == 1:
            return exceptions[0]
        if chosen is None and (policy := self.retry_policy) is not None:
            ti = context["ti"]
            from_server = getattr(ti, "_ti_context_from_server", None)
            max_tries = from_server.max_tries if from_server else ti.max_tries
            weights = []
            decisions: list[RetryDecision | None] = []
            for exc in exceptions:
                try:
                    decision = policy.evaluate(
                        exception=exc, try_number=ti.try_number, max_tries=max_tries, context=context
                    )
                    weights.append(_DECISION_WEIGHT.get(decision.action, 0))
                    decisions.append(decision)
                except Exception:
                    # As the runner does: a policy that fails to evaluate leaves the default.
                    self.log.exception("Retry policy evaluation failed for a sub-task failure")
                    weights.append(0)
                    decisions.append(None)
            if max(weights) > 0:
                index = weights.index(max(weights))
                chosen = exceptions[index]
                if (kept := decisions[index]) is not None:
                    self._state.keep_decision(chosen, kept)
        if chosen is None:
            return group
        if len(exceptions) > 1:
            chosen.__cause__ = group
        return chosen

    def _skip_downstream_of_a_partial_skip(self, context: Context, result: XComIterable | None) -> None:
        """
        Skip the downstream tasks a skipped mapped task instance would have skipped.

        Some iterations skipped and the others succeeded, so the task succeeds. Downstream tasks whose
        trigger rule is satisfied by that are left to run over the values that exist; those whose
        rule skips them when any upstream task instance skipped are skipped here, as they would be
        after a mapped upstream. ``DownstreamTasksSkipped`` ends ``execute`` before the runner
        pushes the return value, so it is pushed here first, for the downstream tasks that do run.
        """
        to_skip = [
            task.task_id
            for task in self.downstream_list
            if task.trigger_rule in SKIPPED_WITH_A_SKIPPED_UPSTREAM
        ]
        if not to_skip:
            return
        ti = context["ti"]
        if TYPE_CHECKING:
            assert isinstance(ti, RuntimeTaskInstance)
        _push_xcom_if_needed(result, ti, self.log)
        raise DownstreamTasksSkipped(tasks=to_skip)

    async def _run_task(
        self,
        executor: AsyncAwareExecutor,
        context: Context,
        task: IndexedTaskInstance,
        trust_checkpoints: bool,
        since: int = 0,
    ) -> tuple[IndexedTaskInstance, Any | None, BaseException | None]:
        # See _run_tasks: checkpoints are consulted only on a retry that is not a rerun after a clear.
        indexed_task_state = await task.aget_state() if trust_checkpoints else None
        if indexed_task_state is not None and (
            indexed_task_state.try_number < since or indexed_task_state.fingerprint != task.input_fingerprint
        ):
            self.log.info(
                "Running task instance %s for %s again: its checkpoint is from before the task was "
                "cleared or for another input",
                task.index,
                task.task_id,
            )
            indexed_task_state = None
        if indexed_task_state is not None and indexed_task_state.status == TaskInstanceState.SUCCESS:
            self.log.info(
                "Skipping task instance %s for %s which already succeeded on a previous attempt",
                task.index,
                task.task_id,
            )
            if indexed_task_state.result is not None:
                await self.axcom_push(task, indexed_task_state.result)
            for key, value in (indexed_task_state.xcoms or {}).items():
                await task.axcom_push(key=key, value=value)
            if indexed_task_state.outlet_events:
                _replay_outlet_events(context["outlet_events"], indexed_task_state.outlet_events)
            return task, None, None
        if indexed_task_state is not None and indexed_task_state.status == TaskInstanceState.SKIPPED:
            return (
                task,
                None,
                AirflowSkipException(
                    f"Sub-task {task.task_id}[{task.index}] was skipped on a previous attempt"
                ),
            )

        # The sub-task runs against its own view of the context (see IndexedTaskRunner.indexed_context),
        # with its own outlet events: sub-tasks run concurrently and each needs its events
        # attributed correctly so they can be checkpointed and merged individually (see
        # _serialize_outlet_events/_merge_outlet_events).
        indexed_task_runner = IndexedTaskRunner(
            task_instance=task,
            register=self._state,
        )
        try:
            if task.is_async:
                with indexed_task_runner:
                    result = await indexed_task_runner.arun(context)
            else:
                # Entered and exited in the worker thread with execute, so the success and skip
                # callbacks fired from the exit run there too: a synchronous SDK call made from them
                # waits for the comms lock, where on the loop thread it would raise.

                def run_item():
                    with indexed_task_runner:
                        return indexed_task_runner.run(context)

                result = await executor.run_sync(run_item)

            indexed_task_state = IndexedTaskState(
                status=TaskInstanceState.SUCCESS,
                fingerprint=task.input_fingerprint,
                try_number=task.try_number,
            )
            # The result is checkpointed as well as pushed to XCom: the runner deletes every XCom
            # key listed by the server before each attempt (xcom_keys_to_clear), so XCom alone
            # cannot survive a retry, while the state store does. Both writes happen here, inside
            # the coroutine, so they overlap with the sub-tasks still running instead of adding a
            # synchronous pass over every index once the executor has drained. With a
            # state_store_backend configured the checkpoint holds only a reference to the payload.
            if result is not None and task.do_xcom_push:
                indexed_task_state.result = result
            serialized_outlet_events = _serialize_outlet_events(indexed_task_runner.outlet_events)
            if serialized_outlet_events:
                indexed_task_state.outlet_events = serialized_outlet_events
            # Written with the one checkpoint, not per push: a retry that skips this sub-task pushes
            # them again, as the runner has deleted them by then.
            if task.pushed_xcoms:
                indexed_task_state.xcoms = dict(task.pushed_xcoms)
            await task.aset_state(indexed_task_state)
        except (asyncio.CancelledError, AirflowTaskTimeout) as stopped:
            # Not this sub-task's outcome: it is being stopped from outside, by the executor
            # cancelling it or by the parent's execution_timeout, whose signal handler raises on the
            # main thread in whichever sub-task happens to run there. Both go on unchanged, so a
            # cancellation stays one and the timeout reaches the runner, which retries the task. The
            # sub-task the timeout struck is reported with the others (a cancelled one has nothing).
            if isinstance(stopped, asyncio.CancelledError):
                # A sync sub-task's thread may still be running: it gets no checkpoint from here on,
                # so its exit must report nothing either (see IndexedTaskRunner.cancel).
                indexed_task_runner.cancel()
            if indexed_task_runner.failure is not None:
                self._state.note_failed(indexed_task_runner)
            raise
        except AirflowSkipException as e:
            await task.aset_state(
                IndexedTaskState(
                    status=TaskInstanceState.SKIPPED,
                    fingerprint=task.input_fingerprint,
                    try_number=task.try_number,
                )
            )
            return task, None, e
        except BaseException as e:
            if indexed_task_runner.failure is not None:
                self._state.note_failed(indexed_task_runner)
            # Written with the input and the attempt, like the other outcomes, so the next attempt
            # tells a plain retry apart from a clear or a changed input and logs only the latter.
            await task.aset_state(
                IndexedTaskState(
                    status=TaskInstanceState.UP_FOR_RETRY,
                    fingerprint=task.input_fingerprint,
                    try_number=task.try_number,
                )
            )
            return task, None, e

        # The work is done and checkpointed: from here on only its publication can fail. A failure
        # leaves the SUCCESS checkpoint as it is, so the retry replays the result from it (the
        # branch at the top) instead of running the operator again for work that already finished.
        try:
            # The result is only checkpointed when the sub-task pushes XComs and returned something,
            # so the same condition decides whether there is a return_value_<index> to push at all.
            if indexed_task_state.result is not None:
                await self.axcom_push(task, indexed_task_state.result)
            _merge_outlet_events(context["outlet_events"], indexed_task_runner.outlet_events)
        except (asyncio.CancelledError, AirflowTaskTimeout):
            raise
        except BaseException as e:
            return task, None, e
        return task, result, None

    def _create_task(
        self,
        context: Context,
        index: int,
        mapped_kwargs: Mapping[str, Any],
        jinja_env: jinja2.Environment,
    ) -> IndexedTaskInstance:
        unmapped_task = self._operator.unmap(mapped_kwargs)
        # Make sure deferred operators will always raise a DeferredTask exception when executed
        unmapped_task.start_from_trigger = False

        indexed_ti = IndexedTaskInstance.create_indexed_task(
            context=context,
            index=index,
            operator=unmapped_task,
        )

        # Render against a copy of the context whose `ti`/`task_instance` are the new sub-task's
        # IndexedTaskInstance, not the parent IterableOperator's own (shared) ti — otherwise
        # context_update_for_unmapped() would mutate the parent's ti.task in place (context.copy()
        # is only a shallow copy).
        self._render_unmapped_operator(
            {**context, "ti": indexed_ti, "task_instance": indexed_ti}, unmapped_task, jinja_env
        )
        # Taken once rendered, so the partial kwargs an upstream provides are in it with their value.
        indexed_ti.input_fingerprint = _fingerprint(
            {**mapped_kwargs, **_partial_inputs_from_upstream(self.partial_kwargs, unmapped_task)}
        )
        return indexed_ti

    def execute(self, context: Context):
        jinja_env = self.get_template_env(dag=self.dag)

        async def tasks() -> AsyncIterator[IndexedTaskInstance]:
            # Resolved and read by the executor on the running event loop, so the input's XCom
            # reads go through asend and cannot deadlock with the sub-tasks' own SDK calls (see
            # AsyncAwareExecutor.imap_unordered).
            self._state.resolved = await self.expand_input.aresolve(context)
            for index in range(self._state.resolved.length):
                # Rendering may call the supervisor synchronously (an XComArg in a partial kwarg,
                # ``{{ var.value.x }}``, ``{{ conn.x }}``), which raises DeadlockImminentError on the
                # loop thread while a sub-task's asend is in flight. From a worker thread the same
                # call waits for it instead, as in XComArg.aresolve.
                yield await asyncio.to_thread(
                    self._create_task,
                    context=context,
                    index=index,
                    mapped_kwargs=await self._state.resolved.aget(index),
                    jinja_env=jinja_env,
                )

        do_xcom_push, skipped = self._run_tasks(context=context, tasks=tasks())
        result = (
            XComIterable(
                task_id=self.task_id,
                dag_id=self.dag_id,
                run_id=context["run_id"],
                length=self._state.length,
                map_index=context["ti"].map_index,
                skipped=skipped,
            )
            if do_xcom_push and self._state.resolved
            else None
        )
        if skipped:
            self._skip_downstream_of_a_partial_skip(context, result)
        return result
