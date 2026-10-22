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
import threading
import time
from collections.abc import (
    AsyncIterator,
    Awaitable,
    Callable,
    Generator,
    Iterable,
    Iterator,
    Mapping,
    Sequence,
)
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from functools import partial, wraps
from typing import TYPE_CHECKING, Any, ClassVar, TypeVar

from airflow.sdk import BaseXCom, TaskInstanceState, TriggerRule
from airflow.sdk.bases.operator import BaseOperator, event_loop
from airflow.sdk.bases.skipmixin import SkipMixin
from airflow.sdk.bases.xcom import XComIterable, _own_coordinate
from airflow.sdk.definitions._internal.fingerprint import InputFingerprint
from airflow.sdk.definitions.param import process_params
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
from airflow.sdk.execution_time.executor import AsyncAwareExecutor
from airflow.sdk.execution_time.task_runner import (
    CheckpointDeserializationError,
    IndexedTaskInstance,
    IndexedTaskRunner,
    IndexedTaskState,
    _push_xcom_if_needed,
)

if TYPE_CHECKING:
    import jinja2

    from airflow.sdk.definitions._internal.abstractoperator import AbstractOperator
    from airflow.sdk.definitions._internal.expandinput import ExpandInput, Resolved
    from airflow.sdk.definitions.context import Context
    from airflow.sdk.definitions.mappedoperator import MappedOperator
    from airflow.sdk.execution_time.context import TaskStateStoreAccessor
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
    from airflow.sdk.types import Logger


# The outcome an indexed task's step returns, kept as it is by keeps_failure_to_its_indexed_task: None is
# the replay's "no checkpoint, run the indexed task".
Outcome = TypeVar("Outcome", bound="IndexedTaskOutcome | None")

# The trigger rules under which one skipped upstream task instance skips a task, whatever the other
# upstream task instances did (see TriggerRuleDep). A skipped iteration has the same effect on a
# downstream task with one of these rules as a skipped mapped task instance would.
SKIPPED_WITH_A_SKIPPED_UPSTREAM = frozenset(
    {TriggerRule.ALL_SUCCESS, TriggerRule.NONE_SKIPPED, TriggerRule.ALL_DONE_MIN_ONE_SUCCESS}
)


# Raised by an indexed task, these fail the task without a retry, as the runner does for a task that raises
# them itself (see _run_task_and_map_outcome and _handle_handler_failure).
FAIL_WITHOUT_RETRY = (AirflowFailException, AirflowSensorTimeout, AirflowTaskTerminated)


def _retryable(exception: BaseException) -> bool:
    """Whether ``exception`` is a failure the runner may retry, judged member by member for a group."""
    match exception:
        case BaseExceptionGroup():
            return all(_retryable(exc) for exc in exception.exceptions)
        # The classes of FAIL_WITHOUT_RETRY: a class pattern cannot take the tuple.
        case AirflowFailException() | AirflowSensorTimeout() | AirflowTaskTerminated():
            return False
        case Exception() | AirflowTaskTimeout():
            return True
        case _:
            return False


@dataclass(frozen=True)
class IndexedTaskOutcome:
    """
    How one indexed task ended: what it returned, or what it raised.

    Every step of :class:`IndexedTaskLifecycle` ends with one, and :meth:`IndexedTaskOutcomes.record`
    takes it. ``task`` is an :class:`UnreadIndex` when the index's input could not be read.
    """

    task: IndexedTaskInstance | UnreadIndex
    result: Any = None
    exception: BaseException | None = None

    def fail_fast(self) -> AirflowFailException | None:
        """
        Return the failure that ends the task at once for an outcome the iteration cannot carry.

        An indexed task is not a task instance of its own: it cannot defer, reschedule, trigger a
        DAG run or skip downstream tasks, and a ``BaseException`` that is no ``Exception``
        (``KeyboardInterrupt``, ``SystemExit``) must never be swallowed into a retry. Each of these
        fails the whole task without a retry, with a message saying why. Any other exception, an
        ``AirflowTaskTimeout`` the operator raised itself included, is the indexed task's own
        failure, which :meth:`IndexedTaskOutcomes.record` collects.
        """
        indexed_task_name = f"Indexed task {self.task.task_id}[{self.task.index}]"
        match self.exception:
            case TaskDeferred():
                return AirflowFailException(
                    f"{indexed_task_name} attempted to defer. Deferrable operators are not supported "
                    "inside IterableOperator."
                )
            case DagRunTriggerException() | DownstreamTasksSkipped():
                return AirflowFailException(
                    f"{indexed_task_name} raised {type(self.exception).__name__}. Triggering DAG runs "
                    "(TriggerDagRunOperator) and skipping downstream tasks (ShortCircuitOperator and "
                    "similar) are not supported inside IterableOperator: an indexed task has no "
                    "downstream tasks or DAG run of its own for the effect to apply to."
                )
            case AirflowRescheduleException():
                return AirflowFailException(
                    f"{indexed_task_name} attempted to reschedule (raised AirflowRescheduleException). "
                    "Reschedule-mode sensors are not supported inside IterableOperator: an indexed "
                    "task has no task instance of its own to reschedule."
                )
            case None | Exception() | AirflowTaskTimeout():
                # An AirflowTaskTimeout here is the operator's own, raised off the main thread (the
                # parent's never gets here, IndexedTaskLifecycle.run re-raises it), and the runner
                # judges it as it does for a plain task that raises it: a failure it retries.
                return None
            case _:
                return AirflowFailException(
                    f"{indexed_task_name} raised a non-Exception BaseException: "
                    f"{type(self.exception).__name__}: {self.exception}"
                )


def keeps_failure_to_its_indexed_task(
    step: Callable[..., Awaitable[Outcome]],
) -> Callable[..., Awaitable[Outcome | IndexedTaskOutcome]]:
    """
    Make what an indexed task's step raises that indexed task's outcome, not the end of the task.

    ``step`` is a coroutine method of :class:`IndexedTaskLifecycle`, whose ``task`` the outcome is.
    Whatever it raises, a ``BaseException`` included, is returned as that task's
    :class:`IndexedTaskOutcome`: the siblings go on, the run counts it with the other outcomes, and a
    non-``Exception`` still fails the task at once (see :meth:`IndexedTaskOutcome.fail_fast`). A
    cancellation and the parent's ``execution_timeout`` are not the step's outcome and go on unchanged.
    """

    @wraps(step)
    async def kept(self: IndexedTaskLifecycle, *args: Any, **kwargs: Any) -> Outcome | IndexedTaskOutcome:
        try:
            return await step(self, *args, **kwargs)
        except (asyncio.CancelledError, AirflowTaskTimeout):
            raise
        except BaseException as e:
            return IndexedTaskOutcome(self.task, exception=e)

    return kept


@dataclass(frozen=True)
class CheckpointBatch:
    """
    The serialized outcomes of indexed tasks, by index, that one store key holds.

    Batches are numbered in the order they are written, and the number names the key.
    """

    # The task state store is already scoped to the parent task instance (dag, run, task and map
    # index), so the key carries no identity, only a namespace that keeps the operator's own entries
    # apart from anything user code stores from inside an indexed task (see
    # IndexedTaskStateStoreAccessor.RESERVED_PREFIX).
    KEY_PREFIX: ClassVar[str] = "_iterable_batch_"

    number: int
    # As stored: IndexedTaskState.deserialize restores an entry, and takes one that is not a
    # checkpoint for none.
    outcomes: Mapping[int, Any]

    @classmethod
    def read(cls, store: TaskStateStoreAccessor, number: int) -> CheckpointBatch | None:
        """Read the batch numbered ``number``, or None when it is missing or not a batch."""
        value = store.get(f"{cls.KEY_PREFIX}{number}")
        if not isinstance(value, Mapping):
            return None
        try:
            return cls(number, {int(index): raw for index, raw in value.items()})
        except (TypeError, ValueError):
            return None

    @property
    def key(self) -> str:
        return f"{self.KEY_PREFIX}{self.number}"

    def to_value(self) -> dict[str, Any]:
        """Return the value to store: a JSON object, whose keys are strings."""
        return {str(index): raw for index, raw in self.outcomes.items()}

    def __len__(self) -> int:
        return len(self.outcomes)


class PendingCheckpoints:
    """The outcomes of indexed tasks recorded since the last batch was written, by index."""

    def __init__(self) -> None:
        self._outcomes: dict[int, dict[str, Any]] = {}

    def __len__(self) -> int:
        return len(self._outcomes)

    def add(self, index: int, state: IndexedTaskState) -> None:
        self._outcomes[index] = state.serialize()

    def batch(self, number: int) -> CheckpointBatch:
        """Return the outcomes pending now as the batch to write under ``number``."""
        return CheckpointBatch(number, dict(self._outcomes))

    def remove(self, written: CheckpointBatch) -> None:
        """Drop the outcomes ``written`` holds, but not one recorded again while it was written."""
        for index, raw in written.outcomes.items():
            if self._outcomes.get(index) is raw:
                del self._outcomes[index]


class EarlierCheckpoints:
    """
    The checkpoints earlier attempts of the task instance left, read once when an attempt starts.

    The batches are read in order until the first missing or unreadable one, the latest entry of an
    index winning: an index run again after a clear appears in several.
    """

    def __init__(self) -> None:
        self._outcomes: dict[int, Any] = {}
        #: How many batches were read, which is the number the next batch gets.
        self.count = 0

    def read(self, store: TaskStateStoreAccessor) -> None:
        while (batch := CheckpointBatch.read(store, self.count)) is not None:
            self._outcomes.update(batch.outcomes)
            self.count += 1

    def state_of(self, index: int) -> IndexedTaskState | None:
        """
        Return the checkpoint of ``index``, or None when it has none.

        Raises :class:`CheckpointDeserializationError` when the checkpoint cannot be restored.
        """
        return IndexedTaskState.deserialize(self._outcomes.get(index))


class CheckpointBatches:
    """
    Write the checkpoints of a run's indexed tasks a round at a time.

    One key per indexed task would cost one task state store request per index, each through the
    supervisor to the API server, and a run over a large input floods both. An indexed task's outcome is
    kept pending instead, and the pending outcomes are written as one key, ``_iterable_batch_<n>``, once
    they make a round: as many as indexed tasks run at once (``round_size``). Rounds follow completion
    order, as the indexed tasks finish in any order, so a slow indexed task holds back no outcome but its
    own. What is left when the run's event loop has closed is written by :meth:`flush`, before the
    completion marker (see :class:`Checkpoints`).

    The batches are numbered on from the ones earlier attempts left (:meth:`start_at`), so an attempt
    never overwrites a batch that was not read. A write that fails keeps its outcomes for the next one,
    and one that fails at the end is logged. A hard kill loses the outcomes not written yet, a round at
    most unless a write failed before it: those indexed tasks run again on the retry, and as they were
    reported when they finished, their success callbacks fire again.
    """

    def __init__(self, store: TaskStateStoreAccessor, *, round_size: int, log: Logger) -> None:
        self._store = store
        self._round_size = max(round_size, 1)
        self._log = log
        self._number = 0
        self._pending = PendingCheckpoints()
        # Raised by the size of a write that failed, so the next attempt waits for a round more.
        self._threshold = self._round_size
        # Bound to the run's event loop when first used, as asyncio's primitives are.
        self._lock = asyncio.Lock()

    def start_at(self, number: int) -> None:
        """Start numbering this attempt's batches at ``number``, after the ones earlier attempts left."""
        self._number = number

    async def add(self, index: int, state: IndexedTaskState) -> None:
        """Keep an indexed task's outcome pending, and write the round once it is full."""
        self._pending.add(index, state)
        if len(self._pending) >= self._threshold:
            await self._write_round()

    async def _write_round(self) -> None:
        async with self._lock:
            # Another indexed task may have written the round while this one waited for the lock.
            if len(self._pending) < self._threshold:
                return
            batch = self._pending.batch(self._number)
            try:
                await self._store.aset(batch.key, batch.to_value())
            except Exception:
                self._threshold = len(batch) + self._round_size
                self._log.warning(
                    "Could not write the checkpoints of %d indexed tasks, kept for the next write",
                    len(batch),
                    exc_info=True,
                )
                return
            self._written(batch)

    @contextmanager
    def flushed(self) -> Generator[None]:
        """
        Write the outcomes left when the block ends, however it ends.

        Entered before the run's event loop, so it exits after the loop closed (see :meth:`flush`).
        """
        try:
            yield
        finally:
            self.flush()

    def flush(self) -> None:
        """
        Write the outcomes left, with the synchronous store call.

        Called once the run's event loop has closed, so no asynchronous call of an indexed task is in
        flight, where :class:`Checkpoints` also writes its marker. A write that fails is logged: those
        indexed tasks run again on a retry.
        """
        if not self._pending:
            return
        batch = self._pending.batch(self._number)
        try:
            self._store.set(batch.key, batch.to_value())
        except Exception:
            self._log.exception(
                "Could not write the checkpoints of %d indexed tasks: a retry runs them again", len(batch)
            )
            return
        self._written(batch)

    def _written(self, batch: CheckpointBatch) -> None:
        self._number += 1
        self._threshold = self._round_size
        self._pending.remove(batch)


class Checkpoints:
    """
    Decide whether one attempt of an IterableOperator may resume from its indexed tasks' checkpoints.

    Checkpoints are only consulted from the second attempt onwards. A completion marker left by a
    previous attempt whose iterations all finished means this attempt follows a manual clear when
    ``max_tries`` changed since: a clear sets it to ``try_number + retries``, a retry leaves it
    alone, and it can go down when the retries were lowered, so any change counts. Every index then
    runs again: the stale ``SUCCESS`` checkpoints are ignored, and the rerun's outcomes are written in
    batches after them, where the latest entry of an index wins. With ``max_tries``
    unchanged, the iterations finished but the task failed afterwards (the worker died, or
    ``post_execute`` or the result push failed), and this retry resumes from the checkpoints that
    attempt could trust; only a clear that leaves ``max_tries`` exactly as it was (the retries
    lowered by as many attempts as ran since) is taken for such a retry. A marker without
    ``max_tries`` counts as a clear. On entry after a clear the marker is replaced by the
    attempt the rerun starts at, so that a crash during the rerun resumes from the checkpoints
    written since, and only from those: an index the crashed rerun did not reach still holds its
    checkpoint from before the clear, which must not be replayed. The marker is written again when
    the block exits without an exception, and when every iteration skipped: the task is then
    ``SKIPPED``, a final state, and a clear of it must run every iteration again rather than
    replay the skips, as a cleared mapped task instance would. An attempt that fails, even with
    some iterations skipped, writes no marker, so a retry, or a clear that keeps the task state,
    resumes after it: iterations that succeeded or skipped keep that outcome and only the others
    run again. A Dag run clear keeps the task state; a task instance clear discards it unless
    ``keep_task_state`` is set, and then finds neither marker nor checkpoints, so every iteration
    runs again.

    The outcomes are written a round of ``task_concurrency`` at a time (see :class:`CheckpointBatches`),
    each round as one ``_iterable_batch_<n>`` key in the ``task_state_store`` table, or behind a
    reference with a ``[workers] state_store_backend``: a run makes about ``input length /
    task_concurrency`` writes, and a retry about as many reads, instead of one per index. The marker
    is written after the last batch. A last batch that cannot be written is logged and the marker still
    written: the attempt's indexed tasks did finish, and on a retry those without a checkpoint run
    again, as they would without the marker.

    The checkpoints themselves are never deleted: one marker write costs the same whatever the indexed
    task count, and the store is scoped to the parent task instance, so state an indexed task stored for
    itself is never touched. They expire with the store's default retention (``[state_store]
    default_retention_days``, 30 days unless configured, 0 disables expiry), which also bounds how
    long a task that exhausted its retries keeps them. Keeping them until then is intended: a manual
    clear of such a task resumes from the checkpoints instead of re-running every index, and the
    marker is what tells a clear-after-success apart from that. Batches expire one by one, so a
    clear after that retention may find the earlier ones gone and the later ones not: the reading then
    stops at the first one missing, and the indexed tasks it does not cover run again.
    """

    # Same namespace as CheckpointBatch.KEY_PREFIX, for the same reason.
    COMPLETION_KEY = "_iterable_completed"

    def __init__(self, context: Context, *, round_size: int, log: Logger) -> None:
        self._store = context["task_state_store"]
        self._try_number = context["ti"].try_number
        self._max_tries = context["ti"].max_tries
        self.trust_checkpoints = False
        # The attempt from which checkpoints may be resumed; older ones predate a manual clear.
        self.since = 0
        self.earlier = EarlierCheckpoints()
        self.batches = CheckpointBatches(self._store, round_size=round_size, log=log)

    def __enter__(self) -> Checkpoints:
        if self._try_number > 1:
            # Read after a clear too: this attempt writes its batches after the ones there.
            self.earlier.read(self._store)
            self.batches.start_at(self.earlier.count)
            marker = self._store.get(self.COMPLETION_KEY)
            if (
                isinstance(marker, Mapping)
                and marker.get("completed")
                and marker.get("max_tries") != self._max_tries
            ):
                self.since = self._try_number
                self._store.set(self.COMPLETION_KEY, {"completed": False, "since": self.since})
            else:
                self.trust_checkpoints = True
                since = marker.get("since") if isinstance(marker, Mapping) else None
                if isinstance(since, int):
                    self.since = since
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        # The outcomes left first, also after a failure: a retry resumes from them, and the marker
        # comes after them.
        self.batches.flush()
        if exc_type is None or issubclass(exc_type, AirflowSkipException):
            self._store.set(
                self.COMPLETION_KEY,
                {
                    "completed": True,
                    "try_number": self._try_number,
                    "max_tries": self._max_tries,
                    "since": self.since,
                },
            )

    def trusts(self, checkpoint: IndexedTaskState, input_fingerprint: str | None) -> bool:
        """
        Whether an indexed task's ``checkpoint`` may settle it: written since the last clear, for its input.

        A checkpoint from before a clear (an older ``try_number`` than :attr:`since`) or for another
        input (another ``input_fingerprint``) is not trusted.
        """
        return checkpoint.try_number >= self.since and checkpoint.fingerprint == input_fingerprint

    def read(self, task: IndexedTaskInstance) -> IndexedTaskState | None:
        """Return the checkpoint an earlier attempt left for ``task`` (see :class:`EarlierCheckpoints`)."""
        return self.earlier.state_of(task.index)

    async def record(self, task: IndexedTaskInstance, state: IndexedTaskState) -> None:
        """Record ``task``'s outcome; it is written with its round (see :class:`CheckpointBatches`)."""
        await self.batches.add(task.index, state)

    def flushed(self) -> AbstractContextManager[None]:
        """Write the outcomes left when the run's event loop closed (see :meth:`CheckpointBatches.flushed`)."""
        return self.batches.flushed()


class IndexedTaskInstanceNotStarted(Exception):
    """
    The outcome of an indexed task instance pulled before a kill and reached after it.

    Never raised: :meth:`IndexedTaskLifecycle.run` returns it as the instance's outcome, so that
    :meth:`IndexedTaskOutcomes.record` counts it apart from the failures. It ran no code, wrote
    no checkpoint and fires no callback; the next attempt runs it.
    """


@dataclass(frozen=True)
class UnreadIndex:
    """
    An index whose input could not be read or rendered, in place of its indexed task instance.

    A ``.map()`` callable runs, and template fields are rendered, when an index is read: what
    they raise is that index's outcome, as it is the mapped task instance's under ``.expand()``.
    A skip leaves the index out of the result, anything else fails it, and the siblings go on.
    No operator was created for it, so unlike a mapped task instance it fires no callback; nor
    does it write a checkpoint, so the next attempt reads its input again.
    """

    task_id: str
    index: int


class IterationState:
    """
    What a kill of one run of an iterated task must reach, kept apart from the operator's configuration.

    It holds the indexed operators in flight and those already killed, so that
    :meth:`IterableOperator.on_kill` reaches each one once; the stop flag a kill sets, which the
    executor consults before starting the next indexed task; and the threads the indexed operators
    are killed from. The rest of what a run holds is :class:`IterationRun`'s, which only exists while
    the run goes; this exists before it, so that a kill that arrives first still stops the run.

    A copy of the operator (``copy.copy`` in ``prepare_for_execution``, the deep copy of
    ``dag.partial_subset``) is another task with nothing in flight and no kill pending, so copying
    the state gives a fresh one. The
    operator renews it once a run ended, not when one starts: a rerun in the same process starts
    clean, while a kill that arrives before the run sets the stop flag on the state the run uses.
    """

    def __init__(self, log: Logger) -> None:
        # Keyed by identity: BaseOperator equality compares fields such as task_id, which every
        # indexed operator of one iterated task shares, so a set would hold one of them at most.
        self._in_flight: dict[int, BaseOperator] = {}
        self._lock = threading.Lock()
        # The runner calls on_kill() again after the execution timeout that made the run call
        # it first, and an indexed operator is killed once.
        self._killed: set[int] = set()
        # A plain flag and a plain list, not an Event and not guarded by the lock: on_kill() runs in
        # the runner's SIGTERM handler, on the main thread, between two bytecodes of whatever the
        # loop thread was doing, register() or unregister() under the lock included. A lock the
        # handler takes that its own thread holds would hang it, so request_stop() and start_kill()
        # take none; take_in_flight() is called from the kill thread instead.
        self._stop_requested = False
        # The threads on_kill() started to kill the indexed operators in flight; see await_kill.
        self._kill_threads: list[threading.Thread] = []
        #: The iterated task's logger, which :meth:`kill` reports an ``on_kill`` failure to.
        self._log = log

    def __copy__(self) -> IterationState:
        return IterationState(self._log)

    def __deepcopy__(self, memo: dict[int, Any]) -> IterationState:
        return IterationState(self._log)

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
        """Return the indexed operators in flight that were not handed out before, and mark them killed."""
        with self._lock:
            operators = [op for key, op in self._in_flight.items() if key not in self._killed]
            self._killed.update(map(id, operators))
        return operators

    def request_stop(self) -> None:
        """Ask the iteration to start nothing else; see :meth:`stop_requested`. Takes no lock."""
        self._stop_requested = True

    def start_kill(self, kill: Callable[[], None]) -> None:
        """
        Run ``kill`` in a thread of its own, kept so that :meth:`await_kill` can wait for it.

        Takes no lock, so it can be called from a signal handler; ``kill`` takes the indexed operators
        in flight itself, with :meth:`take_in_flight`, on its thread.
        """
        thread = threading.Thread(target=kill, name="iterable-operator-on-kill", daemon=True)
        self._kill_threads.append(thread)
        thread.start()

    def kill(self, operators: list[BaseOperator]) -> None:
        """Call ``on_kill`` of each of ``operators``; what one raises does not keep the kill from the others."""
        # Whatever one raises, a BaseException included, is caught here as a plain error is.
        for operator in operators:
            try:
                operator.on_kill()
            except BaseException:
                self._log.exception("Error calling on_kill() for indexed task operator %s", operator.task_id)

    def await_kill(self, timeout: float) -> None:
        """
        Wait up to ``timeout`` seconds for the threads :meth:`start_kill` started.

        Called by :meth:`IterationRun.run` once the loop closed and before the run
        concludes, so the task does not end, and the process with it, while an indexed operator's
        ``on_kill`` is still cleaning up. The threads are daemon threads: one that never returns
        holds the run for ``timeout`` at most, and the supervisor's SIGKILL bounds the rest.
        """
        deadline = time.monotonic() + timeout
        for thread in list(self._kill_threads):
            thread.join(max(0.0, deadline - time.monotonic()))

    def stop_requested(self) -> bool:
        """Whether :meth:`request_stop` was called; passed to the executor as its ``stop``."""
        return self._stop_requested


class IndexedTaskOutcomes:
    """
    What the indexed tasks of one run ended with, and what the task ends with because of it.

    Entered by :meth:`IterationRun.run` around the loop that drives the indexed tasks.
    :meth:`record` takes each outcome: a skip is noted, an outcome the iteration cannot carry
    raises at once, any other failure is logged and collected. :meth:`conclude` turns what was
    collected, and whether the run's :class:`IterationState` was asked to stop, into the task's
    own outcome and raises it: killed, failed on the exception the runner judges, skipped over
    an empty input or because every indexed task skipped. Whatever
    exception leaves the block, those included, the failed indexed tasks' callbacks are reported
    on exit with the task's fate, so they say what the runner then does: retry, or fail for good.
    """

    #: How strongly a retry policy decision speaks for the task when several indexed tasks failed:
    #: one the policy fails the task on outweighs one it retries on, which outweighs the default.
    _DECISION_WEIGHT = {RetryAction.FAIL: 2, RetryAction.RETRY: 1, RetryAction.DEFAULT: 0}

    def __init__(self, operator: IterableOperator, state: IterationState, context: Context) -> None:
        self._operator = operator
        self._state = state
        self._context = context
        self.total = 0
        #: Pulled before a kill and never started (see :class:`IndexedTaskInstanceNotStarted`).
        self.not_started = 0
        self.exceptions: list[BaseException] = []
        self.skipped: dict[int, AirflowSkipException] = {}
        self._failed_runners: list[IndexedTaskRunner] = []
        self._decision: tuple[BaseException, RetryDecision] | None = None

    def __enter__(self) -> IndexedTaskOutcomes:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        # Whatever the task ends with decides every failed indexed task's callback, so they agree.
        if exc_value is not None and self._failed_runners:
            self._report(exc_value)

    def note_failed(self, runner: IndexedTaskRunner) -> None:
        """Remember a failed indexed task's runner: its callback waits for the task's fate."""
        self._failed_runners.append(runner)

    @property
    def log(self) -> Logger:
        """The operator's logger: the outcomes are logged under the task they belong to."""
        return self._operator.log

    @property
    def failed_runners(self) -> tuple[IndexedTaskRunner, ...]:
        """The runners of the indexed tasks that failed in this run, in the order they failed."""
        return tuple(self._failed_runners)

    def record(self, outcome: IndexedTaskOutcome) -> None:
        """Take one indexed task's outcome: nothing, a skip, not started, a failure, or one ending it."""
        task, exception = outcome.task, outcome.exception
        if isinstance(exception, IndexedTaskInstanceNotStarted):
            self.not_started += 1
            return
        self.total += 1
        if exception is None:
            return
        if isinstance(exception, AirflowSkipException):
            self.skipped[task.index] = exception
            return
        # An outcome the iteration cannot carry stops it at once: every later indexed task would
        # end the same way.
        if (fail_fast := outcome.fail_fast()) is not None:
            raise fail_fast from exception
        self.log.exception(
            "An exception occurred for task_id %s with index %s", task.task_id, task.index, exc_info=exception
        )
        self.exceptions.append(exception)

    def conclude(self, length: int | None) -> list[int]:
        """
        Raise the task's outcome from what was recorded, or return the skipped indices.

        ``length`` is how many indexed tasks the resolved input gives, None while it was still being
        resolved; a kill names it.

        A kill comes first: with every killed indexed task returning normally there would be no
        failure to raise, the completion marker would be written and a result returned over
        XComs that do not exist, and a kill while the input resolves is not an empty input. The
        task then fails without a retry, as the runner treats a terminated task, and no marker
        is written, so a later clear resumes from the checkpoints of the indexed tasks that did
        finish. Then the failures, handed to the runner as the exception it judges; then an empty
        input, skipped as a mapped task over nothing is; then every indexed task skipped, which
        skips the task too.
        """
        if self._state.stop_requested():
            raise AirflowTaskTerminated(self._killed_message(length)) from (
                BaseExceptionGroup("Indexed task failures", self.exceptions) if self.exceptions else None
            )
        if self.exceptions:
            raise self._pick_failure_for_runner()
        if self.total == 0:
            raise AirflowSkipException("The input to iterate over is empty.")
        if self.skipped and len(self.skipped) == self.total:
            raise next(iter(self.skipped.values()))
        return sorted(self.skipped)

    def _killed_message(self, length: int | None) -> str:
        """
        Say what a kill left behind, naming only the counts that are not zero.

        Before the input was resolved there is no count to give. Once it is, the indexed tasks
        that ran (the killed ones among them, which came back as failures), those pulled before
        the kill and never started, and those never pulled add up to the length.
        """
        if length is None:
            return "The iterated task was killed before its input was resolved."
        parts = [f"{self.total} of {length} indexed tasks ran"]
        if self.not_started:
            parts.append(f"{self.not_started} pulled but never started")
        if never_pulled := length - self.total - self.not_started:
            parts.append(f"{never_pulled} never pulled")
        return f"The iterated task was killed: {', '.join(parts)}."

    def _report(self, exception: BaseException) -> None:
        """
        Report each failed indexed task as what happens to the task: retried, or failed for good.

        Their callbacks were held back until every indexed task had run, so the retry callback of
        one no longer announces a retry a sibling's ``AirflowFailException`` then rules out. They
        run one after another, on the thread that ran the iteration.
        """
        task_will_retry = self._task_will_retry(exception)
        for runner in self._failed_runners:
            runner.report_failure(task_will_retry=task_will_retry)

    def _task_will_retry(self, exception: BaseException) -> bool:
        """
        Whether the runner retries the task for ``exception``, by the runner's own rules.

        No retry for the fail-fast exceptions nor for what is not an ``Exception`` (other than an
        ``AirflowTaskTimeout``), none when the retry policy decides FAIL, and otherwise a retry
        while the parent has attempts left (``IndexedTaskInstance.is_eligible_to_retry``). A group
        of failures is retried when each of them is: an indexed task's own ``AirflowTaskTimeout`` next to
        another indexed task's ``ValueError`` makes a ``BaseExceptionGroup``, which the runner retries like
        any failure. The policy's decision is the one :meth:`_pick_failure_for_runner` took for
        ``exception`` when it chose it, so the policy is not evaluated again for the callbacks: a
        policy that calls a model may answer differently each time, and the callbacks must say
        what the exception handed over says.
        """
        if not _retryable(exception):
            return False
        if not self._failed_runners[0].task_instance.is_eligible_to_retry:
            return False
        if (policy := self._operator.retry_policy) is not None:
            if self._decision is not None and self._decision[0] is exception:
                decision = self._decision[1]
            else:
                ti = self._context["ti"]
                try:
                    decision = policy.evaluate(
                        exception=exception,
                        try_number=ti.try_number,
                        max_tries=ti.max_tries,
                        context=self._context,
                    )
                except Exception:
                    return True
            if decision.action == RetryAction.FAIL:
                return False
        return True

    def _pick_failure_for_runner(self) -> BaseException:
        """
        Pick the exception the runner decides the task's outcome on.

        The runner classifies by exception type: a fail-fast exception fails without a retry, and a
        ``retry_policy`` matches rules against the type. A ``BaseExceptionGroup`` defeats both, so
        an indexed task's own exception is handed over whenever one decides: the first fail-fast
        one, the only one, or the one whose policy decision weighs most. With several failures the
        others stay attached as its cause, so every traceback reaches the log. Several failures no
        policy decides between are raised as a group, which the task's own retries then apply to.
        The policy is evaluated once per failure, and the decision for the chosen one is kept for
        the callbacks, as is the default for a group no decision outweighs (see
        :meth:`_task_will_retry`).
        """
        exceptions = self.exceptions
        group = BaseExceptionGroup("Multiple indexed task failures", exceptions)
        chosen: BaseException | None = next(
            (exc for exc in exceptions if isinstance(exc, FAIL_WITHOUT_RETRY)), None
        )
        if chosen is None and len(exceptions) == 1:
            return exceptions[0]
        if chosen is None and (policy := self._operator.retry_policy) is not None:
            ti = self._context["ti"]
            max_tries = ti.max_tries
            weights = []
            decisions: list[RetryDecision | None] = []
            for exc in exceptions:
                try:
                    decision = policy.evaluate(
                        exception=exc, try_number=ti.try_number, max_tries=max_tries, context=self._context
                    )
                    weights.append(self._DECISION_WEIGHT.get(decision.action, 0))
                    decisions.append(decision)
                except Exception:
                    # As the runner does: a policy that fails to evaluate leaves the default.
                    self.log.exception("Retry policy evaluation failed for an indexed task failure")
                    weights.append(0)
                    decisions.append(None)
            if max(weights) > 0:
                index = weights.index(max(weights))
                chosen = exceptions[index]
                if (kept := decisions[index]) is not None:
                    self._decision = (chosen, kept)
            else:
                # Every decision was the default, or failed to evaluate, which the runner treats
                # the same: the group carries the default, so the callbacks follow it instead of
                # evaluating the policy once more, on an exception no indexed task raised.
                self._decision = (group, RetryDecision.default())
        if chosen is None:
            return group
        if len(exceptions) > 1:
            chosen.__cause__ = group
        return chosen


class IndexedTaskUnmapper:
    """
    Build the indexed task of each index of one run: unmapped, with its params, rendered and fingerprinted.

    Unmapping and rendering go together, as in :meth:`MappedOperator.render_template_fields`: the
    operator is unmapped from the index's ``mapped_kwargs`` and its template fields rendered against
    the context the indexed task will run with. It holds what that needs and is the same for every
    index of the run: the mapped operator, the template environment and the run's
    :class:`InputFingerprint`. Called from worker threads, one index at a time per thread.
    """

    def __init__(
        self,
        operator: IterableOperator,
        mapped_operator: MappedOperator,
        context: Context,
        *,
        stop_requested: Callable[[], bool],
    ) -> None:
        self._params = operator.params
        self._mapped_operator = mapped_operator
        self._context = context
        self._jinja_env = operator.get_template_env(dag=operator.dag)
        # One per run, as it keeps what is the same for every index of the run.
        self._input_fingerprint = InputFingerprint(
            operator.partial_kwargs, stop_requested=stop_requested, log=operator.log
        )
        #: Whether the operator the task iterates pushes its results, as built for an indexed task:
        #: an operator may set ``do_xcom_push`` in its own ``__init__``, not only through
        #: ``partial()``. The same for every indexed task; True until one is built.
        self.do_xcom_push = True

    def unmap(self, index: int, mapped_kwargs: Mapping[str, Any]) -> IndexedTaskInstance:
        """Build the indexed task of ``index`` from its ``mapped_kwargs``, rendered and fingerprinted."""
        context = self._context
        unmapped_task = self._mapped_operator.unmap(mapped_kwargs)
        self.do_xcom_push = unmapped_task.do_xcom_push
        # Make sure deferred operators will always raise a DeferredTask exception when executed
        unmapped_task.start_from_trigger = False

        indexed_ti = IndexedTaskInstance.create_indexed_task(
            context=context,
            index=index,
            operator=unmapped_task,
        )
        if unmapped_task.params != self._params:
            # Processed as a mapped task instance's are; the parent's processed params, which
            # clone_context copies into every indexed context, stand for any other index.
            indexed_ti.own_params = process_params(
                context["dag"], unmapped_task, context["dag_run"].conf, suppress_exception=False
            )

        # Rendered against the context the indexed task will execute with (its own ti, task, params
        # and store view), not the parent's: a template must read the same values as execute() does.
        unmapped_task._do_render_template_fields(
            parent=unmapped_task,
            template_fields=self._mapped_operator.template_fields,
            context=indexed_ti.context_for(context),
            jinja_env=self._jinja_env,
            seen_oids=set(),
        )
        # Taken once rendered, so the partial kwargs an upstream provides are in it with their value.
        indexed_ti.input_fingerprint = self._input_fingerprint.of(mapped_kwargs, unmapped_task)
        return indexed_ti


class IndexedTaskLifecycle:
    """
    One indexed task of a run, from its checkpoint to its outcome.

    :meth:`run` settles it from its checkpoint on a retry, or runs its operator, writes its checkpoint
    and then publishes it, records its skip or records its failure. Every step returns the outcome
    :class:`IndexedTaskOutcome` the run records; what a step raises is the indexed task's outcome
    (see :func:`keeps_failure_to_its_indexed_task`), not the end of the run.
    """

    def __init__(self, run: IterationRun, task: IndexedTaskInstance, executor: AsyncAwareExecutor) -> None:
        self._run = run
        self.task = task
        self._executor = executor
        # The indexed task runs against its own view of the context (see IndexedTaskRunner.indexed_context),
        # with its own outlet events: indexed tasks run concurrently and each needs its events
        # attributed correctly so they can be checkpointed and merged individually (see
        # IndexedTaskState.record_outlet_events and IndexedTaskRunner.merge_outlet_events_into).
        self._runner = IndexedTaskRunner(task_instance=task, register=run.state)

    async def run(self) -> IndexedTaskOutcome:
        """Settle the indexed task from its checkpoint, or run it; return its outcome."""
        task = self.task
        runner = self._runner
        context = self._run.context
        # Checkpoints are consulted only on a retry that is not a rerun after a clear (see Checkpoints).
        if (
            self._run.checkpoints.trust_checkpoints
            and (replayed := await self._replay_checkpoint()) is not None
        ):
            return replayed

        # Pulled before the kill and reached after it. on_kill() reaches the operators that started
        # and the stop flag keeps the executor from pulling more; an indexed task is neither until its runner
        # starts, so a kill that lands while its checkpoint is read above would otherwise start it
        # after the kill. A sync indexed task is still handed to the pool below, whose threads are as many
        # as the calls in flight, so only that pickup remains.
        if self._run.state.stop_requested():
            return IndexedTaskOutcome(
                task,
                exception=IndexedTaskInstanceNotStarted(
                    f"Indexed task {task.task_id}[{task.index}] was pulled before the kill and never started"
                ),
            )

        try:
            if task.is_async:
                with runner:
                    result = await runner.arun(context)
            else:
                # Entered and exited in the worker thread with execute, so what the exit does runs
                # there too, as the reports below do (see _report).

                def run_indexed_task():
                    with runner:
                        return runner.run(context)

                result = await self._executor.run_sync(run_indexed_task)

            indexed_task_state = IndexedTaskState(
                status=TaskInstanceState.SUCCESS,
                fingerprint=task.input_fingerprint,
                try_number=task.try_number,
            )
            # The result is checkpointed as well as pushed to XCom: the runner deletes every XCom
            # key listed by the server before each attempt (xcom_keys_to_clear), so XCom alone
            # cannot survive a retry, while the state store does. The checkpoint is recorded here,
            # inside the coroutine, and written with its round (see CheckpointBatches), so the
            # writes overlap with the indexed tasks still running. With a state_store_backend
            # configured the checkpoint holds only a reference to the payload.
            if result is not None and task.do_xcom_push:
                indexed_task_state.result = result
            elif task.pushed_return_value is not None:
                # Pushed by the operator itself, which do_xcom_push does not govern: the retry
                # replays it as it replays the other pushed XComs.
                indexed_task_state.result = task.pushed_return_value
            indexed_task_state.record_outlet_events(runner.outlet_events)
            # Written with the one checkpoint, not per push: a retry that skips this indexed task pushes
            # them again, as the runner has deleted them by then.
            if task.pushed_xcoms:
                indexed_task_state.xcoms = dict(task.pushed_xcoms)
            await self._run.checkpoints.record(task, indexed_task_state)
        except (asyncio.CancelledError, AirflowTaskTimeout) as stopped:
            if isinstance(stopped, AirflowTaskTimeout) and runner.timed_out_on_its_own:
                # Raised by the operator in its worker thread, which the parent's signal never
                # reaches: this indexed task's own failure, as it is the mapped task instance's under
                # .expand(); the siblings go on.
                return await self._record_failure(stopped)
            # Not this indexed task's outcome: it is being stopped from outside, by the executor
            # cancelling it or by the parent's execution_timeout, whose signal handler raises on the
            # main thread in whichever indexed task happens to run there. Both go on unchanged, so a
            # cancellation stays one and the timeout reaches the runner, which retries the task. The
            # indexed task the timeout struck is reported with the others (a cancelled one has nothing).
            if isinstance(stopped, asyncio.CancelledError):
                # A sync indexed task's thread may still be running: it gets no checkpoint from here on,
                # so its exit must report nothing either (see IndexedTaskRunner.cancel).
                runner.cancel()
            if runner.failure is not None:
                self._run.outcomes.note_failed(runner)
            raise
        except AirflowSkipException as skipped:
            return await self._record_skip(skipped)
        except BaseException as e:
            return await self._record_failure(e)

        return await self._publish(indexed_task_state, result)

    @keeps_failure_to_its_indexed_task
    async def _replay_checkpoint(self) -> IndexedTaskOutcome | None:
        """
        Settle the indexed task from its checkpoint on a retry, or return None for it to run.

        A checkpoint the run's :class:`Checkpoints` does not trust (from before a clear, or for
        another input) is not replayed. A SUCCESS checkpoint is replayed: its result, other XComs and
        outlet events are pushed again, as the runner deleted the XComs before this attempt. A
        SKIPPED one is skipped again. A read or a replay that fails is the indexed task's outcome and
        leaves the checkpoint as it is: the indexed task cannot tell whether it already succeeded, so
        it does not run, and the next retry reads or replays the checkpoint again. A checkpoint serde
        refuses to restore is the exception: it would be refused on every retry, so the indexed
        task runs again as if there were none, with a warning.
        """
        task = self.task
        log = self._run.log
        try:
            indexed_task_state = self._run.checkpoints.read(task)
        except CheckpointDeserializationError as e:
            # Refused on this attempt, it is refused on every one: kept as the outcome, the indexed
            # task could never finish again, so it runs as if there were no checkpoint.
            log.warning(
                "Running task instance %s for %s again: its checkpoint cannot be restored: %s",
                task.index,
                task.task_id,
                e,
            )
            return None
        if indexed_task_state is None:
            return None
        if not self._run.checkpoints.trusts(indexed_task_state, task.input_fingerprint):
            log.info(
                "Running task instance %s for %s again: its checkpoint is from before the task was "
                "cleared or for another input",
                task.index,
                task.task_id,
            )
            return None
        if indexed_task_state.status == TaskInstanceState.SUCCESS:
            log.info(
                "Skipping task instance %s for %s which already succeeded on a previous attempt",
                task.index,
                task.task_id,
            )
            if indexed_task_state.result is not None:
                await self._push_result(indexed_task_state.result)
            for key, value in (indexed_task_state.xcoms or {}).items():
                await task.axcom_push(key=key, value=value)
            if indexed_task_state.outlet_events:
                indexed_task_state.replay_outlet_events(self._run.context["outlet_events"])
            return IndexedTaskOutcome(task)
        if indexed_task_state.status == TaskInstanceState.SKIPPED:
            return IndexedTaskOutcome(
                task,
                exception=AirflowSkipException(
                    f"Indexed task {task.task_id}[{task.index}] was skipped on a previous attempt"
                ),
            )
        return None

    @keeps_failure_to_its_indexed_task
    async def _publish(self, indexed_task_state: IndexedTaskState, result: Any | None) -> IndexedTaskOutcome:
        """
        Report the succeeded indexed task and publish its result, once its SUCCESS outcome is recorded.

        The outcome becomes durable with its round (see :class:`CheckpointBatches`), so the report does
        not wait for the write: an attempt killed hard before the round was written runs the indexed
        task again on the retry, and its success callback fires again, as the operator's own work
        would run again after a kill during ``execute``. A report or a push that fails, a callback
        raising a BaseException included, leaves the recorded SUCCESS as it is, so the retry replays
        the result (see :meth:`_replay_checkpoint`) instead of running the operator again, and
        reports nothing again.
        """
        await self._report(self._runner.report_success, self.task.task.on_success_callback)
        # The checkpoint holds a result when the indexed task returned one with do_xcom_push, or pushed
        # return_value itself, so it alone decides whether there is a return_value_<index> to push.
        if indexed_task_state.result is not None:
            await self._push_result(indexed_task_state.result)
        self._runner.merge_outlet_events_into(self._run.context["outlet_events"])
        return IndexedTaskOutcome(self.task, result)

    @keeps_failure_to_its_indexed_task
    async def _record_skip(self, skipped: AirflowSkipException) -> IndexedTaskOutcome:
        """
        Checkpoint the skipped indexed task as SKIPPED, report it and return the skip as the outcome.

        A BaseException from ``on_skipped_callback`` is the indexed task's outcome, as for a success,
        and leaves the recorded SKIPPED, which the retry replays.
        """
        task = self.task
        await self._run.checkpoints.record(
            task,
            IndexedTaskState(
                status=TaskInstanceState.SKIPPED,
                fingerprint=task.input_fingerprint,
                try_number=task.try_number,
            ),
        )
        await self._report(self._runner.report_skip, task.task.on_skipped_callback)
        return IndexedTaskOutcome(task, exception=skipped)

    async def _record_failure(self, exception: BaseException) -> IndexedTaskOutcome:
        """Note the failed indexed task for its callbacks, checkpoint it as UP_FOR_RETRY and return it as the outcome."""
        task = self.task
        if self._runner.failure is not None:
            self._run.outcomes.note_failed(self._runner)
        # Recorded with the input and the attempt, like the other outcomes, so the next attempt
        # tells a plain retry apart from a clear or a changed input and logs only the latter. A
        # round write that fails keeps the outcome for the next one (see CheckpointBatches); anything
        # else recording raises leaves the outcome as it is too: the indexed task failed either way,
        # and without the checkpoint the retry runs it again, as it would after UP_FOR_RETRY.
        try:
            await self._run.checkpoints.record(
                task,
                IndexedTaskState(
                    status=TaskInstanceState.UP_FOR_RETRY,
                    fingerprint=task.input_fingerprint,
                    try_number=task.try_number,
                ),
            )
        except (asyncio.CancelledError, AirflowTaskTimeout):
            raise
        except Exception:
            self._run.log.exception(
                "Could not checkpoint task instance %s for %s as up for retry", task.index, task.task_id
            )
        return IndexedTaskOutcome(task, exception=exception)

    async def _report(self, report: Callable[[], None], callbacks: Sequence[Callable]) -> None:
        """
        Run the indexed task's success or skip report where its code ran.

        On the loop for an async operator, in a worker thread for a sync one, where a synchronous
        SDK call made from the callback waits its turn for the channel instead of holding up the loop.
        Without ``callbacks`` the report only sets the state and ``end_date``, which makes no SDK
        call, so it runs on the loop and spares the indexed task a thread hand-off.
        """
        if self.task.is_async or not callbacks:
            report()
        else:
            await self._executor.run_sync(report)

    async def _push_result(self, value: Any) -> None:
        """Push ``value`` as the indexed task's return value."""
        await self.task.axcom_push(key=BaseXCom.XCOM_RETURN_KEY, value=value)


class IterationRun:
    """
    One run of an iterated task, driving every index of its input to an outcome.

    Made by :meth:`IterableOperator.execute` and dropped with the run. It holds what the run shares
    between its indexed tasks, which the operator, a Dag node, has no use for between runs: the
    context, the :class:`Checkpoints`, the :class:`IndexedTaskOutcomes` and the resolved input. Its
    collaborators do the rest: the :class:`IndexedTaskUnmapper` builds each index's indexed task, an
    :class:`IndexedTaskLifecycle` takes that task to its outcome, and the operator's
    :class:`IterationState`, handed in as it exists before the run, is what a kill reaches.
    """

    # How long the run waits for its threads to end once it is over: the executor's worker and
    # coroutine shutdown, and the thread on_kill() kills the indexed operators from.
    _SHUTDOWN_TIMEOUT: float = 10.0

    def __init__(
        self,
        *,
        operator: IterableOperator,
        unmapper: IndexedTaskUnmapper,
        state: IterationState,
        context: Context,
    ) -> None:
        self._operator = operator
        self.unmapper = unmapper
        self.state = state
        self.context = context
        #: How many indexed tasks run at once, which is also how many outcomes a checkpoint batch holds.
        self.max_workers = operator.max_workers
        self.checkpoints = Checkpoints(context, round_size=self.max_workers, log=self.log)
        self.outcomes = IndexedTaskOutcomes(operator, state, context)
        #: The input resolved for this task instance, once ``aresolve`` returned.
        self.resolved: Resolved | None = None

    @property
    def log(self) -> Logger:
        """The iterated task's logger: what the run logs belongs to that task."""
        return self._operator.log

    @property
    def length(self) -> int | None:
        """How many indexed tasks the resolved input gives, or None while it is still being resolved."""
        return self.resolved.length if self.resolved is not None else None

    def run(self) -> tuple[XComIterable | None, list[int]]:
        """Run every index to completion; return the task's result and the skipped indices."""
        skipped = self._run_indexed_tasks()
        if not (self.unmapper.do_xcom_push and self.resolved):
            return None, skipped
        ti = self.context["ti"]
        # Where the items were pushed: inside a loop, every reader then reads this pass's items.
        own = _own_coordinate(ti)
        result = XComIterable(
            task_id=self._operator.task_id,
            dag_id=self._operator.dag_id,
            run_id=self.context["run_id"],
            length=self.length,
            map_index=ti.map_index,
            skipped=skipped,
            region_id=own.region_id,
            region_index=own.region_index,
        )
        return result, skipped

    async def _indices(self) -> AsyncIterator[int]:
        # Resolved by the executor on the running event loop, so the input's XCom reads go
        # through asend and do not hold up the indexed tasks' own SDK calls (see
        # AsyncAwareExecutor.imap_unordered). Each index is read by its own indexed task (see
        # _run_index), so what reading it raises is that index's outcome.
        self.resolved = await self._operator.expand_input.aresolve(self.context)
        for index in range(self.resolved.length):
            yield index

    def _run_indexed_tasks(self) -> list[int]:
        """Run every index to completion; return the skipped indices."""
        max_workers = self.max_workers
        self.log.info("Running tasks with %d workers", max_workers)
        with self.checkpoints, self.outcomes:
            try:
                # The checkpoints left are written once the loop closed, failure or not.
                with (
                    self.checkpoints.flushed(),
                    event_loop() as loop,
                    AsyncAwareExecutor(
                        loop=loop, max_workers=max_workers, shutdown_timeout=self._SHUTDOWN_TIMEOUT
                    ) as executor,
                ):
                    try:
                        for outcome in executor.imap_unordered(
                            partial(self._run_index, executor),
                            self._indices(),
                            stop=self.state.stop_requested,
                        ):
                            self.outcomes.record(outcome)
                    except BaseException:
                        # Whatever ends the loop early (the parent's execution_timeout, a failure that
                        # stops the task) is followed by the executor cancelling the coroutines, which
                        # would leave nothing registered for on_kill(); kill what is in flight first,
                        # off the loop thread, so that an indexed operator's synchronous SDK call in on_kill
                        # waits its turn while the loop goes on serving the indexed tasks' calls in flight.
                        # Awaited here, unlike on_kill()'s own thread: the loop keeps running meanwhile.
                        self.state.request_stop()
                        in_flight = self.state.take_in_flight()
                        # On a thread of its own, as on_kill()'s is, and waited for as long as the
                        # SIGTERM path waits: an on_kill that never returns (a cancel call with no
                        # network timeout) holds the run for _SHUTDOWN_TIMEOUT, not forever.
                        self.state.start_kill(partial(self.state.kill, in_flight))
                        try:
                            loop.run_until_complete(
                                asyncio.to_thread(self.state.await_kill, self._SHUTDOWN_TIMEOUT)
                            )
                        except RuntimeError:
                            self.state.await_kill(self._SHUTDOWN_TIMEOUT)
                        raise
            finally:
                # The kill on_kill() started runs on its own thread and may still be cleaning an
                # indexed operator up once the killed indexed tasks came back and the loop closed. Waited
                # for here, before the failure callbacks fire and the run concludes, so the task does
                # not end with that cleanup half done. The loop is closed, so a synchronous SDK call
                # in that on_kill no longer has an asend to wait for.
                self.state.await_kill(self._SHUTDOWN_TIMEOUT)
            return self.outcomes.conclude(self.length)

    async def _run_index(self, executor: AsyncAwareExecutor, index: int) -> IndexedTaskOutcome:
        """
        Read the input of ``index`` and run its indexed task.

        Reading the input is part of the index's run, as resolving its input is part of a mapped
        task instance's run under ``.expand()``: a ``.map()`` callable that raises, or a template
        that fails to render, ends this index and no other (see :class:`UnreadIndex`).
        Only an ``Exception`` is taken as the index's outcome; a cancellation or the parent's
        timeout stops the run as a whole.
        """
        task_id = self._operator.task_id
        if self.state.stop_requested():
            return IndexedTaskOutcome(
                UnreadIndex(task_id, index),
                exception=IndexedTaskInstanceNotStarted(
                    f"Indexed task {task_id}[{index}] was pulled before the kill and never started"
                ),
            )
        resolved = self.resolved
        if TYPE_CHECKING:
            assert resolved is not None
        try:
            mapped_kwargs = await resolved.aget(index)
            # Rendering may call the supervisor synchronously (an XComArg in a partial kwarg,
            # ``{{ var.value.x }}``, ``{{ conn.x }}``): on the loop thread that call would hold up
            # every indexed task for its round trip, from a worker thread it waits its turn while the
            # loop runs, as in XComArg.aresolve.
            task = await asyncio.to_thread(self.unmapper.unmap, index, mapped_kwargs)
        except Exception as exception:
            return IndexedTaskOutcome(UnreadIndex(task_id, index), exception=exception)
        return await IndexedTaskLifecycle(self, task, executor).run()


class IterableOperator(BaseOperator):
    """
    Operator used for Iterable Tasks (IT) that runs a mapped operator over an iterable input.

    The IterableOperator wraps a :class:`MappedOperator` together with an
    :class:`ExpandInput` and is responsible for creating and running the
    per-index runtime task instances. The IterableOperator itself participates
    in Airflow's native retry mechanism — its ``retries`` and ``retry_delay``
    are inherited from the wrapped operator so that when any indexed task needs
    a retry the whole IterableOperator is retried by Airflow. Already-succeeded
    indexed tasks are skipped on each retry attempt because their state is
    checkpointed in the ``task_state_store``.

    The IterableOperator executes the mapped operator instances using a
    concurrent executor with a configurable number of workers. By default
    the worker count is taken from the mapped operator's ``partial_kwargs``
    (``task_concurrency``) if present, otherwise falls back to
    ``os.cpu_count()`` and finally to ``1``. ``os.cpu_count()`` counts the CPUs of
    the machine: a worker in a container with a CPU limit still sees every CPU of
    its node, so set ``task_concurrency`` there.

    **Crash recovery:** When the worker crashes mid-iteration and the task is re-run (e.g. via a
    manual clear), already-succeeded indexed tasks are skipped and only the pending/failed ones are
    executed again. Every indexed task inherits its ``try_number`` from the IterableOperator's own task
    instance, so the attempt count reported to an indexed task matches the attempt Airflow is currently
    running. The checkpoint is only consulted from the second attempt onwards, and solely to decide
    whether an index already succeeded. Once every index has succeeded, a completion marker is written
    so that a *subsequent* manual clear (which does not reset ``try_number``) re-runs every index from
    scratch instead of replaying the previous run's stale results (see :class:`Checkpoints`). A
    checkpoint carries the indexed task's full result (plus its extra XComs and outlet events) and lives in
    the ``task_state_store`` table for the store's retention, unless a ``[workers]
    state_store_backend`` is configured and only a reference is stored; a custom XCom backend alone
    does not keep large results out of the metadata database.

    :param operator: The :class:`MappedOperator` to unmap and execute for
        each element of ``expand_input``. Each indexed task runs a new instance of it,
        built by ``unmap()`` from that index's kwargs; the partial kwargs are shared
        between the instances, not copied (see the warning on shared inputs below).

    :param expand_input: Provider of the values to iterate
        over. Its ``aresolve(context)`` method gives the indexed task count and the
        per-index ``mapped_kwargs`` used to unmap the operator.

    :param kwargs: Additional keyword arguments forwarded to
        :class:`BaseOperator` when instantiating the IterableOperator
        (e.g. ``dag``, ``start_date``).

    :returns: An :class:`XComIterable` if the mapped operator pushes XComs, otherwise ``None``.

    .. note::
        ``multiple_outputs`` is ignored for iterated tasks. Each indexed task's return value is pushed
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
        ``TriggerDagRunOperator``) and skipping downstream tasks are not supported either: an indexed task
        index has no DAG run or downstream tasks of its own for the trigger/skip to apply to.
        Operators that can skip downstream tasks (``ShortCircuitOperator``, the branch operators,
        ``@task.short_circuit``, ``@task.branch`` and any other ``SkipMixin``) are rejected by
        ``.iterate()`` itself, since inside an iteration they would find nothing to skip and let
        every downstream task run. A trigger, or a
        :class:`~airflow.sdk.exceptions.DownstreamTasksSkipped` raised anyway, fails the whole
        IterableOperator immediately with a clear error rather than silently doing nothing.

        Indexed task outcomes are classified before being aggregated: if any indexed task raises
        :class:`~airflow.sdk.exceptions.AirflowFailException`, that exception is re-raised directly so
        the IterableOperator fails without retrying. An indexed task that raises
        :class:`~airflow.sdk.exceptions.AirflowSkipException` is skipped, as a mapped task instance
        would be: it pushes no XCom, does not fail the task and is not run again on a retry. It is
        left out of the task's :class:`~airflow.sdk.bases.xcom.XComIterable`, so downstream tasks
        only see the values that exist. An indexed task that returns ``None`` is not skipped: it succeeds,
        pushes no XCom and keeps its position in the sequence, which reads ``None`` there, so a
        downstream ``.expand()`` over the result runs over it; ``.expand()`` leaves a mapped task
        instance that returned ``None`` out of the count instead. A direct downstream task whose trigger rule skips it when an
        upstream task instance is skipped (``all_success``, ``none_skipped``,
        ``all_done_min_one_success``) is skipped, as after a mapped upstream; one with a rule such
        as ``none_failed`` runs over the remaining values. If *every* indexed task is skipped, a single
        ``AirflowSkipException`` is re-raised so the IterableOperator itself is marked ``SKIPPED``, and
        so is it over an empty input, as a mapped task over nothing is. All other exceptions of the
        indexed tasks are aggregated into a :class:`BaseExceptionGroup` and treated as a regular
        retryable failure.

    .. warning::
        **Inputs are shared between iterations.**

        All iterations run in one process, so a value handed to several of them is the same object
        in each of them: every value passed through ``.partial()``, and with
        ``.iterate(a=..., b=...)`` every element of ``a`` and of ``b``, which the cross product
        combines more than once. A mapped task instance gets its own copy, because it runs in its
        own process; an iteration does not. Treat inputs as read-only, or copy what the task
        changes in place.

    .. note::
        **Callbacks run per indexed task, and a failed one's wait for the task's fate.**

        ``on_success_callback`` and ``on_skipped_callback`` run once the indexed task's checkpoint is
        written, so they speak for work a retry will not run again, where it ran: in its worker
        thread for a sync operator, on the event loop for an async one. A failed indexed task's
        ``on_failure_callback`` or ``on_retry_callback`` runs once every indexed task has run, on the
        thread that ran the iteration, and says what happens to the task: retried or failed for good
        (see :class:`IndexedTaskOutcomes`). A failure no indexed task
        owns, such as an error resolving the input, fires no callback, and neither does an indexed
        task pulled before a kill and never started: the iterated task has no callbacks of its own.
        Listeners fire once, for the task instance, as for any task; an indexed task is not a task
        instance and fires none.

    .. note::
        **Pools count the task instance, not its iterations.**

        The scheduler reserves ``pool_slots`` once for the iterated task, while up to
        ``task_concurrency`` iterations run inside it. A pool sized to cap the load on a shared
        resource (database connections, the rate limit of an API) therefore sees one reservation
        for that many concurrent uses. Choose ``task_concurrency`` with the pool in mind; reserving
        slots per iteration needs support in the scheduler, which does not exist yet.

    .. note::
        **``map_index_template`` is not applied.**

        Under ``.expand()`` the template names each mapped task instance after its item. An
        iterated task is one task instance with no item of its own, so the wrapped operator's
        template is left out: rendered against the task's context, a template that reads an item's
        kwargs (``{{ task.op_kwargs['x'] }}``) or a value an indexed task set in its context would fail.

    .. note::
        **Async indexed tasks should prefer async SDK calls.**

        IterableOperator runs multiple async indexed tasks concurrently on the same event loop, each
        making async SDK calls of its own (checkpointing, XCom push). If an async indexed task's
        ``aexecute()`` — or a hook/callback it calls — issues a *synchronous* SDK call instead (e.g.
        ``Variable.get``, ``BaseHook.get_connection``/``get_hook``, ``ti.xcom_pull``, or a sync
        ``on_success_callback``/``pre_execute``), the call works but blocks the event loop, and so
        every indexed task on it, for its round trip to the supervisor, waiting first for a sibling's
        call in flight. Use the async equivalents inside async operators:
        :meth:`~airflow.sdk.bases.hook.BaseHook.aget_connection`/``aget_hook``,
        ``ti.axcom_pull`` and ``Variable.aget``/``aset``. Sync indexed tasks are not concerned: they run
        in worker threads, their ``execute``, hooks and callbacks included, where a synchronous SDK
        call waits its turn without holding up the loop; so does ``on_kill`` of the indexed operators,
        which :meth:`on_kill` runs off the loop thread.

    .. warning::
        **``execution_timeout`` caps the whole iteration, not each indexed task.**

        The IterableOperator keeps the wrapped operator's ``execution_timeout`` as a wall-clock limit
        on the entire task instance. The runner enforces it on the main thread exactly as for any
        other task, so an iteration that overruns fails with ``AirflowTaskTimeout`` and
        :meth:`on_kill` is propagated to every indexed task still in flight. Since ``.iterate()`` runs
        all indexed tasks in one task instance, this is the per-instance limit of ``.expand()`` applied
        to the whole iteration rather than to each indexed task: no limit applies per indexed task,
        sync or async (one of the same value, started later, could never fire first). An
        ``AirflowTaskTimeout`` a sync indexed task raises itself (a hook that gave up waiting) is that
        indexed task's own failure, as it is the mapped task instance's under ``.expand()``.
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
        # An iteration has no downstream tasks of its own, so ShortCircuitOperator, the branch operators
        # and any other SkipMixin would skip nothing and let every downstream task run. Checked on the
        # class: MappedOperator._can_skip_downstream is only derived from SkipMixin on the classic path,
        # while the @task path copies a class default that is False even for @task.short_circuit and
        # @task.branch.
        if issubclass(operator.operator_class, SkipMixin):
            raise TypeError(
                f"{operator.operator_name} can skip downstream tasks and cannot be iterated: an iteration "
                f"of {operator.task_id!r} has no downstream tasks of its own, so it would skip nothing and "
                "every downstream task would run. Use .expand() for it instead."
            )

        super().__init__(
            **{
                **kwargs,
                "task_id": operator.task_id_without_group_prefix,
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
                # class docstring); no limit applies per indexed task.
                "execution_timeout": operator.execution_timeout,
                "trigger_rule": operator.trigger_rule,
                "resources": operator.resources,
                "run_as_user": operator.run_as_user,
                # Not forwarded: the template names a mapped task instance after its item, and the
                # iterated task instance has no item of its own. Rendered for it against the
                # parent's context, a template that reads an item's kwargs or a value one indexed task set
                # would fail the task after every indexed task succeeded, and every retry with it.
                "map_index_template": None,
                "max_active_tis_per_dag": operator.max_active_tis_per_dag,
                "max_active_tis_per_dagrun": operator.max_active_tis_per_dagrun,
                "executor": operator.executor,
                "executor_config": operator.executor_config,
                "do_xcom_push": operator.partial_kwargs.get("do_xcom_push", True),
                # Ignored for iterated tasks (also when passed explicitly): the return value pushed by the
                # runner is the XComIterable aggregate, not a dict, and every indexed task result is pushed
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
                # indexed task, from the wrapped operator's partial kwargs. Passed explicitly, since
                # _apply_defaults would otherwise fill them from the DAG's default_args and the
                # runner would run them for the task on top of the indexed tasks' own.
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
        # unmap() would normally apply these three flags to each generated indexed operator, and
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
        # What a kill of the run must reach (see IterationState); fresh for every run and for every
        # copy of the operator.
        self._state = IterationState(self.log)

    def __copy__(self) -> IterableOperator:
        # prepare_for_execution copies the operator with copy.copy, which would share the state of
        # the run with the Dag's operator: a kill in one attempt would then stop the next one under
        # dag.test(). The copy is another run and starts with a state of its own (see IterationState).
        other = type(self).__new__(type(self))
        other.__setstate__({**self.__getstate__(), "_state": copy.copy(self._state)})
        return other

    def on_kill(self) -> None:
        # The default BaseOperator.on_kill() is a no-op, which would otherwise leave every
        # currently in-flight indexed task unaware that the IterableOperator itself was killed
        # (SIGTERM) or hit its execution_timeout: propagate to each active indexed operator instead.
        # First stop the iteration from starting anything else: the killed indexed tasks come back
        # as failures and free their slots, which would otherwise be filled with the next ones.
        state = self._state
        state.request_stop()
        # Always in a thread of its own, which takes the indexed operators in flight itself. The
        # runner's SIGTERM handler calls this on the main thread, between two bytecodes of the loop
        # thread: a lock taken here that register() or unregister() holds at that moment would
        # hang the handler, so nothing here takes one (see IterationState). On that thread a
        # synchronous SDK call in an indexed operator's on_kill would also hold up the event loop, and
        # every indexed task on it, for its round trip; in its own thread the call waits its turn and
        # the loop goes on serving the indexed tasks. IterationRun.run kills what is in
        # flight through IterationState.kill directly, from a thread the loop drives, and waits for
        # this thread before the run concludes (see IterationState.await_kill).
        state.start_kill(lambda: state.kill(state.take_in_flight()))

    def deserialization_allowed_values(self) -> list[Any]:
        """Return the values the operator this task iterates declares: every indexed task pushes what it does."""
        return self._operator.deserialization_allowed_values()

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

    def iter_expansion_sources(self) -> Iterator[AbstractOperator]:
        # The upstream tasks of the input it iterates. Not reported as mapped dependencies: the
        # iterated task is not mapped, and the scheduler must not count those upstream items.
        for operator, _ in XComArg.iter_xcom_references(self.expand_input):
            yield operator

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

    def execute(self, context: Context):
        try:
            state = self._state
            result, skipped = IterationRun(
                operator=self,
                unmapper=IndexedTaskUnmapper(
                    self, self._operator, context, stop_requested=state.stop_requested
                ),
                state=state,
                context=context,
            ).run()
            if skipped:
                self._skip_downstream_of_a_partial_skip(context, result)
            return result
        finally:
            # Renewed once the run ended, not when it starts (see IterationState): a kill that lands
            # before the run must stop it, and a rerun in the same process must not see that kill.
            self._state = IterationState(self.log)
