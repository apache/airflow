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
The durable journal: replay an agent's completed steps when Airflow retries its task.

An Airflow task retry is a fresh process that runs the agent again from the top. The
journal records each step an agent run completes (a model response, a tool result, a
capability operation) under the step's position in that run, and on the retry hands
back the recorded result instead of running the step again. Steps the previous
attempt never reached run live and are recorded in turn.

A step is replayed only when the step at the same position in the previous attempt
had the same name and the same fingerprint. The name says what kind of step it was
(``agent__model.request``, ``agent__function_toolset__x.call_tool:query``); the
fingerprint, computed by the framework adapter, says what it was asked (the message
history, the tool arguments). The first mismatch means the run took a different path
from the previous attempt, so that step and every step after it run live. So does
everything the previous attempt did after a step that raised.

A :class:`DurableJournal` belongs to one task attempt. Each agent run in it is a
:class:`DurableRun`, numbered in the order the runs start, with positions of its own,
so a task that runs several agents replays each of them independently.

Nothing here depends on an agent framework. Payloads are JSON-compatible values and
fingerprints are opaque strings.
:class:`~airflow.providers.common.ai.durable.capability.AirflowDurability` drives the
journal for pydantic-ai. Another framework's adapter starts a run with
:meth:`DurableJournal.start_run` and calls one of two shapes of API on it:

* :meth:`DurableRun.run` wraps a step:
  ``await run.run(name, kind=..., fingerprint=..., body=...)``.
* :meth:`DurableRun.claim` and :meth:`JournalStep.record` split it in two, for a
  framework whose hooks see a step's input and its output in separate callbacks.
"""

from __future__ import annotations

import collections
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

from airflow.providers.common.ai.durable.base import RUN_ID_KEY, RUNS_KEY, build_run_meta_key, build_step_key
from airflow.providers.common.ai.durable.storage import DurableStorage
from airflow.providers.common.ai.observability import make_task_instance_run_key
from airflow.providers.common.ai.utils.task_logger import get_task_logger
from airflow.providers.common.compat.sdk import AirflowException, get_current_context
from airflow.providers.common.compat.version_compat import AIRFLOW_V_3_3_PLUS

if TYPE_CHECKING:
    import logging

    from airflow.providers.common.ai.durable.base import DurableStorageProtocol
    from airflow.sdk import Context

log = get_task_logger()

StepKind = Literal["model", "tool", "other"]
"""What a step is, for the end-of-run summary and for adapters that treat model steps specially."""


@dataclass
class JournalStep:
    """
    One position in a run, claimed by :meth:`DurableRun.claim`.

    When :attr:`replayed` is true, :attr:`payload` holds what the previous attempt
    recorded and the caller returns it instead of running the step. Otherwise the
    caller runs the step inside :meth:`executing` and passes its result to
    :meth:`record`, or the exception it raised to :meth:`fail`; :meth:`run` does all three.
    """

    durable_run: DurableRun = field(repr=False)
    position: int
    name: str
    kind: StepKind
    fingerprint: str | None
    replayable: bool = True
    replayed: bool = False
    payload: Any = None
    # Agent runs started while this step executes; they are numbered under it.
    nested_runs: int = field(default=0, repr=False)

    async def run(
        self, body: Callable[[], Awaitable[Any]], *, to_record: Callable[[Any], Any] | None = None
    ) -> Any:
        """
        Run a step that was not replayed and record what ``body`` returns, or that it raised.

        :param to_record: Transforms the live result before it is recorded, such as
            masking secrets in a tool result. The live result is returned unchanged.
        """
        try:
            with self.executing():
                payload = await body()
        except Exception as error:
            self.fail(error)
            raise
        self.record(payload if to_record is None else to_record(payload))
        return payload

    @contextmanager
    def executing(self) -> Iterator[None]:
        """
        Mark the code in the ``with`` block as this step's work.

        An agent run that starts inside the block, such as an agent a tool calls, is
        numbered under this step, so the runs of later steps keep their numbers on a
        retry that replays this step instead of running it.
        """
        token = _EXECUTING_STEP.set(self)
        try:
            yield
        finally:
            _EXECUTING_STEP.reset(token)

    def record(self, payload: Any) -> bool:
        """
        Record the step's result so a retry can replay it.

        :param payload: A JSON-compatible value.
        :return: Whether it was recorded. A step that was not recorded runs again on retry,
            and one claimed with ``replayable=False`` is never recorded.
        """
        if not self.replayable:
            return False
        entry = {"name": self.name, "kind": self.kind, "fingerprint": self.fingerprint, "payload": payload}
        if self.durable_run.save(self.position, entry):
            self.durable_run.journal.stats.recorded[self.kind] += 1
            log.debug("Durable: recorded step", position=self.position, step=self.name)
            return True
        self.durable_run.journal.stats.not_recorded.append((self.kind, self.name))
        # Named here, not only in the summary: this line is logged on every path,
        # including the failed attempt that Airflow retries.
        log.warning(
            "Durable: step not recorded; a retry runs it again, and may run the steps after it again",
            position=self.position,
            step=self.name,
        )
        return False

    def fail(self, error: BaseException) -> None:
        """
        Note that the step raised ``error``.

        Nothing is recorded for the step, so it runs again on retry. If ``error`` goes on to
        fail the run, :meth:`DurableRun.fail` uses this to tell which steps the run had
        already started when it failed; a run that recovers from the error is unaffected.
        """
        self.durable_run._raised.append((error, self.durable_run.position))


@dataclass
class JournalStats:
    """What one task attempt replayed and recorded, by step kind."""

    replayed: collections.Counter[StepKind] = field(default_factory=collections.Counter)
    recorded: collections.Counter[StepKind] = field(default_factory=collections.Counter)
    # Steps that ran live and could not be recorded, so a retry runs them again.
    not_recorded: list[tuple[StepKind, str]] = field(default_factory=list)


class DurableRun:
    """
    One agent run's steps in the journal.

    Positions are handed out in the order steps are claimed. A step must be claimed
    before its first ``await``, so concurrent steps (parallel tool calls) take their
    positions in the order they were started rather than the order they finish.

    Besides its steps, a run keeps one entry about itself: where the last attempt that
    failed stopped being trustworthy, and the furthest position any attempt reached, so
    a successful run deletes every step an earlier attempt left behind.
    """

    def __init__(self, journal: DurableJournal, key: str) -> None:
        self.journal = journal
        self.key = key
        self.diverged = False
        self._position = 0
        # Read on first use: where the last failed attempt's entries stop being trusted,
        # and how far any attempt got.
        self._diverge_from: int | None = None
        self._high_water = 0
        self._meta_loaded = False
        # Whether the step claimed last replayed; a step without a fingerprint replays only
        # if it did, so it is never matched against a different preceding conversation.
        self._previous_replayed = True
        # Entries read ahead by ``peek`` and not yet claimed.
        self._peeked: dict[int, dict[str, Any] | None] = {}
        # Exceptions steps raised this attempt, with the next position when each was raised.
        self._raised: list[tuple[BaseException, int]] = []
        # The furthest position this attempt recorded a step at.
        self._last_saved = -1

    @property
    def position(self) -> int:
        """The position the next claimed step will take."""
        return self._position

    def _load_meta(self) -> None:
        if self._meta_loaded:
            return
        self._meta_loaded = True
        meta = self.journal.storage.load_step(build_run_meta_key(self.key)) or {}
        diverge_from, high_water = meta.get("diverge_from"), meta.get("high_water")
        self._diverge_from = diverge_from if isinstance(diverge_from, int) else None
        self._high_water = high_water if isinstance(high_water, int) else 0

    def peek(self, position: int) -> dict[str, Any] | None:
        """
        Return the entry the previous attempt recorded at ``position``, without claiming it.

        ``None`` when nothing usable was recorded there, or the run has already diverged.
        The entry is a dict with ``name``, ``kind``, ``fingerprint`` and ``payload``.
        """
        self._load_meta()
        if self.diverged or (self._diverge_from is not None and position >= self._diverge_from):
            return None
        if position not in self._peeked:
            self._peeked[position] = self.journal.storage.load_step(build_step_key(self.key, position))
        return self._peeked[position]

    def claim(
        self, name: str, *, kind: StepKind, fingerprint: str | None, replayable: bool = True
    ) -> JournalStep:
        """
        Take the next position and look up what the previous attempt recorded there.

        :param name: What kind of step this is. Must be the same on every attempt for the
            same step, and should differ between steps that are not interchangeable.
        :param kind: ``"model"``, ``"tool"`` or ``"other"``.
        :param fingerprint: What the step was asked, or ``None`` when it cannot be
            fingerprinted. A step without one replays on a matching name, and only right
            after a step that replayed.
        :param replayable: ``False`` for a step that must always run live, such as a tool
            whose effect Airflow cannot observe. It still takes a position, so the steps
            after it keep theirs, but nothing is looked up or recorded for it.
        """
        position = self._position
        self._position += 1
        step = JournalStep(self, position, name, kind, fingerprint, replayable=replayable)
        self._load_meta()
        if not self.diverged and self._diverge_from is not None and position >= self._diverge_from:
            self._diverge(position, name, reason="the previous attempt failed before reaching this step")
        entry = self.peek(position) if replayable else None
        self._peeked.pop(position, None)
        previous_replayed, self._previous_replayed = self._previous_replayed, False
        if entry is None:
            return step
        if entry.get("name") == name and entry.get("fingerprint") == fingerprint:
            if fingerprint is None and not previous_replayed:
                return step
            step.replayed = self._previous_replayed = True
            step.payload = entry.get("payload")
            self.journal.stats.replayed[kind] += 1
            log.debug("Durable: replayed step", position=position, step=name)
            return step
        self._diverge(
            position,
            name,
            reason=(
                f"the previous attempt ran {entry.get('name')!r} here"
                if entry.get("name") != name
                else "the request differs from the previous attempt's (prompt, model, settings, "
                "tools, arguments or conversation so far)"
            ),
        )
        return step

    async def run(
        self,
        name: str,
        *,
        kind: StepKind,
        fingerprint: str | None,
        body: Callable[[], Awaitable[Any]],
        replayable: bool = True,
        to_record: Callable[[Any], Any] | None = None,
    ) -> Any:
        """Replay the step at the next position, or run ``body`` and record what it returns."""
        step = self.claim(name, kind=kind, fingerprint=fingerprint, replayable=replayable)
        if step.replayed:
            return step.payload
        return await step.run(body, to_record=to_record)

    def save(self, position: int, entry: dict[str, Any]) -> bool:
        """Store ``entry`` at ``position``; ``False`` when the storage skipped it."""
        self._last_saved = max(self._last_saved, position)
        return self.journal.storage.save_step(build_step_key(self.key, position), entry)

    def fail(self, error: BaseException) -> None:
        """
        Record that the run failed with ``error``, so a retry does not replay its error path.

        Steps the run started before the step that raised ``error`` (tool calls running
        alongside it, say) still replay on retry; everything from then on, including what
        error handling recorded, runs again.
        """
        failed_at = self._position
        cause: BaseException | None = error
        while cause is not None:
            failed_at = min([failed_at, *(position for raised, position in self._raised if raised is cause)])
            cause = cause.__cause__ or cause.__context__
        self._load_meta()
        # An attempt that was still replaying an earlier one when it failed, and recorded
        # nothing from the failure on, leaves that earlier attempt's steps as they were.
        diverge_from = failed_at if self.diverged or self._last_saved >= failed_at else self._diverge_from
        meta = {"diverge_from": diverge_from, "high_water": max(self._high_water, self._position)}
        self.journal.storage.save_step(build_run_meta_key(self.key), meta)

    def cleanup(self) -> None:
        """Delete this run's steps. Call only once the run, and anything that consumes it, has succeeded."""
        self._load_meta()
        end = max(self._high_water, self._position)
        # An attempt killed before it could note how far it got may have gone further.
        while self.journal.storage.load_step(build_step_key(self.key, end)) is not None:
            end += 1
        keys = [build_step_key(self.key, position) for position in range(end)]
        self.journal.storage.delete_steps([*keys, build_run_meta_key(self.key)])
        self.journal._forget_run(self.key)

    def reject(self, step: JournalStep, *, reason: str) -> None:
        """
        Run a step that was going to replay live instead, and every step after it.

        For an adapter that finds the recorded payload unusable after :meth:`claim`
        matched it, such as one written by a version whose types have changed since.
        """
        if not step.replayed:
            return
        step.replayed = False
        step.payload = None
        self._previous_replayed = False
        self.journal.stats.replayed[step.kind] -= 1
        self._diverge(step.position, step.name, reason=reason)

    def _diverge(self, position: int, name: str, *, reason: str) -> None:
        self.diverged = True
        self._peeked.clear()
        log.warning(
            "Durable: the run took a different path from the previous attempt; this step and "
            "every step after it run again",
            position=position,
            step=name,
            reason=reason,
        )


class DurableJournal:
    """
    One task attempt's view of the durable journal.

    :param storage: Where steps are stored; see :func:`build_task_storage`.
    :param clean_up_after_run: Delete a run's steps as soon as it succeeds. A journal the
        operator manages leaves this off and calls :meth:`cleanup` itself after the
        task's own post-run work has succeeded.
    """

    def __init__(self, storage: DurableStorageProtocol, *, clean_up_after_run: bool = False) -> None:
        self.storage = storage
        self.clean_up_after_run = clean_up_after_run
        self.stats = JournalStats()
        self._runs: list[DurableRun] = []
        self._top_level_runs = 0
        self._run_id: str | None = None
        # Keys of every run any attempt started, read on first use. Cleanup walks it, so a run
        # an earlier attempt started that this one never reaches (an agent called from a tool
        # that now replays) is still deleted.
        self._known_runs: list[str] | None = None

    def get_run_id(self, *, default: str) -> str:
        """
        Return the id that names the task's agent run on every attempt.

        The first attempt stores ``default`` beside the journal; a retry reads it back. It
        is deleted with the journal when the task succeeds, so a later clear of the task
        starts over with a new id. Capabilities that key their own state on the agent's
        ``run_id``, such as pydantic-ai-harness ``SpendLimits`` and ``StepPersistence``,
        need it to stay the same across a replay.

        :param default: The id to use when no earlier attempt stored one, such as the
            first attempt's task instance id.
        """
        if self._run_id is None:
            entry = self.storage.load_step(RUN_ID_KEY)
            stored = entry.get("run_id") if entry is not None else None
            if isinstance(stored, str):
                self._run_id = stored
            else:
                self._run_id = default
                self.storage.save_step(RUN_ID_KEY, {"run_id": default})
        return self._run_id

    def start_run(self) -> DurableRun:
        """
        Start an agent run.

        A run started while a step of this journal executes (an agent a tool calls) is
        numbered under that step; any other run takes the next top-level number.
        """
        parent = _EXECUTING_STEP.get()
        if parent is not None and parent.durable_run.journal is self:
            parent.nested_runs += 1
            key = f"{parent.durable_run.key}.{parent.position}.{parent.nested_runs}"
        else:
            key = str(self._top_level_runs)
            self._top_level_runs += 1
        durable_run = DurableRun(self, key)
        self._runs.append(durable_run)
        known = self._load_known_runs()
        if key not in known:
            known.append(key)
            self.storage.save_step(RUNS_KEY, {"runs": known})
        return durable_run

    def _load_known_runs(self) -> list[str]:
        if self._known_runs is None:
            entry = self.storage.load_step(RUNS_KEY) or {}
            runs = entry.get("runs")
            self._known_runs = [key for key in runs if isinstance(key, str)] if isinstance(runs, list) else []
        return self._known_runs

    def cleanup(self) -> None:
        """Delete the steps of every run in this attempt. Call only once the task's work has succeeded."""
        started = {durable_run.key: durable_run for durable_run in self._runs}
        for key in dict.fromkeys([*self._load_known_runs(), *started]):
            (started.get(key) or DurableRun(self, key)).cleanup()
        self.storage.delete_steps([RUN_ID_KEY])
        log.debug("Durable journal cleaned up")

    def _forget_run(self, key: str) -> None:
        known = self._load_known_runs()
        if key not in known:
            return
        known.remove(key)
        if known:
            self.storage.save_step(RUNS_KEY, {"runs": known})
        else:
            self.storage.delete_steps([RUNS_KEY])

    def log_summary(self, logger: logging.Logger | Any) -> None:
        """Log what this attempt replayed and recorded, and which steps a retry would run again."""
        replayed, recorded = self.stats.replayed, self.stats.recorded
        logger.info(
            "Durable: replayed %d steps (%d model, %d tool, %d other), recorded %d new steps "
            "(%d model, %d tool, %d other)",
            replayed.total(),
            replayed["model"],
            replayed["tool"],
            replayed["other"],
            recorded.total(),
            recorded["model"],
            recorded["tool"],
            recorded["other"],
        )
        if tools := [name for kind, name in self.stats.not_recorded if kind == "tool"]:
            counts = collections.Counter(tools)
            logger.warning(
                "Durable: %d tool results were not recorded, and a retry runs them again: %s",
                len(tools),
                ", ".join(name if n == 1 else f"{name} (x{n})" for name, n in counts.items()),
            )
        if models := sum(1 for kind, _ in self.stats.not_recorded if kind == "model"):
            logger.warning(
                "Durable: %d model responses were not recorded, and a retry re-runs them "
                "and every step after the first of them",
                models,
            )


def build_task_storage(context: Context) -> DurableStorageProtocol:
    """
    Return the journal storage for the task instance ``context`` belongs to.

    On Airflow >= 3.3 the journal lives in the AIP-103 task state store, which handles
    persistence and large-value offload, so ``[common.ai] durable_cache_path`` is not
    needed. On older versions it is a JSON file under that ObjectStorage path.
    """
    if AIRFLOW_V_3_3_PLUS:
        # Imported lazily: NEVER_EXPIRE and the task state store accessor do not
        # exist on Airflow versions before 3.3.
        from airflow.providers.common.ai.durable.task_state_store import TaskStateStoreDurableStorage

        return TaskStateStoreDurableStorage(context["task_state_store"])

    ti = context["task_instance"]
    return DurableStorage(
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        run_id=ti.run_id,
        map_index=ti.map_index if ti.map_index is not None else -1,
    )


_ACTIVE_JOURNAL: ContextVar[DurableJournal | None] = ContextVar("commonai_durable_journal", default=None)
_EXECUTING_STEP: ContextVar[JournalStep | None] = ContextVar("commonai_durable_step", default=None)
# The journal for the task attempt this process is running, when no caller set one
# explicitly. Keyed by the attempt, so a second attempt in the same process (a test,
# a long-lived worker) never reuses the first attempt's runs.
_task_journal: tuple[str, DurableJournal] | None = None


@contextmanager
def journal_scope(journal: DurableJournal) -> Iterator[DurableJournal]:
    """Make ``journal`` the active journal for the code in the ``with`` block."""
    token = _ACTIVE_JOURNAL.set(journal)
    try:
        yield journal
    finally:
        _ACTIVE_JOURNAL.reset(token)


def current_journal() -> DurableJournal | None:
    """
    Return the active journal, or ``None`` outside an Airflow task.

    A journal set with :func:`journal_scope` wins. Otherwise, inside a running task,
    this returns the task attempt's own journal, created on first use, which deletes the
    steps of each run as soon as it succeeds. Outside a task there is nothing to replay
    into, so durable execution is off and the agent runs as it would without it.
    """
    global _task_journal

    if (journal := _ACTIVE_JOURNAL.get()) is not None:
        return journal
    try:
        context = get_current_context()
    except (RuntimeError, AirflowException):
        # Airflow 3 raises RuntimeError outside a task, Airflow 2 AirflowException.
        return None
    attempt = make_task_instance_run_key(context["task_instance"])
    if _task_journal is None or _task_journal[0] != attempt:
        _task_journal = (attempt, DurableJournal(build_task_storage(context), clean_up_after_run=True))
    return _task_journal[1]
