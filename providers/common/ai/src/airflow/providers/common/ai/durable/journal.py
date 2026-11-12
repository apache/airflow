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
everything the previous attempt did after a step that raised, and, when the previous
attempt failed on the model's answers, everything from the model's last request on. A
tool call that raised on two attempts in a row counts as such an answer: the next retry
asks the model again from the request that chose the call.

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

An adapter that stops the task to wait for input (``request_input()``) calls
:meth:`DurableRun.pause` first, so that the resumed task replays what this attempt recorded.
"""

from __future__ import annotations

import collections
from collections.abc import Awaitable, Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, TypedDict

from airflow.providers.common.ai.durable.base import RUN_ID_KEY, RUNS_KEY, build_run_meta_key, build_step_key
from airflow.providers.common.ai.durable.storage import DurableStorage
from airflow.providers.common.ai.exceptions import DurableJournalError
from airflow.providers.common.ai.observability import make_task_instance_run_key
from airflow.providers.common.ai.utils.task_logger import get_task_logger
from airflow.providers.common.compat.sdk import AirflowException, get_current_context
from airflow.providers.common.compat.version_compat import AIRFLOW_V_3_3_PLUS

if TYPE_CHECKING:
    import logging
    from collections.abc import Sequence

    from airflow.providers.common.ai.durable.base import DurableStorageProtocol
    from airflow.sdk import Context

log = get_task_logger()

StepKind = Literal["model", "tool", "other"]
"""What a step is, for the end-of-run summary and for adapters that treat model steps specially."""

# Attempts in a row a tool call may fail before a retry asks the model again; the first retry runs
# the same call again, in case the failure was transient.
_TOOL_FAILURES_BEFORE_ASKING_MODEL = 2


class _ToolFailure(TypedDict):
    """The tool call that failed a run, in the run's entry, and on how many attempts in a row it did."""

    call: list[int | str | None]  # [position, name, fingerprint]
    attempts: int


@dataclass(frozen=True)
class Unrecordable:
    """
    A step result an adapter could not encode as JSON: the live value, and why.

    A step body returns it in place of the payload; :meth:`JournalStep.run` passes it to
    :meth:`JournalStep.cannot_record` instead of recording it, and returns it to the adapter.
    """

    value: Any
    error: Exception


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
    # Whether this step's replay is in ``stats.replayed``; see :meth:`DurableRun.reserve`.
    _replay_counted: bool = field(default=False, init=False, repr=False)
    # The position of the model request that chose this step: the last model step claimed before it.
    _choosing_model_position: int | None = field(default=None, init=False, repr=False)
    # Agent runs started while this step executes; they are numbered under it.
    nested_runs: int = field(default=0, repr=False)

    def count_as_replayed(self) -> None:
        """
        Count this step as replayed, once.

        For a step from :meth:`DurableRun.reserve`, which :meth:`DurableRun.claim_reserved` does;
        a step from :meth:`DurableRun.claim` is already counted.
        """
        if self.replayed and not self._replay_counted:
            self._replay_counted = True
            self.durable_run.journal.stats.replayed[self.kind] += 1

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
        if isinstance(payload, Unrecordable):
            self.cannot_record(payload.error)
        else:
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
        self._mark_not_recorded()
        return False

    def cannot_record(self, error: Exception) -> None:
        """
        Note that the step's result has no form the journal can hold, such as a value with no JSON form.

        :raises DurableJournalError: When the journal fails on such results, as it does by default.
            Otherwise the step is left unrecorded, as when storage skips a write, and runs again on retry.
        """
        if not self.replayable:
            # Never recorded anyway, so there is nothing to fail or report.
            return
        if self.durable_run.journal.fail_on_unrecordable:
            # The same value fails the same way on every retry, so retrying the task cannot help.
            raise DurableJournalError(
                f"durable execution could not record a step's result, because it is not JSON-serializable: {error}"
            ) from error
        log.warning("Durable: step result is not JSON-serializable", step=self.name, error=str(error))
        self._mark_not_recorded()

    def _mark_not_recorded(self) -> None:
        self.durable_run.journal.stats.not_recorded.append((self.kind, self.name))
        # Named here, not only in the summary: this line is logged on every path,
        # including the failed attempt that Airflow retries.
        log.warning(
            "Durable: step not recorded; a retry runs it again, and may run the steps after it again",
            position=self.position,
            step=self.name,
        )

    def fail(self, error: BaseException) -> None:
        """
        Note that the step raised ``error``.

        Nothing is recorded for the step, so it runs again on retry. If ``error`` goes on to
        fail the run, :meth:`DurableRun.fail` uses this to tell which steps the run had
        already started when it failed; a run that recovers from the error is unaffected.
        """
        self.durable_run._raised.append((error, self.durable_run.position, self))


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
    before its first ``await``, so concurrent steps take their positions in the order they
    were started rather than the order they finish. An adapter whose concurrent steps can
    be delayed before their claim must claim them itself in a deterministic order.

    Besides its steps, a run keeps one entry about itself: where the last attempt that
    failed stopped being trustworthy, and the furthest position any attempt reached, so
    a successful run deletes every step an earlier attempt left behind.
    """

    def __init__(self, journal: DurableJournal, key: str) -> None:
        self.journal = journal
        self.key = key
        self.diverged = False
        # Where this attempt stopped replaying, when it did.
        self._diverged_at: int | None = None
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
        # Exceptions steps raised this attempt, with the next position when each was raised
        # and the step that raised it.
        self._raised: list[tuple[BaseException, int, JournalStep]] = []
        # The furthest position this attempt recorded a step at, and every position it recorded.
        self._last_saved = -1
        self._saved: set[int] = set()
        # The position of the last model step this attempt claimed.
        self._last_model_position: int | None = None
        # Read with the rest of the run's entry: whether the previous attempt failed because
        # of the model's answers, which changes why the retry runs live from ``_diverge_from``.
        self._model_at_fault = False
        # Read with the rest of the run's entry: whether ``_diverge_from`` is where the task paused
        # rather than where an attempt failed.
        self._paused = False
        # Read with the rest of the run's entry: the tool call that failed the previous attempt,
        # and on how many attempts in a row it did.
        self._tool_failure: dict[str, Any] = {}
        # Steps ``reserve`` numbered for the latest model response, by tool call id, until taken.
        self._reserved: dict[str, JournalStep] = {}

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
        self._model_at_fault = meta.get("model_at_fault") is True
        self._paused = meta.get("paused") is True
        tool_failure = meta.get("tool_failure")
        self._tool_failure = tool_failure if isinstance(tool_failure, dict) else {}

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

    def reserve(self, calls: Sequence[tuple[str, str, str | None]], *, kind: StepKind) -> None:
        """
        Take a position for each of ``calls`` in order, and keep them for :meth:`claim_reserved`.

        For an adapter that has to number steps before it knows whether they will run. Each call
        is ``(call_id, name, fingerprint)``. A replay counts in the stats only once the step is
        taken. Replaces what an earlier ``reserve`` left unclaimed, and a call whose id is
        already in ``calls`` takes no position. Like :meth:`claim`, call it before the first ``await``.
        """
        reserved: dict[str, JournalStep] = {}
        for call_id, name, fingerprint in calls:
            if call_id not in reserved:
                reserved[call_id] = self._claim(
                    name, kind=kind, fingerprint=fingerprint, replayable=True, count_replay=False
                )
        self._reserved = reserved

    def has_recorded(self, call_id: str) -> bool:
        """Whether the step :meth:`reserve` kept for ``call_id`` holds a result an earlier attempt recorded."""
        step = self._reserved.get(call_id)
        return step is not None and step.replayed

    def claim_reserved(
        self, call_id: str, name: str, *, kind: StepKind, fingerprint: str | None
    ) -> JournalStep:
        """
        Take the step :meth:`reserve` kept for ``call_id``, counting its replay.

        A call whose id was not reserved, or whose reservation is already taken or belongs to
        another call sharing the id (some non-OpenAI adapters), takes a new position as
        :meth:`claim` does. Call it before the first ``await``.
        """
        step = self._reserved.pop(call_id, None)
        if step is not None and (step.name, step.fingerprint) != (name, fingerprint):
            self._reserved[call_id] = step
            step = None
        if step is None:
            step = self.claim(name, kind=kind, fingerprint=fingerprint)
        step.count_as_replayed()
        return step

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
        return self._claim(name, kind=kind, fingerprint=fingerprint, replayable=replayable, count_replay=True)

    def _claim(
        self, name: str, *, kind: StepKind, fingerprint: str | None, replayable: bool, count_replay: bool
    ) -> JournalStep:
        position = self._position
        self._position += 1
        step = JournalStep(self, position, name, kind, fingerprint, replayable=replayable)
        step._choosing_model_position = self._last_model_position
        if kind == "model":
            self._last_model_position = position
        self._load_meta()
        if not self.diverged and self._diverge_from is not None and position >= self._diverge_from:
            if self._paused:
                # Nothing went wrong: the task paused here and resumed with the input it waited for.
                self.diverged, self._diverged_at = True, position
                self._peeked.clear()
                log.info(
                    "Durable: the task resumed after a pause; this step and the ones after it run live",
                    position=position,
                    step=name,
                )
            else:
                self._diverge(
                    position,
                    name,
                    reason=(
                        "the model's answers from this step on failed the previous attempt, so it is asked again"
                        if self._model_at_fault
                        else "the previous attempt failed before reaching this step"
                    ),
                )
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
            if count_replay:
                step._replay_counted = True
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
        if not self.journal.storage.save_step(build_step_key(self.key, position), entry):
            return False
        self._saved.add(position)
        return True

    def fail(self, error: BaseException, *, model_at_fault: bool = False) -> None:
        """
        Record that the run failed with ``error``, so a retry does not replay its error path.

        Steps the run started before the step that raised ``error`` (tool calls running
        alongside it, say) still replay on retry; everything from then on, including what
        error handling recorded, runs again.

        :param model_at_fault: The run failed because of what the model answered rather than
            in a step, such as the framework giving up after the model's answers failed its
            checks too many times. Those answers were recorded and would replay into the same
            failure on every retry, so the retry asks the model again from its last request.
            Ignored when a step raised ``error``: that step decides where the retry runs live.

        A tool call that raises ``error`` on two attempts in a row, with the same arguments, is
        treated as a failure the model caused as well: the first retry runs the call again in
        case its failure was transient, and the next asks the model again from the request that
        chose the call, rather than replay that choice into the same failure.
        """
        failed_steps, causes, failed_at = self._find_failed_steps(error)
        self._load_meta()
        # An attempt that was still replaying an earlier one when it failed, and recorded
        # nothing from the failure on, leaves that earlier attempt's steps as they were.
        diverge_from = failed_at if self.diverged or self._last_saved >= failed_at else self._diverge_from
        failed_step = min(failed_steps, key=lambda step: step.position, default=None)
        # If so, this run's model did not choose what failed, and this run does not count it.
        owned_by_nested_run = self._is_failure_owned_by_nested_run(failed_steps, causes)
        tool_failure = None if owned_by_nested_run else self._next_tool_failure(failed_step)
        # The model request a retry asks again, if the failure is the model's.
        ask_model_from: int | None = None
        if failed_step is None:
            if model_at_fault:
                # Unlike a step that raised, the failure is known to come after every recorded
                # step, so this holds even for an attempt that was still replaying.
                ask_model_from = self._last_model_position
        elif (
            tool_failure is not None
            and tool_failure["attempts"] >= _TOOL_FAILURES_BEFORE_ASKING_MODEL
            and failed_step._choosing_model_position is not None
        ):
            ask_model_from = failed_step._choosing_model_position
            log.warning(
                "Durable: the same tool call failed on consecutive attempts; a retry asks the model "
                "again from the request that chose it",
                position=failed_step.position,
                step=failed_step.name,
                attempts=tool_failure["attempts"],
            )
            # The count starts over with the model's next answer, even one whose call cannot be
            # told from this one (no fingerprint), so that call too is retried once as it is.
            tool_failure = None
        if owned_by_nested_run or tool_failure is not None or ask_model_from is not None:
            self.journal._run_failures.append((error, self.key))
        rerun_model = False
        if ask_model_from is not None and (diverge_from is None or ask_model_from <= diverge_from):
            diverge_from, rerun_model = ask_model_from, True
        previous = self._diverge_from
        if (
            previous is not None
            and previous not in self._saved
            and (diverge_from is None or previous < diverge_from)
        ):
            # The entry at the previous attempt's point is still that attempt's, as this one
            # recorded nothing over it (the step there raised, say), so a retry must not replay it.
            diverge_from, rerun_model = previous, self._model_at_fault
        self._save_meta(diverge_from, model_at_fault=rerun_model, tool_failure=tool_failure)

    def _find_failed_steps(self, error: BaseException) -> tuple[list[JournalStep], list[BaseException], int]:
        """
        Find the steps of this attempt that raised ``error`` or one of its causes.

        :return: those steps; ``error`` and its causes; and the position the run had reached when
            the earliest of those steps raised, or the current position if none did.
        """
        failed_at = self._position
        failed_steps: list[JournalStep] = []
        causes: list[BaseException] = []
        cause: BaseException | None = error
        while cause is not None:
            causes.append(cause)
            for raised, position, step in self._raised:
                if raised is cause:
                    failed_steps.append(step)
                    failed_at = min(failed_at, position)
            cause = cause.__cause__ or cause.__context__
        return failed_steps, causes, failed_at

    def _is_failure_owned_by_nested_run(
        self, failed_steps: list[JournalStep], causes: list[BaseException]
    ) -> bool:
        """
        Whether an agent run started inside one of ``failed_steps`` took one of ``causes`` as its own.

        Such a run failed with the error first, and counted its tool call's failure or asked its
        model again. Only runs numbered under those steps count (a tool can raise through an
        operation step it started).
        """
        # A run that only has the error in its context, such as a fallback agent started in an
        # ``except``, does not own it.
        inside = tuple(f"{self.key}.{step.position}." for step in failed_steps)
        return any(
            seen is cause and key.startswith(inside)
            for seen, key in self.journal._run_failures
            for cause in causes
        )

    def pause(self) -> None:
        """
        Record that the task is about to pause, so that resuming it replays what this attempt recorded.

        An adapter calls this before it stops the task to wait for input. Where an earlier
        attempt's failure, or a different path this attempt took, left entries that must not
        replay, this attempt has run live from there: what it recorded becomes trusted, and
        the earlier entries at positions it left empty (a call still waiting for approval, a
        tool that raised) are deleted. Positions from here on still run live after the pause.
        """
        self._load_meta()
        start = min((p for p in (self._diverge_from, self._diverged_at) if p is not None), default=None)
        if start is None or self._position <= start:
            # This attempt has not run anything live past the untrusted entries yet.
            return
        stale = {
            position: build_step_key(self.key, position)
            for position in range(start, self._position)
            if position not in self._saved
        }
        if stale:
            self.journal.storage.delete_steps(stale.values())
        # Deleting is best-effort; an entry that survived must still not replay.
        survived = [position for position, key in stale.items() if self.journal.storage.load_step(key)]
        if survived:
            log.warning("Durable: could not delete entries this attempt replaced", positions=survived)
        trusted_until = survived[0] if survived else self._position
        self._diverge_from, self._model_at_fault = trusted_until, False
        # A pause is not a failure, and the task resumes in the same attempt, so the count of
        # attempts in a row a tool call failed carries over to a failure after the resume.
        self._save_meta(
            trusted_until, model_at_fault=False, paused=True, tool_failure=self._tool_failure or None
        )

    def _save_meta(
        self,
        diverge_from: int | None,
        *,
        model_at_fault: bool,
        paused: bool = False,
        tool_failure: Mapping[str, Any] | None = None,
    ) -> None:
        meta = {
            "diverge_from": diverge_from,
            "high_water": max(self._high_water, self._position),
            "model_at_fault": model_at_fault,
            "paused": paused,
            "tool_failure": tool_failure,
        }
        self.journal.storage.save_step(build_run_meta_key(self.key), meta)

    def _next_tool_failure(self, step: JournalStep | None) -> _ToolFailure | None:
        """
        Describe the tool call ``step`` for the run's entry, with how many attempts in a row it failed.

        The call counts as the one that failed the previous attempt when its position, name and
        fingerprint match; the fingerprint holds the call's arguments and the call id from the
        model's answer, so a call from a new answer starts the count over.

        ``None`` when ``step`` is not a tool call.
        """
        if step is None or step.kind != "tool":
            return None
        call: list[int | str | None] = [step.position, step.name, step.fingerprint]
        attempts = self._tool_failure.get("attempts")
        if self._tool_failure.get("call") != call or not isinstance(attempts, int):
            attempts = 0
        return {"call": call, "attempts": attempts + 1}

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
        if step._replay_counted:
            step._replay_counted = False
            self.journal.stats.replayed[step.kind] -= 1
        self._diverge(step.position, step.name, reason=reason)

    def _diverge(self, position: int, name: str, *, reason: str) -> None:
        if not self.diverged:
            self.diverged, self._diverged_at = True, position
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
    :param fail_on_unrecordable: Fail the task on a step result the journal cannot hold
        (see :meth:`JournalStep.cannot_record`). With ``False`` the step is logged and runs
        again on retry instead.
    """

    def __init__(
        self,
        storage: DurableStorageProtocol,
        *,
        clean_up_after_run: bool = False,
        fail_on_unrecordable: bool = True,
    ) -> None:
        self.storage = storage
        self.clean_up_after_run = clean_up_after_run
        self.fail_on_unrecordable = fail_on_unrecordable
        self.stats = JournalStats()
        self._runs: list[DurableRun] = []
        self._top_level_runs = 0
        self._run_id: str | None = None
        # Keys of every run any attempt started, read on first use. Cleanup walks it, so a run
        # an earlier attempt started that this one never reaches (an agent called from a tool
        # that now replays) is still deleted.
        self._known_runs: list[str] | None = None
        # Errors a run of this attempt failed with and took as its own, with the run's key. An agent
        # run inside a step fails before the run the step belongs to, so that run can tell.
        self._run_failures: list[tuple[BaseException, str]] = []

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
