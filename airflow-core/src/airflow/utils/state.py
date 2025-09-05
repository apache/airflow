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


class State:
    """Static class with task instance state constants and color methods to avoid hard-coding."""

    from airflow.models.taskinstance import (
        DagRunState,
        IntermediateTIState,
        TaskInstanceState,
        TerminalTIState,
    )

    # Backwards-compat constants for code that does not yet use the enum
    # These first three are shared by DagState and TaskState
    SUCCESS = TaskInstanceState.SUCCESS
    RUNNING = TaskInstanceState.RUNNING
    FAILED = TaskInstanceState.FAILED

    # These are TaskState only
    NONE = None
    REMOVED = TaskInstanceState.REMOVED
    SCHEDULED = TaskInstanceState.SCHEDULED
    QUEUED = TaskInstanceState.QUEUED
    RESTARTING = TaskInstanceState.RESTARTING
    UP_FOR_RETRY = TaskInstanceState.UP_FOR_RETRY
    UP_FOR_RESCHEDULE = TaskInstanceState.UP_FOR_RESCHEDULE
    UPSTREAM_FAILED = TaskInstanceState.UPSTREAM_FAILED
    SKIPPED = TaskInstanceState.SKIPPED
    DEFERRED = TaskInstanceState.DEFERRED

    finished_dr_states: frozenset[DagRunState] = frozenset([DagRunState.SUCCESS, DagRunState.FAILED])
    unfinished_dr_states: frozenset[DagRunState] = frozenset([DagRunState.QUEUED, DagRunState.RUNNING])

    task_states: tuple[TaskInstanceState | None, ...] = (None, *TaskInstanceState)

    dag_states: tuple[DagRunState, ...] = (
        DagRunState.QUEUED,
        DagRunState.SUCCESS,
        DagRunState.RUNNING,
        DagRunState.FAILED,
    )

    state_color: dict[TaskInstanceState | None, str] = {
        None: "lightblue",
        TaskInstanceState.QUEUED: "gray",
        TaskInstanceState.RUNNING: "lime",
        TaskInstanceState.SUCCESS: "green",
        TaskInstanceState.RESTARTING: "violet",
        TaskInstanceState.FAILED: "red",
        TaskInstanceState.UP_FOR_RETRY: "gold",
        TaskInstanceState.UP_FOR_RESCHEDULE: "turquoise",
        TaskInstanceState.UPSTREAM_FAILED: "orange",
        TaskInstanceState.SKIPPED: "hotpink",
        TaskInstanceState.REMOVED: "lightgrey",
        TaskInstanceState.SCHEDULED: "tan",
        TaskInstanceState.DEFERRED: "mediumpurple",
    }

    @classmethod
    def color(cls, state):
        """Return color for a state."""
        return cls.state_color.get(state, "white")

    @classmethod
    def color_fg(cls, state):
        """Black&white colors for a state."""
        color = cls.color(state)
        if color in ["green", "red"]:
            return "white"
        return "black"

    finished: frozenset[TaskInstanceState] = frozenset(
        [
            TaskInstanceState.SUCCESS,
            TaskInstanceState.FAILED,
            TaskInstanceState.SKIPPED,
            TaskInstanceState.UPSTREAM_FAILED,
            TaskInstanceState.REMOVED,
        ]
    )
    """
    A list of states indicating a task has reached a terminal state (i.e. it has "finished") and needs no
    further action.

    Note that the attempt could have resulted in failure or have been
    interrupted; or perhaps never run at all (skip, or upstream_failed) in any
    case, it is no longer running.
    """

    unfinished: frozenset[TaskInstanceState | None] = frozenset(
        [
            None,
            TaskInstanceState.SCHEDULED,
            TaskInstanceState.QUEUED,
            TaskInstanceState.RUNNING,
            TaskInstanceState.RESTARTING,
            TaskInstanceState.UP_FOR_RETRY,
            TaskInstanceState.UP_FOR_RESCHEDULE,
            TaskInstanceState.DEFERRED,
        ]
    )
    """
    A list of states indicating that a task either has not completed
    a run or has not even started.
    """

    failed_states: frozenset[TaskInstanceState] = frozenset(
        [TaskInstanceState.FAILED, TaskInstanceState.UPSTREAM_FAILED]
    )
    """
    A list of states indicating that a task or dag is a failed state.
    """

    success_states: frozenset[TaskInstanceState] = frozenset(
        [TaskInstanceState.SUCCESS, TaskInstanceState.SKIPPED]
    )
    """
    A list of states indicating that a task or dag is a success state.
    """

    adoptable_states = frozenset(
        [TaskInstanceState.QUEUED, TaskInstanceState.RUNNING, TaskInstanceState.RESTARTING]
    )
    """
    A list of states indicating that a task can be adopted or reset by a scheduler job
    if it was queued by another scheduler job that is not running anymore.
    """


def __getattr__(name: str):
    """Provide backward compatibility for moved classes."""
    if name == "JobState":
        import warnings

        from airflow.jobs.job import JobState

        warnings.warn(
            "The `airflow.utils.state.JobState` attribute is deprecated and will be removed in a future version. "
            "Please use `airflow.jobs.job.JobState` instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return JobState

    if name == "TaskInstanceState":
        import warnings

        from airflow.models.taskinstance import TaskInstanceState

        warnings.warn(
            "The `airflow.utils.state.TaskInstanceState` attribute is deprecated and will be removed in a future version. "
            "Please use `airflow.models.taskinstance.TaskInstanceState` instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return TaskInstanceState

    if name == "DagRunState":
        import warnings

        from airflow.models.taskinstance import DagRunState

        warnings.warn(
            "The `airflow.utils.state.DagRunState` attribute is deprecated and will be removed in a future version. "
            "Please use `airflow.models.taskinstance.DagRunState` instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return DagRunState

    if name == "TerminalTIState":
        import warnings

        from airflow.models.taskinstance import TerminalTIState

        warnings.warn(
            "The `airflow.utils.state.TerminalTIState` attribute is deprecated and will be removed in a future version. "
            "Please use `airflow.models.taskinstance.TerminalTIState` instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return TerminalTIState

    if name == "IntermediateTIState":
        import warnings

        from airflow.models.taskinstance import IntermediateTIState

        warnings.warn(
            "The `airflow.utils.state.IntermediateTIState` attribute is deprecated and will be removed in a future version. "
            "Please use `airflow.models.taskinstance.IntermediateTIState` instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return IntermediateTIState

    raise AttributeError(f"module 'airflow.utils.state' has no attribute '{name}'")
