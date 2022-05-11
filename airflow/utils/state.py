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

from enum import Enum
from typing import Dict, FrozenSet, Tuple

from airflow.settings import STATE_COLORS
from airflow.utils.types import Optional


class TaskInstanceState(str, Enum):
    """
    Enum that represents all possible states that a Task Instance can be in.

    Note that None is also allowed, so always use this in a type hint with Optional.
    """

    # The scheduler sets a TaskInstance state to None when it's created but not
    # yet run, but we don't list it here since TaskInstance is a string enum.
    # Use None instead if need this state.

    # Set by the scheduler
    REMOVED = "removed"  # Task vanished from DAG before it ran
    SCHEDULED = "scheduled"  # Task should run and will be handed to executor soon

    # Set by the task instance itself
    QUEUED = "queued"  # Executor has enqueued the task
    RUNNING = "running"  # Task is executing
    SUCCESS = "success"  # Task completed
    SHUTDOWN = "shutdown"  # External request to shut down (e.g. marked failed when running)
    RESTARTING = "restarting"  # External request to restart (e.g. cleared when running)
    FAILED = "failed"  # Task errored out
    UP_FOR_RETRY = "up_for_retry"  # Task failed but has retries left
    UP_FOR_RESCHEDULE = "up_for_reschedule"  # A waiting `reschedule` sensor
    UPSTREAM_FAILED = "upstream_failed"  # One or more upstream deps failed
    SKIPPED = "skipped"  # Skipped by branching or some other mechanism
    SENSING = "sensing"  # Smart sensor offloaded to the sensor DAG
    DEFERRED = "deferred"  # Deferrable operator waiting on a trigger

    def __str__(self) -> str:
        return self.value


class DagRunState(str, Enum):
    """
    Enum that represents all possible states that a DagRun can be in.

    These are "shared" with TaskInstanceState in some parts of the code,
    so please ensure that their values always match the ones with the
    same name in TaskInstanceState.
    """

    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"

    def __str__(self) -> str:
        return self.value


class State:
    """
    Static class with task instance state constants and color methods to
    avoid hardcoding.
    """

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
    SHUTDOWN = TaskInstanceState.SHUTDOWN
    RESTARTING = TaskInstanceState.RESTARTING
    UP_FOR_RETRY = TaskInstanceState.UP_FOR_RETRY
    UP_FOR_RESCHEDULE = TaskInstanceState.UP_FOR_RESCHEDULE
    UPSTREAM_FAILED = TaskInstanceState.UPSTREAM_FAILED
    SKIPPED = TaskInstanceState.SKIPPED
    SENSING = TaskInstanceState.SENSING
    DEFERRED = TaskInstanceState.DEFERRED

    task_states: Tuple[Optional[TaskInstanceState], ...] = (None,) + tuple(TaskInstanceState)

    dag_states: Tuple[DagRunState, ...] = (
        DagRunState.QUEUED,
        DagRunState.SUCCESS,
        DagRunState.RUNNING,
        DagRunState.FAILED,
    )

    state_color: Dict[Optional[TaskInstanceState], str] = {
        None: 'lightblue',
        TaskInstanceState.QUEUED: 'gray',
        TaskInstanceState.RUNNING: 'lime',
        TaskInstanceState.SUCCESS: 'green',
        TaskInstanceState.SHUTDOWN: 'blue',
        TaskInstanceState.RESTARTING: 'violet',
        TaskInstanceState.FAILED: 'red',
        TaskInstanceState.UP_FOR_RETRY: 'gold',
        TaskInstanceState.UP_FOR_RESCHEDULE: 'turquoise',
        TaskInstanceState.UPSTREAM_FAILED: 'orange',
        TaskInstanceState.SKIPPED: 'hotpink',
        TaskInstanceState.REMOVED: 'lightgrey',
        TaskInstanceState.SCHEDULED: 'tan',
        TaskInstanceState.DEFERRED: 'mediumpurple',
    }
    state_color[TaskInstanceState.SENSING] = state_color[TaskInstanceState.DEFERRED]
    state_color.update(STATE_COLORS)  # type: ignore

    @classmethod
    def color(cls, state):
        """Returns color for a state."""
        return cls.state_color.get(state, 'white')

    @classmethod
    def color_fg(cls, state):
        """Black&white colors for a state."""
        color = cls.color(state)
        if color in ['green', 'red']:
            return 'white'
        return 'black'

    running: FrozenSet[TaskInstanceState] = frozenset(
        [TaskInstanceState.RUNNING, TaskInstanceState.SENSING, TaskInstanceState.DEFERRED]
    )
    """
    A list of states indicating that a task is being executed.
    """

    finished: FrozenSet[TaskInstanceState] = frozenset(
        [
            TaskInstanceState.SUCCESS,
            TaskInstanceState.FAILED,
            TaskInstanceState.SKIPPED,
            TaskInstanceState.UPSTREAM_FAILED,
        ]
    )
    """
    A list of states indicating a task has reached a terminal state (i.e. it has "finished") and needs no
    further action.

    Note that the attempt could have resulted in failure or have been
    interrupted; or perhaps never run at all (skip, or upstream_failed) in any
    case, it is no longer running.
    """

    unfinished: FrozenSet[Optional[TaskInstanceState]] = frozenset(
        [
            None,
            TaskInstanceState.SCHEDULED,
            TaskInstanceState.QUEUED,
            TaskInstanceState.RUNNING,
            TaskInstanceState.SENSING,
            TaskInstanceState.SHUTDOWN,
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

    failed_states: FrozenSet[TaskInstanceState] = frozenset(
        [TaskInstanceState.FAILED, TaskInstanceState.UPSTREAM_FAILED]
    )
    """
    A list of states indicating that a task or dag is a failed state.
    """

    success_states: FrozenSet[TaskInstanceState] = frozenset(
        [TaskInstanceState.SUCCESS, TaskInstanceState.SKIPPED]
    )
    """
    A list of states indicating that a task or dag is a success state.
    """

    terminating_states = frozenset([TaskInstanceState.SHUTDOWN, TaskInstanceState.RESTARTING])
    """
    A list of states indicating that a task has been terminated.
    """


class PokeState:
    """Static class with poke states constants used in smart operator."""

    LANDED = 'landed'
    NOT_LANDED = 'not_landed'
    POKE_EXCEPTION = 'poke_exception'
