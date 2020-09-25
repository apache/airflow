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

from airflow.settings import STATE_COLORS


class State:
    """
    Static class with task instance states constants and color method to
    avoid hardcoding.
    """

    # scheduler
    NONE = None  # type: None
    REMOVED = "removed"
    SCHEDULED = "scheduled"

    # set by the executor (t.b.d.)
    # LAUNCHED = "launched"

    # set by a task
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    SHUTDOWN = "shutdown"  # External request to shut down
    FAILED = "failed"
    UP_FOR_RETRY = "up_for_retry"
    UP_FOR_RESCHEDULE = "up_for_reschedule"
    UPSTREAM_FAILED = "upstream_failed"
    SKIPPED = "skipped"
    SENSING = "sensing"

    task_states = (
        SUCCESS,
        RUNNING,
        FAILED,
        UPSTREAM_FAILED,
        SKIPPED,
        UP_FOR_RETRY,
        UP_FOR_RESCHEDULE,
        QUEUED,
        NONE,
        SCHEDULED,
        SENSING,
    )

    dag_states = (
        SUCCESS,
        RUNNING,
        FAILED,
    )

    state_color = {
        QUEUED: 'gray',
        RUNNING: 'lime',
        SUCCESS: 'green',
        SHUTDOWN: 'blue',
        FAILED: 'red',
        UP_FOR_RETRY: 'gold',
        UP_FOR_RESCHEDULE: 'turquoise',
        UPSTREAM_FAILED: 'orange',
        SKIPPED: 'pink',
        REMOVED: 'lightgrey',
        SCHEDULED: 'tan',
        NONE: 'lightblue',
        SENSING: 'lightseagreen',
    }
    state_color.update(STATE_COLORS)  # type: ignore

    @classmethod
    def color(cls, state):
        """
        Returns color for a state.
        """
        return cls.state_color.get(state, 'white')

    @classmethod
    def color_fg(cls, state):
        """
        Black&white colors for a state.
        """
        color = cls.color(state)
        if color in ['green', 'red']:
            return 'white'
        return 'black'

    @classmethod
    def running(cls):
        """
        A list of states indicating that a task is being executed.
        """
        return [
            cls.RUNNING,
            cls.SENSING
        ]

    @classmethod
    def finished(cls):
        """
        A list of states indicating that a task started and completed a
        run attempt. Note that the attempt could have resulted in failure or
        have been interrupted; in any case, it is no longer running.
        """
        return [
            cls.SUCCESS,
            cls.FAILED,
            cls.SKIPPED,
        ]

    @classmethod
    def unfinished(cls):
        """
        A list of states indicating that a task either has not completed
        a run or has not even started.
        """
        return [
            cls.NONE,
            cls.SCHEDULED,
            cls.QUEUED,
            cls.RUNNING,
            cls.SENSING,
            cls.SHUTDOWN,
            cls.UP_FOR_RETRY,
            cls.UP_FOR_RESCHEDULE,
        ]


class PokeState:
    """
    Static class with poke states constants used in smart operator.
    """

    LANDED = 'landed'
    NOT_LANDED = 'not_landed'
    POKE_EXCEPTION = 'poke_exception'
