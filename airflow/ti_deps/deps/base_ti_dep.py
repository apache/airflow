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

from typing import TYPE_CHECKING, Any, Iterator, NamedTuple

from airflow.ti_deps.dep_context import DepContext
from airflow.utils.session import provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.taskinstance import TaskInstance


class BaseTIDep:
    """
    Abstract base class for task instances dependencies.

    All dependencies must be satisfied in order for task instances to run.
    For example, a task that can only run if a certain number of its upstream tasks succeed.
    This is an abstract class and must be subclassed to be used.
    """

    # If this dependency can be ignored by a context in which it is added to. Needed
    # because some dependencies should never be ignorable in their contexts.
    IGNORABLE = False

    # Whether this dependency is not a global task instance dependency but specific
    # to some tasks (e.g. depends_on_past is not specified by all tasks).
    IS_TASK_DEP = False

    def __eq__(self, other: Any) -> bool:
        return isinstance(self, type(other))

    def __hash__(self) -> int:
        return hash(type(self))

    def __repr__(self) -> str:
        return f"<TIDep({self.name})>"

    @property
    def name(self) -> str:
        """The human-readable name for the dependency.

        Use the class name as the default if ``NAME`` is not provided.
        """
        return getattr(self, "NAME", self.__class__.__name__)

    def _get_dep_statuses(
        self,
        ti: TaskInstance,
        session: Session,
        dep_context: DepContext,
    ) -> Iterator[TIDepStatus]:
        """
        Abstract method that returns an iterable of TIDepStatus objects.

        Each object describes whether the given task instance has this dependency met.

        For example a subclass could return an iterable of TIDepStatus objects, each one
        representing if each of the passed in task's upstream tasks succeeded or not.

        :param ti: the task instance to get the dependency status for
        :param session: database session
        :param dep_context: the context for which this dependency should be evaluated for
        """
        raise NotImplementedError

    @provide_session
    def get_dep_statuses(
        self,
        ti: TaskInstance,
        session: Session,
        dep_context: DepContext | None = None,
    ) -> Iterator[TIDepStatus]:
        """
        Wrap around the private _get_dep_statuses method.

        Contains some global checks for all dependencies.

        :param ti: the task instance to get the dependency status for
        :param session: database session
        :param dep_context: the context for which this dependency should be evaluated for
        """
        cxt = DepContext() if dep_context is None else dep_context

        if self.IGNORABLE and cxt.ignore_all_deps:
            yield self._passing_status(reason="Context specified all dependencies should be ignored.")
            return

        if self.IS_TASK_DEP and cxt.ignore_task_deps:
            yield self._passing_status(reason="Context specified all task dependencies should be ignored.")
            return

        yield from self._get_dep_statuses(ti, session, cxt)

    @provide_session
    def is_met(self, ti: TaskInstance, session: Session, dep_context: DepContext | None = None) -> bool:
        """
        Return whether a dependency is met for a given task instance.

        A dependency is considered met if all the dependency statuses it reports are passing.

        :param ti: the task instance to see if this dependency is met for
        :param session: database session
        :param dep_context: The context this dependency is being checked under that stores
            state that can be used by this dependency.
        """
        return all(status.passed for status in self.get_dep_statuses(ti, session, dep_context))

    @provide_session
    def get_failure_reasons(
        self,
        ti: TaskInstance,
        session: Session,
        dep_context: DepContext | None = None,
    ) -> Iterator[str]:
        """
        Return an iterable of strings that explain why this dependency wasn't met.

        :param ti: the task instance to see if this dependency is met for
        :param session: database session
        :param dep_context: The context this dependency is being checked under that stores
            state that can be used by this dependency.
        """
        for dep_status in self.get_dep_statuses(ti, session, dep_context):
            if not dep_status.passed:
                yield dep_status.reason

    def _failing_status(self, reason: str = "") -> TIDepStatus:
        return TIDepStatus(self.name, False, reason)

    def _passing_status(self, reason: str = "") -> TIDepStatus:
        return TIDepStatus(self.name, True, reason)


class TIDepStatus(NamedTuple):
    """Dependency status for a task instance indicating whether the task instance passed the dependency."""

    dep_name: str
    passed: bool
    reason: str
