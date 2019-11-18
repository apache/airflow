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
"""Context for dependencies."""
from collections import namedtuple
from typing import Any, Generator, Optional, Set, Tuple, Union

from sqlalchemy.orm import Session

from airflow.utils.db import provide_session

# Dependency status for a specific task instance indicating whether or not the task
# instance passed the dependency.

TIDepStatus = namedtuple('TIDepStatus', ['dep_name', 'passed', 'reason'])


class BaseTIDep:
    """
    Abstract base class for dependencies that must be satisfied in order for task
    instances to run. For example, a task that can only run if a certain number of its
    upstream tasks succeed. This is an abstract class and must be subclassed to be used.
    """

    # If this dependency can be ignored by a context in which it is added to. Needed
    # because some dependencies should never be ignoreable in their contexts.
    IGNOREABLE = False

    # Whether this dependency is not a global task instance dependency but specific
    # to some tasks (e.g. depends_on_past is not specified by all tasks).
    IS_TASK_DEP = False

    def __init__(self):
        pass

    def __eq__(self, other):
        return type(self) is type(other)

    def __hash__(self):
        return hash(type(self))

    def __repr__(self):
        return "<TIDep({self.name})>".format(self=self)

    @property
    def name(self) -> str:
        """
        The human-readable name for the dependency. Use the classname as the default name
        if this method is not overridden in the subclass.
        """
        return getattr(self, 'NAME', self.__class__.__name__)

    def _get_dep_statuses(self,
                          ti: Any,  # TaskInstance cannot be used here due to circular deps
                          session: Session,
                          dep_context: Optional['DepContext'] = None) -> \
            Generator[TIDepStatus, None, None]:
        """
        Abstract method that returns an iterable of TIDepStatus objects that describe
        whether the given task instance has this dependency met.

        For example a subclass could return an iterable of TIDepStatus objects, each one
        representing if each of the passed in task's upstream tasks succeeded or not.

        :param ti: the task instance to get the dependency status for
        :param session: database session
        :param dep_context: the context for which this dependency should be evaluated for
        """
        raise NotImplementedError

    @provide_session
    def get_dep_statuses(self,
                         ti: Any,  # TaskInstance cannot be used here due to circular dep
                         session: Session,
                         dep_context: Optional['DepContext'] = None) -> \
            Generator[TIDepStatus, None, None]:
        """
        Wrapper around the private _get_dep_statuses method that contains some global
        checks for all dependencies.

        :param ti: the task instance to get the dependency status for
        :param session: database session
        :param dep_context: the context for which this dependency should be evaluated for
        """
        if dep_context is None:
            dep_context = DepContext()

        if self.IGNOREABLE and dep_context.ignore_all_deps:
            yield self._passing_status(
                reason="Context specified all dependencies should be ignored.")
            return

        if self.IS_TASK_DEP and dep_context.ignore_task_deps:
            yield self._passing_status(
                reason="Context specified all task dependencies should be ignored.")
            return

        yield from self._get_dep_statuses(ti, session, dep_context)

    @provide_session
    def is_met(self,
               ti: Any,  # TaskInstance cannot be used here due to circular dep
               session: Session,
               dep_context: Optional['DepContext'] = None) -> bool:
        """
        Returns whether or not this dependency is met for a given task instance. A
        dependency is considered met if all of the dependency statuses it reports are
        passing.

        :param ti: the task instance to see if this dependency is met for
        :param session: database session
        :param dep_context: The context this dependency is being checked under that stores
            state that can be used by this dependency.
        """
        return all(status.passed for status in
                   self.get_dep_statuses(ti, session, dep_context))

    @provide_session
    def get_failure_reasons(self,
                            ti: Any,  # Task Instance cannot be used here due to circular dep
                            session: Session,
                            dep_context: Optional['DepContext'] = None) -> \
            Generator[str, None, None]:
        """
        Returns an iterable of strings that explain why this dependency wasn't met.

        :param ti: the task instance to see if this dependency is met for
        :param session: database session
        :param dep_context: The context this dependency is being checked under that stores
            state that can be used by this dependency.
        """
        for dep_status in self.get_dep_statuses(ti, session, dep_context):
            if not dep_status.passed:
                yield dep_status.reason

    def _failing_status(self, reason: Union[str, Tuple[str, str]] = '') -> TIDepStatus:
        return TIDepStatus(self.name, False, reason)

    def _passing_status(self, reason: Union[str, Tuple[str, str]] = '') -> TIDepStatus:
        return TIDepStatus(self.name, True, reason)


class DepContext:
    """
    A base class for contexts that specifies which dependencies should be evaluated in
    the context for a task instance to satisfy the requirements of the context. Also
    stores state related to the context that can be used by dependency classes.

    For example there could be a SomeRunContext that subclasses this class which has
    dependencies for:

    - Making sure there are slots available on the infrastructure to run the task instance
    - A task-instance's task-specific dependencies are met (e.g. the previous task
      instance completed successfully)
    - ...

    :param deps: The context-specific dependencies that need to be evaluated for a
        task instance to run in this execution context.
    :type deps: set(airflow.ti_deps.deps.base_ti_dep.BaseTIDep)
    :param flag_upstream_failed: This is a hack to generate the upstream_failed state
        creation while checking to see whether the task instance is runnable. It was the
        shortest path to add the feature. This is bad since this class should be pure (no
        side effects).
    :param ignore_all_deps: Whether or not the context should ignore all ignoreable
        dependencies. Overrides the other ignore_* parameters
    :param ignore_depends_on_past: Ignore depends_on_past parameter of DAGs (e.g. for
        Backfills)
    :param ignore_in_retry_period: Ignore the retry period for task instances
    :param ignore_in_reschedule_period: Ignore the reschedule period for task instances
    :param ignore_task_deps: Ignore task-specific dependencies such as depends_on_past and
        trigger rule
    :param ignore_ti_state: Ignore the task instance's previous failure/success
    """
    def __init__(
            self,
            deps: Optional[Set[BaseTIDep]] = None,
            flag_upstream_failed: bool = False,
            ignore_all_deps: bool = False,
            ignore_depends_on_past: bool = False,
            ignore_in_retry_period: bool = False,
            ignore_in_reschedule_period: bool = False,
            ignore_task_deps: bool = False,
            ignore_ti_state: bool = False):
        self.deps: Set[BaseTIDep] = deps or set()
        self.flag_upstream_failed = flag_upstream_failed
        self.ignore_all_deps = ignore_all_deps
        self.ignore_depends_on_past = ignore_depends_on_past
        self.ignore_in_retry_period = ignore_in_retry_period
        self.ignore_in_reschedule_period = ignore_in_reschedule_period
        self.ignore_task_deps = ignore_task_deps
        self.ignore_ti_state = ignore_ti_state
