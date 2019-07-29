# -*- coding: utf-8 -*-
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

from collections import namedtuple

from airflow.utils.db import provide_session


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
        return type(self) == type(other)

    def __hash__(self):
        return hash(type(self))

    def __repr__(self):
        return "<TIDep({self.name})>".format(self=self)

    @property
    def name(self):
        """
        The human-readable name for the dependency. Use the classname as the default name
        if this method is not overridden in the subclass.
        """
        return getattr(self, 'NAME', self.__class__.__name__)

    def _get_dep_statuses(self, ti, session, dep_context=None):
        """
        Abstract method that returns an iterable of TIDepStatus objects that describe
        whether the given task instance has this dependency met.

        For example a subclass could return an iterable of TIDepStatus objects, each one
        representing if each of the passed in task's upstream tasks succeeded or not.

        :param ti: the task instance to get the dependency status for
        :type ti: airflow.models.TaskInstance
        :param session: database session
        :type session: sqlalchemy.orm.session.Session
        :param dep_context: the context for which this dependency should be evaluated for
        :type dep_context: DepContext
        """
        raise NotImplementedError

    @provide_session
    def get_dep_statuses(self, ti, session, dep_context=None):
        """
        Wrapper around the private _get_dep_statuses method that contains some global
        checks for all dependencies.

        :param ti: the task instance to get the dependency status for
        :type ti: airflow.models.TaskInstance
        :param session: database session
        :type session: sqlalchemy.orm.session.Session
        :param dep_context: the context for which this dependency should be evaluated for
        :type dep_context: DepContext
        """
        # this avoids a circular dependency
        from airflow.ti_deps.dep_context import DepContext

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

        for dep_status in self._get_dep_statuses(ti, session, dep_context):
            yield dep_status

    @provide_session
    def is_met(self, ti, session, dep_context=None):
        """
        Returns whether or not this dependency is met for a given task instance. A
        dependency is considered met if all of the dependency statuses it reports are
        passing.

        :param ti: the task instance to see if this dependency is met for
        :type ti: airflow.models.TaskInstance
        :param session: database session
        :type session: sqlalchemy.orm.session.Session
        :param dep_context: The context this dependency is being checked under that stores
            state that can be used by this dependency.
        :type dep_context: BaseDepContext
        """
        return all(status.passed for status in
                   self.get_dep_statuses(ti, session, dep_context))

    @provide_session
    def get_failure_reasons(self, ti, session, dep_context=None):
        """
        Returns an iterable of strings that explain why this dependency wasn't met.

        :param ti: the task instance to see if this dependency is met for
        :type ti: airflow.models.TaskInstance
        :param session: database session
        :type session: sqlalchemy.orm.session.Session
        :param dep_context: The context this dependency is being checked under that stores
            state that can be used by this dependency.
        :type dep_context: BaseDepContext
        """
        for dep_status in self.get_dep_statuses(ti, session, dep_context):
            if not dep_status.passed:
                yield dep_status.reason

    def _failing_status(self, reason=''):
        return TIDepStatus(self.name, False, reason)

    def _passing_status(self, reason=''):
        return TIDepStatus(self.name, True, reason)


# Dependency status for a specific task instance indicating whether or not the task
# instance passed the dependency.
TIDepStatus = namedtuple('TIDepStatus', ['dep_name', 'passed', 'reason'])
