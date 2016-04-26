from abc import ABCMeta, abstractmethod
from collections import namedtuple


class TIDep(object):
    """
    A dependency that must be satisfied in order for task instances to run. For example, a task can
    only run if a certain number of its upstream tasks succeed.
    """
    __metaclass__ = ABCMeta

    # TODO(aoen): when python 2.x is deprecated add the @classmethod decorator here
    # TODO(aoen): All of the parameters other than the task instance should be replaced with an
    # single context object parameter.
    @abstractmethod
    def get_dep_status(
            cls,
            ti,
            session,
            include_queued,
            ignore_depends_on_past,
            flag_upstream_failed):
        """
        Returns a list of TIDepStatus objects that describe whether the given task instance has this
        dependency met.

        For example a subclass could return a list of TIDepStatus objects, each one representing if
        each of the passed in task's upstream tasks succeeded or not.

        :param ti: the task instance to get the dependency status for
        :type ti: TaskInstance
        """
        raise NotImplementedError

    @classmethod
    def is_met(
            cls,
            ti,
            session,
            include_queued,
            ignore_depends_on_past,
            flag_upstream_failed):
        """
        Returns whether or not this dependency is met for a given task instance. A dependency is
        considered met if all of the dependency statuses it reports are passing.

        :param ti: the task instance to see if this dependency is met for
        :type ti: TaskInstance
        """
        return all([status.passed for status in
                    cls.get_dep_status(
                        ti,
                        session,
                        include_queued,
                        ignore_depends_on_past,
                        flag_upstream_failed)])

    @classmethod
    def get_dep_name(cls):
        return cls.__name__

    @classmethod
    def get_failure_reasons(
            cls,
            ti,
            session,
            include_queued,
            ignore_depends_on_past,
            flag_upstream_failed):
        """
        Returns a list of strings that explain why this dependency wasn't met.

        :param ti: the task instance to get the dependency failure reasons for
        :type ti: TaskInstance
        """
        return [
            dep_status.reason for dep_status in
            cls.get_dep_status(
                ti,
                session,
                include_queued,
                ignore_depends_on_past,
                flag_upstream_failed)]

    @classmethod
    def passing_status(cls, dep_name=None, reason=''):
        if dep_name is None:
            dep_name = cls.get_dep_name()
        return [TIDepStatus(dep_name, True, reason)]

    @classmethod
    def failing_status(cls, dep_name=None, reason=''):
        if dep_name is None:
            dep_name = cls.get_dep_name()
        return [TIDepStatus(dep_name, False, reason)]


class TIDeps(TIDep):
    @classmethod
    def get_dep_status(
            cls,
            ti_deps,
            ti,
            session,
            include_queued,
            ignore_depends_on_past,
            flag_upstream_failed):
        return [dep.get_dep_status(
                    ti,
                    session,
                    include_queued,
                    ignore_depends_on_past,
                    flag_upstream_failed) for dep in ti_deps]

# Information on whether or not a task instance dependency is met and reasons why it isn't met.
TIDepStatus = namedtuple('TIDepStatus', 'dep_name passed reason')
