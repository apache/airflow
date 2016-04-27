from abc import ABCMeta, abstractmethod
from collections import namedtuple


class TIDep(object):
    """
    A dependency that must be satisfied in order for task instances to run. For example, a task can
    only run if a certain number of its upstream tasks succeed.
    """
    __metaclass__ = ABCMeta

    # TODO(aoen): when python 2.x is deprecated add the @abstractmethod decorator here (as
    # combining abstractmethod and classmethod are done differently in 2.x and 3.x)
    # TODO(aoen): All of the parameters other than the task instance should be replaced with an
    # single context object parameter.
    @classmethod
    def get_dep_statuses(
            cls,
            ti,
            session,
            include_queued,
            ignore_depends_on_past,
            flag_upstream_failed):
        """
        Returns an iterable of TIDepStatus objects that describe whether the given task instance has
        this dependency met.

        For example a subclass could return an iterable of TIDepStatus objects, each one
        representing if each of the passed in task's upstream tasks succeeded or not.

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
        return all(status.passed for status in
                   cls.get_dep_statuses(
                       ti,
                       session,
                       include_queued,
                       ignore_depends_on_past,
                       flag_upstream_failed))

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
        Returns an iterable of strings that explain why this dependency wasn't met.

        :param ti: the task instance to get the dependency failure reasons for
        :type ti: TaskInstance
        """
        for dep_status in cls.get_dep_statuses(
                                ti,
                                session,
                                include_queued,
                                ignore_depends_on_past,
                                flag_upstream_failed):
            if not dep_status.passed:
                yield dep_status.reason

    @classmethod
    def passing_status(cls, dep_name=None, reason=''):
        if dep_name is None:
            dep_name = cls.get_dep_name()
        yield TIDepStatus(dep_name, True, reason)

    @classmethod
    def failing_status(cls, dep_name=None, reason=''):
        if dep_name is None:
            dep_name = cls.get_dep_name()
        yield TIDepStatus(dep_name, False, reason)

# Information on whether or not a task instance dependency is met and reasons why it isn't met.
TIDepStatus = namedtuple('TIDepStatus', ['dep_name', 'passed', 'reason'])
