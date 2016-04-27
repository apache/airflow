from abc import ABCMeta, abstractmethod
from collections import namedtuple
from datetime import datetime, timedelta
from sqlalchemy import case, func

import airflow
from airflow.utils.db import provide_session
from airflow.utils.state import State


class BaseTIDep(object):
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

class EndDateAfterExecutionDateDep(BaseTIDep):
    @classmethod
    def get_dep_statuses(
            cls,
            ti,
            session,
            include_queued,
            ignore_depends_on_past,
            flag_upstream_failed):
        if ti.task.end_date and ti.execution_date > ti.task.end_date:
            return cls.failing_status(
                reason="The execution date is {0} but this is after the task's end date {1}."
                           .format(ti.task.end_date.isoformat(), ti.execution_date().isoformat()))
        return cls.passing_status()


class ExecDateNotInFutureDep(BaseTIDep):
    @classmethod
    def get_dep_statuses(
            cls,
            ti,
            session,
            include_queued,
            ignore_depends_on_past,
            flag_upstream_failed):
        cur_date = datetime.now()
        if ti.execution_date > cur_date:
            return cls.failing_status(
                reason="Execution date {0} is in the future (the current "
                        "date is {1}).".format(ti.execution_date.isoformat(), cur_date.isoformat()))
        return cls.passing_status()


class InRunnableStateDep(BaseTIDep):
    @classmethod
    def get_dep_statuses(
            cls,
            ti,
            session,
            include_queued,
            ignore_depends_on_past,
            flag_upstream_failed):
        if not ti.force and ti.state not in State.runnable():
            if ti.state == State.SUCCESS:
                return cls.failing_status(
                    reason="Task previously succeeded on {1}. Cannot run a task that "
                           "already succeeded.".format(ti, ti.end_date))
            elif ti.state == state.RUNNING:
                return cls.failing_status(
                    reason="Task is already running, it started on "
                           "{0}.".format(ti, ti.start_date))
            else:
                return cls.failing_status(
                    reason="Task is in the '{0}' state which is not a runnable "
                           "state.".format(ti.state))

        return cls.passing_status()


class DagUnpausedDep(BaseTIDep):
    @classmethod
    def get_dep_statuses(
            cls,
            ti,
            session,
            include_queued,
            ignore_depends_on_past,
            flag_upstream_failed):
        if ti.task.dag.is_paused:
            return cls.failing_status(
                reason="Task's DAG '{0}' is paused.".format(ti.dag_id))
        else:
            return cls.passing_status()



class MaxConcurrencyNotReachedDep(BaseTIDep):
    @classmethod
    def get_dep_statuses(
            cls,
            ti,
            session,
            include_queued,
            ignore_depends_on_past,
            flag_upstream_failed):
        if ti.task.dag.concurrency_reached:
            return cls.failing_status(
                reason="The maximum number of running tasks ({0}) for this task's DAG '{1}' has "
                       "been reached.".format(ti.dag_id, ti.task.dag.concurrency))
        else:
            return cls.passing_status()


class MaxDagrunsNotReachedDep(BaseTIDep):
    @classmethod
    def get_dep_statuses(
            cls,
            ti,
            session,
            include_queued,
            ignore_depends_on_past,
            flag_upstream_failed):
        if ti.task.dag.concurrency_reached:
            return cls.failing_status(
                reason="The maximum number of active dag runs ({0}) for this task's DAG '{1}' has "
                       "been reached.".format(ti.dag_id, ti.task.dag.max_active_runs))
        else:
            return cls.passing_status()


class NotAlreadyQueuedDep(BaseTIDep):
    @classmethod
    def get_dep_statuses(
            cls,
            ti,
            session,
            include_queued,
            ignore_depends_on_past,
            flag_upstream_failed):
        if ti.state == State.QUEUED and not include_queued:
            return cls.failing_status(
                reason="The task instance has already been queued and will run shortly.")
        return cls.passing_status()


class NotInRetryPeriodDep(BaseTIDep):
    @classmethod
    def get_dep_statuses(
            cls,
            ti,
            session,
            include_queued,
            ignore_depends_on_past,
            flag_upstream_failed):
        # Calculate the date first so that it is always smaller than the timestamp
        # used by ready_for_retry
        if ti.state == State.UP_FOR_RETRY:
            cur_date = datetime.now()
            next_task_retry_date = ti.end_date + ti.task.retry_delay
            if not ti.ready_for_retry():
                return cls.failing_status(
                    reason="Task is not ready for retry yet but will be retried automatically. "
                           "Current date is {0} and task will be retried at {1}.".format(
                               cur_date.isoformat(), next_task_retry_date.isoformat()))
        return cls.passing_status()


class NotSkippedDep(BaseTIDep):
    @classmethod
    def get_dep_statuses(
            cls,
            ti,
            session,
            include_queued,
            ignore_depends_on_past,
            flag_upstream_failed):
        if ti.state == State.SKIPPED:
            return cls.failing_status(reason="The task instance has been skipped.")
        return cls.passing_status()


class PastDagrunDep(BaseTIDep):
    """
    Is the past dagrun in a state that allows this task instance to run, e.g. did
    this task instance's task in the previous dagrun complete if we are depending on past
    """

    @classmethod
    def get_dep_statuses(
            cls,
            ti,
            session,
            include_queued,
            ignore_depends_on_past,
            flag_upstream_failed):

        # Are we still waiting for the previous task instance to succeed?

        if ignore_depends_on_past or not ti.task.depends_on_past:
            return cls.passing_status()

        # The first task instance for a task shouldn't depend on the  task instance before it
        # because there won't be one
        if ti.execution_date == ti.task.start_date:
            return cls.passing_status()

        previous_ti = ti.previous_ti
        if not ti.previous_ti:
            return cls.failing_status(
                reason="depends_on_past is true for this task, but the previous task instance has "
                       "not run yet.")

        if previous_ti.state not in [State.SUCCESS, State.SKIPPED]:
            return cls.failing_status(
                reason="depends_on_past is true for this task, but the previous task instance is "
                "in the state '{0}' which is not a successful state.".format(previous_ti.state))

        previous_ti.task = ti.task
        if (ti.task.wait_for_downstream and
                not previous_ti.are_dependents_done(session=session)):
            return cls.failing_status(
                reason="The tasks downstream of the previous task instance haven't completed.")

        return cls.passing_status()


class PoolHasSpaceDep(BaseTIDep):
    @classmethod
    def get_dep_statuses(
            cls,
            ti,
            session,
            include_queued,
            ignore_depends_on_past,
            flag_upstream_failed):
        if ti.pool_full():
            return cls.failing_status(reason="Task's pool '{0}' is full.".format(ti.pool))
        return cls.passing_status()


class TriggerRuleDep(BaseTIDep):
    """
    Determines if a task's upstream tasks are in a state that allows a given task instance
    to run.
    """

    @classmethod
    def get_dep_statuses(
            cls,
            ti,
            session,
            include_queued,
            ignore_depends_on_past,
            flag_upstream_failed):
        TI = airflow.models.TaskInstance
        TR = airflow.models.TriggerRule

        # Checking that all upstream dependencies have succeeded
        if not ti.task.upstream_list or ti.task.trigger_rule == TR.DUMMY:
            return cls.passing_status()

        qry = (
            session
            .query(
                func.coalesce(func.sum(
                    case([(TI.state == State.SUCCESS, 1)], else_=0)), 0),
                func.coalesce(func.sum(
                    case([(TI.state == State.SKIPPED, 1)], else_=0)), 0),
                func.coalesce(func.sum(
                    case([(TI.state == State.FAILED, 1)], else_=0)), 0),
                func.coalesce(func.sum(
                    case([(TI.state == State.UPSTREAM_FAILED, 1)], else_=0)), 0),
                func.count(TI.task_id),
            )
            .filter(
                TI.dag_id == ti.dag_id,
                TI.task_id.in_(ti.task.upstream_task_ids),
                TI.execution_date == ti.execution_date,
                TI.state.in_([
                    State.SUCCESS, State.FAILED,
                    State.UPSTREAM_FAILED, State.SKIPPED]),
            )
        )

        successes, skipped, failed, upstream_failed, done = qry.first()

        return cls._evaluate_trigger_rule(
                    ti=ti,
                    successes=successes,
                    skipped=skipped,
                    failed=failed,
                    upstream_failed=upstream_failed,
                    done=done,
                    flag_upstream_failed=flag_upstream_failed,
                    session=session)

    """
    :param flag_upstream_failed: This is a hack to generate
        the upstream_failed state creation while checking to see
        whether the task instance is runnable. It was the shortest
        path to add the feature
    :type flag_upstream_failed: boolean
    :param successes: Number of successful upstream tasks
    :type successes: boolean
    :param skipped: Number of skipped upstream tasks
    :type skipped: boolean
    :param failed: Number of failed upstream tasks
    :type failed: boolean
    :param upstream_failed: Number of upstream_failed upstream tasks
    :type upstream_failed: boolean
    :param done: Number of completed upstream tasks
    :type done: boolean
    """
    @classmethod
    @provide_session
    def _evaluate_trigger_rule(
        cls,
        ti,
        successes,
        skipped,
        failed,
        upstream_failed,
        done,
        flag_upstream_failed,
        session):
        TR = airflow.models.TriggerRule

        task = ti.task
        upstream = len(task.upstream_task_ids)
        tr = task.trigger_rule
        upstream_done = done >= upstream

        # handling instant state assignment based on trigger rules
        # TODO(aoen): trigger rules should probably be rewritten as a subclass of
        # BaseTIDep or contain a BaseTIDep, and then the logic could be broken up per each
        # trigger rule class
        if flag_upstream_failed:
            if tr == TR.ALL_SUCCESS:
                if upstream_failed or failed:
                    ti.set_state(State.UPSTREAM_FAILED, session)
                elif skipped:
                    ti.set_state(State.SKIPPED, session)
            elif tr == TR.ALL_FAILED:
                if successes or skipped:
                    ti.set_state(State.SKIPPED, session)
            elif tr == TR.ONE_SUCCESS:
                if upstream_done and not successes:
                    ti.set_state(State.SKIPPED, session)
            elif tr == TR.ONE_FAILED:
                if upstream_done and not (failed or upstream_failed):
                    ti.set_state(State.SKIPPED, session)

        if tr == TR.ONE_SUCCESS:
            if successes > 0:
                return cls.passing_status()
            else:
                return cls.failing_status(
                    reason="Task's trigger rule '{0}' requires one upstream task success, but none "
                           "were found.".format(tr))
        elif tr == TR.ONE_FAILED:
            if failed or upstream_failed:
                return cls.passing_status()
            else:
                return cls.failing_status(
                    reason="Task's trigger rule '{0}' requires one upstream task failure but none, "
                           "were found.").format(tr)
        elif tr == TR.ALL_SUCCESS:
            num_failures = upstream - successes
            if num_failures <= 0:
                return cls.passing_status()
            else:
                return cls.failing_status(
                    reason="Task's trigger rule '{0}' requires all upstream tasks to have "
                           "succeeded, but found {1} non-success(es).".format(tr, num_failures))
        elif tr == TR.ALL_FAILED:
            num_successes = upstream - failed - upstream_failed
            if num_successes <= 0:
                return cls.passing_status()
            else:
                return cls.failing_status(
                    reason="Task's trigger rule '{0}' requires all upstream tasks to have failed, "
                           "but found {1} non-faliure(s).".format(tr, num_successes))
        elif tr == TR.ALL_DONE:
            if upstream_done:
                return cls.passing_status()
            else:
                return cls.failing_status(
                    reason="Task's trigger rule '{0}' requires all upstream tasks to have "
                           "completed, but found '{1}' task(s) that weren't "
                           "done.".format(tr, upstream - done))
        else:
            return cls.failing_status(
                reason="No strategy to evaluate trigger rule '{0}'.".format(tr))
