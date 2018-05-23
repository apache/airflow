from airflow.models import SlaMiss, TaskInstance
from airflow.utils.state import State


def get_task_instances_between(ti, ts):
    """
    Given a `TaskInstance`, yield any `TaskInstance`s that should exist between
    then and a specific time. Since those `TaskInstance`s may not have been
    created in the database yet (pending scheduler availability), this function
    just returns new objects.
    """
    task = ti.task
    dag = task.dag

    # We want to start from the next one.
    next_exc_date = dag.following_schedule(ti.execution_date)

    while next_exc_date < ts:
        yield TaskInstance(task, next_exc_date)

        # Increment by one execution
        next_exc_date = dag.following_schedule(next_exc_date)


def create_sla_misses(ti, ts, session):
    """
    Determine whether a TaskInstance has missed any SLAs as of a provided
    timestamp. If it has, create `SlaMiss` objects in the provided session.
    """
    # Skipped task instances will never trigger SLAs because they
    # were intentionally not scheduled.
    if ti.state == State.SKIPPED:
        return

    # Calculate each type of SLA miss. Wrapping exceptions here is
    # important so that an exception in one type of SLA doesn't
    # prevent other task SLAs from getting triggered.

    # SLA Miss for Expected Duration
    if ti.task.expected_duration and ti.start_date:
        try:
            if ti.state in State.finished():
                duration = ti.end_date - ti.start_date
            else:
                # Use the current time, if the task is still running.
                duration = ts - ti.start_date

            if duration > ti.task.expected_duration:
                session.merge(SlaMiss(
                    task_id=ti.task_id,
                    dag_id=ti.dag_id,
                    execution_date=ti.execution_date,
                    type=SlaMiss.TASK_DURATION_EXCEEDED,
                    timestamp=ts))
        except Exception:
            self.log.warning(
                "Failed to calculate expected duration SLA miss for "
                "task %s",
                ti
            )

    # TODO: SLA Miss for Expected Start

    # TODO: SLA Miss for Expected Finish
