import airflow.models
from airflow.utils.state import State

from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log


def get_task_instances_between(ti, ts):
    """
    Given a `TaskInstance`, yield any `TaskInstance`s that should exist between
    then and a specific time, respecting the end date of the task. Note that
    since those `TaskInstance`s may not have been created in the database yet
    (pending scheduler availability), this function returns new objects that
    are not persisted in the db.
    """
    task = ti.task
    dag = task.dag

    # We want to start from the next one.
    next_exc_date = dag.following_schedule(ti.execution_date)

    while next_exc_date < ts:
        # Stop before exceeding end date.
        if task.end_date and task.end_date <= next_exc_date:
            break

        yield airflow.models.TaskInstance(task, next_exc_date)

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
                session.merge(airflow.models.SlaMiss(
                    task_id=ti.task_id,
                    dag_id=ti.dag_id,
                    execution_date=ti.execution_date,
                    type=airflow.models.SlaMiss.TASK_DURATION_EXCEEDED,
                    timestamp=ts))
        except Exception:
            log.warning(
                "Failed to calculate expected duration SLA miss for "
                "task %s",
                ti
            )

    # TODO: SLA Miss for Expected Start

    # TODO: SLA Miss for Expected Finish
def send_sla_miss_email(context):
    """
    Send an SLA miss email. This is the default SLA miss callback.
    """

    # TODO: Fix this up.
    TI = airflow.models.TaskInstance

    sla_miss_dates = [sla_miss.execution_date for sla_miss in sla_misses]
    qry = (
        session
        .query(TI)
        .filter(TI.state != State.SUCCESS)
        .filter(TI.execution_date.in_(sla_miss_dates))
        .filter(TI.dag_id == self.dag_id)
        .all()
    )
    blocking_tis = []

    for ti in qry:
        if ti.task_id in self.task_ids:
            ti.task = self.get_task(ti.task_id)
            blocking_tis.append(ti)
        else:
            session.delete(ti)
            session.commit()

    task_list = "\n".join([
        sla.task_id + ' on ' + sla.execution_date.isoformat()
        for sla in sla_misses])
    blocking_task_list = "\n".join([
        ti.task_id + ' on ' + ti.execution_date.isoformat()
        for ti in blocking_tis])

    email_content = """\
    Here's a list of tasks that missed their SLAs:
    <pre><code>{task_list}\n<code></pre>
    Blocking tasks:
    <pre><code>{blocking_task_list}\n{bug}<code></pre>
    """.format(bug=asciiart.bug, **locals())
    emails = []
    for t in self.tasks:
        if t.email:
            if isinstance(t.email, basestring):
                l = [t.email]
            elif isinstance(t.email, (list, tuple)):
                l = t.email
            for email in l:
                if email not in emails:
                    emails.append(email)
    if emails and len(slas):

            send_email(
                emails,
                "[airflow] SLA miss on DAG=" + self.dag_id,
                email_content)
            email_sent = True
            notification_sent = True
