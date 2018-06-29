import airflow.models
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State

log = LoggingMixin().log


def get_task_instances_between(ti, ts, session=None):
    """
    Given a `TaskInstance`, yield any `TaskInstance`s that should exist between
    then and a specific time, respecting the end date of the DAG and task. Note
    that since those `TaskInstance`s may not have been created in the database
    yet (pending scheduler availability), this function returns new objects that
    are not persisted in the db.
    """
    task = ti.task
    dag = task.dag

    # Respect DAG and Task end dates.
    end_dates = [ts]
    if task.end_date:
        end_dates.append(task.end_date)
    if dag.end_date:
        end_dates.append(dag.end_date)

    # Get the soonest valid end date.
    end_date = min(end_dates)

    # Return all in-database TIs (including the provided one).
    db_tis = task.get_task_instances(session, start_date=ti.start_date,
                                     end_date=end_date)

    # Helper to iterate through TIs and find one matching an exc date
    def _scan(l, d):
        for t in l:
            if t.execution_date == d:
                return t
            elif t.execution_date > d:
                return False

    for exc_date in dag.date_range(start_date=ts, end_date=end_date):
        ti_on_exc_date = _scan(db_tis, exc_date)

        # Remove a match if found
        if ti_on_exc_date:
            db_tis.remove(ti_on_exc_date)
        # Create a fake TI if not found
        else:
            ti_on_exc_date = airflow.models.TaskInstance(task, exc_date)
        yield ti_on_exc_date


def create_sla_misses(ti, ts, session):
    """
    Determine whether a TaskInstance has missed any SLAs as of a provided
    timestamp. If it has, create `SlaMiss` objects in the provided session.
    Note that one TaskInstance can have multiple SLA miss objects: for example,
    it can both start late and run longer than expected.
    """
    # Skipped task instances will never trigger SLAs because they
    # were intentionally not scheduled. Though, it's still a valid and
    # interesting SLA miss if a task that's *going* to be skipped today is
    # late! That could mean that an upstream task is hanging.
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

    # SLA Miss for Expected Start
    if ti.task.expected_start:
        try:
            # If a TI's exc date is 01-01-2018, we expect it to start by the next
            # execution date (01-02-2018) plus a delta of expected_start.
            expected_start = ti.task.dag.following_schedule(ti.execution_date)
            expected_start += ti.task.expected_start

            # "now" is the start date for comparison, if the TI hasn't started
            actual_start = ti.start_date or ts

            if actual_start > expected_start:
                session.merge(airflow.models.SlaMiss(
                    task_id=ti.task_id,
                    dag_id=ti.dag_id,
                    execution_date=ti.execution_date,
                    type=airflow.models.SlaMiss.TASK_LATE_START,
                    timestamp=ts))
        except Exception:
            log.warning(
                "Failed to calculate expected start SLA miss for "
                "task %s",
                ti
            )

    # SLA Miss for Expected Finish
    if ti.task.expected_finish:
        try:
            # If a TI's exc date is 01-01-2018, we expect it to finish by the next
            # execution date (01-02-2018) plus a delta of expected_finish.
            expected_finish = ti.task.dag.following_schedule(ti.execution_date)
            expected_finish += ti.task.expected_finish

            # "now" is the end date for comparison, if the TI hasn't finished
            actual_finish = ti.end_date or ts

            if actual_finish > expected_finish:
                session.merge(airflow.models.SlaMiss(
                    task_id=ti.task_id,
                    dag_id=ti.dag_id,
                    execution_date=ti.execution_date,
                    type=airflow.models.SlaMiss.TASK_LATE_FINISH,
                    timestamp=ts))
        except Exception:
            log.warning(
                "Failed to calculate expected finish SLA miss for "
                "task %s",
                ti
            )


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
