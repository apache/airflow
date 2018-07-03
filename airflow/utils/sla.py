import airflow.models
from airflow.utils.db import provide_session
from airflow.utils.email import send_email
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
                    sla_type=airflow.models.SlaMiss.TASK_DURATION_EXCEEDED,
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
                    sla_type=airflow.models.SlaMiss.TASK_LATE_START,
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
                    sla_type=airflow.models.SlaMiss.TASK_LATE_FINISH,
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
    sla_miss = context["sla_miss"]

    if sla_miss.sla_type == sla_miss.TASK_DURATION_EXCEEDED:
        email_function = send_task_duration_exceeded_email
    elif sla_miss.sla_type == sla_miss.TASK_LATE_START:
        email_function = send_task_late_start_email
    elif sla_miss.sla_type == sla_miss.TASK_LATE_FINISH:
        email_function = send_task_late_finish_email
    else:
        log.warning("Received unexpected SLA Miss type: %s", sla_miss.sla_type)
        return

    email_to, email_subject, email_body = email_function(context)
    send_email(email_to, email_subject, email_body)


def describe_task_instance(ti):
    """
    Return a string representation of the task instance.
    """
    return "{dag_id}.{task_id}[{exc_date}]".format(
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        exc_date=ti.execution_date
    )


def get_sla_miss_subject(miss_type, ti):
    """
    Return a consistent subject line for SLA miss emails.
    """
    return "[airflow] [SLA] {miss_type} on {task_instance}".format(
        miss_type=miss_type,
        task_instance=describe_task_instance(ti)
    )


def get_subscribers(tasks):
    """
    Return a list of unique emails from a list of tasks.
    """
    def _yield_subscribers(tasks):
        for t in tasks:
            if t.email:
                if isinstance(t.email, basestring):
                    yield t.email
                else:
                    yield from t.email
    return list(set(_yield_emails(tasks)))


@provide_session
def get_blocked_task_instances(task_instance):
    """
    Given a task instance, return task instances that may currently
    be blocked by it.
    """
    dag = task_instance.task.dag

    TI = airflow.models.TaskInstance
    downstream_tasks = task_instance.get_flat_relatives(upstream=False)

    # The intent is to capture states that indicate that work was never started
    # on a task, presumably because this task not achieving its SLA prevented
    # the downstream task from ever successfully starting.

    # It is possible for an upstream task to cause a downstream task to *fail*,
    # like if it never produces a required artifact. But in a well-behaved DAG
    # where dependencies are encoded properly, this shouldn't happen.
    blocked_states = (
        State.UPSTREAM_FAILED,
        State.SCHEDULED
        State.QUEUED,
        State.NONE,
    )

    qry = (
        session
        .query(TI)
        .filter(TI.dag_id == dag.dag_id)
        .filter(TI.task_id.in_(list(t.task_id for t in downstream_tasks)))
        .filter(TI.execution_date == task_instance.execution_date)
        .filter(TI.state.in_(blocked_states))
        .all()
    )
    return qry


def send_task_duration_exceeded_email(context):
    ti = context["ti"]
    target_time = ti.task.expected_duration
    blocked = get_blocked_task_instances(ti)

    email_to = get_subscribers(blocked)
    email_subject = get_sla_miss_subject("Exceeded duration", ti)
    email_body = """\
    <pre><code>{task_string}</pre></code> missed an SLA: duration exceeded <pre><code>{target_time}</pre></code>.

    View Task Details: {ti_url}

    This may be blocking the following downstream tasks:
    <pre><code>{blocked_tasks}\n{art}</pre></code>
    """.format(
        task_string=describe_task_instance(ti),
        target_time=target_time,
        blocked_tasks="\n".join(describe_task_instance(d) for d in blocked),
        ti_url=ti.details_url,
        art=asciiart.snail)

    return email_to, email_subject, email_body


def send_task_late_start_email(context):
    ti = context["ti"]
    target_time = ti.execution_date + ti.task.expected_start
    blocked = get_blocked_task_instances(ti)

    email_to = get_subscribers(blocked)
    email_subject = get_sla_miss_subject("Late start", ti)
    email_body = """\
    <pre><code>{task_string}</pre></code> missed an SLA: did not start by <pre><code>{target_time}</pre></code>.

    View Task Details: {ti_url}

    This may be blocking the following downstream tasks:
    <pre><code>{blocked_tasks}\n{art}</pre></code>
    """.format(
        task_string=describe_task_instance(ti),
        target_time=target_time,
        blocked_tasks="\n".join(describe_task_instance(d) for d in blocked),
        ti_url=ti.details_url,
        art=asciiart.snail)

    return email_to, email_subject, email_body


def send_task_late_finish_email(context):
    ti = context["ti"]
    target_time = ti.execution_date + ti.task.expected_finish
    blocked = get_blocked_task_instances(ti)

    email_to = get_subscribers(blocked)
    email_subject = get_sla_miss_subject("Late finish", ti)
    email_body = """\
    <pre><code>{task_string}</pre></code> missed an SLA: did not finish by <pre><code>{target_time}</pre></code>.

    View Task Details: {ti_url}

    This may be blocking the following downstream tasks:
    <pre><code>{blocked_tasks}\n{art}</pre></code>
    """.format(
        task_string=describe_task_instance(ti),
        target_time=target_time,
        blocked_tasks="\n".join(describe_task_instance(d) for d in blocked),
        ti_url=ti.details_url,
        art=asciiart.bug)

    return email_to, email_subject, email_body
