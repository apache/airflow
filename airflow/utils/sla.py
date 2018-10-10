import airflow.models
from airflow.utils import asciiart
from airflow.utils.db import provide_session
from airflow.utils.email import send_email
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State

log = LoggingMixin().log


def yield_unscheduled_runs(dag, last_scheduled_run, ts):
    """
    Yield new DagRuns that haven't been created yet. This functionality is
    important to SLA misses because it is possible for the scheduler to fall
    so far behind that it cannot create a DAGRun when it is supposed to (like
    if it is offline, or if there are strict concurrency limits). We need to
    understand and alert on what DAGRuns *should* have been created by this
    point in time.
    """

    # TODO: A lot of this logic is duplicated from the scheduler. It would
    # be better to have one function that yields upcoming DAG runs in a
    # consistent way that is usable for both use cases.

    # Start by assuming that there is no next run.
    next_run_date = None

    # The first DAGRun has not been created yet.
    if not last_scheduled_run:
        task_start_dates = [t.start_date for t in dag.tasks]
        if task_start_dates:
            next_run_date = dag.normalize_schedule(min(task_start_dates))
    # The DagRun is @once and has already happened.
    elif dag.schedule_interval == '@once':
        return
    # Start from the next "normal" run.
    else:
        next_run_date = dag.following_schedule(last_scheduled_run.execution_date)

    while True:
        # There should be a next execution.
        if not next_run_date:
            return

        # The next execution shouldn't be in the future.
        if next_run_date > ts:
            return

        # The next execution shouldn't be beyond the DAG's end date.
        # n.b. - tasks have their own end dates checked later
        if next_run_date and dag.end_date and next_run_date > dag.end_date:
            return

        # Calculate the end of this execution period.
        if dag.schedule_interval == '@once':
            period_end = next_run_date
        else:
            period_end = dag.following_schedule(next_run_date)

        # The next execution shouldn't still be mid-period.
        if period_end > ts:
            return

        # We've passed every filter; this is a valid future DagRun that
        # presumably hasn't been scheduled due to concurrency limits.
        # Create a DAGRun, though it won't exist in the db yet.
        next_run = dag.create_dagrun(
            run_id=airflow.models.DagRun.ID_PREFIX + next_run_date.isoformat(),
            execution_date=next_run_date,
            start_date=ts,
            state=State.RUNNING,
            external_trigger=False
        )
        yield next_run

        # Examine the next date.
        next_run_date = dag.following_schedule(next_run_date)


def yield_unscheduled_tis(dag_run, ts, session=None):
    """
    Given an unscheduled `DagRun`, yield any unscheduled TIs that will exist
    for it in the future, respecting the end date of the DAG and task. See note
    above for why this is important for SLA notifications.
    """
    for task in dag_run.dag.tasks:
        end_dates = []
        if dag_run.dag.end_date:
            end_dates.append(dag_run.dag.end_date)
        if task.end_date:
            end_dates.append(task.end_date)

        # Create TIs if there is no end date, or it hasn't happened yet.
        if not end_dates or ts < min(end_dates):
            yield airflow.models.TaskInstance(task, dag_run.execution_date)


def get_sla_misses(ti, session):
    """
    Get all SLA misses that match a particular TaskInstance. There may be
    several matches if the Task has several independent SLAs.
    """
    SM = airflow.models.SlaMiss
    return session.query(SM).filter(
        SM.dag_id == ti.dag_id,
        SM.task_id == ti.task_id,
        SM.execution_date == ti.execution_date
    ).all()


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

    SM = airflow.models.SlaMiss

    # Get existing misses.
    ti_misses = {sm.sla_type: sm for sm in get_sla_misses(ti, session)}

    # Calculate each type of SLA miss. Wrapping exceptions here is
    # important so that an exception in one type of SLA doesn't
    # prevent other task SLAs from getting triggered.

    # SLA Miss for Expected Duration
    if SM.TASK_DURATION_EXCEEDED not in ti_misses \
            and ti.task.expected_duration and ti.start_date:
        try:
            if ti.state in State.finished():
                duration = ti.end_date - ti.start_date
            else:
                # Use the current time, if the task is still running.
                duration = ts - ti.start_date

            if duration > ti.task.expected_duration:
                log.debug("Created duration exceeded SLA miss for %s.%s [%s]",
                          ti.dag_id, ti.task_id, ti.execution_date)
                session.merge(SM(
                    task_id=ti.task_id,
                    dag_id=ti.dag_id,
                    execution_date=ti.execution_date,
                    sla_type=SM.TASK_DURATION_EXCEEDED,
                    timestamp=ts))
        except Exception:
            log.exception(
                "Failed to calculate expected duration SLA miss for "
                "task %s",
                ti
            )

    # SLA Miss for Expected Start
    if SM.TASK_LATE_START not in ti_misses and ti.task.expected_start:
        try:
            # If a TI's exc date is 01-01-2018, we expect it to start by the next
            # execution date (01-02-2018) plus a delta of expected_start.
            expected_start = ti.task.dag.following_schedule(ti.execution_date)
            expected_start += ti.task.expected_start

            # "now" is the start date for comparison, if the TI hasn't started
            actual_start = ti.start_date or ts

            if actual_start > expected_start:
                log.debug("Created expected start SLA miss for %s.%s [%s]",
                          ti.dag_id, ti.task_id, ti.execution_date)
                session.merge(SM(
                    task_id=ti.task_id,
                    dag_id=ti.dag_id,
                    execution_date=ti.execution_date,
                    sla_type=SM.ti_misses,
                    timestamp=ts))
        except Exception:
            log.exception(
                "Failed to calculate expected start SLA miss for "
                "task %s",
                ti
            )

    # SLA Miss for Expected Finish
    if SM.TASK_LATE_FINISH not in ti_misses and ti.task.expected_finish:
        try:
            # If a TI's exc date is 01-01-2018, we expect it to finish by the next
            # execution date (01-02-2018) plus a delta of expected_finish.
            expected_finish = ti.task.dag.following_schedule(ti.execution_date)
            expected_finish += ti.task.expected_finish

            # "now" is the end date for comparison, if the TI hasn't finished
            actual_finish = ti.end_date or ts

            if actual_finish > expected_finish:
                log.debug("Created expected finish SLA miss for %s.%s [%s]",
                          ti.dag_id, ti.task_id, ti.execution_date)
                session.merge(SM(
                    task_id=ti.task_id,
                    dag_id=ti.dag_id,
                    execution_date=ti.execution_date,
                    sla_type=SM.TASK_LATE_FINISH,
                    timestamp=ts))
        except Exception:
            log.exception(
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
                    for e in t.email:
                        yield e
    return list(set(_yield_subscribers(tasks)))


@provide_session
def get_impacted_downstream_task_instances(task_instance, session=None):
    """
    Given a task instance that has had an SLA miss, return any
    downstream task instances that may have been impacted too. In this case, we
    mean any tasks that may now themselves be delayed due to the initial delay,
    even if the downstream tasks themselves do not have SLAs set.
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
        State.SCHEDULED,
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
    """
    This helper function is the default implementation for a duration SLA
    miss callback. It sends an email to all subscribers explaining that the
    task is taking too long, which downstream tasks may be impacted, and
    where to go for further information.

    Note that if the task instance hasn't been created yet (such as scheduler
    concurrency limits), this function may have a "dummy" task instance in its
    context. In that case, it will not exist in the db or have a full set of
    attributes.
    """

    ti = context["ti"]
    target_time = ti.task.expected_duration
    impacted_downstreams = get_impacted_downstream_task_instances(ti)

    email_to = get_subscribers(impacted_downstreams)
    email_subject = get_sla_miss_subject("Exceeded duration", ti)
    email_body = ""
    "<pre><code>{task_string}</pre></code> missed an SLA: duration "
    "exceeded <pre><code>{target_time}</pre></code>.\n\n"

    "View Task Details: {ti_url}\n\n"

    "This may be impacting the following downstream tasks:\n"
    "<pre><code>{blocked_tasks}\n{art}</pre></code>".format(
        task_string=describe_task_instance(ti),
        target_time=target_time,
        impacted_downstreams="\n".join(
            describe_task_instance(d) for d in impacted_downstreams),
        ti_url=ti.details_url,
        art=asciiart.snail)

    return email_to, email_subject, email_body


def send_task_late_start_email(context):
    """
    This helper function is the default implementation for a late finish SLA
    miss callback. It sends an email to all subscribers explaining that the
    task hasn't started on time, which downstream tasks may be impacted, and
    where to go for further information.

    Note that if the task instance hasn't been created yet (such as scheduler
    concurrency limits), this function may have a "dummy" task instance in its
    context. In that case, it will not exist in the db or have a full set of
    attributes.
    """
    ti = context["ti"]
    target_time = ti.execution_date + ti.task.expected_start
    impacted_downstreams = get_impacted_downstream_task_instances(ti)

    email_to = get_subscribers(impacted_downstreams)
    email_subject = get_sla_miss_subject("Late start", ti)
    email_body = ""
    "<pre><code>{task_string}</pre></code> missed an SLA: did not start by "
    "<pre><code>{target_time}</pre></code>.\n\n"

    "View Task Details: {ti_url}\n\n"

    "This may be impacting the following downstream tasks:\n"
    "<pre><code>{impacted_downstreams}\n{art}</pre></code>".format(
        task_string=describe_task_instance(ti),
        target_time=target_time,
        impacted_downstreams="\n".join(
            describe_task_instance(d) for d in impacted_downstreams),
        ti_url=ti.details_url,
        art=asciiart.snail)

    return email_to, email_subject, email_body


def send_task_late_finish_email(context):
    """
    This helper function is the default implementation for a late finish SLA
    miss callback. It sends an email to all subscribers explaining that the
    task hasn't finished on time, which downstream tasks may be impacted, and
    where to go for further information.

    Note that if the task instance hasn't been created yet (such as scheduler
    concurrency limits), this function may have a "dummy" task instance in its
    context. In that case, it will not exist in the db or have a full set of
    attributes.
    """
    ti = context["ti"]
    target_time = ti.execution_date + ti.task.expected_finish
    impacted_downstreams = get_impacted_downstream_task_instances(ti)

    email_to = get_subscribers(impacted_downstreams)
    email_subject = get_sla_miss_subject("Late finish", ti)
    email_body = ""
    "<pre><code>{task_string}</pre></code> missed an SLA: did not finish by "
    "<pre><code>{target_time}</pre></code>.\n\n"

    "View Task Details: {ti_url}\n\n"

    "This may be impacting the following downstream tasks:\n"
    "<pre><code>{impacted_downstreams}\n{art}</pre></code>".format(
        task_string=describe_task_instance(ti),
        target_time=target_time,
        impacted_downstreams="\n".join(
            describe_task_instance(d) for d in impacted_downstreams),
        ti_url=ti.details_url,
        art=asciiart.snail)

    return email_to, email_subject, email_body
