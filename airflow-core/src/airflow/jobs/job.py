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
from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from enum import Enum
from functools import cached_property, lru_cache
from time import sleep
from typing import TYPE_CHECKING, NoReturn

from sqlalchemy import Index, Integer, String, case, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Mapped, backref, foreign, relationship
from sqlalchemy.orm.session import make_transient

from airflow._shared.timezones import timezone
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.executors.executor_loader import ExecutorLoader
from airflow.listeners.listener import get_listener_manager
from airflow.models.base import ID_LEN, Base
from airflow.observability.stats import Stats
from airflow.observability.traces.tracer import DebugTrace, add_debug_span
from airflow.utils.helpers import convert_camel_to_snake
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.net import get_hostname
from airflow.utils.platform import getuser
from airflow.utils.session import NEW_SESSION, create_session, provide_session
from airflow.utils.sqlalchemy import UtcDateTime, mapped_column


class JobState(str, Enum):
    """All possible states that a Job can be in."""

    RUNNING = "running"
    SUCCESS = "success"
    RESTARTING = "restarting"
    FAILED = "failed"

    def __str__(self) -> str:
        return self.value


if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session

    from airflow.executors.base_executor import BaseExecutor


def _resolve_dagrun_model():
    from airflow.models.dagrun import DagRun

    return DagRun


@lru_cache
def health_check_threshold(job_type: str, heartrate: int) -> int | float:
    grace_multiplier = 2.1
    health_check_threshold_value: int | float
    if job_type == "SchedulerJob":
        health_check_threshold_value = conf.getint("scheduler", "scheduler_health_check_threshold")
    elif job_type == "TriggererJob":
        health_check_threshold_value = conf.getfloat("triggerer", "triggerer_health_check_threshold")
    else:
        health_check_threshold_value = heartrate * grace_multiplier
    return health_check_threshold_value


class Job(Base, LoggingMixin):
    """
    The ORM class representing Job stored in the database.

    Jobs are processing items with state and duration that aren't task instances.
    """

    __tablename__ = "job"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dag_id: Mapped[str | None] = mapped_column(
        String(ID_LEN),
    )
    state: Mapped[str | None] = mapped_column(String(20))
    job_type: Mapped[str | None] = mapped_column(String(30))
    start_date: Mapped[datetime | None] = mapped_column(UtcDateTime())
    end_date: Mapped[datetime | None] = mapped_column(UtcDateTime())
    latest_heartbeat: Mapped[datetime | None] = mapped_column(UtcDateTime())
    executor_class: Mapped[str | None] = mapped_column(String(500))
    hostname: Mapped[str | None] = mapped_column(String(500))
    unixname: Mapped[str | None] = mapped_column(String(1000))

    __table_args__ = (
        Index("job_type_heart", job_type, latest_heartbeat),
        Index("idx_job_state_heartbeat", state, latest_heartbeat),
        Index("idx_job_dag_id", dag_id),
    )

    task_instances_enqueued = relationship(
        "TaskInstance",
        primaryjoin="Job.id == foreign(TaskInstance.queued_by_job_id)",
        backref=backref("queued_by_job", uselist=False),
    )

    dag_runs = relationship(
        "DagRun",
        primaryjoin=lambda: Job.id == foreign(_resolve_dagrun_model().creating_job_id),
        backref="creating_job",
    )

    dag_model = relationship(
        "DagModel",
        primaryjoin="Job.dag_id == DagModel.dag_id",
        viewonly=True,
        foreign_keys=[dag_id],
    )

    """
    TaskInstances which have been enqueued by this Job.

    Only makes sense for SchedulerJob.
    """

    def __init__(self, executor: BaseExecutor | None = None, heartrate=None, **kwargs):
        # Save init parameters as DB fields
        self.heartbeat_failed = False
        self.hostname = get_hostname()
        if executor:
            self.executors = [executor]
        self.start_date = timezone.utcnow()
        self.latest_heartbeat = timezone.utcnow()
        self.previous_heartbeat = None
        if heartrate is not None:
            self.heartrate = heartrate
        self.unixname = getuser()
        self.max_tis_per_query: int = conf.getint("scheduler", "max_tis_per_query")
        try:
            get_listener_manager().hook.on_starting(component=self)
        except Exception:
            self.log.exception("error calling listener")
        super().__init__(**kwargs)

    @property
    def executor(self):
        return self.executors[0]

    @cached_property
    def executors(self):
        return ExecutorLoader.init_executors()

    @cached_property
    def heartrate(self) -> float:
        return Job._heartrate(str(self.job_type))

    def is_alive(self) -> bool:
        """
        Is this job currently alive.

        We define alive as in a state of RUNNING, and having sent a heartbeat
        within a multiple of the heartrate (default of 2.1)
        """
        threshold_value = health_check_threshold(self.job_type, self.heartrate)
        return Job._is_alive(
            state=self.state,
            health_check_threshold_value=threshold_value,
            latest_heartbeat=self.latest_heartbeat,
        )

    @provide_session
    def kill(self, session: Session = NEW_SESSION) -> NoReturn:
        """Handle on_kill callback and updates state in database."""
        try:
            self.on_kill()
        except Exception as e:
            self.log.error("on_kill() method failed: %s", e)

        job = session.scalar(select(Job).where(Job.id == self.id).limit(1))
        if job is not None:
            job.end_date = timezone.utcnow()
            session.merge(job)
            session.commit()
        raise AirflowException("Job shut down externally.")

    def on_kill(self):
        """Will be called when an external kill command is received."""

    @provide_session
    def heartbeat(
        self, heartbeat_callback: Callable[[Session], None], session: Session = NEW_SESSION
    ) -> None:
        """
        Update the job's entry in the database with the latest_heartbeat timestamp.

        This allows for the job to be killed externally and allows the system
        to monitor what is actually active.  For instance, an old heartbeat
        for SchedulerJob would mean something is wrong.  This also allows for
        any job to be killed externally, regardless of who is running it or on
        which machine it is running.

        Note that if your heart rate is set to 60 seconds and you call this
        method after 10 seconds of processing since the last heartbeat, it
        will sleep 50 seconds to complete the 60 seconds and keep a steady
        heart rate. If you go over 60 seconds before calling it, it won't
        sleep at all.

        :param heartbeat_callback: Callback that will be run when the heartbeat is recorded in the Job
        :param session to use for saving the job
        """
        previous_heartbeat = self.latest_heartbeat
        with DebugTrace.start_span(span_name="heartbeat", component="Job") as span:
            try:
                span.set_attribute("heartbeat", str(self.latest_heartbeat))
                # This will cause it to load from the db
                session.merge(self)
                previous_heartbeat = self.latest_heartbeat

                if self.state == JobState.RESTARTING:
                    self.kill()

                # Figure out how long to sleep for
                sleep_for: float = 0
                if self.latest_heartbeat:
                    seconds_remaining = (
                        self.heartrate - (timezone.utcnow() - self.latest_heartbeat).total_seconds()
                    )
                    sleep_for = max(0, seconds_remaining)
                if span.is_recording():
                    span.add_event(name="sleep", attributes={"sleep_for": sleep_for})
                sleep(sleep_for)
                # Update last heartbeat time
                with create_session() as session:
                    # Make the session aware of this object
                    session.merge(self)
                    self.latest_heartbeat = timezone.utcnow()
                    session.commit()
                    time_since_last_heartbeat: float = (
                        0
                        if previous_heartbeat is None
                        else (timezone.utcnow() - previous_heartbeat).total_seconds()
                    )
                    health_check_threshold_value = health_check_threshold(self.job_type, self.heartrate)
                    if time_since_last_heartbeat > health_check_threshold_value:
                        self.log.info("Heartbeat recovered after %.2f seconds", time_since_last_heartbeat)
                    # At this point, the DB has updated.
                    previous_heartbeat = self.latest_heartbeat
                    heartbeat_callback(session)
                self.log.debug("[heartbeat]")
                self.heartbeat_failed = False
            except OperationalError:
                Stats.incr(convert_camel_to_snake(self.__class__.__name__) + "_heartbeat_failure", 1, 1)
                if not self.heartbeat_failed:
                    self.log.exception("%s heartbeat failed with error", self.__class__.__name__)
                    self.heartbeat_failed = True
                    msg = f"{self.__class__.__name__} heartbeat got an exception"
                    if span.is_recording():
                        span.add_event(name="error", attributes={"message": msg})
                if self.is_alive():
                    self.log.error(
                        "%s heartbeat failed with error. Scheduler may go into unhealthy state",
                        self.__class__.__name__,
                    )
                    msg = f"{self.__class__.__name__} heartbeat failed with error. Scheduler may go into unhealthy state"
                    if span.is_recording():
                        span.add_event(name="error", attributes={"message": msg})
                else:
                    msg = f"{self.__class__.__name__} heartbeat failed with error. Scheduler is in unhealthy state"
                    self.log.error(msg)
                    if span.is_recording():
                        span.add_event(name="error", attributes={"message": msg})
                # We didn't manage to heartbeat, so make sure that the timestamp isn't updated
                self.latest_heartbeat = previous_heartbeat

    @provide_session
    def prepare_for_execution(self, session: Session = NEW_SESSION):
        """Prepare the job for execution."""
        Stats.incr(self.__class__.__name__.lower() + "_start", 1, 1)
        self.state = JobState.RUNNING
        self.start_date = timezone.utcnow()
        session.add(self)
        session.commit()
        make_transient(self)

    @provide_session
    def complete_execution(self, session: Session = NEW_SESSION):
        try:
            get_listener_manager().hook.before_stopping(component=self)
        except Exception:
            self.log.exception("error calling listener")
        self.end_date = timezone.utcnow()
        session.merge(self)
        session.commit()
        Stats.incr(self.__class__.__name__.lower() + "_end", 1, 1)

    @provide_session
    def most_recent_job(self, session: Session = NEW_SESSION) -> Job | None:
        """Return the most recent job of this type, if any, based on last heartbeat received."""
        return most_recent_job(str(self.job_type), session=session)

    @staticmethod
    def _heartrate(job_type: str) -> float:
        if job_type == "TriggererJob":
            return conf.getfloat("triggerer", "JOB_HEARTBEAT_SEC")
        if job_type == "SchedulerJob":
            return conf.getfloat("scheduler", "SCHEDULER_HEARTBEAT_SEC")
        # Heartrate used to be hardcoded to scheduler, so in all other
        # cases continue to use that value for back compat
        return conf.getfloat("scheduler", "JOB_HEARTBEAT_SEC")

    @staticmethod
    def _is_alive(
        state: JobState | str | None,
        health_check_threshold_value: float | int,
        latest_heartbeat: datetime | None,
    ) -> bool:
        if latest_heartbeat is None:
            return False
        return (
            state == JobState.RUNNING
            and (timezone.utcnow() - latest_heartbeat).total_seconds() < health_check_threshold_value
        )


@provide_session
def most_recent_job(job_type: str, session: Session = NEW_SESSION) -> Job | None:
    """
    Return the most recent job of this type, if any, based on last heartbeat received.

    Jobs in "running" state take precedence over others to make sure alive
    job is returned if it is available.

    :param job_type: job type to query for to get the most recent job for
    :param session: Database session
    """
    return session.scalar(
        select(Job)
        .where(Job.job_type == job_type)
        .order_by(
            # Put "running" jobs at the front.
            case({JobState.RUNNING: 0}, value=Job.state, else_=1),
            Job.latest_heartbeat.desc(),
        )
        .limit(1)
    )


@provide_session
def run_job(
    job: Job, execute_callable: Callable[[], int | None], session: Session = NEW_SESSION
) -> int | None:
    """
    Run the job.

    The Job is always an ORM object and setting the state is happening within the
    same DB session and the session is kept open throughout the whole execution.

    :meta private:
    """
    job.prepare_for_execution(session=session)
    try:
        return execute_job(job, execute_callable=execute_callable)
    finally:
        job.complete_execution(session=session)


def execute_job(job: Job, execute_callable: Callable[[], int | None]) -> int | None:
    """
    Execute the job.

    Job execution requires no session as generally executing session does not require an
    active database connection. The session might be temporary acquired and used if the job
    runs heartbeat during execution, but this connection is only acquired for the time of heartbeat
    and in case of AIP-44 implementation it happens over the Internal API rather than directly via
    the database.

    After the job is completed, state of the Job is updated and it should be updated in the database,
    which happens in the "complete_execution" step (which again can be executed locally in case of
    database operations or over the Internal API call.

    :param job: Job to execute - DB job. It does
      not really matter, because except of running the heartbeat and state setting,
      the runner should not modify the job state.

    :param execute_callable: callable to execute when running the job.

    :meta private:
    """
    ret = None
    try:
        ret = execute_callable()
        # In case of max runs or max duration
        job.state = JobState.SUCCESS
    except SystemExit:
        # In case of ^C or SIGTERM
        job.state = JobState.SUCCESS
    except Exception:
        job.state = JobState.FAILED
        raise
    return ret


@add_debug_span
def perform_heartbeat(
    job: Job, heartbeat_callback: Callable[[Session], None], only_if_necessary: bool
) -> None:
    """
    Perform heartbeat for the Job passed to it,optionally checking if it is necessary.

    :param job: job to perform heartbeat for
    :param heartbeat_callback: callback to run by the heartbeat
    :param only_if_necessary: only heartbeat if it is necessary (i.e. if there are things to run for
        triggerer for example)
    """
    seconds_remaining: float = 0.0
    if job.latest_heartbeat and job.heartrate:
        seconds_remaining = job.heartrate - (timezone.utcnow() - job.latest_heartbeat).total_seconds()
    if seconds_remaining > 0 and only_if_necessary:
        return
    job.heartbeat(heartbeat_callback=heartbeat_callback)
