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

from time import sleep
from typing import Callable, NoReturn

from sqlalchemy import Column, Index, Integer, String, case
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import backref, foreign, relationship
from sqlalchemy.orm.session import Session, make_transient

from airflow.compat.functools import cached_property
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.executors.executor_loader import ExecutorLoader
from airflow.listeners.listener import get_listener_manager
from airflow.models.base import ID_LEN, Base
from airflow.serialization.pydantic.job import JobPydantic
from airflow.stats import Stats
from airflow.utils import timezone
from airflow.utils.helpers import convert_camel_to_snake
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.net import get_hostname
from airflow.utils.platform import getuser
from airflow.utils.session import NEW_SESSION, create_session, provide_session
from airflow.utils.sqlalchemy import UtcDateTime
from airflow.utils.state import State


def _resolve_dagrun_model():
    from airflow.models.dagrun import DagRun

    return DagRun


class Job(Base, LoggingMixin):
    """
    The ORM class representing Job stored in the database.

    Jobs are processing items with state and duration that aren't task instances.
    For instance a BackfillJob is a collection of task instance runs,
    but should have its own state, start and end time.
    """

    __tablename__ = "job"

    id = Column(Integer, primary_key=True)
    dag_id = Column(
        String(ID_LEN),
    )
    state = Column(String(20))
    job_type = Column(String(30))
    start_date = Column(UtcDateTime())
    end_date = Column(UtcDateTime())
    latest_heartbeat = Column(UtcDateTime())
    executor_class = Column(String(500))
    hostname = Column(String(500))
    unixname = Column(String(1000))

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

    """
    TaskInstances which have been enqueued by this Job.

    Only makes sense for SchedulerJob and BackfillJob instances.
    """

    heartrate = conf.getfloat("scheduler", "JOB_HEARTBEAT_SEC")

    def __init__(self, executor=None, heartrate=None, **kwargs):
        # Save init parameters as DB fields
        self.hostname = get_hostname()
        if executor:
            self.executor = executor
            self.executor_class = executor.__class__.__name__
        else:
            self.executor_class = conf.get("core", "EXECUTOR")
        self.start_date = timezone.utcnow()
        self.latest_heartbeat = timezone.utcnow()
        if heartrate is not None:
            self.heartrate = heartrate
        self.unixname = getuser()
        self.max_tis_per_query: int = conf.getint("scheduler", "max_tis_per_query")
        get_listener_manager().hook.on_starting(component=self)
        super().__init__(**kwargs)

    @cached_property
    def executor(self):
        return ExecutorLoader.get_default_executor()

    def is_alive(self, grace_multiplier=2.1):
        """
        Is this job currently alive.

        We define alive as in a state of RUNNING, and having sent a heartbeat
        within a multiple of the heartrate (default of 2.1)

        :param grace_multiplier: multiplier of heartrate to require heart beat
            within
        """
        return (
            self.state == State.RUNNING
            and (timezone.utcnow() - self.latest_heartbeat).total_seconds()
            < self.heartrate * grace_multiplier
        )

    @provide_session
    def kill(self, session: Session = NEW_SESSION) -> NoReturn:
        """Handles on_kill callback and updates state in database."""
        job = session.query(Job).filter(Job.id == self.id).first()
        job.end_date = timezone.utcnow()
        try:
            self.on_kill()
        except Exception as e:
            self.log.error("on_kill() method failed: %s", str(e))
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
        Heartbeats update the job's entry in the database with a timestamp
        for the latest_heartbeat and allows for the job to be killed
        externally. This allows at the system level to monitor what is
        actually active.

        For instance, an old heartbeat for SchedulerJob would mean something
        is wrong.

        This also allows for any job to be killed externally, regardless
        of who is running it or on which machine it is running.

        Note that if your heart rate is set to 60 seconds and you call this
        method after 10 seconds of processing since the last heartbeat, it
        will sleep 50 seconds to complete the 60 seconds and keep a steady
        heart rate. If you go over 60 seconds before calling it, it won't
        sleep at all.

        :param heartbeat_callback: Callback that will be run when the heartbeat is recorded in the Job
        :param session to use for saving the job
        """
        previous_heartbeat = self.latest_heartbeat

        try:
            # This will cause it to load from the db
            session.merge(self)
            previous_heartbeat = self.latest_heartbeat

            if self.state in State.terminating_states:
                # TODO: Make sure it is AIP-44 compliant
                self.kill()

            # Figure out how long to sleep for
            sleep_for = 0
            if self.latest_heartbeat:
                seconds_remaining = (
                    self.heartrate - (timezone.utcnow() - self.latest_heartbeat).total_seconds()
                )
                sleep_for = max(0, seconds_remaining)
            sleep(sleep_for)

            # Update last heartbeat time
            with create_session() as session:
                # Make the session aware of this object
                session.merge(self)
                self.latest_heartbeat = timezone.utcnow()
                session.commit()
                # At this point, the DB has updated.
                previous_heartbeat = self.latest_heartbeat

                heartbeat_callback(session)
                self.log.debug("[heartbeat]")
        except OperationalError:
            Stats.incr(convert_camel_to_snake(self.__class__.__name__) + "_heartbeat_failure", 1, 1)
            self.log.exception("%s heartbeat got an exception", self.__class__.__name__)
            # We didn't manage to heartbeat, so make sure that the timestamp isn't updated
            self.latest_heartbeat = previous_heartbeat

    @provide_session
    def prepare_for_execution(self, session: Session = NEW_SESSION):
        """Prepares the job for execution."""
        Stats.incr(self.__class__.__name__.lower() + "_start", 1, 1)
        self.state = State.RUNNING
        self.start_date = timezone.utcnow()
        session.add(self)
        session.commit()
        make_transient(self)

    @provide_session
    def complete_execution(self, session: Session = NEW_SESSION):
        get_listener_manager().hook.before_stopping(component=self)
        self.end_date = timezone.utcnow()
        session.merge(self)
        session.commit()
        Stats.incr(self.__class__.__name__.lower() + "_end", 1, 1)

    @provide_session
    def most_recent_job(self, session: Session = NEW_SESSION) -> Job | None:
        """Returns the most recent job of this type, if any, based on last heartbeat received."""
        return most_recent_job(self.job_type, session=session)


@provide_session
def most_recent_job(job_type: str, session: Session = NEW_SESSION) -> Job | None:
    """
    Return the most recent job of this type, if any, based on last heartbeat received.

    Jobs in "running" state take precedence over others to make sure alive
    job is returned if it is available.

    :param job_type: job type to query for to get the most recent job for
    :param session: Database session
    """
    return (
        session.query(Job)
        .filter(Job.job_type == job_type)
        .order_by(
            # Put "running" jobs at the front.
            case({State.RUNNING: 0}, value=Job.state, else_=1),
            Job.latest_heartbeat.desc(),
        )
        .first()
    )


@provide_session
def run_job(
    job: Job | JobPydantic, execute_callable: Callable[[], int | None], session: Session = NEW_SESSION
) -> int | None:
    """
    Runs the job. The Job is always an ORM object and setting the state is happening within the
    same DB session and the session is kept open throughout the whole execution

    :meta private:

    TODO: Maybe we should not keep the session during job execution ?.
    """
    # The below assert is a temporary one, to make MyPy happy with partial AIP-44 work - we will remove it
    # once final AIP-44 changes are completed.
    assert not isinstance(job, JobPydantic), "Job should be ORM object not Pydantic one here (AIP-44 WIP)"
    job.prepare_for_execution(session=session)
    try:
        return execute_job(job, execute_callable=execute_callable)
    finally:
        job.complete_execution(session=session)


def execute_job(job: Job | JobPydantic, execute_callable: Callable[[], int | None]) -> int | None:
    """
    Executes the job.

    Job execution requires no session as generally executing session does not require an
    active database connection. The session might be temporary acquired and used if the job
    runs heartbeat during execution, but this connection is only acquired for the time of heartbeat
    and in case of AIP-44 implementation it happens over the Internal API rather than directly via
    the database.

    After the job is completed, state of the Job is updated and it should be updated in the database,
    which happens in the "complete_execution" step (which again can be executed locally in case of
    database operations or over the Internal API call.

    :param job: Job to execute - it can be either DB job or it's Pydantic serialized version. It does
       not really matter, because except of running the heartbeat and state setting,
       the runner should not modify the job state.

    :param execute_callable: callable to execute when running the job.

    :meta private:
    """
    ret = None
    try:
        ret = execute_callable()
        # In case of max runs or max duration
        job.state = State.SUCCESS
    except SystemExit:
        # In case of ^C or SIGTERM
        job.state = State.SUCCESS
    except Exception:
        job.state = State.FAILED
        raise
    return ret


def perform_heartbeat(
    job: Job | JobPydantic, heartbeat_callback: Callable[[Session], None], only_if_necessary: bool
) -> None:
    """
    Performs heartbeat for the Job passed to it,optionally checking if it is necessary.

    :param job: job to perform heartbeat for
    :param heartbeat_callback: callback to run by the heartbeat
    :param only_if_necessary: only heartbeat if it is necessary (i.e. if there are things to run for
        triggerer for example)
    """
    # The below assert is a temporary one, to make MyPy happy with partial AIP-44 work - we will remove it
    # once final AIP-44 changes are completed.
    assert not isinstance(job, JobPydantic), "Job should be ORM object not Pydantic one here (AIP-44 WIP)"
    seconds_remaining: float = 0.0
    if job.latest_heartbeat and job.heartrate:
        seconds_remaining = job.heartrate - (timezone.utcnow() - job.latest_heartbeat).total_seconds()
    if seconds_remaining > 0 and only_if_necessary:
        return
    with create_session() as session:
        job.heartbeat(heartbeat_callback=heartbeat_callback, session=session)
