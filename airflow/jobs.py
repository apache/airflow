# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from builtins import str
from past.builtins import basestring
from collections import defaultdict, Counter
from datetime import datetime
from itertools import product
import getpass
import logging
import signal
import socket
import subprocess
import sys
from time import sleep

from sqlalchemy import Column, Integer, String, DateTime, func, Index, or_
from sqlalchemy.orm.session import make_transient

from airflow import executors, models, settings
from airflow import configuration as conf
from airflow.exceptions import AirflowException
from airflow.utils.state import State
from airflow.utils.db import provide_session, pessimistic_connection_handling
from airflow.utils.email import send_email
from airflow.utils.logging import LoggingMixin
from airflow.utils import asciiart


Base = models.Base
ID_LEN = models.ID_LEN
Stats = settings.Stats


class BaseJob(Base, LoggingMixin):
    """
    Abstract class to be derived for jobs. Jobs are processing items with state
    and duration that aren't task instances. For instance a BackfillJob is
    a collection of task instance runs, but should have it's own state, start
    and end time.
    """

    __tablename__ = "job"

    id = Column(Integer, primary_key=True)
    dag_id = Column(String(ID_LEN),)
    state = Column(String(20))
    job_type = Column(String(30))
    start_date = Column(DateTime())
    end_date = Column(DateTime())
    latest_heartbeat = Column(DateTime())
    executor_class = Column(String(500))
    hostname = Column(String(500))
    unixname = Column(String(1000))

    __mapper_args__ = {
        'polymorphic_on': job_type,
        'polymorphic_identity': 'BaseJob'
    }

    __table_args__ = (
        Index('job_type_heart', job_type, latest_heartbeat),
    )

    def __init__(
            self,
            executor=executors.DEFAULT_EXECUTOR,
            heartrate=conf.getfloat('scheduler', 'JOB_HEARTBEAT_SEC'),
            *args, **kwargs):
        self.hostname = socket.gethostname()
        self.executor = executor
        self.executor_class = executor.__class__.__name__
        self.start_date = datetime.now()
        self.latest_heartbeat = datetime.now()
        self.heartrate = heartrate
        self.unixname = getpass.getuser()
        super(BaseJob, self).__init__(*args, **kwargs)

    def is_alive(self):
        return (
            (datetime.now() - self.latest_heartbeat).seconds <
            (conf.getint('scheduler', 'JOB_HEARTBEAT_SEC') * 2.1)
        )

    def kill(self):
        session = settings.Session()
        job = session.query(BaseJob).filter(BaseJob.id == self.id).first()
        job.end_date = datetime.now()
        try:
            self.on_kill()
        except:
            self.logger.error('on_kill() method failed')
        session.merge(job)
        session.commit()
        session.close()
        raise AirflowException("Job shut down externally.")

    def on_kill(self):
        '''
        Will be called when an external kill command is received
        '''
        pass

    def heartbeat_callback(self):
        pass

    def heartbeat(self):
        '''
        Heartbeats update the job's entry in the database with a timestamp
        for the latest_heartbeat and allows for the job to be killed
        externally. This allows at the system level to monitor what is
        actually active.

        For instance, an old heartbeat for SchedulerJob would mean something
        is wrong.

        This also allows for any job to be killed externally, regardless
        of who is running it or on which machine it is running.

        Note that if your heartbeat is set to 60 seconds and you call this
        method after 10 seconds of processing since the last heartbeat, it
        will sleep 50 seconds to complete the 60 seconds and keep a steady
        heart rate. If you go over 60 seconds before calling it, it won't
        sleep at all.
        '''
        session = settings.Session()
        job = session.query(BaseJob).filter(BaseJob.id == self.id).first()

        if job.state == State.SHUTDOWN:
            self.kill()

        if job.latest_heartbeat:
            sleep_for = self.heartrate - (
                datetime.now() - job.latest_heartbeat).total_seconds()
            if sleep_for > 0:
                sleep(sleep_for)

        job.latest_heartbeat = datetime.now()

        session.merge(job)
        session.commit()
        session.close()

        self.heartbeat_callback()
        self.logger.debug('[heart] Boom.')

    def run(self):
        Stats.incr(self.__class__.__name__.lower()+'_start', 1, 1)
        # Adding an entry in the DB
        session = settings.Session()
        self.state = State.RUNNING
        session.add(self)
        session.commit()
        id_ = self.id
        make_transient(self)
        self.id = id_

        # Run
        self._execute()

        # Marking the success in the DB
        self.end_date = datetime.now()
        self.state = State.SUCCESS
        session.merge(self)
        session.commit()
        session.close()

        Stats.incr(self.__class__.__name__.lower()+'_end', 1, 1)

    def _execute(self):
        raise NotImplementedError("This method needs to be overridden")


class SchedulerJob(BaseJob):
    """
    This SchedulerJob runs indefinitely and constantly schedules the jobs
    that are ready to run. It figures out the latest runs for each
    task and see if the dependencies for the next schedules are met.
    If so it triggers the task instance. It does this for each task
    in each DAG and repeats.

    :param dag_id: to run the scheduler for a single specific DAG
    :type dag_id: string
    :param subdir: to search for DAG under a certain folder only
    :type subdir: string
    :param test_mode: used for unit testing this class only, runs a single
        schedule run
    :type test_mode: bool
    :param refresh_dags_every: force refresh the DAG definition every N
        runs, as specified here
    :type refresh_dags_every: int
    :param do_pickle: to pickle the DAG object and send over to workers
        for non-local executors
    :type do_pickle: bool
    """

    __mapper_args__ = {
        'polymorphic_identity': 'SchedulerJob'
    }

    def __init__(
            self,
            dag_id=None,
            subdir=None,
            test_mode=False,
            refresh_dags_every=10,
            num_runs=None,
            do_pickle=False,
            *args, **kwargs):

        self.dag_id = dag_id
        self.subdir = subdir

        if test_mode:
            self.num_runs = 1
        else:
            self.num_runs = num_runs

        self.refresh_dags_every = refresh_dags_every
        self.do_pickle = do_pickle
        self.queued_tis = set()
        super(SchedulerJob, self).__init__(*args, **kwargs)

        self.heartrate = conf.getint('scheduler', 'SCHEDULER_HEARTBEAT_SEC')

    @provide_session
    def manage_slas(self, dag, session=None):
        """
        Finding all tasks that have SLAs defined, and sending alert emails
        where needed. New SLA misses are also recorded in the database.

        Where assuming that the scheduler runs often, so we only check for
        tasks that should have succeeded in the past hour.
        """
        TI = models.TaskInstance
        sq = (
            session
            .query(
                TI.task_id,
                func.max(TI.execution_date).label('max_ti'))
            .filter(TI.dag_id == dag.dag_id)
            .filter(TI.state == State.SUCCESS)
            .filter(TI.task_id.in_(dag.task_ids))
            .group_by(TI.task_id).subquery('sq')
        )

        max_tis = session.query(TI).filter(
            TI.dag_id == dag.dag_id,
            TI.task_id == sq.c.task_id,
            TI.execution_date == sq.c.max_ti,
        ).all()

        ts = datetime.now()
        SlaMiss = models.SlaMiss
        for ti in max_tis:
            task = dag.get_task(ti.task_id)
            dttm = ti.execution_date
            if task.sla:
                dttm = dag.following_schedule(dttm)
                while dttm < datetime.now():
                    following_schedule = dag.following_schedule(dttm)
                    if following_schedule + task.sla < datetime.now():
                        session.merge(models.SlaMiss(
                            task_id=ti.task_id,
                            dag_id=ti.dag_id,
                            execution_date=dttm,
                            timestamp=ts))
                    dttm = dag.following_schedule(dttm)
        session.commit()

        slas = (
            session
            .query(SlaMiss)
            .filter(SlaMiss.email_sent == False or SlaMiss.notification_sent == False)
            .filter(SlaMiss.dag_id == dag.dag_id)
            .all()
        )

        if slas:
            sla_dates = [sla.execution_date for sla in slas]
            qry = (
                session
                .query(TI)
                .filter(TI.state != State.SUCCESS)
                .filter(TI.execution_date.in_(sla_dates))
                .filter(TI.dag_id == dag.dag_id)
                .all()
            )
            blocking_tis = []
            for ti in qry:
                if ti.task_id in dag.task_ids:
                    ti.task = dag.get_task(ti.task_id)
                    blocking_tis.append(ti)
                else:
                    session.delete(ti)
                    session.commit()

            blocking_tis = ([ti for ti in blocking_tis
                            if ti.are_dependencies_met(main_session=session)])
            task_list = "\n".join([
                sla.task_id + ' on ' + sla.execution_date.isoformat()
                for sla in slas])
            blocking_task_list = "\n".join([
                ti.task_id + ' on ' + ti.execution_date.isoformat()
                for ti in blocking_tis])
            # Track whether email or any alert notification sent
            # We consider email or the alert callback as notifications
            email_sent = False
            notification_sent = False
            if dag.sla_miss_callback:
                # Execute the alert callback
                self.logger.info(' --------------> ABOUT TO CALL SLA MISS CALL BACK ')
                dag.sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis)
                notification_sent = True
            email_content = """\
            Here's a list of tasks thas missed their SLAs:
            <pre><code>{task_list}\n<code></pre>
            Blocking tasks:
            <pre><code>{blocking_task_list}\n{asciiart.bug}<code></pre>
            """.format(**locals())
            emails = []
            for t in dag.tasks:
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
                    "[airflow] SLA miss on DAG=" + dag.dag_id,
                    email_content)
                email_sent = True
                notification_sent = True
            # If we sent any notification, update the sla_miss table
            if notification_sent:
                for sla in slas:
                    if email_sent:
                        sla.email_sent = True
                    sla.notification_sent = True
                    session.merge(sla)
            session.commit()
            session.close()

    def import_errors(self, dagbag):
        session = settings.Session()
        session.query(models.ImportError).delete()
        for filename, stacktrace in list(dagbag.import_errors.items()):
            session.add(models.ImportError(
                filename=filename, stacktrace=stacktrace))
        session.commit()

    def schedule_dag(self, dag):
        """
        This method checks whether a new DagRun needs to be created
        for a DAG based on scheduling interval
        Returns DagRun if one is scheduled. Otherwise returns None.
        """
        if dag.schedule_interval:
            DagRun = models.DagRun
            session = settings.Session()
            qry = session.query(DagRun).filter(
                DagRun.dag_id == dag.dag_id,
                DagRun.external_trigger == False,
                DagRun.state == State.RUNNING,
            )
            active_runs = qry.all()
            if len(active_runs) >= dag.max_active_runs:
                return
            for dr in active_runs:
                if (
                        dr.start_date and dag.dagrun_timeout and
                        dr.start_date < datetime.now() - dag.dagrun_timeout):
                    dr.state = State.FAILED
                    dr.end_date = datetime.now()
            session.commit()

            qry = session.query(func.max(DagRun.execution_date)).filter_by(
                    dag_id = dag.dag_id).filter(
                        or_(DagRun.external_trigger == False,
                            # add % as a wildcard for the like query
                            DagRun.run_id.like(DagRun.ID_PREFIX+'%')))
            last_scheduled_run = qry.scalar()
            next_run_date = None
            if dag.schedule_interval == '@once' and not last_scheduled_run:
                next_run_date = datetime.now()
            elif not last_scheduled_run:
                # First run
                TI = models.TaskInstance
                latest_run = (
                    session.query(func.max(TI.execution_date))
                    .filter_by(dag_id=dag.dag_id)
                    .scalar()
                )
                if latest_run:
                    # Migrating from previous version
                    # make the past 5 runs active
                    next_run_date = dag.date_range(latest_run, -5)[0]
                    if dag.start_date:
                        next_run_date = max(next_run_date, dag.start_date)
                else:
                    task_start_dates = [t.start_date for t in dag.tasks]
                    if task_start_dates:
                        next_run_date = min(task_start_dates)
                    else:
                        next_run_date = None
            elif dag.schedule_interval != '@once':
                next_run_date = dag.following_schedule(last_scheduled_run)

            # this structure is necessary to avoid a TypeError from concatenating
            # NoneType
            if dag.schedule_interval == '@once':
                schedule_end = next_run_date
            elif next_run_date:
                schedule_end = dag.following_schedule(next_run_date)

            if next_run_date and dag.end_date and next_run_date > dag.end_date:
                return

            if next_run_date and schedule_end and schedule_end <= datetime.now():
                next_run = DagRun(
                    dag_id=dag.dag_id,
                    run_id='scheduled__' + next_run_date.isoformat(),
                    execution_date=next_run_date,
                    start_date=datetime.now(),
                    state=State.RUNNING,
                    external_trigger=False
                )
                session.add(next_run)
                session.commit()
                return next_run

    def process_dag(self, dag, executor):
        """
        This method schedules a single DAG by looking at the latest
        run for each task and attempting to schedule the following run.

        As multiple schedulers may be running for redundancy, this
        function takes a lock on the DAG and timestamps the last run
        in ``last_scheduler_run``.
        """
        TI = models.TaskInstance
        DagModel = models.DagModel
        session = settings.Session()

        # picklin'
        pickle_id = None
        if self.do_pickle and self.executor.__class__ not in (
                executors.LocalExecutor, executors.SequentialExecutor):
            pickle_id = dag.pickle(session).id

        db_dag = session.query(DagModel).filter_by(dag_id=dag.dag_id).first()
        last_scheduler_run = db_dag.last_scheduler_run or datetime(2000, 1, 1)
        secs_since_last = (
            datetime.now() - last_scheduler_run).total_seconds()
        # if db_dag.scheduler_lock or
        if secs_since_last < self.heartrate:
            session.commit()
            session.close()
            return None
        else:
            # Taking a lock
            db_dag.scheduler_lock = True
            db_dag.last_scheduler_run = datetime.now()
            session.commit()

        active_runs = dag.get_active_runs()

        self.logger.info('Getting list of tasks to skip for active runs.')
        skip_tis = set()
        if active_runs:
            qry = (
                session.query(TI.task_id, TI.execution_date)
                .filter(
                    TI.dag_id == dag.dag_id,
                    TI.execution_date.in_(active_runs),
                    TI.state.in_((State.RUNNING, State.SUCCESS, State.FAILED)),
                )
            )
            skip_tis = {(ti[0], ti[1]) for ti in qry.all()}

        descartes = [obj for obj in product(dag.tasks, active_runs)]
        self.logger.info('Checking dependencies on {} tasks instances, minus {} '
                     'skippable ones'.format(len(descartes), len(skip_tis)))
        for task, dttm in descartes:
            if task.adhoc or (task.task_id, dttm) in skip_tis:
                continue
            ti = TI(task, dttm)

            ti.refresh_from_db()
            if ti.state in (
                    State.RUNNING, State.QUEUED, State.SUCCESS, State.FAILED):
                continue
            elif ti.is_runnable(flag_upstream_failed=True):
                self.logger.debug('Firing task: {}'.format(ti))
                executor.queue_task_instance(ti, pickle_id=pickle_id)

        # Releasing the lock
        self.logger.debug("Unlocking DAG (scheduler_lock)")
        db_dag = (
            session.query(DagModel)
            .filter(DagModel.dag_id == dag.dag_id)
            .first()
        )
        db_dag.scheduler_lock = False
        session.merge(db_dag)
        session.commit()

        session.close()

    def process_events(self, executor, dagbag):
        """
        Respond to executor events.

        Used to identify queued tasks and schedule them for further processing.
        """
        for key, executor_state in list(executor.get_event_buffer().items()):
            dag_id, task_id, execution_date = key
            if dag_id not in dagbag.dags:
                self.logger.error(
                    'Executor reported a dag_id that was not found in the '
                    'DagBag: {}'.format(dag_id))
                continue
            elif not dagbag.dags[dag_id].has_task(task_id):
                self.logger.error(
                    'Executor reported a task_id that was not found in the '
                    'dag: {} in dag {}'.format(task_id, dag_id))
                continue
            task = dagbag.dags[dag_id].get_task(task_id)
            ti = models.TaskInstance(task, execution_date)
            ti.refresh_from_db()

            if executor_state == State.SUCCESS:
                # collect queued tasks for prioritiztion
                if ti.state == State.QUEUED:
                    self.queued_tis.add(ti)
                elif ti in self.queued_tis:
                    self.queued_tis.remove(ti)
            else:
                # special instructions for failed executions could go here
                pass

    @provide_session
    def prioritize_queued(self, session, executor, dagbag):
        # Prioritizing queued task instances

        pools = {p.pool: p for p in session.query(models.Pool).all()}

        self.logger.info(
            "Prioritizing {} queued jobs".format(len(self.queued_tis)))
        session.expunge_all()
        d = defaultdict(list)
        for ti in self.queued_tis:
            if ti.dag_id not in dagbag.dags:
                self.logger.info(
                    "DAG no longer in dagbag, deleting {}".format(ti))
                session.delete(ti)
                session.commit()
            elif not dagbag.dags[ti.dag_id].has_task(ti.task_id):
                self.logger.info(
                    "Task no longer exists, deleting {}".format(ti))
                session.delete(ti)
                session.commit()
            else:
                d[ti.pool].append(ti)

        dag_blacklist = set(dagbag.paused_dags())
        for pool, tis in list(d.items()):
            if not pool:
                # Arbitrary:
                # If queued outside of a pool, trigger no more than 32 per run
                open_slots = 128
            else:
                open_slots = pools[pool].open_slots(session=session)

            queue_size = len(tis)
            self.logger.info("Pool {pool} has {open_slots} slots, {queue_size} "
                             "task instances in queue".format(**locals()))
            if open_slots <= 0:
                continue
            tis = sorted(
                tis, key=lambda ti: (-ti.priority_weight, ti.start_date))
            for ti in tis:
                if open_slots <= 0:
                    continue
                task = None
                try:
                    task = dagbag.dags[ti.dag_id].get_task(ti.task_id)
                except:
                    self.logger.error("Queued task {} seems gone".format(ti))
                    session.delete(ti)
                    session.commit()
                    continue

                if not task:
                    continue

                ti.task = task

                # picklin'
                dag = dagbag.dags[ti.dag_id]
                pickle_id = None
                if self.do_pickle and self.executor.__class__ not in (
                        executors.LocalExecutor,
                        executors.SequentialExecutor):
                    self.logger.info("Pickling DAG {}".format(dag))
                    pickle_id = dag.pickle(session).id

                if dag.dag_id in dag_blacklist:
                    continue
                if dag.concurrency_reached:
                    dag_blacklist.add(dag.dag_id)
                    continue
                if ti.are_dependencies_met():
                    executor.queue_task_instance(ti, pickle_id=pickle_id)
                    open_slots -= 1
                else:
                    session.delete(ti)
                    continue
                ti.task = task

                session.commit()

    def _execute(self):
        dag_id = self.dag_id

        def signal_handler(signum, frame):
            self.logger.error("SIGINT (ctrl-c) received")
            sys.exit(1)
        signal.signal(signal.SIGINT, signal_handler)

        pessimistic_connection_handling()

        logging.basicConfig(level=logging.DEBUG)
        self.logger.info("Starting the scheduler")

        dagbag = models.DagBag(self.subdir, sync_to_db=True)
        executor = dagbag.executor
        executor.start()
        i = 0
        while not self.num_runs or self.num_runs > i:
            try:
                loop_start_dttm = datetime.now()
                try:
                    self.process_events(executor=executor, dagbag=dagbag)
                    self.prioritize_queued(executor=executor, dagbag=dagbag)
                except Exception as e:
                    self.logger.exception(e)

                i += 1
                try:
                    if i % self.refresh_dags_every == 0:
                        dagbag = models.DagBag(self.subdir, sync_to_db=True)
                    else:
                        dagbag.collect_dags(only_if_updated=True)
                except:
                    self.logger.error("Failed at reloading the dagbag")
                    Stats.incr('dag_refresh_error', 1, 1)
                    sleep(5)

                if dag_id:
                    dags = [dagbag.dags[dag_id]]
                else:
                    dags = [
                        dag for dag in dagbag.dags.values()
                        if not dag.parent_dag]
                paused_dag_ids = dagbag.paused_dags()
                for dag in dags:
                    self.logger.debug("Scheduling {}".format(dag.dag_id))
                    dag = dagbag.get_dag(dag.dag_id)
                    if not dag or (dag.dag_id in paused_dag_ids):
                        continue
                    try:
                        self.schedule_dag(dag)
                        self.process_dag(dag, executor)
                        self.manage_slas(dag)
                    except Exception as e:
                        self.logger.exception(e)
                self.logger.info("Done queuing tasks, calling the executor's "
                              "heartbeat")
                duration_sec = (datetime.now() - loop_start_dttm).total_seconds()
                self.logger.info("Loop took: {} seconds".format(duration_sec))
                try:
                    self.import_errors(dagbag)
                except Exception as e:
                    self.logger.exception(e)
                try:
                    dagbag.kill_zombies()
                except Exception as e:
                    self.logger.exception(e)
                try:
                    # We really just want the scheduler to never ever stop.
                    executor.heartbeat()
                    self.heartbeat()
                except Exception as e:
                    self.logger.exception(e)
                    self.logger.error("Tachycardia!")
            except Exception as deep_e:
                self.logger.exception(deep_e)
                raise
            finally:
                settings.Session.remove()
        executor.end()

    def heartbeat_callback(self):
        Stats.gauge('scheduler_heartbeat', 1, 1)


class BackfillJob(BaseJob):
    """
    A backfill job consists of a dag or subdag for a specific time range. It
    triggers a set of task instance runs, in the right order and lasts for
    as long as it takes for the set of task instance to be completed.
    """

    __mapper_args__ = {
        'polymorphic_identity': 'BackfillJob'
    }

    def __init__(
            self,
            dag, start_date=None, end_date=None, mark_success=False,
            include_adhoc=False,
            donot_pickle=False,
            ignore_dependencies=False,
            ignore_first_depends_on_past=False,
            pool=None,
            *args, **kwargs):
        self.dag = dag
        self.dag_id = dag.dag_id
        self.bf_start_date = start_date
        self.bf_end_date = end_date
        self.mark_success = mark_success
        self.include_adhoc = include_adhoc
        self.donot_pickle = donot_pickle
        self.ignore_dependencies = ignore_dependencies
        self.ignore_first_depends_on_past = ignore_first_depends_on_past
        self.pool = pool
        super(BackfillJob, self).__init__(*args, **kwargs)

    def _execute(self):
        """
        Runs a dag for a specified date range.
        """
        session = settings.Session()

        start_date = self.bf_start_date
        end_date = self.bf_end_date

        # picklin'
        pickle_id = None
        if not self.donot_pickle and self.executor.__class__ not in (
                executors.LocalExecutor, executors.SequentialExecutor):
            pickle = models.DagPickle(self.dag)
            session.add(pickle)
            session.commit()
            pickle_id = pickle.id

        executor = self.executor
        executor.start()
        executor_fails = Counter()

        # Build a list of all instances to run
        tasks_to_run = {}
        failed = []
        succeeded = []
        started = []
        wont_run = []
        not_ready_to_run = set()

        for task in self.dag.tasks:
            if (not self.include_adhoc) and task.adhoc:
                continue

            start_date = start_date or task.start_date
            end_date = end_date or task.end_date or datetime.now()
            for dttm in self.dag.date_range(start_date, end_date=end_date):
                ti = models.TaskInstance(task, dttm)
                tasks_to_run[ti.key] = ti
                session.merge(ti)
        session.commit()

        # Triggering what is ready to get triggered
        deadlocked = False
        while tasks_to_run and not deadlocked:

            for key, ti in list(tasks_to_run.items()):

                ti.refresh_from_db()
                ignore_depends_on_past = (
                    self.ignore_first_depends_on_past and
                    ti.execution_date == (start_date or ti.start_date))

                # Did the task finish without failing? -- then we're done
                if (
                        ti.state in (State.SUCCESS, State.SKIPPED) and
                        key in tasks_to_run):
                    succeeded.append(key)
                    tasks_to_run.pop(key)

                # Is the task runnable? -- the run it
                elif ti.is_queueable(
                        include_queued=True,
                        ignore_depends_on_past=ignore_depends_on_past,
                        flag_upstream_failed=True):
                    executor.queue_task_instance(
                        ti,
                        mark_success=self.mark_success,
                        pickle_id=pickle_id,
                        ignore_dependencies=self.ignore_dependencies,
                        ignore_depends_on_past=ignore_depends_on_past,
                        pool=self.pool)
                    ti.state = State.RUNNING
                    if key not in started:
                        started.append(key)
                    if ti in not_ready_to_run:
                        not_ready_to_run.remove(ti)

                # Mark the task as not ready to run. If the set of tasks
                # that aren't ready ever equals the set of tasks to run,
                # then the backfill is deadlocked
                elif ti.state in (State.NONE, State.UPSTREAM_FAILED):
                    not_ready_to_run.add(ti)
                    if not_ready_to_run == set(tasks_to_run.values()):
                        msg = 'BackfillJob is deadlocked: no tasks can be run.'
                        if any(
                                t.are_dependencies_met() !=
                                t.are_dependencies_met(
                                    ignore_depends_on_past=True)
                                for t in tasks_to_run.values()):
                            msg += (
                                ' Some of the tasks that were unable to '
                                'run have "depends_on_past=True". Try running '
                                'the backfill with the option '
                                '"ignore_first_depends_on_past=True" '
                                ' or passing "-I" at the command line.')
                        self.logger.error(msg)
                        deadlocked = True
                        wont_run.extend(not_ready_to_run)
                        tasks_to_run.clear()

            self.heartbeat()
            executor.heartbeat()

            # Reacting to events
            for key, state in list(executor.get_event_buffer().items()):
                dag_id, task_id, execution_date = key
                if key not in tasks_to_run:
                    continue
                ti = tasks_to_run[key]
                ti.refresh_from_db()

                # executor reports failure
                if state == State.FAILED:

                    # task reports running
                    if ti.state == State.RUNNING:
                        msg = (
                            'Executor reports that task instance {} failed '
                            'although the task says it is running.'.format(key))
                        self.logger.error(msg)
                        ti.handle_failure(msg)

                    # task reports skipped
                    elif ti.state == State.SKIPPED:
                        wont_run.append(key)
                        self.logger.error("Skipping {} ".format(key))

                    # anything else is a failure
                    else:
                        failed.append(key)
                        self.logger.error("Task instance {} failed".format(key))

                    tasks_to_run.pop(key)

                # executor reports success
                elif state == State.SUCCESS:

                    # task reports success
                    if ti.state == State.SUCCESS:
                        self.logger.info(
                            'Task instance {} succeeded'.format(key))
                        succeeded.append(key)
                        tasks_to_run.pop(key)

                    # task reports failure
                    elif ti.state == State.FAILED:
                        self.logger.error("Task instance {} failed".format(key))
                        failed.append(key)
                        tasks_to_run.pop(key)

                    # this probably won't ever be triggered
                    elif key in not_ready_to_run:
                        continue

                    # executor reports success but task does not - this is weird
                    elif ti.state not in (
                            State.SUCCESS,
                            State.QUEUED,
                            State.UP_FOR_RETRY):
                        self.logger.error(
                            "The airflow run command failed "
                            "at reporting an error. This should not occur "
                            "in normal circumstances. Task state is '{}',"
                            "reported state is '{}'. TI is {}"
                            "".format(ti.state, state, ti))

                        # if the executor fails 3 or more times, stop trying to
                        # run the task
                        executor_fails[key] += 1
                        if executor_fails[key] >= 3:
                            msg = (
                                'The airflow run command failed to report an '
                                'error for task {} three or more times. The '
                                'task is being marked as failed. This is very '
                                'unusual and probably means that an error is '
                                'taking place before the task even '
                                'starts.'.format(key))
                            self.logger.error(msg)
                            ti.handle_failure(msg)
                            tasks_to_run.pop(key)

            msg = (
                "[backfill progress] "
                "waiting: {0} | "
                "succeeded: {1} | "
                "kicked_off: {2} | "
                "failed: {3} | "
                "wont_run: {4} ").format(
                    len(tasks_to_run),
                    len(succeeded),
                    len(started),
                    len(failed),
                    len(wont_run))
            self.logger.info(msg)

        executor.end()
        session.close()
        if failed:
            msg = (
                "------------------------------------------\n"
                "Some tasks instances failed, "
                "here's the list:\n{}".format(failed))
            raise AirflowException(msg)
        self.logger.info("All done. Exiting.")


class LocalTaskJob(BaseJob):

    __mapper_args__ = {
        'polymorphic_identity': 'LocalTaskJob'
    }

    def __init__(
            self,
            task_instance,
            ignore_dependencies=False,
            ignore_depends_on_past=False,
            force=False,
            mark_success=False,
            pickle_id=None,
            pool=None,
            *args, **kwargs):
        self.task_instance = task_instance
        self.ignore_dependencies = ignore_dependencies
        self.ignore_depends_on_past = ignore_depends_on_past
        self.force = force
        self.pool = pool
        self.pickle_id = pickle_id
        self.mark_success = mark_success
        super(LocalTaskJob, self).__init__(*args, **kwargs)

    def _execute(self):
        command = self.task_instance.command(
            raw=True,
            ignore_dependencies=self.ignore_dependencies,
            ignore_depends_on_past=self.ignore_depends_on_past,
            force=self.force,
            pickle_id=self.pickle_id,
            mark_success=self.mark_success,
            job_id=self.id,
            pool=self.pool,
        )
        self.process = subprocess.Popen(['bash', '-c', command])
        return_code = None
        while return_code is None:
            self.heartbeat()
            return_code = self.process.poll()

    def on_kill(self):
        self.process.terminate()

    """
    def heartbeat_callback(self):
        if datetime.now() - self.start_date < timedelta(seconds=300):
            return
        # Suicide pill
        TI = models.TaskInstance
        ti = self.task_instance
        session = settings.Session()
        state = session.query(TI.state).filter(
            TI.dag_id==ti.dag_id, TI.task_id==ti.task_id,
            TI.execution_date==ti.execution_date).scalar()
        session.commit()
        session.close()
        if state != State.RUNNING:
            logging.warning(
                "State of this instance has been externally set to "
                "{self.task_instance.state}. "
                "Taking the poison pill. So long.".format(**locals()))
            self.process.terminate()
    """
