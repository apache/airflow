from builtins import str
from past.builtins import basestring
from collections import defaultdict
from datetime import datetime
import getpass
import logging
import signal
import socket
import subprocess
import sys
from time import sleep

from sqlalchemy import Column, Integer, String, DateTime, func, Index
from sqlalchemy.orm.session import make_transient

from airflow import executors, models, settings, utils
from airflow.configuration import conf
from airflow.utils import AirflowException, State


Base = models.Base
ID_LEN = models.ID_LEN

# Setting up a statsd client if needed
statsd = None
if conf.getboolean('scheduler', 'statsd_on'):
    from statsd import StatsClient
    statsd = StatsClient(
        host=conf.get('scheduler', 'statsd_host'),
        port=conf.getint('scheduler', 'statsd_port'),
        prefix=conf.get('scheduler', 'statsd_prefix'))


class BaseJob(Base):
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
            heartrate=conf.getint('scheduler', 'JOB_HEARTBEAT_SEC'),
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
            logging.error('on_kill() method failed')
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
        logging.debug('[heart] Boom.')

    def run(self):
        if statsd:
            statsd.incr(self.__class__.__name__.lower()+'_start', 1, 1)
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

        if statsd:
            statsd.incr(self.__class__.__name__.lower()+'_end', 1, 1)

    def _execute(self):
        raise NotImplemented("This method needs to be overridden")


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
        super(SchedulerJob, self).__init__(*args, **kwargs)

        self.heartrate = conf.getint('scheduler', 'SCHEDULER_HEARTBEAT_SEC')

    @utils.provide_session
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
                dttm += dag.schedule_interval
                while dttm < datetime.now():
                    if dttm + task.sla + dag.schedule_interval < datetime.now():
                        session.merge(models.SlaMiss(
                            task_id=ti.task_id,
                            dag_id=ti.dag_id,
                            execution_date=dttm,
                            timestamp=ts))
                    dttm += dag.schedule_interval
        session.commit()

        slas = (
            session
            .query(SlaMiss)
            .filter(SlaMiss.email_sent == False)
            .filter(SlaMiss.dag_id == dag.dag_id)
            .all()
        )

        if slas:
            sla_dates = [sla.execution_date for sla in slas]
            blocking_tis = (
                session
                .query(TI)
                .filter(TI.state != State.SUCCESS)
                .filter(TI.execution_date.in_(sla_dates))
                .filter(TI.dag_id == dag.dag_id)
                .all()
            )
            for ti in blocking_tis:
                    ti.task = dag.get_task(ti.task_id)
            blocking_tis = ([ti for ti in blocking_tis
                            if ti.are_dependencies_met(main_session=session)])
            task_list = "\n".join([
                sla.task_id + ' on ' + sla.execution_date.isoformat()
                for sla in slas])
            blocking_task_list = "\n".join([
                ti.task_id + ' on ' + ti.execution_date.isoformat()
                for ti in blocking_tis])
            from airflow import ascii
            email_content = """\
            Here's a list of tasks thas missed their SLAs:
            <pre><code>{task_list}\n<code></pre>
            Blocking tasks:
            <pre><code>{blocking_task_list}\n{ascii.bug}<code></pre>
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
                utils.send_email(
                    emails,
                    "[airflow] SLA miss on DAG=" + dag.dag_id,
                    email_content)
                for sla in slas:
                    sla.email_sent = True
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

    def process_dag(self, dag, executor):
        """
        This method schedules a single DAG by looking at the latest
        run for each task and attempting to schedule the following run.

        As multiple schedulers may be running for redundancy, this
        function takes a lock on the DAG and timestamps the last run
        in ``last_scheduler_run``.
        """
        DagModel = models.DagModel
        session = settings.Session()

        # picklin'
        pickle_id = None
        if self.do_pickle and self.executor.__class__ not in (
                executors.LocalExecutor, executors.SequentialExecutor):
            pickle_id = dag.pickle(session).id

        db_dag = session.query(
            DagModel).filter(DagModel.dag_id == dag.dag_id).first()
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

        TI = models.TaskInstance
        logging.info(
            "Getting latest instance "
            "for all tasks in dag " + dag.dag_id)
        sq = (
            session
            .query(
                TI.task_id,
                func.max(TI.execution_date).label('max_ti'))
            .filter(TI.dag_id == dag.dag_id)
            .group_by(TI.task_id).subquery('sq')
        )

        qry = session.query(TI).filter(
            TI.dag_id == dag.dag_id,
            TI.task_id == sq.c.task_id,
            TI.execution_date == sq.c.max_ti,
        )
        logging.debug("Querying max dates for each task")
        latest_ti = qry.all()
        ti_dict = {ti.task_id: ti for ti in latest_ti}
        session.expunge_all()
        session.commit()
        logging.debug("{} rows returned".format(len(latest_ti)))

        for task in dag.tasks:
            if task.adhoc:
                continue
            if task.task_id not in ti_dict:
                # Brand new task, let's get started
                ti = TI(task, task.start_date)
                ti.refresh_from_db()
                if ti.is_queueable(flag_upstream_failed=True):
                    logging.info(
                        'First run for {ti}'.format(**locals()))
                    executor.queue_task_instance(ti, pickle_id=pickle_id)
            else:
                ti = ti_dict[task.task_id]
                ti.task = task  # Hacky but worky
                if ti.state == State.RUNNING:
                    continue  # Only one task at a time
                elif ti.state == State.UP_FOR_RETRY:
                    # If task instance if up for retry, make sure
                    # the retry delay is met
                    if ti.is_runnable():
                        logging.debug('Triggering retry: ' + str(ti))
                        executor.queue_task_instance(ti, pickle_id=pickle_id)
                elif ti.state == State.QUEUED:
                    # If was queued we skipped so that in gets prioritized
                    # in self.prioritize_queued
                    continue
                else:
                    # Trying to run the next schedule
                    next_schedule = (
                        ti.execution_date + task.schedule_interval)
                    if (
                            ti.task.end_date and
                            next_schedule > ti.task.end_date):
                        continue
                    ti = TI(
                        task=task,
                        execution_date=next_schedule,
                    )
                    ti.refresh_from_db()
                    if ti.is_queueable(flag_upstream_failed=True):
                        logging.debug('Queuing next run: ' + str(ti))
                        executor.queue_task_instance(ti, pickle_id=pickle_id)
        # Releasing the lock
        logging.debug("Unlocking DAG (scheduler_lock)")
        db_dag = (
            session.query(DagModel)
            .filter(DagModel.dag_id == dag.dag_id)
            .first()
        )
        db_dag.scheduler_lock = False
        session.merge(db_dag)
        session.commit()

        session.close()

    @utils.provide_session
    def prioritize_queued(self, session, executor, dagbag):
        # Prioritizing queued task instances

        pools = {p.pool: p for p in session.query(models.Pool).all()}
        TI = models.TaskInstance
        queued_tis = (
            session.query(TI)
            .filter(TI.state == State.QUEUED)
            .all()
        )
        session.expunge_all()
        d = defaultdict(list)
        for ti in queued_tis:
            if (
                    ti.dag_id not in dagbag.dags or not
                    dagbag.dags[ti.dag_id].has_task(ti.task_id)):
                # Deleting queued jobs that don't exist anymore
                session.delete(ti)
                session.commit()
            else:
                d[ti.pool].append(ti)

        for pool, tis in list(d.items()):
            open_slots = pools[pool].open_slots(session=session)
            if open_slots > 0:
                tis = sorted(
                    tis, key=lambda ti: (-ti.priority_weight, ti.start_date))
                for ti in tis[:open_slots]:
                    task = None
                    try:
                        task = dagbag.dags[ti.dag_id].get_task(ti.task_id)
                    except:
                        logging.error("Queued task {} seems gone".format(ti))
                        session.delete(ti)
                    if task:
                        ti.task = task

                        # picklin'
                        dag = dagbag.dags[ti.dag_id]
                        pickle_id = None
                        if self.do_pickle and self.executor.__class__ not in (
                            executors.LocalExecutor, executors.SequentialExecutor):
                            pickle_id = dag.pickle(session).id

                        if ti.are_dependencies_met():
                            executor.queue_task_instance(ti, force=True, pickle_id=pickle_id)
                        else:
                            session.delete(ti)
                    session.commit()

    def _execute(self):
        dag_id = self.dag_id

        def signal_handler(signum, frame):
            logging.error("SIGINT (ctrl-c) received")
            sys.exit(1)
        signal.signal(signal.SIGINT, signal_handler)

        utils.pessimistic_connection_handling()

        logging.basicConfig(level=logging.DEBUG)
        logging.info("Starting the scheduler")

        dagbag = models.DagBag(self.subdir, sync_to_db=True)
        executor = dagbag.executor
        executor.start()
        i = 0
        while not self.num_runs or self.num_runs > i:
            try:
                loop_start_dttm = datetime.now()
                try:
                    self.prioritize_queued(executor=executor, dagbag=dagbag)
                except Exception as e:
                    logging.exception(e)

                i += 1
                try:
                    if i % self.refresh_dags_every == 0:
                        dagbag = models.DagBag(self.subdir, sync_to_db=True)
                    else:
                        dagbag.collect_dags(only_if_updated=True)
                except:
                    logging.error("Failed at reloading the dagbag")
                    if statsd:
                        statsd.incr('dag_refresh_error', 1, 1)
                    sleep(5)

                if dag_id:
                    dags = [dagbag.dags[dag_id]]
                else:
                    dags = [
                        dag for dag in dagbag.dags.values() if not dag.parent_dag]
                paused_dag_ids = dagbag.paused_dags()
                for dag in dags:
                    logging.debug("Scheduling {}".format(dag.dag_id))
                    dag = dagbag.get_dag(dag.dag_id)
                    if not dag or (dag.dag_id in paused_dag_ids):
                        continue
                    try:
                        self.process_dag(dag, executor)
                        self.manage_slas(dag)
                    except Exception as e:
                        logging.exception(e)
                logging.info(
                    "Done queuing tasks, calling the executor's heartbeat")
                duration_sec = (datetime.now() - loop_start_dttm).total_seconds()
                logging.info("Loop took: {} seconds".format(duration_sec))
                try:
                    self.import_errors(dagbag)
                except Exception as e:
                    logging.exception(e)
                try:
                    # We really just want the scheduler to never ever stop.
                    executor.heartbeat()
                    self.heartbeat()
                except Exception as e:
                    logging.exception(e)
                    logging.error("Tachycardia!")
            except Exception as deep_e:
                logging.exception(deep_e)

    def heartbeat_callback(self):
        if statsd:
            statsd.gauge('scheduler_heartbeat', 1, 1)


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
            *args, **kwargs):
        self.dag = dag
        dag.override_start_date(start_date)
        self.dag_id = dag.dag_id
        self.bf_start_date = start_date
        self.bf_end_date = end_date
        self.mark_success = mark_success
        self.include_adhoc = include_adhoc
        self.donot_pickle = donot_pickle
        self.ignore_dependencies = ignore_dependencies
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

        # Build a list of all instances to run
        tasks_to_run = {}
        failed = []
        succeeded = []
        started = []
        wont_run = []
        for task in self.dag.tasks:
            if (not self.include_adhoc) and task.adhoc:
                continue

            start_date = start_date or task.start_date
            end_date = end_date or task.end_date or datetime.now()
            for dttm in utils.date_range(
                    start_date, end_date, task.dag.schedule_interval):
                ti = models.TaskInstance(task, dttm)
                tasks_to_run[ti.key] = ti

        # Triggering what is ready to get triggered
        while tasks_to_run:
            for key, ti in list(tasks_to_run.items()):
                ti.refresh_from_db()
                if ti.state == State.SUCCESS and key in tasks_to_run:
                    succeeded.append(key)
                    del tasks_to_run[key]
                elif ti.is_runnable():
                    executor.queue_task_instance(
                        ti,
                        mark_success=self.mark_success,
                        task_start_date=self.bf_start_date,
                        pickle_id=pickle_id,
                        ignore_dependencies=self.ignore_dependencies)
                    ti.state = State.RUNNING
                    if key not in started:
                        started.append(key)
            self.heartbeat()
            executor.heartbeat()

            # Reacting to events
            for key, state in list(executor.get_event_buffer().items()):
                dag_id, task_id, execution_date = key
                if key not in tasks_to_run:
                    continue
                ti = tasks_to_run[key]
                ti.refresh_from_db()
                if ti.state in (State.FAILED, State.SKIPPED):
                    if ti.state == State.FAILED:
                        failed.append(key)
                        logging.error("Task instance " + str(key) + " failed")
                    elif ti.state == State.SKIPPED:
                        wont_run.append(key)
                        logging.error("Skipping " + str(key) + " failed")
                    del tasks_to_run[key]
                    # Removing downstream tasks that also shouldn't run
                    for t in self.dag.get_task(task_id).get_flat_relatives(
                            upstream=False):
                        key = (ti.dag_id, t.task_id, execution_date)
                        if key in tasks_to_run:
                            wont_run.append(key)
                            del tasks_to_run[key]
                elif ti.state == State.SUCCESS:
                    succeeded.append(key)
                    del tasks_to_run[key]

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
            logging.info(msg)

        executor.end()
        session.close()
        if failed:
            raise AirflowException(
                "Some tasks instances failed, here's the list:\n"+str(failed))
        logging.info("All done. Exiting.")


class LocalTaskJob(BaseJob):

    __mapper_args__ = {
        'polymorphic_identity': 'LocalTaskJob'
    }

    def __init__(
            self,
            task_instance,
            ignore_dependencies=False,
            force=False,
            mark_success=False,
            pickle_id=None,
            task_start_date=None,
            *args, **kwargs):
        self.task_instance = task_instance
        self.ignore_dependencies = ignore_dependencies
        self.force = force
        self.pickle_id = pickle_id
        self.mark_success = mark_success
        self.task_start_date = task_start_date
        super(LocalTaskJob, self).__init__(*args, **kwargs)

    def _execute(self):
        command = self.task_instance.command(
            raw=True,
            ignore_dependencies=self.ignore_dependencies,
            force=self.force,
            pickle_id=self.pickle_id,
            mark_success=self.mark_success,
            task_start_date=self.task_start_date,
            job_id=self.id,
        )
        self.process = subprocess.Popen(['bash', '-c', command])
        return_code = None
        while return_code is None:
            self.heartbeat()
            return_code = self.process.poll()

    def on_kill(self):
        self.process.terminate()
