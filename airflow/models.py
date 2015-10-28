from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from future.standard_library import install_aliases
install_aliases()
from builtins import str
from past.builtins import basestring
from builtins import object, bytes
import copy
from datetime import datetime, timedelta
import functools
import getpass
import imp
import jinja2
import json
import logging
import os
import dill
import re
import signal
import socket
import sys
from urllib.parse import urlparse

from sqlalchemy import (
    Column, Integer, String, DateTime, Text, Boolean, ForeignKey, PickleType,
    Index, BigInteger)
from sqlalchemy import case, func, or_, and_
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.orm import relationship, synonym

from airflow import settings, utils
from airflow.executors import DEFAULT_EXECUTOR, LocalExecutor
from airflow.configuration import conf
from airflow.utils import (
    AirflowException, State, apply_defaults, provide_session,
    is_container, as_tuple, TriggerRule)

Base = declarative_base()
ID_LEN = 250
SQL_ALCHEMY_CONN = conf.get('core', 'SQL_ALCHEMY_CONN')
DAGS_FOLDER = os.path.expanduser(conf.get('core', 'DAGS_FOLDER'))
XCOM_RETURN_KEY = 'return_value'

ENCRYPTION_ON = False
try:
    from cryptography.fernet import Fernet
    FERNET = Fernet(conf.get('core', 'FERNET_KEY').encode('utf-8'))
    ENCRYPTION_ON = True
except:
    pass

if 'mysql' in SQL_ALCHEMY_CONN:
    LongText = LONGTEXT
else:
    LongText = Text


def clear_task_instances(tis, session):
    '''
    Clears a set of task instances, but makes sure the running ones
    get killed.
    '''
    job_ids = []
    for ti in tis:
        if ti.state == State.RUNNING:
            if ti.job_id:
                ti.state = State.SHUTDOWN
                job_ids.append(ti.job_id)
        else:
            session.delete(ti)
    if job_ids:
        from airflow.jobs import BaseJob as BJ
        for job in session.query(BJ).filter(BJ.id.in_(job_ids)).all():
            job.state = State.SHUTDOWN


class DagBag(object):
    """
    A dagbag is a collection of dags, parsed out of a folder tree and has high
    level configuration settings, like what database to use as a backend and
    what executor to use to fire off tasks. This makes it easier to run
    distinct environments for say production and development, tests, or for
    different teams or security profiles. What would have been system level
    settings are now dagbag level so that one system can run multiple,
    independent settings sets.

    :param dag_folder: the folder to scan to find DAGs
    :type dag_folder: str
    :param executor: the executor to use when executing task instances
        in this DagBag
    :param include_examples: whether to include the examples that ship
        with airflow or not
    :type include_examples: bool
    :param sync_to_db: whether to sync the properties of the DAGs to
        the metadata DB while finding them, typically should be done
        by the scheduler job only
    :type sync_to_db: bool
    """
    def __init__(
            self,
            dag_folder=None,
            executor=DEFAULT_EXECUTOR,
            include_examples=conf.getboolean('core', 'LOAD_EXAMPLES'),
            sync_to_db=False):

        dag_folder = dag_folder or DAGS_FOLDER
        logging.info("Filling up the DagBag from " + dag_folder)
        self.dag_folder = dag_folder
        self.dags = {}
        self.sync_to_db = sync_to_db
        self.file_last_changed = {}
        self.executor = executor
        self.import_errors = {}
        if include_examples:
            example_dag_folder = os.path.join(
                os.path.dirname(__file__),
                'example_dags')
            self.collect_dags(example_dag_folder)
        self.collect_dags(dag_folder)
        if sync_to_db:
            self.deactivate_inactive_dags()

    def get_dag(self, dag_id):
        """
        Gets the DAG out of the dictionary, and refreshes it if expired
        """
        if dag_id in self.dags:
            dag = self.dags[dag_id]
            if dag.is_subdag:
                orm_dag = DagModel.get_current(dag.parent_dag.dag_id)
            else:
                orm_dag = DagModel.get_current(dag_id)
            if orm_dag and dag.last_loaded < (
                    orm_dag.last_expired or datetime(2100, 1, 1)):
                self.process_file(
                    filepath=orm_dag.fileloc, only_if_updated=False)
                dag = self.dags[dag_id]
        else:
            orm_dag = DagModel.get_current(dag_id)
            self.process_file(
                filepath=orm_dag.fileloc, only_if_updated=False)
            if dag_id in self.dags:
                dag = self.dags[dag_id]
            else:
                dag = None
        return dag

    def process_file(self, filepath, only_if_updated=True, safe_mode=True):
        """
        Given a path to a python module, this method imports the module and
        look for dag objects within it.
        """
        try:
            # This failed before in what may have been a git sync
            # race condition
            dttm = datetime.fromtimestamp(os.path.getmtime(filepath))
            mod_name, file_ext = os.path.splitext(os.path.split(filepath)[-1])
            mod_name = 'unusual_prefix_' + mod_name
        except:
            return

        if safe_mode and os.path.isfile(filepath):
            # Skip file if no obvious references to airflow or DAG are found.
            with open(filepath, 'r') as f:
                content = f.read()
                if not all([s in content for s in ('DAG', 'airflow')]):
                    return

        if (
                not only_if_updated or
                filepath not in self.file_last_changed or
                dttm != self.file_last_changed[filepath]):
            try:
                logging.info("Importing " + filepath)
                if mod_name in sys.modules:
                    del sys.modules[mod_name]
                with utils.timeout(30):
                    m = imp.load_source(mod_name, filepath)
            except Exception as e:
                logging.error("Failed to import: " + filepath)
                self.import_errors[filepath] = str(e)
                logging.exception(e)
                self.file_last_changed[filepath] = dttm
                return

            for dag in list(m.__dict__.values()):
                if isinstance(dag, DAG):
                    dag.full_filepath = filepath
                    dag.is_subdag = False
                    self.bag_dag(dag, parent_dag=dag, root_dag=dag)
                    # dag.pickle()

            self.file_last_changed[filepath] = dttm

    @provide_session
    def kill_zombies(self, session):
        """
        Fails tasks that haven't had a heartbeat in too long
        """
        from airflow.jobs import LocalTaskJob as LJ
        logging.info("Finding 'running' jobs without a recent heartbeat")
        secs = (conf.getint('scheduler', 'job_heartbeat_sec') * 3) + 120
        limit_dttm = datetime.now() - timedelta(seconds=secs)
        print("Failing jobs without heartbeat after {}".format(limit_dttm))
        jobs = (
            session
            .query(LJ)
            .filter(
                LJ.state == State.RUNNING,
                LJ.latest_heartbeat < limit_dttm)
            .all()
        )
        for job in jobs:
            ti = session.query(TaskInstance).filter_by(
                job_id=job.id, state=State.RUNNING).first()
            logging.info("Failing job_id '{}'".format(job.id))
            if ti and ti.dag_id in self.dags:
                dag = self.dags[ti.dag_id]
                if ti.task_id in dag.task_ids:
                    task = dag.get_task(ti.task_id)
                    ti.task = task
                    ti.handle_failure("{} killed as zombie".format(ti))
                    logging.info('Marked zombie job {} as failed'.format(ti))
            else:
                job.state = State.FAILED
        session.commit()

    def bag_dag(self, dag, parent_dag, root_dag):
        """
        Adds the DAG into the bag, recurses into sub dags.
        """
        self.dags[dag.dag_id] = dag
        dag.resolve_template_files()
        dag.last_loaded = datetime.now()

        for task in dag.tasks:
            settings.policy(task)

        if self.sync_to_db:
            session = settings.Session()
            orm_dag = session.query(
                DagModel).filter(DagModel.dag_id == dag.dag_id).first()
            if not orm_dag:
                orm_dag = DagModel(dag_id=dag.dag_id)
            orm_dag.fileloc = root_dag.full_filepath
            orm_dag.is_subdag = dag.is_subdag
            orm_dag.owners = root_dag.owner
            orm_dag.is_active = True
            session.merge(orm_dag)
            session.commit()
            session.close()

        for subdag in dag.subdags:
            subdag.full_filepath = dag.full_filepath
            subdag.parent_dag = dag
            subdag.fileloc = root_dag.full_filepath
            subdag.is_subdag = True
            self.bag_dag(subdag, parent_dag=dag, root_dag=root_dag)
        logging.info('Loaded DAG {dag}'.format(**locals()))

    def collect_dags(
            self,
            dag_folder=None,
            only_if_updated=True):
        """
        Given a file path or a folder, this method looks for python modules,
        imports them and adds them to the dagbag collection.

        Note that if a .airflowignore file is found while processing,
        the directory, it will behaves much like a .gitignore does,
        ignoring files that match any of the regex patterns specified
        in the file.
        """
        dag_folder = dag_folder or self.dag_folder
        if os.path.isfile(dag_folder):
            self.process_file(dag_folder, only_if_updated=only_if_updated)
        elif os.path.isdir(dag_folder):
            patterns = []
            for root, dirs, files in os.walk(dag_folder, followlinks=True):
                ignore_file = [f for f in files if f == '.airflowignore']
                if ignore_file:
                    f = open(os.path.join(root, ignore_file[0]), 'r')
                    patterns += [p for p in f.read().split('\n') if p]
                    f.close()
                for f in files:
                    try:
                        filepath = os.path.join(root, f)
                        if not os.path.isfile(filepath):
                            continue
                        mod_name, file_ext = os.path.splitext(
                            os.path.split(filepath)[-1])
                        if file_ext != '.py':
                            continue
                        if not any(
                                [re.findall(p, filepath) for p in patterns]):
                            self.process_file(
                                filepath, only_if_updated=only_if_updated)
                    except:
                        pass

    def deactivate_inactive_dags(self):
        active_dag_ids = [dag.dag_id for dag in list(self.dags.values())]
        session = settings.Session()
        for dag in session.query(
                DagModel).filter(~DagModel.dag_id.in_(active_dag_ids)).all():
            dag.is_active = False
            session.merge(dag)
        session.commit()
        session.close()

    def paused_dags(self):
        session = settings.Session()
        dag_ids = [dp.dag_id for dp in session.query(DagModel).filter(
            DagModel.is_paused == True)]
        session.commit()
        session.close()
        return dag_ids


class BaseUser(Base):
    __tablename__ = "user"

    id = Column(Integer, primary_key=True)
    username = Column(String(ID_LEN), unique=True)
    email = Column(String(500))

    def __repr__(self):
        return self.username

    def get_id(self):
        return str(self.id)


class Connection(Base):
    """
    Placeholder to store information about different database instances
    connection information. The idea here is that scripts use references to
    database instances (conn_id) instead of hard coding hostname, logins and
    passwords when using operators or hooks.
    """
    __tablename__ = "connection"

    id = Column(Integer(), primary_key=True)
    conn_id = Column(String(ID_LEN))
    conn_type = Column(String(500))
    host = Column(String(500))
    schema = Column(String(500))
    login = Column(String(500))
    _password = Column('password', String(500))
    port = Column(Integer())
    is_encrypted = Column(Boolean, unique=False, default=False)
    extra = Column(String(5000))

    def __init__(
            self, conn_id=None, conn_type=None,
            host=None, login=None, password=None,
            schema=None, port=None, extra=None,
            uri=None):
        self.conn_id = conn_id
        self.conn_type = conn_type
        if uri:
            self.parse_from_uri(uri)
        else:
            self.host = host
            self.login = login
            self.password = password
            self.schema = schema
            self.port = port
            self.extra = extra

    def parse_from_uri(self, uri):
        temp_uri = urlparse(uri)
        hostname = temp_uri.hostname or ''
        if '%2f' in hostname:
            hostname = hostname.replace('%2f', '/').replace('%2F', '/')
        self.host = hostname
        self.schema = temp_uri.path[1:]
        self.login = temp_uri.username
        self.password = temp_uri.password
        self.port = temp_uri.port

    def get_password(self):
        if self._password and self.is_encrypted:
            if not ENCRYPTION_ON:
                raise AirflowException(
                    "Can't decrypt, configuration is missing")
            return FERNET.decrypt(bytes(self._password, 'utf-8')).decode()
        else:
            return self._password

    def set_password(self, value):
        if value:
            try:
                self._password = FERNET.encrypt(bytes(value, 'utf-8')).decode()
                self.is_encrypted = True
            except NameError:
                self._password = value
                self.is_encrypted = False

    @declared_attr
    def password(cls):
        return synonym('_password',
                       descriptor=property(cls.get_password, cls.set_password))

    def get_hook(self):
        from airflow import hooks
        try:
            if self.conn_type == 'mysql':
                return hooks.MySqlHook(mysql_conn_id=self.conn_id)
            elif self.conn_type == 'postgres':
                return hooks.PostgresHook(postgres_conn_id=self.conn_id)
            elif self.conn_type == 'hive_cli':
                return hooks.HiveCliHook(hive_cli_conn_id=self.conn_id)
            elif self.conn_type == 'presto':
                return hooks.PrestoHook(presto_conn_id=self.conn_id)
            elif self.conn_type == 'hiveserver2':
                return hooks.HiveServer2Hook(hiveserver2_conn_id=self.conn_id)
            elif self.conn_type == 'sqlite':
                return hooks.SqliteHook(sqlite_conn_id=self.conn_id)
            elif self.conn_type == 'jdbc':
                return hooks.JdbcHook(jdbc_conn_id=self.conn_id)
            elif self.conn_type == 'mssql':
                return hooks.MsSqlHook(mssql_conn_id=self.conn_id)
            elif self.conn_type == 'oracle':
                return hooks.OracleHook(oracle_conn_id=self.conn_id)
            elif self.conn_type == 'vertica':
                return hooks.VerticaHook(vertica_conn_id=self.conn_id)
        except:
            return None

    def __repr__(self):
        return self.conn_id

    @property
    def extra_dejson(self):
        """Returns the extra property by deserializing json"""
        obj = {}
        if self.extra:
            try:
                obj = json.loads(self.extra)
            except Exception as e:
                logging.exception(e)
                logging.error(
                    "Failed parsing the json for "
                    "conn_id {}".format(self.conn_id))
        return obj


class DagPickle(Base):
    """
    Dags can originate from different places (user repos, master repo, ...)
    and also get executed in different places (different executors). This
    object represents a version of a DAG and becomes a source of truth for
    a BackfillJob execution. A pickle is a native python serialized object,
    and in this case gets stored in the database for the duration of the job.

    The executors pick up the DagPickle id and read the dag definition from
    the database.
    """
    id = Column(Integer, primary_key=True)
    pickle = Column(PickleType(pickler=dill))
    created_dttm = Column(DateTime, default=func.now())
    pickle_hash = Column(Text)

    __tablename__ = "dag_pickle"

    def __init__(self, dag):
        self.dag_id = dag.dag_id
        if hasattr(dag, 'template_env'):
            dag.template_env = None
        self.pickle_hash = hash(dag)
        self.pickle = dag


class TaskInstance(Base):
    """
    Task instances store the state of a task instance. This table is the
    authority and single source of truth around what tasks have run and the
    state they are in.

    The SqlAchemy model doesn't have a SqlAlchemy foreign key to the task or
    dag model deliberately to have more control over transactions.

    Database transactions on this table should insure double triggers and
    any confusion around what task instances are or aren't ready to run
    even while multiple schedulers may be firing task instances.
    """

    __tablename__ = "task_instance"

    task_id = Column(String(ID_LEN), primary_key=True)
    dag_id = Column(String(ID_LEN), primary_key=True)
    execution_date = Column(DateTime, primary_key=True)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    duration = Column(Integer)
    state = Column(String(20))
    try_number = Column(Integer)
    hostname = Column(String(1000))
    unixname = Column(String(1000))
    job_id = Column(Integer)
    pool = Column(String(50))
    queue = Column(String(50))
    priority_weight = Column(Integer)
    operator = Column(String(1000))
    queued_dttm = Column(DateTime)

    __table_args__ = (
        Index('ti_dag_state', dag_id, state),
        Index('ti_state_lkp', dag_id, task_id, execution_date, state),
        Index('ti_pool', pool, state, priority_weight),
    )

    def __init__(self, task, execution_date, state=None):
        self.dag_id = task.dag_id
        self.task_id = task.task_id
        self.execution_date = execution_date
        self.task = task
        self.queue = task.queue
        self.pool = task.pool
        self.priority_weight = task.priority_weight_total
        self.try_number = 1
        self.unixname = getpass.getuser()
        if state:
            self.state = state

    def command(
            self,
            mark_success=False,
            ignore_dependencies=False,
            force=False,
            local=False,
            pickle_id=None,
            raw=False,
            task_start_date=None,
            job_id=None):
        """
        Returns a command that can be executed anywhere where airflow is
        installed. This command is part of the message sent to executors by
        the orchestrator.
        """
        iso = self.execution_date.isoformat()
        mark_success = "--mark_success" if mark_success else ""
        pickle = "--pickle {0}".format(pickle_id) if pickle_id else ""
        job_id = "--job_id {0}".format(job_id) if job_id else ""
        ignore_dependencies = "-i" if ignore_dependencies else ""
        force = "--force" if force else ""
        local = "--local" if local else ""
        task_start_date = \
            "-s " + task_start_date.isoformat() if task_start_date else ""
        raw = "--raw" if raw else ""
        subdir = ""
        if not pickle and self.task.dag and self.task.dag.full_filepath:
            subdir = "-sd DAGS_FOLDER/{0}".format(self.task.dag.filepath)
        return (
            "airflow run "
            "{self.dag_id} {self.task_id} {iso} "
            "{mark_success} "
            "{pickle} "
            "{local} "
            "{ignore_dependencies} "
            "{force} "
            "{job_id} "
            "{raw} "
            "{subdir} "
            "{task_start_date} "
        ).format(**locals())

    @property
    def log_filepath(self):
        iso = self.execution_date.isoformat()
        log = os.path.expanduser(conf.get('core', 'BASE_LOG_FOLDER'))
        return (
            "{log}/{self.dag_id}/{self.task_id}/{iso}.log".format(**locals()))

    @property
    def log_url(self):
        iso = self.execution_date.isoformat()
        BASE_URL = conf.get('webserver', 'BASE_URL')
        return BASE_URL + (
            "/admin/airflow/log"
            "?dag_id={self.dag_id}"
            "&task_id={self.task_id}"
            "&execution_date={iso}"
        ).format(**locals())

    @property
    def mark_success_url(self):
        iso = self.execution_date.isoformat()
        BASE_URL = conf.get('webserver', 'BASE_URL')
        return BASE_URL + (
            "/admin/airflow/action"
            "?action=success"
            "&task_id={self.task_id}"
            "&dag_id={self.dag_id}"
            "&execution_date={iso}"
            "&upstream=false"
            "&downstream=false"
        ).format(**locals())

    def current_state(self, main_session=None):
        """
        Get the very latest state from the database, if a session is passed,
        we use and looking up the state becomes part of the session, otherwise
        a new session is used.
        """
        session = main_session or settings.Session()
        TI = TaskInstance
        ti = session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.task_id == self.task_id,
            TI.execution_date == self.execution_date,
        ).all()
        if ti:
            state = ti[0].state
        else:
            state = None
        if not main_session:
            session.commit()
            session.close()
        return state

    def error(self, main_session=None):
        """
        Forces the task instance's state to FAILED in the database.
        """
        session = settings.Session()
        logging.error("Recording the task instance as FAILED")
        self.state = State.FAILED
        session.merge(self)
        session.commit()
        session.close()

    def refresh_from_db(self, main_session=None):
        """
        Refreshes the task instance from the database based on the primary key
        """
        session = main_session or settings.Session()
        TI = TaskInstance
        ti = session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.task_id == self.task_id,
            TI.execution_date == self.execution_date,
        ).first()
        if ti:
            self.state = ti.state
            self.start_date = ti.start_date
            self.end_date = ti.end_date
            self.try_number = ti.try_number

        if not main_session:
            session.commit()
            session.close()

    @property
    def key(self):
        """
        Returns a tuple that identifies the task instance uniquely
        """
        return (self.dag_id, self.task_id, self.execution_date)

    def is_queueable(self, flag_upstream_failed=False):
        """
        Returns a boolean on whether the task instance has met all dependencies
        and is ready to run. It considers the task's state, the state
        of its dependencies, depends_on_past and makes sure the execution
        isn't in the future. It doesn't take into
        account whether the pool has a slot for it to run.

        :param flag_upstream_failed: This is a hack to generate
            the upstream_failed state creation while checking to see
            whether the task instance is runnable. It was the shortest
            path to add the feature
        :type flag_upstream_failed: boolean
        """
        if self.execution_date > datetime.now():
            return False
        elif self.state == State.UP_FOR_RETRY and not self.ready_for_retry():
            return False
        elif self.task.end_date and self.execution_date > self.task.end_date:
            return False
        elif self.state == State.SKIPPED:
            return False
        elif (
                self.state in State.runnable() and
                self.are_dependencies_met(
                    flag_upstream_failed=flag_upstream_failed)):
            return True
        else:
            return False

    def is_runnable(self, flag_upstream_failed=False):
        """
        Returns whether a task is ready to run AND there's room in the
        queue.
        """
        return self.is_queueable(flag_upstream_failed) and not self.pool_full()

    def are_dependents_done(self, main_session=None):
        """
        Checks whether the dependents of this task instance have all succeeded.
        This is meant to be used by wait_for_downstream.

        This is useful when you do not want to start processing the next
        schedule of a task until the dependents are done. For instance,
        if the task DROPs and recreates a table.
        """
        session = main_session or settings.Session()
        task = self.task

        if not task._downstream_list:
            return True

        downstream_task_ids = [t.task_id for t in task._downstream_list]
        ti = session.query(func.count(TaskInstance.task_id)).filter(
            TaskInstance.dag_id == self.dag_id,
            TaskInstance.task_id.in_(downstream_task_ids),
            TaskInstance.execution_date == self.execution_date,
            TaskInstance.state == State.SUCCESS,
        )
        count = ti[0][0]
        if not main_session:
            session.commit()
            session.close()
        return count == len(task._downstream_list)

    def are_dependencies_met(
            self, main_session=None, flag_upstream_failed=False):
        """
        Returns a boolean on whether the upstream tasks are in a SUCCESS state
        and considers depends_on_past and the previous run's state.

        :param flag_upstream_failed: This is a hack to generate
            the upstream_failed state creation while checking to see
            whether the task instance is runnable. It was the shortest
            path to add the feature
        :type flag_upstream_failed: boolean
        """
        TI = TaskInstance
        TR = TriggerRule

        # Using the session if passed as param
        session = main_session or settings.Session()
        task = self.task

        # Checking that the depends_on_past is fulfilled
        if (task.depends_on_past and
                not self.execution_date == task.start_date):
            previous_ti = session.query(TI).filter(
                TI.dag_id == self.dag_id,
                TI.task_id == task.task_id,
                TI.execution_date ==
                self.execution_date-task.schedule_interval,
                TI.state == State.SUCCESS,
            ).first()
            if not previous_ti:
                return False

            # Applying wait_for_downstream
            previous_ti.task = self.task
            if task.wait_for_downstream and not \
                    previous_ti.are_dependents_done(session):
                return False

        # Checking that all upstream dependencies have succeeded
        if not task._upstream_list or task.trigger_rule == TR.DUMMY:
            return True
        else:
            upstream_task_ids = [t.task_id for t in task._upstream_list]
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
                    TI.dag_id == self.dag_id,
                    TI.task_id.in_(upstream_task_ids),
                    TI.execution_date == self.execution_date,
                    TI.state.in_([
                        State.SUCCESS, State.FAILED,
                        State.UPSTREAM_FAILED, State.SKIPPED]),
                )
            )
            successes, skipped, failed, upstream_failed, done = qry.first()
            if flag_upstream_failed:
                if skipped >= len(task._upstream_list):
                    self.state = State.SKIPPED
                    self.start_date = datetime.now()
                    self.end_date = datetime.now()
                    session.merge(self)
                elif failed + upstream_failed >= len(task._upstream_list):
                    self.state = State.UPSTREAM_FAILED
                    self.start_date = datetime.now()
                    self.end_date = datetime.now()
                    session.merge(self)

            if task.trigger_rule == TR.ONE_SUCCESS and successes > 0:
                return True
            elif (task.trigger_rule == TR.ONE_FAILED and
                  (failed + upstream_failed) > 0):
                return True
            elif (task.trigger_rule == TR.ALL_SUCCESS and
                  successes == len(task._upstream_list)):
                return True
            elif (task.trigger_rule == TR.ALL_FAILED and
                  failed + upstream_failed == len(task._upstream_list)):
                return True
            elif (task.trigger_rule == TR.ALL_DONE and
                  done == len(task._upstream_list)):
                return True

        if not main_session:
            session.commit()
            session.close()
        return False

    def __repr__(self):
        return (
            "<TaskInstance: {ti.dag_id}.{ti.task_id} "
            "{ti.execution_date} [{ti.state}]>"
        ).format(ti=self)

    def ready_for_retry(self):
        """
        Checks on whether the task instance is in the right state and timeframe
        to be retried.
        """
        return self.state == State.UP_FOR_RETRY and \
            self.end_date + self.task.retry_delay < datetime.now()

    @provide_session
    def pool_full(self, session):
        """
        Returns a boolean as to whether the slot pool has room for this
        task to run
        """
        if not self.task.pool:
            return False

        pool = (
            session
            .query(Pool)
            .filter(Pool.pool == self.task.pool)
            .first()
        )
        if not pool:
            raise ValueError('Task specified a pool ({}) but the pool '
                             'doesn\'t exist!').format(self.task.pool)
        open_slots = pool.open_slots(session=session)

        return open_slots <= 0

    def run(
            self,
            verbose=True,
            ignore_dependencies=False,  # Doesn't check for deps, just runs
            force=False,  # Disregards previous successes
            mark_success=False,  # Don't run the task, act as if it succeeded
            test_mode=False,  # Doesn't record success or failure in the DB
            job_id=None,):
        """
        Runs the task instance.
        """
        task = self.task
        session = settings.Session()
        self.refresh_from_db(session)
        session.commit()
        self.job_id = job_id
        iso = datetime.now().isoformat()
        self.hostname = socket.gethostname()
        self.operator = task.__class__.__name__

        if self.state == State.RUNNING:
            logging.warning("Another instance is running, skipping.")
        elif not force and self.state == State.SUCCESS:
            logging.info(
                "Task {self} previously succeeded"
                " on {self.end_date}".format(**locals())
            )
        elif not ignore_dependencies and \
                not self.are_dependencies_met(session):
            logging.warning("Dependencies not met yet")
        elif self.state == State.UP_FOR_RETRY and \
                not self.ready_for_retry():
            next_run = (self.end_date + task.retry_delay).isoformat()
            logging.info(
                "Not ready for retry yet. " +
                "Next run after {0}".format(next_run)
            )
        elif force or self.state in State.runnable():
            msg = "\n" + ("-" * 80)
            if self.state == State.UP_FOR_RETRY:
                msg += "\nRetry run {self.try_number} out of {task.retries} "
                msg += "starting @{iso}\n"
            else:
                msg += "\nNew run starting @{iso}\n"
            msg += ("-" * 80)
            logging.info(msg.format(**locals()))

            self.start_date = datetime.now()
            if not force and task.pool:
                # If a pool is set for this task, marking the task instance
                # as QUEUED
                self.state = State.QUEUED
                self.queued_dttm = datetime.now()
                session.merge(self)
                session.commit()
                session.close()
                logging.info("Queuing into pool {}".format(task.pool))
                return
            if self.state == State.UP_FOR_RETRY:
                self.try_number += 1
            else:
                self.try_number = 1
            if not test_mode:
                session.add(Log(State.RUNNING, self))
            self.state = State.RUNNING
            self.end_date = None
            if not test_mode:
                session.merge(self)
            session.commit()
            if verbose:
                if mark_success:
                    msg = "Marking success for "
                else:
                    msg = "Executing "
                msg += "{self.task} on {self.execution_date}"

            context = {}
            try:
                logging.info(msg.format(self=self))
                if not mark_success:
                    context = self.get_template_context()

                    task_copy = copy.copy(task)
                    self.task = task_copy

                    def signal_handler(signum, frame):
                        '''Setting kill signal handler'''
                        logging.error("Killing subprocess")
                        task_copy.on_kill()
                        raise AirflowException("Task received SIGTERM signal")
                    signal.signal(signal.SIGTERM, signal_handler)

                    self.render_templates()
                    task_copy.pre_execute(context=context)

                    # If a timout is specified for the task, make it fail
                    # if it goes beyond
                    result = None
                    if task_copy.execution_timeout:
                        with utils.timeout(int(
                                task_copy.execution_timeout.total_seconds())):
                            result = task_copy.execute(context=context)

                    else:
                        result = task_copy.execute(context=context)

                    # If the task returns a result, push an XCom containing it
                    if result is not None:
                        self.xcom_push(key=XCOM_RETURN_KEY, value=result)

                    task_copy.post_execute(context=context)
            except (Exception, KeyboardInterrupt) as e:
                self.handle_failure(e, test_mode, context)
                raise

            # Recording SUCCESS
            session = settings.Session()
            self.end_date = datetime.now()
            self.set_duration()
            self.state = State.SUCCESS
            if not test_mode:
                session.add(Log(State.SUCCESS, self))
                session.merge(self)

            # Success callback
            try:
                if task.on_success_callback:
                    task.on_success_callback(context)
            except Exception as e3:
                logging.error("Failed when executing success callback")
                logging.exception(e3)

        session.commit()

    def dry_run(self):
        task = self.task
        task_copy = copy.copy(task)
        self.task = task_copy

        self.render_templates()
        task_copy.dry_run()


    def handle_failure(self, error, test_mode=False, context=None):
        logging.exception(error)
        task = self.task
        session = settings.Session()
        self.end_date = datetime.now()
        self.set_duration()
        if not test_mode:
            session.add(Log(State.FAILED, self))

        # Let's go deeper
        try:
            if self.try_number <= task.retries:
                self.state = State.UP_FOR_RETRY
                if task.email_on_retry and task.email:
                    self.email_alert(error, is_retry=True)
            else:
                self.state = State.FAILED
                if task.email_on_failure and task.email:
                    self.email_alert(error, is_retry=False)
        except Exception as e2:
            logging.error(
                'Failed to send email to: ' + str(task.email))
            logging.exception(e2)

        # Handling callbacks pessimistically
        try:
            if self.state == State.UP_FOR_RETRY and task.on_retry_callback:
                task.on_retry_callback(context)
            if self.state == State.FAILED and task.on_failure_callback:
                task.on_failure_callback(context)
        except Exception as e3:
            logging.error("Failed at executing callback")
            logging.exception(e3)

        if not test_mode:
            session.merge(self)
        session.commit()
        logging.error(str(error))

    def get_template_context(self):
        task = self.task
        from airflow import macros
        tables = None
        if 'tables' in task.params:
            tables = task.params['tables']
        ds = self.execution_date.isoformat()[:10]
        yesterday_ds = (self.execution_date - timedelta(1)).isoformat()[:10]
        tomorrow_ds = (self.execution_date + timedelta(1)).isoformat()[:10]
        ds_nodash = ds.replace('-', '')
        ti_key_str = "{task.dag_id}__{task.task_id}__{ds_nodash}"
        ti_key_str = ti_key_str.format(**locals())

        params = {}
        if hasattr(task, 'dag') and task.dag.params:
            params.update(task.dag.params)
        if task.params:
            params.update(task.params)

        return {
            'dag': task.dag,
            'ds': ds,
            'yesterday_ds': yesterday_ds,
            'tomorrow_ds': tomorrow_ds,
            'END_DATE': ds,
            'ds_nodash': ds_nodash,
            'end_date': ds,
            'execution_date': self.execution_date,
            'latest_date': ds,
            'macros': macros,
            'params': params,
            'tables': tables,
            'task': task,
            'task_instance': self,
            'ti': self,
            'task_instance_key_str': ti_key_str,
            'conf': conf,
        }

    def render_templates(self):
        task = self.task
        jinja_context = self.get_template_context()
        if hasattr(self, 'task') and hasattr(self.task, 'dag'):
            if self.task.dag.user_defined_macros:
                jinja_context.update(
                    self.task.dag.user_defined_macros)

        rt = self.task.render_template  # shortcut to method
        for attr in task.__class__.template_fields:
            content = getattr(task, attr)
            if content:
                rendered_content = self.task.render_template(content, jinja_context)
                setattr(task, attr, rendered_content)

    def email_alert(self, exception, is_retry=False):
        task = self.task
        title = "Airflow alert: {self}".format(**locals())
        exception = str(exception).replace('\n', '<br>')
        try_ = task.retries + 1
        body = (
            "Try {self.try_number} out of {try_}<br>"
            "Exception:<br>{exception}<br>"
            "Log: <a href='{self.log_url}'>Link</a><br>"
            "Host: {self.hostname}<br>"
            "Log file: {self.log_filepath}<br>"
            "Mark success: <a href='{self.mark_success_url}'>Link</a><br>"
        ).format(**locals())
        utils.send_email(task.email, title, body)

    def set_duration(self):
        if self.end_date and self.start_date:
            self.duration = (self.end_date - self.start_date).seconds
        else:
            self.duration = None

    def xcom_push(
            self,
            key,
            value,
            execution_date=None):
        """
        Make an XCom available for tasks to pull.

        :param key: A key for the XCom
        :type key: string
        :param value: A value for the XCom. The value is pickled and stored
            in the database.
        :type value: any pickleable object
        :param execution_date: if provided, the XCom will not be visible until
            this date. This can be used, for example, to send a message to a
            task on a future date without it being immediately visible.
        :type execution_date: datetime
        """

        if execution_date and execution_date < self.execution_date:
            raise ValueError(
                'execution_date can not be in the past (current '
                'execution_date is {}; received {})'.format(
                    self.execution_date, execution_date))

        XCom.set(
            key=key,
            value=value,
            task_id=self.task_id,
            dag_id=self.dag_id,
            execution_date=execution_date or self.execution_date)

    def xcom_pull(
            self,
            task_ids,
            dag_id=None,
            key=XCOM_RETURN_KEY,
            include_prior_dates=False,
            limit=None):
        """
        Pull XComs that optionally meet certain criteria.

        The default value for `key` limits the search to XComs
        that were returned by other tasks (as opposed to those that were pushed
        manually). To remove this filter, pass key=None (or any desired value).

        If a single task_id string is provided, the result is the value of the
        most recent matching XCom from that task_id. If multiple task_ids are
        provided, a tuple of matching values is returned. None is returned
        whenever no matches are found.

        :param key: A key for the XCom. If provided, only XComs with matching
            keys will be returned. The default key is 'return_value', also
            available as a constant XCOM_RETURN_KEY. This key is automatically
            given to XComs returned by tasks (as opposed to being pushed
            manually). To remove the filter, pass key=None.
        :type key: string
        :param task_ids: Only XComs from tasks with matching ids will be
            pulled. Can pass None to remove the filter.
        :type task_ids: string or iterable of strings (representing task_ids)
        :param dag_id: If provided, only pulls XComs from this DAG.
            If None (default), the DAG of the calling task is used.
        :type dag_id: string
        :param include_prior_dates: If False, only XComs from the current
            execution_date are returned. If True, XComs from previous dates
            are returned as well.
        :type include_prior_dates: bool
        :param limit: the maximum number of results to return. Pass None for
            no limit.
        :type limit: int
        """

        if dag_id is None:
            dag_id = self.dag_id

        pull_fn = functools.partial(
            XCom.get_one,
            execution_date=self.execution_date,
            key=key,
            dag_id=dag_id,
            include_prior_dates=include_prior_dates)

        if is_container(task_ids):
            return tuple(pull_fn(task_id=t) for t in task_ids)
        else:
            return pull_fn(task_id=task_ids)


class Log(Base):
    """
    Used to actively log events to the database
    """

    __tablename__ = "log"

    id = Column(Integer, primary_key=True)
    dttm = Column(DateTime)
    dag_id = Column(String(ID_LEN))
    task_id = Column(String(ID_LEN))
    event = Column(String(30))
    execution_date = Column(DateTime)
    owner = Column(String(500))

    def __init__(self, event, task_instance):
        self.dttm = datetime.now()
        self.dag_id = task_instance.dag_id
        self.task_id = task_instance.task_id
        self.execution_date = task_instance.execution_date
        self.event = event
        self.owner = task_instance.task.owner


@functools.total_ordering
class BaseOperator(object):
    """
    Abstract base class for all operators. Since operators create objects that
    become node in the dag, BaseOperator contains many recursive methods for
    dag crawling behavior. To derive this class, you are expected to override
    the constructor as well as the 'execute' method.

    Operators derived from this task should perform or trigger certain tasks
    synchronously (wait for completion). Example of operators could be an
    operator the runs a Pig job (PigOperator), a sensor operator that
    waits for a partition to land in Hive (HiveSensorOperator), or one that
    moves data from Hive to MySQL (Hive2MySqlOperator). Instances of these
    operators (tasks) target specific operations, running specific scripts,
    functions or data transfers.

    This class is abstract and shouldn't be instantiated. Instantiating a
    class derived from this one results in the creation of a task object,
    which ultimately becomes a node in DAG objects. Task dependencies should
    be set by using the set_upstream and/or set_downstream methods.

    Note that this class is derived from SQLAlquemy's Base class, which
    allows us to push metadata regarding tasks to the database. Deriving this
    classes needs to implement the polymorphic specificities documented in
    SQLAlchemy. This should become clear while reading the code for other
    operators.

    :param task_id: a unique, meaningful id for the task
    :type task_id: string
    :param owner: the owner of the task, using the unix username is recommended
    :type owner: string
    :param retries: the number of retries that should be performed before
        failing the task
    :type retries: int
    :param retry_delay: delay between retries
    :type retry_delay: timedelta
    :param start_date: The ``start_date`` for the task, determines
        the ``execution_date`` for the first task instanec. The best practice
        is to have the start_date rounded
        to your DAG's ``schedule_interval``. Daily jobs have their start_date
        some day at 00:00:00, hourly jobs have their start_date at 00:00
        of a specific hour. Note that Airflow simply looks at the latest
        ``execution_date`` and adds the ``schedule_interval`` to determine
        the next ``execution_date``. It is also very important
        to note that different tasks' dependencies
        need to line up in time. If task A depends on task B and their
        start_date are offset in a way that their execution_date don't line
        up, A's dependencies will never be met. If you are looking to delay
        a task, for example running a daily task at 2AM, look into the
        ``TimeSensor`` and ``TimeDeltaSensor``.
    :type start_date: datetime
    :param end_date: if specified, the scheduler won't go beyond this date
    :type end_date: datetime
    :param depends_on_past: when set to true, task instances will run
        sequentially while relying on the previous task's schedule to
        succeed. The task instance for the start_date is allowed to run.
    :type depends_on_past: bool
    :param wait_for_downstream: when set to true, an instance of task
        X will wait for tasks immediately downstream of the previous instance
        of task X to finish successfully before it runs. This is useful if the
        different instances of a task X alter the same asset, and this asset
        is used by tasks downstream of task X. Note that depends_on_past
        is forced to True wherever wait_for_downstream is used.
    :type wait_for_downstream: bool
    :param queue: which queue to target when running this job. Not
        all executors implement queue management, the CeleryExecutor
        does support targeting specific queues.
    :type queue: str
    :param dag: a reference to the dag the task is attached to (if any)
    :type dag: DAG
    :param priority_weight: priority weight of this task against other task.
        This allows the executor to trigger higher priority tasks before
        others when things get backed up.
    :type priority_weight: int
    :param pool: the slot pool this task should run in, slot pools are a
        way to limit concurrency for certain tasks
    :type pool: str
    :param sla: time by which the job is expected to succeed. Note that
        this represents the ``timedelta`` after the period is closed. For
        example if you set an SLA of 1 hour, the scheduler would send dan email
        soon after 1:00AM on the ``2016-01-02`` if the ``2016-01-01`` instance
        has not succeede yet.
        The scheduler pays special attention for jobs with an SLA and
        sends alert
        emails for sla misses. SLA misses are also recorded in the database
        for future reference. All tasks that share the same SLA time
        get bundled in a single email, sent soon after that time. SLA
        notification are sent once and only once for each task instance.
    :type sla: datetime.timedelta
    :param execution_timeout: max time allowed for the execution of
        this task instance, if it goes beyond it will raise and fail.
    :type execution_timeout: datetime.timedelta
    :param on_failure_callback: a function to be called when a task instance
        of this task fails. a context dictionary is passed as a single
        parameter to this function. Context contains references to related
        objects to the task instance and is documented under the macros
        section of the API.
    :type on_failure_callback: callable
    :param on_retry_callback: much like the ``on_failure_callback`` excepts
        that it is executed when retries occur.
    :param on_success_callback: much like the ``on_failure_callback`` excepts
        that it is executed when the task succeeds.
    :type on_success_callback: callable
    :param trigger_rule: defines the rule by which dependencies are applied
        for the task to get triggered. Options are:
        ``{ all_success | all_failed | all_done | one_success |
        one_failed | dummy}``
        default is ``all_success``. Options can be set as string or
        using the constants defined in the static class
        ``airflow.utils.TriggerRule``
    :type trigger_rule: str
    """

    # For derived classes to define which fields will get jinjaified
    template_fields = []
    # Defines wich files extensions to look for in the templated fields
    template_ext = []
    # Defines the color in the UI
    ui_color = '#fff'
    ui_fgcolor = '#000'

    @apply_defaults
    def __init__(
            self,
            task_id,
            owner,
            email=None,
            email_on_retry=True,
            email_on_failure=True,
            retries=0,
            retry_delay=timedelta(seconds=300),
            start_date=None,
            end_date=None,
            schedule_interval=None,  # not hooked as of now
            depends_on_past=False,
            wait_for_downstream=False,
            dag=None,
            params=None,
            default_args=None,
            adhoc=False,
            priority_weight=1,
            queue=conf.get('celery', 'default_queue'),
            pool=None,
            sla=None,
            execution_timeout=None,
            on_failure_callback=None,
            on_success_callback=None,
            on_retry_callback=None,
            trigger_rule=TriggerRule.ALL_SUCCESS,
            *args,
            **kwargs):

        utils.validate_key(task_id)
        self.dag_id = dag.dag_id if dag else 'adhoc_' + owner
        self.task_id = task_id
        self.owner = owner
        self.email = email
        self.email_on_retry = email_on_retry
        self.email_on_failure = email_on_failure
        self.start_date = start_date
        if start_date and not isinstance(start_date, datetime):
            logging.warning(
                "start_date for {} isn't datetime.datetime".format(self))
        self.end_date = end_date
        self.trigger_rule = trigger_rule
        self.depends_on_past = depends_on_past
        self.wait_for_downstream = wait_for_downstream
        if wait_for_downstream:
            self.depends_on_past = True

        if schedule_interval:
            logging.warning(
                "schedule_interval is used for {}, though it has "
                "been deprecated as a task parameter, you need to "
                "specify it as a DAG parameter instead".format(self))
        self._schedule_interval = schedule_interval
        self.retries = retries
        self.queue = queue
        self.pool = pool
        self.sla = sla
        self.execution_timeout = execution_timeout
        self.on_failure_callback = on_failure_callback
        self.on_success_callback = on_success_callback
        self.on_retry_callback = on_retry_callback
        if isinstance(retry_delay, timedelta):
            self.retry_delay = retry_delay
        else:
            logging.debug("retry_delay isn't timedelta object, assuming secs")
            self.retry_delay = timedelta(seconds=retry_delay)
        self.params = params or {}  # Available in templates!
        self.adhoc = adhoc
        self.priority_weight = priority_weight
        if dag:
            dag.add_task(self)
            self.dag = dag

        # Private attributes
        self._upstream_list = []
        self._downstream_list = []

        self._comps = {
            'task_id',
            'dag_id',
            'owner',
            'email',
            'email_on_retry',
            'retry_delay',
            'start_date',
            'schedule_interval',
            'depends_on_past',
            'wait_for_downstream',
            'adhoc',
            'priority_weight',
            'sla',
            'execution_timeout',
            'on_failure_callback',
            'on_success_callback',
            'on_retry_callback',
        }

    def __eq__(self, other):
        return (
            type(self) == type(other) and
            all(self.__dict__.get(c, None) == other.__dict__.get(c, None)
                for c in self._comps))

    def __neq__(self, other):
        return not self == other

    def __lt__(self, other):
        return self.task_id < other.task_id

    def __hash__(self):
        hash_components = [type(self)]
        for c in self._comps:
            val = getattr(self, c, None)
            try:
                hash(val)
                hash_components.append(val)
            except TypeError:
                hash_components.append(repr(val))
        return hash(tuple(hash_components))

    @property
    def schedule_interval(self):
        """
        The schedule interval of the DAG always wins over individual tasks so
        that tasks within a DAG always line up. The task still needs a
        schedule_interval as it may not be attached to a DAG.
        """
        if hasattr(self, 'dag') and self.dag:
            return self.dag.schedule_interval
        else:
            return self._schedule_interval

    @property
    def priority_weight_total(self):
        return sum([
            t.priority_weight
            for t in self.get_flat_relatives(upstream=False)
        ]) + self.priority_weight

    def pre_execute(self, context):
        """
        This is triggered right before self.execute, it's mostly a hook
        for people deriving operators.
        """
        pass

    def execute(self, context):
        """
        This is the main method to derive when creating an operator.
        Context is the same dictionary used as when rendering jinja templates.

        Refer to get_template_context for more context.
        """
        raise NotImplemented()

    def post_execute(self, context):
        """
        This is triggered right after self.execute, it's mostly a hook
        for people deriving operators.
        """
        pass

    def on_kill(self):
        '''
        Override this method to cleanup subprocesses when a task instance
        gets killed. Any use of the threading, subprocess or multiprocessing
        module within an operator needs to be cleaned up or it will leave
        ghost processes behind.
        '''
        pass

    def __deepcopy__(self, memo):
        """
        Hack sorting double chained task lists by task_id to avoid hitting
        max_depth on deepcopy operations.
        """
        sys.setrecursionlimit(5000)  # TODO fix this in a better way
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result

        self._upstream_list = sorted(self._upstream_list, key=lambda x: x.task_id)
        self._downstream_list = sorted(self._downstream_list, key=lambda x: x.task_id)
        for k, v in list(self.__dict__.items()):
            if k not in ('user_defined_macros', 'params'):
                setattr(result, k, copy.deepcopy(v, memo))
        return result

    def render_template_from_field(self, content, context, jinja_env):
        '''
        Renders a template from a field. If the field is a string, it will
        simply render the string and return the result. If it is a collection or
        nested set of collections, it will traverse the structure and render
        all strings in it.
        '''
        rt = self.render_template
        if isinstance(content, basestring):
            result = jinja_env.from_string(content).render(**context)
        elif isinstance(content, (list, tuple)):
            result = [rt(e, context) for e in content]
        elif isinstance(content, dict):
            result = {
                k: rt(v, context)
                for k, v in list(content.items())}
        else:
            param_type = type(content)
            msg = (
                "Type '{param_type}' used for parameter '{attr}' is "
                "not supported for templating").format(**locals())
            raise AirflowException(msg)
        return result

    def render_template(self, content, context):
        '''
        Renders a template either from a file or directly in a field, and returns
        the rendered result.
        '''
        jinja_env = self.dag.get_template_env() \
            if hasattr(self, 'dag') \
            else jinja2.Environment(cache_size=0)

        exts = self.__class__.template_ext
        return jinja_env.get_template(content).render(**context) \
            if isinstance(content, basestring) and any([content.endswith(ext) for ext in exts]) \
            else self.render_template_from_field(content, context, jinja_env)

    def prepare_template(self):
        '''
        Hook that is triggered after the templated fields get replaced
        by their content. If you need your operator to alter the
        content of the file before the template is rendered,
        it should override this method to do so.
        '''
        pass

    def resolve_template_files(self):
        # Getting the content of files for template_field / template_ext
        for attr in self.template_fields:
            content = getattr(self, attr)
            if (content and isinstance(content, basestring) and
                    any([content.endswith(ext) for ext in self.template_ext])):
                env = self.dag.get_template_env()
                try:
                    setattr(self, attr, env.loader.get_source(env, content)[0])
                except Exception as e:
                    logging.exception(e)
        self.prepare_template()

    @property
    def upstream_list(self):
        """@property: list of tasks directly upstream"""
        return self._upstream_list

    @property
    def downstream_list(self):
        """@property: list of tasks directly downstream"""
        return self._downstream_list

    def clear(
            self, start_date=None, end_date=None,
            upstream=False, downstream=False):
        """
        Clears the state of task instances associated with the task, following
        the parameters specified.
        """
        session = settings.Session()

        TI = TaskInstance
        qry = session.query(TI).filter(TI.dag_id == self.dag_id)

        if start_date:
            qry = qry.filter(TI.execution_date >= start_date)
        if end_date:
            qry = qry.filter(TI.execution_date <= end_date)

        tasks = [self.task_id]

        if upstream:
            tasks += \
                [t.task_id for t in self.get_flat_relatives(upstream=True)]

        if downstream:
            tasks += \
                [t.task_id for t in self.get_flat_relatives(upstream=False)]

        qry = qry.filter(TI.task_id.in_(tasks))

        count = qry.count()
        clear_task_instances(qry, session)

        session.commit()
        session.close()
        return count

    def get_task_instances(self, session, start_date=None, end_date=None):
        """
        Get a set of task instance related to this task for a specific date
        range.
        """
        TI = TaskInstance
        end_date = end_date or datetime.now()
        return session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.task_id == self.task_id,
            TI.execution_date >= start_date,
            TI.execution_date <= end_date,
        ).order_by(TI.execution_date).all()

    def get_flat_relatives(self, upstream=False, l=None):
        """
        Get a flat list of relatives, either upstream or downstream.
        """
        if not l:
            l = []
        for t in self.get_direct_relatives(upstream):
            if not utils.is_in(t, l):
                l.append(t)
                t.get_flat_relatives(upstream, l)
        return l

    def detect_downstream_cycle(self, task=None):
        """
        When invoked, this routine will raise an exception if a cycle is
        detected downstream from self. It is invoked when tasks are added to
        the DAG to detect cycles.
        """
        if not task:
            task = self
        for t in self.get_direct_relatives():
            if task is t:
                msg = "Cycle detect in DAG. Faulty task: {0}".format(task)
                raise AirflowException(msg)
            else:
                t.detect_downstream_cycle(task=task)
        return False

    def run(
            self, start_date=None, end_date=None, ignore_dependencies=False,
            force=False, mark_success=False):
        """
        Run a set of task instances for a date range.
        """
        start_date = start_date or self.start_date
        end_date = end_date or self.end_date or datetime.now()

        for dt in utils.date_range(
                start_date, end_date, self.schedule_interval):
            TaskInstance(self, dt).run(
                mark_success=mark_success,
                ignore_dependencies=ignore_dependencies,
                force=force,)

    def dry_run(self):
        logging.info('Dry run')
        for attr in self.template_fields:
            content = getattr(self, attr)
            if content and isinstance(content, basestring):
                logging.info('Rendering template for {0}'.format(attr))
                logging.info(content)


    def get_direct_relatives(self, upstream=False):
        """
        Get the direct relatives to the current task, upstream or
        downstream.
        """
        if upstream:
            return self.upstream_list
        else:
            return self.downstream_list

    def __repr__(self):
        return "<Task({self.__class__.__name__}): {self.task_id}>".format(self=self)

    @property
    def task_type(self):
        return self.__class__.__name__

    def append_only_new(self, l, item):
        if any([item is t for t in l]):
            raise AirflowException(
                'Dependency {self}, {item} already registered'
                ''.format(**locals()))
        else:
            l.append(item)

    def _set_relatives(self, task_or_task_list, upstream=False):
        try:
            task_list = list(task_or_task_list)
        except TypeError:
            task_list = [task_or_task_list]
        for task in task_list:
            if not isinstance(task, BaseOperator):
                raise AirflowException('Expecting a task')
            if upstream:
                self.append_only_new(task._downstream_list, self)
                self.append_only_new(self._upstream_list, task)
            else:
                self.append_only_new(task._upstream_list, self)
                self.append_only_new(self._downstream_list, task)
        self.detect_downstream_cycle()

    def set_downstream(self, task_or_task_list):
        """
        Set a task, or a task task to be directly downstream from the current
        task.
        """
        self._set_relatives(task_or_task_list, upstream=False)

    def set_upstream(self, task_or_task_list):
        """
        Set a task, or a task task to be directly upstream from the current
        task.
        """
        self._set_relatives(task_or_task_list, upstream=True)

    def xcom_push(
            self,
            context,
            key,
            value,
            execution_date=None):
        """
        See TaskInstance.xcom_push()
        """
        context['ti'].xcom_push(
            key=key,
            value=value,
            execution_date=execution_date)

    def xcom_pull(
            self,
            context,
            task_ids,
            dag_id=None,
            key=XCOM_RETURN_KEY,
            include_prior_dates=None):
        """
        See TaskInstance.xcom_pull()
        """
        return context['ti'].xcom_pull(
            key=key,
            task_ids=task_ids,
            dag_id=dag_id,
            include_prior_dates=include_prior_dates)


class DagModel(Base):

    __tablename__ = "dag"
    """
    These items are stored in the database for state related information
    """
    dag_id = Column(String(ID_LEN), primary_key=True)
    # A DAG can be paused from the UI / DB
    is_paused = Column(Boolean, default=False)
    # Whether the DAG is a subdag
    is_subdag = Column(Boolean, default=False)
    # Whether that DAG was seen on the last DagBag load
    is_active = Column(Boolean, default=False)
    # Last time the scheduler started
    last_scheduler_run = Column(DateTime)
    # Last time this DAG was pickled
    last_pickled = Column(DateTime)
    # When the DAG received a refreshed signal last, used to know when
    # we need to force refresh
    last_expired = Column(DateTime)
    # Whether (one  of) the scheduler is scheduling this DAG at the moment
    scheduler_lock = Column(Boolean)
    # Foreign key to the latest pickle_id
    pickle_id = Column(Integer)
    # The location of the file containing the DAG object
    fileloc = Column(String(2000))
    # String representing the owners
    owners = Column(String(2000))

    def __repr__(self):
        return "<DAG: {self.dag_id}>".format(self=self)

    @classmethod
    def get_current(cls, dag_id):
        session = settings.Session()
        obj = session.query(cls).filter(cls.dag_id == dag_id).first()
        session.expunge_all()
        session.commit()
        session.close()
        return obj


@functools.total_ordering
class DAG(object):
    """
    A dag (directed acyclic graph) is a collection of tasks with directional
    dependencies. A dag also has a schedule, a start end an end date
    (optional). For each schedule, (say daily or hourly), the DAG needs to run
    each individual tasks as their dependencies are met. Certain tasks have
    the property of depending on their own past, meaning that they can't run
    until their previous schedule (and upstream tasks) are completed.

    DAGs essentially act as namespaces for tasks. A task_id can only be
    added once to a DAG.

    :param dag_id: The id of the DAG
    :type dag_id: string
    :param schedule_interval: Defines how often that DAG runs, this
        timedelta object gets added to your latest task instance's
        execution_date to figure out the next schedule
    :type schedule_interval: datetime.timedelta or
        dateutil.relativedelta.relativedelta
    :param start_date: The timestamp from which the scheduler will
        attempt to backfill
    :type start_date: datetime.datetime
    :param end_date: A date beyond which your DAG won't run, leave to None
        for open ended scheduling
    :type end_date: datetime.datetime
    :param template_searchpath: This list of folders (non relative)
        defines where jinja will look for your templates. Order matters.
        Note that jinja/airflow includes the path of your DAG file by
        default
    :type template_searchpath: string or list of stings
    :param user_defined_macros: a dictionary of macros that will be exposed
        in your jinja templates. For example, passing ``dict(foo='bar')``
        to this argument allows you to ``{{ foo }}`` in all jinja
        templates related to this DAG. Note that you can pass any
        type of object here.
    :type user_defined_macros: dict
    :param default_args: A dictionary of default parameters to be used
        as constructor keyword parameters when initialising operators.
        Note that operators have the same hook, and precede those defined
        here, meaning that if your dict contains `'depends_on_past': True`
        here and `'depends_on_past': False` in the operator's call
        `default_args`, the actual value will be `False`.
    :type default_args: dict
    :param params: a dictionary of DAG level parameters that are made
        accessible in templates, namespaced under `params`. These
        params can be overridden at the task level.
    :type params: dict
    """

    def __init__(
            self, dag_id,
            schedule_interval=timedelta(days=1),
            start_date=None, end_date=None,
            full_filepath=None,
            template_searchpath=None,
            user_defined_macros=None,
            default_args=None,
            params=None):

        self.user_defined_macros = user_defined_macros
        self.default_args = default_args or {}
        self.params = params
        utils.validate_key(dag_id)
        self.tasks = []
        self.dag_id = dag_id
        self.start_date = start_date
        self.end_date = end_date or datetime.now()
        self.schedule_interval = schedule_interval
        self.full_filepath = full_filepath if full_filepath else ''
        if isinstance(template_searchpath, basestring):
            template_searchpath = [template_searchpath]
        self.template_searchpath = template_searchpath
        self.parent_dag = None  # Gets set when DAGs are loaded
        self.last_loaded = datetime.now()
        self.safe_dag_id = dag_id.replace('.', '__dot__')

        self._comps = {
            'dag_id',
            'tasks',
            'parent_dag',
            'start_date',
            'schedule_interval',
            'full_filepath',
            'template_searchpath',
            'last_loaded',
        }

    def __repr__(self):
        return "<DAG: {self.dag_id}>".format(self=self)

    def __eq__(self, other):
        return (
            type(self) == type(other) and
            all(self.__dict__.get(c, None) == other.__dict__.get(c, None)
                for c in self._comps))

    def __neq__(self, other):
        return not self == other

    def __lt__(self, other):
        return self.dag_id < other.dag_id

    def __hash__(self):
        hash_components = [type(self)]
        for c in self._comps:
            val = getattr(self, c, None)
            try:
                hash(val)
                hash_components.append(val)
            except TypeError:
                hash_components.append(repr(val))
        return hash(tuple(hash_components))

    @property
    def task_ids(self):
        return [t.task_id for t in self.tasks]

    @property
    def filepath(self):
        """
        File location of where the dag object is instantiated
        """
        fn = self.full_filepath.replace(DAGS_FOLDER + '/', '')
        fn = fn.replace(os.path.dirname(__file__) + '/', '')
        return fn

    @property
    def folder(self):
        """
        Folder location of where the dag object is instantiated
        """
        return os.path.dirname(self.full_filepath)

    @property
    def owner(self):
        return ", ".join(list(set([t.owner for t in self.tasks])))

    @property
    def latest_execution_date(self):
        """
        Returns the latest date for which at least one task instance exists
        """
        TI = TaskInstance
        session = settings.Session()
        execution_date = session.query(func.max(TI.execution_date)).filter(
            TI.dag_id == self.dag_id,
            TI.task_id.in_(self.task_ids)
        ).scalar()
        session.commit()
        session.close()
        return execution_date

    @property
    def subdags(self):
        """
        Returns a list of the subdag objects associated to this DAG
        """
        # Late import to prevent circular imports
        from airflow.operators import SubDagOperator
        l = []
        for task in self.tasks:
            if isinstance(task, SubDagOperator):
                l.append(task.subdag)
                l += task.subdag.subdags
        return l

    def resolve_template_files(self):
        for t in self.tasks:
            t.resolve_template_files()

    def crawl_for_tasks(objects):
        """
        Typically called at the end of a script by passing globals() as a
        parameter. This allows to not explicitly add every single task to the
        dag explicitly.
        """
        raise NotImplemented("")

    def override_start_date(self, start_date):
        """
        Sets start_date of all tasks and of the DAG itself to a certain date.
        This is used by BackfillJob.
        """
        for t in self.tasks:
            t.start_date = start_date
        self.start_date = start_date

    def get_template_env(self):
        '''
        Returns a jinja2 Environment while taking into account the DAGs
        template_searchpath and user_defined_macros
        '''
        searchpath = [self.folder]
        if self.template_searchpath:
            searchpath += self.template_searchpath

        env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(searchpath),
            extensions=["jinja2.ext.do"],
            cache_size=0)
        if self.user_defined_macros:
            env.globals.update(self.user_defined_macros)

        return env

    def set_dependency(self, upstream_task_id, downstream_task_id):
        """
        Simple utility method to set dependency between two tasks that
        already have been added to the DAG using add_task()
        """
        self.get_task(upstream_task_id).set_downstream(
            self.get_task(downstream_task_id))

    def get_task_instances(
            self, session, start_date=None, end_date=None, state=None):
        TI = TaskInstance
        if not start_date:
            start_date = (datetime.today()-timedelta(30)).date()
            start_date = datetime.combine(start_date, datetime.min.time())
        if not end_date:
            end_date = datetime.now()
        tis = session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.execution_date >= start_date,
            TI.execution_date <= end_date,
            TI.task_id.in_([t.task_id for t in self.tasks]),
        )
        if state:
            tis = tis.filter(TI.state == state)
        tis = tis.all()
        return tis

    @property
    def roots(self):
        return [t for t in self.tasks if not t.downstream_list]

    def clear(
            self, start_date=None, end_date=None,
            only_failed=False,
            only_running=False,
            confirm_prompt=False,
            include_subdags=True,
            dry_run=False):
        session = settings.Session()
        """
        Clears a set of task instances associated with the current dag for
        a specified date range.
        """

        TI = TaskInstance
        tis = session.query(TI)
        if include_subdags:
            # Crafting the right filter for dag_id and task_ids combo
            conditions = []
            for dag in self.subdags + [self]:
                conditions.append(
                    TI.dag_id.like(dag.dag_id) & TI.task_id.in_(dag.task_ids)
                )
            tis = tis.filter(or_(*conditions))
        else:
            tis = session.query(TI).filter(TI.dag_id == self.dag_id)
            tis = tis.filter(TI.task_id.in_(self.task_ids))

        if start_date:
            tis = tis.filter(TI.execution_date >= start_date)
        if end_date:
            tis = tis.filter(TI.execution_date <= end_date)
        if only_failed:
            tis = tis.filter(TI.state == State.FAILED)
        if only_running:
            tis = tis.filter(TI.state == State.RUNNING)

        if dry_run:
            tis = tis.all()
            session.expunge_all()
            return tis

        count = tis.count()
        if count == 0:
            print("Nothing to clear.")
            return 0
        if confirm_prompt:
            ti_list = "\n".join([str(t) for t in tis])
            question = (
                "You are about to delete these {count} tasks:\n"
                "{ti_list}\n\n"
                "Are you sure? (yes/no): ").format(**locals())
            if utils.ask_yesno(question):
                clear_task_instances(tis, session)
            else:
                count = 0
                print("Bail. Nothing was cleared.")
        else:
            clear_task_instances(tis, session)

        session.commit()
        session.close()
        return count

    def __deepcopy__(self, memo):
        # Swiwtcharoo to go around deepcopying objects coming through the
        # backdoor
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in list(self.__dict__.items()):
            if k not in ('user_defined_macros', 'params'):
                setattr(result, k, copy.deepcopy(v, memo))

        result.user_defined_macros = self.user_defined_macros
        result.params = self.params
        return result

    def sub_dag(
            self, task_regex,
            include_downstream=False, include_upstream=True):
        """
        Returns a subset of the current dag as a deep copy of the current dag
        based on a regex that should match one or many tasks, and includes
        upstream and downstream neighbours based on the flag passed.
        """

        dag = copy.deepcopy(self)

        regex_match = [
            t for t in dag.tasks if re.findall(task_regex, t.task_id)]
        also_include = []
        for t in regex_match:
            if include_downstream:
                also_include += t.get_flat_relatives(upstream=False)
            if include_upstream:
                also_include += t.get_flat_relatives(upstream=True)
        # Compiling the unique list of tasks that made the cut
        tasks = list(set(regex_match + also_include))
        dag.tasks = tasks
        for t in dag.tasks:
            # Removing upstream/downstream references to tasks that did not
            # made the cut
            t._upstream_list = [
                ut for ut in t._upstream_list if utils.is_in(ut, tasks)]
            t._downstream_list = [
                ut for ut in t._downstream_list if utils.is_in(ut, tasks)]

        return dag

    def has_task(self, task_id):
        return task_id in (t.task_id for t in self.tasks)

    def get_task(self, task_id):
        for task in self.tasks:
            if task.task_id == task_id:
                return task
        raise AirflowException("Task {task_id} not found".format(**locals()))

    def pickle(self, main_session=None):
        session = main_session or settings.Session()
        dag = session.query(
            DagModel).filter(DagModel.dag_id == self.dag_id).first()
        dp = None
        if dag and dag.pickle_id:
            dp = session.query(DagPickle).filter(
                DagPickle.id == dag.pickle_id).first()
        if not dp or dp.pickle != self:
            dp = DagPickle(dag=self)
            session.add(dp)
            self.last_pickled = datetime.now()
            session.commit()
            self.pickle_id = dp.id

        if not main_session:
            session.close()
        return dp

    def tree_view(self):
        """
        Shows an ascii tree representation of the DAG
        """
        def get_downstream(task, level=0):
            print((" " * level * 4) + str(task))
            level += 1
            for t in task.upstream_list:
                get_downstream(t, level)

        for t in self.roots:
            get_downstream(t)

    def add_task(self, task):
        '''
        Add a task to the DAG

        :param task: the task you want to add
        :type task: task
        '''
        if not self.start_date and not task.start_date:
            raise AirflowException("Task is missing the start_date parameter")
        if not task.start_date:
            task.start_date = self.start_date

        if task.task_id in [t.task_id for t in self.tasks]:
            raise AirflowException(
                "Task id '{0}' has already been added "
                "to the DAG ".format(task.task_id))
        else:
            self.tasks.append(task)
            task.dag_id = self.dag_id
            task.dag = self
        self.task_count = len(self.tasks)

    def add_tasks(self, tasks):
        '''
        Add a list of tasks to the DAG

        :param task: a lit of tasks you want to add
        :type task: list of tasks
        '''
        for task in tasks:
            self.add_task(task)

    def db_merge(self):
        BO = BaseOperator
        session = settings.Session()
        tasks = session.query(BO).filter(BO.dag_id == self.dag_id).all()
        for t in tasks:
            session.delete(t)
        session.commit()
        session.merge(self)
        session.commit()

    def run(
            self, start_date=None, end_date=None, mark_success=False,
            include_adhoc=False, local=False, executor=None,
            donot_pickle=conf.getboolean('core', 'donot_pickle'), ignore_dependencies=False):
        from airflow.jobs import BackfillJob
        if not executor and local:
            executor = LocalExecutor()
        elif not executor:
            executor = DEFAULT_EXECUTOR
        job = BackfillJob(
            self,
            start_date=start_date,
            end_date=end_date,
            mark_success=mark_success,
            include_adhoc=include_adhoc,
            executor=executor,
            donot_pickle=donot_pickle,
            ignore_dependencies=ignore_dependencies)
        job.run()


class Chart(Base):
    __tablename__ = "chart"

    id = Column(Integer, primary_key=True)
    label = Column(String(200))
    conn_id = Column(String(ID_LEN), nullable=False)
    user_id = Column(Integer(), ForeignKey('user.id'),)
    chart_type = Column(String(100), default="line")
    sql_layout = Column(String(50), default="series")
    sql = Column(Text, default="SELECT series, x, y FROM table")
    y_log_scale = Column(Boolean)
    show_datatable = Column(Boolean)
    show_sql = Column(Boolean, default=True)
    height = Column(Integer, default=600)
    default_params = Column(String(5000), default="{}")
    owner = relationship(
        "User", cascade=False, cascade_backrefs=False, backref='charts')
    x_is_date = Column(Boolean, default=True)
    iteration_no = Column(Integer, default=0)
    last_modified = Column(DateTime, default=datetime.now())

    def __repr__(self):
        return self.label


class KnownEventType(Base):
    __tablename__ = "known_event_type"

    id = Column(Integer, primary_key=True)
    know_event_type = Column(String(200))

    def __repr__(self):
        return self.know_event_type


class KnownEvent(Base):
    __tablename__ = "known_event"

    id = Column(Integer, primary_key=True)
    label = Column(String(200))
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    user_id = Column(Integer(), ForeignKey('user.id'),)
    known_event_type_id = Column(Integer(), ForeignKey('known_event_type.id'),)
    reported_by = relationship(
        "User", cascade=False, cascade_backrefs=False, backref='known_events')
    event_type = relationship(
        "KnownEventType",
        cascade=False,
        cascade_backrefs=False, backref='known_events')
    description = Column(Text)

    def __repr__(self):
        return self.label


class Variable(Base):
    __tablename__ = "variable"

    id = Column(Integer, primary_key=True)
    key = Column(String(ID_LEN), unique=True)
    val = Column(Text)

    def __repr__(self):
        return '{} : {}'.format(self.key, self.val)

    @classmethod
    @provide_session
    def get(cls, key, default_var=None, deserialize_json=False, session=None):
        obj = session.query(cls).filter(cls.key == key).first()
        if obj is None:
            if default_var is not None:
                v = default_var
            else:
                raise ValueError('Variable {} does not exist'.format(key))
        else:
            v = obj.val
        if deserialize_json and v:
            v = json.loads(v)
        return v


class XCom(Base):
    """
    Base class for XCom objects.
    """
    __tablename__ = "xcom"

    id = Column(Integer, primary_key=True)
    key = Column(String(512))
    value = Column(PickleType(pickler=dill))
    timestamp = Column(
        DateTime, default=func.now(), nullable=False)
    execution_date = Column(DateTime, nullable=False)

    # source information
    task_id = Column(String(ID_LEN), nullable=False)
    dag_id = Column(String(ID_LEN), nullable=False)

    def __repr__(self):
        return '<XCom "{key}" ({task_id} @ {execution_date})>'.format(
            key=self.key,
            task_id=self.task_id,
            execution_date=self.execution_date)

    @classmethod
    @provide_session
    def set(
            cls,
            key,
            value,
            execution_date,
            task_id,
            dag_id,
            session=None):
        """
        Store an XCom value.
        """
        session.expunge_all()

        # remove any duplicate XComs
        session.query(cls).filter(
            cls.key == key,
            cls.execution_date == execution_date,
            cls.task_id == task_id,
            cls.dag_id == dag_id).delete()

        # insert new XCom
        session.add(XCom(
            key=key,
            value=value,
            execution_date=execution_date,
            task_id=task_id,
            dag_id=dag_id))

        session.commit()

    @classmethod
    @provide_session
    def get_one(
            cls,
            execution_date,
            key=None,
            task_id=None,
            dag_id=None,
            include_prior_dates=False,
            session=None):
        """
        Retrieve an XCom value, optionally meeting certain criteria
        """
        filters = []
        if key:
            filters.append(cls.key == key)
        if task_id:
            filters.append(cls.task_id == task_id)
        if dag_id:
            filters.append(cls.dag_id == dag_id)
        if include_prior_dates:
            filters.append(cls.execution_date <= execution_date)
        else:
            filters.append(cls.execution_date == execution_date)

        query = (
            session.query(cls.value)
            .filter(and_(*filters))
            .order_by(cls.execution_date.desc(), cls.timestamp.desc())
            .limit(1))

        result = query.first()
        if result:
            return result.value

    @classmethod
    @provide_session
    def get_many(
            cls,
            execution_date,
            key=None,
            task_ids=None,
            dag_ids=None,
            include_prior_dates=False,
            limit=100,
            session=None):
        """
        Retrieve an XCom value, optionally meeting certain criteria
        """
        filters = []
        if key:
            filters.append(cls.key == key)
        if task_ids:
            filters.append(cls.task_id.in_(as_tuple(task_ids)))
        if dag_ids:
            filters.append(cls.dag_id.in_(as_tuple(dag_ids)))
        if include_prior_dates:
            filters.append(cls.execution_date <= execution_date)
        else:
            filters.append(cls.execution_date == execution_date)

        query = (
            session.query(cls)
            .filter(and_(*filters))
            .order_by(cls.execution_date.desc(), cls.timestamp.desc())
            .limit(limit))

        return query.all()


class DagRun(Base):
    """
    DagRun describes an instance of a Dag. It can be created
    by the scheduler (for regular runs) or by an external trigger
    """
    __tablename__ = "dag_run"

    dag_id = Column(String(ID_LEN), primary_key=True)
    execution_date = Column(DateTime, primary_key=True)
    state = Column(String(50))
    run_id = Column(String(ID_LEN))
    external_trigger = Column(Boolean, default=False)

    def __repr__(self):
        return '<DagRun {dag_id} @ {execution_date}: {run_id}, \
            externally triggered: {external_trigger}>'.format(
            dag_id=self.dag_id,
            execution_date=self.execution_date,
            run_id=self.run_id,
            external_trigger=self.external_trigger)
        return str((
            self.dag_id, self.run_id, self.execution_date.isoformat()))


class Pool(Base):
    __tablename__ = "slot_pool"

    id = Column(Integer, primary_key=True)
    pool = Column(String(50), unique=True)
    slots = Column(Integer, default=0)
    description = Column(Text)

    def __repr__(self):
        return self.pool

    @provide_session
    def used_slots(self, session):
        """
        Returns the number of slots used at the moment
        """
        running = (
            session
            .query(TaskInstance)
            .filter(TaskInstance.pool == self.pool)
            .filter(TaskInstance.state == State.RUNNING)
            .count()
        )
        return running

    @provide_session
    def queued_slots(self, session):
        """
        Returns the number of slots used at the moment
        """
        return (
            session
            .query(TaskInstance)
            .filter(TaskInstance.pool == self.pool)
            .filter(TaskInstance.state == State.QUEUED)
            .count()
        )

    @provide_session
    def open_slots(self, session):
        """
        Returns the number of slots open at the moment
        """
        used_slots = self.used_slots(session=session)
        return self.slots - used_slots


class SlaMiss(Base):
    """
    Model that stores a history of the SLA that have been missed.
    It is used to keep track of SLA failures over time and to avoid double
    triggering alert emails.
    """
    __tablename__ = "sla_miss"

    task_id = Column(String(ID_LEN), primary_key=True)
    dag_id = Column(String(ID_LEN), primary_key=True)
    execution_date = Column(DateTime, primary_key=True)
    email_sent = Column(Boolean, default=False)
    timestamp = Column(DateTime)
    description = Column(Text)

    def __repr__(self):
        return str((
            self.dag_id, self.task_id, self.execution_date.isoformat()))


class ImportError(Base):
    __tablename__ = "import_error"
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime)
    filename = Column(String(1024))
    stacktrace = Column(Text)
