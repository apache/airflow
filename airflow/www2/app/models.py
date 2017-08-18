from flask_appbuilder import Model
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Boolean, Text, Float, func, Index
from sqlalchemy.orm import relationship

from airflow.ti_deps.dep_context import DepContext
from airflow.utils.state import State
from airflow import configuration

ID_LEN = 250


class DagModel(Model):
    __tablename__ = "dag"

    dag_id = Column(String(ID_LEN), primary_key=True)
    is_paused_at_creation = configuration.getboolean('core',
                                                     'dags_are_paused_at_creation')
    is_paused = Column(Boolean, default=is_paused_at_creation)
    is_subdag = Column(Boolean, default=False)
    is_active = Column(Boolean, default=False)
    last_scheduler_run = Column(DateTime)
    last_pickled = Column(DateTime)
    last_expired = Column(DateTime)
    scheduler_lock = Column(Boolean)
    pickle_id = Column(Integer)
    fileloc = Column(String(2000))
    owners = Column(String(2000))


class XCom(Model):
    __tablename__ = "xcom"

    id = Column(Integer, primary_key=True)
    key = Column(String(512))
    # todo: FAB can't handle binary
    # value = Column(PickleType(pickler=dill))
    value = None
    timestamp = Column(DateTime, default=func.now(), nullable=False)
    execution_date = Column(DateTime, nullable=False)
    task_id = Column(String(ID_LEN), nullable=False)
    dag_id = Column(String(ID_LEN), nullable=False)


class DagRun(Model):
    __tablename__ = "dag_run"

    ID_PREFIX = 'scheduled__'
    ID_FORMAT_PREFIX = ID_PREFIX + '{0}'
    DEADLOCK_CHECK_DEP_CONTEXT = DepContext(ignore_in_retry_period=True)

    id = Column(Integer, primary_key=True)
    dag_id = Column(String(ID_LEN))
    execution_date = Column(DateTime, default=func.now())
    start_date = Column(DateTime, default=func.now())
    end_date = Column(DateTime)
    state = Column('state', String(50), default=State.RUNNING)
    run_id = Column(String(ID_LEN))
    external_trigger = Column(Boolean, default=True)
    # todo: FAB can't handle binary
    # conf = Column(PickleType)
    conf = None


class Connection(Model):
    __tablename__ = "connection"

    id = Column(Integer(), primary_key=True)
    conn_id = Column(String(ID_LEN))
    conn_type = Column(String(500))
    host = Column(String(500))
    schema = Column(String(500))
    login = Column(String(500))
    password = Column('password', String(5000))
    port = Column(Integer())
    is_encrypted = Column(Boolean, unique=False, default=False)
    is_extra_encrypted = Column(Boolean, unique=False, default=False)
    extra = Column('extra', String(5000))


class Variable(Model):
    __tablename__ = "variable"

    id = Column(Integer, primary_key=True)
    key = Column(String(ID_LEN), unique=True)
    val = Column('val', Text)
    is_encrypted = Column(Boolean, unique=False, default=False)


class Pool(Model):
    __tablename__ = "slot_pool"

    id = Column(Integer, primary_key=True)
    pool = Column(String(50), unique=True)
    slots = Column(Integer, default=0)
    description = Column(Text)

class SlaMiss(Model):
    __tablename__ = "sla_miss"

    task_id = Column(String(ID_LEN), primary_key=True)
    dag_id = Column(String(ID_LEN), primary_key=True)
    execution_date = Column(DateTime, primary_key=True)
    email_sent = Column(Boolean, default=False)
    timestamp = Column(DateTime)
    description = Column(Text)
    notification_sent = Column(Boolean, default=False)


class DagPickle(Model):
    __tablename__ = "dag_pickle"

    id = Column(Integer, primary_key=True)
    # todo: FAB can't handle binary
    # pickle = Column(PickleType(pickler=dill))
    created_dttm = Column(DateTime, default=func.now())
    pickle_hash = Column(Text)


class TaskInstance(Model):
    __tablename__ = "task_instance"

    task_id = Column(String(ID_LEN), primary_key=True)
    dag_id = Column(String(ID_LEN), primary_key=True)
    execution_date = Column(DateTime, primary_key=True)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    duration = Column(Float)
    state = Column(String(20))
    try_number = Column(Integer, default=0)
    max_tries = Column(Integer)
    hostname = Column(String(1000))
    unixname = Column(String(1000))
    job_id = Column(Integer)
    pool = Column(String(50))
    queue = Column(String(50))
    priority_weight = Column(Integer)
    operator = Column(String(1000))
    queued_dttm = Column(DateTime)
    pid = Column(Integer)


class TaskFail(Model):
    __tablename__ = "task_fail"

    task_id = Column(String(ID_LEN), primary_key=True)
    dag_id = Column(String(ID_LEN), primary_key=True)
    execution_date = Column(DateTime, primary_key=True)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    duration = Column(Float)


class Log(Model):
    __tablename__ = "log"

    id = Column(Integer, primary_key=True)
    dttm = Column(DateTime)
    dag_id = Column(String(ID_LEN))
    task_id = Column(String(ID_LEN))
    event = Column(String(30))
    execution_date = Column(DateTime)
    owner = Column(String(500))
    extra = Column(Text)


class Chart(Model):
    __tablename__ = "chart"

    id = Column(Integer, primary_key=True)
    label = Column(String(200))
    conn_id = Column(String(ID_LEN), nullable=False)
    user_id = Column(Integer(), ForeignKey('users.id'), nullable=True)
    chart_type = Column(String(100), default="line")
    sql_layout = Column(String(50), default="series")
    sql = Column(Text, default="SELECT series, x, y FROM table")
    y_log_scale = Column(Boolean)
    show_datatable = Column(Boolean)
    show_sql = Column(Boolean, default=True)
    height = Column(Integer, default=600)
    default_params = Column(String(5000), default="{}")
    owner = relationship(
        "User_", cascade=False, cascade_backrefs=False, backref='charts')
    x_is_date = Column(Boolean, default=True)
    iteration_no = Column(Integer, default=0)
    last_modified = Column(DateTime, default=func.now())


class KnownEventType_(Model): # modified so there's no collision with airflow.models
    __tablename__ = "known_event_type"

    id = Column(Integer, primary_key=True)
    know_event_type = Column(String(200))


class User_(Model): # modified so there's no collision with airflow.models
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    username = Column(String(ID_LEN), unique=True)
    email = Column(String(500))


class KnownEvent(Model):
    __tablename__ = "known_event"

    id = Column(Integer, primary_key=True)
    label = Column(String(200))
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    user_id = Column(Integer(), ForeignKey('users.id'),)
    known_event_type_id = Column(Integer(), ForeignKey('known_event_type.id'),)
    description = Column(Text)
    reported_by = relationship(
        "User_", cascade=False, cascade_backrefs=False, backref='known_events')
    event_type = relationship(
        "KnownEventType_", cascade=False, cascade_backrefs=False, backref='known_events')

class DagStat(Model):
    __tablename__ = "dag_stats"

    dag_id = Column(String(ID_LEN), primary_key=True)
    state = Column(String(50), primary_key=True)
    count = Column(Integer, default=0)
    dirty = Column(Boolean, default=False)


class ImportError(Model):
    __tablename__ = "import_error"
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime)
    filename = Column(String(1024))
    stacktrace = Column(Text)


class BaseJob(Model):
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


class SchedulerJob(BaseJob):
    __mapper_args__ = {
        'polymorphic_identity': 'SchedulerJob'
    }


class BackfillJob(BaseJob):
    __mapper_args__ = {
        'polymorphic_identity': 'BackfillJob'
    }


class LocalTaskJob(BaseJob):
    __mapper_args__ = {
        'polymorphic_identity': 'LocalTaskJob'
    }