from __future__ import print_function
from copy import copy
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import errno
from functools import wraps
import imp
import inspect
import logging
import os
import re
import shutil
import signal
import smtplib
from tempfile import mkdtemp

from contextlib import contextmanager

from sqlalchemy import event, exc
from sqlalchemy.pool import Pool

from airflow import settings
from airflow.configuration import conf


class AirflowException(Exception):
    pass


class AirflowSensorTimeout(Exception):
    pass


class State(object):
    """
    Static class with task instance states constants and color method to
    avoid hardcoding.
    """
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    SHUTDOWN = "shutdown"  # External request to shut down
    FAILED = "failed"
    UP_FOR_RETRY = "up_for_retry"
    UPSTREAM_FAILED = "upstream_failed"
    SKIPPED = "skipped"

    state_color = {
        QUEUED: 'gray',
        RUNNING: 'lime',
        SUCCESS: 'green',
        SHUTDOWN: 'blue',
        FAILED: 'red',
        UP_FOR_RETRY: 'gold',
        UPSTREAM_FAILED: 'orange',
        SKIPPED: 'pink',
    }

    @classmethod
    def color(cls, state):
        return cls.state_color[state]

    @classmethod
    def runnable(cls):
        return [
            None, cls.FAILED, cls.UP_FOR_RETRY, cls.UPSTREAM_FAILED,
            cls.SKIPPED]


def pessimistic_connection_handling():
    @event.listens_for(Pool, "checkout")
    def ping_connection(dbapi_connection, connection_record, connection_proxy):
        '''
        Disconnect Handling - Pessimistic, taken from:
        http://docs.sqlalchemy.org/en/rel_0_9/core/pooling.html
        '''
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("SELECT 1")
        except:
            raise exc.DisconnectionError()
        cursor.close()


def initdb():
    from airflow import models
    logging.info("Creating all tables")
    models.Base.metadata.create_all(settings.engine)

    # Creating the local_mysql DB connection
    C = models.Connection
    session = settings.Session()

    conn = session.query(C).filter(C.conn_id == 'local_mysql').first()
    if not conn:
        session.add(
            models.Connection(
                conn_id='local_mysql', conn_type='mysql',
                host='localhost', login='airflow', password='airflow',
                schema='airflow'))
        session.commit()

    conn = session.query(C).filter(C.conn_id == 'presto_default').first()
    if not conn:
        session.add(
            models.Connection(
                conn_id='presto_default', conn_type='presto',
                host='localhost',
                schema='hive', port=3400))
        session.commit()

    conn = session.query(C).filter(C.conn_id == 'hive_cli_default').first()
    if not conn:
        session.add(
            models.Connection(
                conn_id='hive_cli_default', conn_type='hive_cli',
                schema='default',))
        session.commit()

    conn = session.query(C).filter(C.conn_id == 'hiveserver2_default').first()
    if not conn:
        session.add(
            models.Connection(
                conn_id='hiveserver2_default', conn_type='hiveserver2',
                host='localhost',
                schema='default', port=10000))
        session.commit()

    conn = session.query(C).filter(C.conn_id == 'metastore_default').first()
    if not conn:
        session.add(
            models.Connection(
                conn_id='metastore_default', conn_type='hive_metastore',
                host='localhost',
                port=10001))
        session.commit()

    conn = session.query(C).filter(C.conn_id == 'mysql_default').first()
    if not conn:
        session.add(
            models.Connection(
                conn_id='mysql_default', conn_type='mysql',
                host='localhost'))
        session.commit()

    conn = session.query(C).filter(C.conn_id == 'sqlite_default').first()
    if not conn:
        home = conf.get('core', 'AIRFLOW_HOME')
        session.add(
            models.Connection(
                conn_id='sqlite_default', conn_type='sqlite',
                host='{}/sqlite_default.db'.format(home)))
        session.commit()

    conn = session.query(C).filter(C.conn_id == 'http_default').first()
    if not conn:
        home = conf.get('core', 'AIRFLOW_HOME')
        session.add(
            models.Connection(
                conn_id='http_default', conn_type='http',
                host='http://www.google.com'))
        session.commit()

    conn = session.query(C).filter(C.conn_id == 'mssql_default').first()
    if not conn:
        session.add(
            models.Connection(
                conn_id='mssql_default', conn_type='mssql',
                host='localhost', port=1433))
        session.commit()

    # Known event types
    KET = models.KnownEventType
    if not session.query(KET).filter(KET.know_event_type == 'Holiday').first():
        session.add(KET(know_event_type='Holiday'))
    if not session.query(KET).filter(KET.know_event_type == 'Outage').first():
        session.add(KET(know_event_type='Outage'))
    if not session.query(KET).filter(
            KET.know_event_type == 'Natural Disaster').first():
        session.add(KET(know_event_type='Natural Disaster'))
    if not session.query(KET).filter(
            KET.know_event_type == 'Marketing Campaign').first():
        session.add(KET(know_event_type='Marketing Campaign'))
    session.commit()
    session.close()

    models.DagBag(sync_to_db=True)


def resetdb():
    '''
    Clear out the database
    '''
    from airflow import models

    logging.info("Dropping tables that exist")
    models.Base.metadata.drop_all(settings.engine)
    initdb()


def validate_key(k, max_length=250):
    if type(k) is not str:
        raise TypeError("The key has to be a string")
    elif len(k) > max_length:
        raise AirflowException("The key has to be less than {0} characters".format(
            max_length))
    elif not re.match(r'^[A-Za-z0-9_\-\.]+$', k):
        raise AirflowException(
            "The key ({k}) has to be made of alphanumeric characters, dashes, "
            "dots and underscores exclusively".format(**locals()))
    else:
        return True


def date_range(start_date, end_date=datetime.now(), delta=timedelta(1)):
    l = []
    if end_date >= start_date:
        while start_date <= end_date:
            l.append(start_date)
            start_date += delta
    else:
        raise AirflowException("start_date can't be after end_date")
    return l


def json_ser(obj):
    """
    json serializer that deals with dates
    usage: json.dumps(object, default=utils.json_ser)
    """
    if isinstance(obj, datetime):
        obj = obj.isoformat()
    return obj


def alchemy_to_dict(obj):
    """
    Transforms a SQLAlchemy model instance into a dictionary
    """
    if not obj:
        return None
    d = {}
    for c in obj.__table__.columns:
        value = getattr(obj, c.name)
        if type(value) == datetime:
            value = value.isoformat()
        d[c.name] = value
    return d


def readfile(filepath):
    f = open(filepath)
    content = f.read()
    f.close()
    return content


def provide_session(func):
    """
    Function decorator that provides a session if it isn't provided.
    If you want to reuse a session or run the function as part of a
    database transaction, you pass it to the function, if not this wrapper
    will create one and close it for you.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        needs_session = False
        if 'session' not in kwargs:
            needs_session = True
            session = settings.Session()
            kwargs['session'] = session
        result = func(*args, **kwargs)
        if needs_session:
            session.expunge_all()
            session.commit()
            session.close()
        return result
    return wrapper


def apply_defaults(func):
    """
    Function decorator that Looks for an argument named "default_args", and
    fills the unspecified arguments from it.

    Since python2.* isn't clear about which arguments are missing when
    calling a function, and that this can be quite confusing with multi-level
    inheritance and argument defaults, this decorator also alerts with
    specific information about the missing arguments.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        if len(args) > 1:
            raise AirflowException(
                "Use keyword arguments when initializing operators")
        dag_args = {}
        dag_params = {}
        if 'dag' in kwargs and kwargs['dag']:
            dag = kwargs['dag']
            dag_args = copy(dag.default_args) or {}
            dag_params = copy(dag.params) or {}

        params = {}
        if 'params' in kwargs:
            params = kwargs['params']
        dag_params.update(params)

        default_args = {}
        if 'default_args' in kwargs:
            default_args = kwargs['default_args']
            if 'params' in default_args:
                dag_params.update(default_args['params'])
                del default_args['params']

        dag_args.update(default_args)
        default_args = dag_args
        arg_spec = inspect.getargspec(func)
        num_defaults = len(arg_spec.defaults) if arg_spec.defaults else 0
        non_optional_args = arg_spec.args[:-num_defaults]
        if 'self' in non_optional_args:
            non_optional_args.remove('self')
        for arg in func.__code__.co_varnames:
            if arg in default_args and arg not in kwargs:
                kwargs[arg] = default_args[arg]
        missing_args = list(set(non_optional_args) - set(kwargs))
        if missing_args:
            msg = "Argument {0} is required".format(missing_args)
            raise AirflowException(msg)

        kwargs['params'] = dag_params

        result = func(*args, **kwargs)
        return result
    return wrapper


def ask_yesno(question):
    yes = set(['yes', 'y'])
    no = set(['no', 'n'])

    done = False
    print(question)
    while not done:
        choice = raw_input().lower()
        if choice in yes:
            return True
        elif choice in no:
            return False
        else:
            print("Please respond by yes or no.")


def send_email(to, subject, html_content):
    SMTP_MAIL_FROM = conf.get('smtp', 'SMTP_MAIL_FROM')

    if isinstance(to, unicode) or isinstance(to, str):
        if ',' in to:
            to = to.split(',')
        elif ';' in to:
            to = to.split(';')
        else:
            to = [to]

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = SMTP_MAIL_FROM
    msg['To'] = ", ".join(to)
    mime_text = MIMEText(html_content, 'html')
    msg.attach(mime_text)

    send_MIME_email(SMTP_MAIL_FROM, to, msg)


def send_MIME_email(e_from, e_to, mime_msg):
    SMTP_HOST = conf.get('smtp', 'SMTP_HOST')
    SMTP_PORT = conf.get('smtp', 'SMTP_PORT')
    SMTP_USER = conf.get('smtp', 'SMTP_USER')
    SMTP_PASSWORD = conf.get('smtp', 'SMTP_PASSWORD')
    SMTP_STARTTLS = conf.getboolean('smtp', 'SMTP_STARTTLS')

    s = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
    if SMTP_STARTTLS:
        s.starttls()
    if SMTP_USER and SMTP_PASSWORD:
        s.login(SMTP_USER, SMTP_PASSWORD)
    logging.info("Sent an alert email to " + str(e_to))
    s.sendmail(e_from, e_to, mime_msg.as_string())
    s.quit()


def import_module_attrs(parent_module_globals, module_attrs_dict):
    '''
    Attempts to import a set of modules and specified attributes in the
    form of a dictionary. The attributes are copied in the parent module's
    namespace. The function returns a list of attributes names that can be
    affected to __all__.

    This is used in the context of ``operators`` and ``hooks`` and
    silence the import errors for when libraries are missing. It makes
    for a clean package abstracting the underlying modules and only
    brings functional operators to those namespaces.
    '''
    imported_attrs = []
    for mod, attrs in module_attrs_dict.items():
        try:
            folder = os.path.dirname(parent_module_globals['__file__'])
            f, filename, description = imp.find_module(mod, [folder])
            module = imp.load_module(mod, f, filename, description)
            for attr in attrs:
                parent_module_globals[attr] = getattr(module, attr)
                imported_attrs += [attr]
        except:
            logging.debug("Couldn't import module " + mod)
    return imported_attrs


def is_in(obj, l):
    """
    Checks whether an object is one of the item in the list.
    This is different from ``in`` because ``in`` uses __cmp__ when
    present. Here we change based on the object itself
    """
    for item in l:
        if item is obj:
            return True
    return False


@contextmanager
def TemporaryDirectory(suffix='', prefix=None, dir=None):
    name = mkdtemp(suffix=suffix, prefix=prefix, dir=dir)
    try:
        yield name
    finally:
            try:
                shutil.rmtree(name)
            except OSError as e:
                # ENOENT - no such file or directory
                if e.errno != errno.ENOENT:
                    raise e


class AirflowTaskTimeout(Exception):
    pass


class timeout:
    """
    To be used in a ``with`` block and timeout its content.
    """
    def __init__(self, seconds=1, error_message='Timeout'):
        self.seconds = seconds
        self.error_message = error_message

    def handle_timeout(self, signum, frame):
        logging.error("Process timed out")
        raise AirflowTaskTimeout(self.error_message)

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback):
        signal.alarm(0)
