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

import atexit
import functools
import json
import logging
import os
import sys
import warnings
from typing import TYPE_CHECKING, Any, Callable

import pendulum
import pluggy
import sqlalchemy
from sqlalchemy import create_engine, exc
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session as SASession, scoped_session, sessionmaker
from sqlalchemy.pool import NullPool

from airflow import policies
from airflow.configuration import AIRFLOW_HOME, WEBSERVER_CONFIG, conf  # NOQA F401
from airflow.exceptions import RemovedInAirflow3Warning
from airflow.executors import executor_constants
from airflow.logging_config import configure_logging
from airflow.utils.orm_event_handlers import setup_event_handlers
from airflow.utils.state import State

if TYPE_CHECKING:
    from airflow.www.utils import UIAlert

log = logging.getLogger(__name__)


TIMEZONE = pendulum.tz.timezone("UTC")
try:
    tz = conf.get_mandatory_value("core", "default_timezone")
    if tz == "system":
        TIMEZONE = pendulum.tz.local_timezone()
    else:
        TIMEZONE = pendulum.tz.timezone(tz)
except Exception:
    pass
log.info("Configured default timezone %s", TIMEZONE)


HEADER = "\n".join(
    [
        r"  ____________       _____________",
        r" ____    |__( )_________  __/__  /________      __",
        r"____  /| |_  /__  ___/_  /_ __  /_  __ \_ | /| / /",
        r"___  ___ |  / _  /   _  __/ _  / / /_/ /_ |/ |/ /",
        r" _/_/  |_/_/  /_/    /_/    /_/  \____/____/|__/",
    ]
)

LOGGING_LEVEL = logging.INFO

# the prefix to append to gunicorn worker processes after init
GUNICORN_WORKER_READY_PREFIX = "[ready] "

LOG_FORMAT = conf.get("logging", "log_format")
SIMPLE_LOG_FORMAT = conf.get("logging", "simple_log_format")

SQL_ALCHEMY_CONN: str | None = None
PLUGINS_FOLDER: str | None = None
LOGGING_CLASS_PATH: str | None = None
DONOT_MODIFY_HANDLERS: bool | None = None
DAGS_FOLDER: str = os.path.expanduser(conf.get_mandatory_value("core", "DAGS_FOLDER"))

engine: Engine
Session: Callable[..., SASession]

# The JSON library to use for DAG Serialization and De-Serialization
json = json

# Dictionary containing State and colors associated to each state to
# display on the Webserver
STATE_COLORS = {
    "deferred": "mediumpurple",
    "failed": "red",
    "queued": "gray",
    "removed": "lightgrey",
    "restarting": "violet",
    "running": "lime",
    "scheduled": "tan",
    "shutdown": "blue",
    "skipped": "hotpink",
    "success": "green",
    "up_for_reschedule": "turquoise",
    "up_for_retry": "gold",
    "upstream_failed": "orange",
}


@functools.lru_cache(maxsize=None)
def _get_rich_console(file):
    # Delay imports until we need it
    import rich.console

    return rich.console.Console(file=file)


def custom_show_warning(message, category, filename, lineno, file=None, line=None):
    """Custom function to print rich and visible warnings."""
    # Delay imports until we need it
    from rich.markup import escape

    msg = f"[bold]{line}" if line else f"[bold][yellow]{filename}:{lineno}"
    msg += f" {category.__name__}[/bold]: {escape(str(message))}[/yellow]"
    write_console = _get_rich_console(file or sys.stderr)
    write_console.print(msg, soft_wrap=True)


def replace_showwarning(replacement):
    """Replace ``warnings.showwarning``, returning the original.

    This is useful since we want to "reset" the ``showwarning`` hook on exit to
    avoid lazy-loading issues. If a warning is emitted after Python cleaned up
    the import system, we would no longer be able to import ``rich``.
    """
    original = warnings.showwarning
    warnings.showwarning = replacement
    return original


original_show_warning = replace_showwarning(custom_show_warning)
atexit.register(functools.partial(replace_showwarning, original_show_warning))

POLICY_PLUGIN_MANAGER: Any = None  # type: ignore


def task_policy(task):
    return POLICY_PLUGIN_MANAGER.hook.task_policy(task=task)


def dag_policy(dag):
    return POLICY_PLUGIN_MANAGER.hook.dag_policy(dag=dag)


def task_instance_mutation_hook(task_instance):
    return POLICY_PLUGIN_MANAGER.hook.task_instance_mutation_hook(task_instance=task_instance)


task_instance_mutation_hook.is_noop = True  # type: ignore


def pod_mutation_hook(pod):
    return POLICY_PLUGIN_MANAGER.hook.pod_mutation_hook(pod=pod)


def get_airflow_context_vars(context):
    return POLICY_PLUGIN_MANAGER.hook.get_airflow_context_vars(context=context)


def get_dagbag_import_timeout(dag_file_path: str):
    return POLICY_PLUGIN_MANAGER.hook.get_dagbag_import_timeout(dag_file_path=dag_file_path)


def load_policy_plugins(pm: pluggy.PluginManager):
    # We can't log duration etc  here, as logging hasn't yet been configured!
    pm.load_setuptools_entrypoints("airflow.policy")


def configure_vars():
    """Configure Global Variables from airflow.cfg."""
    global SQL_ALCHEMY_CONN
    global DAGS_FOLDER
    global PLUGINS_FOLDER
    global DONOT_MODIFY_HANDLERS
    global POLICY_PLUGIN_MANAGER
    SQL_ALCHEMY_CONN = conf.get("database", "SQL_ALCHEMY_CONN")
    DAGS_FOLDER = os.path.expanduser(conf.get("core", "DAGS_FOLDER"))

    PLUGINS_FOLDER = conf.get("core", "plugins_folder", fallback=os.path.join(AIRFLOW_HOME, "plugins"))

    # If donot_modify_handlers=True, we do not modify logging handlers in task_run command
    # If the flag is set to False, we remove all handlers from the root logger
    # and add all handlers from 'airflow.task' logger to the root Logger. This is done
    # to get all the logs from the print & log statements in the DAG files before a task is run
    # The handlers are restored after the task completes execution.
    DONOT_MODIFY_HANDLERS = conf.getboolean("logging", "donot_modify_handlers", fallback=False)

    POLICY_PLUGIN_MANAGER = pluggy.PluginManager(policies.local_settings_hookspec.project_name)
    POLICY_PLUGIN_MANAGER.add_hookspecs(policies)
    POLICY_PLUGIN_MANAGER.register(policies.DefaultPolicy)


def configure_orm(disable_connection_pool=False, pool_class=None):
    """Configure ORM using SQLAlchemy."""
    from airflow.utils.log.secrets_masker import mask_secret

    log.debug("Setting up DB connection pool (PID %s)", os.getpid())
    global engine
    global Session
    engine_args = prepare_engine_args(disable_connection_pool, pool_class)

    if conf.has_option("database", "sql_alchemy_connect_args"):
        connect_args = conf.getimport("database", "sql_alchemy_connect_args")
    else:
        connect_args = {}

    engine = create_engine(SQL_ALCHEMY_CONN, connect_args=connect_args, **engine_args)

    mask_secret(engine.url.password)

    setup_event_handlers(engine)

    Session = scoped_session(
        sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=engine,
            expire_on_commit=False,
        )
    )
    if engine.dialect.name == "mssql":
        session = Session()
        try:
            result = session.execute(
                sqlalchemy.text(
                    "SELECT is_read_committed_snapshot_on FROM sys.databases WHERE name=:database_name"
                ),
                params={"database_name": engine.url.database},
            )
            data = result.fetchone()[0]
            if data != 1:
                log.critical("MSSQL database MUST have READ_COMMITTED_SNAPSHOT enabled.")
                log.critical("The database %s has it disabled.", engine.url.database)
                log.critical("This will cause random deadlocks, Refusing to start.")
                log.critical(
                    "See https://airflow.apache.org/docs/apache-airflow/stable/howto/"
                    "set-up-database.html#setting-up-a-mssql-database"
                )
                raise Exception("MSSQL database MUST have READ_COMMITTED_SNAPSHOT enabled.")
        finally:
            session.close()


DEFAULT_ENGINE_ARGS = {
    "postgresql": {
        "executemany_mode": "values",
        "executemany_values_page_size": 10000,
        "executemany_batch_page_size": 2000,
    },
}


def prepare_engine_args(disable_connection_pool=False, pool_class=None):
    """Prepare SQLAlchemy engine args."""
    default_args = {}
    for dialect, default in DEFAULT_ENGINE_ARGS.items():
        if SQL_ALCHEMY_CONN.startswith(dialect):
            default_args = default.copy()
            break

    engine_args: dict = conf.getjson(
        "database", "sql_alchemy_engine_args", fallback=default_args
    )  # type: ignore

    if pool_class:
        # Don't use separate settings for size etc, only those from sql_alchemy_engine_args
        engine_args["poolclass"] = pool_class
    elif disable_connection_pool or not conf.getboolean("database", "SQL_ALCHEMY_POOL_ENABLED"):
        engine_args["poolclass"] = NullPool
        log.debug("settings.prepare_engine_args(): Using NullPool")
    elif not SQL_ALCHEMY_CONN.startswith("sqlite"):
        # Pool size engine args not supported by sqlite.
        # If no config value is defined for the pool size, select a reasonable value.
        # 0 means no limit, which could lead to exceeding the Database connection limit.
        pool_size = conf.getint("database", "SQL_ALCHEMY_POOL_SIZE", fallback=5)

        # The maximum overflow size of the pool.
        # When the number of checked-out connections reaches the size set in pool_size,
        # additional connections will be returned up to this limit.
        # When those additional connections are returned to the pool, they are disconnected and discarded.
        # It follows then that the total number of simultaneous connections
        # the pool will allow is pool_size + max_overflow,
        # and the total number of "sleeping" connections the pool will allow is pool_size.
        # max_overflow can be set to -1 to indicate no overflow limit;
        # no limit will be placed on the total number
        # of concurrent connections. Defaults to 10.
        max_overflow = conf.getint("database", "SQL_ALCHEMY_MAX_OVERFLOW", fallback=10)

        # The DB server already has a value for wait_timeout (number of seconds after
        # which an idle sleeping connection should be killed). Since other DBs may
        # co-exist on the same server, SQLAlchemy should set its
        # pool_recycle to an equal or smaller value.
        pool_recycle = conf.getint("database", "SQL_ALCHEMY_POOL_RECYCLE", fallback=1800)

        # Check connection at the start of each connection pool checkout.
        # Typically, this is a simple statement like "SELECT 1", but may also make use
        # of some DBAPI-specific method to test the connection for liveness.
        # More information here:
        # https://docs.sqlalchemy.org/en/13/core/pooling.html#disconnect-handling-pessimistic
        pool_pre_ping = conf.getboolean("database", "SQL_ALCHEMY_POOL_PRE_PING", fallback=True)

        log.debug(
            "settings.prepare_engine_args(): Using pool settings. pool_size=%d, max_overflow=%d, "
            "pool_recycle=%d, pid=%d",
            pool_size,
            max_overflow,
            pool_recycle,
            os.getpid(),
        )
        engine_args["pool_size"] = pool_size
        engine_args["pool_recycle"] = pool_recycle
        engine_args["pool_pre_ping"] = pool_pre_ping
        engine_args["max_overflow"] = max_overflow

    # The default isolation level for MySQL (REPEATABLE READ) can introduce inconsistencies when
    # running multiple schedulers, as repeated queries on the same session may read from stale snapshots.
    # 'READ COMMITTED' is the default value for PostgreSQL.
    # More information here:
    # https://dev.mysql.com/doc/refman/8.0/en/innodb-transaction-isolation-levels.html"

    # Similarly MSSQL default isolation level should be set to READ COMMITTED.
    # We also make sure that READ_COMMITTED_SNAPSHOT option is on, in order to avoid deadlocks when
    # Select queries are running. This is by default enforced during init/upgrade. More information:
    # https://docs.microsoft.com/en-us/sql/t-sql/statements/set-transaction-isolation-level-transact-sql

    if SQL_ALCHEMY_CONN.startswith(("mysql", "mssql")):
        engine_args["isolation_level"] = "READ COMMITTED"

    # Allow the user to specify an encoding for their DB otherwise default
    # to utf-8 so jobs & users with non-latin1 characters can still use us.
    engine_args["encoding"] = conf.get("database", "SQL_ENGINE_ENCODING", fallback="utf-8")

    return engine_args


def dispose_orm():
    """Properly close pooled database connections."""
    log.debug("Disposing DB connection pool (PID %s)", os.getpid())
    global engine
    global Session

    if Session is not None:  # type: ignore[truthy-function]
        Session.remove()
        Session = None
    if engine:
        engine.dispose()
        engine = None


def reconfigure_orm(disable_connection_pool=False, pool_class=None):
    """Properly close database connections and re-configure ORM."""
    dispose_orm()
    configure_orm(disable_connection_pool=disable_connection_pool, pool_class=pool_class)


def configure_adapters():
    """Register Adapters and DB Converters."""
    from pendulum import DateTime as Pendulum

    if SQL_ALCHEMY_CONN.startswith("sqlite"):
        from sqlite3 import register_adapter

        register_adapter(Pendulum, lambda val: val.isoformat(" "))

    if SQL_ALCHEMY_CONN.startswith("mysql"):
        try:
            import MySQLdb.converters

            MySQLdb.converters.conversions[Pendulum] = MySQLdb.converters.DateTime2literal
        except ImportError:
            pass
        try:
            import pymysql.converters

            pymysql.converters.conversions[Pendulum] = pymysql.converters.escape_datetime
        except ImportError:
            pass


def validate_session():
    """Validate ORM Session."""
    global engine

    worker_precheck = conf.getboolean("celery", "worker_precheck", fallback=False)
    if not worker_precheck:
        return True
    else:
        check_session = sessionmaker(bind=engine)
        session = check_session()
        try:
            session.execute("select 1")
            conn_status = True
        except exc.DBAPIError as err:
            log.error(err)
            conn_status = False
        session.close()
        return conn_status


def configure_action_logging() -> None:
    """Any additional configuration (register callback) for airflow.utils.action_loggers module."""


def prepare_syspath():
    """Ensure certain subfolders of AIRFLOW_HOME are on the classpath."""
    if DAGS_FOLDER not in sys.path:
        sys.path.append(DAGS_FOLDER)

    # Add ./config/ for loading custom log parsers etc, or
    # airflow_local_settings etc.
    config_path = os.path.join(AIRFLOW_HOME, "config")
    if config_path not in sys.path:
        sys.path.append(config_path)

    if PLUGINS_FOLDER not in sys.path:
        sys.path.append(PLUGINS_FOLDER)


def get_session_lifetime_config():
    """Gets session timeout configs and handles outdated configs gracefully."""
    session_lifetime_minutes = conf.get("webserver", "session_lifetime_minutes", fallback=None)
    session_lifetime_days = conf.get("webserver", "session_lifetime_days", fallback=None)
    uses_deprecated_lifetime_configs = session_lifetime_days or conf.get(
        "webserver", "force_log_out_after", fallback=None
    )

    minutes_per_day = 24 * 60
    default_lifetime_minutes = "43200"
    if uses_deprecated_lifetime_configs and session_lifetime_minutes == default_lifetime_minutes:
        warnings.warn(
            "`session_lifetime_days` option from `[webserver]` section has been "
            "renamed to `session_lifetime_minutes`. The new option allows to configure "
            "session lifetime in minutes. The `force_log_out_after` option has been removed "
            "from `[webserver]` section. Please update your configuration.",
            category=RemovedInAirflow3Warning,
        )
        if session_lifetime_days:
            session_lifetime_minutes = minutes_per_day * int(session_lifetime_days)

    if not session_lifetime_minutes:
        session_lifetime_days = 30
        session_lifetime_minutes = minutes_per_day * session_lifetime_days

    logging.debug("User session lifetime is set to %s minutes.", session_lifetime_minutes)

    return int(session_lifetime_minutes)


def import_local_settings():
    """Import airflow_local_settings.py files to allow overriding any configs in settings.py file."""
    try:
        import airflow_local_settings

        if hasattr(airflow_local_settings, "__all__"):
            names = list(airflow_local_settings.__all__)
        else:
            names = list(filter(lambda n: not n.startswith("__"), airflow_local_settings.__dict__.keys()))

        if "policy" in names and "task_policy" not in names:
            warnings.warn(
                "Using `policy` in airflow_local_settings.py is deprecated. "
                "Please rename your `policy` to `task_policy`.",
                RemovedInAirflow3Warning,
                stacklevel=2,
            )
            setattr(airflow_local_settings, "task_policy", airflow_local_settings.policy)
            names.remove("policy")

        plugin_functions = policies.make_plugin_from_local_settings(
            POLICY_PLUGIN_MANAGER, airflow_local_settings, names
        )

        for name in names:
            # If we have already handled a function by adding it to the plugin, then don't clobber the global
            # function
            if name in plugin_functions:
                continue

            globals()[name] = getattr(airflow_local_settings, name)

        if POLICY_PLUGIN_MANAGER.hook.task_instance_mutation_hook.get_hookimpls():
            task_instance_mutation_hook.is_noop = False

        log.info("Loaded airflow_local_settings from %s .", airflow_local_settings.__file__)
    except ModuleNotFoundError as e:
        if e.name == "airflow_local_settings":
            log.debug("No airflow_local_settings to import.", exc_info=True)
        else:
            log.critical(
                "Failed to import airflow_local_settings due to a transitive module not found error.",
                exc_info=True,
            )
            raise
    except ImportError:
        log.critical("Failed to import airflow_local_settings.", exc_info=True)
        raise


def initialize():
    """Initialize Airflow with all the settings from this file."""
    configure_vars()
    prepare_syspath()
    # Load policy plugins _before_ importing airflow_local_settings, as Pluggy uses LIFO and we want anything
    # in airflow_local_settings to take precendec
    load_policy_plugins(POLICY_PLUGIN_MANAGER)
    import_local_settings()
    global LOGGING_CLASS_PATH
    LOGGING_CLASS_PATH = configure_logging()
    State.state_color.update(STATE_COLORS)

    configure_adapters()
    # The webservers import this file from models.py with the default settings.
    configure_orm()
    configure_action_logging()

    # Ensure we close DB connections at scheduler and gunicorn worker terminations
    atexit.register(dispose_orm)


# Const stuff

KILOBYTE = 1024
MEGABYTE = KILOBYTE * KILOBYTE
WEB_COLORS = {"LIGHTBLUE": "#4d9de0", "LIGHTORANGE": "#FF9933"}


# Updating serialized DAG can not be faster than a minimum interval to reduce database
# write rate.
MIN_SERIALIZED_DAG_UPDATE_INTERVAL = conf.getint("core", "min_serialized_dag_update_interval", fallback=30)

# If set to True, serialized DAGs is compressed before writing to DB,
COMPRESS_SERIALIZED_DAGS = conf.getboolean("core", "compress_serialized_dags", fallback=False)

# Fetching serialized DAG can not be faster than a minimum interval to reduce database
# read rate. This config controls when your DAGs are updated in the Webserver
MIN_SERIALIZED_DAG_FETCH_INTERVAL = conf.getint("core", "min_serialized_dag_fetch_interval", fallback=10)

CAN_FORK = hasattr(os, "fork")

EXECUTE_TASKS_NEW_PYTHON_INTERPRETER = not CAN_FORK or conf.getboolean(
    "core",
    "execute_tasks_new_python_interpreter",
    fallback=False,
)

ALLOW_FUTURE_EXEC_DATES = conf.getboolean("scheduler", "allow_trigger_in_future", fallback=False)

# Whether or not to check each dagrun against defined SLAs
CHECK_SLAS = conf.getboolean("core", "check_slas", fallback=True)

USE_JOB_SCHEDULE = conf.getboolean("scheduler", "use_job_schedule", fallback=True)

# By default Airflow plugins are lazily-loaded (only loaded when required). Set it to False,
# if you want to load plugins whenever 'airflow' is invoked via cli or loaded from module.
LAZY_LOAD_PLUGINS = conf.getboolean("core", "lazy_load_plugins", fallback=True)

# By default Airflow providers are lazily-discovered (discovery and imports happen only when required).
# Set it to False, if you want to discover providers whenever 'airflow' is invoked via cli or
# loaded from module.
LAZY_LOAD_PROVIDERS = conf.getboolean("core", "lazy_discover_providers", fallback=True)

# Determines if the executor utilizes Kubernetes
IS_K8S_OR_K8SCELERY_EXECUTOR = conf.get("core", "EXECUTOR") in {
    executor_constants.KUBERNETES_EXECUTOR,
    executor_constants.CELERY_KUBERNETES_EXECUTOR,
    executor_constants.LOCAL_KUBERNETES_EXECUTOR,
}
IS_K8S_EXECUTOR_POD = bool(os.environ.get("AIRFLOW_IS_K8S_EXECUTOR_POD", ""))
"""Will be True if running in kubernetes executor pod."""

HIDE_SENSITIVE_VAR_CONN_FIELDS = conf.getboolean("core", "hide_sensitive_var_conn_fields")

# By default this is off, but is automatically configured on when running task
# instances
MASK_SECRETS_IN_LOGS = False

# Display alerts on the dashboard
# Useful for warning about setup issues or announcing changes to end users
# List of UIAlerts, which allows for specifying the message, category, and roles the
# message should be shown to. For example:
#   from airflow.www.utils import UIAlert
#
#   DASHBOARD_UIALERTS = [
#       UIAlert("Welcome to Airflow"),  # All users
#       UIAlert("Airflow update happening next week", roles=["User"]),  # Only users with the User role
#       # A flash message with html:
#       UIAlert('Visit <a href="http://airflow.apache.org">airflow.apache.org</a>', html=True),
#   ]
#
DASHBOARD_UIALERTS: list[UIAlert] = []

# Prefix used to identify tables holding data moved during migration.
AIRFLOW_MOVED_TABLE_PREFIX = "_airflow_moved"

DAEMON_UMASK: str = conf.get("core", "daemon_umask", fallback="0o077")
