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
import traceback
import warnings
from importlib import metadata
from typing import TYPE_CHECKING, Any, Callable

import pluggy
from packaging.version import Version
from sqlalchemy import create_engine, exc, text
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import NullPool

from airflow import __version__ as airflow_version, policies
from airflow.configuration import AIRFLOW_HOME, WEBSERVER_CONFIG, conf  # noqa: F401
from airflow.exceptions import AirflowInternalRuntimeError
from airflow.executors import executor_constants
from airflow.logging_config import configure_logging
from airflow.utils.orm_event_handlers import setup_event_handlers
from airflow.utils.sqlalchemy import is_sqlalchemy_v1
from airflow.utils.state import State
from airflow.utils.timezone import local_timezone, parse_timezone, utc

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine
    from sqlalchemy.orm import Session as SASession

    from airflow.www.utils import UIAlert

log = logging.getLogger(__name__)

try:
    if (tz := conf.get_mandatory_value("core", "default_timezone")) != "system":
        TIMEZONE = parse_timezone(tz)
    else:
        TIMEZONE = local_timezone()
except Exception:
    TIMEZONE = utc

log.info("Configured default timezone %s", TIMEZONE)

if conf.has_option("database", "sql_alchemy_session_maker"):
    log.info(
        '[Warning] Found config "sql_alchemy_session_maker", make sure you know what you are doing.\n'
        "[Warning] Improper configuration of sql_alchemy_session_maker can lead to serious issues, "
        "including data corruption, unrecoverable application crashes.\n"
        "[Warning] Please review the SQLAlchemy documentation for detailed guidance on "
        "proper configuration and best practices."
    )

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
    """Print rich and visible warnings."""
    # Delay imports until we need it
    import re2
    from rich.markup import escape

    re2_escape_regex = re2.compile(r"(\\*)(\[[a-z#/@][^[]*?])").sub
    msg = f"[bold]{line}" if line else f"[bold][yellow]{filename}:{lineno}"
    msg += f" {category.__name__}[/bold]: {escape(str(message), _escape=re2_escape_regex)}[/yellow]"
    write_console = _get_rich_console(file or sys.stderr)
    write_console.print(msg, soft_wrap=True)


def replace_showwarning(replacement):
    """
    Replace ``warnings.showwarning``, returning the original.

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


def configure_policy_plugin_manager():
    global POLICY_PLUGIN_MANAGER

    POLICY_PLUGIN_MANAGER = pluggy.PluginManager(policies.local_settings_hookspec.project_name)
    POLICY_PLUGIN_MANAGER.add_hookspecs(policies)
    POLICY_PLUGIN_MANAGER.register(policies.DefaultPolicy)


def load_policy_plugins(pm: pluggy.PluginManager):
    # We can't log duration etc  here, as logging hasn't yet been configured!
    pm.load_setuptools_entrypoints("airflow.policy")


def configure_vars():
    """Configure Global Variables from airflow.cfg."""
    global SQL_ALCHEMY_CONN
    global DAGS_FOLDER
    global PLUGINS_FOLDER
    global DONOT_MODIFY_HANDLERS
    SQL_ALCHEMY_CONN = conf.get("database", "SQL_ALCHEMY_CONN")

    DAGS_FOLDER = os.path.expanduser(conf.get("core", "DAGS_FOLDER"))

    PLUGINS_FOLDER = conf.get("core", "plugins_folder", fallback=os.path.join(AIRFLOW_HOME, "plugins"))

    # If donot_modify_handlers=True, we do not modify logging handlers in task_run command
    # If the flag is set to False, we remove all handlers from the root logger
    # and add all handlers from 'airflow.task' logger to the root Logger. This is done
    # to get all the logs from the print & log statements in the DAG files before a task is run
    # The handlers are restored after the task completes execution.
    DONOT_MODIFY_HANDLERS = conf.getboolean("logging", "donot_modify_handlers", fallback=False)


def _run_openlineage_runtime_check():
    """
    Ensure compatibility of OpenLineage provider package and Airflow version.

    Airflow 2.10.0 introduced some core changes (#39336) that made versions <= 1.8.0 of OpenLineage
    provider incompatible with future Airflow versions (>= 2.10.0).
    """
    ol_package = "apache-airflow-providers-openlineage"
    try:
        ol_version = metadata.version(ol_package)
    except metadata.PackageNotFoundError:
        return

    if ol_version and Version(ol_version) < Version("1.8.0.dev0"):
        raise RuntimeError(
            f"You have installed `{ol_package}` == `{ol_version}` that is not compatible with "
            f"`apache-airflow` == `{airflow_version}`. "
            f"For `apache-airflow` >= `2.10.0` you must use `{ol_package}` >= `1.8.0`."
        )


def run_providers_custom_runtime_checks():
    _run_openlineage_runtime_check()


class SkipDBTestsSession:
    """
    This fake session is used to skip DB tests when `_AIRFLOW_SKIP_DB_TESTS` is set.

    :meta private:
    """

    def __init__(self):
        raise AirflowInternalRuntimeError(
            "Your test accessed the DB but `_AIRFLOW_SKIP_DB_TESTS` is set.\n"
            "Either make sure your test does not use database or mark the test with `@pytest.mark.db_test`\n"
            "See https://github.com/apache/airflow/blob/main/contributing-docs/testing/unit_tests.rst#"
            "best-practices-for-db-tests on how "
            "to deal with it and consult examples."
        )

    def remove(*args, **kwargs):
        pass

    def get_bind(
        self,
        mapper=None,
        clause=None,
        bind=None,
        _sa_skip_events=None,
        _sa_skip_for_implicit_returning=False,
    ):
        pass


def get_cleaned_traceback(stack_summary: traceback.StackSummary) -> str:
    clened_traceback = [
        frame
        for frame in stack_summary[:-2]
        if "/_pytest" not in frame.filename and "/pluggy" not in frame.filename
    ]
    return "".join(traceback.format_list(clened_traceback))


class TracebackSession:
    """
    Session that throws error when you try to use it.

    Also stores stack at instantiation call site.

    :meta private:
    """

    def __init__(self):
        self.traceback = traceback.extract_stack()

    def __getattr__(self, item):
        raise RuntimeError(
            "TracebackSession object was used but internal API is enabled. "
            "You'll need to ensure you are making only RPC calls with this object. "
            "The stack list below will show where the TracebackSession object was created."
            + get_cleaned_traceback(self.traceback)
        )

    def remove(*args, **kwargs):
        pass


AIRFLOW_PATH = os.path.dirname(os.path.dirname(__file__))
AIRFLOW_TESTS_PATH = os.path.join(AIRFLOW_PATH, "tests")
AIRFLOW_SETTINGS_PATH = os.path.join(AIRFLOW_PATH, "airflow", "settings.py")
AIRFLOW_UTILS_SESSION_PATH = os.path.join(AIRFLOW_PATH, "airflow", "utils", "session.py")
AIRFLOW_MODELS_BASEOPERATOR_PATH = os.path.join(AIRFLOW_PATH, "airflow", "models", "baseoperator.py")


class TracebackSessionForTests:
    """
    Session that throws error when you try to create a session outside of the test code.

    When we run our tests in "db isolation" mode we expect that "airflow" code will never create
    a session on its own and internal_api server is used for all calls but the test code might use
    the session to setup and teardown in the DB so that the internal API server accesses it.

    :meta private:
    """

    db_session_class = None
    allow_db_access = False
    """For pytests to create/prepare stuff where explicit DB access it needed"""

    def __init__(self):
        self.current_db_session = TracebackSessionForTests.db_session_class()
        self.created_traceback = traceback.extract_stack()

    def __getattr__(self, item):
        test_code, frame_summary = self.is_called_from_test_code()
        if self.allow_db_access or test_code:
            return getattr(self.current_db_session, item)
        raise RuntimeError(
            "TracebackSessionForTests object was used but internal API is enabled. "
            "Only test code is allowed to use this object.\n"
            f"Called from:\n    {frame_summary.filename}: {frame_summary.lineno}\n"
            f"     {frame_summary.line}\n\n"
            "You'll need to ensure you are making only RPC calls with this object. "
            "The stack list below will show where the TracebackSession object was called:\n"
            + get_cleaned_traceback(self.traceback)
            + "\n\nThe stack list below will show where the TracebackSession object was created:\n"
            + get_cleaned_traceback(self.created_traceback)
        )

    def remove(*args, **kwargs):
        pass

    @staticmethod
    def set_allow_db_access(session, flag: bool):
        """Temporarily, e.g. for pytests allow access to DB to prepare stuff."""
        if isinstance(session, TracebackSessionForTests):
            session.allow_db_access = flag

    def is_called_from_test_code(self) -> tuple[bool, traceback.FrameSummary | None]:
        """
        Check if the traceback session was used from the test code.

        This is done by checking if the first "airflow" filename in the traceback
        is "airflow/tests" or "regular airflow".

        :meta: private
        :return: True if the object was created from test code, False otherwise.
        """
        self.traceback = traceback.extract_stack()
        airflow_frames = [
            tb
            for tb in self.traceback
            if tb.filename.startswith(AIRFLOW_PATH)
            and not tb.filename == AIRFLOW_SETTINGS_PATH
            and not tb.filename == AIRFLOW_UTILS_SESSION_PATH
        ]
        if any(
            filename.endswith("conftest.py")
            or filename.endswith("dev/airflow_common_pytest/test_utils/db.py")
            for filename, _, _, _ in airflow_frames
        ):
            # This is a fixture call or testing utilities
            return True, None
        if (
            len(airflow_frames) >= 2
            and airflow_frames[-2].filename.startswith(AIRFLOW_TESTS_PATH)
            and airflow_frames[-1].filename == AIRFLOW_MODELS_BASEOPERATOR_PATH
            and airflow_frames[-1].name == "run"
        ):
            # This is baseoperator run method that is called directly from the test code and this is
            # usual pattern where we create a session in the test code to create dag_runs for tests.
            # If `run` code will be run inside a real "airflow" code the stack trace would be longer
            # and it would not be directly called from the test code. Also if subsequently any of the
            # run_task() method called later from the task code will attempt to execute any DB
            # method, the stack trace will be longer and we will catch it as "illegal" call.
            return True, None
        for tb in airflow_frames[::-1]:
            if tb.filename.startswith(AIRFLOW_PATH):
                if tb.filename.startswith(AIRFLOW_TESTS_PATH):
                    # this is a session created directly in the test code
                    return True, None
                else:
                    return False, tb
        # if it is from elsewhere.... Why???? We should return False in order to crash to find out
        # The traceback line will be always 3rd (two bottom ones are Airflow)
        return False, self.traceback[-2]


def _is_sqlite_db_path_relative(sqla_conn_str: str) -> bool:
    """Determine whether the database connection URI specifies a relative path."""
    # Check for non-empty connection string:
    if not sqla_conn_str:
        return False
    # Check for the right URI scheme:
    if not sqla_conn_str.startswith("sqlite"):
        return False
    # In-memory is not useful for production, but useful for writing tests against Airflow for extensions
    if sqla_conn_str == "sqlite://":
        return False
    # Check for absolute path:
    if sqla_conn_str.startswith(abs_prefix := "sqlite:///") and os.path.isabs(
        sqla_conn_str[len(abs_prefix) :]
    ):
        return False
    return True


def configure_orm(disable_connection_pool=False, pool_class=None):
    """Configure ORM using SQLAlchemy."""
    from airflow.utils.log.secrets_masker import mask_secret

    if _is_sqlite_db_path_relative(SQL_ALCHEMY_CONN):
        from airflow.exceptions import AirflowConfigException

        raise AirflowConfigException(
            f"Cannot use relative path: `{SQL_ALCHEMY_CONN}` to connect to sqlite. "
            "Please use absolute path such as `sqlite:////tmp/airflow.db`."
        )

    global Session
    global engine
    if os.environ.get("_AIRFLOW_SKIP_DB_TESTS") == "true":
        # Skip DB initialization in unit tests, if DB tests are skipped
        Session = SkipDBTestsSession
        engine = None
        return
    if conf.get("database", "sql_alchemy_conn") == "none://":
        from airflow.api_internal.internal_api_call import InternalApiConfig

        InternalApiConfig.set_use_internal_api("ORM reconfigured in forked process.")
        return
    log.debug("Setting up DB connection pool (PID %s)", os.getpid())
    engine_args = prepare_engine_args(disable_connection_pool, pool_class)

    if conf.has_option("database", "sql_alchemy_connect_args"):
        connect_args = conf.getimport("database", "sql_alchemy_connect_args")
    else:
        connect_args = {}

    engine = create_engine(SQL_ALCHEMY_CONN, connect_args=connect_args, **engine_args, future=True)

    mask_secret(engine.url.password)

    setup_event_handlers(engine)

    if conf.has_option("database", "sql_alchemy_session_maker"):
        _session_maker = conf.getimport("database", "sql_alchemy_session_maker")
    else:

        def _session_maker(_engine):
            return sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=_engine,
                expire_on_commit=False,
            )

    Session = scoped_session(_session_maker(engine))


def force_traceback_session_for_untrusted_components(allow_tests_to_use_db=False):
    log.info("Forcing TracebackSession for untrusted components.")
    global Session
    global engine
    if allow_tests_to_use_db:
        old_session_class = Session
        Session = TracebackSessionForTests
        TracebackSessionForTests.db_session_class = old_session_class
    else:
        try:
            dispose_orm()
        except NameError:
            # This exception might be thrown in case the ORM has not been initialized yet.
            pass
        else:
            Session = TracebackSession
        engine = None


DEFAULT_ENGINE_ARGS = {
    "postgresql": {
        "executemany_mode": "values_plus_batch",
        "executemany_values_page_size" if is_sqlalchemy_v1() else "insertmanyvalues_page_size": 10000,
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

    engine_args: dict = conf.getjson("database", "sql_alchemy_engine_args", fallback=default_args)  # type: ignore

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
        # https://docs.sqlalchemy.org/en/14/core/pooling.html#disconnect-handling-pessimistic
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

    if SQL_ALCHEMY_CONN.startswith("mysql"):
        engine_args["isolation_level"] = "READ COMMITTED"

    if is_sqlalchemy_v1():
        # Allow the user to specify an encoding for their DB otherwise default
        # to utf-8 so jobs & users with non-latin1 characters can still use us.
        # This parameter was removed in SQLAlchemy 2.x.
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

    worker_precheck = conf.getboolean("celery", "worker_precheck")
    if not worker_precheck:
        return True
    else:
        check_session = sessionmaker(bind=engine)
        session = check_session()
        try:
            session.execute(text("select 1"))
            conn_status = True
        except exc.DBAPIError as err:
            log.error(err)
            conn_status = False
        session.close()
        return conn_status


def configure_action_logging() -> None:
    """Any additional configuration (register callback) for airflow.utils.action_loggers module."""


def prepare_syspath_for_config_and_plugins():
    """Update sys.path for the config and plugins directories."""
    # Add ./config/ for loading custom log parsers etc, or
    # airflow_local_settings etc.
    config_path = os.path.join(AIRFLOW_HOME, "config")
    if config_path not in sys.path:
        sys.path.append(config_path)

    if PLUGINS_FOLDER not in sys.path:
        sys.path.append(PLUGINS_FOLDER)


def prepare_syspath_for_dags_folder():
    """Update sys.path to include the DAGs folder."""
    if DAGS_FOLDER not in sys.path:
        sys.path.append(DAGS_FOLDER)


def get_session_lifetime_config():
    """Get session timeout configs and handle outdated configs gracefully."""
    session_lifetime_minutes = conf.get("webserver", "session_lifetime_minutes", fallback=None)
    minutes_per_day = 24 * 60
    if not session_lifetime_minutes:
        session_lifetime_days = 30
        session_lifetime_minutes = minutes_per_day * session_lifetime_days

    log.debug("User session lifetime is set to %s minutes.", session_lifetime_minutes)

    return int(session_lifetime_minutes)


def import_local_settings():
    """Import airflow_local_settings.py files to allow overriding any configs in settings.py file."""
    try:
        import airflow_local_settings
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
    else:
        if hasattr(airflow_local_settings, "__all__"):
            names = set(airflow_local_settings.__all__)
        else:
            names = {n for n in airflow_local_settings.__dict__ if not n.startswith("__")}

        plugin_functions = policies.make_plugin_from_local_settings(
            POLICY_PLUGIN_MANAGER, airflow_local_settings, names
        )

        # If we have already handled a function by adding it to the plugin,
        # then don't clobber the global function
        for name in names - plugin_functions:
            globals()[name] = getattr(airflow_local_settings, name)

        if POLICY_PLUGIN_MANAGER.hook.task_instance_mutation_hook.get_hookimpls():
            task_instance_mutation_hook.is_noop = False

        log.info("Loaded airflow_local_settings from %s .", airflow_local_settings.__file__)


def initialize():
    """Initialize Airflow with all the settings from this file."""
    configure_vars()
    prepare_syspath_for_config_and_plugins()
    configure_policy_plugin_manager()
    # Load policy plugins _before_ importing airflow_local_settings, as Pluggy uses LIFO and we want anything
    # in airflow_local_settings to take precendec
    load_policy_plugins(POLICY_PLUGIN_MANAGER)
    import_local_settings()
    prepare_syspath_for_dags_folder()
    global LOGGING_CLASS_PATH
    LOGGING_CLASS_PATH = configure_logging()
    State.state_color.update(STATE_COLORS)

    configure_adapters()
    # The webservers import this file from models.py with the default settings.
    configure_orm()
    configure_action_logging()

    # mask the sensitive_config_values
    conf.mask_secrets()

    # Run any custom runtime checks that needs to be executed for providers
    run_providers_custom_runtime_checks()

    # Ensure we close DB connections at scheduler and gunicorn worker terminations
    atexit.register(dispose_orm)


def is_usage_data_collection_enabled() -> bool:
    """Check if data collection is enabled."""
    return conf.getboolean("usage_data_collection", "enabled", fallback=True) and (
        os.getenv("SCARF_ANALYTICS", "").strip().lower() != "false"
    )


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

USE_JOB_SCHEDULE = conf.getboolean("scheduler", "use_job_schedule", fallback=True)

# By default Airflow plugins are lazily-loaded (only loaded when required). Set it to False,
# if you want to load plugins whenever 'airflow' is invoked via cli or loaded from module.
LAZY_LOAD_PLUGINS: bool = conf.getboolean("core", "lazy_load_plugins", fallback=True)

# By default Airflow providers are lazily-discovered (discovery and imports happen only when required).
# Set it to False, if you want to discover providers whenever 'airflow' is invoked via cli or
# loaded from module.
LAZY_LOAD_PROVIDERS: bool = conf.getboolean("core", "lazy_discover_providers", fallback=True)

# Determines if the executor utilizes Kubernetes
IS_K8S_OR_K8SCELERY_EXECUTOR = conf.get("core", "EXECUTOR") in {
    executor_constants.KUBERNETES_EXECUTOR,
    executor_constants.CELERY_KUBERNETES_EXECUTOR,
    executor_constants.LOCAL_KUBERNETES_EXECUTOR,
}

# Executors can set this to true to configure logging correctly for
# containerized executors.
IS_EXECUTOR_CONTAINER = bool(os.environ.get("AIRFLOW_IS_EXECUTOR_CONTAINER", ""))
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

# AIP-44: internal_api (experimental)
# This feature is not complete yet, so we disable it by default.
_ENABLE_AIP_44: bool = os.environ.get("AIRFLOW_ENABLE_AIP_44", "false").lower() in {
    "true",
    "t",
    "yes",
    "y",
    "1",
}
