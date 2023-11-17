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

import json
import os
import platform
import subprocess
import sys
import warnings
from contextlib import ExitStack, suppress
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
import time_machine
from _pytest.recwarn import WarningsRecorder

# We should set these before loading _any_ of the rest of airflow so that the
# unit test mode config is set as early as possible.
from itsdangerous import URLSafeSerializer

assert "airflow" not in sys.modules, "No airflow module can be imported before these lines"


DEFAULT_WARNING_OUTPUT_PATH = Path("warnings.txt")

warning_output_path = DEFAULT_WARNING_OUTPUT_PATH

# A bit of a Hack - but we need to check args before they are parsed by pytest in order to
# configure the DB before Airflow gets initialized (which happens at airflow import time).
# Using env variables also handles the case, when python-xdist is used - python-xdist spawns separate
# processes and does not pass all args to them (it's done via env variables) so we are doing the
# same here and detect whether `--skip-db-tests` or `--run-db-tests-only` is passed to pytest
# and set env variables so the processes spawned by python-xdist can read the status from there
skip_db_tests = "--skip-db-tests" in sys.argv or os.environ.get("_AIRFLOW_SKIP_DB_TESTS") == "true"
run_db_tests_only = (
    "--run-db-tests-only" in sys.argv or os.environ.get("_AIRFLOW_RUN_DB_TESTS_ONLY") == "true"
)

if skip_db_tests:
    if run_db_tests_only:
        raise Exception("You cannot specify both --skip-db-tests and --run-db-tests-only together")
    # Make sure sqlalchemy will not be usable for pure unit tests even if initialized
    os.environ["AIRFLOW__CORE__SQL_ALCHEMY_CONN"] = "bad_schema:///"
    os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = "bad_schema:///"
    # Force database isolation mode for pure unit tests
    os.environ["AIRFLOW__CORE__DATABASE_ACCESS_ISOLATION"] = "True"
    os.environ["_IN_UNIT_TESTS"] = "true"
    # Set it here to pass the flag to python-xdist spawned processes
    os.environ["_AIRFLOW_SKIP_DB_TESTS"] = "true"

if run_db_tests_only:
    # Set it here to pass the flag to python-xdist spawned processes
    os.environ["_AIRFLOW_RUN_DB_TESTS_ONLY"] = "true"

AIRFLOW_TESTS_DIR = Path(os.path.dirname(os.path.realpath(__file__))).resolve()
AIRFLOW_SOURCES_ROOT_DIR = AIRFLOW_TESTS_DIR.parent.parent

os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = os.fspath(AIRFLOW_TESTS_DIR / "dags")
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
os.environ["AWS_DEFAULT_REGION"] = os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"
os.environ["CREDENTIALS_DIR"] = os.environ.get("CREDENTIALS_DIR") or "/files/airflow-breeze-config/keys"
os.environ["AIRFLOW_ENABLE_AIP_44"] = os.environ.get("AIRFLOW_ENABLE_AIP_44") or "true"

if platform.system() == "Darwin":
    # mocks from unittest.mock work correctly in subprocesses only if they are created by "fork" method
    # but macOS uses "spawn" by default
    os.environ["AIRFLOW__CORE__MP_START_METHOD"] = "fork"

# Ignore files that are really test dags to be ignored by pytest
collect_ignore = [
    "tests/dags/subdir1/test_ignore_this.py",
    "tests/dags/test_invalid_dup_task.pyy",
    "tests/dags_corrupted/test_impersonation_custom.py",
    "tests/test_utils/perf/dags/elastic_dag.py",
]


@pytest.fixture()
def reset_environment():
    """Resets env variables."""
    init_env = os.environ.copy()
    yield
    changed_env = os.environ
    for key in changed_env:
        if key not in init_env:
            del os.environ[key]
        else:
            os.environ[key] = init_env[key]


@pytest.fixture()
def secret_key() -> str:
    """Return secret key configured."""
    from airflow.configuration import conf

    the_key = conf.get("webserver", "SECRET_KEY")
    if the_key is None:
        raise RuntimeError(
            "The secret key SHOULD be configured as `[webserver] secret_key` in the "
            "configuration/environment at this stage! "
        )
    return the_key


@pytest.fixture()
def url_safe_serializer(secret_key) -> URLSafeSerializer:
    return URLSafeSerializer(secret_key)


@pytest.fixture()
def reset_db():
    """Resets Airflow db."""

    from airflow.utils import db

    db.resetdb()
    yield


ALLOWED_TRACE_SQL_COLUMNS = ["num", "time", "trace", "sql", "parameters", "count"]


@pytest.fixture(autouse=True)
def trace_sql(request):
    from tests.test_utils.perf.perf_kit.sqlalchemy import (  # isort: skip
        count_queries,
        trace_queries,
    )

    """Displays queries from the tests to console."""
    trace_sql_option = request.config.option.trace_sql
    if not trace_sql_option:
        yield
        return

    terminal_reporter = request.config.pluginmanager.getplugin("terminalreporter")
    # if no terminal reporter plugin is present, nothing we can do here;
    # this can happen when this function executes in a worker node
    # when using pytest-xdist, for example
    if terminal_reporter is None:
        yield
        return

    columns = [col.strip() for col in trace_sql_option.split(",")]

    def pytest_print(text):
        return terminal_reporter.write_line(text)

    with ExitStack() as exit_stack:
        if columns == ["num"]:
            # It is very unlikely that the user wants to display only numbers, but probably
            # the user just wants to count the queries.
            exit_stack.enter_context(count_queries(print_fn=pytest_print))
        elif any(c in columns for c in ["time", "trace", "sql", "parameters"]):
            exit_stack.enter_context(
                trace_queries(
                    display_num="num" in columns,
                    display_time="time" in columns,
                    display_trace="trace" in columns,
                    display_sql="sql" in columns,
                    display_parameters="parameters" in columns,
                    print_fn=pytest_print,
                )
            )

        yield


def pytest_addoption(parser):
    """Add options parser for custom plugins."""
    group = parser.getgroup("airflow")
    group.addoption(
        "--with-db-init",
        action="store_true",
        dest="db_init",
        help="Forces database initialization before tests",
    )
    group.addoption(
        "--integration",
        action="append",
        dest="integration",
        metavar="INTEGRATIONS",
        help="only run tests matching integration specified: "
        "[cassandra,kerberos,mongo,celery,statsd,trino]. ",
    )
    group.addoption(
        "--skip-db-tests",
        action="store_true",
        dest="skip_db_tests",
        help="skip tests that require database",
    )
    group.addoption(
        "--run-db-tests-only",
        action="store_true",
        dest="run_db_tests_only",
        help="only run tests requiring database",
    )
    group.addoption(
        "--backend",
        action="store",
        dest="backend",
        metavar="BACKEND",
        help="only run tests matching the backend: [sqlite,postgres,mysql].",
    )
    group.addoption(
        "--system",
        action="append",
        dest="system",
        metavar="SYSTEMS",
        help="only run tests matching the system specified [google.cloud, google.marketing_platform]",
    )
    group.addoption(
        "--include-long-running",
        action="store_true",
        dest="include_long_running",
        help="Includes long running tests (marked with long_running marker). They are skipped by default.",
    )
    group.addoption(
        "--include-quarantined",
        action="store_true",
        dest="include_quarantined",
        help="Includes quarantined tests (marked with quarantined marker). They are skipped by default.",
    )
    group.addoption(
        "--exclude-virtualenv-operator",
        action="store_true",
        dest="exclude_virtualenv_operator",
        help="Excludes virtualenv operators tests (marked with virtualenv_test marker).",
    )
    group.addoption(
        "--exclude-external-python-operator",
        action="store_true",
        dest="exclude_external_python_operator",
        help="Excludes external python operator tests (marked with external_python_test marker).",
    )
    allowed_trace_sql_columns_list = ",".join(ALLOWED_TRACE_SQL_COLUMNS)
    group.addoption(
        "--trace-sql",
        action="store",
        dest="trace_sql",
        help=(
            "Trace SQL statements. As an argument, you must specify the columns to be "
            f"displayed as a comma-separated list. Supported values: [f{allowed_trace_sql_columns_list}]"
        ),
        metavar="COLUMNS",
    )
    group.addoption(
        "--no-db-cleanup",
        action="store_false",
        dest="db_cleanup",
        help="Disable DB clear before each test module.",
    )
    group.addoption(
        "--warning-output-path",
        action="store",
        dest="warning_output_path",
        default=DEFAULT_WARNING_OUTPUT_PATH.resolve().as_posix(),
    )


def initial_db_init():
    from flask import Flask

    from airflow.configuration import conf
    from airflow.utils import db
    from airflow.www.extensions.init_appbuilder import init_appbuilder
    from airflow.www.extensions.init_auth_manager import get_auth_manager

    db.resetdb()
    db.bootstrap_dagbag()
    # minimal app to add roles
    flask_app = Flask(__name__)
    flask_app.config["SQLALCHEMY_DATABASE_URI"] = conf.get("database", "SQL_ALCHEMY_CONN")
    init_appbuilder(flask_app)
    get_auth_manager().init()


@pytest.fixture(autouse=True, scope="session")
def initialize_airflow_tests(request):
    """Helper that setups Airflow testing environment."""
    print(" AIRFLOW ".center(60, "="))

    # Setup test environment for breeze
    home = os.path.expanduser("~")
    airflow_home = os.environ.get("AIRFLOW_HOME") or os.path.join(home, "airflow")

    print(f"Home of the user: {home}\nAirflow home {airflow_home}")

    # Initialize Airflow db if required
    lock_file = os.path.join(airflow_home, ".airflow_db_initialised")
    if not skip_db_tests:
        if request.config.option.db_init:
            print("Initializing the DB - forced with --with-db-init switch.")
            initial_db_init()
        elif not os.path.exists(lock_file):
            print(
                "Initializing the DB - first time after entering the container.\n"
                "You can force re-initialization the database by adding --with-db-init switch to run-tests."
            )
            initial_db_init()
            # Create pid file
            with open(lock_file, "w+"):
                pass
        else:
            print(
                "Skipping initializing of the DB as it was initialized already.\n"
                "You can re-initialize the database by adding --with-db-init flag when running tests."
            )
    integration_kerberos = os.environ.get("INTEGRATION_KERBEROS")
    if integration_kerberos == "true":
        # Initialize kerberos
        kerberos = os.environ.get("KRB5_KTNAME")
        if kerberos:
            subprocess.check_call(["kinit", "-kt", kerberos, "bob@EXAMPLE.COM"])
        else:
            print("Kerberos enabled! Please setup KRB5_KTNAME environment variable")
            sys.exit(1)


def pytest_configure(config):
    config.addinivalue_line("filterwarnings", "error::airflow.utils.context.AirflowContextDeprecationWarning")
    config.addinivalue_line("markers", "integration(name): mark test to run with named integration")
    config.addinivalue_line("markers", "backend(name): mark test to run with named backend")
    config.addinivalue_line("markers", "system(name): mark test to run with named system")
    config.addinivalue_line("markers", "long_running: mark test that run for a long time (many minutes)")
    config.addinivalue_line(
        "markers", "quarantined: mark test that are in quarantine (i.e. flaky, need to be isolated and fixed)"
    )
    config.addinivalue_line(
        "markers", "credential_file(name): mark tests that require credential file in CREDENTIALS_DIR"
    )
    config.addinivalue_line(
        "markers", "need_serialized_dag: mark tests that require dags in serialized form to be present"
    )
    config.addinivalue_line(
        "markers",
        "db_test: mark tests that require database to be present",
    )
    config.addinivalue_line(
        "markers",
        "non_db_test_override: you can mark individual tests with this marker to override the db_test marker",
    )
    config.addinivalue_line(
        "markers",
        "virtualenv_operator: virtualenv operator tests are 'long', we should run them separately",
    )
    config.addinivalue_line(
        "markers",
        "external_python_operator: external python operator tests are 'long', we should run them separately",
    )

    os.environ["_AIRFLOW__SKIP_DATABASE_EXECUTOR_COMPATIBILITY_CHECK"] = "1"
    configure_warning_output(config)


def pytest_unconfigure(config):
    del os.environ["_AIRFLOW__SKIP_DATABASE_EXECUTOR_COMPATIBILITY_CHECK"]


def skip_if_not_marked_with_integration(selected_integrations, item):
    for marker in item.iter_markers(name="integration"):
        integration_name = marker.args[0]
        if integration_name in selected_integrations or "all" in selected_integrations:
            return
    pytest.skip(
        f"The test is skipped because it does not have the right integration marker. "
        f"Only tests marked with pytest.mark.integration(INTEGRATION) are run with INTEGRATION "
        f"being one of {selected_integrations}. {item}"
    )


def skip_if_not_marked_with_backend(selected_backend, item):
    for marker in item.iter_markers(name="backend"):
        backend_names = marker.args
        if selected_backend in backend_names:
            return
    pytest.skip(
        f"The test is skipped because it does not have the right backend marker. "
        f"Only tests marked with pytest.mark.backend('{selected_backend}') are run: {item}"
    )


def skip_if_not_marked_with_system(selected_systems, item):
    for marker in item.iter_markers(name="system"):
        systems_name = marker.args[0]
        if systems_name in selected_systems or "all" in selected_systems:
            return
    pytest.skip(
        f"The test is skipped because it does not have the right system marker. "
        f"Only tests marked with pytest.mark.system(SYSTEM) are run with SYSTEM "
        f"being one of {selected_systems}. {item}"
    )


def skip_system_test(item):
    for marker in item.iter_markers(name="system"):
        pytest.skip(
            f"The test is skipped because it has system marker. System tests are only run when "
            f"--system flag with the right system ({marker.args[0]}) is passed to pytest. {item}"
        )


def skip_long_running_test(item):
    for _ in item.iter_markers(name="long_running"):
        pytest.skip(
            f"The test is skipped because it has long_running marker. "
            f"And --include-long-running flag is not passed to pytest. {item}"
        )


def skip_quarantined_test(item):
    for _ in item.iter_markers(name="quarantined"):
        pytest.skip(
            f"The test is skipped because it has quarantined marker. "
            f"And --include-quarantined flag is not passed to pytest. {item}"
        )


def skip_virtualenv_operator_test(item):
    for _ in item.iter_markers(name="virtualenv_operator"):
        pytest.skip(
            f"The test is skipped because it has virtualenv_operator marker. "
            f"And --exclude-virtualenv-operator flag is not passed to pytest. {item}"
        )


def skip_external_python_operator_test(item):
    for _ in item.iter_markers(name="external_python_operator"):
        pytest.skip(
            f"The test is skipped because it has external_python_operator marker. "
            f"And --exclude-external-python-operator flag is not passed to pytest. {item}"
        )


def skip_db_test(item):
    if next(item.iter_markers(name="db_test"), None):
        if next(item.iter_markers(name="non_db_test_override"), None):
            # non_db_test can override the db_test set for example on module or class level
            return
        else:
            pytest.skip(
                f"The test is skipped as it is DB test "
                f"and --skip-db-tests is flag is passed to pytest. {item}"
            )
    if next(item.iter_markers(name="backend"), None):
        # also automatically skip tests marked with `backend` marker as they are implicitly
        # db tests
        pytest.skip(
            f"The test is skipped as it is DB test "
            f"and --skip-db-tests is flag is passed to pytest. {item}"
        )


def only_run_db_test(item):
    if next(item.iter_markers(name="db_test"), None) and not next(
        item.iter_markers(name="non_db_test_override"), None
    ):
        # non_db_test at individual level can override the db_test set for example on module or class level
        return
    else:
        if next(item.iter_markers(name="backend"), None):
            # Also do not skip the tests marked with `backend` marker - as it is implicitly a db test
            return
        pytest.skip(
            f"The test is skipped as it is not a DB tests "
            f"and --run-db-tests-only flag is passed to pytest. {item}"
        )


def skip_if_integration_disabled(marker, item):
    integration_name = marker.args[0]
    environment_variable_name = "INTEGRATION_" + integration_name.upper()
    environment_variable_value = os.environ.get(environment_variable_name)
    if not environment_variable_value or environment_variable_value != "true":
        pytest.skip(
            "The test requires {integration_name} integration started and "
            "{name} environment variable to be set to true (it is '{value}')."
            " It can be set by specifying '--integration {integration_name}' at breeze startup"
            ": {item}".format(
                name=environment_variable_name,
                value=environment_variable_value,
                integration_name=integration_name,
                item=item,
            )
        )


def skip_if_wrong_backend(marker, item):
    valid_backend_names = marker.args
    environment_variable_name = "BACKEND"
    environment_variable_value = os.environ.get(environment_variable_name)
    if not environment_variable_value or environment_variable_value not in valid_backend_names:
        pytest.skip(
            f"The test requires one of {valid_backend_names} backend started and "
            f"{environment_variable_name} environment variable to be set to 'true' (it is "
            f"'{environment_variable_value}'). It can be set by specifying backend at breeze startup: {item}"
        )


def skip_if_credential_file_missing(item):
    for marker in item.iter_markers(name="credential_file"):
        credential_file = marker.args[0]
        credential_path = os.path.join(os.environ.get("CREDENTIALS_DIR"), credential_file)
        if not os.path.exists(credential_path):
            pytest.skip(f"The test requires credential file {credential_path}: {item}")


def pytest_runtest_setup(item):
    selected_integrations_list = item.config.option.integration
    selected_systems_list = item.config.option.system

    include_long_running = item.config.option.include_long_running
    include_quarantined = item.config.option.include_quarantined
    exclude_virtualenv_operator = item.config.option.exclude_virtualenv_operator
    exclude_external_python_operator = item.config.option.exclude_external_python_operator

    for marker in item.iter_markers(name="integration"):
        skip_if_integration_disabled(marker, item)
    if selected_integrations_list:
        skip_if_not_marked_with_integration(selected_integrations_list, item)
    if selected_systems_list:
        skip_if_not_marked_with_system(selected_systems_list, item)
    else:
        skip_system_test(item)
    for marker in item.iter_markers(name="backend"):
        skip_if_wrong_backend(marker, item)
    selected_backend = item.config.option.backend
    if selected_backend:
        skip_if_not_marked_with_backend(selected_backend, item)
    if not include_long_running:
        skip_long_running_test(item)
    if not include_quarantined:
        skip_quarantined_test(item)
    if exclude_virtualenv_operator:
        skip_virtualenv_operator_test(item)
    if exclude_external_python_operator:
        skip_external_python_operator_test(item)
    if skip_db_tests:
        skip_db_test(item)
    if run_db_tests_only:
        only_run_db_test(item)
    skip_if_credential_file_missing(item)


@pytest.fixture
def frozen_sleep(monkeypatch):
    """Use time-machine to "stub" sleep.

    This means the ``sleep()`` takes no time, but ``datetime.now()`` appears to move forwards.

    If your module under test does ``import time`` and then ``time.sleep``:

    .. code-block:: python

        def test_something(frozen_sleep):
            my_mod.fn_under_test()

    If your module under test does ``from time import sleep`` then you will
    have to mock that sleep function directly:

    .. code-block:: python

        def test_something(frozen_sleep, monkeypatch):
            monkeypatch.setattr("my_mod.sleep", frozen_sleep)
            my_mod.fn_under_test()
    """
    traveller = None

    def fake_sleep(seconds):
        nonlocal traveller
        utcnow = datetime.utcnow()
        if traveller is not None:
            traveller.stop()
        traveller = time_machine.travel(utcnow + timedelta(seconds=seconds))
        traveller.start()

    monkeypatch.setattr("time.sleep", fake_sleep)
    yield fake_sleep

    if traveller is not None:
        traveller.stop()


@pytest.fixture(scope="session")
def app():
    from tests.test_utils.config import conf_vars

    with conf_vars({("webserver", "auth_rate_limited"): "False"}):
        from airflow.www import app

        yield app.create_app(testing=True)


@pytest.fixture
def dag_maker(request):
    """Fixture to help create DAG, DagModel, and SerializedDAG automatically.

    You have to use the dag_maker as a context manager and it takes
    the same argument as DAG::

        with dag_maker(dag_id="mydag") as dag:
            task1 = EmptyOperator(task_id='mytask')
            task2 = EmptyOperator(task_id='mytask2')

    If the DagModel you want to use needs different parameters than the one
    automatically created by the dag_maker, you have to update the DagModel as below::

        dag_maker.dag_model.is_active = False
        session.merge(dag_maker.dag_model)
        session.commit()

    For any test you use the dag_maker, make sure to create a DagRun::

        dag_maker.create_dagrun()

    The dag_maker.create_dagrun takes the same arguments as dag.create_dagrun

    If you want to operate on serialized DAGs, then either pass
    ``serialized=True`` to the ``dag_maker()`` call, or you can mark your
    test/class/file with ``@pytest.mark.need_serialized_dag(True)``. In both of
    these cases the ``dag`` returned by the context manager will be a
    lazily-evaluated proxy object to the SerializedDAG.
    """
    import lazy_object_proxy

    # IMPORTANT: Delay _all_ imports from `airflow.*` to _inside a method_.
    # This fixture is "called" early on in the pytest collection process, and
    # if we import airflow.* here the wrong (non-test) config will be loaded
    # and "baked" in to various constants

    want_serialized = False

    # Allow changing default serialized behaviour with `@pytest.mark.need_serialized_dag` or
    # `@pytest.mark.need_serialized_dag(False)`
    serialized_marker = request.node.get_closest_marker("need_serialized_dag")
    if serialized_marker:
        (want_serialized,) = serialized_marker.args or (True,)

    from airflow.utils.log.logging_mixin import LoggingMixin

    class DagFactory(LoggingMixin):
        _own_session = False

        def __init__(self):
            from airflow.models import DagBag

            # Keep all the serialized dags we've created in this test
            self.dagbag = DagBag(os.devnull, include_examples=False, read_dags_from_db=False)

        def __enter__(self):
            self.dag.__enter__()
            if self.want_serialized:
                return lazy_object_proxy.Proxy(self._serialized_dag)
            return self.dag

        def _serialized_dag(self):
            return self.serialized_model.dag

        def get_serialized_data(self):
            try:
                data = self.serialized_model.data
            except AttributeError:
                raise RuntimeError("DAG serialization not requested")
            if isinstance(data, str):
                return json.loads(data)
            return data

        def __exit__(self, type, value, traceback):
            from airflow.models import DagModel
            from airflow.models.serialized_dag import SerializedDagModel

            dag = self.dag
            dag.__exit__(type, value, traceback)
            if type is not None:
                return

            dag.clear(session=self.session)
            dag.sync_to_db(processor_subdir=self.processor_subdir, session=self.session)
            self.dag_model = self.session.get(DagModel, dag.dag_id)

            if self.want_serialized:
                self.serialized_model = SerializedDagModel(
                    dag, processor_subdir=self.dag_model.processor_subdir
                )
                self.session.merge(self.serialized_model)
                serialized_dag = self._serialized_dag()
                self.dagbag.bag_dag(serialized_dag, root_dag=serialized_dag)
                self.session.flush()
            else:
                self.dagbag.bag_dag(self.dag, self.dag)

        def create_dagrun(self, **kwargs):
            from airflow.utils import timezone
            from airflow.utils.state import State
            from airflow.utils.types import DagRunType

            dag = self.dag
            kwargs = {
                "state": State.RUNNING,
                "start_date": self.start_date,
                "session": self.session,
                **kwargs,
            }
            # Need to provide run_id if the user does not either provide one
            # explicitly, or pass run_type for inference in dag.create_dagrun().
            if "run_id" not in kwargs and "run_type" not in kwargs:
                kwargs["run_id"] = "test"

            if "run_type" not in kwargs:
                kwargs["run_type"] = DagRunType.from_run_id(kwargs["run_id"])
            if kwargs.get("execution_date") is None:
                if kwargs["run_type"] == DagRunType.MANUAL:
                    kwargs["execution_date"] = self.start_date
                else:
                    kwargs["execution_date"] = dag.next_dagrun_info(None).logical_date
            if "data_interval" not in kwargs:
                logical_date = timezone.coerce_datetime(kwargs["execution_date"])
                if kwargs["run_type"] == DagRunType.MANUAL:
                    data_interval = dag.timetable.infer_manual_data_interval(run_after=logical_date)
                else:
                    data_interval = dag.infer_automated_data_interval(logical_date)
                kwargs["data_interval"] = data_interval

            self.dag_run = dag.create_dagrun(**kwargs)
            for ti in self.dag_run.task_instances:
                ti.refresh_from_task(dag.get_task(ti.task_id))
            return self.dag_run

        def create_dagrun_after(self, dagrun, **kwargs):
            next_info = self.dag.next_dagrun_info(self.dag.get_run_data_interval(dagrun))
            if next_info is None:
                raise ValueError(f"cannot create run after {dagrun}")
            return self.create_dagrun(
                execution_date=next_info.logical_date,
                data_interval=next_info.data_interval,
                **kwargs,
            )

        def __call__(
            self,
            dag_id="test_dag",
            serialized=want_serialized,
            fileloc=None,
            processor_subdir=None,
            session=None,
            **kwargs,
        ):
            from airflow import settings
            from airflow.models.dag import DAG
            from airflow.utils import timezone

            if session is None:
                self._own_session = True
                session = settings.Session()

            self.kwargs = kwargs
            self.session = session
            self.start_date = self.kwargs.get("start_date", None)
            default_args = kwargs.get("default_args", None)
            if default_args and not self.start_date:
                if "start_date" in default_args:
                    self.start_date = default_args.get("start_date")
            if not self.start_date:
                if hasattr(request.module, "DEFAULT_DATE"):
                    self.start_date = getattr(request.module, "DEFAULT_DATE")
                else:
                    DEFAULT_DATE = timezone.datetime(2016, 1, 1)
                    self.start_date = DEFAULT_DATE
            self.kwargs["start_date"] = self.start_date
            self.dag = DAG(dag_id, **self.kwargs)
            self.dag.fileloc = fileloc or request.module.__file__
            self.want_serialized = serialized
            self.processor_subdir = processor_subdir

            return self

        def cleanup(self):
            from airflow.models import DagModel, DagRun, TaskInstance, XCom
            from airflow.models.dataset import DatasetEvent
            from airflow.models.serialized_dag import SerializedDagModel
            from airflow.models.taskmap import TaskMap
            from airflow.utils.retries import run_with_db_retries

            for attempt in run_with_db_retries(logger=self.log):
                with attempt:
                    dag_ids = list(self.dagbag.dag_ids)
                    if not dag_ids:
                        return
                    # To isolate problems here with problems from elsewhere on the session object
                    self.session.rollback()

                    self.session.query(SerializedDagModel).filter(
                        SerializedDagModel.dag_id.in_(dag_ids)
                    ).delete(synchronize_session=False)
                    self.session.query(DagRun).filter(DagRun.dag_id.in_(dag_ids)).delete(
                        synchronize_session=False,
                    )
                    self.session.query(TaskInstance).filter(TaskInstance.dag_id.in_(dag_ids)).delete(
                        synchronize_session=False,
                    )
                    self.session.query(XCom).filter(XCom.dag_id.in_(dag_ids)).delete(
                        synchronize_session=False,
                    )
                    self.session.query(DagModel).filter(DagModel.dag_id.in_(dag_ids)).delete(
                        synchronize_session=False,
                    )
                    self.session.query(TaskMap).filter(TaskMap.dag_id.in_(dag_ids)).delete(
                        synchronize_session=False,
                    )
                    self.session.query(DatasetEvent).filter(DatasetEvent.source_dag_id.in_(dag_ids)).delete(
                        synchronize_session=False,
                    )
                    self.session.commit()
                    if self._own_session:
                        self.session.expunge_all()

    factory = DagFactory()

    try:
        yield factory
    finally:
        factory.cleanup()
        with suppress(AttributeError):
            del factory.session


@pytest.fixture
def create_dummy_dag(dag_maker):
    """Create a `DAG` with a single `EmptyOperator` task.

    DagRun and DagModel is also created.

    Apart from the already existing arguments, any other argument in kwargs
    is passed to the DAG and not to the EmptyOperator task.

    If you have an argument that you want to pass to the EmptyOperator that
    is not here, please use `default_args` so that the DAG will pass it to the
    Task::

        dag, task = create_dummy_dag(default_args={'start_date':timezone.datetime(2016, 1, 1)})

    You cannot be able to alter the created DagRun or DagModel, use `dag_maker` fixture instead.
    """
    from airflow.operators.empty import EmptyOperator
    from airflow.utils.types import DagRunType

    def create_dag(
        dag_id="dag",
        task_id="op1",
        max_active_tis_per_dag=16,
        max_active_tis_per_dagrun=None,
        pool="default_pool",
        executor_config={},
        trigger_rule="all_done",
        on_success_callback=None,
        on_execute_callback=None,
        on_failure_callback=None,
        on_retry_callback=None,
        email=None,
        with_dagrun_type=DagRunType.SCHEDULED,
        **kwargs,
    ):
        with dag_maker(dag_id, **kwargs) as dag:
            op = EmptyOperator(
                task_id=task_id,
                max_active_tis_per_dag=max_active_tis_per_dag,
                max_active_tis_per_dagrun=max_active_tis_per_dagrun,
                executor_config=executor_config,
                on_success_callback=on_success_callback,
                on_execute_callback=on_execute_callback,
                on_failure_callback=on_failure_callback,
                on_retry_callback=on_retry_callback,
                email=email,
                pool=pool,
                trigger_rule=trigger_rule,
            )
        if with_dagrun_type is not None:
            dag_maker.create_dagrun(run_type=with_dagrun_type)
        return dag, op

    return create_dag


if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance


@pytest.fixture
def create_task_instance(dag_maker, create_dummy_dag):
    """Create a TaskInstance, and associated DB rows (DagRun, DagModel, etc).

    Uses ``create_dummy_dag`` to create the dag structure.
    """

    def maker(
        execution_date=None,
        dagrun_state=None,
        state=None,
        run_id=None,
        run_type=None,
        data_interval=None,
        map_index=-1,
        **kwargs,
    ) -> TaskInstance:
        if execution_date is None:
            from airflow.utils import timezone

            execution_date = timezone.utcnow()
        _, task = create_dummy_dag(with_dagrun_type=None, **kwargs)

        dagrun_kwargs = {"execution_date": execution_date, "state": dagrun_state}
        if run_id is not None:
            dagrun_kwargs["run_id"] = run_id
        if run_type is not None:
            dagrun_kwargs["run_type"] = run_type
        if data_interval is not None:
            dagrun_kwargs["data_interval"] = data_interval
        dagrun = dag_maker.create_dagrun(**dagrun_kwargs)
        (ti,) = dagrun.task_instances
        ti.task = task
        ti.state = state
        ti.map_index = map_index

        dag_maker.session.flush()
        return ti

    return maker


@pytest.fixture()
def create_task_instance_of_operator(dag_maker):
    def _create_task_instance(
        operator_class,
        *,
        dag_id,
        execution_date=None,
        session=None,
        **operator_kwargs,
    ) -> TaskInstance:
        with dag_maker(dag_id=dag_id, session=session):
            operator_class(**operator_kwargs)
        if execution_date is None:
            dagrun_kwargs = {}
        else:
            dagrun_kwargs = {"execution_date": execution_date}
        (ti,) = dag_maker.create_dagrun(**dagrun_kwargs).task_instances
        return ti

    return _create_task_instance


@pytest.fixture()
def create_task_of_operator(dag_maker):
    def _create_task_of_operator(operator_class, *, dag_id, session=None, **operator_kwargs):
        with dag_maker(dag_id=dag_id, session=session):
            task = operator_class(**operator_kwargs)
        return task

    return _create_task_of_operator


@pytest.fixture
def session():
    from airflow.utils.session import create_session

    with create_session() as session:
        yield session
        session.rollback()


@pytest.fixture()
def get_test_dag():
    def _get(dag_id):
        from airflow.models.dagbag import DagBag
        from airflow.models.serialized_dag import SerializedDagModel

        dag_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), "dags", f"{dag_id}.py")
        dagbag = DagBag(dag_folder=dag_file, include_examples=False)

        dag = dagbag.get_dag(dag_id)
        dag.sync_to_db()
        SerializedDagModel.write_dag(dag)

        return dag

    return _get


@pytest.fixture()
def create_log_template(request):
    from airflow import settings
    from airflow.models.tasklog import LogTemplate

    session = settings.Session()

    def _create_log_template(filename_template, elasticsearch_id=""):
        log_template = LogTemplate(filename=filename_template, elasticsearch_id=elasticsearch_id)
        session.add(log_template)
        session.commit()

        def _delete_log_template():
            session.delete(log_template)
            session.commit()

        request.addfinalizer(_delete_log_template)

    return _create_log_template


@pytest.fixture()
def reset_logging_config():
    import logging.config

    from airflow import settings
    from airflow.utils.module_loading import import_string

    logging_config = import_string(settings.LOGGING_CLASS_PATH)
    logging.config.dictConfig(logging_config)


@pytest.fixture(scope="module", autouse=True)
def _clear_db(request):
    from tests.test_utils.db import clear_all

    """Clear DB before each test module run."""
    if not request.config.option.db_cleanup:
        return
    if skip_db_tests:
        return
    from airflow.configuration import conf

    sql_alchemy_conn = conf.get("database", "sql_alchemy_conn")
    if sql_alchemy_conn.startswith("sqlite"):
        sql_alchemy_file = sql_alchemy_conn.replace("sqlite:///", "")
        if not os.path.exists(sql_alchemy_file):
            print(f"The sqlite file `{sql_alchemy_file}` does not exist. Attempt to initialize it.")
            initial_db_init()

    dist_option = getattr(request.config.option, "dist", "no")
    if dist_option != "no" or hasattr(request.config, "workerinput"):
        # Skip if pytest-xdist detected (controller or worker)
        return

    try:
        clear_all()
    except Exception as ex:
        exc_name_parts = [type(ex).__name__]
        exc_module = type(ex).__module__
        if exc_module != "builtins":
            exc_name_parts.insert(0, exc_module)
        extra_msg = "" if request.config.option.db_init else ", try to run with flag --with-db-init"
        pytest.exit(f"Unable clear test DB{extra_msg}, got error {'.'.join(exc_name_parts)}: {ex}")


@pytest.fixture(autouse=True)
def clear_lru_cache():
    from airflow.executors.executor_loader import ExecutorLoader
    from airflow.utils.entry_points import _get_grouped_entry_points

    ExecutorLoader.validate_database_executor_compatibility.cache_clear()
    _get_grouped_entry_points.cache_clear()


@pytest.fixture(autouse=True)
def refuse_to_run_test_from_wrongly_named_files(request):
    dirname: str = request.node.fspath.dirname
    filename: str = request.node.fspath.basename
    is_system_test: bool = "tests/system/" in dirname
    if is_system_test and not request.node.fspath.basename.startswith("example_"):
        raise Exception(
            f"All test method files in tests/system must start with 'example_'. Seems that {filename} "
            f"contains {request.function} that looks like a test case. Please rename the file to "
            f"follow the example_* pattern if you want to run the tests in it."
        )
    if not is_system_test and not request.node.fspath.basename.startswith("test_"):
        raise Exception(
            f"All test method files in tests/ must start with 'test_'. Seems that {filename} "
            f"contains {request.function} that looks like a test case. Please rename the file to "
            f"follow the test_* pattern if you want to run the tests in it."
        )


@pytest.fixture(autouse=True)
def initialize_providers_manager():
    from airflow.providers_manager import ProvidersManager

    ProvidersManager().initialize_providers_configuration()


@pytest.fixture(autouse=True, scope="function")
def close_all_sqlalchemy_sessions():
    from sqlalchemy.orm import close_all_sessions

    close_all_sessions()
    yield
    close_all_sessions()


# The code below is a modified version of capture-warning code from
# https://github.com/athinkingape/pytest-capture-warnings

# MIT License
#
# Portions Copyright (c) 2022 A Thinking Ape Entertainment Ltd.
# Portions Copyright (c) 2022 Pyschojoker (Github)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

captured_warnings: dict[tuple[str, int, type[Warning], str], warnings.WarningMessage] = {}
captured_warnings_count: dict[tuple[str, int, type[Warning], str], int] = {}
warnings_recorder = WarningsRecorder()
default_formatwarning = warnings_recorder._module.formatwarning  # type: ignore[attr-defined]
default_showwarning = warnings_recorder._module.showwarning  # type: ignore[attr-defined]


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(item):
    """
    Needed to grab the item.location information
    """
    global warnings_recorder

    if os.environ.get("PYTHONWARNINGS") == "ignore":
        yield
        return

    warnings_recorder.__enter__()
    yield
    warnings_recorder.__exit__(None, None, None)

    for warning in warnings_recorder.list:
        # this code is adapted from python official warnings module

        # Search the filters
        for filter in warnings.filters:
            action, msg, cat, mod, ln = filter

            module = warning.filename or "<unknown>"
            if module[-3:].lower() == ".py":
                module = module[:-3]  # XXX What about leading pathname?

            if (
                (msg is None or msg.match(str(warning.message)))
                and issubclass(warning.category, cat)
                and (mod is None or mod.match(module))
                and (ln == 0 or warning.lineno == ln)
            ):
                break
        else:
            action = warnings.defaultaction

        # Early exit actions
        if action == "ignore":
            continue

        warning.item = item
        quadruplet: tuple[str, int, type[Warning], str] = (
            warning.filename,
            warning.lineno,
            warning.category,
            str(warning.message),
        )

        if quadruplet in captured_warnings:
            captured_warnings_count[quadruplet] += 1
            continue
        else:
            captured_warnings[quadruplet] = warning
            captured_warnings_count[quadruplet] = 1


@pytest.hookimpl(hookwrapper=True)
def pytest_terminal_summary(terminalreporter, exitstatus, config=None):
    pwd = os.path.realpath(os.curdir)

    def cut_path(path):
        if path.startswith(pwd):
            path = path[len(pwd) + 1 :]
        if "/site-packages/" in path:
            path = path.split("/site-packages/")[1]
        return path

    def format_test_function_location(item):
        return f"{item.location[0]}::{item.location[2]}:{item.location[1]}"

    yield

    if captured_warnings:
        print("\n ======================== Warning summary =============================\n")
        print(f"   The tests generated {sum(captured_warnings_count.values())} warnings.")
        print(f"   After removing duplicates, {len(captured_warnings.values())}  of them remained.")
        print(f"   They are stored in {warning_output_path} file.")
        print("\n ======================================================================\n")
        warnings_as_json = []

        for warning in captured_warnings.values():
            serialized_warning = {
                x: str(getattr(warning.message, x)) for x in dir(warning.message) if not x.startswith("__")
            }

            serialized_warning.update(
                {
                    "path": cut_path(warning.filename),
                    "lineno": warning.lineno,
                    "count": 1,
                    "warning_message": str(warning.message),
                }
            )

            # How we format the warnings: pylint parseable format
            # {path}:{line}: [{msg_id}({symbol}), {obj}] {msg}
            # Always:
            # {path}:{line}: [W0513(warning), ] {msg}

            if "with_traceback" in serialized_warning:
                del serialized_warning["with_traceback"]
            warnings_as_json.append(serialized_warning)

        with warning_output_path.open("w") as f:
            for i in warnings_as_json:
                f.write(f'{i["path"]}:{i["lineno"]}: [W0513(warning), ] {i["warning_message"]}')
                f.write("\n")
    else:
        # nothing, clear file
        with warning_output_path.open("w") as f:
            pass


def configure_warning_output(config):
    global warning_output_path
    warning_output_path = Path(config.getoption("warning_output_path"))
    if (
        "CAPTURE_WARNINGS_OUTPUT" in os.environ
        and warning_output_path.resolve() != DEFAULT_WARNING_OUTPUT_PATH.resolve()
    ):
        warning_output_path = os.environ["CAPTURE_WARNINGS_OUTPUT"]


# End of modified code from  https://github.com/athinkingape/pytest-capture-warnings
