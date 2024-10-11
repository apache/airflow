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
import re
import subprocess
import sys
from contextlib import ExitStack, suppress
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
import time_machine

if TYPE_CHECKING:
    from itsdangerous import URLSafeSerializer

    from dev.tests_common._internals.capture_warnings import CaptureWarningsPlugin  # noqa: F401
    from dev.tests_common._internals.forbidden_warnings import ForbiddenWarningsPlugin  # noqa: F401

# https://docs.pytest.org/en/stable/reference/reference.html#stash
capture_warnings_key = pytest.StashKey["CaptureWarningsPlugin"]()
forbidden_warnings_key = pytest.StashKey["ForbiddenWarningsPlugin"]()

keep_env_variables = "--keep-env-variables" in sys.argv

if not keep_env_variables:
    # Clear all Environment Variables that might have side effect,
    # For example, defined in /files/airflow-breeze-config/variables.env
    _AIRFLOW_CONFIG_PATTERN = re.compile(r"^AIRFLOW__(.+)__(.+)$")
    _KEEP_CONFIGS_SETTINGS: dict[str, dict[str, set[str]]] = {
        # Keep always these configurations
        "always": {
            "database": {"sql_alchemy_conn"},
            "core": {"sql_alchemy_conn"},
            "celery": {"result_backend", "broker_url"},
        },
        # Keep per enabled integrations
        "celery": {"celery": {"*"}, "celery_broker_transport_options": {"*"}},
        "kerberos": {"kerberos": {"*"}},
    }
    if os.environ.get("RUN_TESTS_WITH_DATABASE_ISOLATION", "false").lower() == "true":
        _KEEP_CONFIGS_SETTINGS["always"].update(
            {
                "core": {
                    "internal_api_url",
                    "fernet_key",
                    "database_access_isolation",
                    "internal_api_secret_key",
                    "internal_api_clock_grace",
                },
            }
        )
    _ENABLED_INTEGRATIONS = {e.split("_", 1)[-1].lower() for e in os.environ if e.startswith("INTEGRATION_")}
    _KEEP_CONFIGS: dict[str, set[str]] = {}
    for keep_settings_key in ("always", *_ENABLED_INTEGRATIONS):
        if keep_settings := _KEEP_CONFIGS_SETTINGS.get(keep_settings_key):
            for section, options in keep_settings.items():
                if section not in _KEEP_CONFIGS:
                    _KEEP_CONFIGS[section] = options
                else:
                    _KEEP_CONFIGS[section].update(options)
    for env_key in os.environ.copy():
        if m := _AIRFLOW_CONFIG_PATTERN.match(env_key):
            section, option = m.group(1).lower(), m.group(2).lower()
            if not (ko := _KEEP_CONFIGS.get(section)) or not ("*" in ko or option in ko):
                del os.environ[env_key]

SUPPORTED_DB_BACKENDS = ("sqlite", "postgres", "mysql")

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
    os.environ["_IN_UNIT_TESTS"] = "true"
    # Set it here to pass the flag to python-xdist spawned processes
    os.environ["_AIRFLOW_SKIP_DB_TESTS"] = "true"

if run_db_tests_only:
    # Set it here to pass the flag to python-xdist spawned processes
    os.environ["_AIRFLOW_RUN_DB_TESTS_ONLY"] = "true"

_airflow_sources = os.getenv("AIRFLOW_SOURCES", None)
AIRFLOW_SOURCES_ROOT_DIR = (
    Path(_airflow_sources) if _airflow_sources else Path(__file__).parents[2]
).resolve()
AIRFLOW_TESTS_DIR = AIRFLOW_SOURCES_ROOT_DIR / "tests"

os.environ["AIRFLOW__CORE__PLUGINS_FOLDER"] = os.fspath(AIRFLOW_TESTS_DIR / "plugins")
os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = os.fspath(AIRFLOW_TESTS_DIR / "dags")
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
os.environ["AWS_DEFAULT_REGION"] = os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"
os.environ["CREDENTIALS_DIR"] = os.environ.get("CREDENTIALS_DIR") or "/files/airflow-breeze-config/keys"
os.environ["AIRFLOW_ENABLE_AIP_44"] = os.environ.get("AIRFLOW_ENABLE_AIP_44") or "true"

if platform.system() == "Darwin":
    # mocks from unittest.mock work correctly in subprocesses only if they are created by "fork" method
    # but macOS uses "spawn" by default
    os.environ["AIRFLOW__CORE__MP_START_METHOD"] = "fork"


@pytest.fixture
def reset_db():
    """Resets Airflow db."""

    from airflow.utils import db

    db.resetdb()


ALLOWED_TRACE_SQL_COLUMNS = ["num", "time", "trace", "sql", "parameters", "count"]


@pytest.fixture(autouse=True)
def trace_sql(request):
    from dev.tests_common.test_utils.perf.perf_kit.sqlalchemy import (  # isort: skip
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


@pytest.fixture(autouse=True, scope="session")
def set_db_isolation_mode():
    if os.environ.get("RUN_TESTS_WITH_DATABASE_ISOLATION", "false").lower() == "true":
        from airflow.api_internal.internal_api_call import InternalApiConfig

        InternalApiConfig.set_use_internal_api("tests", allow_tests_to_use_db=True)


def skip_if_database_isolation_mode(item):
    if os.environ.get("RUN_TESTS_WITH_DATABASE_ISOLATION", "false").lower() == "true":
        for _ in item.iter_markers(name="skip_if_database_isolation_mode"):
            pytest.skip("This test is skipped because it is not allowed in database isolation mode.")


def pytest_addoption(parser: pytest.Parser):
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
        "--keep-env-variables",
        action="store_true",
        dest="keep_env_variables",
        help="do not clear environment variables that might have side effect while running tests",
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
        "--disable-forbidden-warnings",
        action="store_true",
        dest="disable_forbidden_warnings",
        help="Disable raising an error if forbidden warnings detected.",
    )
    group.addoption(
        "--disable-capture-warnings",
        action="store_true",
        dest="disable_capture_warnings",
        help="Disable internal capture warnings.",
    )
    group.addoption(
        "--warning-output-path",
        action="store",
        dest="warning_output_path",
        metavar="PATH",
        help=(
            "Path for resulting captured warnings. Absolute or relative to the `tests` directory. "
            "If not provided or environment variable `CAPTURE_WARNINGS_OUTPUT` not set "
            "then 'warnings.txt' will be used."
        ),
    )
    parser.addini(
        name="forbidden_warnings",
        type="linelist",
        help="List of internal Airflow warnings which are prohibited during tests execution.",
    )


@pytest.fixture(autouse=True, scope="session")
def initialize_airflow_tests(request):
    """Helper that setups Airflow testing environment."""
    print(" AIRFLOW ".center(60, "="))

    from dev.tests_common.test_utils.db import initial_db_init

    # Setup test environment for breeze
    home = os.path.expanduser("~")
    airflow_home = os.environ.get("AIRFLOW_HOME") or os.path.join(home, "airflow")

    print(f"Home of the user: {home}\nAirflow home {airflow_home}")

    # Initialize Airflow db if required
    lock_file = os.path.join(airflow_home, ".airflow_db_initialised")
    if not skip_db_tests:
        if request.config.option.db_init:
            from dev.tests_common.test_utils.db import initial_db_init

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


def pytest_configure(config: pytest.Config) -> None:
    # Ensure that the airflow sources dir is at the end of the sys path if it's not already there. Needed to
    # run import from `providers/tests/`
    desired = AIRFLOW_SOURCES_ROOT_DIR.as_posix()
    for path in sys.path:
        if path == desired:
            break
    else:
        sys.path.append(desired)

    if (backend := config.getoption("backend", default=None)) and backend not in SUPPORTED_DB_BACKENDS:
        msg = (
            f"Provided DB backend {backend!r} not supported, "
            f"expected one of: {', '.join(map(repr, SUPPORTED_DB_BACKENDS))}"
        )
        pytest.exit(msg, returncode=6)

    config.addinivalue_line("markers", "integration(name): mark test to run with named integration")
    config.addinivalue_line("markers", "backend(name): mark test to run with named backend")
    config.addinivalue_line("markers", "system(name): mark test to run with named system")
    config.addinivalue_line("markers", "platform(name): mark test to run with specific platform/environment")
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
    config.addinivalue_line("markers", "enable_redact: do not mock redact secret masker")
    config.addinivalue_line("markers", "skip_if_database_isolation_mode: skip if DB isolation is enabled")

    os.environ["_AIRFLOW__SKIP_DATABASE_EXECUTOR_COMPATIBILITY_CHECK"] = "1"

    # Setup internal warnings plugins
    if "ignore" in sys.warnoptions:
        config.option.disable_forbidden_warnings = True
        config.option.disable_capture_warnings = True
    if not config.pluginmanager.get_plugin("warnings"):
        # Internal forbidden warnings plugin depends on builtin pytest warnings plugin
        config.option.disable_forbidden_warnings = True

    forbidden_warnings: list[str] | None = config.getini("forbidden_warnings")
    if not config.option.disable_forbidden_warnings and forbidden_warnings:
        from dev.tests_common._internals.forbidden_warnings import ForbiddenWarningsPlugin

        forbidden_warnings_plugin = ForbiddenWarningsPlugin(
            config=config,
            forbidden_warnings=tuple(map(str.strip, forbidden_warnings)),
        )
        config.pluginmanager.register(forbidden_warnings_plugin)
        config.stash[forbidden_warnings_key] = forbidden_warnings_plugin

    if not config.option.disable_capture_warnings:
        from dev.tests_common._internals.capture_warnings import CaptureWarningsPlugin

        capture_warnings_plugin = CaptureWarningsPlugin(
            config=config, output_path=config.getoption("warning_output_path", default=None)
        )
        config.pluginmanager.register(capture_warnings_plugin)
        config.stash[capture_warnings_key] = capture_warnings_plugin


def pytest_unconfigure(config: pytest.Config) -> None:
    os.environ.pop("_AIRFLOW__SKIP_DATABASE_EXECUTOR_COMPATIBILITY_CHECK", None)
    if forbidden_warnings_plugin := config.stash.get(forbidden_warnings_key, None):
        del config.stash[forbidden_warnings_key]
        config.pluginmanager.unregister(forbidden_warnings_plugin)
    if capture_warnings_plugin := config.stash.get(capture_warnings_key, None):
        del config.stash[capture_warnings_key]
        config.pluginmanager.unregister(capture_warnings_plugin)


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


def skip_if_platform_doesnt_match(marker):
    allowed_platforms = ("linux", "breeze")
    if not (args := marker.args):
        pytest.fail(f"No platform specified, expected one of: {', '.join(map(repr, allowed_platforms))}")
    elif not all(a in allowed_platforms for a in args):
        pytest.fail(
            f"Allowed platforms {', '.join(map(repr, allowed_platforms))}; "
            f"but got: {', '.join(map(repr, args))}"
        )
    if "linux" in args:
        if not sys.platform.startswith("linux"):
            pytest.skip("Test expected to run on Linux platform.")
    if "breeze" in args:
        if not os.path.isfile("/.dockerenv") or os.environ.get("BREEZE", "").lower() != "true":
            raise pytest.skip(
                "Test expected to run into Airflow Breeze container. "
                "Maybe because it is to dangerous to run it outside."
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
            f"The test requires {integration_name} integration started and "
            f"{environment_variable_name} environment variable to be set to true (it is '{environment_variable_value}')."
            f" It can be set by specifying '--integration {integration_name}' at breeze startup"
            f": {item}"
        )


def skip_if_wrong_backend(marker: pytest.Mark, item: pytest.Item) -> None:
    if not (backend_names := marker.args):
        reason = (
            "`pytest.mark.backend` expect to get at least one of the following backends: "
            f"{', '.join(map(repr, SUPPORTED_DB_BACKENDS))}."
        )
        pytest.fail(reason)
    elif unsupported_backends := list(filter(lambda b: b not in SUPPORTED_DB_BACKENDS, backend_names)):
        reason = (
            "Airflow Tests supports only the following backends in `pytest.mark.backend` marker: "
            f"{', '.join(map(repr, SUPPORTED_DB_BACKENDS))}, "
            f"but got {', '.join(map(repr, unsupported_backends))}."
        )
        pytest.fail(reason)

    env_name = "BACKEND"
    if not (backend := os.environ.get(env_name)) or backend not in backend_names:
        reason = (
            f"The test {item.nodeid!r} requires one of {', '.join(map(repr, backend_names))} backend started "
            f"and {env_name!r} environment variable to be set (currently it set to {backend!r}). "
            f"It can be set by specifying backend at breeze startup."
        )
        pytest.skip(reason)


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
    for marker in item.iter_markers(name="platform"):
        skip_if_platform_doesnt_match(marker)
    for marker in item.iter_markers(name="backend"):
        skip_if_wrong_backend(marker, item)
    skip_if_database_isolation_mode(item)
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
        utcnow = datetime.now(tz=timezone.utc)
        if traveller is not None:
            traveller.stop()
        traveller = time_machine.travel(utcnow + timedelta(seconds=seconds))
        traveller.start()

    monkeypatch.setattr("time.sleep", fake_sleep)
    yield fake_sleep

    if traveller is not None:
        traveller.stop()


@pytest.fixture
def dag_maker(request):
    """Fixture to help create DAG, DagModel, and SerializedDAG automatically.

    You have to use the dag_maker as a context manager and it takes
    the same argument as DAG::

        with dag_maker(dag_id="mydag") as dag:
            task1 = EmptyOperator(task_id="mytask")
            task2 = EmptyOperator(task_id="mytask2")

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

        def _bag_dag_compat(self, dag):
            # This is a compatibility shim for the old bag_dag method in Airflow <3.0
            # TODO: Remove this when we drop support for Airflow <3.0 in Providers
            if hasattr(dag, "parent_dag"):
                return self.dagbag.bag_dag(dag, root_dag=dag)
            return self.dagbag.bag_dag(dag)

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
                self._bag_dag_compat(serialized_dag)
                self.session.flush()
            else:
                self._bag_dag_compat(self.dag)

        def create_dagrun(self, **kwargs):
            from airflow.utils import timezone
            from airflow.utils.state import State
            from airflow.utils.types import DagRunType

            from dev.tests_common.test_utils.compat import AIRFLOW_V_3_0_PLUS

            if AIRFLOW_V_3_0_PLUS:
                from airflow.utils.types import DagRunTriggeredByType

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
            if AIRFLOW_V_3_0_PLUS and "triggered_by" not in kwargs:
                kwargs["triggered_by"] = DagRunTriggeredByType.TEST

            self.dag_run = dag.create_dagrun(**kwargs)
            for ti in self.dag_run.task_instances:
                ti.refresh_from_task(dag.get_task(ti.task_id))
            if self.want_serialized:
                self.session.commit()
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
            schedule=timedelta(days=1),
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
            # Set schedule argument to explicitly set value, or a default if no
            # other scheduling arguments are set.
            self.dag = DAG(dag_id, schedule=schedule, **self.kwargs)
            self.dag.fileloc = fileloc or request.module.__file__
            self.want_serialized = serialized
            self.processor_subdir = processor_subdir

            return self

        def cleanup(self):
            from airflow.models import DagModel, DagRun, TaskInstance, XCom
            from airflow.models.serialized_dag import SerializedDagModel
            from airflow.models.taskmap import TaskMap
            from airflow.utils.retries import run_with_db_retries

            from dev.tests_common.test_utils.compat import AssetEvent

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
                    self.session.query(AssetEvent).filter(AssetEvent.source_dag_id.in_(dag_ids)).delete(
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

        dag, task = create_dummy_dag(default_args={"start_date": timezone.datetime(2016, 1, 1)})

    You cannot be able to alter the created DagRun or DagModel, use `dag_maker` fixture instead.
    """
    from airflow.operators.empty import EmptyOperator
    from airflow.utils.types import DagRunType

    def create_dag(
        dag_id="dag",
        task_id="op1",
        task_display_name=None,
        max_active_tis_per_dag=16,
        max_active_tis_per_dagrun=None,
        pool="default_pool",
        executor_config=None,
        trigger_rule="all_done",
        on_success_callback=None,
        on_execute_callback=None,
        on_failure_callback=None,
        on_retry_callback=None,
        email=None,
        with_dagrun_type=DagRunType.SCHEDULED,
        **kwargs,
    ):
        op_kwargs = {}
        from dev.tests_common.test_utils.compat import AIRFLOW_V_2_9_PLUS

        if AIRFLOW_V_2_9_PLUS:
            op_kwargs["task_display_name"] = task_display_name
        with dag_maker(dag_id, **kwargs) as dag:
            op = EmptyOperator(
                task_id=task_id,
                max_active_tis_per_dag=max_active_tis_per_dag,
                max_active_tis_per_dagrun=max_active_tis_per_dagrun,
                executor_config=executor_config or {},
                on_success_callback=on_success_callback,
                on_execute_callback=on_execute_callback,
                on_failure_callback=on_failure_callback,
                on_retry_callback=on_retry_callback,
                email=email,
                pool=pool,
                trigger_rule=trigger_rule,
                **op_kwargs,
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
    from airflow.operators.empty import EmptyOperator

    def maker(
        execution_date=None,
        dagrun_state=None,
        state=None,
        run_id=None,
        run_type=None,
        data_interval=None,
        external_executor_id=None,
        dag_id="dag",
        task_id="op1",
        task_display_name=None,
        max_active_tis_per_dag=16,
        max_active_tis_per_dagrun=None,
        pool="default_pool",
        executor_config=None,
        trigger_rule="all_done",
        on_success_callback=None,
        on_execute_callback=None,
        on_failure_callback=None,
        on_retry_callback=None,
        email=None,
        map_index=-1,
        **kwargs,
    ) -> TaskInstance:
        from dev.tests_common.test_utils.compat import AIRFLOW_V_3_0_PLUS

        if AIRFLOW_V_3_0_PLUS:
            from airflow.utils.types import DagRunTriggeredByType

        if execution_date is None:
            from airflow.utils import timezone

            execution_date = timezone.utcnow()
        with dag_maker(dag_id, **kwargs):
            op_kwargs = {}
            from dev.tests_common.test_utils.compat import AIRFLOW_V_2_9_PLUS

            if AIRFLOW_V_2_9_PLUS:
                op_kwargs["task_display_name"] = task_display_name
            task = EmptyOperator(
                task_id=task_id,
                max_active_tis_per_dag=max_active_tis_per_dag,
                max_active_tis_per_dagrun=max_active_tis_per_dagrun,
                executor_config=executor_config or {},
                on_success_callback=on_success_callback,
                on_execute_callback=on_execute_callback,
                on_failure_callback=on_failure_callback,
                on_retry_callback=on_retry_callback,
                email=email,
                pool=pool,
                trigger_rule=trigger_rule,
                **op_kwargs,
            )

        dagrun_kwargs = {
            "execution_date": execution_date,
            "state": dagrun_state,
        }
        dagrun_kwargs.update({"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {})
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
        ti.external_executor_id = external_executor_id
        ti.map_index = map_index

        dag_maker.session.flush()
        return ti

    return maker


@pytest.fixture
def create_serialized_task_instance_of_operator(dag_maker):
    def _create_task_instance(
        operator_class,
        *,
        dag_id,
        execution_date=None,
        session=None,
        **operator_kwargs,
    ) -> TaskInstance:
        with dag_maker(dag_id=dag_id, serialized=True, session=session):
            operator_class(**operator_kwargs)
        if execution_date is None:
            dagrun_kwargs = {}
        else:
            dagrun_kwargs = {"execution_date": execution_date}
        (ti,) = dag_maker.create_dagrun(**dagrun_kwargs).task_instances
        return ti

    return _create_task_instance


@pytest.fixture
def create_task_instance_of_operator(dag_maker):
    def _create_task_instance(
        operator_class,
        *,
        dag_id,
        execution_date=None,
        session=None,
        **operator_kwargs,
    ) -> TaskInstance:
        with dag_maker(dag_id=dag_id, session=session, serialized=True):
            operator_class(**operator_kwargs)
        if execution_date is None:
            dagrun_kwargs = {}
        else:
            dagrun_kwargs = {"execution_date": execution_date}
        (ti,) = dag_maker.create_dagrun(**dagrun_kwargs).task_instances
        return ti

    return _create_task_instance


@pytest.fixture
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


@pytest.fixture
def get_test_dag():
    def _get(dag_id: str):
        from airflow.models.dagbag import DagBag
        from airflow.models.serialized_dag import SerializedDagModel

        dag_file = AIRFLOW_TESTS_DIR / "dags" / f"{dag_id}.py"
        dagbag = DagBag(dag_folder=dag_file, include_examples=False)

        dag = dagbag.get_dag(dag_id)
        dag.sync_to_db()
        SerializedDagModel.write_dag(dag)

        return dag

    return _get


@pytest.fixture
def create_log_template(request):
    from airflow import settings
    from airflow.models.tasklog import LogTemplate

    session = settings.Session()

    def _create_log_template(filename_template, elasticsearch_id=""):
        log_template = LogTemplate(filename=filename_template, elasticsearch_id=elasticsearch_id)
        session.add(log_template)
        session.commit()

        def _delete_log_template():
            from airflow.models import DagRun, TaskInstance

            session.query(TaskInstance).delete()
            session.query(DagRun).delete()
            session.delete(log_template)
            session.commit()

        request.addfinalizer(_delete_log_template)

    return _create_log_template


@pytest.fixture
def reset_logging_config():
    import logging.config

    from airflow import settings
    from airflow.utils.module_loading import import_string

    logging_config = import_string(settings.LOGGING_CLASS_PATH)
    logging.config.dictConfig(logging_config)


@pytest.fixture(scope="session", autouse=True)
def suppress_info_logs_for_dag_and_fab():
    import logging

    from dev.tests_common.test_utils.compat import AIRFLOW_V_2_9_PLUS

    dag_logger = logging.getLogger("airflow.models.dag")
    dag_logger.setLevel(logging.WARNING)

    if AIRFLOW_V_2_9_PLUS:
        fab_logger = logging.getLogger("airflow.providers.fab.auth_manager.security_manager.override")
        fab_logger.setLevel(logging.WARNING)
    else:
        fab_logger = logging.getLogger("airflow.www.fab_security")
        fab_logger.setLevel(logging.WARNING)


@pytest.fixture(scope="module", autouse=True)
def _clear_db(request):
    """Clear DB before each test module run."""
    from dev.tests_common.test_utils.db import clear_all, initial_db_init

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
    try:
        _get_grouped_entry_points.cache_clear()
        try:
            yield
        finally:
            _get_grouped_entry_points.cache_clear()
    finally:
        ExecutorLoader.validate_database_executor_compatibility.cache_clear()


@pytest.fixture(autouse=True)
def refuse_to_run_test_from_wrongly_named_files(request: pytest.FixtureRequest):
    filepath = request.node.path
    is_system_test: bool = "tests/system/" in os.fspath(filepath)
    test_name = request.node.name
    if request.node.cls:
        test_name = f"{request.node.cls.__name__}.{test_name}"
    if is_system_test and not filepath.name.startswith(("example_", "test_")):
        pytest.fail(
            f"All test method files in tests/system must start with 'example_' or 'test_'. "
            f"Seems that {os.fspath(filepath)!r} contains {test_name!r} that looks like a test case. "
            f"Please rename the file to follow the example_* or test_* pattern if you want to run the tests "
            f"in it."
        )
    elif not is_system_test and not filepath.name.startswith("test_"):
        pytest.fail(
            f"All test method files in tests/ must start with 'test_'. Seems that {os.fspath(filepath)!r} "
            f"contains {test_name!r} that looks like a test case. Please rename the file to "
            f"follow the test_* pattern if you want to run the tests in it."
        )


@pytest.fixture(autouse=True)
def initialize_providers_manager():
    from airflow.providers_manager import ProvidersManager

    ProvidersManager().initialize_providers_configuration()


@pytest.fixture(autouse=True)
def close_all_sqlalchemy_sessions():
    from sqlalchemy.orm import close_all_sessions

    with suppress(Exception):
        close_all_sessions()
    yield
    with suppress(Exception):
        close_all_sessions()


@pytest.fixture
def cleanup_providers_manager():
    from airflow.providers_manager import ProvidersManager

    ProvidersManager()._cleanup()
    ProvidersManager().initialize_providers_configuration()
    try:
        yield
    finally:
        ProvidersManager()._cleanup()


@pytest.fixture(autouse=True)
def _disable_redact(request: pytest.FixtureRequest, mocker):
    """Disable redacted text in tests, except specific."""
    from airflow import settings

    if next(request.node.iter_markers("enable_redact"), None):
        with pytest.MonkeyPatch.context() as mp_ctx:
            mp_ctx.setattr(settings, "MASK_SECRETS_IN_LOGS", True)
            yield
        return

    mocked_redact = mocker.patch("airflow.utils.log.secrets_masker.SecretsMasker.redact")
    mocked_redact.side_effect = lambda item, name=None, max_depth=None: item
    with pytest.MonkeyPatch.context() as mp_ctx:
        mp_ctx.setattr(settings, "MASK_SECRETS_IN_LOGS", False)
        yield
    return


@pytest.fixture
def providers_src_folder() -> Path:
    import airflow.providers

    return Path(airflow.providers.__path__[0]).parents[1]


@pytest.fixture
def hook_lineage_collector():
    from airflow.lineage import hook

    hook._hook_lineage_collector = None
    hook._hook_lineage_collector = hook.HookLineageCollector()
    yield hook.get_hook_lineage_collector()
    hook._hook_lineage_collector = None


@pytest.fixture
def clean_dags_and_dagruns():
    """Fixture that cleans the database before and after every test."""
    from dev.tests_common.test_utils.db import clear_db_dags, clear_db_runs

    clear_db_runs()
    clear_db_dags()
    yield  # Test runs here
    clear_db_dags()
    clear_db_runs()


@pytest.fixture(scope="session")
def app():
    from dev.tests_common.test_utils.config import conf_vars

    with conf_vars({("fab", "auth_rate_limited"): "False"}):
        from airflow.www import app

        yield app.create_app(testing=True)


@pytest.fixture
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


@pytest.fixture
def url_safe_serializer(secret_key) -> URLSafeSerializer:
    from itsdangerous import URLSafeSerializer

    return URLSafeSerializer(secret_key)


@pytest.fixture
def new_thing():
    return None
