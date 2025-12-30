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

import importlib
import json
import logging
import os
import platform
import re
import subprocess
import sys
import warnings
from collections.abc import Callable, Generator
from contextlib import ExitStack, suppress
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar
from unittest import mock

import pytest
import time_machine
from _pytest.config.findpaths import ConfigValue

if TYPE_CHECKING:
    from uuid import UUID

    from itsdangerous import URLSafeSerializer
    from sqlalchemy.orm import Session

    from airflow.models.dagrun import DagRun, DagRunType
    from airflow.models.taskinstance import TaskInstance
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.sdk import DAG, BaseOperator, Context, TriggerRule
    from airflow.sdk.api.datamodels._generated import TaskInstanceState as TIState
    from airflow.sdk.definitions.dag import ScheduleArg
    from airflow.sdk.execution_time.comms import StartupDetails, ToSupervisor
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
    from airflow.sdk.types import DagRunProtocol, Operator
    from airflow.serialization.definitions.dag import SerializedDAG
    from airflow.timetables.base import DataInterval
    from airflow.typing_compat import Self
    from airflow.utils.state import DagRunState, TaskInstanceState

    from tests_common._internals.capture_warnings import CaptureWarningsPlugin  # noqa: F401
    from tests_common._internals.forbidden_warnings import ForbiddenWarningsPlugin  # noqa: F401

Dag = TypeVar("Dag", "DAG", "SerializedDAG", covariant=True)
Op = TypeVar("Op", bound="BaseOperator")

# NOTE: DO NOT IMPORT AIRFLOW THINGS HERE!
#
# This plugin is responsible for configuring Airflow correctly to run tests.
# Importing Airflow here loads Airflow too eagerly and break the configurations.
# Instead, import what you want lazily inside a fixture function.
#
# Be aware that many things in tests_common also indirectly imports Airflow, so
# those modules also should not be imported globally.
#
# (Things in the TYPE_CHECKING block are fine because they are not actually
# imported at runtime; those imports are only hints to the type checker.)

assert "airflow" not in sys.modules, (
    "Airflow SHOULD NOT have been imported at this point! "
    "Read comments in pytest_plugin.py to understand more."
)

# https://docs.pytest.org/en/stable/reference/reference.html#stash
capture_warnings_key = pytest.StashKey["CaptureWarningsPlugin"]()
forbidden_warnings_key = pytest.StashKey["ForbiddenWarningsPlugin"]()

keep_env_variables = "--keep-env-variables" in sys.argv

if not keep_env_variables:
    # Clear all Environment Variables that might have side effect,
    # For example, defined in /files/airflow-breeze-config/environment_variables.env
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
        "redis": {"redis": {"*"}},
    }
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
    # Set it here to pass the flag to python-xdist spawned processes
    os.environ["_AIRFLOW_SKIP_DB_TESTS"] = "true"

if run_db_tests_only:
    # Set it here to pass the flag to python-xdist spawned processes
    os.environ["_AIRFLOW_RUN_DB_TESTS_ONLY"] = "true"

os.environ["_IN_UNIT_TESTS"] = "true"

_airflow_sources = os.getenv("AIRFLOW_SOURCES", None)
AIRFLOW_ROOT_PATH = (Path(_airflow_sources) if _airflow_sources else Path(__file__).parents[3]).resolve()
AIRFLOW_PYPROJECT_TOML_FILE_PATH = AIRFLOW_ROOT_PATH / "pyproject.toml"
AIRFLOW_CORE_SOURCES_PATH = AIRFLOW_ROOT_PATH / "airflow-core" / "src"
AIRFLOW_CORE_TESTS_PATH = AIRFLOW_ROOT_PATH / "airflow-core" / "tests"
AIRFLOW_PROVIDERS_ROOT_PATH = AIRFLOW_ROOT_PATH / "providers"
PROVIDER_DEPENDENCIES_JSON_PATH = AIRFLOW_ROOT_PATH / "generated" / "provider_dependencies.json"
PROVIDER_DEPENDENCIES_JSON_HASH_PATH = (
    AIRFLOW_ROOT_PATH / "generated" / "provider_dependencies.json.sha256sum"
)
UPDATE_PROVIDER_DEPENDENCIES_SCRIPT = (
    AIRFLOW_ROOT_PATH / "scripts" / "ci" / "prek" / "update_providers_dependencies.py"
)

# Deliberately copied from breeze - we want to keep it in sync but we do not want to import code from
# Breeze here as we want to do it quickly
ALL_PYPROJECT_TOML_FILES = []


def get_all_provider_pyproject_toml_provider_yaml_files() -> Generator[Path, None, None]:
    pyproject_toml_content = AIRFLOW_PYPROJECT_TOML_FILE_PATH.read_text().splitlines()
    in_workspace = False
    for line in pyproject_toml_content:
        trimmed_line = line.strip()
        if not in_workspace and trimmed_line.startswith("[tool.uv.workspace]"):
            in_workspace = True
        elif in_workspace:
            if trimmed_line.startswith("#"):
                continue
            if trimmed_line.startswith('"'):
                path = trimmed_line.split('"')[1]
                ALL_PYPROJECT_TOML_FILES.append(AIRFLOW_ROOT_PATH / path / "pyproject.toml")
                if trimmed_line.startswith('"providers/'):
                    yield AIRFLOW_ROOT_PATH / path / "pyproject.toml"
                    yield AIRFLOW_ROOT_PATH / path / "provider.yaml"
            elif trimmed_line.startswith("]"):
                break


def _calculate_provider_deps_hash():
    import hashlib

    hasher = hashlib.sha256()
    for file in sorted(get_all_provider_pyproject_toml_provider_yaml_files()):
        hasher.update(file.read_bytes())
    return hasher.hexdigest()


if not PROVIDER_DEPENDENCIES_JSON_PATH.exists() or not PROVIDER_DEPENDENCIES_JSON_HASH_PATH.exists():
    subprocess.check_call(["uv", "run", UPDATE_PROVIDER_DEPENDENCIES_SCRIPT.as_posix()])
else:
    calculated_provider_deps_hash = _calculate_provider_deps_hash()
    if calculated_provider_deps_hash.strip() != PROVIDER_DEPENDENCIES_JSON_HASH_PATH.read_text().strip():
        subprocess.check_call(["uv", "run", UPDATE_PROVIDER_DEPENDENCIES_SCRIPT.as_posix()])
        PROVIDER_DEPENDENCIES_JSON_HASH_PATH.write_text(calculated_provider_deps_hash)
# End of copied code from breeze

os.environ["AIRFLOW__CORE__ALLOWED_DESERIALIZATION_CLASSES"] = "airflow.*\nunit.*\n"
os.environ["AIRFLOW__CORE__PLUGINS_FOLDER"] = os.fspath(AIRFLOW_CORE_TESTS_PATH / "unit" / "plugins")

IS_MOCK_PLUGINS_MANAGER = bool(
    os.environ.get("_AIRFLOW_SKIP_DB_TESTS", "false") == "true" and importlib.util.find_spec("airflow")
)


@pytest.fixture(scope="session", autouse=True)
def mock_plugins_manager_for_all_non_db_tests():
    if not IS_MOCK_PLUGINS_MANAGER:
        yield None
        return
    from tests_common.test_utils.mock_plugins import mock_plugin_manager

    with mock_plugin_manager(plugins=[]) as _fixture:
        yield _fixture


os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = os.fspath(AIRFLOW_CORE_TESTS_PATH / "unit" / "dags")
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
os.environ["AWS_DEFAULT_REGION"] = os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"
os.environ["CREDENTIALS_DIR"] = os.environ.get("CREDENTIALS_DIR") or "/files/airflow-breeze-config/keys"

if platform.system() == "Darwin":
    # mocks from unittest.mock work correctly in subprocesses only if they are created by "fork" method
    # but macOS uses "spawn" by default
    os.environ["AIRFLOW__CORE__MP_START_METHOD"] = "fork"


@pytest.fixture
def reset_db():
    """Reset Airflow db."""
    from airflow.utils import db

    db.resetdb()


ALLOWED_TRACE_SQL_COLUMNS = ["num", "time", "trace", "sql", "parameters", "count"]


@pytest.fixture(autouse=True)
def trace_sql(request):
    try:
        from tests_common.test_utils.perf.perf_kit.sqlalchemy import count_queries, trace_queries
    except ImportError:
        yield
        return

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


def pytest_addoption(parser: pytest.Parser):
    """Add options parser for custom plugins."""
    group = parser.getgroup("airflow")
    group.addoption(
        "--with-db-init",
        action="store_true",
        dest="db_init",
        help="Forces database initialization before tests, if false it a DB reset still may occur.",
    )
    group.addoption(
        "--without-db-init",
        action="store_true",
        dest="no_db_init",
        help="Forces NO database initialization before tests, takes precedent over --with-db-init.",
    )
    group.addoption(
        "--integration",
        action="append",
        dest="integration",
        metavar="INTEGRATIONS",
        help="only run tests matching integration specified: "
        "[cassandra,kerberos,mongo,celery,statsd,trino,redis]. ",
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
        action="store_true",
        dest="system",
        help="run system tests",
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
    """Set up Airflow testing environment."""
    # To separate this line from test name in case of verbosity runs
    print("\n" + " AIRFLOW ".center(60, "="))

    # Setup test environment for breeze
    home = os.path.expanduser("~")
    airflow_home = os.environ.get("AIRFLOW_HOME") or os.path.join(home, "airflow")

    print(f"Home of the user: {home}\nAirflow home {airflow_home}")

    if not skip_db_tests and not request.config.option.no_db_init:
        _initialize_airflow_db(request.config.option.db_init, airflow_home)

    if os.environ.get("INTEGRATION_KERBEROS") == "true":
        _initialize_kerberos()


def _initialize_airflow_db(force_db_init: bool, airflow_home: str | Path):
    db_init_lock_file = Path(airflow_home).joinpath(".airflow_db_initialised")
    if not force_db_init and db_init_lock_file.exists():
        print(
            "Skipping initializing of the DB as it was initialized already.\n"
            "You can re-initialize the database by adding --with-db-init flag when running tests."
        )
        return

    from tests_common.test_utils.db import initial_db_init

    if force_db_init:
        print("Initializing the DB - forced with --with-db-init flag.")
    else:
        print(
            "Initializing the DB - first time after entering the container.\n"
            "Initialization can be also forced by adding --with-db-init flag when running tests."
        )

    initial_db_init()
    db_init_lock_file.touch(exist_ok=True)


def _initialize_kerberos():
    kerberos = os.environ.get("KRB5_KTNAME")
    if not kerberos:
        print("Kerberos enabled! Please setup KRB5_KTNAME environment variable")
        sys.exit(1)

    subprocess.check_call(["kinit", "-kt", kerberos, "bob@EXAMPLE.COM"])


# for performance reasons, we do not want to rglob deprecation ignore files
# because in MacOS in docker it takes a lot of time to rglob them
# so we opt to hardcode the paths here
DEPRECATIONS_IGNORE_FILES = [
    AIRFLOW_CORE_TESTS_PATH / "deprecations_ignore.yml",
    AIRFLOW_ROOT_PATH / "providers" / "google" / "tests" / "deprecations_ignore.yml",
]


def _find_all_deprecation_ignore_files() -> list[str]:
    all_deprecation_ignore_files = DEPRECATIONS_IGNORE_FILES.copy()
    return list(path.as_posix() for path in all_deprecation_ignore_files)


def pytest_configure(config: pytest.Config) -> None:
    # Ensure that the airflow sources dir is at the end of the sys path if it's not already there.
    if os.environ.get("USE_AIRFLOW_VERSION") == "":
        # if USE_AIRFLOW_VERSION is not empty, we are running tests against the installed version of Airflow
        # and providers so there is no need to add the sources directory to the path
        desired = AIRFLOW_ROOT_PATH.as_posix()
        for path in sys.path:
            if path == desired:
                break
        else:
            # This "desired" path should be the Airflow source directory (repo root)
            assert (AIRFLOW_ROOT_PATH / ".asf.yaml").exists(), f"Path {desired} is not Airflow root"
            sys.path.append(desired)

        if (backend := config.getoption("backend", default=None)) and backend not in SUPPORTED_DB_BACKENDS:
            msg = (
                f"Provided DB backend {backend!r} not supported, "
                f"expected one of: {', '.join(map(repr, SUPPORTED_DB_BACKENDS))}"
            )
            pytest.exit(msg, returncode=6)
    config.inicfg["airflow_deprecations_ignore"] = ConfigValue(
        value=_find_all_deprecation_ignore_files(), origin="override", mode="ini"
    )
    config.addinivalue_line("markers", "integration(name): mark test to run with named integration")
    config.addinivalue_line("markers", "backend(name): mark test to run with named backend")
    config.addinivalue_line("markers", "system: mark test to run as system test")
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
    config.addinivalue_line("markers", "want_activate_assets: mark tests that require assets to be activated")
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
    config.addinivalue_line("markers", "mock_plugin_manager: mark a test to use mock_plugin_manager")

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
        from tests_common._internals.forbidden_warnings import ForbiddenWarningsPlugin

        forbidden_warnings_plugin = ForbiddenWarningsPlugin(
            config=config,
            forbidden_warnings=tuple(map(str.strip, forbidden_warnings)),
        )
        config.pluginmanager.register(forbidden_warnings_plugin)
        config.stash[forbidden_warnings_key] = forbidden_warnings_plugin

    if not config.option.disable_capture_warnings:
        from tests_common._internals.capture_warnings import CaptureWarningsPlugin

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


def skip_if_not_marked_with_system(item):
    if not next(item.iter_markers(name="system"), None):
        pytest.skip(
            f"The test is skipped because it does not have the system marker. "
            f"Only tests marked with pytest.mark.system are run.{item}"
        )


def skip_system_test(item):
    if next(item.iter_markers(name="system"), None):
        pytest.skip(
            f"The test is skipped because it has system marker. System tests are only run when "
            f"--system flag is passed to pytest. {item}"
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


def skip_db_test(item):
    if next(item.iter_markers(name="db_test"), None):
        if next(item.iter_markers(name="non_db_test_override"), None):
            # non_db_test can override the db_test set for example on module or class level
            return
        pytest.skip(
            f"The test is skipped as it is DB test and --skip-db-tests is flag is passed to pytest. {item}"
        )
    if next(item.iter_markers(name="backend"), None):
        # also automatically skip tests marked with `backend` marker as they are implicitly
        # db tests
        pytest.skip(
            f"The test is skipped as it is DB test and --skip-db-tests is flag is passed to pytest. {item}"
        )


def only_run_db_test(item):
    if next(item.iter_markers(name="db_test"), None) and not next(
        item.iter_markers(name="non_db_test_override"), None
    ):
        # non_db_test at individual level can override the db_test set for example on module or class level
        return
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

    include_long_running = item.config.option.include_long_running
    include_quarantined = item.config.option.include_quarantined

    for marker in item.iter_markers(name="integration"):
        skip_if_integration_disabled(marker, item)
    if selected_integrations_list:
        skip_if_not_marked_with_integration(selected_integrations_list, item)
    if item.config.option.system:
        skip_if_not_marked_with_system(item)
    else:
        skip_system_test(item)
    for marker in item.iter_markers(name="platform"):
        skip_if_platform_doesnt_match(marker)
    for marker in item.iter_markers(name="backend"):
        skip_if_wrong_backend(marker, item)
    selected_backend = item.config.option.backend
    if selected_backend:
        skip_if_not_marked_with_backend(selected_backend, item)
    if not include_long_running:
        skip_long_running_test(item)
    if not include_quarantined:
        skip_quarantined_test(item)
    if skip_db_tests:
        skip_db_test(item)
    if run_db_tests_only:
        only_run_db_test(item)
    skip_if_credential_file_missing(item)


@pytest.fixture
def frozen_sleep(monkeypatch):
    """
    Use time-machine to "stub" sleep.

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


class DagMaker(Generic[Dag], Protocol):
    """
    Interface definition for dag_maker return value.

    This class exists so tests can import the class for type hints. The actual
    implementation is done in the dag_maker fixture.
    """

    session: Session
    dag: DAG

    def __enter__(self) -> Dag: ...

    def __exit__(self, type, value, traceback) -> None: ...

    def get_serialized_data(self) -> dict[str, Any]: ...

    def create_dagrun(
        self,
        run_id: str = ...,
        logical_date: datetime = ...,
        data_interval: DataInterval = ...,
        run_type: DagRunType = ...,
        **kwargs,
    ) -> DagRun: ...

    def create_dagrun_after(self, dagrun: DagRun, **kwargs) -> DagRun: ...

    def run_ti(
        self,
        task_id: str,
        dag_run: DagRun | None = ...,
        dag_run_kwargs: dict | None = ...,
        map_index: int = ...,
        **kwargs,
    ) -> TaskInstance: ...

    def __call__(
        self,
        dag_id: str = "test_dag",
        schedule: ScheduleArg = timedelta(days=1),
        serialized: bool = ...,
        activate_assets: bool = ...,
        fileloc: str | None = None,
        session: Session | None = None,
        **kwargs,
    ) -> Self: ...


@pytest.fixture
def dag_maker(request) -> Generator[DagMaker, None, None]:
    """
    Fixture to help create DAG, DagModel, and SerializedDAG automatically.

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
    from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_1_PLUS, NOTSET

    want_serialized = False
    want_activate_assets = True  # Only has effect if want_serialized=True on Airflow 3.

    # Allow changing default serialized behaviour with `@pytest.mark.need_serialized_dag` or
    # `@pytest.mark.need_serialized_dag(False)`
    if serialized_marker := request.node.get_closest_marker("need_serialized_dag"):
        (want_serialized,) = serialized_marker.args or (True,)
    if serialized_marker := request.node.get_closest_marker("want_activate_assets"):
        (want_activate_assets,) = serialized_marker.args or (True,)

    from airflow.utils.log.logging_mixin import LoggingMixin

    class DagFactory(LoggingMixin, DagMaker):
        _own_session = False

        def __init__(self):
            from airflow.models import DagBag

            # Keep all the serialized dags we've created in this test
            self.dagbag = DagBag(os.devnull, include_examples=False)

        def __enter__(self):
            self.serialized_model = None

            self.dag.__enter__()
            if self.want_serialized:
                factory = self

                class DAGProxy(lazy_object_proxy.Proxy):
                    """Wrapper to make test patterns work with serialized dag."""

                    task = factory.dag.task  # Expose the @dag.task decorator.

                    # When adding a task to the dag, automatically re-serialize.
                    def add_task(self, task):
                        factory.dag.add_task(task)
                        factory._make_serdag(factory.dag)

                return DAGProxy(self._serialized_dag)
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

        def _activate_assets(self):
            from sqlalchemy import or_, select

            from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
            from airflow.models.asset import AssetModel, DagScheduleAssetReference, TaskOutletAssetReference

            if AIRFLOW_V_3_1_PLUS:
                from airflow.models.asset import TaskInletAssetReference

                assets_select_condition = or_(
                    AssetModel.scheduled_dags.any(DagScheduleAssetReference.dag_id == self.dag.dag_id),
                    AssetModel.consuming_tasks.any(TaskInletAssetReference.dag_id == self.dag.dag_id),
                    AssetModel.producing_tasks.any(TaskOutletAssetReference.dag_id == self.dag.dag_id),
                )
            else:
                assets_select_condition = or_(
                    AssetModel.consuming_dags.any(DagScheduleAssetReference.dag_id == self.dag.dag_id),
                    AssetModel.producing_tasks.any(TaskOutletAssetReference.dag_id == self.dag.dag_id),
                )

            assets = self.session.scalars(select(AssetModel).where(assets_select_condition)).all()
            SchedulerJobRunner._activate_referenced_assets(assets, session=self.session)

        def __exit__(self, type, value, traceback):
            from airflow.configuration import conf
            from airflow.models import DagModel

            dag = self.dag
            dag.__exit__(type, value, traceback)
            if type is not None:
                return

            if dag.access_control and "FabAuthManager" in conf.get("core", "auth_manager"):
                if AIRFLOW_V_3_0_PLUS:
                    from airflow.providers.fab.www.security_appless import ApplessAirflowSecurityManager
                else:
                    from airflow.www.security_appless import ApplessAirflowSecurityManager  # type: ignore
                security_manager = ApplessAirflowSecurityManager(session=self.session)
                security_manager.sync_perm_for_dag(dag.dag_id, dag.access_control)
            self._make_serdag(dag)
            self.dag_model = self.session.get(DagModel, dag.dag_id)
            self.session.commit()

        def _make_serdag(self, dag: DAG):
            from sqlalchemy import select

            from airflow.models.serialized_dag import SerializedDagModel

            if AIRFLOW_V_3_1_PLUS:
                from airflow.serialization.serialized_objects import LazyDeserializedDAG

                self.serialized_model = SerializedDagModel(LazyDeserializedDAG.from_dag(dag))
            else:
                self.serialized_model = SerializedDagModel(dag)  # type: ignore[arg-type]

            sdm = self.session.scalar(
                select(SerializedDagModel).where(
                    SerializedDagModel.dag_id == dag.dag_id,
                    SerializedDagModel.dag_hash == self.serialized_model.dag_hash,
                )
            )

            if AIRFLOW_V_3_0_PLUS:
                from airflow.serialization.serialized_objects import SerializedDAG

                SerializedDAG.bulk_write_to_db(
                    self.bundle_name,
                    self.bundle_version,
                    [dag],
                    session=self.session,
                )
            else:
                dag.sync_to_db(session=self.session)  # type: ignore[attr-defined]

            if AIRFLOW_V_3_0_PLUS and self.serialized_model != sdm:
                from airflow.models.dag_version import DagVersion
                from airflow.models.dagcode import DagCode

                dagv = DagVersion.write_dag(
                    dag_id=dag.dag_id,
                    bundle_name=self.bundle_name,
                    bundle_version=self.bundle_version,
                    session=self.session,
                )
                self.session.add(dagv)
                self.session.flush()
                dag_code = DagCode(dagv, dag.fileloc, "Source")
                self.session.merge(dag_code)
                self.serialized_model.dag_version = dagv
                if self.want_activate_assets:
                    self._activate_assets()
            if sdm:
                sdm._SerializedDagModel__data_cache = self.serialized_model._SerializedDagModel__data_cache
                sdm._data = self.serialized_model._data
                self.serialized_model = sdm
            else:
                sdm = self.session.merge(self.serialized_model)
            self._bag_dag_compat(dag)
            self.session.flush()

        def create_dagrun(self, *, logical_date=NOTSET, **kwargs):
            from airflow.utils.state import DagRunState
            from airflow.utils.types import DagRunType

            timezone = _import_timezone()

            if AIRFLOW_V_3_0_PLUS:
                from airflow.utils.types import DagRunTriggeredByType
            else:
                DagRunType.ASSET_TRIGGERED = DagRunType.DATASET_TRIGGERED

            if "execution_date" in kwargs:
                warnings.warn(
                    "'execution_date' parameter is preserved only for backward compatibility with Airflow 2 "
                    "and will be removed in future version. In Airflow 3, use logical_date instead.",
                    DeprecationWarning,
                    stacklevel=2,
                )
                logical_date = kwargs.pop("execution_date")

            dag = self._serialized_dag()
            kwargs = {
                "state": DagRunState.RUNNING,
                "start_date": self.start_date,
                "session": self.session,
                **kwargs,
            }

            run_type = kwargs.get("run_type", DagRunType.MANUAL)
            if not isinstance(run_type, DagRunType):
                run_type = DagRunType(run_type)

            if logical_date is None:
                # Explicit non requested
                logical_date = None
            elif logical_date is NOTSET:
                if run_type == DagRunType.MANUAL:
                    logical_date = self.start_date
                else:
                    logical_date = dag.next_dagrun_info(None).logical_date
            logical_date = timezone.coerce_datetime(logical_date)

            data_interval = None
            try:
                data_interval = kwargs["data_interval"]
            except KeyError:
                if logical_date is not None:
                    if run_type == DagRunType.MANUAL:
                        data_interval = dag.timetable.infer_manual_data_interval(run_after=logical_date)
                    elif AIRFLOW_V_3_1_PLUS:
                        from airflow.models.dag import infer_automated_data_interval

                        data_interval = infer_automated_data_interval(dag.timetable, logical_date)
                    else:
                        data_interval = dag.infer_automated_data_interval(logical_date)
            kwargs["data_interval"] = data_interval

            if "run_id" not in kwargs:
                if "run_type" not in kwargs:
                    kwargs["run_id"] = "test"
                else:
                    if AIRFLOW_V_3_0_PLUS:
                        kwargs["run_id"] = dag.timetable.generate_run_id(
                            run_type=run_type,
                            run_after=logical_date or timezone.coerce_datetime(timezone.utcnow()),
                            data_interval=data_interval,
                        )
                    else:
                        kwargs["run_id"] = dag.timetable.generate_run_id(
                            run_type=run_type,
                            logical_date=logical_date or timezone.coerce_datetime(timezone.utcnow()),
                            data_interval=data_interval,
                        )
            kwargs["run_type"] = run_type

            if AIRFLOW_V_3_0_PLUS:
                kwargs.setdefault("triggered_by", DagRunTriggeredByType.TEST)
                kwargs["logical_date"] = logical_date
                kwargs.setdefault("run_after", data_interval[-1] if data_interval else timezone.utcnow())
            else:
                kwargs.pop("triggered_by", None)
                kwargs["execution_date"] = logical_date

            self.dag_run = dag.create_dagrun(**kwargs)
            for ti in self.dag_run.task_instances:
                # This need to always operate on the _real_ dag
                ti.refresh_from_task(self.dag.get_task(ti.task_id))
            self.session.commit()
            return self.dag_run

        def create_dagrun_after(self, dagrun, **kwargs):
            sdag = self._serialized_dag()
            if AIRFLOW_V_3_1_PLUS:
                from airflow.models.dag import get_run_data_interval

                next_info = sdag.next_dagrun_info(get_run_data_interval(sdag.timetable, dagrun))
            else:
                next_info = sdag.next_dagrun_info(sdag.get_run_data_interval(dagrun))
            if next_info is None:
                raise ValueError(f"cannot create run after {dagrun}")
            return self.create_dagrun(
                logical_date=next_info.logical_date,
                data_interval=next_info.data_interval,
                **kwargs,
            )

        def run_ti(self, task_id, dag_run=None, dag_run_kwargs=None, map_index=-1, **kwargs):
            """
            Create a dagrun and run a specific task instance with proper task refresh.

            This is a convenience method for running a single task instance:
            1. Create a dagrun if it does not exist
            2. Get the specific task instance by task_id
            3. Refresh the task instance from the DAG task
            4. Run the task instance

            Returns the created TaskInstance.
            """
            if dag_run is None:
                if dag_run_kwargs is None:
                    dag_run_kwargs = {}
                dag_run = self.create_dagrun(**dag_run_kwargs)
            ti = dag_run.get_task_instance(task_id=task_id, map_index=map_index)
            if ti is None:
                available_task_ids = [task.task_id for task in self.dag.tasks]
                raise ValueError(
                    f"Task instance with task_id '{task_id}' not found in dag run. "
                    f"Available task_ids: {available_task_ids}"
                )
            task = self.dag.get_task(ti.task_id)

            if not AIRFLOW_V_3_1_PLUS:
                # Airflow <3.1 has a bug for DecoratedOperator has an unused signature for
                # `DecoratedOperator._handle_output` for xcom_push
                # This worked for `models.BaseOperator` since it had xcom_push method but for
                # `airflow.sdk.BaseOperator`, this does not exist, so this returns an AttributeError
                # Since this is an unused attribute anyway, we just monkey patch it with a lambda.
                # Error otherwise:
                # /usr/local/lib/python3.11/site-packages/airflow/sdk/bases/decorator.py:253: in execute
                #     return self._handle_output(return_value=return_value, context=context, xcom_push=self.xcom_push)
                #                                                                                      ^^^^^^^^^^^^^^
                # E   AttributeError: '_PythonDecoratedOperator' object has no attribute 'xcom_push'
                task.xcom_push = lambda *args, **kwargs: None
            ti.refresh_from_task(task)
            ti.run(**kwargs)
            return ti

        def sync_dagbag_to_db(self):
            if AIRFLOW_V_3_1_PLUS:
                try:
                    from airflow.dag_processing.dagbag import sync_bag_to_db
                except ImportError:
                    from airflow.models.dagbag import sync_bag_to_db

                sync_bag_to_db(self.dagbag, self.bundle_name, None)
            elif AIRFLOW_V_3_0_PLUS:
                self.dagbag.sync_to_db(self.bundle_name, None)
            else:
                self.dagbag.sync_to_db()

        def __call__(
            self,
            dag_id="test_dag",
            schedule=timedelta(days=1),
            serialized=want_serialized,
            activate_assets=want_activate_assets,
            fileloc=None,
            relative_fileloc=None,
            bundle_name=None,
            bundle_version=None,
            session=None,
            **kwargs,
        ):
            from airflow import settings

            # Don't change this to AIRFLOW_V_3_0_PLUS. Although SDK DAG exists
            # before 3.1, things in dag maker setup can't handle it in compat
            # tests. They are probably fixable, but it's not worthwhile to.
            if AIRFLOW_V_3_1_PLUS:
                from airflow.sdk import DAG
            else:
                from airflow import DAG

            timezone = _import_timezone()

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
            if AIRFLOW_V_3_0_PLUS:
                self.dag.relative_fileloc = relative_fileloc or Path(request.module.__file__).name
            self.want_serialized = serialized
            self.want_activate_assets = activate_assets
            self.bundle_name = bundle_name or "dag_maker"
            self.bundle_version = bundle_version
            if AIRFLOW_V_3_0_PLUS:
                from sqlalchemy import func, select

                from airflow.models.dagbundle import DagBundleModel

                if (
                    self.session.scalar(
                        select(func.count())
                        .select_from(DagBundleModel)
                        .where(DagBundleModel.name == self.bundle_name)
                    )
                    == 0
                ):
                    self.session.add(DagBundleModel(name=self.bundle_name))
                    self.session.commit()

            return self

        def cleanup(self):
            from airflow.models import DagModel, DagRun, TaskInstance
            from airflow.models.serialized_dag import SerializedDagModel
            from airflow.models.taskmap import TaskMap
            from airflow.utils.retries import run_with_db_retries

            from tests_common.test_utils.compat import AssetEvent

            if AIRFLOW_V_3_0_PLUS:
                from airflow.models.xcom import XComModel as XCom
            else:
                from airflow.models.xcom import XCom

            for attempt in run_with_db_retries(logger=self.log):
                with attempt:
                    dag_ids = list(self.dagbag.dag_ids)
                    if not dag_ids:
                        return
                    # To isolate problems here with problems from elsewhere on the session object
                    self.session.rollback()

                    if AIRFLOW_V_3_0_PLUS:
                        from sqlalchemy import delete

                        from airflow.models.dag_version import DagVersion

                        self.session.execute(delete(DagRun).where(DagRun.dag_id.in_(dag_ids)))
                        self.session.execute(delete(TaskInstance).where(TaskInstance.dag_id.in_(dag_ids)))
                        self.session.execute(delete(DagVersion).where(DagVersion.dag_id.in_(dag_ids)))
                    else:
                        from sqlalchemy import delete

                        self.session.execute(
                            delete(SerializedDagModel).where(SerializedDagModel.dag_id.in_(dag_ids))
                        )
                        self.session.execute(delete(DagRun).where(DagRun.dag_id.in_(dag_ids)))
                        self.session.execute(delete(TaskInstance).where(TaskInstance.dag_id.in_(dag_ids)))
                    self.session.execute(delete(XCom).where(XCom.dag_id.in_(dag_ids)))
                    self.session.execute(delete(DagModel).where(DagModel.dag_id.in_(dag_ids)))
                    self.session.execute(delete(TaskMap).where(TaskMap.dag_id.in_(dag_ids)))
                    self.session.execute(delete(AssetEvent).where(AssetEvent.source_dag_id.in_(dag_ids)))
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


class CreateDummyDAG(Protocol):
    """Type stub for create_dummy_dag."""

    def __call__(
        self,
        *,
        dag_id: str = "dag",
        task_id: str = "op1",
        task_display_name: str = ...,
        max_active_tis_per_dag: int = 16,
        max_active_tis_per_dagrun: int = ...,
        pool: str = "default_pool",
        executor_config: dict = ...,
        trigger_rule: TriggerRule = ...,
        on_success_callback: Callable = ...,
        on_execute_callback: Callable = ...,
        on_failure_callback: Callable = ...,
        on_retry_callback: Callable = ...,
        email: str = ...,
        with_dagrun_type="scheduled",
        **kwargs,
    ) -> tuple[DAG, EmptyOperator]: ...


@pytest.fixture
def create_dummy_dag(dag_maker: DagMaker) -> CreateDummyDAG:
    """
    Create a `DAG` with a single `EmptyOperator` task.

    DagRun and DagModel is also created.

    Apart from the already existing arguments, any other argument in kwargs
    is passed to the DAG and not to the EmptyOperator task.

    If you have an argument that you want to pass to the EmptyOperator that
    is not here, please use `default_args` so that the DAG will pass it to the
    Task::

        dag, task = create_dummy_dag(default_args={"start_date": timezone.datetime(2016, 1, 1)})

    You cannot be able to alter the created DagRun or DagModel, use `dag_maker` fixture instead.
    """
    from airflow.providers.standard.operators.empty import EmptyOperator
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


class CreateTaskInstance(Protocol):
    """Type stub for create_task_instance."""

    def __call__(
        self,
        *,
        logical_date: datetime | None = ...,
        dagrun_state: DagRunState = ...,
        state: TaskInstanceState = ...,
        run_id: str = ...,
        run_type: DagRunType = ...,
        data_interval: DataInterval = ...,
        external_executor_id: str = ...,
        dag_id: str = "dag",
        task_id: str = "op1",
        task_display_name: str = ...,
        max_active_tis_per_dag: int = 16,
        max_active_tis_per_dagrun: int = ...,
        pool: str = "default_pool",
        executor_config: dict = ...,
        trigger_rule: TriggerRule = ...,
        on_success_callback: Callable = ...,
        on_execute_callback: Callable = ...,
        on_failure_callback: Callable = ...,
        on_retry_callback: Callable = ...,
        email: str = ...,
        map_index: int = -1,
        **kwargs,
    ) -> TaskInstance: ...


@pytest.fixture
def create_task_instance(dag_maker: DagMaker, create_dummy_dag: CreateDummyDAG) -> CreateTaskInstance:
    """
    Create a TaskInstance, and associated DB rows (DagRun, DagModel, etc).

    Uses ``create_dummy_dag`` to create the dag structure.
    """
    from airflow.providers.standard.operators.empty import EmptyOperator

    from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, NOTSET, ArgNotSet

    def maker(
        logical_date: datetime | None | ArgNotSet = NOTSET,
        run_after=None,
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
        on_skipped_callback=None,
        inlets=None,
        outlets=None,
        email=None,
        map_index=-1,
        hostname=None,
        pid=None,
        last_heartbeat_at=None,
        **kwargs,
    ) -> TaskInstance:
        timezone = _import_timezone()
        if run_after is None:
            run_after = timezone.utcnow()
        if logical_date is NOTSET:
            # For now: default to having a logical date if None is not explicitly passed.
            logical_date = timezone.utcnow()
        with dag_maker(dag_id, **kwargs):
            op_kwargs = {}
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
                on_skipped_callback=on_skipped_callback,
                inlets=inlets,
                outlets=outlets,
                email=email,
                pool=pool,
                trigger_rule=trigger_rule,
                **op_kwargs,
            )
        if AIRFLOW_V_3_0_PLUS:
            dagrun_kwargs = {
                "logical_date": logical_date,
                "run_after": run_after,
                "state": dagrun_state,
            }
        else:
            dagrun_kwargs = {
                "logical_date": logical_date if logical_date not in (None, NOTSET) else run_after,
                "state": dagrun_state,
            }
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
        ti.hostname = hostname or ""
        ti.pid = pid
        ti.last_heartbeat_at = last_heartbeat_at
        dag_maker.session.flush()
        return ti

    return maker


class CreateTaskInstanceOfOperator(Protocol):
    """Type stub for create_task_instance_of_operator and create_serialized_task_instance_of_operator."""

    def __call__(
        self,
        operator_class: type[BaseOperator],
        *,
        dag_id: str,
        logical_date: datetime = ...,
        session: Session = ...,
        **kwargs,
    ) -> TaskInstance: ...


@pytest.fixture
def create_serialized_task_instance_of_operator(dag_maker: DagMaker) -> CreateTaskInstanceOfOperator:
    from tests_common.test_utils.version_compat import NOTSET

    def _create_task_instance(
        operator_class,
        *,
        dag_id,
        logical_date=NOTSET,
        session=None,
        **operator_kwargs,
    ) -> TaskInstance:
        with dag_maker(dag_id=dag_id, serialized=True, session=session):
            operator_class(**operator_kwargs)
        (ti,) = dag_maker.create_dagrun(logical_date=logical_date).task_instances
        return ti

    return _create_task_instance


@pytest.fixture
def create_task_instance_of_operator(dag_maker: DagMaker) -> CreateTaskInstanceOfOperator:
    from tests_common.test_utils.version_compat import NOTSET

    def _create_task_instance(
        operator_class,
        *,
        dag_id,
        logical_date=NOTSET,
        session=None,
        **operator_kwargs,
    ) -> TaskInstance:
        with dag_maker(dag_id=dag_id, session=session, serialized=True):
            operator_class(**operator_kwargs)
        (ti,) = dag_maker.create_dagrun(logical_date=logical_date).task_instances
        return ti

    return _create_task_instance


class CreateTaskOfOperator(Protocol):
    """Type stub for create_task_of_operator."""

    def __call__(
        self,
        operator_class: type[Op],
        *,
        dag_id: str,
        session: Session = ...,
        **kwargs,
    ) -> Op: ...


@pytest.fixture
def create_task_of_operator(dag_maker: DagMaker) -> CreateTaskOfOperator:
    def _create_task_of_operator(operator_class, *, dag_id, session=None, **operator_kwargs):
        default_timeout = 7
        with dag_maker(dag_id=dag_id, session=session):
            if "timeout" not in operator_kwargs:
                operator_kwargs["timeout"] = default_timeout
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
        from airflow import settings
        from airflow.models.serialized_dag import SerializedDagModel

        from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_2_PLUS

        if AIRFLOW_V_3_2_PLUS:
            from airflow.dag_processing.dagbag import DagBag
        else:
            from airflow.models.dagbag import DagBag  # type: ignore[no-redef, attribute-defined]

        dag_file = AIRFLOW_CORE_TESTS_PATH / "unit" / "dags" / f"{dag_id}.py"
        dagbag = DagBag(dag_folder=dag_file, include_examples=False)

        dag = dagbag.get_dag(dag_id)

        if dagbag.import_errors:
            if settings.Session is None:
                raise RuntimeError("Session not configured. Call configure_orm() first.")
            session = settings.Session()
            from airflow.models.errors import ParseImportError

            timezone = _import_timezone()

            # Add the new import errors
            for _filename, stacktrace in dagbag.import_errors.items():
                session.add(
                    ParseImportError(
                        filename=str(dag_file),
                        bundle_name="testing",
                        timestamp=timezone.utcnow(),
                        stacktrace=stacktrace,
                    )
                )

            return

        if AIRFLOW_V_3_0_PLUS:
            from sqlalchemy import func, select

            from airflow.models.dagbundle import DagBundleModel
            from airflow.serialization.serialized_objects import SerializedDAG

            if settings.Session is None:
                raise RuntimeError("Session not configured. Call configure_orm() first.")
            session = settings.Session()
            if not session.scalar(select(func.count()).where(DagBundleModel.name == "testing")):
                session.add(DagBundleModel(name="testing"))
                session.flush()
            SerializedDAG.bulk_write_to_db("testing", None, [dag], session=session)
            session.commit()
            session.close()
        else:
            dag.sync_to_db()
        SerializedDagModel.write_dag(dag, bundle_name="testing")

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
            from sqlalchemy import delete

            from airflow.models import DagRun, TaskInstance

            session.execute(delete(TaskInstance))
            session.execute(delete(DagRun))
            session.delete(log_template)
            session.commit()

        request.addfinalizer(_delete_log_template)

    return _create_log_template


@pytest.fixture
def reset_logging_config():
    from airflow.logging_config import configure_logging

    configure_logging()


@pytest.fixture(scope="session", autouse=True)
def suppress_info_logs_for_dag_and_fab():
    import logging

    dag_logger = logging.getLogger("airflow.models.dag")
    dag_logger.setLevel(logging.WARNING)

    fab_logger = logging.getLogger("airflow.providers.fab.auth_manager.security_manager.override")
    fab_logger.setLevel(logging.WARNING)


@pytest.fixture(scope="module", autouse=True)
def _clear_db(request):
    """Clear DB before each test module run."""
    if importlib.util.find_spec("airflow") is None:
        # If airflow is not installed, we should not clear the DB
        return
    from tests_common.test_utils.db import clear_all, initial_db_init

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
    if importlib.util.find_spec("airflow") is None:
        # If airflow is not installed, we should not clear the cache
        yield
        return

    from airflow.utils.entry_points import _get_grouped_entry_points

    _get_grouped_entry_points.cache_clear()
    try:
        yield
    finally:
        _get_grouped_entry_points.cache_clear()


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


@pytest.fixture(autouse=True, scope="session")
def initialize_providers_manager(request: pytest.FixtureRequest):
    request.getfixturevalue("_ensure_configured_logging")
    if importlib.util.find_spec("airflow") is None:
        # If airflow is not installed, we should not initialize providers manager
        return
    from airflow.providers_manager import ProvidersManager

    ProvidersManager().initialize_providers_configuration()


@pytest.fixture(autouse=True)
def close_all_sqlalchemy_sessions():
    try:
        from sqlalchemy.orm import close_all_sessions

        with suppress(Exception):
            close_all_sessions()
        yield
        with suppress(Exception):
            close_all_sessions()
    except ImportError:
        # If sqlalchemy is not installed, we should not close all sessions
        yield
        return


@pytest.fixture
def cleanup_providers_manager():
    from airflow.providers_manager import ProvidersManager

    ProvidersManager()._cleanup()
    ProvidersManager().initialize_providers_configuration()
    try:
        yield
    finally:
        ProvidersManager()._cleanup()
        ProvidersManager().initialize_providers_configuration()


@pytest.fixture(autouse=True)
def _disable_redact(request: pytest.FixtureRequest, mocker):
    """Disable redacted text in tests, except specific."""
    try:
        from airflow import settings
    except ImportError:
        # If airflow is not installed, we should not mock redact
        yield
        return

    from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_1_PLUS

    if next(request.node.iter_markers("enable_redact"), None):
        with pytest.MonkeyPatch.context() as mp_ctx:
            if AIRFLOW_V_3_1_PLUS:
                from airflow._shared.secrets_masker import (
                    SecretsMasker as CoreSecretsMasker,
                )
                from airflow.sdk._shared.secrets_masker import (
                    SecretsMasker as SDKSecretsMasker,
                )

                mp_ctx.setattr(CoreSecretsMasker, "mask_secrets_in_logs", True)
                mp_ctx.setattr(SDKSecretsMasker, "mask_secrets_in_logs", True)
            else:
                # Fallback for older versions
                mp_ctx.setattr(settings, "MASK_SECRETS_IN_LOGS", True)
            yield
        return

    # Rest of the mocking logic remains the same
    targets = []
    if AIRFLOW_V_3_1_PLUS:
        targets = [
            "airflow._shared.secrets_masker.SecretsMasker.redact",
            "airflow.sdk._shared.secrets_masker.SecretsMasker.redact",
        ]
    elif AIRFLOW_V_3_0_PLUS:
        targets = ["airflow.sdk.execution_time.secrets_masker.SecretsMasker.redact"]
    else:
        targets = ["airflow.utils.log.secrets_masker.SecretsMasker.redact"]

    for target in targets:
        mocked_redact = mocker.patch(target)
        mocked_redact.side_effect = lambda item, *args, **kwargs: item

    with pytest.MonkeyPatch.context() as mp_ctx:
        # NEW: Set class variable instead of settings
        if AIRFLOW_V_3_1_PLUS:
            from airflow._shared.secrets_masker import (
                SecretsMasker as CoreSecretsMasker,
            )
            from airflow.sdk._shared.secrets_masker import (
                SecretsMasker as SDKSecretsMasker,
            )

            mp_ctx.setattr(CoreSecretsMasker, "mask_secrets_in_logs", True)
            mp_ctx.setattr(SDKSecretsMasker, "mask_secrets_in_logs", True)
        else:
            # Fallback for older versions
            mp_ctx.setattr(settings, "MASK_SECRETS_IN_LOGS", False)
        yield
    return


@pytest.fixture(autouse=True)
def _mock_plugins(request: pytest.FixtureRequest):
    """Mock the plugin manager if marked with this fixture."""
    if mark := next(request.node.iter_markers("mock_plugin_manager"), None):
        from tests_common.test_utils.mock_plugins import mock_plugin_manager

        with mock_plugin_manager(**mark.kwargs):
            yield
            return
    yield


@pytest.fixture
def hook_lineage_collector():
    from airflow.lineage.hook import HookLineageCollector

    hlc = HookLineageCollector()
    with mock.patch(
        "airflow.lineage.hook.get_hook_lineage_collector",
        return_value=hlc,
    ):
        # Redirect calls to compat provider to support back-compat tests of 2.x as well
        from airflow.providers.common.compat.lineage.hook import get_hook_lineage_collector

        yield get_hook_lineage_collector()


@pytest.fixture
def clean_dags_dagruns_and_dagbundles():
    """Fixture that cleans the database before and after every test."""
    from tests_common.test_utils.db import clear_db_dag_bundles, clear_db_dags, clear_db_runs
    from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

    clear_db_runs()
    clear_db_dags()
    if AIRFLOW_V_3_0_PLUS:
        clear_db_dag_bundles()
    yield  # Test runs here
    clear_db_dags()
    clear_db_runs()
    if AIRFLOW_V_3_0_PLUS:
        clear_db_dag_bundles()


@pytest.fixture
def clean_executor_loader():
    """Clean the executor_loader state, as it stores global variables in the module, causing side effects for some tests."""
    from airflow.executors.executor_loader import ExecutorLoader

    from tests_common.test_utils.executor_loader import clean_executor_loader_module

    clean_executor_loader_module()
    yield  # Test runs here
    clean_executor_loader_module()
    ExecutorLoader.init_executors()


@pytest.fixture
def secret_key() -> str:
    """Return secret key configured."""
    from airflow.configuration import conf

    the_key = conf.get("api", "SECRET_KEY")
    if the_key is None:
        raise RuntimeError(
            "The secret key SHOULD be configured as `[api] secret_key` in the "
            "configuration/environment at this stage! "
        )
    return the_key


@pytest.fixture
def url_safe_serializer(secret_key) -> URLSafeSerializer:
    from itsdangerous import URLSafeSerializer

    return URLSafeSerializer(secret_key)


@pytest.fixture
def create_db_api_hook(request):
    from unittest.mock import MagicMock

    from sqlalchemy.engine import Inspector

    from airflow.providers.common.sql.hooks.sql import DbApiHook

    columns, primary_keys, reserved_words, escape_column_names = request.param

    inspector = MagicMock(spec=Inspector)
    inspector.get_columns.side_effect = lambda table_name, schema: columns

    test_db_hook = MagicMock(placeholder="?", inspector=inspector, spec=DbApiHook)
    test_db_hook.run.side_effect = lambda *args: primary_keys
    test_db_hook.reserved_words = reserved_words
    test_db_hook.escape_word_format = "[{}]"
    test_db_hook.escape_column_names = escape_column_names or False

    return test_db_hook


@pytest.fixture(autouse=True, scope="session")
def add_expected_folders_to_pythonpath():
    """
    Add all expected folders to the python path.

    Appends all provider "tests" folder to the end od the  python path and inserts
    airflow core src folder to be always first on the path.

    This way:

    * all the objects defined in __init__.py of ``airflow`` package are available for pytest discovery.
      Pytest discovery does not understand legacy namespace packages, and while generally speaking -
      all "src" packages (including airflow-core/src) should be automatically added to python path, we need
      to make sure that it's FIRST on the path, otherwise, pytest will not be able to discover tests that
      import airflow.
    * all the tests can import any other unit, system, integration tests and treat root of their provider
      "tests" folder as the place from everything is imported.


    :return: updated python path with expected folders added
    """
    old_path = sys.path.copy()
    all_provider_test_folders: list[Path] = list(AIRFLOW_ROOT_PATH.glob("providers/*/tests"))
    all_provider_test_folders.extend(list(AIRFLOW_ROOT_PATH.glob("providers/*/*/tests")))
    for provider_test_folder in all_provider_test_folders:
        sys.path.append(str(provider_test_folder))
    yield
    sys.path.clear()
    sys.path.extend(old_path)


@pytest.fixture
def cap_structlog(monkeypatch, request):
    """
    Test that structlog messages are logged.

    This extends the feature built in to structlog to make it easier to find if a message is logged.

    >>> def test_something(cap_structlog):
    ...     log.info("some event", field1=False, field2=[1, 2])
    ...     log.info("some event", field1=True)
    ...     assert "some_event" in cap_structlog  # a string searches on `event` field
    ...     assert {"event": "some_event", "field1": True} in cap_structlog  # Searches only on passed fields
    ...     assert {"field2": [1, 2]} in cap_structlog
    ...
    ...     assert "not logged" not in cap_structlog  # not in works too
    """
    import structlog.stdlib
    from structlog import DropEvent, configure, get_config

    from tests_common.test_utils.logs import StructlogCapture

    cap = StructlogCapture()
    # Modify `_Configuration.default_processors` set via `configure` but always
    # keep the list instance intact to not break references held by bound
    # loggers.
    processors = get_config()["processors"]
    old_processors = processors.copy()

    # And modify the stdlib logging to capture too
    handler = logging.root.handlers[0]
    if not isinstance(handler.formatter, structlog.stdlib.ProcessorFormatter):
        raise AssertionError(
            f"{type(handler.formatter)} is not an instance of structlog.stblid.ProcessorFormatter"
        )

    std_formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=handler.formatter.foreign_pre_chain,
        pass_foreign_args=True,
        use_get_message=False,
        processor=cap,
    )

    def stdlib_filter(record):
        with suppress(DropEvent):
            std_formatter.format(record)
        return False

    dict_exc_formatter = structlog.tracebacks.ExceptionDictTransformer(
        use_rich=False,
        show_locals=False,
    )

    dict_tracebacks = structlog.processors.ExceptionRenderer(dict_exc_formatter)
    timestamper = structlog.processors.MaybeTimeStamper(fmt="iso")

    level = logging.INFO
    for setting_name in ("log_cli_level", "log_level"):
        log_level = request.config.getoption(setting_name)
        if log_level is None:
            log_level = request.config.getini(setting_name)
        if log_level:
            level = structlog.processors.NAME_TO_LEVEL[log_level.lower()]
            break

    monkeypatch.setattr(logging.root, "level", level)
    # Ensure the handler doesn't filter anything itself (in stblib both loggers and handlers have their own
    # independent level!)
    monkeypatch.setattr(handler, "level", 0)
    monkeypatch.setattr(handler, "filters", [stdlib_filter])

    try:
        # clear processors list and use LogCapture for testing
        processors.clear()
        processors.append(timestamper)
        processors.append(dict_tracebacks)
        processors.append(cap)
        configure(processors=processors)
        yield cap
    finally:
        cap._finalize()
        # remove LogCapture and restore original processors
        processors.clear()
        processors.extend(old_processors)
        configure(processors=processors)


@pytest.fixture(scope="session", autouse=True)
def _ensure_configured_logging(request):
    try:
        from airflow.sdk._shared.logging import configure_logging
    except ModuleNotFoundError:
        try:
            from airflow.sdk._shared.logging import configure_logging
        except ModuleNotFoundError:
            return

    log_level = logging.INFO
    for setting_name in ("log_cli_level", "log_level"):
        log_level = request.config.getoption(setting_name)
        if log_level is None:
            log_level = request.config.getini(setting_name)
        if log_level:
            break
    configure_logging(log_level=log_level)


@pytest.fixture(name="caplog")
def override_caplog(request):
    """
    Override the builtin caplog test fixture to also re-configure Airflow logging test afterwards.

    This is in an effort to reduce flakiness from caplog related tests where one test file can change log
    behaviour and bleed in to affecting other test files
    """

    try:
        import airflow.sdk._shared.logging
    except ModuleNotFoundError:
        try:
            import airflow.sdk._shared.logging
        except ModuleNotFoundError:
            # No structlog available, fallback to the stock one. Compat for pre-3.1

            # We need this `_ispytest` so it doesn't warn about using private
            fixture = pytest.LogCaptureFixture(request.node, _ispytest=True)
            yield fixture
            fixture._finalize()

            if "airflow.logging_config" in sys.modules:
                import airflow.logging_config

                airflow.logging_config.configure_logging()
            return

    yield request.getfixturevalue("cap_structlog")
    return


@pytest.fixture
def mock_supervisor_comms(monkeypatch):
    # for back-compat
    from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

    if not AIRFLOW_V_3_0_PLUS:
        yield None
        return

    from airflow.sdk.execution_time import comms, task_runner

    # Deal with TaskSDK 1.0/1.1 vs 1.2+. Annoying, and shouldn't need to exist once the separation between
    # core and TaskSDK is finished
    if CommsDecoder := getattr(comms, "CommsDecoder", None):
        comms = mock.create_autospec(CommsDecoder)
        monkeypatch.setattr(task_runner, "SUPERVISOR_COMMS", comms, raising=False)
    else:
        CommsDecoder = getattr(task_runner, "CommsDecoder")
        comms = mock.create_autospec(CommsDecoder)
        comms.send = comms.get_message
        monkeypatch.setattr(task_runner, "SUPERVISOR_COMMS", comms, raising=False)
    yield comms


@pytest.fixture
def sdk_connection_not_found(mock_supervisor_comms):
    """
    Fixture that mocks supervisor comms to return CONNECTION_NOT_FOUND error.

    This eliminates the need to manually set up the mock in every test that
    needs a connection not found message through supervisor comms.

    Example:
        @pytest.mark.db_test
        def test_invalid_location(self, sdk_connection_not_found):
            # Test logic that expects CONNECTION_NOT_FOUND error
            with pytest.raises(AirflowException):
                operator.execute(context)
    """
    from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

    if not AIRFLOW_V_3_0_PLUS:
        yield None
        return

    from airflow.sdk.exceptions import ErrorType
    from airflow.sdk.execution_time.comms import ErrorResponse

    error_response = ErrorResponse(error=ErrorType.CONNECTION_NOT_FOUND)
    mock_supervisor_comms.send.return_value = error_response
    if hasattr(mock_supervisor_comms, "asend"):
        mock_supervisor_comms.asend.return_value = error_response

    yield mock_supervisor_comms


@pytest.fixture
def mocked_parse(spy_agency):
    """
    Fixture to set up an inline DAG and use it in a stubbed `parse` function.

    Use this fixture if you want to isolate and test `parse` or `run` logic without having to define a DAG file.
    In most cases, you should use `create_runtime_ti` fixture instead where you can directly pass an operator
    compared to lower level AIP-72 constructs like `StartupDetails`.

    This fixture returns a helper function `set_dag` that:
    1. Creates an in line DAG with the given `dag_id` and `task` (limited to one task)
    2. Constructs a `RuntimeTaskInstance` based on the provided `StartupDetails` and task.
    3. Stubs the `parse` function using `spy_agency`, to return the mocked `RuntimeTaskInstance`.

    After adding the fixture in your test function signature, you can use it like this ::

            mocked_parse(
                StartupDetails(
                    ti=TaskInstance(
                        id=uuid7(), task_id="hello", dag_id="super_basic_run", run_id="c", try_number=1
                    ),
                    file="",
                ),
                "example_dag_id",
                CustomOperator(task_id="hello"),
            )
    """

    def set_dag(what: StartupDetails, dag_id: str, task: BaseOperator) -> RuntimeTaskInstance:
        from airflow.sdk import DAG
        from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance, parse

        timezone = _import_timezone()

        if not task.has_dag():
            dag = DAG(dag_id=dag_id, start_date=timezone.datetime(2024, 12, 3))
            task.dag = dag
            # Fixture only helps in regular base operator tasks, so mypy is wrong here
            task = dag.task_dict[task.task_id]  # type: ignore[assignment]
        else:
            dag = task.dag
        if what.ti_context.dag_run.conf:
            dag.params = what.ti_context.dag_run.conf  # type: ignore[assignment]
        ti = RuntimeTaskInstance.model_construct(
            **what.ti.model_dump(exclude_unset=True),
            task=task,
            _ti_context_from_server=what.ti_context,
            max_tries=what.ti_context.max_tries,
            start_date=what.start_date,
        )
        if hasattr(parse, "spy"):
            spy_agency.unspy(parse)
        spy_agency.spy_on(parse, call_fake=lambda _, log: ti)
        return ti

    return set_dag


class _XComHelperProtocol(Protocol):
    def get(
        self,
        key: str,
        task_id: str | None = None,
        dag_id: str | None = None,
        run_id: str | None = None,
        map_index: int | None = None,
    ) -> Any: ...

    def assert_pushed(
        self,
        key: str,
        value: Any,
        task_id: str | None = None,
        dag_id: str | None = None,
        run_id: str | None = None,
        map_index: int | None = None,
        **kwargs,
    ) -> None: ...

    def clear(self): ...


class RunTaskCallable(Protocol):
    """Protocol for better type hints for the fixture `run_task`."""

    @property
    def state(self) -> TIState: ...

    @property
    def msg(self) -> ToSupervisor | None: ...

    @property
    def error(self) -> BaseException | None: ...

    @property
    def ti(self) -> RuntimeTaskInstance: ...

    @property
    def dagrun(self) -> DagRunProtocol: ...

    @property
    def context(self) -> Context: ...

    xcom: _XComHelperProtocol

    def __call__(
        self,
        task: BaseOperator,
        dag_id: str = ...,
        run_id: str = ...,
        logical_date: datetime | None = None,
        start_date: datetime | None = None,
        run_type: str = ...,
        try_number: int = ...,
        map_index: int | None = ...,
        ti_id: UUID | None = None,
        max_tries: int | None = None,
        context_update: dict[str, Any] | None = None,
    ) -> tuple[TIState, ToSupervisor | None, BaseException | None]: ...


@pytest.fixture
def create_runtime_ti(mocked_parse):
    """
    Fixture to create a Runtime TaskInstance for testing purposes without defining a dag file.

    It mimics the behavior of the `parse` function by creating a `RuntimeTaskInstance` based on the provided
    `StartupDetails` (formed from arguments) and task. This allows you to test the logic of a task without
    having to define a DAG file, parse it, get context from the server, etc.

    Example usage: ::

        def test_custom_task_instance(create_runtime_ti):
            class MyTaskOperator(BaseOperator):
                def execute(self, context):
                    assert context["dag_run"].run_id == "test_run"

            task = MyTaskOperator(task_id="test_task")
            ti = create_runtime_ti(task)
            # Further test logic...
    """
    from uuid6 import uuid7

    from airflow.sdk import DAG
    from airflow.sdk.api.datamodels._generated import TaskInstance
    from airflow.sdk.execution_time.comms import BundleInfo, StartupDetails
    from airflow.timetables.base import TimeRestriction

    from tests_common.test_utils.version_compat import AIRFLOW_V_3_2_PLUS

    if AIRFLOW_V_3_2_PLUS:
        from airflow.serialization.encoders import coerce_to_core_timetable
    else:
        coerce_to_core_timetable = lambda t: t

    timezone = _import_timezone()

    def _create_task_instance(
        task: Operator,
        dag_id: str = "test_dag",
        run_id: str = "test_run",
        logical_date: str | datetime | None = "2024-12-01T01:00:00Z",
        start_date: str | datetime = "2024-12-01T01:00:00Z",
        run_type: str = "manual",
        try_number: int = 1,
        map_index: int | None = -1,
        upstream_map_indexes: dict[str, int | list[int] | None] | None = None,
        task_reschedule_count: int = 0,
        ti_id: UUID | None = None,
        conf: dict[str, Any] | None = None,
        should_retry: bool | None = None,
        max_tries: int | None = None,
    ) -> RuntimeTaskInstance:
        from airflow.sdk.api.datamodels._generated import DagRun, DagRunState, TIRunContext
        from airflow.utils.types import DagRunType

        if isinstance(logical_date, str):
            logical_date = timezone.parse(logical_date)
        else:
            logical_date = timezone.coerce_datetime(logical_date)
        if isinstance(start_date, str):
            start_date = timezone.parse(start_date)
        else:
            start_date = timezone.coerce_datetime(start_date)

        if TYPE_CHECKING:
            from pendulum import DateTime

            assert logical_date is None or isinstance(logical_date, DateTime)
            assert isinstance(start_date, DateTime)

        if not ti_id:
            ti_id = uuid7()

        if not task.has_dag():
            task.dag = DAG(dag_id=dag_id, start_date=timezone.datetime(2024, 12, 3))

        if TYPE_CHECKING:
            assert task.dag is not None

        data_interval_start = None
        data_interval_end = None

        timetable = coerce_to_core_timetable(task.dag.timetable)
        if run_type == DagRunType.MANUAL:
            if logical_date is not None:
                data_interval_start, data_interval_end = timetable.infer_manual_data_interval(
                    run_after=logical_date,
                )
        else:
            drinfo = timetable.next_dagrun_info(
                last_automated_data_interval=None,
                restriction=TimeRestriction(earliest=None, latest=None, catchup=False),
            )
            if drinfo:
                data_interval = drinfo.data_interval
                data_interval_start, data_interval_end = data_interval.start, data_interval.end

        dag_id = task.dag.dag_id
        task_retries = task.retries or 0
        run_after = data_interval_end or logical_date or timezone.utcnow()

        ti_context = TIRunContext(
            dag_run=DagRun.model_validate(
                {
                    "dag_id": dag_id,
                    "run_id": run_id,
                    "logical_date": logical_date,  # type: ignore
                    "data_interval_start": data_interval_start,
                    "data_interval_end": data_interval_end,
                    "start_date": start_date,  # type: ignore
                    "run_type": run_type,  # type: ignore
                    "run_after": run_after,  # type: ignore
                    "conf": conf,
                    "consumed_asset_events": [],
                    **({"state": DagRunState.RUNNING} if "state" in DagRun.model_fields else {}),
                }
            ),
            task_reschedule_count=task_reschedule_count,
            max_tries=task_retries if max_tries is None else max_tries,
            should_retry=should_retry if should_retry is not None else try_number <= task_retries,
            upstream_map_indexes=upstream_map_indexes,
        )

        if upstream_map_indexes is not None:
            ti_context.upstream_map_indexes = upstream_map_indexes

        compat_fields = {
            "requests_fd": 0,
            "sentry_integration": "",
        }

        startup_details = StartupDetails(
            ti=TaskInstance(
                id=ti_id,
                task_id=task.task_id,
                dag_id=dag_id,
                run_id=run_id,
                try_number=try_number,
                map_index=map_index,
                dag_version_id=uuid7(),
            ),
            dag_rel_path="",
            bundle_info=BundleInfo(name="anything", version="any"),
            ti_context=ti_context,
            start_date=start_date,  # type: ignore
            # Back-compat of task-sdk. Only affects us when we manually create these objects in tests.
            **{k: v for k, v in compat_fields.items() if k in StartupDetails.model_fields},  # type: ignore
        )

        ti = mocked_parse(startup_details, dag_id, task)
        return ti

    return _create_task_instance


@pytest.fixture
def run_task(create_runtime_ti, mock_supervisor_comms, spy_agency) -> RunTaskCallable:
    """
    Fixture to run a task without defining a dag file.

    This fixture builds on top of create_runtime_ti to provide a convenient way to execute tasks and get their results.

    The fixture provides:
    - run_task.state - Get the task state
    - run_task.msg - Get the task message
    - run_task.error - Get the task error
    - run_task.xcom.get(key) - Get an XCom value
    - run_task.xcom.assert_pushed(key, value, ...) - Assert an XCom was pushed

    Example usage: ::

        def test_custom_task(run_task):
            class MyTaskOperator(BaseOperator):
                def execute(self, context):
                    return "hello"

            task = MyTaskOperator(task_id="test_task")
            run_task(task)
            assert run_task.state == TaskInstanceState.SUCCESS
            assert run_task.error is None
    """
    import structlog

    from airflow.sdk.execution_time.task_runner import run
    from airflow.sdk.execution_time.xcom import XCom

    timezone = _import_timezone()

    # Set up spies once at fixture level
    if hasattr(XCom.set, "spy"):
        spy_agency.unspy(XCom.set)
    if hasattr(XCom.get_one, "spy"):
        spy_agency.unspy(XCom.get_one)
    spy_agency.spy_on(XCom.set, call_original=True)
    spy_agency.spy_on(
        XCom.get_one, call_fake=lambda cls, *args, **kwargs: _get_one_from_set_calls(*args, **kwargs)
    )

    def _get_one_from_set_calls(*args, **kwargs) -> Any | None:
        """Get the most recent value from XCom.set calls that matches the criteria."""
        key = kwargs.get("key")
        task_id = kwargs.get("task_id")
        dag_id = kwargs.get("dag_id")
        run_id = kwargs.get("run_id")
        map_index = kwargs.get("map_index") or -1

        for call in reversed(XCom.set.calls):
            if (
                call.kwargs.get("task_id") == task_id
                and call.kwargs.get("dag_id") == dag_id
                and call.kwargs.get("run_id") == run_id
                and call.kwargs.get("map_index") == map_index
            ):
                if call.args and len(call.args) >= 2:
                    call_key, value = call.args
                    if call_key == key:
                        return value
        return None

    class XComHelper:
        def __init__(self):
            self._ti = None

        def get(
            self,
            key: str,
            task_id: str | None = None,
            dag_id: str | None = None,
            run_id: str | None = None,
            map_index: int | None = None,
        ) -> Any:
            # Use task instance values as defaults
            task_id = task_id or self._ti.task_id
            dag_id = dag_id or self._ti.dag_id
            run_id = run_id or self._ti.run_id
            map_index = map_index if map_index is not None else self._ti.map_index

            return XCom.get_one(
                key=key,
                task_id=task_id,
                dag_id=dag_id,
                run_id=run_id,
                map_index=map_index,
            )

        def assert_pushed(
            self,
            key: str,
            value: Any,
            task_id: str | None = None,
            dag_id: str | None = None,
            run_id: str | None = None,
            map_index: int | None = None,
            **kwargs,
        ):
            """Assert that an XCom was pushed with the given key and value."""
            task_id = task_id or self._ti.task_id
            dag_id = dag_id or self._ti.dag_id
            run_id = run_id or self._ti.run_id
            map_index = map_index if map_index is not None else self._ti.map_index

            spy_agency.assert_spy_called_with(
                XCom.set,
                key,
                value,
                task_id=task_id,
                dag_id=dag_id,
                run_id=run_id,
                map_index=map_index,
                **kwargs,
            )

        def clear(self):
            """Clear all XCom calls."""
            if hasattr(XCom.set, "spy"):
                spy_agency.unspy(XCom.set)
            if hasattr(XCom.get_one, "spy"):
                spy_agency.unspy(XCom.get_one)

    class RunTaskWithXCom(RunTaskCallable):
        def __init__(self, create_runtime_ti):
            self.create_runtime_ti = create_runtime_ti
            self.xcom = XComHelper()
            self._state = None
            self._msg = None
            self._error = None
            self._ti = None
            self._dagrun = None
            self._context = None

        @property
        def state(self) -> TIState:
            """Get the task state."""
            return self._state

        @property
        def msg(self) -> ToSupervisor | None:
            """Get the task message to send to supervisor."""
            return self._msg

        @property
        def error(self) -> BaseException | None:
            """Get the error message if there was any."""
            return self._error

        @property
        def ti(self) -> RuntimeTaskInstance:
            return self._ti

        @property
        def dagrun(self) -> DagRunProtocol:
            return self._dagrun

        @property
        def context(self) -> Context:
            return self._context

        def __call__(
            self,
            task: BaseOperator,
            dag_id: str = "test_dag",
            run_id: str = "test_run",
            logical_date: datetime | None = None,
            start_date: datetime | None = None,
            run_type: str = "manual",
            try_number: int = 1,
            map_index: int | None = -1,
            ti_id: UUID | None = None,
            max_tries: int | None = None,
            context_update: dict[str, Any] | None = None,
        ) -> tuple[TIState, ToSupervisor | None, BaseException | None]:
            now = timezone.utcnow()
            if logical_date is None:
                logical_date = now

            if start_date is None:
                start_date = now

            ti = self.create_runtime_ti(
                task=task,
                dag_id=dag_id,
                run_id=run_id,
                logical_date=logical_date,
                start_date=start_date,
                run_type=run_type,
                try_number=try_number,
                map_index=map_index,
                ti_id=ti_id,
                max_tries=max_tries,
            )

            context = ti.get_template_context()
            if context_update:
                context.update(context_update)
            log = structlog.get_logger(logger_name="task")

            # Store the task instance for XCom operations
            self.xcom._ti = ti

            # Run the task
            state, msg, error = run(ti, context, log)
            self._state = state
            self._msg = msg
            self._error = error
            self._ti = ti
            self._dagrun = context.get("dag_run")
            self._context = context

            return state, msg, error

    return RunTaskWithXCom(create_runtime_ti)


@pytest.fixture
def mock_xcom_backend():
    with mock.patch("airflow.sdk.execution_time.task_runner.XCom", create=True) as xcom_backend:
        yield xcom_backend


@pytest.fixture
def testing_dag_bundle():
    from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

    if AIRFLOW_V_3_0_PLUS:
        from sqlalchemy import func, select

        from airflow.models.dagbundle import DagBundleModel
        from airflow.utils.session import create_session

        with create_session() as session:
            if (
                session.scalar(
                    select(func.count()).select_from(DagBundleModel).where(DagBundleModel.name == "testing")
                )
                == 0
            ):
                testing = DagBundleModel(name="testing")
                session.add(testing)


@pytest.fixture
def testing_team():
    from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

    if AIRFLOW_V_3_0_PLUS:
        from sqlalchemy import select

        from airflow.models.team import Team
        from airflow.utils.session import create_session

        with create_session() as session:
            team = session.scalar(select(Team).where(Team.name == "testing"))
            if not team:
                team = Team(name="testing")
                session.add(team)
                session.flush()
            yield team


@pytest.fixture
def create_connection_without_db(monkeypatch):
    """
    Fixture to create connections for tests without using the database.

    This fixture uses monkeypatch to set the appropriate AIRFLOW_CONN_{conn_id} environment variable.
    """

    def _create_conn(connection, session=None):
        """Create connection using environment variable."""

        env_var_name = f"AIRFLOW_CONN_{connection.conn_id.upper()}"
        monkeypatch.setenv(env_var_name, connection.as_json())

    return _create_conn


def _import_timezone():
    try:
        from airflow.sdk import timezone
    except ImportError:
        try:
            from airflow._shared.timezones import timezone
        except ImportError:
            from airflow.utils import timezone
    return timezone


@pytest.fixture
def create_dag_without_db():
    def create_dag(dag_id: str):
        from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

        if AIRFLOW_V_3_0_PLUS:
            from airflow.sdk import DAG
        else:
            from airflow import DAG

        return DAG(dag_id=dag_id, schedule=None, render_template_as_native_obj=True)

    return create_dag


@pytest.fixture
def mock_task_instance():
    def _create_mock_task_instance(
        task_id: str = "test_task",
        dag_id: str = "test_dag",
        run_id: str = "test_run",
        try_number: int = 0,
        state: str = "running",
        max_tries: int = 0,
    ):
        from airflow.models import TaskInstance

        mock_ti = mock.MagicMock(spec=TaskInstance)
        mock_ti.task_id = task_id
        mock_ti.dag_id = dag_id
        mock_ti.run_id = run_id
        mock_ti.try_number = try_number
        mock_ti.state = state
        mock_ti.max_tries = max_tries
        return mock_ti

    return _create_mock_task_instance


@pytest.fixture
def listener_manager():
    """
    Fixture that provides a listener manager for tests.

    This fixture registers listeners with both the core listener manager
    (used by Jobs, DAG runs, etc.) and the SDK listener manager (used by
    task execution). This ensures listeners work correctly regardless of
    which code path calls them.

    Usage:
        def test_something(listener_manager):
            listener_manager(full_listener)
    """
    from airflow.listeners.listener import get_listener_manager as get_core_lm

    try:
        from airflow.sdk.listeners.listener import get_listener_manager as get_sdk_lm
    except ImportError:
        get_sdk_lm = None

    core_lm = get_core_lm()
    sdk_lm = get_sdk_lm() if get_sdk_lm else None

    core_lm.clear()
    if sdk_lm:
        sdk_lm.clear()

    def add_listener(listener):
        core_lm.add_listener(listener)
        if sdk_lm:
            sdk_lm.add_listener(listener)

    yield add_listener

    core_lm.clear()
    if sdk_lm:
        sdk_lm.clear()
