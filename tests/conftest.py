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

import os
import subprocess
import sys

import pytest

from airflow.utils import db


@pytest.fixture()
def reset_environment():
    """
    Resets env variables.
    """
    init_env = os.environ.copy()
    yield
    changed_env = os.environ
    for key in changed_env:
        if key not in init_env:
            del os.environ[key]
        else:
            os.environ[key] = init_env[key]


@pytest.fixture()
def reset_db():
    """
    Resets Airflow db.
    """
    db.resetdb()
    yield


def pytest_addoption(parser):
    """
    Add options parser for custom plugins
    """
    group = parser.getgroup("airflow")
    group.addoption(
        "--with-db-init",
        action="store_true",
        dest="db_init",
        help="Forces database initialization before tests",
    )
    group.addoption(
        "--integrations",
        action="store",
        metavar="INTEGRATIONS",
        help="only run tests matching comma separated integrations: "
             "[cassandra,mongo,openldap,rabbitmq,redis]. "
             "Use 'all' to select all integrations.",
    )
    group.addoption(
        "--backend",
        action="store",
        metavar="BACKEND",
        help="only run tests matching the backend: [sqlite,postgres,mysql].",
    )
    group.addoption(
        "--runtime",
        action="store",
        metavar="RUNTIME",
        help="only run tests matching the runtime: [kubernetes].",
    )


@pytest.fixture(autouse=True, scope="session")
def breeze_test_helper(request):
    """
    Helper that setups Airflow testing environment. It does the same thing
    as the old 'run-tests' script.
    """

    # fixme: this should use some other env variable ex. RUNNING_ON_K8S
    if os.environ.get("SKIP_INIT_DB"):
        print("Skipping db initialization. Tests do not require database")
        return

    print(" AIRFLOW ".center(60, "="))

    # Setup test environment for breeze
    home = os.path.expanduser("~")
    airflow_home = os.environ.get("AIRFLOW_HOME") or os.path.join(home, "airflow")
    tests_directory = os.path.dirname(os.path.realpath(__file__))

    os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = os.path.join(tests_directory, "dags")
    os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
    os.environ["AWS_DEFAULT_REGION"] = (
        os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"
    )

    print(f"Home of the user: {home}\nAirflow home {airflow_home}")

    # Initialize Airflow db if required
    lock_file = os.path.join(airflow_home, ".airflow_db_initialised")
    if request.config.option.db_init:
        print("Initializing the DB - forced with --with-db-init switch.")
        try:
            db.initdb()
        except:  # pylint: disable=bare-except # noqa
            print("Skipping db initialization because database already exists.")
        db.resetdb()
    elif not os.path.exists(lock_file):
        print(
            "Initializing the DB - first time after entering the container.\n"
            "You can force re-initialization the database by adding --with-db-init switch to run-tests."
        )
        try:
            db.initdb()
        except:  # pylint: disable=bare-except # noqa
            print("Skipping db initialization because database already exists.")
        db.resetdb()
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
            subprocess.check_call(["kinit", "-kt", kerberos, "airflow"])
        else:
            print("Kerberos enabled! Please setup KRB5_KTNAME environment variable")
            sys.exit(1)


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "integration(name): mark test to run with named integration"
    )
    config.addinivalue_line(
        "markers", "backend(name): mark test to run with named backend"
    )
    config.addinivalue_line(
        "markers", "runtime(name): mark test to run with named runtime"
    )


def skip_if_not_marked_with_integration(selected_integrations, item):
    for marker in item.iter_markers(name="integration"):
        integration_name = marker.args[0]
        if integration_name in selected_integrations or "all" in selected_integrations:
            return
    pytest.skip("The test is skipped because it does not have the right integration marker. "
                "Only tests marked with pytest.mark.integration(INTEGRATION) are run with INTEGRATION"
                " being one of {}. {item}".
                format(selected_integrations, item=item))


def skip_if_not_marked_with_backend(selected_backend, item):
    for marker in item.iter_markers(name="backend"):
        backend_names = marker.args
        if selected_backend in backend_names:
            return
    pytest.skip("The test is skipped because it does not have the right backend marker "
                "Only tests marked with pytest.mark.backend('{}') are run"
                ": {item}".
                format(selected_backend, item=item))


def skip_if_not_marked_with_runtime(selected_runtime, item):
    for marker in item.iter_markers(name="runtime"):
        runtime_name = marker.args[0]
        if runtime_name == selected_runtime:
            return
    pytest.skip("The test is skipped because it has not been selected via --runtime switch. "
                "Only tests marked with pytest.mark.runtime('{}') are run: {item}".
                format(selected_runtime, item=item))


def skip_if_integration_disabled(marker, item):
    integration_name = marker.args[0]
    environment_variable_name = "INTEGRATION_" + integration_name.upper()
    environment_variable_value = os.environ.get(environment_variable_name)
    if not environment_variable_value or environment_variable_value != "true":
        pytest.skip("The test requires {integration_name} integration started and "
                    "{} environment variable to be set to true (it is '{}')."
                    " It can be set by specifying '--integration {integration_name}' at breeze startup"
                    ": {item}".
                    format(environment_variable_name, environment_variable_value,
                           integration_name=integration_name, item=item))


def skip_if_runtime_disabled(marker, item):
    runtime_name = marker.args[0]
    environment_variable_name = "RUNTIME"
    environment_variable_value = os.environ.get(environment_variable_name)
    if not environment_variable_value or environment_variable_value != runtime_name:
        pytest.skip("The test requires {runtime_name} integration started and "
                    "{} environment variable to be set to true (it is '{}')."
                    " It can be set by specifying '--environment {runtime_name}' at breeze startup"
                    ": {item}".
                    format(environment_variable_name, environment_variable_value,
                           runtime_name=runtime_name, item=item))


def skip_if_wrong_backend(marker, item):
    valid_backend_names = marker.args
    environment_variable_name = "BACKEND"
    environment_variable_value = os.environ.get(environment_variable_name)
    if not environment_variable_value or environment_variable_value not in valid_backend_names:
        pytest.skip("The test requires one of {valid_backend_names} backend started and "
                    "{} environment variable to be set to true (it is '{}')."
                    " It can be set by specifying backend at breeze startup"
                    ": {item}".
                    format(environment_variable_name, environment_variable_value,
                           valid_backend_names=valid_backend_names, item=item))


def pytest_runtest_setup(item):
    selected_integrations = item.config.getoption("--integrations")
    selected_integrations_list = selected_integrations.split(",") if selected_integrations else []
    for marker in item.iter_markers(name="integration"):
        skip_if_integration_disabled(marker, item)
    if selected_integrations_list:
        skip_if_not_marked_with_integration(selected_integrations, item)
    for marker in item.iter_markers(name="backend"):
        skip_if_wrong_backend(marker, item)
    selected_backend = item.config.getoption("--backend")
    if selected_backend:
        skip_if_not_marked_with_backend(selected_backend, item)
    for marker in item.iter_markers(name="runtime"):
        skip_if_runtime_disabled(marker, item)
    selected_runtime = item.config.getoption("--runtime")
    if selected_runtime:
        skip_if_not_marked_with_runtime(selected_runtime, item)
