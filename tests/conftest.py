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
    home = os.getcwd()
    airflow_home = os.environ.get("AIRFLOW_HOME") or home
    os.environ["AIRFLOW_SOURCES"] = home
    os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = os.path.join(home, "tests", "dags")
    os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
    os.environ["AWS_DEFAULT_REGION"] = (
        os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"
    )

    print(f"Airflow home {airflow_home}\nHome of the user: {home}")

    # Initialize Airflow db if required
    pid_file = os.path.join(home, ".airflow_db_initialised")
    if request.config.option.db_init:
        print("Initializing the DB - forced with --with-db-init switch.")
        try:
            db.initdb()
        except:  # pylint: disable=bare-except # noqa
            print("Skipping db initialization because database already exists.")
        db.resetdb()
    elif not os.path.exists(pid_file):
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
        with open(pid_file, "w+"):
            pass
    else:
        print(
            "Skipping initializing of the DB as it was initialized already.\n"
            "You can re-initialize the database by adding --with-db-init flag when running tests."
        )

    # Initialize kerberos
    kerberos = os.environ.get("KRB5_KTNAME")
    if kerberos:
        subprocess.check_call(["kinit", "-kt", kerberos, "airflow"])
