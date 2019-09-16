# -*- coding: utf-8 -*-
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
import os
import subprocess
import unittest

from tests.contrib.utils.run_once_decorator import run_once
from tests.gcp.utils.gcp_authenticator import GcpAuthenticator

GCP_DAG_FOLDER = "airflow/gcp/example_dags"

AIRFLOW_MAIN_FOLDER = os.path.realpath(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir, os.pardir
    )
)

AIRFLOW_PARENT_FOLDER = os.path.realpath(
    os.path.join(AIRFLOW_MAIN_FOLDER, os.pardir, os.pardir, os.pardir)
)
ENV_FILE_RETRIEVER = os.path.join(
    AIRFLOW_PARENT_FOLDER, "get_system_test_environment_variables.py"
)

AIRFLOW_HOME = os.environ.get(
    "AIRFLOW_HOME", os.path.join(os.path.expanduser("~"), "airflow")
)

POSTGRES_LOCAL_EXECUTOR = os.path.realpath(
    os.path.join(
        AIRFLOW_HOME, "tests", "contrib", "operators", "postgres_local_executor.cfg"
    )
)


SKIP_TEST_WARNING = """
The test is only run when the test is run in environment with GCP-system-tests enabled
environment. You can enable it in one of two ways:

* Set GCP_CONFIG_DIR environment variable to point to the GCP configuration
  directory which keeps variables.env file with environment variables to set
  and keys directory which keeps service account keys in .json format
* Run this test within automated environment variable workspace where
  config directory is checked out next to the airflow one.

"""

SKIP_LONG_TEST_WARNING = """
The test is only run when the test is run in with GCP-system-tests enabled
environment. And environment variable GCP_ENABLE_LONG_TESTS is set to True.
You can enable it in one of two ways:

* Set GCP_CONFIG_DIR environment variable to point to the GCP configuration
  directory which keeps variables.env file with environment variables to set
  and keys directory which keeps service account keys in .json format and
  set GCP_ENABLE_LONG_TESTS to True
* Run this test within automated environment variable workspace where
  config directory is checked out next to the airflow one.
"""


LOCAL_EXECUTOR_WARNING = """
The test requires local executor. Please set AIRFLOW_CONFIG variable to '{}'
and make sure you have a Postgres server running locally and
airflow/airflow.db database created.

You can create the database via these commands:
'createuser root'
'createdb airflow/airflow.db`

"""


class RetrieveVariables:
    """
    Retrieve environment variables from parent directory retriever - it should be
    in the path ${AIRFLOW_SOURCES}/../../get_system_test_environment_variables.py
    and it should print all the variables in form of key=value to the stdout
    """

    @staticmethod
    @run_once
    def retrieve_variables():
        if os.path.isfile(ENV_FILE_RETRIEVER):
            if os.environ.get("AIRFLOW__CORE__UNIT_TEST_MODE"):
                raise Exception("Please unset the AIRFLOW__CORE__UNIT_TEST_MODE")
            variables = subprocess.check_output([ENV_FILE_RETRIEVER]).decode("utf-8")
            print("Applying variables retrieved")
            for line in variables.split("\n"):
                try:
                    variable, key = line.split("=")
                except ValueError:
                    continue
                print("{}={}".format(variable, key))
                os.environ[variable] = key


RetrieveVariables.retrieve_variables()


def skip_gcp_system(
    service_key: str,
    long_lasting: bool = False,
    require_local_executor: bool = False,
):
    """
    Decorator for skipping GCP system tests.

    :param service_key: name of the service key that will be used to provide credentials
    :type service_key: str
    :param long_lasting: set True if a test take relatively long time
    :type long_lasting: bool
    :param require_local_executor: set True if test config must use local executor
    :type require_local_executor: bool
    """
    if GcpAuthenticator(service_key).full_key_path is None:
        return unittest.skip(SKIP_TEST_WARNING)

    if long_lasting and os.environ.get("GCP_ENABLE_LONG_TESTS") == "True":
        return unittest.skip(SKIP_LONG_TEST_WARNING)

    if require_local_executor and POSTGRES_LOCAL_EXECUTOR != os.environ.get(
        "AIRFLOW_CONFIG"
    ):
        return unittest.skip(LOCAL_EXECUTOR_WARNING.format(POSTGRES_LOCAL_EXECUTOR))

    return lambda cls: cls
