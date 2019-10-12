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
from glob import glob
from shutil import move
from tempfile import mkdtemp

from airflow import AirflowException, LoggingMixin, models
from airflow.utils import db as db_utils
from airflow.utils.timezone import datetime
from tests.contrib.utils.run_once_decorator import run_once
from tests.gcp.utils.gcp_authenticator import GcpAuthenticator

AIRFLOW_MAIN_FOLDER = os.path.realpath(os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    os.pardir, os.pardir, os.pardir))

AIRFLOW_PARENT_FOLDER = os.path.realpath(os.path.join(AIRFLOW_MAIN_FOLDER,
                                                      os.pardir, os.pardir, os.pardir))
ENV_FILE_RETRIEVER = os.path.join(AIRFLOW_PARENT_FOLDER,
                                  "get_system_test_environment_variables.py")


# Retrieve environment variables from parent directory retriever - it should be
# in the path ${AIRFLOW_SOURCES}/../../get_system_test_environment_variables.py
# and it should print all the variables in form of key=value to the stdout
class RetrieveVariables:
    @staticmethod
    @run_once
    def retrieve_variables():
        if os.path.isfile(ENV_FILE_RETRIEVER):
            if os.environ.get('AIRFLOW__CORE__UNIT_TEST_MODE'):
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

DEFAULT_DATE = datetime(2015, 1, 1)

GCP_OPERATORS_EXAMPLES_DAG_FOLDER = os.path.join(
    AIRFLOW_MAIN_FOLDER, "airflow", "gcp", "example_dags")

OPERATORS_EXAMPLES_DAG_FOLDER = os.path.join(
    AIRFLOW_MAIN_FOLDER, "airflow", "example_dags")

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME',
                              os.path.join(os.path.expanduser('~'), 'airflow'))

DAG_FOLDER = os.path.join(AIRFLOW_HOME, "dags")


SKIP_TEST_WARNING = """
The test is only run when the test is run in with GCP-system-tests enabled
environment. You can enable it in one of two ways:

* Set GCP_CONFIG_DIR environment variable to point to the GCP configuration
  directory which keeps variables.env file with environment variables to set
  and keys directory which keeps service account keys in .json format
* Run this test within automated environment variable workspace where
  config directory is checked out next to the airflow one.

""".format(__file__)

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
""".format(__file__)


class TestBaseGcpSystem(unittest.TestCase, LoggingMixin):
    def __init__(self,
                 method_name,
                 gcp_key,
                 project_extra=None):
        super().__init__(methodName=method_name)
        self.gcp_authenticator = GcpAuthenticator(gcp_key=gcp_key,
                                                  project_extra=project_extra)
        self.setup_called = False

    @staticmethod
    def skip_check(key_name):
        return GcpAuthenticator(key_name).full_key_path is None

    @staticmethod
    def skip_long(key_name):
        if os.environ.get('GCP_ENABLE_LONG_TESTS') == 'True':
            return GcpAuthenticator(key_name).full_key_path is None
        return True

    def setUp(self):
        self.gcp_authenticator.gcp_store_authentication()
        self.gcp_authenticator.gcp_authenticate()
        # We checked that authentication works. Ne we revoke it to make
        # sure we are not relying on the default authentication
        self.gcp_authenticator.gcp_revoke_authentication()
        self.setup_called = True

    # noinspection PyPep8Naming
    def tearDown(self):
        self.gcp_authenticator.gcp_restore_authentication()


class TestDagGcpSystem(TestBaseGcpSystem):
    def __init__(self,
                 method_name,
                 gcp_key,
                 dag_id=None,
                 dag_name=None,
                 require_local_executor=False,
                 example_dags_folder=GCP_OPERATORS_EXAMPLES_DAG_FOLDER,
                 project_extra=None):
        super().__init__(method_name=method_name,
                         gcp_key=gcp_key,
                         project_extra=project_extra)
        self.dag_id = dag_id
        self.dag_name = self.dag_id + '.py' if not dag_name else dag_name
        self.example_dags_folder = example_dags_folder
        self.require_local_executor = require_local_executor
        self.temp_dir = None
        self.args = {}

    @staticmethod
    def _get_dag_folder():
        return DAG_FOLDER

    @staticmethod
    def _get_files_to_link(path):
        """
        Returns all file names (note - file names not paths)
        that have the same base name as the .py dag file (for example dag_name.sql etc.)
        :param path: path to the dag file.
        :return: list of files matching the base name
        """
        prefix, ext = os.path.splitext(path)
        assert ext == '.py', "Dag name should be a .py file and is {} file".format(ext)
        files_to_link = []
        for file in glob(prefix + ".*"):
            files_to_link.append(os.path.basename(file))
        return files_to_link

    def _symlink_dag_and_associated_files(self, remove=False):
        target_folder = self._get_dag_folder()
        source_path = os.path.join(self.example_dags_folder, self.dag_name)
        for file_name in self._get_files_to_link(source_path):
            source_path = os.path.join(self.example_dags_folder, file_name)
            target_path = os.path.join(target_folder, file_name)
            if remove:
                try:
                    self.log.info("Remove symlink: %s -> %s", target_path, source_path)
                    os.remove(target_path)
                except OSError:
                    pass
            else:
                if not os.path.exists(target_path):
                    self.log.info("Symlink: %s -> %s ", target_path, source_path)
                    os.symlink(source_path, target_path)
                else:
                    self.log.info("Symlink %s already exists. Not symlinking it.", target_path)

    def _store_dags_to_temporary_directory(self):
        dag_folder = self._get_dag_folder()
        self.temp_dir = mkdtemp()
        self.log.info("Storing DAGS from %s to temporary directory %s", dag_folder, self.temp_dir)
        try:
            os.mkdir(dag_folder)
        except OSError:
            pass
        for file in os.listdir(dag_folder):
            move(os.path.join(dag_folder, file), os.path.join(self.temp_dir, file))

    def _restore_dags_from_temporary_directory(self):
        dag_folder = self._get_dag_folder()
        self.log.info("Restoring DAGS to %s from temporary directory %s", dag_folder, self.temp_dir)
        for file in os.listdir(self.temp_dir):
            move(os.path.join(self.temp_dir, file), os.path.join(dag_folder, file))

    def _run_dag(self, dag_id=None):
        self.log.info("Attempting to run DAG: %s", self.dag_id)
        if not self.setup_called:
            raise AirflowException("Please make sure to call super.setUp() in your "
                                   "test class!")
        dag_folder = self._get_dag_folder()
        dag_bag = models.DagBag(dag_folder=dag_folder, include_examples=False)
        self.args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        dag = dag_bag.get_dag(self.dag_id or dag_id)
        if dag is None:
            raise AirflowException(
                "The Dag {} could not be found. It's either an import problem or "
                "the dag {} was not symlinked to the DAGs folder. "
                "The content of the {} folder is {}".
                format(self.dag_id,
                       self.dag_name,
                       dag_folder,
                       os.listdir(dag_folder)))
        dag.clear(reset_dag_runs=True)
        dag.run(ignore_first_depends_on_past=True, verbose=True)

    @staticmethod
    def _check_local_executor_setup():
        postgres_path = os.path.realpath(os.path.join(
            AIRFLOW_MAIN_FOLDER,
            "tests", "contrib", "operators", "postgres_local_executor.cfg"))
        if postgres_path != os.environ.get('AIRFLOW_CONFIG'):
            raise AirflowException(
                """
Please set AIRFLOW_CONFIG variable to '{}'
and make sure you have a Postgres server running locally and
airflow/airflow.db database created.

You can create the database via these commands:
'createuser root'
'createdb airflow/airflow.db`

""".format(postgres_path))

    # noinspection PyPep8Naming
    def setUp(self):
        if self.require_local_executor:
            self._check_local_executor_setup()
        try:
            # We want to avoid random errors while database got reset - those
            # Are apparently triggered by parser trying to parse DAGs while
            # The tables are dropped. We move the dags temporarily out of the dags folder
            # and move them back after reset
            self._store_dags_to_temporary_directory()
            try:
                db_utils.upgradedb()
                db_utils.resetdb()
            finally:
                self._restore_dags_from_temporary_directory()
            self._symlink_dag_and_associated_files()
            super().setUp()

        except Exception as e:
            # In case of any error during setup - restore the authentication
            self.gcp_authenticator.gcp_restore_authentication()
            raise e

    def tearDown(self):
        self._symlink_dag_and_associated_files(remove=True)
        super().tearDown()
