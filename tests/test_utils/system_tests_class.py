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
import shutil
import sys
from datetime import datetime
from unittest import TestCase

from airflow.configuration import AIRFLOW_HOME, AirflowConfigParser, get_airflow_config
from airflow.exceptions import AirflowException
from airflow.models.dagbag import DagBag
from airflow.utils.file import mkdirs
from airflow.utils.log.logging_mixin import LoggingMixin
from tests.test_utils import AIRFLOW_MAIN_FOLDER
from tests.utils.logging_command_executor import get_executor

DEFAULT_DAG_FOLDER = os.path.join(AIRFLOW_MAIN_FOLDER, "airflow", "example_dags")


def resolve_logs_folder() -> str:
    """
    Returns LOGS folder specified in current Airflow config.
    """
    config_file = get_airflow_config(AIRFLOW_HOME)
    conf = AirflowConfigParser()
    conf.read(config_file)
    try:
        logs = conf.get("logging", "base_log_folder")
    except AirflowException:
        try:
            logs = conf.get("core", "base_log_folder")
        except AirflowException:
            logs = os.path.join(AIRFLOW_HOME, 'logs')
    return logs


class SystemTest(TestCase, LoggingMixin):
    @staticmethod
    def execute_cmd(*args, **kwargs):
        executor = get_executor()
        return executor.execute_cmd(*args, **kwargs)

    @staticmethod
    def check_output(*args, **kwargs):
        executor = get_executor()
        return executor.check_output(*args, **kwargs)

    def setUp(self) -> None:
        """
        We want to avoid random errors while database got reset - those
        Are apparently triggered by parser trying to parse DAGs while
        The tables are dropped. We move the dags temporarily out of the dags folder
        and move them back after reset.

        We also remove all logs from logs directory to have a clear log state and see only logs from this
        test.
        """
        print()
        print("Removing all log files except previous_runs")
        print()
        logs_folder = resolve_logs_folder()
        files = os.listdir(logs_folder)
        for file in files:
            file_path = os.path.join(logs_folder, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
            elif os.path.isdir(file) and not file == "previous_runs":
                shutil.rmtree(file_path, ignore_errors=True)
        super().setUp()

    def tearDown(self) -> None:
        """
        We save the logs to a separate directory so that we can see them later.
        """
        date_str = datetime.now().strftime("%Y-%m-%d_%H_%M_%S")
        logs_folder = resolve_logs_folder()
        print()
        print(f"Saving all log files to {logs_folder}/previous_runs/{date_str}")
        print()
        target_dir = os.path.join(logs_folder, "previous_runs", date_str)
        mkdirs(target_dir, 0o755)
        files = os.listdir(logs_folder)
        for file in files:
            if file != "previous_runs":
                file_path = os.path.join(logs_folder, file)
                shutil.move(file_path, target_dir)
        super().tearDown()

    @staticmethod
    def _print_all_log_files():
        print()
        print("Printing all log files")
        print()
        logs_folder = resolve_logs_folder()
        for dirpath, _, filenames in os.walk(logs_folder):
            if "/previous_runs" not in dirpath:
                for name in filenames:
                    filepath = os.path.join(dirpath, name)
                    print()
                    print(f" ================ Content of {filepath} ===============================")
                    print()
                    with open(filepath, "r") as f:
                        print(f.read())

    def run_dag(self, dag_id: str, dag_folder: str = DEFAULT_DAG_FOLDER) -> None:
        """
        Runs example dag by it's ID.

        :param dag_id: id of a DAG to be run
        :type dag_id: str
        :param dag_folder: directory where to look for the specific DAG. Relative to AIRFLOW_HOME.
        :type dag_folder: str
        """
        if os.environ.get("RUN_AIRFLOW_1_10") == "true":
            # For system tests purpose we are changing airflow/providers
            # to side packages path of the installed providers package
            python = f"python{sys.version_info.major}.{sys.version_info.minor}"
            dag_folder = dag_folder.replace(
                "/opt/airflow/airflow/providers",
                f"/usr/local/lib/{python}/site-packages/airflow/providers",
            )
        self.log.info("Looking for DAG: %s in %s", dag_id, dag_folder)
        dag_bag = DagBag(dag_folder=dag_folder, include_examples=False)
        dag = dag_bag.get_dag(dag_id)
        if dag is None:
            raise AirflowException(
                "The Dag {dag_id} could not be found. It's either an import problem,"
                "wrong dag_id or DAG is not in provided dag_folder."
                "The content of the {dag_folder} folder is {content}".format(
                    dag_id=dag_id,
                    dag_folder=dag_folder,
                    content=os.listdir(dag_folder),
                )
            )

        self.log.info("Attempting to run DAG: %s", dag_id)
        dag.clear(reset_dag_runs=True)
        try:
            dag.run(ignore_first_depends_on_past=True, verbose=True)
        except Exception:
            self._print_all_log_files()
            raise

    @staticmethod
    def create_dummy_file(filename, dir_path="/tmp"):
        os.makedirs(dir_path, exist_ok=True)
        full_path = os.path.join(dir_path, filename)
        with open(full_path, "wb") as f:
            f.write(os.urandom(1 * 1024 * 1024))

    @staticmethod
    def delete_dummy_file(filename, dir_path):
        full_path = os.path.join(dir_path, filename)
        try:
            os.remove(full_path)
        except FileNotFoundError:
            pass
        if dir_path != "/tmp":
            shutil.rmtree(dir_path, ignore_errors=True)
