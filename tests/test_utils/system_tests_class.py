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
from contextlib import ContextDecorator
from datetime import datetime
from shutil import move
from tempfile import mkdtemp
from unittest import TestCase

from airflow.configuration import AIRFLOW_HOME, AirflowConfigParser, get_airflow_config
from airflow.exceptions import AirflowException
from airflow.models.dagbag import DagBag
from airflow.utils.file import mkdirs
from airflow.utils.log.logging_mixin import LoggingMixin
from tests.contrib.utils.logging_command_executor import get_executor
from tests.test_utils import AIRFLOW_MAIN_FOLDER

DEFAULT_DAG_FOLDER = os.path.join(AIRFLOW_MAIN_FOLDER, "airflow", "example_dags")


def resolve_dags_folder() -> str:
    """
    Returns DAG folder specified in current Airflow config.
    """
    config_file = get_airflow_config(AIRFLOW_HOME)
    conf = AirflowConfigParser()
    conf.read(config_file)
    try:
        dags = conf.get("core", "dags_folder")
    except AirflowException:
        dags = os.path.join(AIRFLOW_HOME, 'dags')
    return dags


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


class EmptyDagsDirectory(  # pylint: disable=invalid-name
    ContextDecorator, LoggingMixin
):
    """
    Context manager that temporally removes DAGs from provided directory.
    """

    def __init__(self, dag_directory: str) -> None:
        super().__init__()
        self.dag_directory = dag_directory
        self.temp_dir = mkdtemp()

    def __enter__(self) -> str:
        self._store_dags_to_temporary_directory(self.dag_directory, self.temp_dir)
        return self.temp_dir

    def __exit__(self, *args, **kwargs) -> None:
        self._restore_dags_from_temporary_directory(self.dag_directory, self.temp_dir)

    def _store_dags_to_temporary_directory(
        self, dag_folder: str, temp_dir: str
    ) -> None:
        self.log.info(
            "Storing DAGS from %s to temporary directory %s", dag_folder, temp_dir
        )
        try:
            os.mkdir(dag_folder)
        except OSError:
            pass
        for file in os.listdir(dag_folder):
            move(os.path.join(dag_folder, file), os.path.join(temp_dir, file))

    def _restore_dags_from_temporary_directory(
        self, dag_folder: str, temp_dir: str
    ) -> None:
        self.log.info(
            "Restoring DAGS to %s from temporary directory %s", dag_folder, temp_dir
        )
        for file in os.listdir(temp_dir):
            move(os.path.join(temp_dir, file), os.path.join(dag_folder, file))


class SystemTest(TestCase, LoggingMixin):
    @staticmethod
    def execute_cmd(*args, **kwargs):
        executor = get_executor()
        return executor.execute_cmd(*args, **kwargs)

    def setUp(self) -> None:
        """
        We want to avoid random errors while database got reset - those
        Are apparently triggered by parser trying to parse DAGs while
        The tables are dropped. We move the dags temporarily out of the dags folder
        and move them back after reset.

        We also remove all logs from logs directory to have a clear log state and see only logs from this
        test.
        """
        dag_folder = resolve_dags_folder()
        with EmptyDagsDirectory(dag_folder):
            self.initial_db_init()
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
        super().setUp()

    def initial_db_init(self):
        if os.environ.get("RUN_AIRFLOW_1_10"):
            print("Attempting to reset the db using airflow command")
            os.system("airflow resetdb -y")
        else:
            from airflow.utils import db
            db.resetdb()

    def _print_all_log_files(self):
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

    def correct_imports_for_airflow_1_10(self, directory):
        for dirpath, _, filenames in os.walk(directory):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                if filepath.endswith(".py"):
                    self.replace_airflow_1_10_imports(filepath)

    def replace_airflow_1_10_imports(self, filepath):
        replacements = [
            ("airflow.operators.bash", "airflow.operators.bash_operator"),
            ("airflow.operators.python", "airflow.operators.python_operator"),
        ]
        with open(filepath, "rt") as file:
            data = file.read()
        for replacement in replacements:
            data = data.replace(replacement[0], replacement[1])
        with open(filepath, "wt") as file:
            file.write(data)

    def run_dag(self, dag_id: str, dag_folder: str = DEFAULT_DAG_FOLDER) -> None:
        """
        Runs example dag by it's ID.

        :param dag_id: id of a DAG to be run
        :type dag_id: str
        :param dag_folder: directory where to look for the specific DAG. Relative to AIRFLOW_HOME.
        :type dag_folder: str
        """
        if os.environ.get("RUN_AIRFLOW_1_10"):
            # For system tests purpose we are mounting airflow/providers to /providers folder
            # So that we can get example_dags from there
            dag_folder = dag_folder.replace("/opt/airflow/airflow/providers", "/providers")
            temp_dir = mkdtemp()
            os.rmdir(temp_dir)
            shutil.copytree(dag_folder, temp_dir)
            dag_folder = temp_dir
            self.correct_imports_for_airflow_1_10(temp_dir)
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
