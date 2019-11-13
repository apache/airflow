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
import argparse
import os
import shutil
from contextlib import ContextDecorator
from shutil import move
from tempfile import mkdtemp
from typing import Callable, Dict, Optional

import pytest

from airflow import AirflowException, models
from airflow.configuration import AIRFLOW_HOME, AirflowConfigParser, get_airflow_config
from airflow.utils import db
from airflow.utils.log.logging_mixin import LoggingMixin
from tests.contrib.utils.logging_command_executor import LoggingCommandExecutor

AIRFLOW_MAIN_FOLDER = os.path.realpath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir)
)
DEFAULT_DAG_FOLDER = os.path.join(AIRFLOW_MAIN_FOLDER, "airflow", "example_dags")

SKIP_SYSTEM_TEST_WARNING = """Skipping system test.
To allow system test set ENABLE_SYSTEM_TESTS=true.
"""


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
        dags = os.path.join(AIRFLOW_HOME, "dags")
    return dags


class empty_dags_directory(  # pylint: disable=invalid-name
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


class DevCli:
    commands: Dict[str, Callable] = {}

    @classmethod
    def commands_registry(cls) -> Callable:
        """
        Decorator that register commands tha will be available
        in system tests cli.
        """

        def wraps(func):
            cls.commands[func.__name__] = func
            return func

        return wraps

    @classmethod
    def cli(cls) -> None:
        """
        This is system test helper that help run setup
        and teardown methods while developing.
        """
        desc = """
        This is system test helper that will help you run
        setup and teardown methods while developing.\n
        """
        parser = argparse.ArgumentParser(prog=desc)
        subparsers = parser.add_subparsers()

        for cmd, func in cls.commands.items():
            sub = subparsers.add_parser(cmd, help=func.__doc__)
            sub.set_defaults(func=func)
            parser.add_argument(cmd, action="store_true")

        args = parser.parse_args()
        args.func()


class SystemTest(DevCli):
    # This can be used by classes that inherit from this one
    # to create class methods.
    executor = LoggingCommandExecutor()

    @staticmethod
    def skip(*args, **kwargs) -> Callable:
        """
        Skip decorator for system tests. Tests that use external
        services should override this decorator with proper checks.
        """
        if os.environ.get("ENABLE_SYSTEM_TESTS") != "true":
            pytest.skip(SKIP_SYSTEM_TEST_WARNING)
        return lambda cls: cls

    @staticmethod
    def run_dag(dag_id: str, dag_folder: str = DEFAULT_DAG_FOLDER) -> None:
        """
        Runs example dag by it's ID.

        :param dag_id: id of a DAG to be run
        :type dag_id: str
        :param dag_folder: directory where to look for the specific DAG. Relative to AIRFLOW_HOME.
        :type dag_folder: str
        """

        # We want to avoid random errors while database got reset - those
        # Are apparently triggered by parser trying to parse DAGs while
        # The tables are dropped. We move the dags temporarily out of the dags folder
        # and move them back after reset
        initial_dag_folder = resolve_dags_folder()
        with empty_dags_directory(initial_dag_folder):
            db.resetdb()

        # Run example dag
        dag_bag = models.DagBag(dag_folder=dag_folder, include_examples=False)
        dag = dag_bag.get_dag(dag_id)
        if dag is None:
            raise AirflowException(
                "The Dag {dag_id} could not be found. It's either an import problem,"
                "wrong dag_id or DAG is not in provided dag_folder."
                "The content of the {dag_folder} folder is {content}".format(
                    dag_id=dag_id, dag_folder=dag_folder, content=os.listdir(dag_folder)
                )
            )

        dag.clear(reset_dag_runs=True)
        dag.run(ignore_first_depends_on_past=True, verbose=True)

    @classmethod
    def create_temp_file(cls, filename: str, dir_path: str = "/tmp", content: Optional[str] = None) -> None:
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
            full_path = os.path.join(dir_path, filename)
        else:
            full_path = filename

        if content:
            with open(full_path, "w+") as f:
                f.write(content)
        else:
            with open(full_path, "wb") as f:  # type: ignore
                f.write(os.urandom(1 * 1024 * 1024))  # type: ignore

    @classmethod
    def delete_temp_file(cls, filename: str, dir_path: Optional[str] = None) -> None:
        full_path = os.path.join(dir_path, filename) if dir_path else filename
        try:
            os.remove(full_path)
        except FileNotFoundError:
            pass
        if dir_path and dir_path != "/tmp":
            shutil.rmtree(dir_path, ignore_errors=True)

    @classmethod
    def delete_temp_dir(cls, dir_path: str) -> None:
        if dir_path != "/tmp":
            shutil.rmtree(dir_path, ignore_errors=True)


command = SystemTest.commands_registry()
