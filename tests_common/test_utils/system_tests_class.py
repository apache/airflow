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
from __future__ import annotations

import contextlib
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path

import pytest

from airflow.configuration import AIRFLOW_HOME, AirflowConfigParser, get_airflow_config
from airflow.exceptions import AirflowException
from tests_common.test_utils import AIRFLOW_MAIN_FOLDER
from tests_common.test_utils.logging_command_executor import get_executor

DEFAULT_DAG_FOLDER = os.path.join(AIRFLOW_MAIN_FOLDER, "airflow", "example_dags")


def get_default_logs_if_none(logs: str | None) -> str:
    if logs is None:
        return os.path.join(AIRFLOW_HOME, "logs")
    return logs


def resolve_logs_folder() -> str:
    """Return LOGS folder specified in current Airflow config."""
    config_file = get_airflow_config(AIRFLOW_HOME)
    conf = AirflowConfigParser()
    conf.read(config_file)
    try:
        return get_default_logs_if_none(conf.get("logging", "base_log_folder"))
    except AirflowException:
        try:
            return get_default_logs_if_none(conf.get("core", "base_log_folder"))
        except AirflowException:
            pass
    return get_default_logs_if_none(None)


class SystemTest:
    """Base class for system tests."""

    log: logging.Logger

    @staticmethod
    @pytest.fixture(autouse=True, scope="class")
    def setup_logger(request):
        klass = request.cls
        klass.log = logging.getLogger(klass.__module__ + "." + klass.__name__)

    @pytest.fixture(autouse=True)
    def setup_system(self):
        """
        Set up system tests.

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
            elif os.path.isdir(file) and file != "previous_runs":
                shutil.rmtree(file_path, ignore_errors=True)
        yield
        #  We save the logs to a separate directory so that we can see them later.
        date_str = datetime.now().strftime("%Y-%m-%d_%H_%M_%S")
        logs_folder = resolve_logs_folder()
        print()
        print(f"Saving all log files to {logs_folder}/previous_runs/{date_str}")
        print()
        target_dir = os.path.join(logs_folder, "previous_runs", date_str)
        Path(target_dir).mkdir(parents=True, exist_ok=True, mode=0o755)
        files = os.listdir(logs_folder)
        for file in files:
            if file != "previous_runs":
                file_path = os.path.join(logs_folder, file)
                shutil.move(file_path, target_dir)

    @staticmethod
    def execute_cmd(*args, **kwargs):
        executor = get_executor()
        return executor.execute_cmd(*args, **kwargs)

    @staticmethod
    def check_output(*args, **kwargs):
        executor = get_executor()
        return executor.check_output(*args, **kwargs)

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
                    with open(filepath) as f:
                        print(f.read())

    @staticmethod
    def create_dummy_file(filename, dir_path="/tmp"):
        os.makedirs(dir_path, exist_ok=True)
        full_path = os.path.join(dir_path, filename)
        with open(full_path, "wb") as f:
            f.write(os.urandom(1 * 1024 * 1024))

    @staticmethod
    def delete_dummy_file(filename, dir_path):
        full_path = os.path.join(dir_path, filename)
        with contextlib.suppress(FileNotFoundError):
            os.remove(full_path)
        if dir_path != "/tmp":
            shutil.rmtree(dir_path, ignore_errors=True)
