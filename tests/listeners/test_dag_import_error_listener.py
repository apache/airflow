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

import os
import sys
from unittest import mock

import pytest

from airflow import settings
from airflow.configuration import TEST_DAGS_FOLDER
from airflow.dag_processing.processor import DagFileProcessor
from airflow.listeners.listener import get_listener_manager
from airflow.models import DagModel
from airflow.models.errors import ParseImportError
from airflow.utils import timezone
from tests.listeners import dag_import_error_listener

from dev.tests_common.test_utils.config import conf_vars, env_vars
from dev.tests_common.test_utils.db import (
    clear_db_dags,
    clear_db_import_errors,
    clear_db_jobs,
    clear_db_pools,
    clear_db_runs,
    clear_db_serialized_dags,
)
from dev.tests_common.test_utils.mock_executor import MockExecutor

pytestmark = pytest.mark.db_test

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
PY311 = sys.version_info >= (3, 11)

# Include the words "airflow" and "dag" in the file contents,
# tricking airflow into thinking these
# files contain a DAG (otherwise Airflow will skip them)
PARSEABLE_DAG_FILE_CONTENTS = '"airflow DAG"'
UNPARSEABLE_DAG_FILE_CONTENTS = "airflow DAG"
INVALID_DAG_WITH_DEPTH_FILE_CONTENTS = "def something():\n    return airflow_DAG\nsomething()"

# Filename to be used for dags that are created in an ad-hoc manner and can be removed/
# created at runtime
TEMP_DAG_FILENAME = "temp_dag.py"


@pytest.fixture(scope="class")
def disable_load_example():
    with conf_vars({("core", "load_examples"): "false"}):
        with env_vars({"AIRFLOW__CORE__LOAD_EXAMPLES": "false"}):
            yield


@pytest.mark.usefixtures("disable_load_example")
class TestDagFileProcessor:
    @staticmethod
    def clean_db():
        clear_db_runs()
        clear_db_pools()
        clear_db_dags()
        clear_db_import_errors()
        clear_db_jobs()
        clear_db_serialized_dags()

    def setup_class(self):
        self.clean_db()

    def setup_method(self):
        # Speed up some tests by not running the tasks, just look at what we
        # enqueue!
        self.null_exec = MockExecutor()
        self.scheduler_job = None

    def teardown_method(self) -> None:
        if self.scheduler_job and self.scheduler_job.job_runner.processor_agent:
            self.scheduler_job.job_runner.processor_agent.end()
            self.scheduler_job = None
        self.clean_db()

    def _process_file(self, file_path, dag_directory, session):
        dag_file_processor = DagFileProcessor(
            dag_ids=[], dag_directory=str(dag_directory), log=mock.MagicMock()
        )

        dag_file_processor.process_file(file_path, [], False)

    @pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
    def test_newly_added_import_error(self, tmp_path, session):
        dag_import_error_listener.clear()
        get_listener_manager().add_listener(dag_import_error_listener)

        dag_file = os.path.join(TEST_DAGS_FOLDER, "test_example_bash_operator.py")
        temp_dagfile = tmp_path.joinpath(TEMP_DAG_FILENAME).as_posix()
        with open(dag_file) as main_dag, open(temp_dagfile, "w") as next_dag:
            for line in main_dag:
                next_dag.write(line)
        # first we parse the dag
        self._process_file(temp_dagfile, dag_directory=tmp_path, session=session)
        # assert DagModel.has_import_errors is false
        dm = session.query(DagModel).filter(DagModel.fileloc == temp_dagfile).first()
        assert not dm.has_import_errors
        # corrupt the file
        with open(temp_dagfile, "a") as file:
            file.writelines(UNPARSEABLE_DAG_FILE_CONTENTS)

        self._process_file(temp_dagfile, dag_directory=tmp_path, session=session)
        import_errors = session.query(ParseImportError).all()

        assert len(import_errors) == 1
        import_error = import_errors[0]
        assert import_error.filename == temp_dagfile
        assert import_error.stacktrace
        dm = session.query(DagModel).filter(DagModel.fileloc == temp_dagfile).first()
        assert dm.has_import_errors

        # Ensure the listener was notified
        assert len(dag_import_error_listener.new) == 1
        assert dag_import_error_listener.new["filename"] == import_error.stacktrace

    @pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
    def test_already_existing_import_error(self, tmp_path):
        dag_import_error_listener.clear()
        get_listener_manager().add_listener(dag_import_error_listener)

        filename_to_parse = tmp_path.joinpath(TEMP_DAG_FILENAME).as_posix()
        # Generate original import error
        with open(filename_to_parse, "w") as file_to_parse:
            file_to_parse.writelines(UNPARSEABLE_DAG_FILE_CONTENTS)
        session = settings.Session()
        self._process_file(filename_to_parse, dag_directory=tmp_path, session=session)

        import_error_1 = (
            session.query(ParseImportError).filter(ParseImportError.filename == filename_to_parse).one()
        )

        # process the file multiple times
        for _ in range(10):
            self._process_file(filename_to_parse, dag_directory=tmp_path, session=session)

        import_error_2 = (
            session.query(ParseImportError).filter(ParseImportError.filename == filename_to_parse).one()
        )

        # assert that the ID of the import error did not change
        assert import_error_1.id == import_error_2.id

        # Ensure the listener was notified
        assert len(dag_import_error_listener.existing) == 1
        assert dag_import_error_listener.existing["filename"] == import_error_1.stacktrace
        assert dag_import_error_listener.existing["filename"] == import_error_2.stacktrace
