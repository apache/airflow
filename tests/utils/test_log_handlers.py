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

import logging
import logging.config
import os
import re

from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.models import DAG, DagRun, TaskInstance
from airflow.operators.python import PythonOperator
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import set_context
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.timezone import datetime
from airflow.utils.types import DagRunType

DEFAULT_DATE = datetime(2016, 1, 1)
TASK_LOGGER = "airflow.task"
FILE_TASK_HANDLER = "task"


class TestFileTaskLogHandler:
    def clean_up(self):
        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TaskInstance).delete()

    def setup_method(self):
        logging.config.dictConfig(DEFAULT_LOGGING_CONFIG)
        logging.root.disabled = False
        self.clean_up()
        # We use file task handler by default.

    def teardown_method(self):
        self.clean_up()

    def test_default_task_logging_setup(self):
        # file task handler is used by default.
        logger = logging.getLogger(TASK_LOGGER)
        handlers = logger.handlers
        assert len(handlers) == 1
        handler = handlers[0]
        assert handler.name == FILE_TASK_HANDLER

    def test_file_task_handler_when_ti_value_is_invalid(self):
        def task_callable(ti):
            ti.log.info("test")

        dag = DAG("dag_for_testing_file_task_handler", start_date=DEFAULT_DATE)
        dagrun = dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            state=State.RUNNING,
            execution_date=DEFAULT_DATE,
        )
        task = PythonOperator(
            task_id="task_for_testing_file_log_handler",
            dag=dag,
            python_callable=task_callable,
        )
        ti = TaskInstance(task=task, run_id=dagrun.run_id)

        logger = ti.log
        ti.log.disabled = False

        file_handler = next(
            (handler for handler in logger.handlers if handler.name == FILE_TASK_HANDLER), None
        )
        assert file_handler is not None

        set_context(logger, ti)
        assert file_handler.handler is not None
        # We expect set_context generates a file locally.
        log_filename = file_handler.handler.baseFilename
        assert os.path.isfile(log_filename)
        assert log_filename.endswith("1.log"), log_filename

        ti.run(ignore_ti_state=True)

        file_handler.flush()
        file_handler.close()

        assert hasattr(file_handler, "read")
        # Return value of read must be a tuple of list and list.
        # passing invalid `try_number` to read function
        logs, metadatas = file_handler.read(ti, 0)
        assert isinstance(logs, list)
        assert isinstance(metadatas, list)
        assert len(logs) == 1
        assert len(logs) == len(metadatas)
        assert isinstance(metadatas[0], dict)
        assert logs[0][0][0] == "default_host"
        assert logs[0][0][1] == "Error fetching the logs. Try number 0 is invalid."

        # Remove the generated tmp log file.
        os.remove(log_filename)

    def test_file_task_handler(self):
        def task_callable(ti):
            ti.log.info("test")

        dag = DAG("dag_for_testing_file_task_handler", start_date=DEFAULT_DATE)
        dagrun = dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            state=State.RUNNING,
            execution_date=DEFAULT_DATE,
        )
        task = PythonOperator(
            task_id="task_for_testing_file_log_handler",
            dag=dag,
            python_callable=task_callable,
        )
        ti = TaskInstance(task=task, run_id=dagrun.run_id)

        logger = ti.log
        ti.log.disabled = False

        file_handler = next(
            (handler for handler in logger.handlers if handler.name == FILE_TASK_HANDLER), None
        )
        assert file_handler is not None

        set_context(logger, ti)
        assert file_handler.handler is not None
        # We expect set_context generates a file locally.
        log_filename = file_handler.handler.baseFilename
        assert os.path.isfile(log_filename)
        assert log_filename.endswith("1.log"), log_filename

        ti.run(ignore_ti_state=True)

        file_handler.flush()
        file_handler.close()

        assert hasattr(file_handler, "read")
        # Return value of read must be a tuple of list and list.
        logs, metadatas = file_handler.read(ti)
        assert isinstance(logs, list)
        assert isinstance(metadatas, list)
        assert len(logs) == 1
        assert len(logs) == len(metadatas)
        assert isinstance(metadatas[0], dict)
        target_re = r"\n\[[^\]]+\] {test_log_handlers.py:\d+} INFO - test\n"

        # We should expect our log line from the callable above to appear in
        # the logs we read back
        assert re.search(target_re, logs[0][0][-1]), "Logs were " + str(logs)

        # Remove the generated tmp log file.
        os.remove(log_filename)

    def test_file_task_handler_running(self):
        def task_callable(ti):
            ti.log.info("test")

        dag = DAG("dag_for_testing_file_task_handler", start_date=DEFAULT_DATE)
        task = PythonOperator(
            task_id="task_for_testing_file_log_handler",
            python_callable=task_callable,
            dag=dag,
        )
        dagrun = dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            state=State.RUNNING,
            execution_date=DEFAULT_DATE,
        )
        ti = TaskInstance(task=task, run_id=dagrun.run_id)

        ti.try_number = 2
        ti.state = State.RUNNING

        logger = ti.log
        ti.log.disabled = False

        file_handler = next(
            (handler for handler in logger.handlers if handler.name == FILE_TASK_HANDLER), None
        )
        assert file_handler is not None

        set_context(logger, ti)
        assert file_handler.handler is not None
        # We expect set_context generates a file locally.
        log_filename = file_handler.handler.baseFilename
        assert os.path.isfile(log_filename)
        assert log_filename.endswith("2.log"), log_filename

        logger.info("Test")

        # Return value of read must be a tuple of list and list.
        logs, metadatas = file_handler.read(ti)
        assert isinstance(logs, list)
        # Logs for running tasks should show up too.
        assert isinstance(logs, list)
        assert isinstance(metadatas, list)
        assert len(logs) == 2
        assert len(logs) == len(metadatas)
        assert isinstance(metadatas[0], dict)

        # Remove the generated tmp log file.
        os.remove(log_filename)


class TestFilenameRendering:
    def test_python_formatting(self, create_log_template, create_task_instance):
        create_log_template("{dag_id}/{task_id}/{execution_date}/{try_number}.log")
        filename_rendering_ti = create_task_instance(
            dag_id="dag_for_testing_filename_rendering",
            task_id="task_for_testing_filename_rendering",
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
        )

        expected_filename = (
            f"dag_for_testing_filename_rendering/task_for_testing_filename_rendering/"
            f"{DEFAULT_DATE.isoformat()}/42.log"
        )
        fth = FileTaskHandler("")
        rendered_filename = fth._render_filename(filename_rendering_ti, 42)
        assert expected_filename == rendered_filename

    def test_jinja_rendering(self, create_log_template, create_task_instance):
        create_log_template("{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log")
        filename_rendering_ti = create_task_instance(
            dag_id="dag_for_testing_filename_rendering",
            task_id="task_for_testing_filename_rendering",
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
        )

        expected_filename = (
            f"dag_for_testing_filename_rendering/task_for_testing_filename_rendering/"
            f"{DEFAULT_DATE.isoformat()}/42.log"
        )
        fth = FileTaskHandler("")
        rendered_filename = fth._render_filename(filename_rendering_ti, 42)
        assert expected_filename == rendered_filename


class TestLogUrl:
    def test_log_retrieval_valid(self, create_task_instance):
        log_url_ti = create_task_instance(
            dag_id="dag_for_testing_filename_rendering",
            task_id="task_for_testing_filename_rendering",
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
        )
        log_url_ti.hostname = "hostname"
        url = FileTaskHandler._get_log_retrieval_url(log_url_ti, "DYNAMIC_PATH")
        assert url == "http://hostname:8793/log/DYNAMIC_PATH"
