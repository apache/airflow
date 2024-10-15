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

import json
import logging
import os
import random
import shutil
import tempfile
from pathlib import Path
from unittest import mock

import pytest
from tests_common.test_utils.compat import AIRFLOW_V_2_10_PLUS, AIRFLOW_V_3_0_PLUS
from tests_common.test_utils.config import conf_vars

from airflow.jobs.job import Job
from airflow.jobs.local_task_job_runner import LocalTaskJobRunner
from airflow.listeners.listener import get_listener_manager
from airflow.models import DagBag, TaskInstance
from airflow.providers.google.cloud.openlineage.utils import get_from_nullable_chain
from airflow.providers.openlineage.plugins.listener import OpenLineageListener
from airflow.task.task_runner.standard_task_runner import StandardTaskRunner
from airflow.utils import timezone
from airflow.utils.state import State

if AIRFLOW_V_3_0_PLUS:
    from airflow.utils.types import DagRunTriggeredByType

# TODO(potiuk): Document that openlineage is not supported in DB isolation mode
pytestmark = pytest.mark.skip_if_database_isolation_mode

TEST_DAG_FOLDER = os.environ["AIRFLOW__CORE__DAGS_FOLDER"]
DEFAULT_DATE = timezone.datetime(2016, 1, 1)


log = logging.getLogger(__name__)


def read_file_content(file_path: str) -> str:
    with open(file_path) as file:
        return file.read()


def get_sorted_events(event_dir: str) -> list[str]:
    event_paths = [os.path.join(event_dir, event_path) for event_path in sorted(os.listdir(event_dir))]
    return [json.loads(read_file_content(event_path)) for event_path in event_paths]


def has_value_in_events(events, chain, value):
    x = [get_from_nullable_chain(event, chain) for event in events]
    log.error(x)
    y = [z == value for z in x]
    return any(y)


with tempfile.TemporaryDirectory(prefix="venv") as tmp_dir:
    listener_path = Path(tmp_dir) / "event"

    @pytest.mark.skipif(not AIRFLOW_V_2_10_PLUS, reason="Test requires Airflow 2.10+")
    @pytest.mark.usefixtures("reset_logging_config")
    class TestOpenLineageExecution:
        @pytest.fixture(autouse=True)
        def clean_listener_manager(self):
            get_listener_manager().clear()
            yield
            get_listener_manager().clear()

        def setup_job(self, task_name, run_id):
            dirpath = Path(tmp_dir)
            if dirpath.exists():
                shutil.rmtree(dirpath)
            dirpath.mkdir(exist_ok=True, parents=True)
            lm = get_listener_manager()
            lm.add_listener(OpenLineageListener())

            dagbag = DagBag(
                dag_folder=TEST_DAG_FOLDER,
                include_examples=False,
            )
            dag = dagbag.dags.get("test_openlineage_execution")
            task = dag.get_task(task_name)

            triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
            dag.create_dagrun(
                run_id=run_id,
                data_interval=(DEFAULT_DATE, DEFAULT_DATE),
                state=State.RUNNING,
                start_date=DEFAULT_DATE,
                **triggered_by_kwargs,
            )
            ti = TaskInstance(task=task, run_id=run_id)
            job = Job(id=random.randint(0, 23478197), dag_id=ti.dag_id)
            job_runner = LocalTaskJobRunner(job=job, task_instance=ti, ignore_ti_state=True)
            task_runner = StandardTaskRunner(job_runner)
            with mock.patch("airflow.task.task_runner.get_task_runner", return_value=task_runner):
                job_runner._execute()

            return task_runner.return_code(timeout=60)

        @pytest.mark.db_test
        @conf_vars({("openlineage", "transport"): f'{{"type": "file", "log_file_path": "{listener_path}"}}'})
        def test_not_stalled_task_emits_proper_lineage(self):
            task_name = "execute_no_stall"
            run_id = "test1"
            self.setup_job(task_name, run_id)

            events = get_sorted_events(tmp_dir)
            assert has_value_in_events(events, ["inputs", "name"], "on-start")
            assert has_value_in_events(events, ["inputs", "name"], "on-complete")

        @pytest.mark.db_test
        @conf_vars({("openlineage", "transport"): f'{{"type": "file", "log_file_path": "{listener_path}"}}'})
        def test_not_stalled_failing_task_emits_proper_lineage(self):
            task_name = "execute_fail"
            run_id = "test_failure"
            self.setup_job(task_name, run_id)

            events = get_sorted_events(tmp_dir)
            assert has_value_in_events(events, ["inputs", "name"], "on-start")
            assert has_value_in_events(events, ["inputs", "name"], "on-failure")

        @conf_vars(
            {
                ("openlineage", "transport"): f'{{"type": "file", "log_file_path": "{listener_path}"}}',
                ("openlineage", "execution_timeout"): "15",
            }
        )
        @pytest.mark.db_test
        def test_short_stalled_task_emits_proper_lineage(self):
            self.setup_job("execute_short_stall", "test_short_stalled_task_emits_proper_lineage")
            events = get_sorted_events(tmp_dir)
            assert has_value_in_events(events, ["inputs", "name"], "on-start")
            assert has_value_in_events(events, ["inputs", "name"], "on-complete")

        @conf_vars(
            {
                ("openlineage", "transport"): f'{{"type": "file", "log_file_path": "{listener_path}"}}',
                ("openlineage", "execution_timeout"): "3",
            }
        )
        @pytest.mark.db_test
        def test_short_stalled_task_extraction_with_low_execution_is_killed_by_ol_timeout(self):
            self.setup_job(
                "execute_short_stall",
                "test_short_stalled_task_extraction_with_low_execution_is_killed_by_ol_timeout",
            )
            events = get_sorted_events(tmp_dir)
            assert has_value_in_events(events, ["inputs", "name"], "on-start")
            assert not has_value_in_events(events, ["inputs", "name"], "on-complete")

        @conf_vars({("openlineage", "transport"): f'{{"type": "file", "log_file_path": "{listener_path}"}}'})
        @pytest.mark.db_test
        def test_mid_stalled_task_is_killed_by_ol_timeout(self):
            self.setup_job("execute_mid_stall", "test_mid_stalled_task_is_killed_by_openlineage")
            events = get_sorted_events(tmp_dir)
            assert has_value_in_events(events, ["inputs", "name"], "on-start")
            assert not has_value_in_events(events, ["inputs", "name"], "on-complete")

        @conf_vars(
            {
                ("openlineage", "transport"): f'{{"type": "file", "log_file_path": "{listener_path}"}}',
                ("openlineage", "execution_timeout"): "60",
                ("core", "task_success_overtime"): "3",
            }
        )
        @pytest.mark.db_test
        def test_long_stalled_task_is_killed_by_listener_overtime_if_ol_timeout_long_enough(self):
            dirpath = Path(tmp_dir)
            if dirpath.exists():
                shutil.rmtree(dirpath)
            dirpath.mkdir(exist_ok=True, parents=True)
            lm = get_listener_manager()
            lm.add_listener(OpenLineageListener())

            dagbag = DagBag(
                dag_folder=TEST_DAG_FOLDER,
                include_examples=False,
            )
            dag = dagbag.dags.get("test_openlineage_execution")
            task = dag.get_task("execute_long_stall")

            triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
            dag.create_dagrun(
                run_id="test_long_stalled_task_is_killed_by_listener_overtime_if_ol_timeout_long_enough",
                data_interval=(DEFAULT_DATE, DEFAULT_DATE),
                state=State.RUNNING,
                start_date=DEFAULT_DATE,
                **triggered_by_kwargs,
            )
            ti = TaskInstance(
                task=task,
                run_id="test_long_stalled_task_is_killed_by_listener_overtime_if_ol_timeout_long_enough",
            )
            job = Job(id="1", dag_id=ti.dag_id)
            job_runner = LocalTaskJobRunner(job=job, task_instance=ti, ignore_ti_state=True)
            job_runner._execute()

            events = get_sorted_events(tmp_dir)
            assert has_value_in_events(events, ["inputs", "name"], "on-start")
            assert not has_value_in_events(events, ["inputs", "name"], "on-complete")
