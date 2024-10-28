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
import re
from importlib import reload
from unittest import mock
from unittest.mock import patch

import pytest
from kubernetes.client import models as k8s

from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.executors import executor_loader
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python import PythonOperator
from airflow.utils.log.file_task_handler import (
    FileTaskHandler,
)
from airflow.utils.log.logging_mixin import set_context
from airflow.utils.session import create_session
from airflow.utils.state import State, TaskInstanceState
from airflow.utils.timezone import datetime
from airflow.utils.types import DagRunType

from tests_common.test_utils.compat import AIRFLOW_V_3_0_PLUS
from tests_common.test_utils.config import conf_vars

if AIRFLOW_V_3_0_PLUS:
    from airflow.utils.types import DagRunTriggeredByType

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]

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

    @mock.patch(
        "airflow.providers.cncf.kubernetes.executors.kubernetes_executor.KubernetesExecutor.get_task_log"
    )
    @pytest.mark.parametrize(
        "state", [TaskInstanceState.RUNNING, TaskInstanceState.SUCCESS]
    )
    def test__read_for_k8s_executor(
        self, mock_k8s_get_task_log, create_task_instance, state
    ):
        """Test for k8s executor, the log is read from get_task_log method"""
        mock_k8s_get_task_log.return_value = ([], [])
        executor_name = "KubernetesExecutor"
        ti = create_task_instance(
            dag_id="dag_for_testing_k8s_executor_log_read",
            task_id="task_for_testing_k8s_executor_log_read",
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
        )
        ti.state = state
        ti.triggerer_job = None
        with conf_vars({("core", "executor"): executor_name}):
            reload(executor_loader)
            fth = FileTaskHandler("")
            fth._read(ti=ti, try_number=2)
        if state == TaskInstanceState.RUNNING:
            mock_k8s_get_task_log.assert_called_once_with(ti, 2)
        else:
            mock_k8s_get_task_log.assert_not_called()

    @pytest.mark.parametrize(
        "pod_override, namespace_to_call",
        [
            pytest.param(
                k8s.V1Pod(metadata=k8s.V1ObjectMeta(namespace="namespace-A")),
                "namespace-A",
            ),
            pytest.param(
                k8s.V1Pod(metadata=k8s.V1ObjectMeta(namespace="namespace-B")),
                "namespace-B",
            ),
            pytest.param(k8s.V1Pod(), "default"),
            pytest.param(None, "default"),
            pytest.param(
                k8s.V1Pod(metadata=k8s.V1ObjectMeta(name="pod-name-xxx")), "default"
            ),
        ],
    )
    @patch.dict("os.environ", AIRFLOW__CORE__EXECUTOR="KubernetesExecutor")
    @patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    def test_read_from_k8s_under_multi_namespace_mode(
        self, mock_kube_client, pod_override, namespace_to_call
    ):
        mock_read_log = mock_kube_client.return_value.read_namespaced_pod_log
        mock_list_pod = mock_kube_client.return_value.list_namespaced_pod

        def task_callable(ti):
            ti.log.info("test")

        with DAG(
            "dag_for_testing_file_task_handler", schedule=None, start_date=DEFAULT_DATE
        ) as dag:
            task = PythonOperator(
                task_id="task_for_testing_file_log_handler",
                python_callable=task_callable,
                executor_config={"pod_override": pod_override},
            )
        triggered_by_kwargs = (
            {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        )
        dagrun = dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            state=State.RUNNING,
            execution_date=DEFAULT_DATE,
            data_interval=dag.timetable.infer_manual_data_interval(
                run_after=DEFAULT_DATE
            ),
            **triggered_by_kwargs,
        )
        ti = TaskInstance(task=task, run_id=dagrun.run_id)
        ti.try_number = 3

        logger = ti.log
        ti.log.disabled = False

        file_handler = next(
            (h for h in logger.handlers if h.name == FILE_TASK_HANDLER), None
        )
        set_context(logger, ti)
        ti.run(ignore_ti_state=True)
        ti.state = TaskInstanceState.RUNNING
        file_handler.read(ti, 2)

        # first we find pod name
        mock_list_pod.assert_called_once()
        actual_kwargs = mock_list_pod.call_args.kwargs
        assert actual_kwargs["namespace"] == namespace_to_call
        actual_selector = actual_kwargs["label_selector"]
        assert re.match(
            (
                "dag_id=dag_for_testing_file_task_handler,"
                "kubernetes_executor=True,"
                "run_id=manual__2016-01-01T0000000000-2b88d1d57,"
                "task_id=task_for_testing_file_log_handler,"
                "try_number=2,"
                "airflow-worker"
            ),
            actual_selector,
        )

        # then we read log
        mock_read_log.assert_called_once_with(
            name=mock_list_pod.return_value.items[0].metadata.name,
            namespace=namespace_to_call,
            container="base",
            follow=False,
            tail_lines=100,
            _preload_content=False,
        )
