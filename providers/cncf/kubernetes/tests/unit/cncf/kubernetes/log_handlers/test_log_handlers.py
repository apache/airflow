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
import re
from importlib import reload
from unittest import mock
from unittest.mock import patch

import pytest
from kubernetes.client import models as k8s

from airflow.executors import executor_loader
from airflow.models.dag import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.utils.log.file_task_handler import (
    FileTaskHandler,
)
from airflow.utils.log.log_reader import TaskLogReader
from airflow.utils.log.logging_mixin import set_context
from airflow.utils.state import State, TaskInstanceState
from airflow.utils.types import DagRunType

from tests_common.test_utils.compat import PythonOperator
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.dag import sync_dag_to_db
from tests_common.test_utils.db import clear_db_dag_bundles, clear_db_dags, clear_db_runs
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_1_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.utils.types import DagRunTriggeredByType
if AIRFLOW_V_3_1_PLUS:
    from airflow.sdk.timezone import datetime
else:
    from airflow.utils.timezone import datetime  # type: ignore[attr-defined,no-redef]

pytestmark = pytest.mark.db_test

DEFAULT_DATE = datetime(2016, 1, 1)
TASK_LOGGER = "airflow.task"
FILE_TASK_HANDLER = "task"


class TestFileTaskLogHandler:
    def clean_up(self):
        clear_db_dags()
        clear_db_runs()
        if AIRFLOW_V_3_0_PLUS:
            clear_db_dag_bundles()

    def setup_method(self):
        logging.root.disabled = False
        self.clean_up()
        # We use file task handler by default.

    def teardown_method(self):
        self.clean_up()

    @mock.patch(
        "airflow.providers.cncf.kubernetes.executors.kubernetes_executor.KubernetesExecutor.get_task_log"
    )
    @pytest.mark.parametrize("state", [TaskInstanceState.RUNNING, TaskInstanceState.SUCCESS])
    @pytest.mark.usefixtures("clean_executor_loader")
    def test__read_for_k8s_executor(self, mock_k8s_get_task_log, create_task_instance, state):
        """Test for k8s executor, the log is read from get_task_log method"""
        mock_k8s_get_task_log.return_value = ([], [])
        executor_name = "KubernetesExecutor"
        ti = create_task_instance(
            dag_id="dag_for_testing_k8s_executor_log_read",
            task_id="task_for_testing_k8s_executor_log_read",
            run_type=DagRunType.SCHEDULED,
            logical_date=DEFAULT_DATE,
        )
        ti.state = state
        ti.triggerer_job = None
        ti.executor = executor_name
        with conf_vars({("core", "executor"): executor_name}):
            reload(executor_loader)
            fth = FileTaskHandler("")
            fth._read(ti=ti, try_number=2)
        if state == TaskInstanceState.RUNNING:
            mock_k8s_get_task_log.assert_called_once_with(ti, 2)
        else:
            mock_k8s_get_task_log.assert_not_called()

    @pytest.mark.parametrize(
        ("pod_override", "namespace_to_call"),
        [
            pytest.param(k8s.V1Pod(metadata=k8s.V1ObjectMeta(namespace="namespace-A")), "namespace-A"),
            pytest.param(k8s.V1Pod(metadata=k8s.V1ObjectMeta(namespace="namespace-B")), "namespace-B"),
            pytest.param(k8s.V1Pod(), "default"),
            pytest.param(None, "default"),
            pytest.param(k8s.V1Pod(metadata=k8s.V1ObjectMeta(name="pod-name-xxx")), "default"),
        ],
    )
    @conf_vars({("core", "executor"): "KubernetesExecutor"})
    @patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    def test_read_from_k8s_under_multi_namespace_mode(
        self, mock_kube_client, pod_override, namespace_to_call, testing_dag_bundle
    ):
        reload(executor_loader)
        mock_read_log = mock_kube_client.return_value.read_namespaced_pod_log
        mock_list_pod = mock_kube_client.return_value.list_namespaced_pod

        def task_callable(ti):
            ti.task.log.info("test")

        with DAG("dag_for_testing_file_task_handler", schedule=None, start_date=DEFAULT_DATE) as dag:
            task = PythonOperator(
                task_id="task_for_testing_file_log_handler",
                python_callable=task_callable,
                executor_config={"pod_override": pod_override},
            )

        if AIRFLOW_V_3_0_PLUS:
            dagrun_kwargs: dict = {
                "logical_date": DEFAULT_DATE,
                "run_after": DEFAULT_DATE,
                "triggered_by": DagRunTriggeredByType.TEST,
            }
            dag = sync_dag_to_db(dag)
        else:
            dagrun_kwargs = {"execution_date": DEFAULT_DATE}
        dagrun = dag.create_dagrun(
            run_id="test",
            run_type=DagRunType.MANUAL,
            state=State.RUNNING,
            data_interval=dag.timetable.infer_manual_data_interval(run_after=DEFAULT_DATE),
            **dagrun_kwargs,
        )
        if AIRFLOW_V_3_0_PLUS:
            ti = TaskInstance(task=task, run_id=dagrun.run_id, dag_version_id=dagrun.created_dag_version_id)
        else:
            ti = TaskInstance(task=task, run_id=dagrun.run_id)
        ti.try_number = 3
        ti.executor = "KubernetesExecutor"

        logger = ti.log
        ti.task.log.disabled = False

        file_handler = TaskLogReader().log_handler
        set_context(logger, ti)
        ti.run(ignore_ti_state=True)
        ti.state = TaskInstanceState.RUNNING
        # clear executor_instances cache
        file_handler.executor_instances = {}
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
                "run_id=test,"
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
