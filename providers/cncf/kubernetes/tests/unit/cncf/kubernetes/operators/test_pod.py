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

import datetime
import re
from contextlib import contextmanager, nullcontext
from io import BytesIO
from typing import TYPE_CHECKING
from unittest import mock
from unittest.mock import MagicMock, mock_open, patch

import pendulum
import pytest
from kubernetes.client import ApiClient, V1Pod, V1PodSecurityContext, V1PodStatus, models as k8s
from kubernetes.client.exceptions import ApiException
from urllib3 import HTTPResponse

from airflow.exceptions import (
    AirflowException,
    AirflowSkipException,
    TaskDeferred,
)
from airflow.models import DAG, DagModel, DagRun, TaskInstance
from airflow.providers.cncf.kubernetes import pod_generator
from airflow.providers.cncf.kubernetes.operators.pod import (
    KubernetesPodOperator,
    PodEventType,
    _optionally_suppress,
)
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.providers.cncf.kubernetes.triggers.pod import KubernetesPodTrigger
from airflow.providers.cncf.kubernetes.utils.pod_manager import OnFinishAction, PodLoggingStatus, PodPhase
from airflow.providers.cncf.kubernetes.utils.xcom_sidecar import PodDefaults
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.types import DagRunType

from tests_common.test_utils import db
from tests_common.test_utils.dag import sync_dag_to_db
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_1_PLUS

if AIRFLOW_V_3_0_PLUS or AIRFLOW_V_3_1_PLUS:
    from airflow.models.xcom import XComModel as XCom
else:
    from airflow.models.xcom import XCom  # type: ignore[no-redef]

if TYPE_CHECKING:
    from airflow.utils.context import Context

pytestmark = pytest.mark.db_test

DEFAULT_DATE = timezone.datetime(2016, 1, 1, 1, 0, 0)
KPO_MODULE = "airflow.providers.cncf.kubernetes.operators.pod"
POD_MANAGER_CLASS = "airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager"
POD_MANAGER_MODULE = "airflow.providers.cncf.kubernetes.utils.pod_manager"
HOOK_CLASS = "airflow.providers.cncf.kubernetes.operators.pod.KubernetesHook"
KUB_OP_PATH = "airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator.{}"

TEST_TASK_ID = "kubernetes_task_async"
TEST_NAMESPACE = "default"
TEST_IMAGE = "ubuntu:16.04"
TEST_CMDS = ["bash", "-cx"]
TEST_ARGS = ["echo", "10", "echo pwd"]
TEST_LABELS = {"foo": "bar"}
TEST_NAME = "test-pod"
TEST_SUCCESS_MESSAGE = "All containers inside pod have started successfully."


@contextmanager
def temp_override_attr(obj, attr, val):
    orig = getattr(obj, attr)
    setattr(obj, attr, val)
    yield
    setattr(obj, attr, orig)


@pytest.fixture(autouse=True)
def clear_db():
    # Clear all database objects before and after each test
    _clear_all_db_objects()
    yield
    _clear_all_db_objects()


def _clear_all_db_objects():
    """Comprehensive cleanup of all database objects that might interfere with tests."""
    db.clear_db_dags()
    db.clear_db_runs()
    if AIRFLOW_V_3_0_PLUS:
        db.clear_db_dag_bundles()


def create_context(task, persist_to_db=False, map_index=None):
    if task.has_dag():
        dag = task.dag
    else:
        dag = DAG(dag_id="dag", schedule=None, start_date=pendulum.now())
        dag.add_task(task)
    now = timezone.utcnow()
    if AIRFLOW_V_3_0_PLUS:
        sync_dag_to_db(dag)
        dag_run = DagRun(
            run_id=DagRun.generate_run_id(
                run_type=DagRunType.MANUAL, logical_date=DEFAULT_DATE, run_after=DEFAULT_DATE
            ),
            run_type=DagRunType.MANUAL,
            dag_id=dag.dag_id,
            logical_date=now,
            data_interval=(now, now),
            run_after=now,
        )
    else:
        dag_run = DagRun(
            run_id=DagRun.generate_run_id(DagRunType.MANUAL, DEFAULT_DATE),
            run_type=DagRunType.MANUAL,
            dag_id=dag.dag_id,
            execution_date=now,
        )
    if AIRFLOW_V_3_0_PLUS:
        from airflow.models.dag_version import DagVersion

        dag_version = DagVersion.get_latest_version(dag.dag_id)
        if dag_version is None:
            with create_session() as session:
                dag_version = DagVersion(dag_id=dag.dag_id, version_number=1)
                session.add(dag_version)
                session.commit()
                session.refresh(dag_version)
        task_instance = TaskInstance(task=task, run_id=dag_run.run_id, dag_version_id=dag_version.id)
    else:
        task_instance = TaskInstance(task=task, run_id=dag_run.run_id)
    task_instance.dag_run = dag_run
    if map_index is not None:
        task_instance.map_index = map_index
    if persist_to_db:
        with create_session() as session:
            if not AIRFLOW_V_3_0_PLUS:
                session.add(DagModel(dag_id=dag.dag_id))
            session.add(dag_run)
            session.add(task_instance)
            session.commit()
    return {
        "dag": dag,
        "ts": DEFAULT_DATE.isoformat(),
        "task": task,
        "ti": task_instance,
        "task_instance": task_instance,
        "run_id": "test",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("get_logs", [True, False])
async def test_async_write_logs_should_execute_successfully(mocker, get_logs):
    """
    Asserts that `execute_complete` logs the pod's logs if provided in the trigger event.
    """
    operator = KubernetesPodOperator(
        task_id=TEST_TASK_ID,
        name=TEST_NAME,
        namespace=TEST_NAMESPACE,
        image=TEST_IMAGE,
        cmds=TEST_CMDS,
        get_logs=get_logs,
        deferrable=True,
    )
    context = create_context(operator)
    mocker.patch.object(operator, "get_remote_pod", return_value=None)
    mock_log_info = mocker.patch.object(operator.log, "info")

    pod_log_content = "This is a unique log line."

    if get_logs:
        # Simulate trigger returning logs
        event = {"status": "success", "pod_log": pod_log_content}
        await operator.execute_complete(context=context, event=event)
        mock_log_info.assert_any_call("Pod log:\n%s", pod_log_content)
    else:
        # Simulate trigger not returning logs
        event = {"status": "success"}
        await operator.execute_complete(context=context, event=event)

        # Verify that the specific pod log message was not logged, while other logs are allowed.
        was_log_written = any(
            call.args and call.args[0] == "Pod log:\n%s" for call in mock_log_info.call_args_list
        )
        assert not was_log_written, "Pod log was written unexpectedly"


@pytest.mark.execution_timeout(300)
class TestKubernetesPodOperator:
    @pytest.fixture(autouse=True)
    def setup_tests(self, dag_maker):
        self.create_pod_patch = patch(f"{POD_MANAGER_CLASS}.create_pod")
        self.watch_pod_events = patch(f"{POD_MANAGER_CLASS}.watch_pod_events")
        self.await_pod_patch = patch(f"{POD_MANAGER_CLASS}.await_pod_start")
        self.await_pod_completion_patch = patch(f"{POD_MANAGER_CLASS}.await_pod_completion")
        self._default_client_patch = patch(f"{HOOK_CLASS}._get_default_client")
        self.watch_pod_events_mock = self.watch_pod_events.start()
        self.create_mock = self.create_pod_patch.start()
        self.await_start_mock = self.await_pod_patch.start()
        self.await_pod_mock = self.await_pod_completion_patch.start()
        self._default_client_mock = self._default_client_patch.start()
        self.dag_maker = dag_maker

        yield

        patch.stopall()

    def test_templates(self, create_task_instance_of_operator, session):
        dag_id = "TestKubernetesPodOperator"
        ti = create_task_instance_of_operator(
            KubernetesPodOperator,
            session=session,
            dag_id=dag_id,
            task_id="task-id",
            name="{{ dag.dag_id }}",
            hostname="{{ dag.dag_id }}",
            base_container_name="{{ dag.dag_id }}",
            namespace="{{ dag.dag_id }}",
            container_resources=k8s.V1ResourceRequirements(
                requests={"memory": "{{ dag.dag_id }}", "cpu": "{{ dag.dag_id }}"},
                limits={"memory": "{{ dag.dag_id }}", "cpu": "{{ dag.dag_id }}"},
            ),
            volume_mounts=[
                k8s.V1VolumeMount(
                    name="{{ dag.dag_id }}",
                    mount_path="mount_path",
                    sub_path="{{ dag.dag_id }}",
                )
            ],
            pod_template_file="{{ dag.dag_id }}",
            config_file="{{ dag.dag_id }}",
            labels="{{ dag.dag_id }}",
            env_vars=["{{ dag.dag_id }}"],
            arguments="{{ dag.dag_id }}",
            cmds="{{ dag.dag_id }}",
            image="image",
            in_cluster=False,
            cluster_context="{{ dag.dag_id }}",
            secrets=[Secret("{{ dag.dag_id }}", "{{ dag.dag_id }}", "{{ dag.dag_id }}")],
            reattach_on_restart=False,  # This can't be templated.
            startup_timeout_seconds=120,  # This can't be templated.
            get_logs=False,  # This can't be templated.
            do_xcom_push=False,  # This can't be templated.
        )

        ti.render_templates()

        assert ti.task.name == dag_id
        assert ti.task.hostname == dag_id
        assert ti.task.base_container_name == dag_id
        assert ti.task.namespace == dag_id
        assert ti.task.container_resources.requests["memory"] == dag_id
        assert ti.task.container_resources.requests["cpu"] == dag_id
        assert ti.task.container_resources.limits["memory"] == dag_id
        assert ti.task.container_resources.limits["cpu"] == dag_id
        assert ti.task.volume_mounts[0].name == dag_id
        assert ti.task.volume_mounts[0].sub_path == dag_id
        assert ti.task.pod_template_file == dag_id
        assert ti.task.config_file == dag_id
        assert ti.task.labels == dag_id
        assert ti.task.env_vars[0] == dag_id
        assert ti.task.secrets[0].deploy_type == dag_id
        assert ti.task.secrets[0].deploy_target == dag_id
        assert ti.task.secrets[0].secret == dag_id
        assert ti.task.arguments == dag_id
        assert ti.task.cmds == dag_id
        assert ti.task.cluster_context == dag_id