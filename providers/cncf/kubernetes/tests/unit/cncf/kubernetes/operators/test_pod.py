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
from typing import TYPE_CHECKING
from unittest import mock
from unittest.mock import MagicMock, mock_open, patch

import pendulum
import pytest
from kubernetes.client import ApiClient, V1Pod, V1PodSecurityContext, V1PodStatus, models as k8s
from kubernetes.client.exceptions import ApiException

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
from airflow.providers.common.compat.sdk import XCOM_RETURN_KEY
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
            image="{{ dag.dag_id }}",
            annotations={"dag-id": "{{ dag.dag_id }}"},
            configmaps=["{{ dag.dag_id }}"],
            volumes=[
                k8s.V1Volume(
                    name="{{ dag.dag_id }}",
                    config_map=k8s.V1ConfigMapVolumeSource(
                        name="{{ dag.dag_id }}", items=[k8s.V1KeyToPath(key="key", path="path")]
                    ),
                )
            ],
        )

        session.add(ti)
        session.commit()
        rendered = ti.render_templates()

        assert dag_id == rendered.container_resources.limits["memory"]
        assert dag_id == rendered.container_resources.limits["cpu"]
        assert dag_id == rendered.container_resources.requests["memory"]
        assert dag_id == rendered.container_resources.requests["cpu"]
        assert dag_id == rendered.volume_mounts[0].name
        assert dag_id == rendered.volume_mounts[0].sub_path
        assert dag_id == ti.task.image
        assert dag_id == ti.task.cmds
        assert dag_id == ti.task.name
        assert dag_id == ti.task.hostname
        assert dag_id == ti.task.base_container_name
        assert dag_id == ti.task.namespace
        assert dag_id == ti.task.config_file
        assert dag_id == ti.task.labels
        assert dag_id == ti.task.pod_template_file
        assert dag_id == ti.task.arguments
        assert dag_id == ti.task.env_vars[0]
        assert dag_id == rendered.annotations["dag-id"]
        assert dag_id == ti.task.env_from[0].config_map_ref.name
        assert dag_id == rendered.volumes[0].name
        assert dag_id == rendered.volumes[0].config_map.name

    def run_pod(self, operator: KubernetesPodOperator, map_index: int = -1) -> tuple[k8s.V1Pod, Context]:
        with self.dag_maker(dag_id="dag") as dag:
            operator.dag = dag

        dr = self.dag_maker.create_dagrun(run_id="test")
        (ti,) = dr.task_instances
        ti.map_index = map_index
        self.dag_run = dr
        context = ti.get_template_context(session=self.dag_maker.session)
        self.dag_maker.session.commit()  # So 'execute' can read dr and ti.

        remote_pod_mock = MagicMock()
        remote_pod_mock.status.phase = "Succeeded"
        self.await_pod_mock.return_value = remote_pod_mock
        operator.execute(context=context)
        return self.await_start_mock.call_args.kwargs["pod"], context

    def sanitize_for_serialization(self, obj):
        return ApiClient().sanitize_for_serialization(obj)

    @patch(HOOK_CLASS)
    def test_config_path(self, hook_mock):
        file_path = "/tmp/fake_file"
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            do_xcom_push=False,
            config_file=file_path,
        )
        remote_pod_mock = MagicMock()
        remote_pod_mock.status.phase = "Succeeded"
        self.await_pod_mock.return_value = remote_pod_mock
        self.run_pod(k)
        hook_mock.assert_called_once_with(
            cluster_context=None,
            conn_id="kubernetes_default",
            config_file=file_path,
            in_cluster=None,
        )

    @pytest.mark.parametrize(
        ("input", "render_template_as_native_obj", "raises_error"),
        [
            pytest.param([k8s.V1EnvVar(name="{{ bar }}", value="{{ foo }}")], False, False, id="current"),
            pytest.param({"{{ bar }}": "{{ foo }}"}, False, False, id="backcompat"),
            pytest.param("{{ env }}", True, False, id="xcom_args"),
            pytest.param("bad env", False, True, id="error"),
        ],
    )
    def test_env_vars(self, input, render_template_as_native_obj, raises_error):
        dag = DAG(
            dag_id="dag",
            schedule=None,
            start_date=pendulum.now(),
            render_template_as_native_obj=render_template_as_native_obj,
        )
        k = KubernetesPodOperator(
            env_vars=input,
            task_id="task",
            name="test",
            dag=dag,
        )
        k.render_template_fields(
            context={"foo": "footemplated", "bar": "bartemplated", "env": {"bartemplated": "footemplated"}}
        )
        if raises_error:
            with pytest.raises(AirflowException):
                k.build_pod_request_obj()
        else:
            k.build_pod_request_obj()
            assert k.env_vars[0].name == "bartemplated"
            assert k.env_vars[0].value == "footemplated"

    def test_pod_runtime_info_envs(self):
        k = KubernetesPodOperator(
            task_id="task",
            name="test",
            pod_runtime_info_envs=[k8s.V1EnvVar(name="bar", value="foo")],
        )
        k.build_pod_request_obj()

        assert k.env_vars[0].name == "bar"
        assert k.env_vars[0].value == "foo"

    def test_security_context(self):
        security_context = V1PodSecurityContext(run_as_user=1245)
        k = KubernetesPodOperator(
            security_context=security_context,
            task_id="task",
        )
        pod = k.build_pod_request_obj(create_context(k))
        assert pod.spec.security_context == security_context

    def test_host_aliases(self):
        host_aliases = [k8s.V1HostAlias(ip="192.0.2.1", hostnames=["my.service.com"])]
        k = KubernetesPodOperator(
            host_aliases=host_aliases,
            task_id="task",
        )
        pod = k.build_pod_request_obj(create_context(k))
        assert pod.spec.host_aliases == host_aliases

    def test_container_security_context(self):
        container_security_context = {"allowPrivilegeEscalation": False}
        k = KubernetesPodOperator(
            container_security_context=container_security_context,
            task_id="task",
        )
        pod = k.build_pod_request_obj(create_context(k))
        assert pod.spec.containers[0].security_context == container_security_context

    def test_envs_from_configmaps(self):
        env_from = [k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name="test-config-map"))]
        k = KubernetesPodOperator(
            task_id="task",
            env_from=env_from,
        )
        pod = k.build_pod_request_obj(create_context(k))
        assert pod.spec.containers[0].env_from == env_from

    def test_envs_from_configmaps_backcompat(self):
        # todo: formally deprecate / remove this?
        k = KubernetesPodOperator(
            task_id="task",
            configmaps=["test-config-map"],
        )
        expected = [k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name="test-config-map"))]
        pod = k.build_pod_request_obj(create_context(k))
        assert pod.spec.containers[0].env_from == expected

    def test_envs_from_secrets(self):
        secret_ref = "secret_name"
        secrets = [Secret("env", None, secret_ref)]
        k = KubernetesPodOperator(
            secrets=secrets,
            task_id="test",
        )
        pod = k.build_pod_request_obj()
        assert pod.spec.containers[0].env_from == [
            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name=secret_ref))
        ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("in_cluster", (True, False))
    @patch(HOOK_CLASS)
    def test_labels(self, hook_mock, in_cluster):
        hook_mock.return_value.is_in_cluster = in_cluster
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            labels={"foo": "bar", "none_value": None},
            name="test",
            task_id="task",
            in_cluster=in_cluster,
            do_xcom_push=False,
        )
        pod, _ = self.run_pod(k)
        assert pod.metadata.labels == {
            "foo": "bar",
            "dag_id": "dag",
            "kubernetes_pod_operator": "True",
            "task_id": "task",
            # Try number behaves differently on different versions of Airflow
            "try_number": mock.ANY,
            "airflow_version": mock.ANY,
            "run_id": "test",
            "airflow_kpo_in_cluster": str(in_cluster),
            "none_value": "",  # None converted to empty string
        }

    def test_labels_mapped(self):
        k = KubernetesPodOperator(
            name="test",
            task_id="task",
        )
        pod = k.build_pod_request_obj(create_context(k, map_index=10))
        assert pod.metadata.labels == {
            "dag_id": "dag",
            "kubernetes_pod_operator": "True",
            "task_id": "task",
            "try_number": mock.ANY,
            "airflow_version": mock.ANY,
            "run_id": "test",
            "map_index": "10",
            "airflow_kpo_in_cluster": str(k.hook.is_in_cluster),
        }

    def test_find_custom_pod_labels(self):
        k = KubernetesPodOperator(
            labels={"foo": "bar", "hello": "airflow"},
            name="test",
            task_id="task",
        )
        context = create_context(k)
        label_selector = k._build_find_pod_label_selector(context)
        assert "foo=bar" in label_selector
        assert "hello=airflow" in label_selector

    def test_build_find_pod_label_selector(self):
        """Comprehensive single test combining all label normalization scenarios.

        Includes: normal labels, None values, empty string, zero, False.
        Asserts: None -> empty assignment, other falsy preserved, core airflow labels present, no literal 'None'.
        """
        k = KubernetesPodOperator(
            labels={
                "foo": "bar",
                "hello": "airflow",
                "a": None,
                "c": None,
                "empty_str": "",
                "zero": 0,
                "false": False,
                "none": None,
            },
            name="test",
            task_id="task",
        )
        context = create_context(k)
        label_selector = k._build_find_pod_label_selector(context)

        # Standard labels
        assert "foo=bar" in label_selector
        assert "hello=airflow" in label_selector

        # None normalization (shows as key= with no value)
        for key in ["a", "c", "none"]:
            assert f"{key}=" in label_selector

        # Falsy but non-None values preserved verbatim
        assert "empty_str=" in label_selector
        assert "zero=0" in label_selector
        assert "false=False" in label_selector

        # Core Airflow identifying labels always present
        for core in ["dag_id=dag", "task_id=task", "kubernetes_pod_operator=True", "run_id=test"]:
            assert core in label_selector

        # Never include literal string 'None'
        assert "None" not in label_selector

    @pytest.mark.asyncio
    @patch(HOOK_CLASS, new=MagicMock)
    def test_find_pod_labels(self):
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
        )
        self.run_pod(k)
        _, kwargs = k.client.list_namespaced_pod.call_args
        assert kwargs["label_selector"] == (
            "dag_id=dag,foo=bar,kubernetes_pod_operator=True,run_id=test,task_id=task,"
            "already_checked!=True,!airflow-worker"
        )

    @patch(HOOK_CLASS, new=MagicMock)
    def test_pod_dns_options(self):
        dns_config = k8s.V1PodDNSConfig(
            nameservers=["192.0.2.1", "192.0.2.3"],
            searches=["ns1.svc.cluster-domain.example", "my.dns.search.suffix"],
            options=[
                k8s.V1PodDNSConfigOption(
                    name="ndots",
                    value="2",
                )
            ],
        )
        hostname = "busybox-2"
        subdomain = "busybox-subdomain"

        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            dns_config=dns_config,
            hostname=hostname,
            subdomain=subdomain,
        )

        self.run_pod(k)
        pod_spec = k.pod.spec
        assert pod_spec.dns_config == dns_config
        assert pod_spec.subdomain == subdomain
        assert pod_spec.hostname == hostname

    @pytest.mark.parametrize(
        "val",
        [
            pytest.param([k8s.V1LocalObjectReference("fakeSecret")], id="current"),
            pytest.param("fakeSecret", id="backcompat"),
        ],
    )
    def test_image_pull_secrets_correctly_set(self, val):
        k = KubernetesPodOperator(
            task_id="task",
            image_pull_secrets=val,
        )

        pod = k.build_pod_request_obj(create_context(k))
        assert pod.spec.image_pull_secrets == [k8s.V1LocalObjectReference(name="fakeSecret")]

    def test_omitted_name(self):
        k = KubernetesPodOperator(task_id="this-task-name")
        pod = k.build_pod_request_obj(create_context(k))
        assert re.match("this-task-name-[a-z0-9]+", pod.metadata.name) is not None

    @pytest.mark.parametrize("use_template", [True, False])
    @pytest.mark.parametrize("use_pod_spec", [True, False])
    @patch("pathlib.Path")
    @patch(f"{KPO_MODULE}.KubernetesPodOperator.find_pod")
    @patch.dict(
        "os.environ", AIRFLOW_CONN_MY_CONN='{"extra": {"extra__kubernetes__namespace": "extra-namespace"}}'
    )
    def test_omitted_namespace_with_conn(
        self, mock_find, mock_path, pod_template_file, use_template, pod_spec, use_pod_spec
    ):
        """
        Namespace precedence is as follows:
            - KPO
            - airflow connection
            - infer from k8s when in a cluster
            - 'default' namespace as a fallback

        Here we check when KPO omitted but we do have a conn where namespace defined.
        In this case, the namespace should be as defined in connection.
        """
        k = KubernetesPodOperator(
            task_id="task",
            kubernetes_conn_id="my_conn",
            **(dict(pod_template_file=pod_template_file) if use_template else {}),
            **(dict(full_pod_spec=pod_spec) if use_pod_spec else {}),
        )
        context = create_context(k)
        pod = k.build_pod_request_obj(context)
        mock_path.assert_not_called()
        if use_pod_spec:
            expected_namespace = "podspecnamespace"
        elif use_template:
            expected_namespace = "templatenamespace"
        else:
            expected_namespace = "extra-namespace"
        assert pod.metadata.namespace == expected_namespace
        mock_find.return_value = pod
        k.get_or_create_pod(
            pod_request_obj=pod,
            context=context,
        )
        mock_find.assert_called_once_with(expected_namespace, context=context)

    @patch("pathlib.Path")
    @patch(f"{KPO_MODULE}.KubernetesPodOperator.find_pod")
    @patch.dict("os.environ", AIRFLOW_CONN_MY_CONN='{"extra": {}}')
    def test_omitted_namespace_with_conn_no_value(self, mock_find, mock_path):
        """
        Namespace precedence is as follows:
            - KPO
            - airflow connection
            - infer from k8s when in a cluster
            - 'default' namespace as a fallback

        Here we check when KPO omitted but we do have a conn where namespace defined.
        In this case, we should continue down the change.
        Here we mock not in k8s and therefore get 'default'.
        """
        k = KubernetesPodOperator(
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            task_id="task",
            name="hello",
            kubernetes_conn_id="my_conn",
        )

        mock_path.return_value.exists.return_value = False
        context = create_context(k)
        pod = k.build_pod_request_obj(context)
        mock_path.assert_called()
        assert pod.metadata.namespace == "default"
        mock_find.return_value = pod
        k.get_or_create_pod(
            pod_request_obj=pod,
            context=context,
        )
        mock_find.assert_called_once_with("default", context=context)

    @patch("pathlib.Path")
    @patch(f"{KPO_MODULE}.KubernetesPodOperator.find_pod")
    def test_omitted_namespace_no_conn(self, mock_find, mock_path):
        """
        Namespace precedence is as follows:
            - KPO
            - airflow connection
            - infer from k8s when in a cluster
            - 'default' namespace as a fallback

        Here we check when KPO omitted and no airflow connection, but we are in k8s.
        In this case, we should use the value from k8s.
        """
        mock_path.return_value.exists.return_value = True
        mock_path.return_value.read_text.return_value = "abc"
        k = KubernetesPodOperator(
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            task_id="task",
            name="hello",
        )

        context = create_context(k)
        pod = k.build_pod_request_obj(context)
        mock_path.assert_called_once_with("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
        assert pod.metadata.namespace == "abc"
        mock_find.return_value = pod
        k.get_or_create_pod(
            pod_request_obj=pod,
            context=context,
        )
        mock_find.assert_called_once_with("abc", context=context)

    @patch("pathlib.Path")
    @patch(f"{KPO_MODULE}.KubernetesPodOperator.find_pod")
    def test_omitted_namespace_no_conn_not_in_k8s(self, mock_find, mock_path):
        """
        Namespace precedence is as follows:
            - KPO
            - airflow connection
            - infer from k8s when in a cluster
            - 'default' namespace as a fallback

        Here we check when KPO omitted and no airflow connection and not in a k8s pod.
        In this case we should end up with the 'default' namespace.
        """
        mock_path.return_value.exists.return_value = False
        k = KubernetesPodOperator(
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            task_id="task",
            name="hello",
        )

        context = create_context(k)
        pod = k.build_pod_request_obj(context)
        mock_path.assert_called_once_with("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
        assert pod.metadata.namespace == "default"
        mock_find.return_value = pod
        k.get_or_create_pod(
            pod_request_obj=pod,
            context=context,
        )
        mock_find.assert_called_once_with("default", context=context)

    @pytest.mark.parametrize(
        "pod_phase",
        [
            PodPhase.SUCCEEDED,
            PodPhase.FAILED,
            PodPhase.RUNNING,
        ],
    )
    @patch(f"{KPO_MODULE}.PodManager.create_pod")
    @patch(f"{KPO_MODULE}.KubernetesPodOperator.process_pod_deletion")
    @patch(f"{KPO_MODULE}.KubernetesPodOperator.find_pod")
    def test_get_or_create_pod_reattach_terminated(
        self, mock_find, mock_process_pod_deletion, mock_create_pod, pod_phase
    ):
        """Check that get_or_create_pod reattaches to existing pod."""
        k = KubernetesPodOperator(
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            task_id="task",
            name="hello",
            log_pod_spec_on_failure=False,
        )
        k.reattach_on_restart = True
        context = create_context(k)
        mock_pod_request_obj = MagicMock()
        mock_pod_request_obj.to_dict.return_value = {"metadata": {"name": "test-pod"}}

        mock_found_pod = MagicMock()
        mock_found_pod.status.phase = pod_phase
        mock_find.return_value = mock_found_pod
        result = k.get_or_create_pod(
            pod_request_obj=mock_pod_request_obj,
            context=context,
        )
        if pod_phase == PodPhase.RUNNING:
            mock_create_pod.assert_not_called()
            mock_process_pod_deletion.assert_not_called()
            assert result == mock_found_pod
        else:
            mock_process_pod_deletion.assert_called_once_with(mock_found_pod)
            mock_create_pod.assert_called_once_with(pod=mock_pod_request_obj)
            assert result == mock_pod_request_obj

    @pytest.mark.parametrize(
        ("pod_phase", "pod_reason", "should_reuse"),
        [
            ("Running", "", True),
            ("Running", "Evicted", False),
            ("Succeeded", None, False),
            ("Failed", None, False),
        ],
        ids=["running", "evicted", "succeeded", "failed"],
    )
    @patch(f"{KPO_MODULE}.PodManager.create_pod")
    @patch(f"{KPO_MODULE}.KubernetesPodOperator.process_pod_deletion")
    @patch(f"{KPO_MODULE}.KubernetesPodOperator.find_pod")
    def test_get_or_create_pod_reattach_with_evicted(
        self,
        mock_find,
        mock_process_pod_deletion,
        mock_create_pod,
        pod_phase,
        pod_reason,
        should_reuse,
    ):
        """Test that get_or_create_pod reattaches or recreates pod based on phase and reason."""
        k = KubernetesPodOperator(
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            task_id="task",
            name="hello",
            log_pod_spec_on_failure=False,
        )
        k.reattach_on_restart = True
        context = create_context(k)
        mock_pod_request_obj = MagicMock()
        mock_pod_request_obj.to_dict.return_value = {"metadata": {"name": "test-pod"}}

        mock_found_pod = MagicMock()
        mock_found_pod.status.phase = pod_phase
        mock_found_pod.status.reason = pod_reason
        mock_find.return_value = mock_found_pod

        result = k.get_or_create_pod(
            pod_request_obj=mock_pod_request_obj,
            context=context,
        )

        if should_reuse:
            mock_create_pod.assert_not_called()
            mock_process_pod_deletion.assert_not_called()
            assert result == mock_found_pod
        else:
            mock_process_pod_deletion.assert_called_once_with(mock_found_pod)
            mock_create_pod.assert_called_once_with(pod=mock_pod_request_obj)
            assert result == mock_pod_request_obj

    def test_xcom_sidecar_container_image_custom(self):
        image = "private.repo/alpine:3.13"
        with temp_override_attr(PodDefaults.SIDECAR_CONTAINER, "image", image):
            k = KubernetesPodOperator(
                name="test",
                task_id="task",
                do_xcom_push=True,
            )
            pod = k.build_pod_request_obj(create_context(k))
        assert pod.spec.containers[1].image == image

    def test_xcom_sidecar_container_image_default(self):
        k = KubernetesPodOperator(
            name="test",
            task_id="task",
            do_xcom_push=True,
        )
        pod = k.build_pod_request_obj(create_context(k))
        assert pod.spec.containers[1].image == "alpine"

    def test_xcom_sidecar_container_resources_default(self):
        k = KubernetesPodOperator(
            name="test",
            task_id="task",
            do_xcom_push=True,
        )
        pod = k.build_pod_request_obj(create_context(k))
        assert pod.spec.containers[1].resources == k8s.V1ResourceRequirements(
            requests={
                "cpu": "1m",
                "memory": "10Mi",
            },
        )

    def test_xcom_sidecar_container_resources_custom(self):
        resources = {
            "requests": {"cpu": "1m", "memory": "10Mi"},
            "limits": {"cpu": "10m", "memory": "50Mi"},
        }
        with temp_override_attr(PodDefaults.SIDECAR_CONTAINER, "resources", resources):
            k = KubernetesPodOperator(
                name="test",
                task_id="task",
                do_xcom_push=True,
            )
            pod = k.build_pod_request_obj(create_context(k))
            assert pod.spec.containers[1].resources == resources

    def test_image_pull_policy_correctly_set(self):
        k = KubernetesPodOperator(
            task_id="task",
            image_pull_policy="Always",
        )
        pod = k.build_pod_request_obj(create_context(k))
        assert pod.spec.containers[0].image_pull_policy == "Always"

    def test_termination_message_policy_correctly_set(self):
        k = KubernetesPodOperator(
            task_id="task",
            termination_message_policy="FallbackToLogsOnError",
        )

        pod = k.build_pod_request_obj(create_context(k))
        assert pod.spec.containers[0].termination_message_policy == "FallbackToLogsOnError"

    def test_termination_message_policy_default_value_correctly_set(self):
        k = KubernetesPodOperator(
            task_id="task",
        )

        pod = k.build_pod_request_obj(create_context(k))
        assert pod.spec.containers[0].termination_message_policy == "File"

    def test_termination_grace_period_correctly_set(self):
        k = KubernetesPodOperator(
            task_id="task",
            termination_grace_period=300,
        )

        pod = k.build_pod_request_obj(create_context(k))
        assert pod.spec.termination_grace_period_seconds == 300

    def test_termination_grace_period_default_value_correctly_set(self):
        k = KubernetesPodOperator(
            task_id="task",
        )

        pod = k.build_pod_request_obj(create_context(k))
        assert pod.spec.termination_grace_period_seconds is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("task_kwargs", "base_container_fail", "expect_to_delete_pod"),
        [
            ({"on_finish_action": "delete_pod"}, True, True),
            ({"on_finish_action": "delete_pod"}, False, True),
            ({"on_finish_action": "keep_pod"}, False, False),
            ({"on_finish_action": "keep_pod"}, True, False),
            ({"on_finish_action": "delete_succeeded_pod"}, False, True),
            ({"on_finish_action": "delete_succeeded_pod"}, True, False),
        ],
    )
    @patch(f"{POD_MANAGER_CLASS}.delete_pod")
    @patch(f"{KPO_MODULE}.KubernetesPodOperator.is_istio_enabled")
    @patch(f"{POD_MANAGER_CLASS}.await_pod_completion")
    @patch(f"{KPO_MODULE}.KubernetesPodOperator.find_pod")
    def test_pod_with_istio_delete_after_await_container_error(
        self,
        find_pod_mock,
        await_pod_completion_mock,
        is_istio_enabled_mock,
        delete_pod_mock,
        task_kwargs,
        base_container_fail,
        expect_to_delete_pod,
    ):
        """
        When KPO fails unexpectedly during await_container, we should still try to delete the pod,
        and the pod we try to delete should be the one returned from find_pod earlier.
        """
        sidecar = MagicMock()
        sidecar.name = "istio-proxy"
        sidecar.namespace = "default"
        sidecar.image = "istio/proxyv2:1.18.2"
        sidecar.args = []
        sidecar.state.running = True

        cont_status_1 = MagicMock()
        cont_status_1.name = "base"
        cont_status_1.state.running = False
        cont_status_1.state.terminated.exit_code = 0
        if base_container_fail:
            cont_status_1.state.terminated.exit_code = 1
            cont_status_1.state.terminated.message = "my-failure"

        cont_status_2 = MagicMock()
        cont_status_2.name = "istio-proxy"
        cont_status_2.state.running = True
        cont_status_2.state.terminated = False

        await_pod_completion_mock.return_value.spec.containers = [sidecar, cont_status_1, cont_status_2]
        await_pod_completion_mock.return_value.status.phase = "Running"
        await_pod_completion_mock.return_value.status.container_statuses = [cont_status_1, cont_status_2]
        await_pod_completion_mock.return_value.metadata.name = "pod-with-istio-sidecar"
        await_pod_completion_mock.return_value.metadata.namespace = "default"

        find_pod_mock.return_value.spec.containers = [sidecar, cont_status_1, cont_status_2]
        find_pod_mock.return_value.status.phase = "Running"
        find_pod_mock.return_value.status.container_statuses = [cont_status_1, cont_status_2]
        find_pod_mock.return_value.metadata.name = "pod-with-istio-sidecar"
        find_pod_mock.return_value.metadata.namespace = "default"

        k = KubernetesPodOperator(task_id="task", **task_kwargs)

        context = create_context(k)
        context["ti"].xcom_push = MagicMock()

        if base_container_fail:
            self.await_pod_mock.side_effect = AirflowException("fake failure")
            with pytest.raises(AirflowException, match="my-failure"):
                k.execute(context=context)
        else:
            k.execute(context=context)

        if expect_to_delete_pod:
            assert k.is_istio_enabled(find_pod_mock.return_value)
            delete_pod_mock.assert_called_with(await_pod_completion_mock.return_value)
        else:
            delete_pod_mock.assert_not_called()

    @pytest.mark.parametrize(
        ("task_kwargs", "should_be_deleted"),
        [
            pytest.param({}, True, id="default"),  # default values
            pytest.param({"on_finish_action": "delete_pod"}, True, id="delete-pod"),
            pytest.param({"on_finish_action": "delete_succeeded_pod"}, False, id="delete-succeeded-pod"),
            pytest.param({"on_finish_action": "keep_pod"}, False, id="keep-pod"),
        ],
    )
    @patch(f"{POD_MANAGER_CLASS}.delete_pod")
    @patch(f"{KPO_MODULE}.KubernetesPodOperator.find_pod")
    def test_pod_delete_after_await_container_error(
        self, find_pod_mock, delete_pod_mock, task_kwargs, should_be_deleted
    ):
        """
        When KPO fails unexpectedly during await_container, we should still try to delete the pod,
        and the pod we try to delete should be the one returned from find_pod earlier.
        """
        cont_status = MagicMock()
        cont_status.name = "base"
        cont_status.state.terminated.message = "my-failure"
        find_pod_mock.return_value.status.container_statuses = [cont_status]
        k = KubernetesPodOperator(task_id="task", **task_kwargs)
        self.await_pod_mock.side_effect = AirflowException("fake failure")
        context = create_context(k)
        context["ti"].xcom_push = MagicMock()
        with pytest.raises(AirflowException, match="my-failure"):
            k.execute(context=context)
        if should_be_deleted:
            delete_pod_mock.assert_called_with(find_pod_mock.return_value)
        else:
            delete_pod_mock.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("should_fail", [True, False])
    @patch(f"{POD_MANAGER_CLASS}.delete_pod")
    @patch(f"{POD_MANAGER_CLASS}.await_pod_completion")
    def test_pod_delete_not_called_when_creation_fails(self, await_pod_mock, delete_pod_mock, should_fail):
        """
        When pod creation fails, we never get a read of the remote pod.  In this case we don't attempt
        to delete the pod.
        """
        k = KubernetesPodOperator(
            task_id="task",
            on_finish_action="delete_pod",
        )

        if should_fail:
            self.create_mock.side_effect = AirflowException("fake failure")
        else:
            await_pod_mock.return_value.status.phase = "Succeeded"

        cm = pytest.raises(AirflowException) if should_fail else nullcontext()
        with cm:
            self.run_pod(k)

        if should_fail:
            delete_pod_mock.assert_not_called()
        else:
            delete_pod_mock.assert_called()

    @pytest.mark.parametrize("randomize", [True, False])
    def test_provided_pod_name(self, randomize):
        name_base = "test"
        k = KubernetesPodOperator(
            name=name_base,
            random_name_suffix=randomize,
            task_id="task",
        )
        context = create_context(k)
        pod = k.build_pod_request_obj(context)

        if randomize:
            assert pod.metadata.name.startswith(name_base)
            assert pod.metadata.name != name_base
        else:
            assert pod.metadata.name == name_base

    @pytest.fixture
    def pod_spec(self):
        return k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(name="hello", labels={"foo": "bar"}, namespace="podspecnamespace"),
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        image="ubuntu:16.04",
                        command=["something"],
                    )
                ]
            ),
        )

    @pytest.mark.parametrize("randomize_name", (True, False))
    def test_full_pod_spec(self, randomize_name, pod_spec):
        pod_spec_name_base = pod_spec.metadata.name

        k = KubernetesPodOperator(
            task_id="task",
            random_name_suffix=randomize_name,
            full_pod_spec=pod_spec,
        )
        context = create_context(k)
        pod = k.build_pod_request_obj(context)

        if randomize_name:
            assert pod.metadata.name.startswith(pod_spec_name_base)
            assert pod.metadata.name != pod_spec_name_base
        else:
            assert pod.metadata.name == pod_spec_name_base
        assert pod.metadata.namespace == pod_spec.metadata.namespace
        assert pod.spec.containers[0].image == pod_spec.spec.containers[0].image
        assert pod.spec.containers[0].command == pod_spec.spec.containers[0].command
        # Check labels are added from pod_template_file and
        # the pod identifying labels including Airflow version
        assert pod.metadata.labels == {
            "foo": "bar",
            "dag_id": "dag",
            "kubernetes_pod_operator": "True",
            "task_id": "task",
            "try_number": mock.ANY,
            "airflow_version": mock.ANY,
            "airflow_kpo_in_cluster": str(k.hook.is_in_cluster),
            "run_id": "test",
        }

    @pytest.mark.parametrize("randomize_name", (True, False))
    def test_full_pod_spec_kwargs(self, randomize_name, pod_spec):
        # kwargs take precedence, however
        image = "some.custom.image:andtag"
        name_base = "world"
        k = KubernetesPodOperator(
            task_id="task",
            random_name_suffix=randomize_name,
            full_pod_spec=pod_spec,
            name=name_base,
            image=image,
            labels={"hello": "world"},
        )
        pod = k.build_pod_request_obj(create_context(k))

        # make sure the kwargs takes precedence (and that name is randomized when expected)
        if randomize_name:
            assert pod.metadata.name.startswith(name_base)
            assert pod.metadata.name != name_base
        else:
            assert pod.metadata.name == name_base
        assert pod.spec.containers[0].image == image
        # Check labels are added from pod_template_file, the operator itself and
        # the pod identifying labels including Airflow version
        assert pod.metadata.labels == {
            "foo": "bar",
            "hello": "world",
            "dag_id": "dag",
            "kubernetes_pod_operator": "True",
            "task_id": "task",
            "try_number": mock.ANY,
            "airflow_version": mock.ANY,
            "airflow_kpo_in_cluster": str(k.hook.is_in_cluster),
            "run_id": "test",
        }

    @pytest.fixture
    def pod_template_file(self, tmp_path):
        pod_template_yaml = """
            apiVersion: v1
            kind: Pod
            metadata:
              name: hello
              namespace: templatenamespace
              labels:
                foo: bar
            spec:
              serviceAccountName: foo
              automountServiceAccountToken: false
              affinity:
                nodeAffinity:
                  requiredDuringSchedulingIgnoredDuringExecution:
                    nodeSelectorTerms:
                    - matchExpressions:
                      - key: kubernetes.io/role
                        operator: In
                        values:
                        - foo
                        - bar
                  preferredDuringSchedulingIgnoredDuringExecution:
                  - weight: 1
                    preference:
                      matchExpressions:
                      - key: kubernetes.io/role
                        operator: In
                        values:
                        - foo
                        - bar
              containers:
                - name: base
                  image: ubuntu:16.04
                  imagePullPolicy: Always
                  command:
                    - something
        """

        tpl_file = tmp_path / "template.yaml"
        tpl_file.write_text(pod_template_yaml)

        return tpl_file

    @pytest.mark.parametrize("randomize_name", (True, False))
    def test_pod_template_file(self, randomize_name, pod_template_file):
        k = KubernetesPodOperator(
            task_id="task",
            random_name_suffix=randomize_name,
            pod_template_file=pod_template_file,
        )
        pod = k.build_pod_request_obj(create_context(k))

        if randomize_name:
            assert pod.metadata.name.startswith("hello")
            assert pod.metadata.name != "hello"
        else:
            assert pod.metadata.name == "hello"
        # Check labels are added from pod_template_file and
        # the pod identifying labels including Airflow version
        assert pod.metadata.labels == {
            "foo": "bar",
            "dag_id": "dag",
            "kubernetes_pod_operator": "True",
            "task_id": "task",
            "try_number": mock.ANY,
            "airflow_version": mock.ANY,
            "airflow_kpo_in_cluster": str(k.hook.is_in_cluster),
            "run_id": "test",
        }
        assert pod.metadata.namespace == "templatenamespace"
        assert pod.spec.containers[0].image == "ubuntu:16.04"
        assert pod.spec.containers[0].image_pull_policy == "Always"
        assert pod.spec.containers[0].command == ["something"]
        assert pod.spec.service_account_name == "foo"
        assert not pod.spec.automount_service_account_token

        affinity = {
            "node_affinity": {
                "preferred_during_scheduling_ignored_during_execution": [
                    {
                        "preference": {
                            "match_expressions": [
                                {"key": "kubernetes.io/role", "operator": "In", "values": ["foo", "bar"]}
                            ],
                            "match_fields": None,
                        },
                        "weight": 1,
                    }
                ],
                "required_during_scheduling_ignored_during_execution": {
                    "node_selector_terms": [
                        {
                            "match_expressions": [
                                {"key": "kubernetes.io/role", "operator": "In", "values": ["foo", "bar"]}
                            ],
                            "match_fields": None,
                        }
                    ]
                },
            },
            "pod_affinity": None,
            "pod_anti_affinity": None,
        }

        assert pod.spec.affinity.to_dict() == affinity

    @pytest.mark.parametrize("randomize_name", (True, False))
    def test_pod_template_file_kwargs_override(self, randomize_name, pod_template_file):
        # kwargs take precedence, however
        image = "some.custom.image:andtag"
        name_base = "world"
        k = KubernetesPodOperator(
            task_id="task",
            pod_template_file=pod_template_file,
            name=name_base,
            random_name_suffix=randomize_name,
            image=image,
            labels={"hello": "world"},
        )
        pod = k.build_pod_request_obj(create_context(k))

        # make sure the kwargs takes precedence (and that name is randomized when expected)
        if randomize_name:
            assert pod.metadata.name.startswith(name_base)
            assert pod.metadata.name != name_base
        else:
            assert pod.metadata.name == name_base
        assert pod.spec.containers[0].image == image
        # Check labels are added from pod_template_file, the operator itself and
        # the pod identifying labels including Airflow version
        assert pod.metadata.labels == {
            "foo": "bar",
            "hello": "world",
            "dag_id": "dag",
            "kubernetes_pod_operator": "True",
            "task_id": "task",
            "try_number": mock.ANY,
            "airflow_version": mock.ANY,
            "airflow_kpo_in_cluster": str(k.hook.is_in_cluster),
            "run_id": "test",
        }

    @pytest.mark.parametrize("randomize_name", (True, False))
    def test_pod_template_dict(self, randomize_name):
        templated_pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(
                namespace="templatenamespace",
                name="hello",
                labels={"release": "stable"},
            ),
            spec=k8s.V1PodSpec(
                containers=[],
                init_containers=[
                    k8s.V1Container(
                        name="git-clone",
                        image="registry.k8s.io/git-sync:v3.1.1",
                        args=[
                            "--repo=git@github.com:airflow/some_repo.git",
                            "--branch={{ params.get('repo_branch', 'master') }}",
                        ],
                    ),
                ],
            ),
        )
        k = KubernetesPodOperator(
            task_id="task",
            random_name_suffix=randomize_name,
            pod_template_dict=pod_generator.PodGenerator.serialize_pod(templated_pod),
            labels={"hello": "world"},
        )

        # render templated fields before checking generated pod spec
        k.render_template_fields(context={"params": {"repo_branch": "test_branch"}})
        pod = k.build_pod_request_obj(create_context(k))

        if randomize_name:
            assert pod.metadata.name.startswith("hello")
            assert pod.metadata.name != "hello"
        else:
            assert pod.metadata.name == "hello"

        assert pod.metadata.labels == {
            "hello": "world",
            "release": "stable",
            "dag_id": "dag",
            "kubernetes_pod_operator": "True",
            "task_id": "task",
            "try_number": mock.ANY,
            "airflow_version": mock.ANY,
            "airflow_kpo_in_cluster": str(k.hook.is_in_cluster),
            "run_id": "test",
        }

        assert pod.spec.init_containers[0].name == "git-clone"
        assert pod.spec.init_containers[0].image == "registry.k8s.io/git-sync:v3.1.1"
        assert pod.spec.init_containers[0].args == [
            "--repo=git@github.com:airflow/some_repo.git",
            "--branch=test_branch",
        ]

    @patch(f"{POD_MANAGER_CLASS}.fetch_container_logs")
    @patch(f"{POD_MANAGER_CLASS}.await_container_completion", new=MagicMock)
    def test_no_handle_failure_on_success(self, fetch_container_mock):
        name_base = "test"

        k = KubernetesPodOperator(name=name_base, task_id="task")

        fetch_container_mock.return_value = None
        remote_pod_mock = MagicMock()
        remote_pod_mock.status.phase = "Succeeded"
        self.await_pod_mock.return_value = remote_pod_mock

        # assert does not raise
        self.run_pod(k)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("randomize", [True, False])
    @patch(f"{POD_MANAGER_CLASS}.await_container_completion", new=MagicMock)
    @patch(f"{POD_MANAGER_CLASS}.fetch_requested_container_logs")
    def test_name_normalized_on_execution(self, fetch_container_mock, randomize):
        name_base = "test_extra-123"
        normalized_name = "test-extra-123"

        k = KubernetesPodOperator(
            name=name_base,
            random_name_suffix=randomize,
            task_id="task",
            get_logs=False,
        )

        pod, _ = self.run_pod(k)
        if randomize:
            # To avoid
            assert isinstance(pod.metadata.name, str)
            assert pod.metadata.name.startswith(normalized_name)
            assert k.name.startswith(normalized_name)
        else:
            assert pod.metadata.name == normalized_name
            assert k.name == normalized_name

    @pytest.mark.parametrize("name", ["name@extra", "a" * 300], ids=["bad", "long"])
    def test_name_validation_on_execution(self, name):
        k = KubernetesPodOperator(
            name=name,
            task_id="task",
        )

        with pytest.raises(AirflowException):
            self.run_pod(k)

    def test_create_with_affinity(self):
        affinity = {
            "nodeAffinity": {
                "preferredDuringSchedulingIgnoredDuringExecution": [
                    {
                        "weight": 1,
                        "preference": {
                            "matchExpressions": [{"key": "disktype", "operator": "In", "values": ["ssd"]}]
                        },
                    }
                ]
            }
        }

        k = KubernetesPodOperator(
            task_id="task",
            affinity=affinity,
        )

        pod = k.build_pod_request_obj(create_context(k))
        sanitized_pod = self.sanitize_for_serialization(pod)
        assert isinstance(pod.spec.affinity, k8s.V1Affinity)
        assert sanitized_pod["spec"]["affinity"] == affinity

        k8s_api_affinity = k8s.V1Affinity(
            node_affinity=k8s.V1NodeAffinity(
                preferred_during_scheduling_ignored_during_execution=[
                    k8s.V1PreferredSchedulingTerm(
                        weight=1,
                        preference=k8s.V1NodeSelectorTerm(
                            match_expressions=[
                                k8s.V1NodeSelectorRequirement(key="disktype", operator="In", values=["ssd"])
                            ]
                        ),
                    )
                ]
            ),
        )

        _clear_all_db_objects()

        k = KubernetesPodOperator(
            task_id="task",
            affinity=k8s_api_affinity,
        )

        pod = k.build_pod_request_obj(create_context(k))
        sanitized_pod = self.sanitize_for_serialization(pod)
        assert isinstance(pod.spec.affinity, k8s.V1Affinity)
        assert sanitized_pod["spec"]["affinity"] == affinity

    def test_tolerations(self):
        k8s_api_tolerations = [k8s.V1Toleration(key="key", operator="Equal", value="value")]

        tolerations = [{"key": "key", "operator": "Equal", "value": "value"}]

        k = KubernetesPodOperator(
            task_id="task",
            tolerations=tolerations,
        )

        pod = k.build_pod_request_obj(create_context(k))
        sanitized_pod = self.sanitize_for_serialization(pod)
        assert isinstance(pod.spec.tolerations[0], k8s.V1Toleration)
        assert sanitized_pod["spec"]["tolerations"] == tolerations

        _clear_all_db_objects()

        k = KubernetesPodOperator(
            task_id="task",
            tolerations=k8s_api_tolerations,
        )

        pod = k.build_pod_request_obj(create_context(k))
        sanitized_pod = self.sanitize_for_serialization(pod)
        assert isinstance(pod.spec.tolerations[0], k8s.V1Toleration)
        assert sanitized_pod["spec"]["tolerations"] == tolerations

    def test_node_selector(self):
        node_selector = {"beta.kubernetes.io/os": "linux"}

        k = KubernetesPodOperator(
            task_id="task",
            node_selector=node_selector,
        )

        pod = k.build_pod_request_obj(create_context(k))
        sanitized_pod = self.sanitize_for_serialization(pod)
        assert isinstance(pod.spec.node_selector, dict)
        assert sanitized_pod["spec"]["nodeSelector"] == node_selector

    def test_automount_service_account_token(self):
        automount_service_account_token = False

        k = KubernetesPodOperator(
            task_id="task",
            automount_service_account_token=automount_service_account_token,
        )

        pod = k.build_pod_request_obj(create_context(k))
        sanitized_pod = self.sanitize_for_serialization(pod)
        assert isinstance(pod.spec.automount_service_account_token, bool)
        assert sanitized_pod["spec"]["automountServiceAccountToken"] == automount_service_account_token

    @pytest.mark.asyncio
    @pytest.mark.parametrize("do_xcom_push", [True, False])
    @patch(f"{POD_MANAGER_CLASS}.extract_xcom")
    @patch(f"{POD_MANAGER_CLASS}.await_xcom_sidecar_container_start")
    def test_push_xcom_pod_info(
        self, mock_await_xcom_sidecar_container_start, mock_extract_xcom, do_xcom_push
    ):
        """pod name and namespace are *always* pushed; do_xcom_push only controls xcom sidecar"""
        mock_extract_xcom.return_value = "{}"
        mock_await_xcom_sidecar_container_start.return_value = None
        k = KubernetesPodOperator(
            task_id="task",
            do_xcom_push=do_xcom_push,
        )

        pod, _ = self.run_pod(k)
        if AIRFLOW_V_3_0_PLUS:
            if AIRFLOW_V_3_1_PLUS:
                with create_session() as session:
                    pod_name = session.execute(
                        XCom.get_many(
                            run_id=self.dag_run.run_id, task_ids="task", key="pod_name"
                        ).with_only_columns(XCom.value)
                    ).first()
                    pod_namespace = session.execute(
                        XCom.get_many(
                            run_id=self.dag_run.run_id, task_ids="task", key="pod_namespace"
                        ).with_only_columns(XCom.value)
                    ).first()
            else:
                pod_name = XCom.get_many(run_id=self.dag_run.run_id, task_ids="task", key="pod_name").first()
                pod_namespace = XCom.get_many(
                    run_id=self.dag_run.run_id, task_ids="task", key="pod_namespace"
                ).first()
            pod_name = XCom.deserialize_value(pod_name)
            pod_namespace = XCom.deserialize_value(pod_namespace)
        else:
            pod_name = XCom.get_one(run_id=self.dag_run.run_id, task_id="task", key="pod_name")
            pod_namespace = XCom.get_one(run_id=self.dag_run.run_id, task_id="task", key="pod_namespace")

        assert pod_name == pod.metadata.name
        assert pod_namespace == pod.metadata.namespace

    @pytest.mark.asyncio
    @patch(HOOK_CLASS, new=MagicMock)
    def test_previous_pods_ignored_for_reattached(self):
        """
        When looking for pods to possibly reattach to,
        ignore pods from previous tries that were properly finished
        """
        k = KubernetesPodOperator(
            namespace="default",
            task_id="task",
        )
        self.run_pod(k)
        _, kwargs = k.client.list_namespaced_pod.call_args
        assert "already_checked!=True" in kwargs["label_selector"]

    @patch(KUB_OP_PATH.format("find_pod"))
    @patch(f"{POD_MANAGER_CLASS}.delete_pod")
    @patch(f"{KPO_MODULE}.KubernetesPodOperator.patch_already_checked")
    def test_mark_checked_unexpected_exception(
        self, mock_patch_already_checked, mock_delete_pod, find_pod_mock
    ):
        """If we aren't deleting pods and have an exception, mark it so we don't reattach to it"""
        k = KubernetesPodOperator(
            task_id="task",
            on_finish_action="keep_pod",
        )
        found_pods = [MagicMock(), MagicMock(), MagicMock()]
        find_pod_mock.side_effect = [None] + found_pods
        self.await_pod_mock.side_effect = AirflowException("oops")
        context = create_context(k, persist_to_db=True)
        with pytest.raises(AirflowException):
            k.execute(context=context)
        mock_patch_already_checked.assert_called_once()
        mock_delete_pod.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("do_xcom_push", [True, False])
    @patch(f"{POD_MANAGER_CLASS}.extract_xcom")
    @patch(f"{POD_MANAGER_CLASS}.await_xcom_sidecar_container_start")
    def test_wait_for_xcom_sidecar_iff_push_xcom(self, mock_await, mock_extract_xcom, do_xcom_push):
        """Assert we wait for xcom sidecar container if and only if we push xcom."""
        mock_extract_xcom.return_value = "{}"
        mock_await.return_value = None
        k = KubernetesPodOperator(
            task_id="task",
            do_xcom_push=do_xcom_push,
        )
        self.run_pod(k)
        if do_xcom_push:
            mock_await.assert_called_once()
        else:
            mock_await.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("task_kwargs", "should_fail", "should_be_deleted"),
        [
            ({}, False, True),
            ({}, True, True),
            ({"on_finish_action": "keep_pod"}, False, False),
            ({"on_finish_action": "keep_pod"}, True, False),
            ({"on_finish_action": "delete_pod"}, False, True),
            ({"on_finish_action": "delete_pod"}, True, True),
            ({"on_finish_action": "delete_succeeded_pod"}, False, True),
            ({"on_finish_action": "delete_succeeded_pod"}, True, False),
        ],
    )
    @patch(f"{POD_MANAGER_CLASS}.delete_pod")
    @patch(f"{KPO_MODULE}.KubernetesPodOperator.patch_already_checked")
    def test_mark_checked_if_not_deleted(
        self, mock_patch_already_checked, mock_delete_pod, task_kwargs, should_fail, should_be_deleted
    ):
        """If we aren't deleting pods mark "checked" if the task completes (successful or otherwise)"""
        dag = DAG("hello2", schedule=None, start_date=pendulum.now())
        k = KubernetesPodOperator(
            task_id="task",
            dag=dag,
            **task_kwargs,
        )
        remote_pod_mock = MagicMock()
        remote_pod_mock.status.phase = "Failed" if should_fail else "Succeeded"
        self.await_pod_mock.return_value = remote_pod_mock
        context = create_context(k, persist_to_db=True)
        if should_fail:
            with pytest.raises(AirflowException):
                k.execute(context=context)
        else:
            k.execute(context=context)
        if should_fail or not should_be_deleted:
            mock_patch_already_checked.assert_called_once()
        else:
            mock_patch_already_checked.assert_not_called()
        if should_be_deleted:
            mock_delete_pod.assert_called_once()
        else:
            mock_delete_pod.assert_not_called()

    @patch(HOOK_CLASS, new=MagicMock)
    def test_patch_already_checked(self):
        """Make sure we patch the pods with the right label"""
        k = KubernetesPodOperator(task_id="task")
        pod = k.build_pod_request_obj()
        k.patch_already_checked(pod)
        k.client.patch_namespaced_pod.assert_called_once_with(
            name=pod.metadata.name,
            namespace=pod.metadata.namespace,
            body={"metadata": {"labels": {"already_checked": "True"}}},
        )

    def test_task_id_as_name(self):
        k = KubernetesPodOperator(
            task_id=".hi.-_09HI",
            random_name_suffix=False,
        )
        pod = k.build_pod_request_obj({})
        assert pod.metadata.name == "hi-09hi"

    def test_task_id_as_name_with_suffix(self):
        k = KubernetesPodOperator(
            task_id=".hi.-_09HI",
            random_name_suffix=True,
        )
        pod = k.build_pod_request_obj({})
        expected = "hi-09hi"
        assert pod.metadata.name[: len(expected)] == expected
        assert re.match(rf"{expected}-[a-z0-9]{{8}}", pod.metadata.name) is not None

    def test_task_id_as_name_with_suffix_very_long(self):
        k = KubernetesPodOperator(
            task_id="a" * 250,
            random_name_suffix=True,
        )
        pod = k.build_pod_request_obj({})
        assert (
            re.match(
                r"a{54}-[a-z0-9]{8}",
                pod.metadata.name,
            )
            is not None
        )

    def test_task_id_as_name_dag_id_is_ignored(self):
        dag = DAG(dag_id="this_is_a_dag_name", schedule=None, start_date=pendulum.now())
        k = KubernetesPodOperator(
            task_id="a_very_reasonable_task_name",
            dag=dag,
        )
        pod = k.build_pod_request_obj({})
        assert re.match(r"a-very-reasonable-task-name-[a-z0-9-]+", pod.metadata.name) is not None

    @pytest.mark.parametrize(
        ("labels", "expected"),
        [
            pytest.param({}, {}, id="empty"),
            pytest.param(
                {"a": None, "b": "value", "c": None}, {"a": "", "b": "value", "c": ""}, id="with-none"
            ),
            pytest.param(
                {"empty_str": "", "zero": 0, "false": False, "none": None},
                {"empty_str": "", "zero": 0, "false": False, "none": ""},
                id="preserve-other-values",
            ),
        ],
    )
    def test_normalize_labels_dict(self, labels, expected):
        """normalize_labels_dict should transform only None values to empty strings and preserve others"""
        from airflow.providers.cncf.kubernetes.operators.pod import _normalize_labels_dict

        normalized = _normalize_labels_dict(labels)
        assert normalized == expected

    @patch(f"{POD_MANAGER_CLASS}.extract_xcom")
    @patch(f"{POD_MANAGER_CLASS}.await_xcom_sidecar_container_start")
    @patch(f"{POD_MANAGER_CLASS}.await_pod_completion")
    def test_xcom_push_failed_pod(self, remote_pod, mock_await, mock_extract_xcom):
        """Tests that an xcom is pushed after a pod failed but xcom output exists."""
        k = KubernetesPodOperator(task_id="task", on_finish_action="delete_pod", do_xcom_push=True)

        remote_pod.return_value.status.phase = "Failed"
        mock_extract_xcom.return_value = '{"Test key": "Test value"}'
        context = create_context(k)
        context["ti"].xcom_push = MagicMock()

        with pytest.raises(AirflowException):
            k.execute(context=context)

        context["ti"].xcom_push.assert_called_with(XCOM_RETURN_KEY, {"Test key": "Test value"})

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("kwargs", "actual_exit_code", "expected_exc"),
        [
            ({}, 0, None),
            ({}, 100, AirflowException),
            ({}, 101, AirflowException),
            ({"skip_on_exit_code": None}, 0, None),
            ({"skip_on_exit_code": None}, 100, AirflowException),
            ({"skip_on_exit_code": None}, 101, AirflowException),
            ({"skip_on_exit_code": 100}, 0, None),
            ({"skip_on_exit_code": 100}, 100, AirflowSkipException),
            ({"skip_on_exit_code": 100}, 101, AirflowException),
            ({"skip_on_exit_code": 0}, 0, AirflowSkipException),
            ({"skip_on_exit_code": [100]}, 0, None),
            ({"skip_on_exit_code": [100]}, 100, AirflowSkipException),
            ({"skip_on_exit_code": [100]}, 101, AirflowException),
            ({"skip_on_exit_code": [100, 102]}, 101, AirflowException),
            ({"skip_on_exit_code": (100,)}, 0, None),
            ({"skip_on_exit_code": (100,)}, 100, AirflowSkipException),
            ({"skip_on_exit_code": (100,)}, 101, AirflowException),
        ],
    )
    @patch(f"{POD_MANAGER_CLASS}.await_pod_completion")
    def test_task_skip_when_pod_exit_with_certain_code(
        self, remote_pod, kwargs, actual_exit_code, expected_exc
    ):
        """Tests that an AirflowSkipException is raised when the container exits with the skip_on_exit_code"""
        k = KubernetesPodOperator(task_id="task", on_finish_action="delete_pod", **kwargs)

        base_container = MagicMock()
        base_container.name = k.base_container_name
        base_container.state.terminated.exit_code = actual_exit_code
        sidecar_container = MagicMock()
        sidecar_container.name = "airflow-xcom-sidecar"
        sidecar_container.state.terminated.exit_code = 0
        remote_pod.return_value.status.container_statuses = [base_container, sidecar_container]
        remote_pod.return_value.status.phase = "Succeeded" if actual_exit_code == 0 else "Failed"

        if expected_exc is None:
            self.run_pod(k)
        else:
            with pytest.raises(expected_exc):
                self.run_pod(k)

    @pytest.mark.asyncio
    @patch(f"{POD_MANAGER_CLASS}.extract_xcom")
    @patch(f"{POD_MANAGER_CLASS}.await_xcom_sidecar_container_start")
    @patch(f"{POD_MANAGER_CLASS}.await_container_completion")
    @patch(f"{POD_MANAGER_CLASS}.fetch_requested_container_logs")
    @patch(HOOK_CLASS)
    def test_get_logs_but_not_for_base_container(
        self,
        hook_mock,
        mock_fetch_log,
        mock_await_container_completion,
        mock_await_xcom_sidecar,
        mock_extract_xcom,
    ):
        hook_mock.return_value.get_xcom_sidecar_container_image.return_value = None
        hook_mock.return_value.get_xcom_sidecar_container_resources.return_value = None
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            do_xcom_push=True,
            container_logs=["some_init_container"],
            get_logs=True,
        )
        mock_extract_xcom.return_value = "{}"
        remote_pod_mock = MagicMock()
        remote_pod_mock.status.phase = "Succeeded"
        self.await_pod_mock.return_value = remote_pod_mock
        pod, _ = self.run_pod(k)

        # check that the base container is not included in the logs
        mock_fetch_log.assert_called_once_with(
            pod=pod,
            containers=["some_init_container"],
            follow_logs=True,
            container_name_log_prefix_enabled=True,
            log_formatter=None,
        )
        # check that KPO waits for the base container to complete before proceeding to extract XCom
        mock_await_container_completion.assert_called_once_with(
            pod=pod, container_name="base", polling_time=1
        )
        # check that we wait for the xcom sidecar to start before extracting XCom
        mock_await_xcom_sidecar.assert_called_once_with(pod=pod)

    @patch(HOOK_CLASS, new=MagicMock)
    @patch(KUB_OP_PATH.format("find_pod"))
    def test_execute_sync_callbacks(self, find_pod_mock):
        from airflow.providers.cncf.kubernetes.callbacks import ExecutionMode

        from unit.cncf.kubernetes.test_callbacks import (
            MockKubernetesPodOperatorCallback,
            MockWrapper,
        )

        MockWrapper.reset()
        mock_callbacks = MockWrapper.mock_callbacks
        found_pods = [MagicMock(), MagicMock(), MagicMock()]
        find_pod_mock.side_effect = [None] + found_pods

        remote_pod_mock = MagicMock()
        remote_pod_mock.status.phase = "Succeeded"
        self.await_pod_mock.return_value = remote_pod_mock
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            do_xcom_push=False,
            callbacks=MockKubernetesPodOperatorCallback,
        )
        _, context = self.run_pod(k)

        # check on_sync_client_creation callback
        mock_callbacks.on_sync_client_creation.assert_called_once()
        assert mock_callbacks.on_sync_client_creation.call_args.kwargs == {"client": k.client, "operator": k}

        # check on_pod_manifest_created callback
        mock_callbacks.on_pod_manifest_created.assert_called_once()

        # check on_pod_creation callback
        mock_callbacks.on_pod_creation.assert_called_once()
        assert mock_callbacks.on_pod_creation.call_args.kwargs == {
            "client": k.client,
            "mode": ExecutionMode.SYNC,
            "pod": found_pods[0],
            "operator": k,
            "context": context,
        }

        # check on_pod_starting callback
        mock_callbacks.on_pod_starting.assert_called_once()
        assert mock_callbacks.on_pod_starting.call_args.kwargs == {
            "client": k.client,
            "mode": ExecutionMode.SYNC,
            "pod": found_pods[1],
            "operator": k,
            "context": context,
        }

        # check on_pod_completion callback
        mock_callbacks.on_pod_completion.assert_called_once()
        assert mock_callbacks.on_pod_completion.call_args.kwargs == {
            "client": k.client,
            "mode": ExecutionMode.SYNC,
            "pod": found_pods[2],
            "operator": k,
            "context": context,
        }

        mock_callbacks.on_pod_teardown.assert_called_once()
        assert mock_callbacks.on_pod_teardown.call_args.kwargs == {
            "client": k.client,
            "mode": ExecutionMode.SYNC,
            "pod": found_pods[2],
            "operator": k,
            "context": context,
        }

        # check on_pod_cleanup callback
        mock_callbacks.on_pod_cleanup.assert_called_once()
        assert mock_callbacks.on_pod_cleanup.call_args.kwargs == {
            "client": k.client,
            "mode": ExecutionMode.SYNC,
            "pod": k.pod,
            "operator": k,
            "context": context,
        }

    @pytest.mark.asyncio
    @patch(HOOK_CLASS, new=MagicMock)
    @patch(KUB_OP_PATH.format("find_pod"))
    def test_execute_sync_multiple_callbacks(self, find_pod_mock):
        from airflow.providers.cncf.kubernetes.callbacks import ExecutionMode

        from unit.cncf.kubernetes.test_callbacks import (
            MockKubernetesPodOperatorCallback,
            MockWrapper,
        )

        MockWrapper.reset()
        mock_callbacks = MockWrapper.mock_callbacks
        found_pods = [MagicMock(), MagicMock(), MagicMock()]
        find_pod_mock.side_effect = [None] + found_pods

        remote_pod_mock = MagicMock()
        remote_pod_mock.status.phase = "Succeeded"
        self.await_pod_mock.return_value = remote_pod_mock
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            do_xcom_push=False,
            callbacks=[MockKubernetesPodOperatorCallback, MockKubernetesPodOperatorCallback],
        )
        _, context = self.run_pod(k)

        # check on_sync_client_creation callback
        assert mock_callbacks.on_sync_client_creation.call_count == 2
        assert mock_callbacks.on_sync_client_creation.call_args.kwargs == {"client": k.client, "operator": k}

        # check on_pod_manifest_created callback
        assert mock_callbacks.on_pod_manifest_created.call_count == 2

        # check on_pod_creation callback
        assert mock_callbacks.on_pod_creation.call_count == 2
        assert mock_callbacks.on_pod_creation.call_args.kwargs == {
            "client": k.client,
            "mode": ExecutionMode.SYNC,
            "pod": found_pods[0],
            "operator": k,
            "context": context,
        }

        # check on_pod_starting callback
        assert mock_callbacks.on_pod_starting.call_count == 2
        assert mock_callbacks.on_pod_starting.call_args.kwargs == {
            "client": k.client,
            "mode": ExecutionMode.SYNC,
            "pod": found_pods[1],
            "operator": k,
            "context": context,
        }

        # check on_pod_completion callback
        assert mock_callbacks.on_pod_completion.call_count == 2
        assert mock_callbacks.on_pod_completion.call_args.kwargs == {
            "client": k.client,
            "mode": ExecutionMode.SYNC,
            "pod": found_pods[2],
            "operator": k,
            "context": context,
        }

        assert mock_callbacks.on_pod_teardown.call_count == 2
        assert mock_callbacks.on_pod_teardown.call_args.kwargs == {
            "client": k.client,
            "mode": ExecutionMode.SYNC,
            "pod": found_pods[2],
            "operator": k,
            "context": context,
        }

        # check on_pod_cleanup callback
        assert mock_callbacks.on_pod_cleanup.call_count == 2
        assert mock_callbacks.on_pod_cleanup.call_args.kwargs == {
            "client": k.client,
            "mode": ExecutionMode.SYNC,
            "pod": k.pod,
            "operator": k,
            "context": context,
        }

    @patch(HOOK_CLASS, new=MagicMock)
    def test_execute_async_callbacks(self):
        from airflow.providers.cncf.kubernetes.callbacks import ExecutionMode

        from unit.cncf.kubernetes.test_callbacks import (
            MockKubernetesPodOperatorCallback,
            MockWrapper,
        )

        MockWrapper.reset()
        mock_callbacks = MockWrapper.mock_callbacks
        remote_pod_mock = MagicMock()
        remote_pod_mock.status.phase = "Succeeded"
        self.await_pod_mock.return_value = remote_pod_mock

        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            do_xcom_push=False,
            callbacks=MockKubernetesPodOperatorCallback,
        )
        context = create_context(k)

        k.trigger_reentry(
            context=context,
            event={
                "status": "success",
                "message": TEST_SUCCESS_MESSAGE,
                "name": TEST_NAME,
                "namespace": TEST_NAMESPACE,
            },
        )

        # check on_operator_resuming callback
        mock_callbacks.on_pod_cleanup.assert_called_once()
        assert mock_callbacks.on_pod_cleanup.call_args.kwargs == {
            "client": k.client,
            "mode": ExecutionMode.SYNC,
            "pod": remote_pod_mock,
            "operator": k,
            "context": context,
        }

        # check on_pod_cleanup callback
        mock_callbacks.on_pod_cleanup.assert_called_once()
        assert mock_callbacks.on_pod_cleanup.call_args.kwargs == {
            "client": k.client,
            "mode": ExecutionMode.SYNC,
            "pod": remote_pod_mock,
            "operator": k,
            "context": context,
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize("get_logs", [True, False])
    @patch(f"{POD_MANAGER_CLASS}.fetch_requested_container_logs")
    @patch(f"{POD_MANAGER_CLASS}.await_container_completion")
    @patch(f"{POD_MANAGER_CLASS}.read_pod")
    def test_await_container_completion_refreshes_properties_on_exception(
        self, mock_read_pod, mock_await_container_completion, fetch_requested_container_logs, get_logs
    ):
        k = KubernetesPodOperator(task_id="task", get_logs=get_logs)
        pod, _ = self.run_pod(k)
        client, hook, pod_manager = k.client, k.hook, k.pod_manager

        # no exception doesn't update properties
        k.await_pod_completion(pod)
        assert client == k.client
        assert hook == k.hook
        assert pod_manager == k.pod_manager

        # exception refreshes properties
        mock_await_container_completion.side_effect = [ApiException(status=401), mock.DEFAULT]
        fetch_requested_container_logs.side_effect = [ApiException(status=401), mock.DEFAULT]
        k.await_pod_completion(pod)

        if get_logs:
            fetch_requested_container_logs.assert_has_calls(
                [
                    mock.call(
                        pod=pod,
                        containers=k.container_logs,
                        follow_logs=True,
                        container_name_log_prefix_enabled=True,
                        log_formatter=None,
                    )
                ]
                * 3
            )
        else:
            mock_await_container_completion.assert_has_calls(
                [mock.call(pod=pod, container_name=k.base_container_name, polling_time=1)] * 3
            )
        mock_read_pod.assert_called()
        assert client != k.client
        assert hook != k.hook
        assert pod_manager != k.pod_manager

    @pytest.mark.asyncio
    @patch(f"{POD_MANAGER_CLASS}.await_container_completion")
    @patch(f"{POD_MANAGER_CLASS}.read_pod")
    def test_await_container_completion_raises_unauthorized_if_credentials_still_invalid_after_refresh(
        self, mock_read_pod, mock_await_container_completion
    ):
        k = KubernetesPodOperator(task_id="task", get_logs=False)
        pod, _ = self.run_pod(k)
        client, hook, pod_manager = k.client, k.hook, k.pod_manager

        mock_await_container_completion.side_effect = [ApiException(status=401)]
        mock_read_pod.side_effect = [ApiException(status=401)]

        with pytest.raises(ApiException):
            k.await_pod_completion(pod)

        mock_read_pod.assert_called()
        # assert cache was refreshed
        assert client != k.client
        assert hook != k.hook
        assert pod_manager != k.pod_manager

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("side_effect", "exception_type", "expect_exc"),
        [
            ([ApiException(401), mock.DEFAULT], ApiException, True),  # works after one 401
            ([ApiException(401)] * 3 + [mock.DEFAULT], ApiException, True),  # works after 3 retries
            ([ApiException(402)], ApiException, False),  # exc on non-401
            ([ApiException(500)], ApiException, False),  # exc on non-401
            ([Exception], Exception, False),  # exc on different exception
        ],
    )
    @patch(f"{POD_MANAGER_CLASS}.await_container_completion")
    def test_await_container_completion_retries_on_specific_exception(
        self, mock_await_container_completion, side_effect, exception_type, expect_exc
    ):
        k = KubernetesPodOperator(
            task_id="task",
            get_logs=False,
        )
        pod, _ = self.run_pod(k)
        mock_await_container_completion.side_effect = side_effect
        if expect_exc:
            k.await_pod_completion(pod)
        else:
            with pytest.raises(exception_type):
                k.await_pod_completion(pod)
        expected_call_count = len(side_effect)
        mock_await_container_completion.assert_has_calls(
            [mock.call(pod=pod, container_name=k.base_container_name, polling_time=1)] * expected_call_count
        )

    @pytest.mark.parametrize(
        "on_finish_action", [OnFinishAction.KEEP_POD, OnFinishAction.DELETE_SUCCEEDED_POD]
    )
    @patch(KUB_OP_PATH.format("patch_already_checked"))
    @patch(KUB_OP_PATH.format("process_pod_deletion"))
    def test_process_duplicate_label_pods__label_patched_if_action_is_not_delete_pod(
        self,
        process_pod_deletion_mock,
        patch_already_checked_mock,
        on_finish_action,
    ):
        now = datetime.datetime.now()
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:22.04",
            cmds=["bash", "-cx"],
            arguments=["echo 12"],
            name="test",
            task_id="task",
            do_xcom_push=False,
            reattach_on_restart=False,
            on_finish_action=on_finish_action,
        )
        context = create_context(k)
        pod_1 = k.get_or_create_pod(pod_request_obj=k.build_pod_request_obj(context), context=context)
        pod_2 = k.get_or_create_pod(pod_request_obj=k.build_pod_request_obj(context), context=context)

        pod_1.status = {"start_time": now}
        pod_2.status = {"start_time": now + datetime.timedelta(seconds=60)}
        pod_2.metadata.labels.update({"try_number": "2"})

        result = k.process_duplicate_label_pods([pod_1, pod_2])

        patch_already_checked_mock.assert_called_once_with(pod_1, reraise=False)
        process_pod_deletion_mock.assert_not_called()
        assert result.metadata.name == pod_2.metadata.name

    @patch(KUB_OP_PATH.format("patch_already_checked"))
    @patch(KUB_OP_PATH.format("process_pod_deletion"))
    def test_process_duplicate_label_pods__pod_removed_if_delete_pod(
        self, process_pod_deletion_mock, patch_already_checked_mock
    ):
        now = datetime.datetime.now()
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:22.04",
            cmds=["bash", "-cx"],
            arguments=["echo 12"],
            name="test",
            task_id="task",
            do_xcom_push=False,
            reattach_on_restart=False,
            on_finish_action=OnFinishAction.DELETE_POD,
        )
        context = create_context(k)
        pod_1 = k.get_or_create_pod(pod_request_obj=k.build_pod_request_obj(context), context=context)
        pod_2 = k.get_or_create_pod(pod_request_obj=k.build_pod_request_obj(context), context=context)

        pod_1.status = {"start_time": now}
        pod_2.status = {"start_time": now + datetime.timedelta(seconds=60)}
        pod_2.metadata.labels.update({"try_number": "2"})

        result = k.process_duplicate_label_pods([pod_1, pod_2])

        patch_already_checked_mock.assert_called_once_with(pod_1, reraise=False)
        process_pod_deletion_mock.assert_called_once_with(pod_1)
        assert result.metadata.name == pod_2.metadata.name


class TestSuppress:
    def test__suppress(self, caplog):
        with _optionally_suppress(ValueError):
            raise ValueError("failure")
        assert "ValueError: failure" in caplog.text

    def test__suppress_no_args(self, caplog):
        """By default, suppresses Exception, so should suppress and log RuntimeError"""
        with _optionally_suppress():
            raise RuntimeError("failure")
        assert "RuntimeError: failure" in caplog.text

    def test__suppress_no_args_reraise(self, caplog):
        """
        By default, suppresses Exception, but with reraise=True,
        should raise RuntimeError and not log.
        """
        with pytest.raises(RuntimeError):
            with _optionally_suppress(reraise=True):
                raise RuntimeError("failure")
        assert caplog.text == ""

    def test__suppress_wrong_error(self, caplog):
        """
        Here, we specify only catch ValueError. But we raise RuntimeError.
        So it should raise and not log.
        """
        with pytest.raises(RuntimeError):
            with _optionally_suppress(ValueError):
                raise RuntimeError("failure")
        assert caplog.text == ""

    def test__suppress_wrong_error_multiple(self, caplog):
        """
        Here, we specify only catch RuntimeError/IndexError.
        But we raise RuntimeError. So it should raise and not log.
        """
        with pytest.raises(RuntimeError):
            with _optionally_suppress(ValueError, IndexError):
                raise RuntimeError("failure")
        assert caplog.text == ""

    def test__suppress_right_error_multiple(self, caplog):
        """
        Here, we specify catch RuntimeError/IndexError.
        And we raise RuntimeError. So it should suppress and log.
        """
        with _optionally_suppress(ValueError, IndexError):
            raise IndexError("failure")
        assert "IndexError: failure" in caplog.text

    def test__suppress_no_error(self, caplog):
        """When no error in context, should do nothing."""
        with _optionally_suppress():
            print("hi")
        assert caplog.text == ""


@pytest.mark.execution_timeout(300)
class TestKubernetesPodOperatorAsync:
    @pytest.fixture(autouse=True)
    def setup(self, dag_maker):
        self.create_pod_patch = patch(f"{POD_MANAGER_CLASS}.create_pod")
        self.await_pod_patch = patch(f"{POD_MANAGER_CLASS}.await_pod_start")
        self.await_pod_completion_patch = patch(f"{POD_MANAGER_CLASS}.await_pod_completion")
        self._default_client_patch = patch(f"{HOOK_CLASS}._get_default_client")
        self.await_pod_get_patch = patch(f"{HOOK_CLASS}.get_pod")
        self.create_mock = self.create_pod_patch.start()
        self.await_start_mock = self.await_pod_patch.start()
        self.await_pod_mock = self.await_pod_completion_patch.start()
        self._default_client_mock = self._default_client_patch.start()
        self.await_pod_get = self.await_pod_get_patch.start()
        self.dag_maker = dag_maker

        yield

        patch.stopall()

    def run_pod_async(self, operator: KubernetesPodOperator, map_index: int = -1):
        with self.dag_maker(dag_id="dag") as dag:
            operator.dag = dag

        dr = self.dag_maker.create_dagrun(run_id="test")
        (ti,) = dr.task_instances
        ti.map_index = map_index
        self.dag_run = dr
        context = ti.get_template_context(session=self.dag_maker.session)
        self.dag_maker.session.commit()  # So 'execute' can read dr and ti.

        remote_pod_mock = MagicMock()
        remote_pod_mock.status.phase = "Succeeded"
        remote_pod_mock.metadata.name = TEST_NAME
        remote_pod_mock.metadata.namespace = TEST_NAMESPACE
        self.await_pod_mock.return_value = remote_pod_mock

        operator.trigger_reentry(
            context=context,
            event={
                "status": "success",
                "message": TEST_SUCCESS_MESSAGE,
                "name": TEST_NAME,
                "namespace": TEST_NAMESPACE,
            },
        )
        return remote_pod_mock

    @pytest.mark.parametrize("do_xcom_push", [True, False])
    @patch(KUB_OP_PATH.format("client"))
    @patch(KUB_OP_PATH.format("find_pod"))
    @patch(KUB_OP_PATH.format("build_pod_request_obj"))
    @patch(KUB_OP_PATH.format("get_or_create_pod"))
    def test_async_create_pod_should_execute_successfully(
        self, mocked_pod, mocked_pod_obj, mocked_found_pod, mocked_client, do_xcom_push, mocker
    ):
        """
        Asserts that a task is deferred and the KubernetesCreatePodTrigger will be fired
        when the KubernetesPodOperator is executed in deferrable mode when deferrable=True.

        pod name and namespace are *always* pushed; do_xcom_push only controls xcom sidecar
        """

        k = KubernetesPodOperator(
            task_id=TEST_TASK_ID,
            namespace=TEST_NAMESPACE,
            image=TEST_IMAGE,
            cmds=TEST_CMDS,
            arguments=TEST_ARGS,
            labels=TEST_LABELS,
            name=TEST_NAME,
            on_finish_action="keep_pod",
            in_cluster=True,
            get_logs=True,
            deferrable=True,
            do_xcom_push=do_xcom_push,
        )

        mock_file = mock_open(read_data='{"a": "b"}')
        mocker.patch("builtins.open", mock_file)

        mocked_pod.return_value.metadata.name = TEST_NAME
        mocked_pod.return_value.metadata.namespace = TEST_NAMESPACE

        context = create_context(k)
        ti_mock = MagicMock(**{"map_index": -1})
        context["ti"] = ti_mock

        with pytest.raises(TaskDeferred) as exc:
            k.execute(context)

        assert ti_mock.xcom_push.call_count == 2
        ti_mock.xcom_push.assert_any_call(key="pod_name", value=TEST_NAME)
        ti_mock.xcom_push.assert_any_call(key="pod_namespace", value=TEST_NAMESPACE)
        assert isinstance(exc.value.trigger, KubernetesPodTrigger)

    @pytest.mark.parametrize("status", ["error", "failed", "timeout"])
    @patch(KUB_OP_PATH.format("log"))
    @patch(KUB_OP_PATH.format("cleanup"))
    @patch(HOOK_CLASS)
    def test_async_create_pod_should_throw_exception(self, mocked_hook, mocked_cleanup, mocked_log, status):
        """Tests that an AirflowException is raised in case of error event and event is logged"""

        mocked_hook.return_value.get_pod.return_value = MagicMock()
        k = KubernetesPodOperator(
            task_id=TEST_TASK_ID,
            namespace=TEST_NAMESPACE,
            image=TEST_IMAGE,
            cmds=TEST_CMDS,
            arguments=TEST_ARGS,
            labels=TEST_LABELS,
            name=TEST_NAME,
            on_finish_action="keep_pod",
            in_cluster=True,
            get_logs=True,
            deferrable=True,
        )

        message = "Some message"
        with pytest.raises(AirflowException):
            k.trigger_reentry(
                context=None,
                event={
                    "status": status,
                    "message": message,
                    "name": TEST_NAME,
                    "namespace": TEST_NAMESPACE,
                },
            )

        log_message = "Trigger emitted an %s event, failing the task: %s"
        mocked_log.error.assert_called_once_with(log_message, status, message)

    @pytest.mark.parametrize(
        ("kwargs", "actual_exit_code", "expected_exc", "pod_status", "event_status"),
        [
            ({}, 0, None, "Succeeded", "success"),
            ({}, 100, AirflowException, "Failed", "error"),
            ({}, 101, AirflowException, "Failed", "error"),
            ({"skip_on_exit_code": None}, 0, None, "Succeeded", "success"),
            ({"skip_on_exit_code": None}, 100, AirflowException, "Failed", "error"),
            ({"skip_on_exit_code": None}, 101, AirflowException, "Failed", "error"),
            ({"skip_on_exit_code": 100}, 100, AirflowSkipException, "Failed", "error"),
            ({"skip_on_exit_code": 100}, 101, AirflowException, "Failed", "error"),
            ({"skip_on_exit_code": 0}, 0, AirflowSkipException, "Failed", "error"),
            ({"skip_on_exit_code": [100]}, 100, AirflowSkipException, "Failed", "error"),
            ({"skip_on_exit_code": [100]}, 101, AirflowException, "Failed", "error"),
            ({"skip_on_exit_code": [100, 102]}, 101, AirflowException, "Failed", "error"),
            ({"skip_on_exit_code": (100,)}, 100, AirflowSkipException, "Failed", "error"),
            ({"skip_on_exit_code": (100,)}, 101, AirflowException, "Failed", "error"),
        ],
    )
    @patch(KUB_OP_PATH.format("pod_manager"))
    @patch(HOOK_CLASS)
    def test_async_create_pod_with_skip_on_exit_code_should_skip(
        self,
        mocked_hook,
        mock_manager,
        kwargs,
        actual_exit_code,
        expected_exc,
        pod_status,
        event_status,
    ):
        """Tests that an AirflowSkipException is raised when the container exits with the skip_on_exit_code"""

        k = KubernetesPodOperator(
            task_id=TEST_TASK_ID,
            namespace=TEST_NAMESPACE,
            image=TEST_IMAGE,
            cmds=TEST_CMDS,
            arguments=TEST_ARGS,
            labels=TEST_LABELS,
            name=TEST_NAME,
            on_finish_action="keep_pod",
            in_cluster=True,
            get_logs=True,
            deferrable=True,
            **kwargs,
        )

        base_container = MagicMock()
        base_container.name = k.base_container_name
        base_container.state.terminated.exit_code = actual_exit_code
        sidecar_container = MagicMock()
        sidecar_container.name = "airflow-xcom-sidecar"
        sidecar_container.state.terminated.exit_code = 0
        remote_pod = MagicMock()
        remote_pod.status.phase = pod_status
        remote_pod.status.container_statuses = [base_container, sidecar_container]
        mocked_hook.return_value.get_pod.return_value = remote_pod
        mock_manager.await_pod_completion.return_value = remote_pod

        context = {
            "ti": MagicMock(),
        }
        event = {
            "status": event_status,
            "message": "Some msg",
            "name": TEST_NAME,
            "namespace": TEST_NAMESPACE,
        }

        if expected_exc:
            with pytest.raises(expected_exc):
                k.trigger_reentry(context=context, event=event)
        else:
            k.trigger_reentry(context=context, event=event)

    @pytest.mark.parametrize("do_xcom_push", [True, False])
    @patch(KUB_OP_PATH.format("post_complete_action"))
    @patch(KUB_OP_PATH.format("extract_xcom"))
    @patch(POD_MANAGER_CLASS)
    @patch(HOOK_CLASS)
    def test_async_create_pod_xcom_push_should_execute_successfully(
        self, mocked_hook, mock_manager, mocked_extract, post_complete_action, do_xcom_push
    ):
        mocked_hook.return_value.get_pod.return_value = MagicMock()
        mock_manager.return_value.await_pod_completion.return_value = MagicMock()
        k = KubernetesPodOperator(
            task_id="task",
            do_xcom_push=do_xcom_push,
            get_logs=False,
            deferrable=True,
        )

        context = create_context(k)
        context["ti"] = MagicMock()
        k.trigger_reentry(
            context=context,
            event={
                "status": "success",
                "message": TEST_SUCCESS_MESSAGE,
                "name": TEST_NAME,
                "namespace": TEST_NAMESPACE,
            },
        )

        if do_xcom_push:
            mocked_extract.assert_called_once()
            post_complete_action.assert_called_once()
        else:
            mocked_extract.assert_not_called()

    def test_async_xcom_sidecar_container_image_default_should_execute_successfully(self):
        k = KubernetesPodOperator(
            name=TEST_NAME,
            task_id="task",
            do_xcom_push=True,
            deferrable=True,
        )
        pod = k.build_pod_request_obj(create_context(k))
        assert pod.spec.containers[1].image == "alpine"

    def test_async_xcom_sidecar_container_resources_default_should_execute_successfully(self):
        k = KubernetesPodOperator(
            name=TEST_NAME,
            task_id="task",
            do_xcom_push=True,
            deferrable=True,
        )
        pod = k.build_pod_request_obj(create_context(k))
        assert pod.spec.containers[1].resources == k8s.V1ResourceRequirements(
            requests={
                "cpu": "1m",
                "memory": "10Mi",
            },
        )

    @pytest.mark.parametrize("get_logs", [True, False])
    @patch(KUB_OP_PATH.format("post_complete_action"))
    @patch(KUB_OP_PATH.format("_write_logs"))
    @patch(POD_MANAGER_CLASS)
    @patch(HOOK_CLASS)
    def test_async_get_logs_should_execute_successfully(
        self, mocked_hook, mock_manager, mocked_write_logs, post_complete_action, get_logs
    ):
        mocked_hook.return_value.get_pod.return_value = MagicMock()
        mock_manager.return_value.await_pod_completion.return_value = MagicMock()
        k = KubernetesPodOperator(
            task_id="task",
            get_logs=get_logs,
            deferrable=True,
        )

        context = create_context(k)
        context["ti"] = MagicMock()
        k.trigger_reentry(
            context=context,
            event={
                "status": "success",
                "message": TEST_SUCCESS_MESSAGE,
                "name": TEST_NAME,
                "namespace": TEST_NAMESPACE,
            },
        )

        if get_logs:
            mocked_write_logs.assert_called_once()
            post_complete_action.assert_called_once()
        else:
            mocked_write_logs.assert_not_called()

    @pytest.mark.parametrize("get_logs", [True, False])
    @patch(KUB_OP_PATH.format("post_complete_action"))
    @patch(KUB_OP_PATH.format("client"))
    @patch(KUB_OP_PATH.format("extract_xcom"))
    @patch(HOOK_CLASS)
    @patch(KUB_OP_PATH.format("pod_manager"))
    def test_async_write_logs_should_execute_successfully(
        self,
        mock_manager,
        mocked_hook,
        mock_extract_xcom,
        mocked_client,
        post_complete_action,
        get_logs,
        caplog,
    ):
        test_logs = "ok"
        # Mock client.read_namespaced_pod_log to return an iterable of bytes
        mocked_client.read_namespaced_pod_log.return_value = [test_logs.encode("utf-8")]
        mock_manager.await_pod_completion.return_value = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(name=TEST_NAME, namespace=TEST_NAMESPACE)
        )
        mocked_hook.return_value.get_pod.return_value = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(name=TEST_NAME, namespace=TEST_NAMESPACE)
        )
        mock_extract_xcom.return_value = "{}"
        k = KubernetesPodOperator(
            task_id="task",
            get_logs=get_logs,
            deferrable=True,
        )
        self.run_pod_async(k)

        if get_logs:
            # Verify that client.read_namespaced_pod_log was called
            mocked_client.read_namespaced_pod_log.assert_called_once()
            # Verify the log output using caplog
            assert f"[base] logs: {test_logs}" in caplog.text
            post_complete_action.assert_called_once()
        else:
            # When get_logs=False, _write_logs should not be called, so client.read_namespaced_pod_log should not be called
            mocked_client.read_namespaced_pod_log.assert_not_called()

    @patch(KUB_OP_PATH.format("post_complete_action"))
    @patch(KUB_OP_PATH.format("client"))
    @patch(KUB_OP_PATH.format("extract_xcom"))
    @patch(HOOK_CLASS)
    @patch(KUB_OP_PATH.format("pod_manager"))
    def test_async_write_logs_handler_api_exception(
        self, mock_manager, mocked_hook, mock_extract_xcom, post_complete_action, mocked_client
    ):
        mocked_client.read_namespaced_pod_log.side_effect = ApiException(status=404)
        mock_manager.await_pod_completion.side_effect = ApiException(status=404)
        mocked_hook.return_value.get_pod.return_value = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(name=TEST_NAME, namespace=TEST_NAMESPACE)
        )
        mock_extract_xcom.return_value = "{}"
        k = KubernetesPodOperator(
            task_id="task",
            get_logs=True,
            deferrable=True,
        )
        self.run_pod_async(k)
        post_complete_action.assert_not_called()

    @pytest.mark.parametrize(
        ("log_pod_spec_on_failure", "expect_match"),
        [
            (True, r"Pod task-.* returned a failure.\nremote_pod:.*"),
            (False, r"Pod task-.* returned a failure.(?!\nremote_pod:)"),
        ],
    )
    def test_cleanup_log_pod_spec_on_failure(self, log_pod_spec_on_failure, expect_match):
        k = KubernetesPodOperator(task_id="task", log_pod_spec_on_failure=log_pod_spec_on_failure)
        pod = k.build_pod_request_obj(create_context(k))
        pod.status = V1PodStatus(phase=PodPhase.FAILED)
        with pytest.raises(AirflowException, match=expect_match):
            k.cleanup(pod, pod)

    @patch(KUB_OP_PATH.format("_write_logs"))
    @patch("airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator.cleanup")
    @patch("airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator.find_pod")
    @patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.fetch_container_logs")
    def test_get_logs_not_running(self, fetch_container_logs, find_pod, cleanup, mock_write_log):
        pod = MagicMock()
        find_pod.return_value = pod
        op = KubernetesPodOperator(task_id="test_task", name="test-pod", get_logs=True)
        fetch_container_logs.return_value = PodLoggingStatus(False, None)
        op.trigger_reentry(
            create_context(op), event={"name": TEST_NAME, "namespace": TEST_NAMESPACE, "status": "success"}
        )
        fetch_container_logs.is_called_with(pod, "base")

    @patch(KUB_OP_PATH.format("_write_logs"))
    @patch("airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator.cleanup")
    @patch("airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator.find_pod")
    def test_trigger_error(self, find_pod, cleanup, mock_write_log):
        """Assert that trigger_reentry raise exception in case of error"""
        find_pod.return_value = MagicMock()
        op = KubernetesPodOperator(task_id="test_task", name="test-pod", get_logs=True)
        context = create_context(op)
        with pytest.raises(AirflowException):
            op.trigger_reentry(
                context,
                {
                    "status": "timeout",
                    "message": "any message",
                    "name": TEST_NAME,
                    "namespace": TEST_NAMESPACE,
                },
            )

    @patch(HOOK_CLASS)
    def test_execute_async_callbacks(self, mocked_hook):
        from airflow.providers.cncf.kubernetes.callbacks import ExecutionMode

        from unit.cncf.kubernetes.test_callbacks import (
            MockKubernetesPodOperatorCallback,
            MockWrapper,
        )

        MockWrapper.reset()
        mock_callbacks = MockWrapper.mock_callbacks
        remote_pod_mock = MagicMock()
        remote_pod_mock.status.phase = "Succeeded"
        self.await_pod_mock.return_value = remote_pod_mock
        mocked_hook.return_value.get_pod.return_value = remote_pod_mock

        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            do_xcom_push=False,
            callbacks=MockKubernetesPodOperatorCallback,
        )
        context = create_context(k)

        callback_event = {
            "status": "success",
            "message": TEST_SUCCESS_MESSAGE,
            "name": TEST_NAME,
            "namespace": TEST_NAMESPACE,
        }
        k.trigger_reentry(context=context, event=callback_event)

        # check on_pod_cleanup callback
        mock_callbacks.on_pod_cleanup.assert_called_once()
        assert mock_callbacks.on_pod_cleanup.call_args.kwargs == {
            "client": k.client,
            "mode": ExecutionMode.SYNC,
            "pod": remote_pod_mock,
            "operator": k,
            "context": context,
        }

        # check on_pod_completion callback
        mock_callbacks.on_pod_completion.assert_called_once()
        assert mock_callbacks.on_pod_completion.call_args.kwargs == {
            "client": k.client,
            "mode": ExecutionMode.SYNC,
            "pod": remote_pod_mock,
            "operator": k,
            "context": context,
        }

        # check on_pod_teardown callback
        mock_callbacks.on_pod_teardown.assert_called_once()
        assert mock_callbacks.on_pod_teardown.call_args.kwargs == {
            "client": k.client,
            "mode": ExecutionMode.SYNC,
            "pod": remote_pod_mock,
            "operator": k,
            "context": context,
        }


@pytest.mark.parametrize("do_xcom_push", [True, False])
@patch(KUB_OP_PATH.format("extract_xcom"))
@patch(KUB_OP_PATH.format("post_complete_action"))
@patch(HOOK_CLASS)
def test_async_kpo_wait_termination_before_cleanup_on_success(
    mocked_hook, post_complete_action, mock_extract_xcom, do_xcom_push
):
    metadata = {"metadata.name": TEST_NAME, "metadata.namespace": TEST_NAMESPACE}
    running_state = mock.MagicMock(**metadata, **{"status.phase": "Running"})
    succeeded_state = mock.MagicMock(**metadata, **{"status.phase": "Succeeded"})
    mocked_hook.return_value.get_pod.return_value = running_state
    read_pod_mock = mocked_hook.return_value.core_v1_client.read_namespaced_pod
    read_pod_mock.side_effect = [
        running_state,
        running_state,
        succeeded_state,
    ]

    success_event = {
        "status": "success",
        "message": TEST_SUCCESS_MESSAGE,
        "name": TEST_NAME,
        "namespace": TEST_NAMESPACE,
    }

    k = KubernetesPodOperator(task_id="task", deferrable=True, do_xcom_push=do_xcom_push)
    context = create_context(k)
    context["ti"].xcom_push = MagicMock()

    result = k.trigger_reentry(context, success_event)

    # check if it gets the pod
    mocked_hook.return_value.get_pod.assert_called_once_with(TEST_NAME, TEST_NAMESPACE)

    # assert that the xcom are extracted/not extracted
    if do_xcom_push:
        mock_extract_xcom.assert_called_once()
        context["ti"].xcom_push.assert_called_with(XCOM_RETURN_KEY, mock_extract_xcom.return_value)
    else:
        mock_extract_xcom.assert_not_called()
        assert result is None

    # check if it waits for the pod to complete
    assert read_pod_mock.call_count == 3

    # assert that the cleanup is called
    post_complete_action.assert_called_once()


@pytest.mark.parametrize("do_xcom_push", [True, False])
@patch(KUB_OP_PATH.format("extract_xcom"))
@patch(KUB_OP_PATH.format("post_complete_action"))
@patch(HOOK_CLASS)
def test_async_kpo_wait_termination_before_cleanup_on_failure(
    mocked_hook, post_complete_action, mock_extract_xcom, do_xcom_push
):
    metadata = {"metadata.name": TEST_NAME, "metadata.namespace": TEST_NAMESPACE}
    running_state = mock.MagicMock(**metadata, **{"status.phase": "Running"})
    failed_state = mock.MagicMock(**metadata, **{"status.phase": "Failed"})
    mocked_hook.return_value.get_pod.return_value = running_state
    read_pod_mock = mocked_hook.return_value.core_v1_client.read_namespaced_pod
    read_pod_mock.side_effect = [
        running_state,
        running_state,
        failed_state,
    ]

    ti_mock = MagicMock()

    success_event = {"status": "failed", "message": "error", "name": TEST_NAME, "namespace": TEST_NAMESPACE}

    post_complete_action.side_effect = AirflowException()

    k = KubernetesPodOperator(task_id="task", deferrable=True, do_xcom_push=do_xcom_push)

    with pytest.raises(AirflowException):
        k.trigger_reentry({"ti": ti_mock}, success_event)

    # check if it gets the pod
    mocked_hook.return_value.get_pod.assert_called_once_with(TEST_NAME, TEST_NAMESPACE)

    # assert that it does not push the xcom
    ti_mock.xcom_push.assert_not_called()

    if do_xcom_push:
        # assert that the xcom are not extracted if do_xcom_push is False
        mock_extract_xcom.assert_called_once()
    else:
        # but that it is extracted when do_xcom_push is true because the sidecare
        # needs to be terminated
        mock_extract_xcom.assert_not_called()

    # check if it waits for the pod to complete
    assert read_pod_mock.call_count == 3

    # assert that the cleanup is called
    post_complete_action.assert_called_once()


def test_default_container_logs():
    class TestSubclassKPO(KubernetesPodOperator):
        BASE_CONTAINER_NAME = "test-base-container"

    k = TestSubclassKPO(task_id="task")
    assert k.container_logs == "test-base-container"


@patch(KUB_OP_PATH.format("post_complete_action"))
@patch(HOOK_CLASS)
@patch(KUB_OP_PATH.format("pod_manager"))
def test_async_skip_kpo_wait_termination_with_timeout_event(mock_manager, mocked_hook, post_complete_action):
    metadata = {"metadata.name": TEST_NAME, "metadata.namespace": TEST_NAMESPACE}
    pending_state = mock.MagicMock(**metadata, **{"status.phase": "Pending"})
    mocked_hook.return_value.get_pod.return_value = pending_state
    ti_mock = MagicMock()

    event = {"status": "timeout", "message": "timeout", "name": TEST_NAME, "namespace": TEST_NAMESPACE}

    k = KubernetesPodOperator(task_id="task", deferrable=True)

    # assert that the AirflowException is raised when the timeout event is present
    with pytest.raises(AirflowException):
        k.trigger_reentry({"ti": ti_mock}, event)

    # assert that the await_pod_completion is not called
    mock_manager.await_pod_completion.assert_not_called()

    # assert that the cleanup is called
    post_complete_action.assert_called_once()


@patch("airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator.pod_manager")
@patch("airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator.log")
def test_read_pod_events(mock_log, mock_pod_manager):
    # Create a mock pod
    pod = V1Pod()

    # Create mock events
    mock_event_normal = MagicMock()
    mock_event_normal.type = PodEventType.NORMAL.value
    mock_event_normal.reason = "test-reason-normal"
    mock_event_normal.message = "test-message-normal"

    mock_event_error = MagicMock()
    mock_event_error.type = PodEventType.WARNING.value
    mock_event_error.reason = "test-reason-error"
    mock_event_error.message = "test-message-error"

    mock_pod_manager.read_pod_events.return_value.items = [mock_event_normal, mock_event_error]

    operator = KubernetesPodOperator(task_id="test-task")
    operator._read_pod_events(pod)

    # Assert that event with type `Normal` is logged as info.
    mock_log.info.assert_called_once_with(
        "Pod Event: %s - %s",
        mock_event_normal.reason,
        mock_event_normal.message,
    )
    # Assert that event with type `Warning` is logged as warning.
    mock_log.warning.assert_called_once_with(
        "Pod Event: %s - %s",
        mock_event_error.reason,
        mock_event_error.message,
    )
