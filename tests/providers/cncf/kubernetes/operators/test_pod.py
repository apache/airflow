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

import re
from contextlib import contextmanager, nullcontext
from io import BytesIO
from unittest import mock
from unittest.mock import MagicMock, patch

import pendulum
import pytest
from kubernetes.client import ApiClient, V1PodSecurityContext, V1PodStatus, models as k8s
from urllib3 import HTTPResponse

from airflow.exceptions import AirflowException, AirflowSkipException, TaskDeferred
from airflow.models import DAG, DagModel, DagRun, TaskInstance
from airflow.models.xcom import XCom
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator, _optionally_suppress
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.providers.cncf.kubernetes.triggers.pod import KubernetesPodTrigger
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodPhase
from airflow.providers.cncf.kubernetes.utils.xcom_sidecar import PodDefaults
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.types import DagRunType
from tests.test_utils import db

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


@pytest.fixture(scope="function", autouse=True)
def clear_db():
    db.clear_db_dags()
    db.clear_db_runs()
    yield


def create_context(task, persist_to_db=False, map_index=None):
    if task.has_dag():
        dag = task.dag
    else:
        dag = DAG(dag_id="dag", start_date=pendulum.now())
        dag.add_task(task)
    dag_run = DagRun(
        run_id=DagRun.generate_run_id(DagRunType.MANUAL, DEFAULT_DATE),
        run_type=DagRunType.MANUAL,
        dag_id=dag.dag_id,
    )
    task_instance = TaskInstance(task=task, run_id=dag_run.run_id)
    task_instance.dag_run = dag_run
    if map_index is not None:
        task_instance.map_index = map_index
    if persist_to_db:
        with create_session() as session:
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
        self.await_pod_patch = patch(f"{POD_MANAGER_CLASS}.await_pod_start")
        self.await_pod_completion_patch = patch(f"{POD_MANAGER_CLASS}.await_pod_completion")
        self._default_client_patch = patch(f"{HOOK_CLASS}._get_default_client")
        self.create_mock = self.create_pod_patch.start()
        self.await_start_mock = self.await_pod_patch.start()
        self.await_pod_mock = self.await_pod_completion_patch.start()
        self._default_client_mock = self._default_client_patch.start()
        self.dag_maker = dag_maker

        yield

        patch.stopall()

    def test_templates(self, create_task_instance_of_operator):
        dag_id = "TestKubernetesPodOperator"
        ti = create_task_instance_of_operator(
            KubernetesPodOperator,
            dag_id=dag_id,
            task_id="task-id",
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
        )

        rendered = ti.render_templates()

        assert dag_id == rendered.container_resources.limits["memory"]
        assert dag_id == rendered.container_resources.limits["cpu"]
        assert dag_id == rendered.container_resources.requests["memory"]
        assert dag_id == rendered.container_resources.requests["cpu"]
        assert dag_id == rendered.volume_mounts[0].name
        assert dag_id == rendered.volume_mounts[0].sub_path
        assert dag_id == ti.task.image
        assert dag_id == ti.task.cmds
        assert dag_id == ti.task.namespace
        assert dag_id == ti.task.config_file
        assert dag_id == ti.task.labels
        assert dag_id == ti.task.pod_template_file
        assert dag_id == ti.task.arguments
        assert dag_id == ti.task.env_vars[0]

    def run_pod(self, operator: KubernetesPodOperator, map_index: int = -1) -> k8s.V1Pod:
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
        return self.await_start_mock.call_args.kwargs["pod"]

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
        "input",
        [
            pytest.param([k8s.V1EnvVar(name="{{ bar }}", value="{{ foo }}")], id="current"),
            pytest.param({"{{ bar }}": "{{ foo }}"}, id="backcompat"),
        ],
    )
    def test_env_vars(self, input):
        k = KubernetesPodOperator(
            env_vars=input,
            task_id="task",
        )
        k.render_template_fields(context={"foo": "footemplated", "bar": "bartemplated"})
        assert k.env_vars[0].value == "footemplated"
        assert k.env_vars[0].name == "bartemplated"

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

    @pytest.mark.parametrize(("in_cluster",), ([True], [False]))
    @patch(HOOK_CLASS)
    def test_labels(self, hook_mock, in_cluster):
        hook_mock.return_value.is_in_cluster = in_cluster
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=in_cluster,
            do_xcom_push=False,
        )
        pod = self.run_pod(k)
        assert pod.metadata.labels == {
            "foo": "bar",
            "dag_id": "dag",
            "kubernetes_pod_operator": "True",
            "task_id": "task",
            "try_number": "1",
            "airflow_version": mock.ANY,
            "run_id": "test",
            "airflow_kpo_in_cluster": str(in_cluster),
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
            "try_number": "1",
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
        assert "foo=bar" in label_selector and "hello=airflow" in label_selector

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

    @pytest.mark.parametrize(
        "task_kwargs, base_container_fail, expect_to_delete_pod",
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
        "task_kwargs, should_be_deleted",
        [
            ({}, True),  # default values
            ({"is_delete_operator_pod": True}, True),  # check b/c of is_delete_operator_pod
            ({"is_delete_operator_pod": False}, False),  # check b/c of is_delete_operator_pod
            ({"on_finish_action": "delete_pod"}, True),
            ({"on_finish_action": "delete_succeeded_pod"}, False),
            ({"on_finish_action": "keep_pod"}, False),
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
        with pytest.raises(AirflowException, match="my-failure"):
            context = create_context(k)
            context["ti"].xcom_push = MagicMock()
            k.execute(context=context)
        if should_be_deleted:
            delete_pod_mock.assert_called_with(find_pod_mock.return_value)
        else:
            delete_pod_mock.assert_not_called()

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

    @pytest.mark.parametrize(("randomize_name",), ([True], [False]))
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
            "try_number": "1",
            "airflow_version": mock.ANY,
            "airflow_kpo_in_cluster": str(k.hook.is_in_cluster),
            "run_id": "test",
        }

    @pytest.mark.parametrize(("randomize_name",), ([True], [False]))
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
            "try_number": "1",
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

        yield tpl_file

    @pytest.mark.parametrize(("randomize_name",), ([True], [False]))
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
            "try_number": "1",
            "airflow_version": mock.ANY,
            "airflow_kpo_in_cluster": str(k.hook.is_in_cluster),
            "run_id": "test",
        }
        assert pod.metadata.namespace == "templatenamespace"
        assert pod.spec.containers[0].image == "ubuntu:16.04"
        assert pod.spec.containers[0].image_pull_policy == "Always"
        assert pod.spec.containers[0].command == ["something"]
        assert pod.spec.service_account_name == "foo"
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

    @pytest.mark.parametrize(("randomize_name",), ([True], [False]))
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
            "try_number": "1",
            "airflow_version": mock.ANY,
            "airflow_kpo_in_cluster": str(k.hook.is_in_cluster),
            "run_id": "test",
        }

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

        pod = self.run_pod(k)
        pod_name = XCom.get_one(run_id=self.dag_run.run_id, task_id="task", key="pod_name")
        pod_namespace = XCom.get_one(run_id=self.dag_run.run_id, task_id="task", key="pod_namespace")
        assert pod_name == pod.metadata.name
        assert pod_namespace == pod.metadata.namespace

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

    @patch(f"{POD_MANAGER_CLASS}.delete_pod")
    @patch(f"{KPO_MODULE}.KubernetesPodOperator.patch_already_checked")
    def test_mark_checked_unexpected_exception(self, mock_patch_already_checked, mock_delete_pod):
        """If we aren't deleting pods and have an exception, mark it so we don't reattach to it"""
        k = KubernetesPodOperator(
            task_id="task",
            on_finish_action="keep_pod",
        )
        self.await_pod_mock.side_effect = AirflowException("oops")
        context = create_context(k)
        with pytest.raises(AirflowException):
            k.execute(context=context)
        mock_patch_already_checked.assert_called_once()
        mock_delete_pod.assert_not_called()

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

    @pytest.mark.parametrize(
        "task_kwargs, should_fail, should_be_deleted",
        [
            ({}, False, True),
            ({}, True, True),
            (
                {"is_delete_operator_pod": True, "on_finish_action": "keep_pod"},
                False,
                True,
            ),  # check backcompat of is_delete_operator_pod
            (
                {"is_delete_operator_pod": True, "on_finish_action": "keep_pod"},
                True,
                True,
            ),  # check b/c of is_delete_operator_pod
            (
                {"is_delete_operator_pod": False, "on_finish_action": "delete_pod"},
                False,
                False,
            ),  # check b/c of is_delete_operator_pod
            (
                {"is_delete_operator_pod": False, "on_finish_action": "delete_pod"},
                True,
                False,
            ),  # check b/c of is_delete_operator_pod
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
        dag = DAG("hello2", start_date=pendulum.now())
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
                r"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-[a-z0-9]{8}",
                pod.metadata.name,
            )
            is not None
        )

    def test_task_id_as_name_dag_id_is_ignored(self):
        dag = DAG(dag_id="this_is_a_dag_name", start_date=pendulum.now())
        k = KubernetesPodOperator(
            task_id="a_very_reasonable_task_name",
            dag=dag,
        )
        pod = k.build_pod_request_obj({})
        assert re.match(r"a-very-reasonable-task-name-[a-z0-9-]+", pod.metadata.name) is not None

    @pytest.mark.parametrize(
        "extra_kwargs, actual_exit_code, expected_exc",
        [
            (None, 99, AirflowException),
            ({"skip_on_exit_code": 100}, 100, AirflowSkipException),
            ({"skip_on_exit_code": 100}, 101, AirflowException),
            ({"skip_on_exit_code": None}, 100, AirflowException),
            ({"skip_on_exit_code": [100]}, 100, AirflowSkipException),
            ({"skip_on_exit_code": (100, 101)}, 100, AirflowSkipException),
            ({"skip_on_exit_code": 100}, 101, AirflowException),
            ({"skip_on_exit_code": [100, 102]}, 101, AirflowException),
            ({"skip_on_exit_code": None}, 0, None),
        ],
    )
    @patch(f"{POD_MANAGER_CLASS}.await_pod_completion")
    def test_task_skip_when_pod_exit_with_certain_code(
        self, remote_pod, extra_kwargs, actual_exit_code, expected_exc
    ):
        """Tests that an AirflowSkipException is raised when the container exits with the skip_on_exit_code"""
        k = KubernetesPodOperator(task_id="task", on_finish_action="delete_pod", **(extra_kwargs or {}))

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
        pod = self.run_pod(k)

        # check that the base container is not included in the logs
        mock_fetch_log.assert_called_once_with(pod=pod, containers=["some_init_container"], follow_logs=True)
        # check that KPO waits for the base container to complete before proceeding to extract XCom
        mock_await_container_completion.assert_called_once_with(pod=pod, container_name="base")
        # check that we wait for the xcom sidecar to start before extracting XCom
        mock_await_xcom_sidecar.assert_called_once_with(pod=pod)


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

        operator.execute_complete(
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
    @patch(KUB_OP_PATH.format("build_pod_request_obj"))
    @patch(KUB_OP_PATH.format("get_or_create_pod"))
    def test_async_create_pod_should_execute_successfully(self, mocked_pod, mocked_pod_obj, do_xcom_push):
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
        k.config_file_in_dict_representation = {"a": "b"}

        mocked_pod.return_value.metadata.name = TEST_NAME
        mocked_pod.return_value.metadata.namespace = TEST_NAMESPACE

        context = create_context(k)
        ti_mock = MagicMock()
        context["ti"] = ti_mock

        with pytest.raises(TaskDeferred) as exc:
            k.execute(context)

        assert ti_mock.xcom_push.call_count == 2
        ti_mock.xcom_push.assert_any_call(key="pod_name", value=TEST_NAME)
        ti_mock.xcom_push.assert_any_call(key="pod_namespace", value=TEST_NAMESPACE)
        assert isinstance(exc.value.trigger, KubernetesPodTrigger)

    @patch(KUB_OP_PATH.format("cleanup"))
    @patch(HOOK_CLASS)
    def test_async_create_pod_should_throw_exception(self, mocked_hook, mocked_cleanup):
        """Tests that an AirflowException is raised in case of error event"""

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

        with pytest.raises(AirflowException):
            k.execute_complete(
                context=None,
                event={
                    "status": "error",
                    "message": "Some error",
                    "name": TEST_NAME,
                    "namespace": TEST_NAMESPACE,
                },
            )

    @pytest.mark.parametrize(
        "extra_kwargs, actual_exit_code, expected_exc, pod_status, event_status",
        [
            (None, 0, None, "Succeeded", "success"),
            (None, 99, AirflowException, "Failed", "error"),
            ({"skip_on_exit_code": 100}, 100, AirflowSkipException, "Failed", "error"),
            ({"skip_on_exit_code": 100}, 101, AirflowException, "Failed", "error"),
            ({"skip_on_exit_code": None}, 100, AirflowException, "Failed", "error"),
        ],
    )
    @patch(KUB_OP_PATH.format("pod_manager"))
    @patch(HOOK_CLASS)
    def test_async_create_pod_with_skip_on_exit_code_should_skip(
        self,
        mocked_hook,
        mock_manager,
        extra_kwargs,
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
            **(extra_kwargs or {}),
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
                k.execute_complete(context=context, event=event)
        else:
            k.execute_complete(context=context, event=event)

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
        k.execute_complete(
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
    @patch(KUB_OP_PATH.format("write_logs"))
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
        k.execute_complete(
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
    @patch(KUB_OP_PATH.format("extract_xcom"))
    @patch(HOOK_CLASS)
    @patch(KUB_OP_PATH.format("pod_manager"))
    def test_async_write_logs_should_execute_successfully(
        self, mock_manager, mocked_hook, mock_extract_xcom, post_complete_action, get_logs
    ):
        test_logs = "ok"
        mock_manager.read_pod_logs.return_value = HTTPResponse(
            body=BytesIO(test_logs.encode("utf-8")),
            preload_content=False,
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
            assert f"Container logs: {test_logs}"
            post_complete_action.assert_called_once()
        else:
            mock_manager.return_value.read_pod_logs.assert_not_called()

    @pytest.mark.parametrize(
        "log_pod_spec_on_failure,expect_match",
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
    k.execute_complete({}, success_event)

    # check if it gets the pod
    mocked_hook.return_value.get_pod.assert_called_once_with(TEST_NAME, TEST_NAMESPACE)

    # assert that the xcom are extracted/not extracted
    if do_xcom_push:
        mock_extract_xcom.assert_called_once()
    else:
        mock_extract_xcom.assert_not_called()

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
        k.execute_complete({"ti": ti_mock}, success_event)

    # check if it gets the pod
    mocked_hook.return_value.get_pod.assert_called_once_with(TEST_NAME, TEST_NAMESPACE)

    # assert that it does not push the xcom
    ti_mock.xcom_push.assert_not_called()

    # assert that the xcom are not extracted
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
        k.execute_complete({"ti": ti_mock}, event)

    # assert that the await_pod_completion is not called
    mock_manager.await_pod_completion.assert_not_called()

    # assert that the cleanup is called
    post_complete_action.assert_called_once()
