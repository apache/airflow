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
from contextlib import nullcontext
from unittest import mock
from unittest.mock import MagicMock, patch

import pendulum
import pytest
from kubernetes.client import ApiClient, models as k8s

from airflow.exceptions import AirflowException
from airflow.models import DAG, DagModel, DagRun, TaskInstance
from airflow.models.xcom import XCom
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
    _suppress,
    _task_id_to_pod_name,
)
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.types import DagRunType
from tests.test_utils import db

DEFAULT_DATE = timezone.datetime(2016, 1, 1, 1, 0, 0)
KPO_MODULE = "airflow.providers.cncf.kubernetes.operators.kubernetes_pod"
POD_MANAGER_CLASS = "airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager"
POD_MANAGER_MODULE = "airflow.providers.cncf.kubernetes.utils.pod_manager"
HOOK_CLASS = "airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesHook"


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
    def setup(self, dag_maker):
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
        return self.await_start_mock.call_args[1]["pod"]

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
            conn_id=None,
            config_file=file_path,
            in_cluster=None,
        )

    def test_env_vars(self):
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            env_vars=[k8s.V1EnvVar(name="{{ bar }}", value="{{ foo }}")],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
        )
        k.render_template_fields(context={"foo": "footemplated", "bar": "bartemplated"})
        assert k.env_vars[0].value == "footemplated"
        assert k.env_vars[0].name == "bartemplated"

    def test_security_context(self):
        security_context = {
            "runAsUser": 1245,
        }
        k = KubernetesPodOperator(
            security_context=security_context,
            task_id="task",
        )
        pod = k.build_pod_request_obj(create_context(k))
        assert pod.spec.security_context == security_context

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
            "dag_id=dag,kubernetes_pod_operator=True,run_id=test,task_id=task,"
            "already_checked!=True,!airflow-worker"
        )

    def test_image_pull_secrets_correctly_set(self):
        fake_pull_secrets = "fakeSecret"
        k = KubernetesPodOperator(
            task_id="task",
            image_pull_secrets=[k8s.V1LocalObjectReference(fake_pull_secrets)],
        )

        pod = k.build_pod_request_obj(create_context(k))
        assert pod.spec.image_pull_secrets == [k8s.V1LocalObjectReference(name=fake_pull_secrets)]

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

    @patch(HOOK_CLASS)
    def test_xcom_sidecar_container_image_default(self, hook_mock):
        hook_mock.return_value.get_xcom_sidecar_container_image.return_value = None
        k = KubernetesPodOperator(
            name="test",
            task_id="task",
            do_xcom_push=True,
        )
        pod = k.build_pod_request_obj(create_context(k))
        assert pod.spec.containers[1].image == "alpine"

    @patch(HOOK_CLASS)
    def test_xcom_sidecar_container_image_custom(self, hook_mock):
        hook_mock.return_value.get_xcom_sidecar_container_image.return_value = "private.repo/alpine:3.13"
        k = KubernetesPodOperator(
            name="test",
            task_id="task",
            do_xcom_push=True,
        )
        pod = k.build_pod_request_obj(create_context(k))
        assert pod.spec.containers[1].image == "private.repo/alpine:3.13"

    def test_image_pull_policy_correctly_set(self):
        k = KubernetesPodOperator(
            task_id="task",
            image_pull_policy="Always",
        )
        pod = k.build_pod_request_obj(create_context(k))
        assert pod.spec.containers[0].image_pull_policy == "Always"

    @patch(f"{POD_MANAGER_CLASS}.delete_pod")
    @patch(f"{KPO_MODULE}.KubernetesPodOperator.find_pod")
    def test_pod_delete_after_await_container_error(self, find_pod_mock, delete_pod_mock):
        """
        When KPO fails unexpectedly during await_container, we should still try to delete the pod,
        and the pod we try to delete should be the one returned from find_pod earlier.
        """
        cont_status = MagicMock()
        cont_status.name = "base"
        cont_status.state.terminated.message = "my-failure"
        find_pod_mock.return_value.status.container_statuses = [cont_status]
        k = KubernetesPodOperator(task_id="task")
        self.await_pod_mock.side_effect = AirflowException("fake failure")
        with pytest.raises(AirflowException, match="my-failure"):
            context = create_context(k)
            k.execute(context=context)
        delete_pod_mock.assert_called_with(find_pod_mock.return_value)

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
            is_delete_operator_pod=True,
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
        pod_template_yaml = b"""
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
        tpl_file.write_bytes(pod_template_yaml)

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
            is_delete_operator_pod=False,
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

    @pytest.mark.parametrize("should_fail", [True, False])
    @patch(f"{POD_MANAGER_CLASS}.delete_pod")
    @patch(f"{KPO_MODULE}.KubernetesPodOperator.patch_already_checked")
    def test_mark_checked_if_not_deleted(self, mock_patch_already_checked, mock_delete_pod, should_fail):
        """If we aren't deleting pods mark "checked" if the task completes (successful or otherwise)"""
        dag = DAG("hello2", start_date=pendulum.now())
        k = KubernetesPodOperator(
            task_id="task",
            is_delete_operator_pod=False,
            dag=dag,
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
        mock_patch_already_checked.assert_called_once()
        mock_delete_pod.assert_not_called()

    def test_task_id_as_name(self):
        k = KubernetesPodOperator(
            task_id=".hi.-_09HI",
            random_name_suffix=False,
        )
        pod = k.build_pod_request_obj({})
        assert pod.metadata.name == "0.hi.--09hi"

    def test_task_id_as_name_with_suffix(self):
        k = KubernetesPodOperator(
            task_id=".hi.-_09HI",
            random_name_suffix=True,
        )
        pod = k.build_pod_request_obj({})
        expected = "0.hi.--09hi"
        assert pod.metadata.name.startswith(expected)
        assert re.match(rf"{expected}-[a-z0-9-]+", pod.metadata.name) is not None

    def test_task_id_as_name_with_suffix_very_long(self):
        k = KubernetesPodOperator(
            task_id="a" * 250,
            random_name_suffix=True,
        )
        pod = k.build_pod_request_obj({})
        assert re.match(r"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-[a-z0-9-]+", pod.metadata.name) is not None

    def test_task_id_as_name_dag_id_is_ignored(self):
        dag = DAG(dag_id="this_is_a_dag_name", start_date=pendulum.now())
        k = KubernetesPodOperator(
            task_id="a_very_reasonable_task_name",
            dag=dag,
        )
        pod = k.build_pod_request_obj({})
        assert re.match(r"a-very-reasonable-task-name-[a-z0-9-]+", pod.metadata.name) is not None


def test__suppress(caplog):
    with _suppress(ValueError):
        raise ValueError("failure")

    assert "ValueError: failure" in caplog.text


@pytest.mark.parametrize(
    "val, expected",
    [
        ("task-id", "task-id"),  # no problem
        ("task_id", "task-id"),  # underscores
        ("task.id", "task.id"),  # dots ok
        (".task.id", "0.task.id"),  # leading dot invalid
        ("-90Abc*&", "0-90abc--0"),  # invalid ends
        ("90AçLbˆˆç˙ßß˜˜˙c*a", "90a-lb---------c-a"),  # weird unicode
    ],
)
def test_task_id_to_pod_name(val, expected):
    assert _task_id_to_pod_name(val) == expected


def test_task_id_to_pod_name_long():
    with pytest.raises(ValueError, match="longer than 253"):
        _task_id_to_pod_name("0" * 254)
