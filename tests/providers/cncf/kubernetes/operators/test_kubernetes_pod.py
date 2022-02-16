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
from tempfile import NamedTemporaryFile
from unittest import mock
from unittest.mock import MagicMock

import pytest
from kubernetes.client import ApiClient, models as k8s
from parameterized import parameterized

from airflow.exceptions import AirflowException
from airflow.models import DAG, DagRun, TaskInstance
from airflow.models.xcom import IN_MEMORY_DAGRUN_ID
from airflow.operators.dummy import DummyOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
    _prune_dict,
    _suppress,
)
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2016, 1, 1, 1, 0, 0)


def create_context(task):
    dag = DAG(dag_id="dag")
    task_instance = TaskInstance(task=task, run_id=IN_MEMORY_DAGRUN_ID)
    task_instance.dag_run = DagRun(run_id=IN_MEMORY_DAGRUN_ID)
    return {
        "dag": dag,
        "ts": DEFAULT_DATE.isoformat(),
        "task": task,
        "ti": task_instance,
        "task_instance": task_instance,
    }


POD_MANAGER_CLASS = "airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager"


class TestKubernetesPodOperator:
    def setup_method(self):
        self.create_pod_patch = mock.patch(f"{POD_MANAGER_CLASS}.create_pod")
        self.await_pod_patch = mock.patch(f"{POD_MANAGER_CLASS}.await_pod_start")
        self.await_pod_completion_patch = mock.patch(f"{POD_MANAGER_CLASS}.await_pod_completion")
        self.client_patch = mock.patch("airflow.kubernetes.kube_client.get_kube_client")
        self.create_mock = self.create_pod_patch.start()
        self.await_start_mock = self.await_pod_patch.start()
        self.await_pod_mock = self.await_pod_completion_patch.start()
        self.client_mock = self.client_patch.start()

    def teardown_method(self):
        self.create_pod_patch.stop()
        self.await_pod_patch.stop()
        self.await_pod_completion_patch.stop()
        self.client_patch.stop()

    @staticmethod
    def create_context(task):
        dag = DAG(dag_id="dag")
        task_instance = TaskInstance(task=task, run_id=IN_MEMORY_DAGRUN_ID)
        task_instance.dag_run = DagRun(run_id=IN_MEMORY_DAGRUN_ID)
        return {
            "dag": dag,
            "ts": DEFAULT_DATE.isoformat(),
            "task": task,
            "ti": task_instance,
            "task_instance": task_instance,
        }

    def run_pod(self, operator, context=None) -> k8s.V1Pod:
        if context is None:
            context = create_context(operator)
        remote_pod_mock = MagicMock()
        remote_pod_mock.status.phase = 'Succeeded'
        self.await_pod_mock.return_value = remote_pod_mock
        operator.execute(context=context)
        return self.await_start_mock.call_args[1]['pod']

    def sanitize_for_serialization(self, obj):
        return ApiClient().sanitize_for_serialization(obj)

    def test_config_path(self):
        file_path = "/tmp/fake_file"
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            config_file=file_path,
            cluster_context="default",
        )
        remote_pod_mock = MagicMock()
        remote_pod_mock.status.phase = 'Succeeded'
        self.await_pod_mock.return_value = remote_pod_mock
        self.client_mock.list_namespaced_pod.return_value = []
        context = create_context(k)
        k.execute(context=context)
        self.client_mock.assert_called_once_with(
            in_cluster=False,
            cluster_context="default",
            config_file=file_path,
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

    def test_envs_from_configmaps(
        self,
    ):
        configmap_name = "test-config-map"
        env_from = [k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name=configmap_name))]
        # WHEN
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            env_from=env_from,
        )
        pod = self.run_pod(k)
        assert pod.spec.containers[0].env_from == env_from

    def test_labels(self):
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
        pod = self.run_pod(k)
        assert pod.metadata.labels == {
            "foo": "bar",
            "dag_id": "dag",
            "kubernetes_pod_operator": "True",
            "task_id": "task",
            "try_number": "1",
            "airflow_version": mock.ANY,
            "execution_date": mock.ANY,
        }

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
        self.client_mock.return_value.list_namespaced_pod.assert_called_once()
        _, kwargs = self.client_mock.return_value.list_namespaced_pod.call_args
        assert (
            kwargs['label_selector']
            == 'dag_id=dag,execution_date=2016-01-01T0100000000-26816529d,task_id=task,already_checked!=True'
        )

    def test_image_pull_secrets_correctly_set(self):
        fake_pull_secrets = "fakeSecret"
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            image_pull_secrets=[k8s.V1LocalObjectReference(fake_pull_secrets)],
            cluster_context="default",
        )

        pod = k.build_pod_request_obj(create_context(k))
        assert pod.spec.image_pull_secrets == [k8s.V1LocalObjectReference(name=fake_pull_secrets)]

    def test_image_pull_policy_correctly_set(self):
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            image_pull_policy="Always",
            cluster_context="default",
        )
        pod = k.build_pod_request_obj(create_context(k))
        assert pod.spec.containers[0].image_pull_policy == "Always"

    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.delete_pod")
    def test_pod_delete_even_on_launcher_error(self, delete_pod_mock):
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            cluster_context="default",
            is_delete_operator_pod=True,
        )
        self.await_pod_mock.side_effect = AirflowException("fake failure")
        with pytest.raises(AirflowException):
            context = create_context(k)
            k.execute(context=context)
        assert delete_pod_mock.called

    @pytest.mark.parametrize('randomize', [True, False])
    def test_provided_pod_name(self, randomize):
        name_base = "test"

        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            name=name_base,
            random_name_suffix=randomize,
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            cluster_context="default",
        )
        context = create_context(k)
        pod = k.build_pod_request_obj(context)

        if randomize:
            assert pod.metadata.name.startswith(name_base)
            assert pod.metadata.name != name_base
        else:
            assert pod.metadata.name == name_base

    def test_pod_name_required(self):
        with pytest.raises(AirflowException, match="`name` is required"):
            KubernetesPodOperator(
                namespace="default",
                image="ubuntu:16.04",
                task_id="task",
                in_cluster=False,
                do_xcom_push=False,
                cluster_context="default",
            )

    @parameterized.expand([[True], [False]])
    def test_full_pod_spec(self, randomize_name):
        pod_spec_name_base = "hello"
        pod_spec = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(
                name=pod_spec_name_base, labels={"foo": "bar"}, namespace="mynamespace"
            ),
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

        k = KubernetesPodOperator(
            task_id="task",
            random_name_suffix=randomize_name,
            in_cluster=False,
            do_xcom_push=False,
            cluster_context="default",
            full_pod_spec=pod_spec,
        )
        pod = self.run_pod(k)

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
            "execution_date": mock.ANY,
        }

        # kwargs take precedence, however
        image = "some.custom.image:andtag"
        name_base = "world"
        k = KubernetesPodOperator(
            task_id="task",
            random_name_suffix=randomize_name,
            in_cluster=False,
            do_xcom_push=False,
            cluster_context="default",
            full_pod_spec=pod_spec,
            name=name_base,
            image=image,
            labels={"hello": "world"},
        )
        pod = self.run_pod(k)

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
            "execution_date": mock.ANY,
        }

    @parameterized.expand([[True], [False]])
    def test_pod_template_file(self, randomize_name):
        pod_template_yaml = b"""
            apiVersion: v1
            kind: Pod
            metadata:
              name: hello
              namespace: mynamespace
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

        with NamedTemporaryFile() as tpl_file:
            tpl_file.write(pod_template_yaml)
            tpl_file.flush()

            k = KubernetesPodOperator(
                task_id="task",
                random_name_suffix=randomize_name,
                pod_template_file=tpl_file.name,
            )
            pod = self.run_pod(k)

            if randomize_name:
                assert pod.metadata.name.startswith("hello")
                assert pod.metadata.name != "hello"
            else:
                pod.metadata.name == "hello"
            # Check labels are added from pod_template_file and
            # the pod identifying labels including Airflow version
            assert pod.metadata.labels == {
                "foo": "bar",
                "dag_id": "dag",
                "kubernetes_pod_operator": "True",
                "task_id": "task",
                "try_number": "1",
                "airflow_version": mock.ANY,
                "execution_date": mock.ANY,
            }
            assert pod.metadata.namespace == "mynamespace"
            assert pod.spec.containers[0].image == "ubuntu:16.04"
            assert pod.spec.containers[0].image_pull_policy == "Always"
            assert pod.spec.containers[0].command == ["something"]
            assert pod.spec.service_account_name == "foo"
            affinity = {
                'node_affinity': {
                    'preferred_during_scheduling_ignored_during_execution': [
                        {
                            'preference': {
                                'match_expressions': [
                                    {'key': 'kubernetes.io/role', 'operator': 'In', 'values': ['foo', 'bar']}
                                ],
                                'match_fields': None,
                            },
                            'weight': 1,
                        }
                    ],
                    'required_during_scheduling_ignored_during_execution': {
                        'node_selector_terms': [
                            {
                                'match_expressions': [
                                    {'key': 'kubernetes.io/role', 'operator': 'In', 'values': ['foo', 'bar']}
                                ],
                                'match_fields': None,
                            }
                        ]
                    },
                },
                'pod_affinity': None,
                'pod_anti_affinity': None,
            }

            assert pod.spec.affinity.to_dict() == affinity

            # kwargs take precedence, however
            image = "some.custom.image:andtag"
            name_base = "world"
            k = KubernetesPodOperator(
                task_id="task",
                pod_template_file=tpl_file.name,
                name=name_base,
                random_name_suffix=randomize_name,
                image=image,
                labels={"hello": "world"},
            )
            pod = self.run_pod(k)

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
                "execution_date": mock.ANY,
            }

    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.follow_container_logs")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.await_container_completion")
    def test_describes_pod_on_failure(self, await_container_mock, follow_container_mock):
        name_base = "test"

        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name=name_base,
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            cluster_context="default",
        )
        follow_container_mock.return_value = None
        remote_pod_mock = MagicMock()
        remote_pod_mock.status.phase = 'Failed'
        self.await_pod_mock.return_value = remote_pod_mock

        with pytest.raises(AirflowException, match=f"Pod {name_base}.[a-z0-9]+ returned a failure: .*"):
            context = create_context(k)
            k.execute(context=context)

        assert not self.client_mock.return_value.read_namespaced_pod.called

    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.follow_container_logs")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.await_container_completion")
    def test_no_handle_failure_on_success(self, await_container_mock, follow_container_mock):
        name_base = "test"

        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name=name_base,
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            cluster_context="default",
        )

        follow_container_mock.return_value = None
        remote_pod_mock = MagicMock()
        remote_pod_mock.status.phase = 'Succeeded'
        self.await_pod_mock.return_value = remote_pod_mock

        context = create_context(k)

        # assert does not raise
        k.execute(context=context)

    def test_create_with_affinity(self):
        name_base = "test"

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
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name=name_base,
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            cluster_context="default",
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
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name=name_base,
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            cluster_context="default",
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
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="name",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            cluster_context="default",
            tolerations=tolerations,
        )

        pod = k.build_pod_request_obj(create_context(k))
        sanitized_pod = self.sanitize_for_serialization(pod)
        assert isinstance(pod.spec.tolerations[0], k8s.V1Toleration)
        assert sanitized_pod["spec"]["tolerations"] == tolerations

        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="name",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            cluster_context="default",
            tolerations=k8s_api_tolerations,
        )

        pod = k.build_pod_request_obj(create_context(k))
        sanitized_pod = self.sanitize_for_serialization(pod)
        assert isinstance(pod.spec.tolerations[0], k8s.V1Toleration)
        assert sanitized_pod["spec"]["tolerations"] == tolerations

    def test_node_selector(self):
        node_selector = {"beta.kubernetes.io/os": "linux"}

        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="name",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            cluster_context="default",
            node_selector=node_selector,
        )

        pod = k.build_pod_request_obj(create_context(k))
        sanitized_pod = self.sanitize_for_serialization(pod)
        assert isinstance(pod.spec.node_selector, dict)
        assert sanitized_pod["spec"]["nodeSelector"] == node_selector

        # repeat tests using deprecated parameter
        with pytest.warns(
            DeprecationWarning, match="node_selectors is deprecated. Please use node_selector instead."
        ):
            k = KubernetesPodOperator(
                namespace="default",
                image="ubuntu:16.04",
                cmds=["bash", "-cx"],
                arguments=["echo 10"],
                labels={"foo": "bar"},
                name="name",
                task_id="task",
                in_cluster=False,
                do_xcom_push=False,
                cluster_context="default",
                node_selectors=node_selector,
            )

        pod = k.build_pod_request_obj(create_context(k))
        sanitized_pod = self.sanitize_for_serialization(pod)
        assert isinstance(pod.spec.node_selector, dict)
        assert sanitized_pod["spec"]["nodeSelector"] == node_selector

    @pytest.mark.parametrize('do_xcom_push', [True, False])
    @mock.patch(f"{POD_MANAGER_CLASS}.extract_xcom")
    def test_push_xcom_pod_info(self, mock_extract_xcom, dag_maker, do_xcom_push):
        """pod name and namespace are *always* pushed; do_xcom_push only controls xcom sidecar"""
        mock_extract_xcom.return_value = '{}'
        with dag_maker():
            KubernetesPodOperator(
                namespace="default",
                image="ubuntu:16.04",
                cmds=["bash", "-cx"],
                name="test",
                task_id="task",
                in_cluster=False,
                do_xcom_push=do_xcom_push,
            )
            DummyOperator(task_id='task_to_pull_xcom')
        dagrun = dag_maker.create_dagrun()
        tis = {ti.task_id: ti for ti in dagrun.task_instances}

        pod = self.run_pod(
            tis["task"].task,
            context=tis["task"].get_template_context(session=dag_maker.session),
        )
        pod_name = tis["task_to_pull_xcom"].xcom_pull(task_ids="task", key='pod_name')
        pod_namespace = tis["task_to_pull_xcom"].xcom_pull(task_ids="task", key='pod_namespace')
        assert pod_name and pod_name == pod.metadata.name
        assert pod_namespace and pod_namespace == pod.metadata.namespace

    def test_previous_pods_ignored_for_reattached(self):
        """
        When looking for pods to possibly reattach to,
        ignore pods from previous tries that were properly finished
        """
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            name="test",
            task_id="task",
        )
        self.run_pod(k)
        self.client_mock.return_value.list_namespaced_pod.assert_called_once()
        _, kwargs = self.client_mock.return_value.list_namespaced_pod.call_args
        assert 'already_checked!=True' in kwargs['label_selector']

    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.delete_pod")
    @mock.patch(
        "airflow.providers.cncf.kubernetes.operators.kubernetes_pod"
        ".KubernetesPodOperator.patch_already_checked"
    )
    def test_mark_created_pod_if_not_deleted(self, mock_patch_already_checked, mock_delete_pod):
        """If we aren't deleting pods and have a failure, mark it so we don't reattach to it"""
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            name="test",
            task_id="task",
            is_delete_operator_pod=False,
        )
        remote_pod_mock = MagicMock()
        remote_pod_mock.status.phase = 'Failed'
        self.await_pod_mock.return_value = remote_pod_mock
        context = create_context(k)
        with pytest.raises(AirflowException):
            k.execute(context=context)
        mock_patch_already_checked.assert_called_once()
        mock_delete_pod.assert_not_called()

    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.delete_pod")
    @mock.patch(
        "airflow.providers.cncf.kubernetes.operators.kubernetes_pod"
        ".KubernetesPodOperator.patch_already_checked"
    )
    def test_mark_created_pod_if_not_deleted_during_exception(
        self, mock_patch_already_checked, mock_delete_pod
    ):
        """If we aren't deleting pods and have an exception, mark it so we don't reattach to it"""
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            name="test",
            task_id="task",
            is_delete_operator_pod=False,
        )
        self.await_pod_mock.side_effect = AirflowException("oops")
        context = create_context(k)
        with pytest.raises(AirflowException):
            k.execute(context=context)
        mock_patch_already_checked.assert_called_once()
        mock_delete_pod.assert_not_called()

    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.delete_pod")
    @mock.patch(
        "airflow.providers.cncf.kubernetes.operators."
        "kubernetes_pod.KubernetesPodOperator.patch_already_checked"
    )
    def test_mark_reattached_pod_if_not_deleted(self, mock_patch_already_checked, mock_delete_pod):
        """If we aren't deleting pods and have a failure, mark it so we don't reattach to it"""
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            name="test",
            task_id="task",
            is_delete_operator_pod=False,
        )
        remote_pod_mock = MagicMock()
        remote_pod_mock.status.phase = 'Failed'
        self.await_pod_mock.return_value = remote_pod_mock

        context = create_context(k)
        with pytest.raises(AirflowException):
            k.execute(context=context)
        mock_patch_already_checked.assert_called_once()
        mock_delete_pod.assert_not_called()


def test__suppress():
    with mock.patch('logging.Logger.error') as mock_error:

        with _suppress(ValueError):
            raise ValueError("failure")

        mock_error.assert_called_once_with("failure", exc_info=True)


@pytest.mark.parametrize(
    'mode, expected',
    [
        (
            'strict',
            {
                'b': '',
                'c': {'b': '', 'c': 'hi', 'd': ['', 0, '1']},
                'd': ['', 0, '1'],
                'e': ['', 0, {'b': '', 'c': 'hi', 'd': ['', 0, '1']}, ['', 0, '1'], ['']],
            },
        ),
        (
            'truthy',
            {
                'c': {'c': 'hi', 'd': ['1']},
                'd': ['1'],
                'e': [{'c': 'hi', 'd': ['1']}, ['1']],
            },
        ),
    ],
)
def test__prune_dict(mode, expected):
    l1 = ['', 0, '1', None]
    d1 = {'a': None, 'b': '', 'c': 'hi', 'd': l1}
    d2 = {'a': None, 'b': '', 'c': d1, 'd': l1, 'e': [None, '', 0, d1, l1, ['']]}
    assert _prune_dict(d2, mode=mode) == expected
