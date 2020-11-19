# pylint: disable=unused-argument
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
import unittest
from unittest import mock

import pendulum
from kubernetes.client import ApiClient, models as k8s

from airflow.exceptions import AirflowException
from airflow.models import DAG, TaskInstance
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils import timezone


class TestKubernetesPodOperator(unittest.TestCase):
    @staticmethod
    def create_context(task):
        dag = DAG(dag_id="dag")
        tzinfo = pendulum.timezone("Europe/Amsterdam")
        execution_date = timezone.datetime(2016, 1, 1, 1, 0, 0, tzinfo=tzinfo)
        task_instance = TaskInstance(task=task, execution_date=execution_date)
        return {
            "dag": dag,
            "ts": execution_date.isoformat(),
            "task": task,
            "ti": task_instance,
        }

    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.start_pod")
    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.monitor_pod")
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    def test_config_path(self, client_mock, monitor_mock, start_mock):  # pylint: disable=unused-argument
        from airflow.utils.state import State

        file_path = "/tmp/fake_file"
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
            config_file=file_path,
            cluster_context='default',
        )
        monitor_mock.return_value = (State.SUCCESS, None)
        client_mock.list_namespaced_pod.return_value = []
        context = self.create_context(k)
        k.execute(context=context)
        client_mock.assert_called_once_with(
            in_cluster=False,
            cluster_context='default',
            config_file=file_path,
        )

    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.start_pod")
    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.monitor_pod")
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    def test_image_pull_secrets_correctly_set(self, mock_client, monitor_mock, start_mock):
        from airflow.utils.state import State

        fake_pull_secrets = "fakeSecret"
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
            image_pull_secrets=[k8s.V1LocalObjectReference(fake_pull_secrets)],
            cluster_context='default',
        )
        monitor_mock.return_value = (State.SUCCESS, None)
        context = self.create_context(k)
        k.execute(context=context)
        self.assertEqual(
            start_mock.call_args[0][0].spec.image_pull_secrets,
            [k8s.V1LocalObjectReference(name=fake_pull_secrets)],
        )

    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.start_pod")
    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.monitor_pod")
    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.delete_pod")
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    def test_pod_delete_even_on_launcher_error(
        self, mock_client, delete_pod_mock, monitor_pod_mock, start_pod_mock
    ):
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
            cluster_context='default',
            is_delete_operator_pod=True,
        )
        monitor_pod_mock.side_effect = AirflowException('fake failure')
        with self.assertRaises(AirflowException):
            context = self.create_context(k)
            k.execute(context=context)
        assert delete_pod_mock.called

    def test_jinja_templated_fields(self):
        task = KubernetesPodOperator(
            namespace='default',
            image="{{ image_jinja }}:16.04",
            cmds=["bash", "-cx"],
            name="test_pod",
            task_id="task",
        )

        self.assertEqual(task.image, "{{ image_jinja }}:16.04")
        task.render_template_fields(context={"image_jinja": "ubuntu"})
        self.assertEqual(task.image, "ubuntu:16.04")

    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.start_pod")
    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.monitor_pod")
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    def test_randomize_pod_name(self, mock_client, monitor_mock, start_mock):
        from airflow.utils.state import State

        name_base = 'test'

        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name=name_base,
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            cluster_context='default',
        )
        monitor_mock.return_value = (State.SUCCESS, None)
        context = self.create_context(k)
        k.execute(context=context)

        assert start_mock.call_args[0][0].metadata.name.startswith(name_base)
        assert start_mock.call_args[0][0].metadata.name != name_base

    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.start_pod")
    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.monitor_pod")
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    def test_describes_pod_on_failure(self, mock_client, monitor_mock, start_mock):
        from airflow.utils.state import State

        name_base = 'test'

        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name=name_base,
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            cluster_context='default',
        )
        monitor_mock.return_value = (State.FAILED, None)
        failed_pod_status = 'read_pod_namespaced_result'
        read_namespaced_pod_mock = mock_client.return_value.read_namespaced_pod
        read_namespaced_pod_mock.return_value = failed_pod_status

        with self.assertRaises(AirflowException) as cm:
            context = self.create_context(k)
            k.execute(context=context)

        self.assertEqual(
            str(cm.exception),
            f"Pod Launching failed: Pod {k.pod.metadata.name} returned a failure: {failed_pod_status}",
        )
        assert mock_client.return_value.read_namespaced_pod.called
        self.assertEqual(read_namespaced_pod_mock.call_args[0][0], k.pod.metadata.name)

    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.start_pod")
    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.monitor_pod")
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    def test_no_need_to_describe_pod_on_success(self, mock_client, monitor_mock, start_mock):
        from airflow.utils.state import State

        name_base = 'test'

        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name=name_base,
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            cluster_context='default',
        )
        monitor_mock.return_value = (State.SUCCESS, None)

        context = self.create_context(k)
        k.execute(context=context)

        assert not mock_client.return_value.read_namespaced_pod.called

    def test_create_with_affinity(self):
        name_base = 'test'

        affinity = {
            'nodeAffinity': {
                'preferredDuringSchedulingIgnoredDuringExecution': [
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
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name=name_base,
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            cluster_context='default',
            affinity=affinity,
        )

        result = k.create_pod_request_obj()
        client = ApiClient()
        self.assertEqual(type(result.spec.affinity), k8s.V1Affinity)
        self.assertEqual(client.sanitize_for_serialization(result)['spec']['affinity'], affinity)

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
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name=name_base,
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            cluster_context='default',
            affinity=k8s_api_affinity,
        )

        result = k.create_pod_request_obj()
        self.assertEqual(type(result.spec.affinity), k8s.V1Affinity)
        self.assertEqual(client.sanitize_for_serialization(result)['spec']['affinity'], affinity)

    def test_tolerations(self):
        k8s_api_tolerations = [k8s.V1Toleration(key="key", operator="Equal", value="value")]

        tolerations = [{'key': "key", 'operator': 'Equal', 'value': 'value'}]

        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="name",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            cluster_context='default',
            tolerations=tolerations,
        )

        result = k.create_pod_request_obj()
        client = ApiClient()
        self.assertEqual(type(result.spec.tolerations[0]), k8s.V1Toleration)
        self.assertEqual(client.sanitize_for_serialization(result)['spec']['tolerations'], tolerations)

        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="name",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            cluster_context='default',
            tolerations=k8s_api_tolerations,
        )

        result = k.create_pod_request_obj()
        self.assertEqual(type(result.spec.tolerations[0]), k8s.V1Toleration)
        self.assertEqual(client.sanitize_for_serialization(result)['spec']['tolerations'], tolerations)

    def test_node_selector(self):
        k8s_api_node_selector = k8s.V1NodeSelector(
            node_selector_terms=[
                k8s.V1NodeSelectorTerm(
                    match_expressions=[
                        k8s.V1NodeSelectorRequirement(key="disktype", operator="In", values=["ssd"])
                    ]
                )
            ]
        )

        node_selector = {
            'nodeSelectorTerms': [
                {'matchExpressions': [{'key': 'disktype', 'operator': 'In', 'values': ['ssd']}]}
            ]
        }

        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="name",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            cluster_context='default',
            node_selector=k8s_api_node_selector,
        )

        result = k.create_pod_request_obj()
        client = ApiClient()
        self.assertEqual(type(result.spec.node_selector), k8s.V1NodeSelector)
        self.assertEqual(client.sanitize_for_serialization(result)['spec']['nodeSelector'], node_selector)

        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="name",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            cluster_context='default',
            node_selector=k8s_api_node_selector,
        )

        result = k.create_pod_request_obj()
        client = ApiClient()
        self.assertEqual(type(result.spec.node_selector), k8s.V1NodeSelector)
        self.assertEqual(client.sanitize_for_serialization(result)['spec']['nodeSelector'], node_selector)

        # repeat tests using deprecated parameter
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="name",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            cluster_context='default',
            node_selectors=node_selector,
        )

        result = k.create_pod_request_obj()
        client = ApiClient()
        self.assertEqual(type(result.spec.node_selector), k8s.V1NodeSelector)
        self.assertEqual(client.sanitize_for_serialization(result)['spec']['nodeSelector'], node_selector)

        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="name",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            cluster_context='default',
            node_selectors=node_selector,
        )

        result = k.create_pod_request_obj()
        client = ApiClient()
        self.assertEqual(type(result.spec.node_selector), k8s.V1NodeSelector)
        self.assertEqual(client.sanitize_for_serialization(result)['spec']['nodeSelector'], node_selector)
