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

import kubernetes.client.models as k8s
import pendulum

from airflow.exceptions import AirflowException
from airflow.models import DAG, TaskInstance
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils import timezone


# noinspection PyUnusedLocal
class TestKubernetesPodOperator(unittest.TestCase):

    # noinspection DuplicatedCode
    @staticmethod
    def create_context(task):
        dag = DAG(dag_id="dag")
        tzinfo = pendulum.timezone("Europe/Amsterdam")
        execution_date = timezone.datetime(2016, 1, 1, 1, 0, 0, tzinfo=tzinfo)
        task_instance = TaskInstance(task=task,
                                     execution_date=execution_date)
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
            image_pull_secrets=fake_pull_secrets,
            cluster_context='default',
        )
        monitor_mock.return_value = (State.SUCCESS, None)
        context = self.create_context(k)
        k.execute(context=context)
        self.assertEqual(
            start_mock.call_args[0][0].spec.image_pull_secrets,
            [k8s.V1LocalObjectReference(name=fake_pull_secrets)]
        )

    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.start_pod")
    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.monitor_pod")
    @mock.patch("airflow.kubernetes.pod_launcher.PodLauncher.delete_pod")
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    def test_pod_delete_even_on_launcher_error(
            self,
            mock_client,
            delete_pod_mock,
            monitor_pod_mock,
            start_pod_mock):
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
