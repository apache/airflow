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

from unittest import mock

from airflow.callbacks.callback_requests import CallbackRequest
from airflow.configuration import conf
from airflow.executors.local_executor import LocalExecutor
from airflow.executors.local_kubernetes_executor import LocalKubernetesExecutor


class TestLocalKubernetesExecutor:
    def test_supports_pickling(self):
        assert not LocalKubernetesExecutor.supports_pickling

    def test_supports_sentry(self):
        assert not LocalKubernetesExecutor.supports_sentry

    def test_is_local_default_value(self):
        assert not LocalKubernetesExecutor.is_local

    def test_serve_logs_default_value(self):
        assert LocalKubernetesExecutor.serve_logs

    def test_queued_tasks(self):
        local_executor_mock = mock.MagicMock()
        k8s_executor_mock = mock.MagicMock()
        local_kubernetes_executor = LocalKubernetesExecutor(local_executor_mock, k8s_executor_mock)

        local_queued_tasks = {("dag_id", "task_id", "2020-08-30", 1): "queued_command"}
        k8s_queued_tasks = {("dag_id_2", "task_id_2", "2020-08-30", 2): "queued_command"}

        local_executor_mock.queued_tasks = local_queued_tasks
        k8s_executor_mock.queued_tasks = k8s_queued_tasks

        expected_queued_tasks = {**local_queued_tasks, **k8s_queued_tasks}

        assert local_kubernetes_executor.queued_tasks == expected_queued_tasks
        assert len(local_kubernetes_executor.queued_tasks) == 2

    def test_running(self):
        local_executor_mock = mock.MagicMock()
        k8s_executor_mock = mock.MagicMock()
        local_kubernetes_executor = LocalKubernetesExecutor(local_executor_mock, k8s_executor_mock)

        local_running_tasks = {("dag_id", "task_id", "2020-08-30", 1)}
        k8s_running_tasks = {}

        local_executor_mock.running = local_running_tasks
        k8s_executor_mock.running = k8s_running_tasks

        assert local_kubernetes_executor.running == local_running_tasks.union(k8s_running_tasks)
        assert len(local_kubernetes_executor.running) == 1

    def test_slots_available(self):
        local_executor = LocalExecutor()
        k8s_executor_mock = mock.MagicMock()
        local_kubernetes_executor = LocalKubernetesExecutor(local_executor, k8s_executor_mock)

        # Should be equal to Local Executor default parallelism.
        assert local_kubernetes_executor.slots_available == conf.getint("core", "PARALLELISM")

    def test_kubernetes_executor_knows_its_queue(self):
        local_executor_mock = mock.MagicMock()
        k8s_executor_mock = mock.MagicMock()
        LocalKubernetesExecutor(local_executor_mock, k8s_executor_mock)

        assert k8s_executor_mock.kubernetes_queue == conf.get("local_kubernetes_executor", "kubernetes_queue")

    def test_log_is_fetched_from_k8s_executor_only_for_k8s_queue(self):
        local_executor_mock = mock.MagicMock()
        k8s_executor_mock = mock.MagicMock()

        KUBERNETES_QUEUE = conf.get("local_kubernetes_executor", "kubernetes_queue")
        LocalKubernetesExecutor(local_executor_mock, k8s_executor_mock)
        local_k8s_exec = LocalKubernetesExecutor(local_executor_mock, k8s_executor_mock)
        simple_task_instance = mock.MagicMock()
        simple_task_instance.queue = KUBERNETES_QUEUE
        local_k8s_exec.get_task_log(ti=simple_task_instance, log="")
        k8s_executor_mock.get_task_log.assert_called_once_with(ti=simple_task_instance, log=mock.ANY)

        k8s_executor_mock.reset_mock()

        simple_task_instance.queue = "test-queue"
        log = local_k8s_exec.get_task_log(ti=simple_task_instance, log="")
        k8s_executor_mock.get_task_log.assert_not_called()
        assert log is None

    def test_send_callback(self):
        local_executor_mock = mock.MagicMock()
        k8s_executor_mock = mock.MagicMock()
        local_k8s_exec = LocalKubernetesExecutor(local_executor_mock, k8s_executor_mock)
        local_k8s_exec.callback_sink = mock.MagicMock()

        callback = CallbackRequest(full_filepath="fake")
        local_k8s_exec.send_callback(callback)

        local_k8s_exec.callback_sink.send.assert_called_once_with(callback)
