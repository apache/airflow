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
from unittest import mock

from airflow.executors.celery_kubernetes_executor import CeleryKubernetesExecutor


class TestCeleryKubernetesExecutor:
    def test_queued_tasks(self):
        celery_executor_mock = mock.MagicMock()
        k8s_executor_mock = mock.MagicMock()
        cke = CeleryKubernetesExecutor(celery_executor_mock, k8s_executor_mock)

        celery_queued_tasks = {('dag_id', 'task_id', '2020-08-30', 1): 'queued_command'}
        k8s_queued_tasks = {('dag_id_2', 'task_id_2', '2020-08-30', 2): 'queued_command'}

        celery_executor_mock.queued_tasks = celery_queued_tasks
        k8s_executor_mock.queued_tasks = k8s_queued_tasks

        expected_queued_tasks = {**celery_queued_tasks, **k8s_queued_tasks}
        assert cke.queued_tasks == expected_queued_tasks

    def test_running(self):
        celery_executor_mock = mock.MagicMock()
        k8s_executor_mock = mock.MagicMock()
        cke = CeleryKubernetesExecutor(celery_executor_mock, k8s_executor_mock)

        celery_running_tasks = {('dag_id', 'task_id', '2020-08-30', 1)}
        k8s_running_tasks = {('dag_id_2', 'task_id_2', '2020-08-30', 2)}

        celery_executor_mock.running = celery_running_tasks
        k8s_executor_mock.running = k8s_running_tasks

        assert cke.running == celery_running_tasks.union(k8s_running_tasks)

    def test_start(self):
        celery_executor_mock = mock.MagicMock()
        k8s_executor_mock = mock.MagicMock()
        cke = CeleryKubernetesExecutor(celery_executor_mock, k8s_executor_mock)

        cke.start()

        celery_executor_mock.start.assert_called()
        k8s_executor_mock.start.assert_called()

    def test_queue_command(self):
        command = ['airflow', 'run', 'dag']
        priority = 1
        queue = 'default'

        def when_using_k8s_executor():
            celery_executor_mock = mock.MagicMock()
            k8s_executor_mock = mock.MagicMock()
            cke = CeleryKubernetesExecutor(celery_executor_mock, k8s_executor_mock)

            simple_task_instance = mock.MagicMock()
            simple_task_instance.queue = CeleryKubernetesExecutor.KUBERNETES_QUEUE

            cke.queue_command(simple_task_instance, command, priority, queue)

            k8s_executor_mock.queue_command.assert_called_once_with(
                simple_task_instance, command, priority, queue)
            celery_executor_mock.queue_command.assert_not_called()

        def when_using_celery_executor():
            celery_executor_mock = mock.MagicMock()
            k8s_executor_mock = mock.MagicMock()
            cke = CeleryKubernetesExecutor(celery_executor_mock, k8s_executor_mock)

            simple_task_instance = mock.MagicMock()
            simple_task_instance.queue = 'non-kubernetes-queue'

            cke.queue_command(simple_task_instance, command, priority, queue)

            celery_executor_mock.queue_command.assert_called_once_with(
                simple_task_instance, command, priority, queue)
            k8s_executor_mock.queue_command.assert_not_called()

        when_using_k8s_executor()
        when_using_celery_executor()

    def test_queue_task_instance(self):
        mark_success = False
        pickle_id = None
        ignore_all_deps = False
        ignore_depends_on_past = False
        ignore_task_deps = False
        ignore_ti_state = False
        pool = None
        cfg_path = None

        def when_using_k8s_executor():
            celery_executor_mock = mock.MagicMock()
            k8s_executor_mock = mock.MagicMock()
            cke = CeleryKubernetesExecutor(celery_executor_mock, k8s_executor_mock)

            ti = mock.MagicMock()
            ti.queue = CeleryKubernetesExecutor.KUBERNETES_QUEUE

            cke.queue_task_instance(
                ti,
                mark_success,
                pickle_id,
                ignore_all_deps,
                ignore_depends_on_past,
                ignore_task_deps,
                ignore_ti_state,
                pool,
                cfg_path
            )

            k8s_executor_mock.queue_task_instance.assert_called_once_with(
                ti,
                mark_success,
                pickle_id,
                ignore_all_deps,
                ignore_depends_on_past,
                ignore_task_deps,
                ignore_ti_state,
                pool,
                cfg_path
            )
            celery_executor_mock.queue_task_instance.assert_not_called()

        def when_using_celery_executor():
            celery_executor_mock = mock.MagicMock()
            k8s_executor_mock = mock.MagicMock()
            cke = CeleryKubernetesExecutor(celery_executor_mock, k8s_executor_mock)

            ti = mock.MagicMock()
            ti.queue = 'non-kubernetes-queue'

            cke.queue_task_instance(
                ti,
                mark_success,
                pickle_id,
                ignore_all_deps,
                ignore_depends_on_past,
                ignore_task_deps,
                ignore_ti_state,
                pool,
                cfg_path
            )

            k8s_executor_mock.queue_task_instance.assert_not_called()
            celery_executor_mock.queue_task_instance.assert_called_once_with(
                ti,
                mark_success,
                pickle_id,
                ignore_all_deps,
                ignore_depends_on_past,
                ignore_task_deps,
                ignore_ti_state,
                pool,
                cfg_path
            )

        when_using_k8s_executor()
        when_using_celery_executor()

    def test_has_tasks(self):
        ti = mock.MagicMock

        def when_ti_in_k8s_executor():
            celery_executor_mock = mock.MagicMock()
            k8s_executor_mock = mock.MagicMock()
            cke = CeleryKubernetesExecutor(celery_executor_mock, k8s_executor_mock)

            celery_executor_mock.has_task.return_value = False
            k8s_executor_mock.has_task.return_value = True

            assert cke.has_task(ti)
            celery_executor_mock.has_task.assert_called_once_with(ti)
            k8s_executor_mock.has_task.assert_called_once_with(ti)

        def when_ti_in_celery_executor():
            celery_executor_mock = mock.MagicMock()
            k8s_executor_mock = mock.MagicMock()
            cke = CeleryKubernetesExecutor(celery_executor_mock, k8s_executor_mock)

            celery_executor_mock.has_task.return_value = True

            assert cke.has_task(ti)
            celery_executor_mock.has_task.assert_called_once_with(ti)

        when_ti_in_k8s_executor()
        when_ti_in_celery_executor()

    def test_adopt_tasks(self):
        ti = mock.MagicMock

        def when_ti_in_k8s_executor():
            celery_executor_mock = mock.MagicMock()
            k8s_executor_mock = mock.MagicMock()
            ti.queue = "kubernetes"
            cke = CeleryKubernetesExecutor(celery_executor_mock, k8s_executor_mock)

            celery_executor_mock.try_adopt_task_instances.return_value = []
            k8s_executor_mock.try_adopt_task_instances.return_value = []

            cke.try_adopt_task_instances([ti])
            celery_executor_mock.try_adopt_task_instances.assert_called_once_with([])
            k8s_executor_mock.try_adopt_task_instances.assert_called_once_with([ti])

        def when_ti_in_celery_executor():
            celery_executor_mock = mock.MagicMock()
            k8s_executor_mock = mock.MagicMock()
            ti.queue = "default"
            cke = CeleryKubernetesExecutor(celery_executor_mock, k8s_executor_mock)

            celery_executor_mock.try_adopt_task_instances.return_value = []
            k8s_executor_mock.try_adopt_task_instances.return_value = []

            cke.try_adopt_task_instances([ti])
            celery_executor_mock.try_adopt_task_instances.assert_called_once_with([ti])
            k8s_executor_mock.try_adopt_task_instances.assert_called_once_with([])

        when_ti_in_k8s_executor()
        when_ti_in_celery_executor()

    def test_get_event_buffer(self):
        celery_executor_mock = mock.MagicMock()
        k8s_executor_mock = mock.MagicMock()
        cke = CeleryKubernetesExecutor(celery_executor_mock, k8s_executor_mock)

        dag_ids = ['dag_ids']

        events_in_celery = {('dag_id', 'task_id', '2020-08-30', 1): ('failed', 'failed task')}
        events_in_k8s = {('dag_id_2', 'task_id_2', '2020-08-30', 1): ('success', None)}

        celery_executor_mock.get_event_buffer.return_value = events_in_celery
        k8s_executor_mock.get_event_buffer.return_value = events_in_k8s

        events = cke.get_event_buffer(dag_ids)

        assert events == {**events_in_celery, **events_in_k8s}

        celery_executor_mock.get_event_buffer.assert_called_once_with(dag_ids)
        k8s_executor_mock.get_event_buffer.assert_called_once_with(dag_ids)

    def test_end(self):
        celery_executor_mock = mock.MagicMock()
        k8s_executor_mock = mock.MagicMock()
        cke = CeleryKubernetesExecutor(celery_executor_mock, k8s_executor_mock)

        cke.end()

        celery_executor_mock.end.assert_called_once()
        k8s_executor_mock.end.assert_called_once()

    def test_terminate(self):
        celery_executor_mock = mock.MagicMock()
        k8s_executor_mock = mock.MagicMock()
        cke = CeleryKubernetesExecutor(celery_executor_mock, k8s_executor_mock)

        cke.terminate()

        celery_executor_mock.terminate.assert_called_once()
        k8s_executor_mock.terminate.assert_called_once()
