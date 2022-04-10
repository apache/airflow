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

from airflow.decorators import task
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2021, 9, 1)


class TestKubernetesDecorator:
    @mock.patch("airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator.execute")
    def test_task_creation_with_params(self, execute_mock, dag_maker):
        @task.kubernetes(image='python:3.8-slim-buster', name='k8s_test', namespace='default')
        def k8s_decorator_func():
            print("decorator func")

        with dag_maker():
            ret = k8s_decorator_func()
            ret

        dr = dag_maker.create_dagrun()
        ret.operator.run(start_date=dr.execution_date, end_date=dr.execution_date)

        ti = dr.get_task_instances()[0]
        assert ti.task_id == 'k8s_decorator_func'
        assert ti.state == 'success'

    @mock.patch("airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator.execute")
    def test_task_creation_default_params(self, execute_mock, dag_maker):
        @task.kubernetes(image='python:3.8-slim-buster')
        def k8s_decorator_func():
            print("decorator func")

        with dag_maker():
            ret = k8s_decorator_func()
            ret

        dr = dag_maker.create_dagrun()
        ret.operator.run(start_date=dr.execution_date, end_date=dr.execution_date)

        ti = dr.get_task_instances()[0]
        assert ti.task_id == 'k8s_decorator_func'
        assert ti.state == 'success'

        # Default pod parameters
        assert ret.operator.cmds[0] == 'bash'
        assert ret.operator.arguments[0] == '-cx'
        assert ret.operator.env_vars[0].name == '__PYTHON_SCRIPT'
