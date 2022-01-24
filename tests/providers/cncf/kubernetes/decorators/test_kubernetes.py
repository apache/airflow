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
from unittest.mock import MagicMock

import pytest
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2021, 9, 1)


class TestKubernetesDecorator:
    @mock.patch("airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator.execute")
    @mock.patch("airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator.__init__")
    def test_basic_kubernetes_operator(self, execute_mock, init_mock, dag_maker):

        @task
        def dummy_f():
            pass

        @task.kubernetes(task_id="kubernetes_operator")
        def f():
            import random
            return [random.random() for _ in range(100)]

        with dag_maker():
            df = dummy_f()
            ret = f()
            df.set_downstream(ret)

        dr = dag_maker.create_dagrun()
        ret.operator.run(start_date=dr.execution_date, end_date=dr.execution_date)
        ti = dr.get_task_instances()[0]
        assert len(ti.xcom_pull()) == 100
