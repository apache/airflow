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

import sys
import tempfile
from base64 import b64decode
from unittest.mock import patch

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2021, 9, 1)

DOCKER_IMAGE = "quay.io/bitnami/python:3.9"


def docker_execute(self, context):
    code = b64decode(self.environment['__PYTHON_SCRIPT'])
    args = b64decode(self.environment['__PYTHON_INPUT'])
    with tempfile.NamedTemporaryFile() as f:
        with tempfile.NamedTemporaryFile() as g:
            g.write(args)
            g.flush()
            with patch.object(sys, 'argv', [None, g.name, f.name]):
                exec(code)
        f.seek(0)
        try:
            return self.pickling_library.load(f)
        except EOFError:
            return


@patch.object(DockerOperator, "execute", new=docker_execute)
class TestDockerDecorator:
    def test_basic_docker_operator(self, dag_maker):
        @task.docker(image=DOCKER_IMAGE)
        def f():
            import random

            return [random.random() for _ in range(100)]

        with dag_maker():
            ret = f()

        dr = dag_maker.create_dagrun()
        ret.operator.run(start_date=dr.execution_date, end_date=dr.execution_date)
        ti = dr.get_task_instances()[0]
        assert len(ti.xcom_pull()) == 100

    def test_basic_docker_operator_with_param(self, dag_maker):
        @task.docker(image=DOCKER_IMAGE)
        def f(num_results):
            import random

            return [random.random() for _ in range(num_results)]

        with dag_maker():
            ret = f(50)

        dr = dag_maker.create_dagrun()
        ret.operator.run(start_date=dr.execution_date, end_date=dr.execution_date)
        ti = dr.get_task_instances()[0]
        result = ti.xcom_pull()
        assert isinstance(result, list)
        assert len(result) == 50

    def test_basic_docker_operator_multiple_output(self, dag_maker):
        @task.docker(
            image=DOCKER_IMAGE,
            multiple_outputs=True,
        )
        def return_dict(number: int):
            return {"number": number + 1, "43": 43}

        test_number = 10
        with dag_maker():
            ret = return_dict(test_number)

        dr = dag_maker.create_dagrun()
        ret.operator.run(start_date=dr.execution_date, end_date=dr.execution_date)

        ti = dr.get_task_instances()[0]
        assert ti.xcom_pull(key="number") == test_number + 1
        assert ti.xcom_pull(key="43") == 43
        assert ti.xcom_pull() == {"number": test_number + 1, "43": 43}

    def test_no_return(self, dag_maker):
        @task.docker(image=DOCKER_IMAGE)
        def f():
            pass

        with dag_maker():
            ret = f()

        dr = dag_maker.create_dagrun()
        ret.operator.run(start_date=dr.execution_date, end_date=dr.execution_date)
        ti = dr.get_task_instances()[0]
        assert ti.xcom_pull() is None

    def test_call_decorated_multiple_times(self):
        """Test calling decorated function 21 times in a DAG"""

        @task.docker(
            image=DOCKER_IMAGE,
            network_mode="bridge",
            api_version="auto",
        )
        def do_run():
            return 4

        with DAG("test", start_date=DEFAULT_DATE) as dag:
            do_run()
            for _ in range(20):
                do_run()

        assert len(dag.task_ids) == 21
        assert dag.task_ids[-1] == 'do_run__20'
