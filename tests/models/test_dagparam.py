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
from airflow.decorators import task
from airflow.utils import timezone
from airflow.utils.types import DagRunType
from tests.test_utils.db import clear_db_dags, clear_db_runs


class TestDagParamRuntime:
    VALUE = 42
    DEFAULT_DATE = timezone.datetime(2016, 1, 1)

    @staticmethod
    def clean_db():
        clear_db_runs()
        clear_db_dags()

    def setup_method(self):
        self.clean_db()

    def teardown_method(self):
        self.clean_db()

    def test_dag_param_resolves(self, dag_maker):
        """Test dagparam resolves on operator execution"""
        with dag_maker(dag_id="test_xcom_pass_to_op") as dag:
            value = dag.param('value', default=self.VALUE)

            @task
            def return_num(num):
                return num

            xcom_arg = return_num(value)

        dr = dag_maker.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
        )

        xcom_arg.operator.run(dr.execution_date, dr.execution_date)

        ti = dr.get_task_instances()[0]
        assert ti.xcom_pull() == self.VALUE

    def test_dag_param_overwrite(self, dag_maker):
        """Test dag param is overwritten from dagrun config"""
        with dag_maker(dag_id="test_xcom_pass_to_op") as dag:
            value = dag.param('value', default=self.VALUE)

            @task
            def return_num(num):
                return num

            xcom_arg = return_num(value)

        assert dag.params['value'] == self.VALUE
        new_value = 2
        dr = dag_maker.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            conf={'value': new_value},
        )

        xcom_arg.operator.run(dr.execution_date, dr.execution_date)

        ti = dr.get_task_instances()[0]
        assert ti.xcom_pull() == new_value

    def test_dag_param_default(self, dag_maker):
        """Test dag param is overwritten from dagrun config"""
        with dag_maker(dag_id="test_xcom_pass_to_op", params={'value': 'test'}) as dag:
            value = dag.param('value')

            @task
            def return_num(num):
                return num

            xcom_arg = return_num(value)

        dr = dag_maker.create_dagrun(run_id=DagRunType.MANUAL.value, start_date=timezone.utcnow())

        xcom_arg.operator.run(dr.execution_date, dr.execution_date)

        ti = dr.get_task_instances()[0]
        assert ti.xcom_pull() == 'test'
