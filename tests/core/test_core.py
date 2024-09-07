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

from datetime import timedelta
from time import sleep

import pytest

from airflow.exceptions import AirflowTaskTimeout
from airflow.models import TaskInstance
from airflow.models.baseoperator import BaseOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.timezone import datetime
from airflow.utils.types import DagRunType

from tests_common.test_utils.db import clear_db_dags, clear_db_runs

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]

DEFAULT_DATE = datetime(2015, 1, 1)


class TestCore:
    @staticmethod
    def clean_db():
        clear_db_dags()
        clear_db_runs()

    def teardown_method(self):
        self.clean_db()

    def test_dryrun(self, dag_maker):
        with dag_maker():
            op = BashOperator(task_id="test_dryrun", bash_command="echo success")
        dag_maker.create_dagrun()
        op.dry_run()

    def test_dryrun_with_invalid_template_field(self, dag_maker):
        class InvalidTemplateFieldOperator(BaseOperator):
            template_fields = ["missing_field"]

        with dag_maker():
            op = InvalidTemplateFieldOperator(task_id="test_dryrun_invalid_template_field")
        dag_maker.create_dagrun()

        error_message = (
            "'missing_field' is configured as a template field but InvalidTemplateFieldOperator does not "
            "have this attribute."
        )
        with pytest.raises(AttributeError, match=error_message):
            op.dry_run()

    def test_timeout(self, dag_maker):
        def sleep_and_catch_other_exceptions():
            try:
                sleep(5)
                # Catching Exception should NOT catch AirflowTaskTimeout
            except Exception:
                pass

        with dag_maker(serialized=True):
            op = PythonOperator(
                task_id="test_timeout",
                execution_timeout=timedelta(seconds=1),
                python_callable=sleep_and_catch_other_exceptions,
            )
        dag_maker.create_dagrun()
        with pytest.raises(AirflowTaskTimeout):
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_externally_triggered_dagrun(self, dag_maker):
        TI = TaskInstance

        # Create the dagrun between two "scheduled" execution dates of the DAG
        execution_date = DEFAULT_DATE + timedelta(days=2)
        execution_ds = execution_date.strftime("%Y-%m-%d")
        execution_ds_nodash = execution_ds.replace("-", "")

        with dag_maker(schedule=timedelta(weeks=1), serialized=True):
            task = EmptyOperator(task_id="test_externally_triggered_dag_context")
        dr = dag_maker.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=execution_date,
            external_trigger=True,
        )
        task.run(start_date=execution_date, end_date=execution_date)

        ti = TI(task=task, run_id=dr.run_id)
        ti.refresh_from_db()
        context = ti.get_template_context()

        # next_ds should be the execution date for manually triggered runs
        with pytest.deprecated_call():
            assert context["next_ds"] == execution_ds
        with pytest.deprecated_call():
            assert context["next_ds_nodash"] == execution_ds_nodash

    def test_dag_params_and_task_params(self, dag_maker):
        # This test case guards how params of DAG and Operator work together.
        # - If any key exists in either DAG's or Operator's params,
        #   it is guaranteed to be available eventually.
        # - If any key exists in both DAG's params and Operator's params,
        #   the latter has precedence.
        TI = TaskInstance

        with dag_maker(
            schedule=timedelta(weeks=1),
            params={"key_1": "value_1", "key_2": "value_2_old"},
            serialized=True,
        ):
            task1 = EmptyOperator(
                task_id="task1",
                params={"key_2": "value_2_new", "key_3": "value_3"},
            )
            task2 = EmptyOperator(task_id="task2")
        dr = dag_maker.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            external_trigger=True,
        )
        task1.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        task2.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        ti1 = TI(task=task1, run_id=dr.run_id)
        ti2 = TI(task=task2, run_id=dr.run_id)
        ti1.refresh_from_db()
        ti2.refresh_from_db()
        context1 = ti1.get_template_context()
        context2 = ti2.get_template_context()

        assert context1["params"] == {"key_1": "value_1", "key_2": "value_2_new", "key_3": "value_3"}
        assert context2["params"] == {"key_1": "value_1", "key_2": "value_2_old"}
