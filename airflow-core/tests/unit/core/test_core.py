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

import pytest

from airflow._shared.timezones.timezone import datetime
from airflow.models.baseoperator import BaseOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.types import DagRunType

from tests_common.test_utils.db import clear_db_dags, clear_db_runs

pytestmark = pytest.mark.db_test

DEFAULT_DATE = datetime(2015, 1, 1)


class TestCore:
    @staticmethod
    def clean_db():
        clear_db_dags()
        clear_db_runs()

    def teardown_method(self):
        self.clean_db()

    def test_dryrun(self, dag_maker):
        class TemplateFieldOperator(BaseOperator):
            template_fields = ["bash_command"]

            def __init__(self, bash_command, **kwargs):
                self.bash_command = bash_command
                super().__init__(**kwargs)

        with dag_maker():
            op = TemplateFieldOperator(task_id="test_dryrun", bash_command="echo success")
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

    def test_dag_params_and_task_params(self, dag_maker):
        # This test case guards how params of DAG and Operator work together.
        # - If any key exists in either DAG's or Operator's params,
        #   it is guaranteed to be available eventually.
        # - If any key exists in both DAG's params and Operator's params,
        #   the latter has precedence.

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
        )
        ti1 = dag_maker.run_ti(task1.task_id, dr)
        ti2 = dag_maker.run_ti(task2.task_id, dr)

        context1 = ti1.get_template_context()
        context2 = ti2.get_template_context()

        assert context1["params"] == {"key_1": "value_1", "key_2": "value_2_new", "key_3": "value_3"}
        assert context2["params"] == {"key_1": "value_1", "key_2": "value_2_old"}
