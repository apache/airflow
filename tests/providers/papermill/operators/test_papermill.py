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

import unittest
from unittest.mock import patch

from airflow.models import DAG, DagRun, TaskInstance
from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2021, 1, 1)


class TestPapermillOperator(unittest.TestCase):
    @patch('airflow.providers.papermill.operators.papermill.pm')
    def test_execute(self, mock_papermill):
        in_nb = "/tmp/does_not_exist"
        out_nb = "/tmp/will_not_exist"
        kernel_name = "python3"
        language_name = "python"
        parameters = {"msg": "hello_world", "train": 1}

        op = PapermillOperator(
            input_nb=in_nb,
            output_nb=out_nb,
            parameters=parameters,
            task_id="papermill_operator_test",
            kernel_name=kernel_name,
            language_name=language_name,
            dag=None,
        )

        op.execute(context={})

        mock_papermill.execute_notebook.assert_called_once_with(
            in_nb,
            out_nb,
            parameters=parameters,
            kernel_name=kernel_name,
            language=language_name,
            progress_bar=False,
            report_mode=True,
        )

    def test_render_template(self):
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        dag = DAG('test_render_template', default_args=args)

        operator = PapermillOperator(
            task_id="render_dag_test",
            input_nb="/tmp/{{ dag.dag_id }}.ipynb",
            output_nb="/tmp/out-{{ dag.dag_id }}.ipynb",
            parameters={"msgs": "dag id is {{ dag.dag_id }}!"},
            kernel_name="python3",
            language_name="python",
            dag=dag,
        )

        ti = TaskInstance(operator, run_id="papermill_test")
        ti.dag_run = DagRun(execution_date=DEFAULT_DATE)
        ti.render_templates()

        assert "/tmp/test_render_template.ipynb" == operator.input_nb
        assert '/tmp/out-test_render_template.ipynb' == operator.output_nb
        assert {"msgs": "dag id is test_render_template!"} == operator.parameters
        assert "python3" == operator.kernel_name
        assert "python" == operator.language_name
