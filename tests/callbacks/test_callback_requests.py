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
from datetime import datetime

from parameterized import parameterized

from airflow.callbacks.callback_requests import (
    CallbackRequest,
    DagCallbackRequest,
    SlaCallbackRequest,
    TaskCallbackRequest,
)
from airflow.models.dag import DAG
from airflow.models.taskinstance import SimpleTaskInstance, TaskInstance
from airflow.operators.bash import BashOperator
from airflow.utils.state import State

TI = TaskInstance(
    task=BashOperator(task_id="test", bash_command="true", dag=DAG(dag_id='id'), start_date=datetime.now()),
    run_id="fake_run",
    state=State.RUNNING,
)


class TestCallbackRequest(unittest.TestCase):
    @parameterized.expand(
        [
            (CallbackRequest(full_filepath="filepath", msg="task_failure"), CallbackRequest),
            (
                TaskCallbackRequest(
                    full_filepath="filepath",
                    simple_task_instance=SimpleTaskInstance.from_ti(ti=TI),
                    is_failure_callback=True,
                ),
                TaskCallbackRequest,
            ),
            (
                DagCallbackRequest(
                    full_filepath="filepath",
                    dag_id="fake_dag",
                    run_id="fake_run",
                    is_failure_callback=False,
                ),
                DagCallbackRequest,
            ),
            (SlaCallbackRequest(full_filepath="filepath", dag_id="fake_dag"), SlaCallbackRequest),
        ]
    )
    def test_from_json(self, input, request_class):
        json_str = input.to_json()

        result = getattr(request_class, 'from_json')(json_str=json_str)

        self.assertEqual(result, input)
