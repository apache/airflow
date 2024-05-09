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

from unittest.mock import MagicMock

from airflow.operators.bash import BashOperator
from airflow.providers.openlineage.utils.utils import get_fully_qualified_class_name, get_job_name


def test_get_job_name_correct_format():
    task_instance = MagicMock(dag_id="example_dag", task_id="example_task")
    expected_result = "example_dag.example_task"
    assert get_job_name(task_instance) == expected_result


def test_get_job_name_with_empty_ids():
    task_instance = MagicMock(dag_id="", task_id="")
    expected_result = "."
    assert get_job_name(task_instance) == expected_result


def test_get_fully_qualified_class_name_bash_operator():
    result = get_fully_qualified_class_name(BashOperator(task_id="test", bash_command="echo 0;"))
    expected_result = "airflow.operators.bash.BashOperator"
    assert result == expected_result
