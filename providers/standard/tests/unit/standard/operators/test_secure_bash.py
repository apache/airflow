# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-8.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import pytest
from airflow.providers.standard.operators.secure_bash import SecureBashOperator, lift_untrusted_expressions


class TestSecureBashOperator:
    def test_lift_untrusted_dag_run_conf(self):
        template = 'echo "Hello {{ dag_run.conf["user"] }}"'
        modified, bindings = lift_untrusted_expressions(template, "task_id")
        
        assert modified == 'echo "Hello ${_AIRFLOW_LIFTED_task_id_0}"'
        assert bindings == {"_AIRFLOW_LIFTED_task_id_0": '{{ dag_run.conf["user"] }}'}

    def test_lift_untrusted_params(self):
        template = 'cat {{ params.filename }}'
        modified, bindings = lift_untrusted_expressions(template, "task_id")
        
        assert modified == 'cat ${_AIRFLOW_LIFTED_task_id_0}'
        assert bindings == {"_AIRFLOW_LIFTED_task_id_0": '{{ params.filename }}'}

    def test_safe_expressions_untouched(self):
        template = 'echo "Date: {{ ds }} User: {{ dag_run.conf["user"] }}"'
        modified, bindings = lift_untrusted_expressions(template, "task_id")
        
        assert "{{ ds }}" in modified
        assert "dag_run.conf" not in modified
        assert len(bindings) == 1

    def test_no_untrusted_vars_fast_path(self):
        template = 'echo "Today is {{ ds }}"'
        modified, bindings = lift_untrusted_expressions(template, "task_id")
        
        assert modified == template
        assert not bindings
