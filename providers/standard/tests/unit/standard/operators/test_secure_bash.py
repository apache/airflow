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

import logging
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

    def test_lift_xcom_pull(self):
        template = 'echo {{ ti.xcom_pull("upstream") }}'
        modified, bindings = lift_untrusted_expressions(template, "task")
        
        assert modified == 'echo ${_AIRFLOW_LIFTED_task_0}'
        assert bindings == {"_AIRFLOW_LIFTED_task_0": '{{ ti.xcom_pull("upstream") }}'}

    def test_sanitize_task_id_for_env(self):
        template = 'echo {{ params.foo }}'
        # Task ID with hyphens and dots
        modified, bindings = lift_untrusted_expressions(template, "my-task.v1")
        
        assert "_AIRFLOW_LIFTED_my_task_v1_0" in modified
        assert "_AIRFLOW_LIFTED_my_task_v1_0" in bindings

    def test_safe_expressions_untouched(self):
        template = 'echo "Date: {{ ds }} User: {{ dag_run.conf["user"] }}"'
        modified, bindings = lift_untrusted_expressions(template, "task_id")
        
        assert "{{ ds }}" in modified
        assert "dag_run.conf" not in modified
        assert len(bindings) == 1

    def test_single_quote_warning(self, caplog):
        template = "echo '{{ params.foo }}'"
        with caplog.at_level(logging.WARNING):
            modified, bindings = lift_untrusted_expressions(template, "task")
        
        assert "detected inside single quotes" in caplog.text

    def test_eval_warning(self, caplog):
        template = "eval {{ params.foo }}"
        with caplog.at_level(logging.WARNING):
            modified, bindings = lift_untrusted_expressions(template, "task")
        
        assert "'eval' or 'source' detected" in caplog.text

    def test_no_untrusted_vars_fast_path(self):
        template = 'echo "Today is {{ ds }}"'
        modified, bindings = lift_untrusted_expressions(template, "task_id")
        
        assert modified == template
        assert not bindings

    def test_lift_connections_plural(self):
        template = 'cat {{ connections.my_db }}'
        modified, bindings = lift_untrusted_expressions(template, "task_id")
        assert "_AIRFLOW_LIFTED_task_id_0" in modified
        assert "_AIRFLOW_LIFTED_task_id_0" in bindings

    def test_jinja_with_string_literals(self):
        template = 'echo {{ dag_run.conf.get("user", "}}default") }}'
        modified, bindings = lift_untrusted_expressions(template, "task_id")
        assert modified == 'echo ${_AIRFLOW_LIFTED_task_id_0}'
        assert bindings["_AIRFLOW_LIFTED_task_id_0"] == '{{ dag_run.conf.get("user", "}}default") }}'

    def test_secure_bash_operator_does_not_mutate_env(self):
        shared_env = {"EXISTING": "value"}
        operator = SecureBashOperator(
            task_id="test_task",
            bash_command="echo {{ params.foo }}",
            env=shared_env
        )
        operator.render_template_fields(context={}, jinja_env=None)
        assert "EXISTING" in shared_env
        assert len(shared_env) == 1
        assert "EXISTING" in operator.env
        assert "_AIRFLOW_LIFTED_test_task_0" in operator.env
