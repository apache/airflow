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

from pendulum import now

from airflow.decorators import dag, task
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.openlineage.utils.selective_enable import (
    DISABLE_OL_PARAM,
    ENABLE_OL_PARAM,
    ENABLE_OL_PARAM_NAME,
    disable_lineage,
    enable_lineage,
)


class TestOpenLineageSelectiveEnable:
    def setup_method(self):
        @dag(dag_id="test_selective_enable_decorated_dag", schedule=None, start_date=now())
        def decorated_dag():
            @task
            def decorated_task():
                return "test"

            self.decorated_task = decorated_task()

        self.decorated_dag = decorated_dag()

        with DAG(dag_id="test_selective_enable_dag", schedule=None, start_date=now()) as self.dag:
            self.task = EmptyOperator(task_id="test_selective_enable")

    def test_enable_lineage_task_level(self):
        assert ENABLE_OL_PARAM_NAME not in self.task.params
        enable_lineage(self.task)
        assert ENABLE_OL_PARAM.value == self.task.params[ENABLE_OL_PARAM_NAME]

    def test_disable_lineage_task_level(self):
        assert ENABLE_OL_PARAM_NAME not in self.task.params
        disable_lineage(self.task)
        assert DISABLE_OL_PARAM.value == self.task.params[ENABLE_OL_PARAM_NAME]

    def test_enable_lineage_dag_level(self):
        assert ENABLE_OL_PARAM_NAME not in self.dag.params
        enable_lineage(self.dag)
        assert ENABLE_OL_PARAM.value == self.dag.params[ENABLE_OL_PARAM_NAME]
        # check if param propagates to the task
        assert ENABLE_OL_PARAM.value == self.task.params[ENABLE_OL_PARAM_NAME]

    def test_enable_lineage_decorated_dag(self):
        enable_lineage(self.decorated_dag)
        assert ENABLE_OL_PARAM.value == self.decorated_dag.params[ENABLE_OL_PARAM_NAME]
        # check if param propagates to the task
        assert ENABLE_OL_PARAM.value == self.decorated_task.operator.params[ENABLE_OL_PARAM_NAME]

    def test_enable_lineage_decorated_task(self):
        enable_lineage(self.decorated_task)
        assert ENABLE_OL_PARAM.value == self.decorated_task.operator.params[ENABLE_OL_PARAM_NAME]
