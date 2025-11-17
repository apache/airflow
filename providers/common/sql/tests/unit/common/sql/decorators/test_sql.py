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

import pytest

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_1_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import task
else:
    from airflow.decorators import task


class TestSqlDecorator:

    @pytest.fixture(autouse=True)
    def setup(self, dag_maker):
        self.dag_maker = dag_maker

        with dag_maker(dag_id="sql_deco_dag") as dag:
            ...

        self.dag = dag

    def test_sql_decorator_init(self):
        """Test the initialization of the @task.sql decorator."""
        with self.dag_maker:

            @task.sql
            def sql(): ...

            sql_task = sql()

        assert sql_task.operator.task_id == "sql"
