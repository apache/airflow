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

import pytest

from airflow.decorators import task_group
from airflow.models.taskinstance import TaskInstance as TI
from airflow.operators.empty import EmptyOperator
from airflow.providers.openlineage.plugins.facets import AirflowMappedTaskRunFacet
from airflow.providers.openlineage.utils.utils import get_custom_facets
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2016, 1, 1)


@pytest.mark.db_test
def test_get_custom_facets(dag_maker):
    with dag_maker(dag_id="dag_test_get_custom_facets") as dag:

        @task_group
        def task_group_op(k):
            EmptyOperator(task_id="empty_operator")

        task_group_op.expand(k=[0])

        dag_maker.create_dagrun()
        ti_0 = TI(dag.get_task("task_group_op.empty_operator"), execution_date=DEFAULT_DATE, map_index=0)

        assert ti_0.map_index == 0

        assert get_custom_facets(ti_0)["airflow_mappedTask"] == AirflowMappedTaskRunFacet(
            mapIndex=0,
            operatorClass=f"{ti_0.task.operator_class.__module__}.{ti_0.task.operator_class.__name__}",
        )
