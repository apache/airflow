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

from airflow.api_fastapi.common.db.task_instance import get_task_instance_or_history_for_try_number
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskinstancehistory import TaskInstanceHistory
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils import timezone
from airflow.utils.types import DagRunType

from tests_common.test_utils.db import clear_db_runs

pytestmark = pytest.mark.db_test


class TestDBTaskInstance:
    DAG_ID = "dag_for_testing_db_task_instance"
    RUN_ID = "dag_run_id_for_testing_db_task_instance"
    TASK_ID = "task_for_testing_db_task_instance"
    TRY_NUMBER = 1

    default_time = "2020-06-10T20:00:00+00:00"

    @pytest.fixture(autouse=True)
    def setup_attrs(self, dag_maker, session) -> None:
        with dag_maker(self.DAG_ID, start_date=timezone.parse(self.default_time), session=session) as dag:
            EmptyOperator(task_id=self.TASK_ID)

        dr = dag_maker.create_dagrun(
            run_id=self.RUN_ID,
            run_type=DagRunType.SCHEDULED,
            logical_date=timezone.parse(self.default_time),
            start_date=timezone.parse(self.default_time),
        )

        for ti in dr.task_instances:
            ti.try_number = 1
            ti.hostname = "localhost"
            session.merge(ti)
        dag.clear()
        for ti in dr.task_instances:
            ti.try_number = 2
            ti.hostname = "localhost"
            session.merge(ti)
        session.commit()

    def teardown_method(self):
        clear_db_runs()

    @pytest.mark.parametrize("try_number", [1, 2])
    def test_get_task_instance_or_history_for_try_number(self, try_number, session):
        ti = get_task_instance_or_history_for_try_number(
            self.DAG_ID,
            self.RUN_ID,
            self.TASK_ID,
            try_number,
            session=session,
            map_index=-1,
        )

        assert isinstance(ti, TaskInstanceHistory) if try_number == 1 else TaskInstance
