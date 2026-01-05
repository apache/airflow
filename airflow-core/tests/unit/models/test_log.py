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
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from airflow.models.log import Log
from airflow.operators.empty import EmptyOperator
from airflow.utils.state import TaskInstanceState

pytestmark = pytest.mark.db_test


class TestLogTaskInstanceReproduction:
    def test_log_task_instance_join_correctness(self, dag_maker, session):
        # Create dag_1 with a task
        with dag_maker("dag_1", session=session):
            EmptyOperator(task_id="common_task_id")

        dr1 = dag_maker.create_dagrun()
        ti1 = dr1.get_task_instance("common_task_id")
        ti1.state = TaskInstanceState.SUCCESS
        session.merge(ti1)
        session.commit()

        # Create dag_2 with the SAME task_id
        with dag_maker("dag_2", session=session):
            EmptyOperator(task_id="common_task_id")

        dr2 = dag_maker.create_dagrun()
        ti2 = dr2.get_task_instance("common_task_id")
        ti2.state = TaskInstanceState.FAILED
        session.merge(ti2)
        session.commit()

        # Create a log entry specifically for dag_1's task instance
        log = Log(
            event="test_event",
            task_instance=ti1,
        )
        session.add(log)
        session.commit()

        # Query with joinedload to trigger the relationship join

        stmt = select(Log).where(Log.id == log.id).options(joinedload(Log.task_instance))
        loaded_log = session.scalar(stmt)

        assert loaded_log.task_instance is not None
        assert loaded_log.task_instance.dag_id == "dag_1"
        assert loaded_log.task_instance.run_id == ti1.run_id

        # Verify incorrect join for second dag
        log2 = Log(
            event="test_event_2",
            task_instance=ti2,
        )
        session.add(log2)
        session.commit()

        stmt2 = select(Log).where(Log.id == log2.id).options(joinedload(Log.task_instance))
        loaded_log2 = session.scalar(stmt2)

        # This should fail if the join is ambiguous and picks the first one (dag_1)
        assert loaded_log2.task_instance is not None
        assert loaded_log2.task_instance.dag_id == "dag_2"
        assert loaded_log2.task_instance.run_id == ti2.run_id
