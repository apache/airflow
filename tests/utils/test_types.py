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

from datetime import timedelta

import pytest

from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType

from tests.models import DEFAULT_DATE
from tests_common.test_utils.compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.utils.types import DagRunTriggeredByType

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]


def test_runtype_enum_escape():
    """
    Make sure DagRunType.SCHEDULE is converted to string 'scheduled' when
    referenced in DB query
    """
    with create_session() as session:
        dag = DAG(
            dag_id="test_enum_dags", schedule=timedelta(days=1), start_date=DEFAULT_DATE
        )
        data_interval = dag.timetable.infer_manual_data_interval(run_after=DEFAULT_DATE)
        triggered_by_kwargs = (
            {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        )
        dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            state=State.RUNNING,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            session=session,
            data_interval=data_interval,
            **triggered_by_kwargs,
        )

        query = session.query(
            DagRun.dag_id,
            DagRun.state,
            DagRun.run_type,
        ).filter(
            DagRun.dag_id == dag.dag_id,
            # make sure enum value can be used in filter queries
            DagRun.run_type == DagRunType.SCHEDULED,
        )
        assert str(query.statement.compile(compile_kwargs={"literal_binds": True})) == (
            "SELECT dag_run.dag_id, dag_run.state, dag_run.run_type \n"
            "FROM dag_run \n"
            "WHERE dag_run.dag_id = 'test_enum_dags' AND dag_run.run_type = 'scheduled'"
        )

        rows = query.all()
        assert len(rows) == 1
        assert rows[0].dag_id == dag.dag_id
        assert rows[0].state == State.RUNNING
        # make sure value in db is stored as `scheduled`, not `DagRunType.SCHEDULED`
        assert rows[0].run_type == "scheduled"

        session.rollback()
