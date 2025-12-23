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
from sqlalchemy import select

from airflow.models.dagrun import DagRun
from airflow.utils.state import State
from airflow.utils.types import DagRunType

pytestmark = pytest.mark.db_test


def test_runtype_enum_escape(dag_maker, session):
    """
    Make sure DagRunType.SCHEDULE is converted to string 'scheduled' when
    referenced in DB query
    """
    with dag_maker(dag_id="test_enum_dags", schedule=timedelta(days=1), session=session):
        pass
    dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)

    query = session.scalars(
        select(
            DagRun.dag_id,
            DagRun.state,
            DagRun.run_type,
        ).where(
            DagRun.dag_id == "test_enum_dags",
            # make sure enum value can be used in filter queries
            DagRun.run_type == DagRunType.SCHEDULED,
        )
    )
    assert str(query.statement.compile(compile_kwargs={"literal_binds": True})) == (
        "SELECT dag_run.dag_id, dag_run.state, dag_run.run_type \n"
        "FROM dag_run \n"
        "WHERE dag_run.dag_id = 'test_enum_dags' AND dag_run.run_type = 'scheduled'"
    )

    rows = query.all()
    assert len(rows) == 1
    assert rows[0].dag_id == "test_enum_dags"
    assert rows[0].state == State.RUNNING
    # make sure value in db is stored as `scheduled`, not `DagRunType.SCHEDULED`
    assert rows[0].run_type == "scheduled"

    session.rollback()
