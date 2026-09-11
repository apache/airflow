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

from typing import TYPE_CHECKING

import pytest
from sqlalchemy import func, select

from airflow.api.common.delete_dag import delete_dag
from airflow.models import DagModel
from airflow.providers.standard.operators.empty import EmptyOperator

if TYPE_CHECKING:
    from airflow.serialization.definitions.dag import SerializedDAG

    from tests_common.pytest_plugin import DagMaker

pytestmark = [pytest.mark.db_test, pytest.mark.need_serialized_dag]

DAG_ID = "dag_to_delete"


def test_delete_dag_does_not_read_back_deleted_row_keys(dag_maker: DagMaker[SerializedDAG], session):
    """
    delete_dag must not ask the database for the keys of the rows it deletes.

    Forcing SQLAlchemy's "fetch" synchronization strategy reads every deleted primary key
    back, which costs memory proportional to the Dag's history. Backends with RETURNING
    stream those keys back on the DELETE itself, and those without it (MySQL) run a
    second full SELECT beforehand, so both shapes are asserted against here.
    """
    from sqlalchemy import event

    import airflow.settings

    with dag_maker(DAG_ID, session=session):
        EmptyOperator(task_id="task")
    dag_maker.create_dagrun()
    session.commit()

    executed_statements: list[str] = []

    def capture(_conn, _cursor, statement, _parameters, _context, _executemany):
        executed_statements.append(" ".join(statement.split()).upper())

    event.listen(airflow.settings.engine, "before_cursor_execute", capture)
    try:
        delete_dag(DAG_ID, keep_records_in_log=False, session=session)
        session.commit()
    finally:
        event.remove(airflow.settings.engine, "before_cursor_execute", capture)

    deletes = [s for s in executed_statements if s.startswith("DELETE")]
    assert deletes, "Expected delete_dag to issue DELETE statements"
    assert [s for s in deletes if "RETURNING" in s] == [], "DELETEs must not read back deleted keys"

    after_first_delete = executed_statements[executed_statements.index(deletes[0]) :]
    assert [s for s in after_first_delete if s.startswith("SELECT")] == [], (
        "No SELECT may precede a DELETE to collect the keys it is about to remove"
    )

    assert session.scalar(select(func.count()).select_from(DagModel).where(DagModel.dag_id == DAG_ID)) == 0
