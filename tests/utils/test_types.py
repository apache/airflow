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

from airflow import settings
from airflow.models import DAG
from airflow.models.dagrun import DagRun
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from tests.models import DEFAULT_DATE


def test_runtype_enum_escape():
    session = settings.Session()

    dag = DAG(dag_id='test_enum_dags', start_date=DEFAULT_DATE)
    dag.create_dagrun(
        run_type=DagRunType.SCHEDULED,
        state=State.RUNNING,
        execution_date=DEFAULT_DATE,
        start_date=DEFAULT_DATE,
        session=session,
    )

    rows = (
        session.query(
            DagRun.dag_id,
            DagRun.state,
            DagRun.run_type,
        )
        .filter(
            DagRun.dag_id == dag.dag_id,
            # make sure enum value can be used in filter queries
            DagRun.run_type == DagRunType.SCHEDULED,
        )
        .all()
    )
    assert len(rows) == 1
    assert rows[0].dag_id == dag.dag_id
    assert rows[0].state == State.RUNNING
    # make sure value in db is stored as `scheduled`, not `DagRunType.SCHEDULED`
    assert rows[0].run_type == 'scheduled'

    session.rollback()
    session.close()
