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

from airflow.models import DagModel
from airflow.models.dagbag import DagBag
from airflow.models.serialized_dag import SerializedDagModel
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from tests.test_utils.db import clear_db_runs

pytestmark = pytest.mark.db_test


@pytest.fixture()
def running_subdag(admin_client, dag_maker):
    with dag_maker(dag_id="running_dag.subdag") as subdag:
        EmptyOperator(task_id="empty")

    with pytest.deprecated_call(), dag_maker(dag_id="running_dag") as dag:
        SubDagOperator(task_id="subdag", subdag=subdag)

    dag_bag = DagBag(include_examples=False)
    dag_bag.bag_dag(dag, root_dag=dag)

    with create_session() as session:
        # This writes both DAGs to DagModel, but only serialize the parent DAG.
        dag_bag.sync_to_db(session=session)

        # Simulate triggering the SubDagOperator to run the subdag.
        logical_date = timezone.datetime(2016, 1, 1)
        subdag.create_dagrun(
            run_id="blocked_run_example_bash_operator",
            state=State.RUNNING,
            execution_date=logical_date,
            data_interval=(logical_date, logical_date),
            start_date=timezone.datetime(2016, 1, 1),
            session=session,
        )

        # Now delete the parent DAG but leave the subdag.
        session.query(DagModel).filter(DagModel.dag_id == dag.dag_id).delete()
        session.query(SerializedDagModel).filter(SerializedDagModel.dag_id == dag.dag_id).delete()

    yield subdag

    with create_session() as session:
        session.query(DagModel).filter(DagModel.dag_id == subdag.dag_id).delete()
    clear_db_runs()


def test_blocked_subdag_success(admin_client, running_subdag):
    """Test the /blocked endpoint works when a DAG is deleted.

    When a DAG is bagged, it is written to both DagModel and SerializedDagModel,
    but its subdags are only written to DagModel (without serialization). Thus,
    ``DagBag.get_dag(subdag_id)`` would raise ``SerializedDagNotFound`` if the
    subdag was not previously bagged in the dagbag (perhaps due to its root DAG
    being deleted). ``DagBag.get_dag()`` calls should catch the exception and
    properly handle this situation.
    """
    resp = admin_client.post("/blocked", data={"dag_ids": [running_subdag.dag_id]})
    assert resp.status_code == 200
    assert resp.json == [
        {
            "dag_id": running_subdag.dag_id,
            "active_dag_run": 1,
            "max_active_runs": 0,  # Default value for an unserialized DAG.
        },
    ]
