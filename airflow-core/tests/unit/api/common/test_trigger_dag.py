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

from airflow.api.common.trigger_dag import trigger_dag
from airflow.models import DagModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.types import DagRunTriggeredByType

from tests_common.test_utils.db import (
    clear_db_dag_bundles,
    clear_db_dags,
    clear_db_runs,
    clear_db_serialized_dags,
)

pytestmark = [pytest.mark.db_test, pytest.mark.need_serialized_dag]


@pytest.fixture(autouse=True)
def clean_db():
    clear_db_runs()
    clear_db_dags()
    clear_db_dag_bundles()
    clear_db_serialized_dags()
    yield
    clear_db_runs()
    clear_db_dags()
    clear_db_dag_bundles()
    clear_db_serialized_dags()


def test_trigger_dag_raises_error_if_manual_runs_denied(dag_maker, session):
    with dag_maker(session=session, dag_id="TEST_DAG_1", schedule="0 * * * *"):
        EmptyOperator(task_id="mytask")
    session.commit()
    dag_model = session.scalar(select(DagModel).where(DagModel.dag_id == "TEST_DAG_1"))
    dag_model.allowed_run_types = ["scheduled"]
    session.commit()

    with pytest.raises(ValueError, match="Dag with dag_id: 'TEST_DAG_1' does not allow manual runs"):
        trigger_dag(
            dag_id="TEST_DAG_1",
            triggered_by=DagRunTriggeredByType.REST_API,
            session=session,
        )
