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

from airflow.models.log import Log

from tests_common.test_utils.db import clear_db_dags

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def dags(create_dummy_dag):
    paused_dag, _ = create_dummy_dag(dag_id="paused_dag", is_paused_upon_creation=True)
    dag, _ = create_dummy_dag(dag_id="unpaused_dag")

    yield dag, paused_dag

    clear_db_dags()


def test_logging_pause_dag(admin_client, dags, session):
    dag, _ = dags
    # is_paused=false mean pause the dag
    admin_client.post(f"/paused?is_paused=false&dag_id={dag.dag_id}", follow_redirects=True)
    dag_query = session.query(Log).filter(Log.dag_id == dag.dag_id)
    assert '{"is_paused": true}' in dag_query.first().extra


def test_logging_unpause_dag(admin_client, dags, session):
    _, paused_dag = dags
    # is_paused=true mean unpause the dag
    admin_client.post(f"/paused?is_paused=true&dag_id={paused_dag.dag_id}", follow_redirects=True)
    dag_query = session.query(Log).filter(Log.dag_id == paused_dag.dag_id)
    assert '{"is_paused": false}' in dag_query.first().extra
