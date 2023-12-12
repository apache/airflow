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

from airflow.models.log import Log
from airflow.utils.session import create_session


def test_logging_pause_dag(admin_client, create_dummy_dag):
    dag, _ = create_dummy_dag()
    # is_paused=false mean pause the dag
    admin_client.post(f"/paused?is_paused=false&dag_id={dag.dag_id}", follow_redirects=True)
    with create_session() as session:
        dag_query = session.query(Log).filter(Log.dag_id == dag.dag_id)
        assert "('is_paused', True)" in dag_query.first().extra


def test_logging_unpuase_dag(admin_client, create_dummy_dag):
    dag, _ = create_dummy_dag(is_paused_upon_creation=True)
    # is_paused=true mean unpause the dag
    admin_client.post(f"/paused?is_paused=true&dag_id={dag.dag_id}", follow_redirects=True)
    with create_session() as session:
        dag_query = session.query(Log).filter(Log.dag_id == dag.dag_id)
        assert "('is_paused', False)" in dag_query.first().extra
