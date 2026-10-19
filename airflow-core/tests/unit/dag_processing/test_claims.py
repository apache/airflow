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
from sqlalchemy import select, text

from airflow.dag_processing.claims import get_available_work_filter
from airflow.models.callback import DagProcessorCallback
from airflow.models.dagbag import DagPriorityParsingRequest

pytestmark = pytest.mark.db_test


@pytest.mark.parametrize("model", [DagProcessorCallback, DagPriorityParsingRequest])
def test_available_work_looks_up_owner_without_scanning_job_history(session, model):
    dialect = session.get_bind().dialect
    if dialect.name != "sqlite":
        pytest.skip("Reads the SQLite query plan")

    query = select(model.id).where(get_available_work_filter(model.processor_job_id)).limit(100)
    sql = query.compile(dialect=dialect, compile_kwargs={"literal_binds": True})
    plans = [row[-1] for row in session.execute(text(f"EXPLAIN QUERY PLAN {sql}"))]

    assert any("SEARCH job USING INTEGER PRIMARY KEY" in plan for plan in plans), plans
    assert not any("SCAN job" in plan for plan in plans), plans
