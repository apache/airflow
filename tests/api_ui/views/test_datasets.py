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

from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator

pytestmark = pytest.mark.db_test


def test_next_run_datasets_unauthenticated(unauthenticated_test_client, dag_maker):
    response = unauthenticated_test_client.get("/ui/next_run_datasets/upstream")
    assert response.status_code == 401


def test_next_run_datasets_success(authenticated_test_client, dag_maker):
    with dag_maker(dag_id="upstream", schedule=[Dataset(uri="s3://bucket/key/1")], serialized=True):
        EmptyOperator(task_id="task1")

    dag_maker.create_dagrun()
    dag_maker.dagbag.sync_to_db()

    response = authenticated_test_client.get(
        "/ui/next_run_datasets/upstream",
    )

    assert response.status_code == 200
    assert response.json() == {
        "dataset_expression": {"all": ["s3://bucket/key/1"]},
        "events": [{"id": 17, "uri": "s3://bucket/key/1", "lastUpdate": None}],
    }
