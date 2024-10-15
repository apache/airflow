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

from airflow.assets import Asset
from airflow.operators.empty import EmptyOperator
from tests_common.test_utils.db import initial_db_init

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def cleanup():
    """
    Before each test re-init the database dropping and recreating the tables.
    This will allow to reset indexes to be able to assert auto-incremented primary keys.
    """
    initial_db_init()


def test_next_run_assets(test_client, dag_maker):
    with dag_maker(dag_id="upstream", schedule=[Asset(uri="s3://bucket/key/1")], serialized=True):
        EmptyOperator(task_id="task1")

    dag_maker.create_dagrun()
    dag_maker.dagbag.sync_to_db()

    response = test_client.get("/ui/next_run_assets/upstream")

    assert response.status_code == 200
    assert response.json() == {
        "dataset_expression": {"all": ["s3://bucket/key/1"]},
        "events": [{"id": 17, "uri": "s3://bucket/key/1", "lastUpdate": None}],
    }
