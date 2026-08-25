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
from sqlalchemy import inspect

from airflow import settings

pytestmark = pytest.mark.db_test


def test_task_instance_launch_table_schema():
    inspector = inspect(settings.engine)

    columns = {column["name"]: column for column in inspector.get_columns("task_instance_launch")}
    assert set(columns) == {
        "token",
        "task_instance_id",
        "dag_id",
        "task_id",
        "run_id",
        "map_index",
        "try_number",
        "executor",
        "state",
        "created_at",
        "updated_at",
        "consumed_at",
        "superseded_at",
    }
    assert inspector.get_pk_constraint("task_instance_launch")["constrained_columns"] == ["token"]
    assert {index["name"] for index in inspector.get_indexes("task_instance_launch")} == {
        "idx_task_instance_launch_state_updated",
        "idx_task_instance_launch_task_instance_id",
        "idx_task_instance_launch_created_at",
    }
    constraints = inspector.get_check_constraints("task_instance_launch")
    assert "ck_task_instance_launch_state_enum" in {constraint["name"] for constraint in constraints}
