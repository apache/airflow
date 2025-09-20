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

import uuid

from airflow.executors.workloads import TaskInstance


def test_taskinstance_dag_version_id_is_optional_in_model_construction():
    # Should construct without providing dag_version_id
    ti = TaskInstance(
        id=uuid.uuid4(),
        task_id="t1",
        dag_id="d1",
        run_id="r1",
        try_number=1,
        pool_slots=1,
        queue="default",
        priority_weight=1,
    )
    assert getattr(ti, "dag_version_id", None) is None


def test_taskinstance_json_schema_marks_dag_version_id_as_nullable_optional():
    schema = TaskInstance.model_json_schema()

    # Field should exist in properties
    assert "dag_version_id" in schema["properties"], "dag_version_id should be present in properties"

    # It should not be in required
    assert "required" in schema
    assert "dag_version_id" not in schema["required"], "dag_version_id should not be required"

    # Validate the OpenAPI/JSON Schema for optional nullable uuid
    dag_version_schema = schema["properties"]["dag_version_id"]

    # pydantic v2 emits anyOf: [{type: string, format: uuid}, {type: 'null'}]
    assert "anyOf" in dag_version_schema, "dag_version_id should be nullable (anyOf with null)"
    any_of = dag_version_schema["anyOf"]
    assert any(item.get("type") == "string" and item.get("format") == "uuid" for item in any_of), (
        "one variant must be uuid string"
    )
    assert any(item.get("type") == "null" for item in any_of), "one variant must be null"
