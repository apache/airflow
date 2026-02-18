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

from uuid import UUID

import pytest

from airflow.api_fastapi.core_api.datamodels.dag_versions import DagVersionResponse
from airflow.api_fastapi.core_api.datamodels.task_instances import TaskInstanceResponse

TEST_UUID_36 = "019c39c7-4fea-76b6-891d-ebb3267d427d"
TEST_UUID_32 = TEST_UUID_36.replace("-", "")
TEST_UUID = UUID(TEST_UUID_32)


@pytest.mark.parametrize(
    "model_cls",
    [
        TaskInstanceResponse,
        DagVersionResponse,
    ],
    ids=["TaskInstanceResponse", "DagVersionResponse"],
)
def test_uuid_field_serializes_as_dashed_string(model_cls):
    """Ensure UUID fields serialize to dashed string format in JSON responses.

    This guards against regressions that could break airflow-client-python
    and other consumers of the Public API that expect UUID fields to be
    plain dashed strings in JSON.
    """
    instance = model_cls.model_construct(id=TEST_UUID)
    json_data = instance.model_dump(mode="json")
    assert isinstance(json_data["id"], str)
    assert json_data["id"] == TEST_UUID_36
