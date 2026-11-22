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

from unittest import mock

import pytest

from airflow.providers.databricks.exceptions import DatabricksVectorSearchError, DatabricksVectorSearchTimeout
from airflow.providers.databricks.triggers.vector_search import DatabricksVectorSearchIndexTrigger

INDEX_NAME = "catalog.schema.index"


def test_serialization_roundtrip():
    trigger = DatabricksVectorSearchIndexTrigger(
        index_name=INDEX_NAME,
        databricks_conn_id="custom_connection",
        timeout=120,
        polling_period_seconds=5,
        retry_limit=4,
        retry_delay=2,
        retry_args={"reraise": True},
    )

    class_path, kwargs = trigger.serialize()

    assert (
        class_path == "airflow.providers.databricks.triggers.vector_search.DatabricksVectorSearchIndexTrigger"
    )
    assert DatabricksVectorSearchIndexTrigger(**kwargs).serialize() == (class_path, kwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "status", "message"),
    [
        (None, "success", "The Vector Search index reports no pending updates."),
        (
            DatabricksVectorSearchTimeout("remote secret"),
            "timeout",
            "Timed out waiting for the Vector Search index.",
        ),
        (
            DatabricksVectorSearchError("remote secret"),
            "error",
            "The Vector Search index failed to reach a state without pending updates.",
        ),
        (RuntimeError("remote secret"), "error", "Failed to read the Vector Search index status."),
    ],
)
@mock.patch("airflow.providers.databricks.triggers.vector_search.DatabricksHook", autospec=True)
async def test_trigger_outcome(mock_hook, error, status, message):
    mock_hook.return_value.a_wait_for_vector_search_index.side_effect = error
    trigger = DatabricksVectorSearchIndexTrigger(
        index_name=INDEX_NAME,
        databricks_conn_id="custom_connection",
        timeout=120,
        polling_period_seconds=5,
        retry_limit=4,
        retry_delay=2,
        retry_args={"reraise": True},
    )

    events = [event async for event in trigger.run()]

    assert len(events) == 1
    assert events[0].payload == {"status": status, "index_name": INDEX_NAME, "message": message}
    mock_hook.assert_called_once_with(
        "custom_connection",
        retry_limit=4,
        retry_delay=2,
        retry_args={"reraise": True},
        caller="DatabricksVectorSearchIndexTrigger",
    )
    mock_hook.return_value.a_wait_for_vector_search_index.assert_awaited_once_with(
        INDEX_NAME, timeout=120, polling_period_seconds=5
    )


def test_rejects_nonserializable_retries():
    with pytest.raises(ValueError, match="non-serializable"):
        DatabricksVectorSearchIndexTrigger(index_name=INDEX_NAME, retry_args={"stop": lambda retry: True})


def test_rejects_invalid_wait_budget():
    with pytest.raises(ValueError, match="timeout must be finite and greater than zero"):
        DatabricksVectorSearchIndexTrigger(index_name=INDEX_NAME, timeout=0)
