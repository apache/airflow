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

import json
from unittest.mock import Mock, patch

import pytest

from airflow.providers.common.compat.sdk import AirflowException, Connection, TaskDeferred
from airflow.providers.vespa.operators.vespa_ingest import VespaIngestOperator


def _make_context() -> dict[str, Mock]:
    return {"task_instance": Mock(spec=["task_id"])}


def _make_connection(
    *,
    host: str,
    schema: str | None,
    port: int | None = None,
    extra: dict | None = None,
) -> Connection:
    return Connection(
        conn_id="vespa_test",
        conn_type="vespa",
        host=host,
        schema=schema,
        port=port,
        extra=json.dumps(extra or {}),
    )


class TestVespaIngestOperator:
    def test_init(self):
        docs = [{"id": "1", "title": "Test"}]
        op = VespaIngestOperator(
            task_id="test_ingest",
            docs=docs,
            vespa_conn_id="test_conn",
            operation_type="feed",
        )

        assert op.docs == docs
        assert op.vespa_conn_id == "test_conn"
        assert op.operation_type == "feed"
        assert op.feed_kwargs == {}

    def test_init_with_feed_kwargs(self):
        op = VespaIngestOperator(
            task_id="test_ingest",
            docs=[{"id": "1", "fields": {"x": {"assign": 1}}}],
            operation_type="update",
            feed_kwargs={"auto_assign": False, "create": True},
        )

        assert op.feed_kwargs == {"auto_assign": False, "create": True}

    def test_init_invalid_operation_type(self):
        with pytest.raises(ValueError, match="Invalid operation_type"):
            VespaIngestOperator(task_id="test", docs=[{"id": "1"}], operation_type="upsert")

    @pytest.mark.parametrize(
        ("extra", "expected_namespace"),
        [
            pytest.param({"namespace": "test"}, "test", id="bare-namespace"),
            pytest.param({"extra__vespa__namespace": "legacy-test"}, "legacy-test", id="legacy-namespace"),
            pytest.param({}, "default", id="default-namespace"),
        ],
    )
    def test_execute_defers_to_trigger(self, extra, expected_namespace):
        with patch(
            "airflow.providers.vespa.operators.vespa_ingest.BaseHook.get_connection"
        ) as mock_get_connection:
            mock_get_connection.return_value = _make_connection(
                host="https://vespa.test",
                port=19071,
                schema="doc",
                extra=extra,
            )

            docs = [{"id": "1", "content": "test"}]
            op = VespaIngestOperator(task_id="test_task", docs=docs, vespa_conn_id="test_conn")

            with pytest.raises(TaskDeferred) as exc_info:
                op.execute(_make_context())

        trigger = exc_info.value.trigger
        assert trigger.docs == docs
        assert trigger.conn_info == {
            "host": "https://vespa.test",
            "port": 19071,
            "schema": "doc",
            "namespace": expected_namespace,
            "extra": extra,
        }
        assert trigger.operation_type == "feed"
        assert trigger.feed_kwargs == {}
        assert exc_info.value.method_name == "execute_complete"

    @patch("airflow.providers.vespa.operators.vespa_ingest.BaseHook.get_connection")
    def test_execute_forwards_feed_kwargs(self, mock_get_connection):
        mock_get_connection.return_value = _make_connection(
            host="https://vespa.test:8080",
            schema="doc",
            extra={"namespace": "test"},
        )

        op = VespaIngestOperator(
            task_id="test_task",
            docs=[{"id": "1", "fields": {"x": 1}}],
            vespa_conn_id="test_conn",
            operation_type="update",
            feed_kwargs={"auto_assign": False},
        )

        with pytest.raises(TaskDeferred) as exc_info:
            op.execute(_make_context())

        assert exc_info.value.trigger.feed_kwargs == {"auto_assign": False}

    @patch("airflow.providers.vespa.operators.vespa_ingest.BaseHook.get_connection")
    def test_execute_materializes_iterator(self, mock_get_connection):
        mock_get_connection.return_value = _make_connection(
            host="https://vespa.test:8080",
            schema="doc",
            extra={"namespace": "test"},
        )

        docs_iter = iter([{"id": "1"}, {"id": "2"}])
        op = VespaIngestOperator(task_id="test_task", docs=docs_iter, vespa_conn_id="test_conn")

        with pytest.raises(TaskDeferred) as exc_info:
            op.execute(_make_context())

        assert exc_info.value.trigger.docs == [{"id": "1"}, {"id": "2"}]

    def test_execute_complete_success(self):
        docs = [{"id": "1"}, {"id": "2"}]
        op = VespaIngestOperator(task_id="test_ingest", docs=docs, vespa_conn_id="test_conn")

        result = op.execute_complete({}, {"success": True, "sent": 2})
        assert result == {"ingested": 2}

    def test_execute_complete_failure_with_errors(self):
        docs = [{"id": "1"}, {"id": "2"}, {"id": "3"}]
        op = VespaIngestOperator(task_id="test_ingest", docs=docs, vespa_conn_id="test_conn")

        failure_event = {
            "success": False,
            "sent": 3,
            "errors": [
                {"id": "2", "status": 400, "reason": {"error": "Invalid format"}},
                {"id": "3", "status": 500, "reason": {"error": "Server error"}},
            ],
        }

        with pytest.raises(AirflowException) as exc_info:
            op.execute_complete({}, failure_event)

        msg = str(exc_info.value)
        assert "2 document(s) failed" in msg

    def test_execute_complete_failure_with_exception_error(self):
        op = VespaIngestOperator(task_id="test_ingest", docs=[{"id": "1"}], vespa_conn_id="test_conn")

        failure_event = {
            "success": False,
            "errors": [{"error": "Trigger failed: ConnectionError: Unable to connect"}],
        }

        with pytest.raises(AirflowException) as exc_info:
            op.execute_complete({}, failure_event)

        assert "ConnectionError" in str(exc_info.value)

    @patch("airflow.providers.vespa.operators.vespa_ingest.BaseHook.get_connection")
    def test_execute_materializes_to_self_docs(self, mock_get_connection):
        """After execute(), self.docs should be a list so execute_complete can use len()."""
        mock_get_connection.return_value = _make_connection(
            host="https://vespa.test:8080",
            schema="doc",
            extra={"namespace": "test"},
        )

        docs_iter = iter([{"id": "1"}, {"id": "2"}])
        op = VespaIngestOperator(task_id="test_task", docs=docs_iter, vespa_conn_id="test_conn")

        with pytest.raises(TaskDeferred):
            op.execute(_make_context())

        assert isinstance(op.docs, list)
        assert len(op.docs) == 2

    @patch("airflow.providers.vespa.operators.vespa_ingest.BaseHook.get_connection")
    def test_execute_rejects_non_dict_docs(self, mock_get_connection):
        mock_get_connection.return_value = _make_connection(
            host="https://vespa.test:8080",
            schema="doc",
            extra={"namespace": "test"},
        )

        op = VespaIngestOperator(
            task_id="test_task",
            docs=[{"id": "1"}, "not-a-dict"],
            vespa_conn_id="test_conn",
        )

        with pytest.raises(TypeError, match="docs\\[1\\] must be a dict"):
            op.execute(_make_context())

    @patch("airflow.providers.vespa.operators.vespa_ingest.BaseHook.get_connection")
    def test_execute_rejects_non_serializable_feed_kwargs(self, mock_get_connection):
        mock_get_connection.return_value = _make_connection(
            host="https://vespa.test:8080",
            schema="doc",
            extra={"namespace": "test"},
        )

        op = VespaIngestOperator(
            task_id="test_task",
            docs=[{"id": "1", "fields": {"x": 1}}],
            vespa_conn_id="test_conn",
            feed_kwargs={"callback": lambda response: response},
        )

        with pytest.raises(ValueError, match="feed_kwargs must be JSON-serializable"):
            op.execute(_make_context())
