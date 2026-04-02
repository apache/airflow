#
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

import logging
from unittest import mock

from airflow.providers.common.sql.hooks.lineage import (
    SqlJobHookLineageExtra,
    send_sql_hook_lineage,
)


class TestSqlJobHookLineageExtra:
    def test_key_value(self):
        assert SqlJobHookLineageExtra.KEY.value == "sql_job"

    def test_value_keys_includes_all_value_members(self):
        keys = SqlJobHookLineageExtra.value_keys()
        assert len(keys) == 7
        assert keys == (
            SqlJobHookLineageExtra.VALUE__SQL_STATEMENT,
            SqlJobHookLineageExtra.VALUE__SQL_STATEMENT_PARAMETERS,
            SqlJobHookLineageExtra.VALUE__JOB_ID,
            SqlJobHookLineageExtra.VALUE__ROW_COUNT,
            SqlJobHookLineageExtra.VALUE__DEFAULT_DB,
            SqlJobHookLineageExtra.VALUE__DEFAULT_SCHEMA,
            SqlJobHookLineageExtra.VALUE__EXTRA,
        )


class TestSendSqlHookLineage:
    """Test send_sql_hook_lineage calls get_hook_lineage_collector().add_extra with correct params."""

    @mock.patch("airflow.providers.common.sql.hooks.lineage.get_hook_lineage_collector")
    def test_add_extra_called_with_minimal_args(self, mock_get_collector):
        mock_collector = mock.MagicMock()
        mock_get_collector.return_value = mock_collector
        mock_context = mock.MagicMock()

        send_sql_hook_lineage(context=mock_context, sql="SELECT 1")

        mock_collector.add_extra.assert_called_once()
        call_kw = mock_collector.add_extra.call_args.kwargs
        assert len(call_kw) == 3
        assert call_kw["context"] is mock_context
        assert call_kw["key"] == "sql_job"
        assert len(call_kw["value"]) == 1
        assert call_kw["value"] == {"sql": "SELECT 1"}

    @mock.patch("airflow.providers.common.sql.hooks.lineage.get_hook_lineage_collector")
    def test_add_extra_called_with_sql_list_joined(self, mock_get_collector):
        mock_collector = mock.MagicMock()
        mock_get_collector.return_value = mock_collector
        mock_context = mock.MagicMock()

        send_sql_hook_lineage(context=mock_context, sql=["SELECT 1", "SELECT 2"])

        mock_collector.add_extra.assert_called_once()
        call_kw = mock_collector.add_extra.call_args.kwargs
        assert len(call_kw) == 3
        assert call_kw["context"] is mock_context
        assert call_kw["key"] == "sql_job"
        assert len(call_kw["value"]) == 1
        assert call_kw["value"] == {"sql": "SELECT 1; SELECT 2"}

    @mock.patch("airflow.providers.common.sql.hooks.lineage.get_hook_lineage_collector")
    def test_add_extra_called_with_all_args_no_cursor(self, mock_get_collector):
        mock_collector = mock.MagicMock()
        mock_get_collector.return_value = mock_collector
        mock_context = mock.MagicMock()

        send_sql_hook_lineage(
            context=mock_context,
            sql="INSERT INTO t VALUES (%s)",
            sql_parameters=("x",),
            job_id="job-123",
            row_count=42,
            default_db="mydb",
            default_schema="myschema",
            extra={"custom": "data"},
        )

        mock_collector.add_extra.assert_called_once()
        call_kw = mock_collector.add_extra.call_args.kwargs
        assert len(call_kw) == 3
        assert call_kw["context"] is mock_context
        assert call_kw["key"] == "sql_job"
        value = call_kw["value"]
        assert len(value) == 7
        assert value == {
            "sql": "INSERT INTO t VALUES (%s)",
            "sql_parameters": ("x",),
            "job_id": "job-123",
            "row_count": 42,
            "default_db": "mydb",
            "default_schema": "myschema",
            "extra": {"custom": "data"},
        }

    @mock.patch("airflow.providers.common.sql.hooks.lineage.get_hook_lineage_collector")
    def test_add_extra_job_id_from_cursor(self, mock_get_collector):
        mock_collector = mock.MagicMock()
        mock_get_collector.return_value = mock_collector
        mock_context = mock.MagicMock()
        mock_cur = mock.MagicMock()
        mock_cur.query_id = "cursor-query-id"

        send_sql_hook_lineage(context=mock_context, sql="SELECT 1", cur=mock_cur)

        call_kw = mock_collector.add_extra.call_args.kwargs
        assert len(call_kw) == 3
        assert call_kw["context"] is mock_context
        assert call_kw["key"] == "sql_job"
        value = call_kw["value"]
        assert len(value) == 2
        assert value == {"sql": "SELECT 1", "job_id": "cursor-query-id"}

    @mock.patch("airflow.providers.common.sql.hooks.lineage.get_hook_lineage_collector")
    def test_add_extra_row_count_from_cursor(self, mock_get_collector):
        mock_collector = mock.MagicMock()
        mock_get_collector.return_value = mock_collector
        mock_context = mock.MagicMock()
        mock_cur = mock.MagicMock()
        mock_cur.rowcount = 10
        mock_cur.query_id = "123"

        send_sql_hook_lineage(context=mock_context, sql="SELECT 1", cur=mock_cur)

        call_kw = mock_collector.add_extra.call_args.kwargs
        assert len(call_kw) == 3
        assert call_kw["context"] is mock_context
        assert call_kw["key"] == "sql_job"
        value = call_kw["value"]
        assert len(value) == 3
        assert value == {"sql": "SELECT 1", "row_count": 10, "job_id": "123"}

    @mock.patch("airflow.providers.common.sql.hooks.lineage.get_hook_lineage_collector")
    def test_explicit_job_id_overrides_cursor(self, mock_get_collector):
        mock_collector = mock.MagicMock()
        mock_get_collector.return_value = mock_collector
        mock_context = mock.MagicMock()
        mock_cur = mock.MagicMock()
        mock_cur.query_id = "cursor-id"

        send_sql_hook_lineage(context=mock_context, sql="SELECT 1", cur=mock_cur, job_id="explicit-id")

        call_kw = mock_collector.add_extra.call_args.kwargs
        assert len(call_kw) == 3
        assert call_kw["context"] is mock_context
        assert call_kw["key"] == "sql_job"
        value = call_kw["value"]
        assert len(value) == 2
        assert value == {"sql": "SELECT 1", "job_id": "explicit-id"}

    @mock.patch("airflow.providers.common.sql.hooks.lineage.get_hook_lineage_collector")
    def test_explicit_row_count_overrides_cursor(self, mock_get_collector):
        mock_collector = mock.MagicMock()
        mock_get_collector.return_value = mock_collector
        mock_context = mock.MagicMock()
        mock_cur = mock.MagicMock()
        mock_cur.rowcount = 99
        del mock_cur.query_id
        del mock_cur.sfqid

        send_sql_hook_lineage(context=mock_context, sql="SELECT 1", cur=mock_cur, row_count=1)

        call_kw = mock_collector.add_extra.call_args.kwargs
        assert len(call_kw) == 3
        assert call_kw["context"] is mock_context
        assert call_kw["key"] == "sql_job"
        value = call_kw["value"]
        assert len(value) == 2
        assert value == {"sql": "SELECT 1", "row_count": 1}

    @mock.patch("airflow.providers.common.sql.hooks.lineage.get_hook_lineage_collector")
    def test_exception_is_swallowed_and_logged(self, mock_get_collector, caplog):
        mock_collector = mock.MagicMock()
        mock_collector.add_extra.side_effect = RuntimeError("collector broke")
        mock_get_collector.return_value = mock_collector

        with caplog.at_level(logging.WARNING, logger="airflow.providers.common.sql.hooks.lineage"):
            send_sql_hook_lineage(context=mock.MagicMock(), sql="SELECT 1")

        assert "Sending SQL hook level lineage failed" in caplog.text
        assert "RuntimeError: collector broke" in caplog.text
