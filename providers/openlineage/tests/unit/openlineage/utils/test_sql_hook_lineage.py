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

import pytest
from openlineage.client.event_v2 import Dataset as OpenLineageDataset
from openlineage.client.facet_v2 import external_query_run, sql_job

from airflow.providers.common.sql.hooks.lineage import SqlJobHookLineageExtra
from airflow.providers.openlineage.extractors.base import OperatorLineage
from airflow.providers.openlineage.sqlparser import SQLParser
from airflow.providers.openlineage.utils.sql_hook_lineage import (
    _get_hook_conn_id,
    _resolve_namespace,
    emit_lineage_from_sql_extras,
)

_MODULE = "airflow.providers.openlineage.utils.sql_hook_lineage"


def _make_extra(sql="", job_id=None, hook=None, default_db=None):
    """Helper to create a mock ExtraLineageInfo with the expected structure."""
    value = {}
    if sql:
        value[SqlJobHookLineageExtra.VALUE__SQL_STATEMENT.value] = sql
    if job_id is not None:
        value[SqlJobHookLineageExtra.VALUE__JOB_ID.value] = job_id
    if default_db is not None:
        value[SqlJobHookLineageExtra.VALUE__DEFAULT_DB.value] = default_db
    extra = mock.MagicMock()
    extra.value = value
    extra.context = hook or mock.MagicMock()
    return extra


class TestGetHookConnId:
    def test_get_conn_id_from_method(self):
        hook = mock.MagicMock()
        hook.get_conn_id.return_value = "my_conn"
        assert _get_hook_conn_id(hook) == "my_conn"

    def test_get_conn_id_from_attribute(self):
        hook = mock.MagicMock(spec=[])
        hook.conn_name_attr = "my_conn_attr"
        hook.my_conn_attr = "fallback_conn"
        assert _get_hook_conn_id(hook) == "fallback_conn"

    def test_returns_none_when_nothing_available(self):
        hook = mock.MagicMock(spec=[])
        assert _get_hook_conn_id(hook) is None


class TestResolveNamespace:
    def test_from_ol_database_info(self):
        hook = mock.MagicMock()
        connection = mock.MagicMock()
        hook.get_connection.return_value = connection
        database_info = mock.MagicMock()
        hook.get_openlineage_database_info.return_value = database_info

        with mock.patch(
            "airflow.providers.openlineage.utils.sql_hook_lineage.SQLParser.create_namespace",
            return_value="postgres://host:5432/mydb",
        ) as mock_create_ns:
            result = _resolve_namespace(hook, "my_conn")

        hook.get_connection.assert_called_once_with("my_conn")
        hook.get_openlineage_database_info.assert_called_once_with(connection)
        mock_create_ns.assert_called_once_with(database_info)
        assert result == "postgres://host:5432/mydb"

    def test_returns_none_when_no_namespace_available(self):
        hook = mock.MagicMock()
        hook.__class__.__name__ = "SomeUnknownHook"
        hook.get_connection.side_effect = Exception("no method")

        with mock.patch.dict("sys.modules"):
            result = _resolve_namespace(hook, "my_conn")

        assert result is None

    def test_returns_none_when_no_conn_id(self):
        hook = mock.MagicMock()
        hook.__class__.__name__ = "SomeUnknownHook"

        with mock.patch.dict("sys.modules"):
            result = _resolve_namespace(hook, None)

        assert result is None


class TestEmitLineageFromSqlExtras:
    @pytest.fixture(autouse=True)
    def _patch_deps(self):
        with (
            mock.patch(f"{_MODULE}._get_hook_conn_id", return_value="my_conn") as mock_conn_id,
            mock.patch(f"{_MODULE}._resolve_namespace") as mock_ns,
            mock.patch(f"{_MODULE}.get_openlineage_facets_with_sql") as mock_facets_fn,
            mock.patch(f"{_MODULE}._create_ol_event_pair") as mock_build,
            mock.patch(f"{_MODULE}.get_openlineage_listener") as mock_listener,
        ):
            self.mock_conn_id = mock_conn_id
            self.mock_ns = mock_ns
            self.mock_facets_fn = mock_facets_fn
            self.mock_build = mock_build
            self.mock_listener = mock_listener
            mock_build.return_value = (mock.sentinel.start_event, mock.sentinel.end_event)
            yield

    @pytest.mark.parametrize(
        "sql_extras",
        [
            pytest.param([], id="empty_list"),
            pytest.param([_make_extra(sql="", job_id=None)], id="single_empty_extra"),
            pytest.param(
                [_make_extra(sql=None, job_id=None), _make_extra(sql="", job_id=None), _make_extra(sql="")],
                id="multiple_empty_extras",
            ),
        ],
    )
    def test_no_processable_extras(self, sql_extras):
        result = emit_lineage_from_sql_extras(
            task_instance=mock.MagicMock(),
            sql_extras=sql_extras,
        )
        assert result is None
        self.mock_build.assert_not_called()
        self.mock_listener.assert_not_called()

    def test_single_query_delegates_to_create_ol_event_pair(self):
        self.mock_ns.return_value = "postgres://host/db"
        mock_ti = mock.MagicMock(dag_id="dag_id", task_id="task_id")

        parsed_sql_facet = sql_job.SQLJobFacet(query="SELECT 1")
        parsed_inputs = [OpenLineageDataset(namespace="ns", name="in_table")]
        parsed_outputs = [OpenLineageDataset(namespace="ns", name="out_table")]
        self.mock_facets_fn.return_value = OperatorLineage(
            inputs=parsed_inputs,
            outputs=parsed_outputs,
            job_facets={"sql": parsed_sql_facet},
        )

        result = emit_lineage_from_sql_extras(
            task_instance=mock_ti,
            sql_extras=[_make_extra(sql="SELECT 1", job_id="qid-1")],
            is_successful=True,
        )

        assert result is None
        self.mock_build.assert_called_once()
        call = self.mock_build.call_args
        assert call.kwargs["task_instance"] is mock_ti
        assert call.kwargs["job_name"] == "dag_id.task_id.query.1"
        assert call.kwargs["is_successful"] is True
        assert call.kwargs["inputs"] == parsed_inputs
        assert call.kwargs["outputs"] == parsed_outputs
        assert call.kwargs["run_facets"]["externalQuery"].externalQueryId == "qid-1"
        assert call.kwargs["run_facets"]["externalQuery"].source == "postgres://host/db"
        assert call.kwargs["job_facets"]["sql"] is parsed_sql_facet

        adapter = self.mock_listener.return_value.adapter
        assert adapter.emit.call_args_list == [
            mock.call(mock.sentinel.start_event),
            mock.call(mock.sentinel.end_event),
        ]

    def test_multiple_queries_increment_counter(self):
        self.mock_ns.return_value = "postgres://host/db"
        mock_ti = mock.MagicMock(dag_id="dag_id", task_id="task_id")
        self.mock_facets_fn.side_effect = lambda **kw: OperatorLineage(
            job_facets={"sql": sql_job.SQLJobFacet(query=kw.get("sql", ""))},
        )
        pair1 = (mock.MagicMock(), mock.MagicMock())
        pair2 = (mock.MagicMock(), mock.MagicMock())
        self.mock_build.side_effect = [pair1, pair2]

        extras = [
            _make_extra(sql="SELECT 1", job_id="qid-1"),
            _make_extra(sql="SELECT 2", job_id="qid-2"),
        ]
        emit_lineage_from_sql_extras(task_instance=mock_ti, sql_extras=extras)

        assert self.mock_build.call_count == 2
        first, second = self.mock_build.call_args_list
        assert first.kwargs["job_name"] == "dag_id.task_id.query.1"
        assert second.kwargs["job_name"] == "dag_id.task_id.query.2"

        adapter = self.mock_listener.return_value.adapter
        assert adapter.emit.call_args_list == [
            mock.call(pair1[0]),
            mock.call(pair1[1]),
            mock.call(pair2[0]),
            mock.call(pair2[1]),
        ]

    def test_sql_parsing_failure_falls_back_to_normalized_sql_facet(self):
        self.mock_ns.return_value = "ns"
        self.mock_facets_fn.side_effect = Exception("parse error")
        mock_ti = mock.MagicMock(dag_id="dag_id", task_id="task_id")

        result = emit_lineage_from_sql_extras(
            task_instance=mock_ti,
            sql_extras=[_make_extra(sql="SELECT broken(", job_id="qid-1")],
        )

        assert result is None
        # When SQL parsing fails, a bare OperatorLineage with just the normalized SQL facet is
        # constructed; externalQuery is added from (job_id, namespace).
        self.mock_build.assert_called_once()
        call = self.mock_build.call_args
        assert call.kwargs["inputs"] == []
        assert call.kwargs["outputs"] == []
        assert call.kwargs["job_facets"]["sql"].query == SQLParser.normalize_sql("SELECT broken(")
        assert call.kwargs["run_facets"]["externalQuery"].externalQueryId == "qid-1"

    def test_no_namespace_skips_external_query_facet(self):
        """When namespace cannot be resolved, no externalQuery facet is added."""
        self.mock_ns.return_value = None
        self.mock_facets_fn.return_value = None
        mock_ti = mock.MagicMock(dag_id="dag_id", task_id="task_id")

        emit_lineage_from_sql_extras(
            task_instance=mock_ti,
            sql_extras=[_make_extra(sql="SELECT 1", job_id="qid-1")],
        )

        self.mock_build.assert_called_once()
        assert "externalQuery" not in self.mock_build.call_args.kwargs["run_facets"]

    def test_failed_state_forwarded(self):
        self.mock_ns.return_value = "postgres://host/db"
        mock_ti = mock.MagicMock(dag_id="dag_id", task_id="task_id")
        self.mock_facets_fn.return_value = OperatorLineage(
            job_facets={"sql": sql_job.SQLJobFacet(query="SELECT 1")},
        )

        emit_lineage_from_sql_extras(
            task_instance=mock_ti,
            sql_extras=[_make_extra(sql="SELECT 1", job_id="qid-1")],
            is_successful=False,
        )

        self.mock_build.assert_called_once()
        assert self.mock_build.call_args.kwargs["is_successful"] is False

    def test_emission_failure_does_not_raise(self, caplog):
        self.mock_ns.return_value = None
        self.mock_facets_fn.return_value = OperatorLineage()
        self.mock_listener.side_effect = Exception("listener unavailable")
        mock_ti = mock.MagicMock(dag_id="dag_id", task_id="task_id")

        with caplog.at_level(logging.WARNING, logger=_MODULE):
            result = emit_lineage_from_sql_extras(
                task_instance=mock_ti,
                sql_extras=[_make_extra(sql="SELECT 1")],
            )

        assert result is None
        assert "Failed to emit OpenLineage events for SQL hook lineage" in caplog.text

    def test_job_id_only_extra_is_processed(self):
        """An extra with only job_id (no SQL text) still builds and emits an event pair."""
        self.mock_conn_id.return_value = None
        self.mock_ns.return_value = "ns"
        self.mock_facets_fn.return_value = None
        mock_ti = mock.MagicMock(dag_id="dag_id", task_id="task_id")

        emit_lineage_from_sql_extras(
            task_instance=mock_ti,
            sql_extras=[_make_extra(sql="", job_id="external-123")],
        )

        self.mock_build.assert_called_once()
        call = self.mock_build.call_args
        assert call.kwargs["run_facets"]["externalQuery"].externalQueryId == "external-123"
        assert "sql" not in call.kwargs["job_facets"]

    def test_parser_run_facets_preserved_over_external_query(self):
        """Parser-produced externalQuery run facet is preserved (setdefault is a no-op)."""
        self.mock_ns.return_value = "ns"
        self.mock_conn_id.return_value = "conn"
        mock_ti = mock.MagicMock(dag_id="dag_id", task_id="task_id")

        parser_ext_query = external_query_run.ExternalQueryRunFacet(
            externalQueryId="parser-produced-id", source="parser-source"
        )
        self.mock_facets_fn.return_value = OperatorLineage(
            run_facets={"externalQuery": parser_ext_query},
        )

        emit_lineage_from_sql_extras(
            task_instance=mock_ti,
            sql_extras=[_make_extra(sql="SELECT 1", job_id="qid-1")],
        )

        call = self.mock_build.call_args
        assert call.kwargs["run_facets"]["externalQuery"] is parser_ext_query
