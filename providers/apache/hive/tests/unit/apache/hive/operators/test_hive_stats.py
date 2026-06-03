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

import os
import re
from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.apache.hive.operators.hive_stats import HiveStatsCollectionOperator
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.presto.hooks.presto import PrestoHook

from unit.apache.hive import (
    DEFAULT_DATE,
    DEFAULT_DATE_DS,
    MockConnectionCursor,
    MockHiveMetastoreHook,
    MockMySqlHook,
    TestHiveEnvironment,
)


class _FakeCol:
    def __init__(self, col_name, col_type):
        self.name = col_name
        self.type = col_type


fake_col = _FakeCol("col", "string")


class MockPrestoHook(PrestoHook):
    def __init__(self, *args, **kwargs):
        self.conn = MockConnectionCursor()

        self.conn.execute = MagicMock()
        self.get_conn = MagicMock(return_value=self.conn)
        self.get_first = MagicMock(return_value=[["val_0", "val_1"], "val_2"])

        super().__init__(*args, **kwargs)

    def get_connection(self, *args):
        return self.conn


class TestHiveStatsCollectionOperator(TestHiveEnvironment):
    def setup_method(self, method):
        self.kwargs = dict(
            table="table",
            partition=dict(col="col", value="value"),
            metastore_conn_id="metastore_conn_id",
            presto_conn_id="presto_conn_id",
            mysql_conn_id="mysql_conn_id",
            task_id="test_hive_stats_collection_operator",
        )
        super().setup_method(method)

    def test_get_default_exprs(self):
        col = "col"

        default_exprs = HiveStatsCollectionOperator(**self.kwargs).get_default_exprs(col, None)

        assert default_exprs == {(col, "non_null"): f"COUNT({col})"}

    def test_get_default_exprs_excluded_cols(self):
        col = "excluded_col"
        self.kwargs.update(dict(excluded_columns=[col]))

        default_exprs = HiveStatsCollectionOperator(**self.kwargs).get_default_exprs(col, None)

        assert default_exprs == {}

    def test_get_default_exprs_number(self):
        col = "col"
        for col_type in ["double", "int", "bigint", "float"]:
            default_exprs = HiveStatsCollectionOperator(**self.kwargs).get_default_exprs(col, col_type)

            assert default_exprs == {
                (col, "avg"): f"AVG({col})",
                (col, "max"): f"MAX({col})",
                (col, "min"): f"MIN({col})",
                (col, "non_null"): f"COUNT({col})",
                (col, "sum"): f"SUM({col})",
            }

    def test_get_default_exprs_boolean(self):
        col = "col"
        col_type = "boolean"

        default_exprs = HiveStatsCollectionOperator(**self.kwargs).get_default_exprs(col, col_type)

        assert default_exprs == {
            (col, "false"): f"SUM(CASE WHEN NOT {col} THEN 1 ELSE 0 END)",
            (col, "non_null"): f"COUNT({col})",
            (col, "true"): f"SUM(CASE WHEN {col} THEN 1 ELSE 0 END)",
        }

    def test_get_default_exprs_string(self):
        col = "col"
        col_type = "string"

        default_exprs = HiveStatsCollectionOperator(**self.kwargs).get_default_exprs(col, col_type)

        assert default_exprs == {
            (col, "approx_distinct"): f"APPROX_DISTINCT({col})",
            (col, "len"): f"SUM(CAST(LENGTH({col}) AS BIGINT))",
            (col, "non_null"): f"COUNT({col})",
        }

    @patch("airflow.providers.apache.hive.operators.hive_stats.json.dumps")
    @patch("airflow.providers.apache.hive.operators.hive_stats.MySqlHook")
    @patch("airflow.providers.apache.hive.operators.hive_stats.PrestoHook")
    @patch("airflow.providers.apache.hive.operators.hive_stats.HiveMetastoreHook")
    def test_execute(self, mock_hive_metastore_hook, mock_presto_hook, mock_mysql_hook, mock_json_dumps):
        mock_hive_metastore_hook.return_value.get_table.return_value.sd.cols = [fake_col]
        mock_mysql_hook.return_value.get_records.return_value = False
        mock_presto_hook.return_value.placeholder = "?"

        hive_stats_collection_operator = HiveStatsCollectionOperator(**self.kwargs)
        hive_stats_collection_operator.execute(context={})

        mock_hive_metastore_hook.assert_called_once_with(
            metastore_conn_id=hive_stats_collection_operator.metastore_conn_id
        )
        mock_hive_metastore_hook.return_value.get_table.assert_called_once_with(
            table_name=hive_stats_collection_operator.table
        )
        mock_presto_hook.assert_called_once_with(presto_conn_id=hive_stats_collection_operator.presto_conn_id)
        mock_mysql_hook.assert_called_once_with(hive_stats_collection_operator.mysql_conn_id)
        mock_json_dumps.assert_called_once_with(hive_stats_collection_operator.partition, sort_keys=True)
        field_types = {
            col.name: col.type for col in mock_hive_metastore_hook.return_value.get_table.return_value.sd.cols
        }
        exprs = {("", "count"): "COUNT(*)"}
        for col, col_type in list(field_types.items()):
            exprs.update(hive_stats_collection_operator.get_default_exprs(col, col_type))
        rows = [
            (
                hive_stats_collection_operator.ds,
                hive_stats_collection_operator.dttm,
                hive_stats_collection_operator.table,
                mock_json_dumps.return_value,
            )
            + (r[0][0], r[0][1], r[1])
            for r in zip(exprs, mock_presto_hook.return_value.get_first.return_value)
        ]
        mock_mysql_hook.return_value.insert_rows.assert_called_once_with(
            table="hive_stats",
            rows=rows,
            target_fields=[
                "ds",
                "dttm",
                "table_name",
                "partition_repr",
                "col",
                "metric",
                "value",
            ],
        )

    @patch("airflow.providers.apache.hive.operators.hive_stats.json.dumps")
    @patch("airflow.providers.apache.hive.operators.hive_stats.MySqlHook")
    @patch("airflow.providers.apache.hive.operators.hive_stats.PrestoHook")
    @patch("airflow.providers.apache.hive.operators.hive_stats.HiveMetastoreHook")
    def test_execute_with_assignment_func(
        self, mock_hive_metastore_hook, mock_presto_hook, mock_mysql_hook, mock_json_dumps
    ):
        def assignment_func(col, _):
            return {(col, "test"): f"TEST({col})"}

        self.kwargs.update(dict(assignment_func=assignment_func))
        mock_hive_metastore_hook.return_value.get_table.return_value.sd.cols = [fake_col]
        mock_mysql_hook.return_value.get_records.return_value = False
        mock_presto_hook.return_value.placeholder = "?"

        hive_stats_collection_operator = HiveStatsCollectionOperator(**self.kwargs)
        hive_stats_collection_operator.execute(context={})

        field_types = {
            col.name: col.type for col in mock_hive_metastore_hook.return_value.get_table.return_value.sd.cols
        }
        exprs = {("", "count"): "COUNT(*)"}
        for col, col_type in list(field_types.items()):
            exprs.update(hive_stats_collection_operator.assignment_func(col, col_type))
        rows = [
            (
                hive_stats_collection_operator.ds,
                hive_stats_collection_operator.dttm,
                hive_stats_collection_operator.table,
                mock_json_dumps.return_value,
            )
            + (r[0][0], r[0][1], r[1])
            for r in zip(exprs, mock_presto_hook.return_value.get_first.return_value)
        ]
        mock_mysql_hook.return_value.insert_rows.assert_called_once_with(
            table="hive_stats",
            rows=rows,
            target_fields=[
                "ds",
                "dttm",
                "table_name",
                "partition_repr",
                "col",
                "metric",
                "value",
            ],
        )

    @patch("airflow.providers.apache.hive.operators.hive_stats.json.dumps")
    @patch("airflow.providers.apache.hive.operators.hive_stats.MySqlHook")
    @patch("airflow.providers.apache.hive.operators.hive_stats.PrestoHook")
    @patch("airflow.providers.apache.hive.operators.hive_stats.HiveMetastoreHook")
    def test_execute_with_assignment_func_no_return_value(
        self, mock_hive_metastore_hook, mock_presto_hook, mock_mysql_hook, mock_json_dumps
    ):
        def assignment_func(_, __):
            pass

        self.kwargs.update(dict(assignment_func=assignment_func))
        mock_hive_metastore_hook.return_value.get_table.return_value.sd.cols = [fake_col]
        mock_mysql_hook.return_value.get_records.return_value = False
        mock_presto_hook.return_value.placeholder = "?"

        hive_stats_collection_operator = HiveStatsCollectionOperator(**self.kwargs)
        hive_stats_collection_operator.execute(context={})

        field_types = {
            col.name: col.type for col in mock_hive_metastore_hook.return_value.get_table.return_value.sd.cols
        }
        exprs = {("", "count"): "COUNT(*)"}
        for col, col_type in list(field_types.items()):
            exprs.update(hive_stats_collection_operator.get_default_exprs(col, col_type))
        rows = [
            (
                hive_stats_collection_operator.ds,
                hive_stats_collection_operator.dttm,
                hive_stats_collection_operator.table,
                mock_json_dumps.return_value,
            )
            + (r[0][0], r[0][1], r[1])
            for r in zip(exprs, mock_presto_hook.return_value.get_first.return_value)
        ]
        mock_mysql_hook.return_value.insert_rows.assert_called_once_with(
            table="hive_stats",
            rows=rows,
            target_fields=[
                "ds",
                "dttm",
                "table_name",
                "partition_repr",
                "col",
                "metric",
                "value",
            ],
        )

    @patch("airflow.providers.apache.hive.operators.hive_stats.MySqlHook")
    @patch("airflow.providers.apache.hive.operators.hive_stats.PrestoHook")
    @patch("airflow.providers.apache.hive.operators.hive_stats.HiveMetastoreHook")
    def test_execute_no_query_results(self, mock_hive_metastore_hook, mock_presto_hook, mock_mysql_hook):
        mock_hive_metastore_hook.return_value.get_table.return_value.sd.cols = [fake_col]
        mock_mysql_hook.return_value.get_records.return_value = False
        mock_presto_hook.return_value.get_first.return_value = None
        mock_presto_hook.return_value.placeholder = "?"

        with pytest.raises(AirflowException):
            HiveStatsCollectionOperator(**self.kwargs).execute(context={})

    @patch("airflow.providers.apache.hive.operators.hive_stats.json.dumps")
    @patch("airflow.providers.apache.hive.operators.hive_stats.MySqlHook")
    @patch("airflow.providers.apache.hive.operators.hive_stats.PrestoHook")
    @patch("airflow.providers.apache.hive.operators.hive_stats.HiveMetastoreHook")
    def test_execute_delete_previous_runs_rows(
        self, mock_hive_metastore_hook, mock_presto_hook, mock_mysql_hook, mock_json_dumps
    ):
        mock_hive_metastore_hook.return_value.get_table.return_value.sd.cols = [fake_col]
        mock_mysql_hook.return_value.get_records.return_value = True
        mock_presto_hook.return_value.placeholder = "?"

        hive_stats_collection_operator = HiveStatsCollectionOperator(**self.kwargs)
        hive_stats_collection_operator.execute(context={})

        expected_sql = """
            DELETE FROM hive_stats
            WHERE
                table_name = %s AND
                partition_repr = %s AND
                dttm = %s;
            """
        mock_mysql_hook.return_value.run.assert_called_once_with(
            expected_sql,
            parameters=(
                hive_stats_collection_operator.table,
                mock_json_dumps.return_value,
                hive_stats_collection_operator.dttm,
            ),
        )

    @pytest.mark.parametrize(
        ("table", "expected_from"),
        [
            # Plain identifiers (including a qualified <db>.<table>) are emitted
            # unquoted exactly as before, so existing Dags are unaffected.
            ("plain_table", "FROM plain_table"),
            ("db.tbl", "FROM db.tbl"),
            # Identifiers with other characters are double-quoted; a qualified
            # name is quoted per dotted component.
            ("weird-table", 'FROM "weird-table"'),
            ("my-db.my-tbl", 'FROM "my-db"."my-tbl"'),
            # An embedded double quote is doubled, so an injection payload is
            # contained as a single inert identifier instead of breaking out of
            # the FROM clause (and is no longer rejected outright).
            ('evil"; DROP TABLE users--', 'FROM "evil""; DROP TABLE users--"'),
        ],
    )
    @patch("airflow.providers.apache.hive.operators.hive_stats.MySqlHook")
    @patch("airflow.providers.apache.hive.operators.hive_stats.PrestoHook")
    @patch("airflow.providers.apache.hive.operators.hive_stats.HiveMetastoreHook")
    def test_execute_quotes_table_identifier(
        self, mock_hive_metastore_hook, mock_presto_hook, mock_mysql_hook, table, expected_from
    ):
        mock_hive_metastore_hook.return_value.get_table.return_value.sd.cols = [fake_col]
        mock_mysql_hook.return_value.get_records.return_value = False
        mock_presto_hook.return_value.placeholder = "?"

        self.kwargs["table"] = table
        HiveStatsCollectionOperator(**self.kwargs).execute(context={})

        presto_sql = mock_presto_hook.return_value.get_first.call_args.args[0]
        assert expected_from in presto_sql

    @patch("airflow.providers.apache.hive.operators.hive_stats.MySqlHook")
    @patch("airflow.providers.apache.hive.operators.hive_stats.PrestoHook")
    @patch("airflow.providers.apache.hive.operators.hive_stats.HiveMetastoreHook")
    def test_execute_quotes_partition_column(
        self, mock_hive_metastore_hook, mock_presto_hook, mock_mysql_hook
    ):
        # A partition key that is not a plain identifier (here it contains a
        # space) is double-quoted in the WHERE clause while its value is still
        # bound as a parameter, so special-character columns keep working
        # without relying on the caller to escape them.
        mock_hive_metastore_hook.return_value.get_table.return_value.sd.cols = [fake_col]
        mock_mysql_hook.return_value.get_records.return_value = False
        mock_presto_hook.return_value.placeholder = "?"

        self.kwargs["partition"] = {"evil col": "value"}
        HiveStatsCollectionOperator(**self.kwargs).execute(context={})

        presto_call = mock_presto_hook.return_value.get_first.call_args
        assert '"evil col" = ?' in presto_call.args[0]
        assert presto_call.kwargs["parameters"] == ("value",)

    @patch("airflow.providers.apache.hive.operators.hive_stats.MySqlHook")
    @patch("airflow.providers.apache.hive.operators.hive_stats.PrestoHook")
    @patch("airflow.providers.apache.hive.operators.hive_stats.HiveMetastoreHook")
    def test_execute_quotes_special_char_projection_column(
        self, mock_hive_metastore_hook, mock_presto_hook, mock_mysql_hook
    ):
        # A metastore column whose name is not a plain identifier (here it contains
        # a hyphen) is double-quoted everywhere it is interpolated into the Presto
        # SELECT -- both in the stat expressions and in their aliases -- so the
        # projection no longer emits invalid SQL like COUNT(weird-col). The bare
        # column name is still what gets stored in hive_stats.col; the quoting must
        # not leak into the inserted data (the dict key stays unquoted).
        mock_hive_metastore_hook.return_value.get_table.return_value.sd.cols = [
            _FakeCol("weird-col", "string")
        ]
        mock_mysql_hook.return_value.get_records.return_value = False
        mock_presto_hook.return_value.placeholder = "?"
        # A string column yields three default exprs (non_null, len, approx_distinct)
        # plus the COUNT(*) entry; supply matching positional results so the pivot
        # that builds the inserted rows is exercised rather than zipping to empty.
        mock_presto_hook.return_value.get_first.return_value = [1, 2, 3, 4]

        HiveStatsCollectionOperator(**self.kwargs).execute(context={})

        presto_sql = mock_presto_hook.return_value.get_first.call_args.args[0]
        assert 'COUNT("weird-col")' in presto_sql
        assert 'AS "weird-col__non_null"' in presto_sql
        assert "COUNT(weird-col)" not in presto_sql

        inserted_rows = mock_mysql_hook.return_value.insert_rows.call_args.kwargs["rows"]
        col_values = {row[4] for row in inserted_rows}
        assert "weird-col" in col_values  # the bare column name is stored, ...
        assert '"weird-col"' not in col_values  # ... the SQL quoting did not leak into the data

    @patch("airflow.providers.apache.hive.operators.hive_stats.json.dumps")
    @patch("airflow.providers.apache.hive.operators.hive_stats.MySqlHook")
    @patch("airflow.providers.apache.hive.operators.hive_stats.PrestoHook")
    @patch("airflow.providers.apache.hive.operators.hive_stats.HiveMetastoreHook")
    def test_execute_parameterizes_mysql_bookkeeping_queries(
        self, mock_hive_metastore_hook, mock_presto_hook, mock_mysql_hook, mock_json_dumps
    ):
        # The bookkeeping SELECT and DELETE against hive_stats bind table,
        # partition_repr, and dttm as %s parameters instead of interpolating
        # them into the SQL body, so the operator does not rely on the
        # caller to escape those values. We use distinctive values for table
        # and dttm so the absence-from-SQL assertion is not satisfied by
        # accidental substrings of keywords like "table_name".
        mock_hive_metastore_hook.return_value.get_table.return_value.sd.cols = [fake_col]
        mock_mysql_hook.return_value.get_records.return_value = True
        mock_presto_hook.return_value.placeholder = "?"

        self.kwargs["table"] = "audit_db.audit_stats_table"
        self.kwargs["dttm"] = "audit-dttm-marker-2099"
        op = HiveStatsCollectionOperator(**self.kwargs)
        op.execute(context={})

        select_call = mock_mysql_hook.return_value.get_records.call_args
        delete_call = mock_mysql_hook.return_value.run.call_args

        select_sql = select_call.args[0]
        delete_sql = delete_call.args[0]
        assert "%s" in select_sql
        assert "%s" in delete_sql
        assert "audit_db.audit_stats_table" not in select_sql
        assert "audit_db.audit_stats_table" not in delete_sql
        assert "audit-dttm-marker-2099" not in select_sql
        assert "audit-dttm-marker-2099" not in delete_sql

        expected_params = (op.table, mock_json_dumps.return_value, op.dttm)
        assert select_call.kwargs["parameters"] == expected_params
        assert delete_call.kwargs["parameters"] == expected_params

    @patch("airflow.providers.apache.hive.operators.hive_stats.MySqlHook")
    @patch("airflow.providers.apache.hive.operators.hive_stats.PrestoHook")
    @patch("airflow.providers.apache.hive.operators.hive_stats.HiveMetastoreHook")
    def test_execute_parameterizes_presto_partition_values(
        self, mock_hive_metastore_hook, mock_presto_hook, mock_mysql_hook
    ):
        # Partition values cannot influence the Presto SQL body — they are
        # passed as bound parameters alongside the SELECT. PrestoHook uses
        # `?` as its driver placeholder (not the default `%s`).
        mock_hive_metastore_hook.return_value.get_table.return_value.sd.cols = [fake_col]
        mock_mysql_hook.return_value.get_records.return_value = False
        mock_presto_hook.return_value.placeholder = "?"

        self.kwargs["partition"] = {"col": "value"}
        HiveStatsCollectionOperator(**self.kwargs).execute(context={})

        presto_call = mock_presto_hook.return_value.get_first.call_args
        assert "col = ?" in presto_call.args[0]
        assert "'value'" not in presto_call.args[0]
        assert presto_call.kwargs["parameters"] == ("value",)

    @pytest.mark.skipif(
        "AIRFLOW_RUNALL_TESTS" not in os.environ, reason="Skipped because AIRFLOW_RUNALL_TESTS is not set"
    )
    @patch(
        "airflow.providers.apache.hive.operators.hive_stats.HiveMetastoreHook",
        side_effect=MockHiveMetastoreHook,
    )
    def test_runs_for_hive_stats(self, mock_hive_metastore_hook):
        mock_mysql_hook = MockMySqlHook()
        mock_presto_hook = MockPrestoHook()
        with patch(
            "airflow.providers.apache.hive.operators.hive_stats.PrestoHook", return_value=mock_presto_hook
        ):
            with patch(
                "airflow.providers.apache.hive.operators.hive_stats.MySqlHook", return_value=mock_mysql_hook
            ):
                op = HiveStatsCollectionOperator(
                    task_id="hive_stats_check",
                    table="airflow.static_babynames_partitioned",
                    partition={"ds": DEFAULT_DATE_DS},
                    dag=self.dag,
                )
                op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        select_count_query = (
            "SELECT COUNT(*) AS __count FROM airflow.static_babynames_partitioned WHERE ds = ?;"
        )
        presto_call = mock_presto_hook.get_first.call_args
        actual_presto_query = re.sub(r"\s{2,}", " ", presto_call.args[0]).strip()
        assert actual_presto_query == select_count_query
        assert presto_call.kwargs["parameters"] == ("2015-01-01",)

        expected_stats_select_query = (
            "SELECT 1 FROM hive_stats WHERE table_name = %s AND partition_repr = %s AND dttm = %s LIMIT 1;"
        )

        stats_select_call = mock_mysql_hook.get_records.call_args_list[0]
        raw_stats_select_query = stats_select_call[0][0]
        actual_stats_select_query = re.sub(r"\s{2,}", " ", raw_stats_select_query).strip()

        assert expected_stats_select_query == actual_stats_select_query
        assert stats_select_call.kwargs["parameters"] == (
            "airflow.static_babynames_partitioned",
            '{"ds": "2015-01-01"}',
            "2015-01-01T00:00:00+00:00",
        )

        insert_rows_val = [
            (
                "2015-01-01",
                "2015-01-01T00:00:00+00:00",
                "airflow.static_babynames_partitioned",
                '{"ds": "2015-01-01"}',
                "",
                "count",
                ["val_0", "val_1"],
            )
        ]

        mock_mysql_hook.insert_rows.assert_called_with(
            table="hive_stats",
            rows=insert_rows_val,
            target_fields=[
                "ds",
                "dttm",
                "table_name",
                "partition_repr",
                "col",
                "metric",
                "value",
            ],
        )
