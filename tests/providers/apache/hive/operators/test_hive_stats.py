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

import os
import re
import unittest
from collections import OrderedDict
from unittest.mock import patch

from airflow.exceptions import AirflowException
from airflow.providers.apache.hive.operators.hive_stats import HiveStatsCollectionOperator
from tests.providers.apache.hive import DEFAULT_DATE, DEFAULT_DATE_DS, TestHiveEnvironment
from tests.test_utils.mock_hooks import MockHiveMetastoreHook, MockMySqlHook, MockPrestoHook


class _FakeCol:
    def __init__(self, col_name, col_type):
        self.name = col_name
        self.type = col_type


fake_col = _FakeCol('col', 'string')


class TestHiveStatsCollectionOperator(TestHiveEnvironment):
    def setUp(self):
        self.kwargs = dict(
            table='table',
            partition=dict(col='col', value='value'),
            metastore_conn_id='metastore_conn_id',
            presto_conn_id='presto_conn_id',
            mysql_conn_id='mysql_conn_id',
            task_id='test_hive_stats_collection_operator',
        )
        super().setUp()

    def test_get_default_exprs(self):
        col = 'col'

        default_exprs = HiveStatsCollectionOperator(**self.kwargs).get_default_exprs(col, None)

        self.assertEqual(default_exprs, {(col, 'non_null'): 'COUNT({})'.format(col)})

    def test_get_default_exprs_excluded_cols(self):
        col = 'excluded_col'
        self.kwargs.update(dict(excluded_columns=[col]))

        default_exprs = HiveStatsCollectionOperator(**self.kwargs).get_default_exprs(col, None)

        self.assertEqual(default_exprs, {})

    def test_get_default_exprs_number(self):
        col = 'col'
        for col_type in ['double', 'int', 'bigint', 'float']:
            default_exprs = HiveStatsCollectionOperator(**self.kwargs).get_default_exprs(col, col_type)

            self.assertEqual(
                default_exprs,
                {
                    (col, 'avg'): 'AVG({})'.format(col),
                    (col, 'max'): 'MAX({})'.format(col),
                    (col, 'min'): 'MIN({})'.format(col),
                    (col, 'non_null'): 'COUNT({})'.format(col),
                    (col, 'sum'): 'SUM({})'.format(col),
                },
            )

    def test_get_default_exprs_boolean(self):
        col = 'col'
        col_type = 'boolean'

        default_exprs = HiveStatsCollectionOperator(**self.kwargs).get_default_exprs(col, col_type)

        self.assertEqual(
            default_exprs,
            {
                (col, 'false'): 'SUM(CASE WHEN NOT {} THEN 1 ELSE 0 END)'.format(col),
                (col, 'non_null'): 'COUNT({})'.format(col),
                (col, 'true'): 'SUM(CASE WHEN {} THEN 1 ELSE 0 END)'.format(col),
            },
        )

    def test_get_default_exprs_string(self):
        col = 'col'
        col_type = 'string'

        default_exprs = HiveStatsCollectionOperator(**self.kwargs).get_default_exprs(col, col_type)

        self.assertEqual(
            default_exprs,
            {
                (col, 'approx_distinct'): 'APPROX_DISTINCT({})'.format(col),
                (col, 'len'): 'SUM(CAST(LENGTH({}) AS BIGINT))'.format(col),
                (col, 'non_null'): 'COUNT({})'.format(col),
            },
        )

    @patch('airflow.providers.apache.hive.operators.hive_stats.json.dumps')
    @patch('airflow.providers.apache.hive.operators.hive_stats.MySqlHook')
    @patch('airflow.providers.apache.hive.operators.hive_stats.PrestoHook')
    @patch('airflow.providers.apache.hive.operators.hive_stats.HiveMetastoreHook')
    def test_execute(self, mock_hive_metastore_hook, mock_presto_hook, mock_mysql_hook, mock_json_dumps):
        mock_hive_metastore_hook.return_value.get_table.return_value.sd.cols = [fake_col]
        mock_mysql_hook.return_value.get_records.return_value = False

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
        exprs = {('', 'count'): 'COUNT(*)'}
        for col, col_type in list(field_types.items()):
            exprs.update(hive_stats_collection_operator.get_default_exprs(col, col_type))
        exprs = OrderedDict(exprs)
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
            table='hive_stats',
            rows=rows,
            target_fields=['ds', 'dttm', 'table_name', 'partition_repr', 'col', 'metric', 'value',],
        )

    @patch('airflow.providers.apache.hive.operators.hive_stats.json.dumps')
    @patch('airflow.providers.apache.hive.operators.hive_stats.MySqlHook')
    @patch('airflow.providers.apache.hive.operators.hive_stats.PrestoHook')
    @patch('airflow.providers.apache.hive.operators.hive_stats.HiveMetastoreHook')
    def test_execute_with_assignment_func(
        self, mock_hive_metastore_hook, mock_presto_hook, mock_mysql_hook, mock_json_dumps
    ):
        def assignment_func(col, _):
            return {(col, 'test'): 'TEST({})'.format(col)}

        self.kwargs.update(dict(assignment_func=assignment_func))
        mock_hive_metastore_hook.return_value.get_table.return_value.sd.cols = [fake_col]
        mock_mysql_hook.return_value.get_records.return_value = False

        hive_stats_collection_operator = HiveStatsCollectionOperator(**self.kwargs)
        hive_stats_collection_operator.execute(context={})

        field_types = {
            col.name: col.type for col in mock_hive_metastore_hook.return_value.get_table.return_value.sd.cols
        }
        exprs = {('', 'count'): 'COUNT(*)'}
        for col, col_type in list(field_types.items()):
            exprs.update(hive_stats_collection_operator.assignment_func(col, col_type))
        exprs = OrderedDict(exprs)
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
            table='hive_stats',
            rows=rows,
            target_fields=['ds', 'dttm', 'table_name', 'partition_repr', 'col', 'metric', 'value',],
        )

    @patch('airflow.providers.apache.hive.operators.hive_stats.json.dumps')
    @patch('airflow.providers.apache.hive.operators.hive_stats.MySqlHook')
    @patch('airflow.providers.apache.hive.operators.hive_stats.PrestoHook')
    @patch('airflow.providers.apache.hive.operators.hive_stats.HiveMetastoreHook')
    def test_execute_with_assignment_func_no_return_value(
        self, mock_hive_metastore_hook, mock_presto_hook, mock_mysql_hook, mock_json_dumps
    ):
        def assignment_func(_, __):
            pass

        self.kwargs.update(dict(assignment_func=assignment_func))
        mock_hive_metastore_hook.return_value.get_table.return_value.sd.cols = [fake_col]
        mock_mysql_hook.return_value.get_records.return_value = False

        hive_stats_collection_operator = HiveStatsCollectionOperator(**self.kwargs)
        hive_stats_collection_operator.execute(context={})

        field_types = {
            col.name: col.type for col in mock_hive_metastore_hook.return_value.get_table.return_value.sd.cols
        }
        exprs = {('', 'count'): 'COUNT(*)'}
        for col, col_type in list(field_types.items()):
            exprs.update(hive_stats_collection_operator.get_default_exprs(col, col_type))
        exprs = OrderedDict(exprs)
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
            table='hive_stats',
            rows=rows,
            target_fields=['ds', 'dttm', 'table_name', 'partition_repr', 'col', 'metric', 'value',],
        )

    @patch('airflow.providers.apache.hive.operators.hive_stats.MySqlHook')
    @patch('airflow.providers.apache.hive.operators.hive_stats.PrestoHook')
    @patch('airflow.providers.apache.hive.operators.hive_stats.HiveMetastoreHook')
    def test_execute_no_query_results(self, mock_hive_metastore_hook, mock_presto_hook, mock_mysql_hook):
        mock_hive_metastore_hook.return_value.get_table.return_value.sd.cols = [fake_col]
        mock_mysql_hook.return_value.get_records.return_value = False
        mock_presto_hook.return_value.get_first.return_value = None

        self.assertRaises(AirflowException, HiveStatsCollectionOperator(**self.kwargs).execute, context={})

    @patch('airflow.providers.apache.hive.operators.hive_stats.json.dumps')
    @patch('airflow.providers.apache.hive.operators.hive_stats.MySqlHook')
    @patch('airflow.providers.apache.hive.operators.hive_stats.PrestoHook')
    @patch('airflow.providers.apache.hive.operators.hive_stats.HiveMetastoreHook')
    def test_execute_delete_previous_runs_rows(
        self, mock_hive_metastore_hook, mock_presto_hook, mock_mysql_hook, mock_json_dumps
    ):
        mock_hive_metastore_hook.return_value.get_table.return_value.sd.cols = [fake_col]
        mock_mysql_hook.return_value.get_records.return_value = True

        hive_stats_collection_operator = HiveStatsCollectionOperator(**self.kwargs)
        hive_stats_collection_operator.execute(context={})

        sql = """
            DELETE FROM hive_stats
            WHERE
                table_name='{}' AND
                partition_repr='{}' AND
                dttm='{}';
            """.format(
            hive_stats_collection_operator.table,
            mock_json_dumps.return_value,
            hive_stats_collection_operator.dttm,
        )
        mock_mysql_hook.return_value.run.assert_called_once_with(sql)

    @unittest.skipIf(
        'AIRFLOW_RUNALL_TESTS' not in os.environ, "Skipped because AIRFLOW_RUNALL_TESTS is not set"
    )
    @patch(
        'airflow.providers.apache.hive.operators.hive_stats.HiveMetastoreHook',
        side_effect=MockHiveMetastoreHook,
    )
    def test_runs_for_hive_stats(self, mock_hive_metastore_hook):
        mock_mysql_hook = MockMySqlHook()
        mock_presto_hook = MockPrestoHook()
        with patch(
            'airflow.providers.apache.hive.operators.hive_stats.PrestoHook', return_value=mock_presto_hook
        ):
            with patch(
                'airflow.providers.apache.hive.operators.hive_stats.MySqlHook', return_value=mock_mysql_hook
            ):
                op = HiveStatsCollectionOperator(
                    task_id='hive_stats_check',
                    table="airflow.static_babynames_partitioned",
                    partition={'ds': DEFAULT_DATE_DS},
                    dag=self.dag,
                )
                op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        select_count_query = (
            "SELECT COUNT(*) AS __count FROM airflow."
            + "static_babynames_partitioned WHERE ds = '2015-01-01';"
        )
        mock_presto_hook.get_first.assert_called_with(hql=select_count_query)

        expected_stats_select_query = (
            "SELECT 1 FROM hive_stats WHERE table_name='airflow."
            + "static_babynames_partitioned' AND "
            + "partition_repr='{\"ds\": \"2015-01-01\"}' AND "
            + "dttm='2015-01-01T00:00:00+00:00' "
            + "LIMIT 1;"
        )

        raw_stats_select_query = mock_mysql_hook.get_records.call_args_list[0][0][0]
        actual_stats_select_query = re.sub(r'\s{2,}', ' ', raw_stats_select_query).strip()

        self.assertEqual(expected_stats_select_query, actual_stats_select_query)

        insert_rows_val = [
            (
                '2015-01-01',
                '2015-01-01T00:00:00+00:00',
                'airflow.static_babynames_partitioned',
                '{"ds": "2015-01-01"}',
                '',
                'count',
                ['val_0', 'val_1'],
            )
        ]

        mock_mysql_hook.insert_rows.assert_called_with(
            table='hive_stats',
            rows=insert_rows_val,
            target_fields=['ds', 'dttm', 'table_name', 'partition_repr', 'col', 'metric', 'value',],
        )
